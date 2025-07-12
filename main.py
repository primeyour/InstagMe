import os
import threading
import concurrent.futures
from http.server import HTTPServer, BaseHTTPRequestHandler

from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton
from instagrapi import Client as InstaClient
from dotenv import load_dotenv

# === LOAD ENV ===
load_dotenv()
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "20836266"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "bbdd206f92e1ca4bc4935b43dfd4a2a1")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7983901811:AAGi4rscPTCS_WNND9unHi8ZaUgkMmVz1vI")
INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME", "")
INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASSWORD", "")
INSTAGRAM_PROXY = os.getenv("INSTAGRAM_PROXY", "")  # Example: http://user:pass@ip:port

AUTHORIZED_USERS_FILE = "authorized_users.txt"
SESSION_FILE = "insta_settings.json"

insta_client = InstaClient()
app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

main_menu = ReplyKeyboardMarkup(
    [
        [KeyboardButton("\ud83d\udce4 Upload a Reel")],
        [KeyboardButton("\ud83d\udce4 Upload Multiple Reels")]
    ],
    resize_keyboard=True
)

user_states = {}

def is_authorized(user_id):
    try:
        with open(AUTHORIZED_USERS_FILE, "r") as file:
            return str(user_id) in file.read().splitlines()
    except FileNotFoundError:
        return False

def safe_instagram_login():
    if INSTAGRAM_PROXY:
        insta_client.set_proxy(INSTAGRAM_PROXY)
    if os.path.exists(SESSION_FILE):
        insta_client.load_settings(SESSION_FILE)
    insta_client.login(INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD)
    insta_client.dump_settings(SESSION_FILE)

@app.on_message(filters.command("start"))
async def start(client, message):
    user_id = message.from_user.id
    if not is_authorized(user_id):
        await message.reply(f"\u26d4 Not authorized.\n\ud83c\udd94 Your ID: {user_id}")
        return
    await message.reply("\ud83d\udc4b Welcome! Choose an option below:", reply_markup=main_menu)

@app.on_message(filters.command("login"))
async def login_instagram(client, message):
    user_id = message.from_user.id
    if not is_authorized(user_id):
        await message.reply("\u26d4 You are not authorized.")
        return

    try:
        args = message.text.split(maxsplit=2)
        if len(args) != 3:
            await message.reply("\u2757 Usage: /login username password")
            return

        username, password = args[1], args[2]
        await message.reply("\ud83d\udd10 Logging into Instagram...")

        def do_login():
            temp_client = InstaClient()
            if INSTAGRAM_PROXY:
                temp_client.set_proxy(INSTAGRAM_PROXY)
            temp_client.login(username, password)
            temp_client.dump_settings(SESSION_FILE)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(do_login)
            future.result(timeout=30)

        await message.reply("\u2705 Instagram login successful and session saved.")

    except concurrent.futures.TimeoutError:
        await message.reply("\u274c Login timeout. Proxy/Instagram may be slow or blocked.")
    except Exception as e:
        await message.reply(f"\u274c Login failed: {str(e)}")

@app.on_message(filters.text & filters.regex("^\ud83d\udce4 Upload a Reel$"))
async def upload_prompt(client, message):
    user_states[message.chat.id] = {"step": "awaiting_video"}
    await message.reply("\ud83c\udfa5 Send your reel video now.")

@app.on_message(filters.video)
async def handle_video(client, message):
    user_id = message.chat.id
    if not is_authorized(user_id):
        await message.reply("\u26d4 You are not authorized.")
        return

    state = user_states.get(user_id)
    if not state or state.get("step") != "awaiting_video":
        await message.reply("\u2757 Click \ud83d\udce4 Upload a Reel first.")
        return

    file_path = await message.download()
    user_states[user_id] = {"step": "awaiting_title", "file_path": file_path}
    await message.reply("\ud83d\udcdd Now send the title for your reel.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == "awaiting_title"))
async def handle_title(client, message):
    user_id = message.chat.id
    user_states[user_id]["title"] = message.text
    user_states[user_id]["step"] = "awaiting_hashtags"
    await message.reply("\ud83c\udff7\ufe0f Now send hashtags (e.g. #funny #reel).")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == "awaiting_hashtags"))
async def handle_hashtags(client, message):
    user_id = message.chat.id
    title = user_states[user_id].get("title", "")
    hashtags = message.text.strip()
    file_path = user_states[user_id]["file_path"]
    caption = f"{title}\n\n{hashtags}"

    try:
        safe_instagram_login()
        insta_client.clip_upload(file_path, caption)
        await message.reply("\u2705 Uploaded to Instagram!")
    except Exception as e:
        await message.reply(f"\u274c Upload failed: {e}")

    user_states.pop(user_id)

# === KEEP ALIVE ===
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_server():
    httpd = HTTPServer(('0.0.0.0', 8080), Handler)
    httpd.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

app.run()
