import os
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton
from instagrapi import Client as InstaClient
from dotenv import load_dotenv

# === LOAD ENV ===
load_dotenv()
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID", "20836266")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "bbdd206f92e1ca4bc4935b43dfd4a2a1")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7983901811:AAGi4rscPTCS_WNND9unHi8ZaUgkMmVz1vI")
INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME", "")
INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASSWORD", "")
INSTAGRAM_PROXY = os.getenv("INSTAGRAM_PROXY", "http://user:pass@157.46.4.46:8000")
# === FILES ===
AUTHORIZED_USERS_FILE = "authorized_users.txt"
SESSION_FILE = "insta_settings.json"

# === INIT CLIENTS ===
insta_client = InstaClient()
app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

# === MAIN MENU ===
main_menu = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📤 Upload a Reel")],
        [KeyboardButton("📤 Upload Multiple Reels")]
    ],
    resize_keyboard=True
)

# === STATE ===
user_states = {}

# === UTILITY ===
def is_authorized(user_id):
    try:
        with open(AUTHORIZED_USERS_FILE, "r") as file:
            return str(user_id) in file.read().splitlines()
    except FileNotFoundError:
        return False

def safe_instagram_login():
    try:
        if INSTAGRAM_PROXY:
            insta_client.set_proxy({
                "http": INSTAGRAM_PROXY,
                "https": INSTAGRAM_PROXY
            })

        if os.path.exists(SESSION_FILE):
            insta_client.load_settings(SESSION_FILE)

        insta_client.login(INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD)
        insta_client.dump_settings(SESSION_FILE)
    except Exception as e:
        raise Exception("Instagram login failed: " + str(e))

# === COMMANDS ===
@app.on_message(filters.command("start"))
async def start(client, message):
    user_id = message.from_user.id
    if not is_authorized(user_id):
        await message.reply(f"⛔ Not authorized.\n🆔 Your ID: {user_id}")
        return
    await message.reply("👋 Welcome! Choose an option below:", reply_markup=main_menu)

@app.on_message(filters.command("login"))
async def login_instagram(client, message):
    try:
        _, username, password = message.text.split(maxsplit=2)
        if INSTAGRAM_PROXY:
            insta_client.set_proxy({
                "http": INSTAGRAM_PROXY,
                "https": INSTAGRAM_PROXY
            })
        insta_client.login(username, password)
        insta_client.dump_settings(SESSION_FILE)
        await message.reply("✅ Instagram login successful.")
    except Exception as e:
        await message.reply(f"❌ Login failed: {e}")

# === BUTTONS ===
@app.on_message(filters.text & filters.regex("^📤 Upload a Reel$"))
async def upload_prompt(client, message):
    user_states[message.chat.id] = {"step": "awaiting_video"}
    await message.reply("🎥 Send your reel video now.")

# === VIDEO HANDLER ===
@app.on_message(filters.video)
async def handle_video(client, message):
    user_id = message.chat.id
    if not is_authorized(user_id):
        await message.reply("⛔ You are not authorized.")
        return

    state = user_states.get(user_id)
    if not state or state.get("step") != "awaiting_video":
        await message.reply("❗ Click 📤 Upload a Reel first.")
        return

    file_path = await message.download()
    user_states[user_id] = {"step": "awaiting_title", "file_path": file_path}
    await message.reply("📝 Now send the title for your reel.")

# === TITLE HANDLER ===
@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == "awaiting_title"))
async def handle_title(client, message):
    user_id = message.chat.id
    user_states[user_id]["title"] = message.text
    user_states[user_id]["step"] = "awaiting_hashtags"
    await message.reply("🏷️ Now send hashtags (e.g. #funny #reel).")

# === HASHTAGS HANDLER ===
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
        await message.reply("✅ Uploaded to Instagram!")
    except Exception as e:
        await message.reply(f"❌ Upload failed: {e}")

    user_states.pop(user_id)

# === KEEP SERVER ALIVE FOR KOYEB ===
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_server():
    httpd = HTTPServer(('0.0.0.0', 8080), Handler)
    httpd.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# === RUN ===
app.run()
