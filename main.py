import os
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton
from instagrapi import Client as InstaClient

# === LOAD .env ===
load_dotenv()
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID", "20836266")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "bbdd206f92e1ca4bc4935b43dfd4a2a1")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7983901811:AAGi4rscPTCS_WNND9unHi8ZaUgkMmVz1vI")
INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME", "")
INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASSWORD", "")

# === FILES ===
AUTHORIZED_USERS_FILE = "authorized_users.txt"

# === CLIENTS ===
insta_client = InstaClient()
app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

main_menu = ReplyKeyboardMarkup([
    [KeyboardButton("📤 Upload a Reel")],
    [KeyboardButton("📤 Upload Multiple Reels")]
], resize_keyboard=True)

# === STATE MANAGEMENT ===
user_states = {}

# === AUTH ===
def is_authorized(user_id):
    try:
        with open(AUTHORIZED_USERS_FILE, "r") as f:
            return str(user_id) in f.read().splitlines()
    except:
        return False

# === COMMANDS ===
@app.on_message(filters.command("start"))
async def start_handler(client, message):
    user_id = message.from_user.id
    if not is_authorized(user_id):
        await message.reply(f"⛔ You are not authorized to use this bot.\n\n🆔 Your ID: {user_id}")
        return
    await message.reply("👋 Welcome! Choose an option below:", reply_markup=main_menu)

@app.on_message(filters.command("login"))
async def login_handler(client, message):
    user_id = message.from_user.id
    if not is_authorized(user_id):
        await message.reply("⛔ You are not authorized.")
        return
    try:
        _, username, password = message.text.strip().split(maxsplit=2)
        insta_client.login(username, password)
        await message.reply("✅ Instagram login successful.")
    except Exception as e:
        await message.reply(f"❌ Login failed: {e}")

# === REPLY BUTTONS ===
@app.on_message(filters.text & filters.regex("^📤 Upload a Reel$"))
async def upload_single_prompt(client, message):
    user_id = message.from_user.id
    if not is_authorized(user_id):
        await message.reply("⛔ You are not authorized.")
        return
    user_states[user_id] = {"step": "awaiting_video"}
    await message.reply("🎥 Send your video to upload with caption.")

# === VIDEO HANDLER ===
@app.on_message(filters.video)
async def handle_video(client, message):
    user_id = message.from_user.id
    if not is_authorized(user_id): return

    if user_states.get(user_id, {}).get("step") == "awaiting_video":
        path = await message.download()
        user_states[user_id]["step"] = "awaiting_title"
        user_states[user_id]["video_path"] = path
        await message.reply("📝 Please send the **title** for your video.")
    else:
        await message.reply("⚠️ Use the menu to start uploading.")

# === TEXT HANDLER ===
@app.on_message(filters.text & ~filters.command)
async def handle_text(client, message):
    user_id = message.from_user.id
    if not is_authorized(user_id): return

    state = user_states.get(user_id)
    if not state:
        return

    if state["step"] == "awaiting_title":
        user_states[user_id]["title"] = message.text.strip()
        user_states[user_id]["step"] = "awaiting_tags"
        await message.reply("🏷️ Now send **hashtags** (e.g. #funny #reels)")
    elif state["step"] == "awaiting_tags":
        caption = f"{state['title']}\n\n{message.text.strip()}"
        video_path = state["video_path"]
        await message.reply(f"🧾 Preview:\n\n{caption}\n\nUploading...")

        try:
            insta_client.clip_upload(video_path, caption)
            await message.reply("✅ Successfully uploaded to Instagram!")
        except Exception as e:
            await message.reply(f"⚠️ Upload failed: {e}")
        finally:
            user_states.pop(user_id, None)
            await asyncio.sleep(2)

# === KOYEB WEB SERVER ===
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_server():
    httpd = HTTPServer(('0.0.0.0', 8080), Handler)
    httpd.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# === START ===
app.run()
