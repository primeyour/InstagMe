import os
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton
from instagrapi import Client as InstaClient
from dotenv import load_dotenv

# === ENV LOADING ===
load_dotenv()
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID", "20836266")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "bbdd206f92e1ca4bc4935b43dfd4a2a1")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7983901811:AAGi4rscPTCS_WNND9unHi8ZaUgkMmVz1vI")
INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME", "")
INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASSWORD", "")

# === FILES ===
AUTHORIZED_USERS_FILE = "authorized_users.txt"
CAPTION_FILE = "caption.txt"

# === INIT CLIENTS ===
insta_client = InstaClient()  # <-- do NOT login here

app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

main_menu = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📤 Upload a Reel")],
        [KeyboardButton("📤 Upload Multiple Reels")]
    ],
    resize_keyboard=True
)

# === UTILITY ===
def is_authorized(user_id):
    try:
        with open(AUTHORIZED_USERS_FILE, "r") as file:
            return str(user_id) in file.read().splitlines()
    except FileNotFoundError:
        return False

# === COMMANDS ===
@app.on_message(filters.command("start"))
async def start(client, message):
    user_id = message.from_user.id
    if not is_authorized(user_id):
        await message.reply(f"⛔ You are not authorized to use this bot.\n\n🆔 Your ID: {user_id}")
        return
    await message.reply("👋 Welcome! Choose an option below:", reply_markup=main_menu)

@app.on_message(filters.command("login"))
async def login_instagram(client, message):
    try:
        _, username, password = message.text.split(maxsplit=2)
        insta_client.login(username, password)
        await message.reply("✅ Instagram login successful.")
    except Exception as e:
        await message.reply(f"❌ Login failed: {e}")

@app.on_message(filters.command("setcaption"))
async def set_caption(client, message):
    caption = message.text.replace("/setcaption", "").strip()
    with open(CAPTION_FILE, "w", encoding="utf-8") as f:
        f.write(caption)
    await message.reply("✅ Caption saved.")

# === BUTTONS ===
@app.on_message(filters.text & filters.regex("^📤 Upload a Reel$"))
async def upload_one_prompt(client, message):
    await message.reply("🎥 Send your video to upload with caption.")

@app.on_message(filters.text & filters.regex("^📤 Upload Multiple Reels$"))
async def upload_multiple_prompt(client, message):
    await message.reply("🎥 Send multiple videos. They’ll be uploaded every 30 seconds.")

# === VIDEO UPLOAD ===
@app.on_message(filters.video)
async def video_upload(client, message):
    user_id = message.from_user.id
    if not is_authorized(user_id):
        await message.reply("⛔ You are not authorized.")
        return

    try:
        path = await message.download()

        await message.reply("📝 Please send the title for your video.")
        title_msg = await app.listen(message.chat.id, timeout=60)

        await message.reply("🏷️ Now send hashtags (e.g. #funny #reel).")
        tags_msg = await app.listen(message.chat.id, timeout=60)

        caption = f"{title_msg.text.strip()}\n\n{tags_msg.text.strip()}"

        await message.reply(f"🧾 Preview:\n\n{caption}\n\nSend ✅ to confirm or ❌ to cancel.")
        confirm = await app.listen(message.chat.id, timeout=30)

        if confirm.text.strip() != "✅":
            await message.reply("❌ Cancelled.")
            return

        insta_client.clip_upload(path, caption)
        await message.reply("✅ Uploaded to Instagram!")

        await asyncio.sleep(30)

    except Exception as e:
        await message.reply(f"⚠️ Error: {e}")

# === FAKE WEB SERVER FOR KOYEB ===
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_server():
    httpd = HTTPServer(('0.0.0.0', 8080), Handler)
    httpd.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# === FIX TIME SYNC ISSUE ===
import ntplib
from time import time

try:
    ntp = ntplib.NTPClient()
    ntp.request('pool.ntp.org')  # Just sync to avoid Pyrogram crash
    print("[INFO] NTP time sync done.")
except Exception as e:
    print(f"[WARN] Time sync failed: {e}")

# === START BOT ===
app.run()
