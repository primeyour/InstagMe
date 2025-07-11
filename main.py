import os import asyncio from pyrogram import Client, filters from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton from instagrapi import Client as InstaClient

=== ENV VARS ===

TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID", "") TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "") TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

=== INSTAGRAM ===

INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME", "") INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASSWORD", "")

=== FILES ===

AUTHORIZED_USERS_FILE = "authorized_users.txt" CAPTION_FILE = "caption.txt"

=== INIT ===

insta_client = InstaClient() insta_client.login(INSTAGRAM_USERNAME, INSTAGRAM_PASSWORD)

app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

main_menu = ReplyKeyboardMarkup( [ [KeyboardButton("\ud83d\udce4 Upload a Reel")], [KeyboardButton("\ud83d\udce4 Upload Multiple Reels")] ], resize_keyboard=True )

=== UTILS ===

def is_authorized(user_id): try: with open(AUTHORIZED_USERS_FILE, "r") as file: return str(user_id) in file.read().splitlines() except FileNotFoundError: return False

=== COMMAND: START ===

@app.on_message(filters.command("start")) async def start(client, message): user_id = message.from_user.id if not is_authorized(user_id): await message.reply(f"\u26d4 You are not authorized to use this bot.\n\n\ud83c\udd94 Your ID: {user_id}") return await message.reply("\ud83d\udc4b Welcome! Choose an option below:", reply_markup=main_menu)

=== COMMAND: LOGIN ===

@app.on_message(filters.command("login")) async def login_instagram(client, message): try: _, username, password = message.text.split(maxsplit=2) insta_client.login(username, password) await message.reply("\u2705 Instagram login successful.") except: await message.reply("\u274c Login failed. Use: /login username password")

=== COMMAND: SET CAPTION ===

@app.on_message(filters.command("setcaption")) async def set_caption(client, message): caption = message.text.replace("/setcaption", "").strip() with open(CAPTION_FILE, "w", encoding="utf-8") as f: f.write(caption) await message.reply("\u2705 Caption saved.")

=== BUTTONS ===

@app.on_message(filters.text & filters.regex("^\ud83d\udce4 Upload a Reel$")) async def upload_one_prompt(client, message): await message.reply("\ud83c\udfa5 Send your video to upload with caption.")

@app.on_message(filters.text & filters.regex("^\ud83d\udce4 Upload Multiple Reels$")) async def upload_multiple_prompt(client, message): await message.reply("\ud83c\udfa5 Send multiple videos. They’ll be uploaded every 30 seconds.")

=== VIDEO UPLOAD ===

@app.on_message(filters.video) async def video_upload(client, message): user_id = message.from_user.id if not is_authorized(user_id): await message.reply("\u26d4 You are not authorized.") return

try:
    # 1. Download Video
    path = await message.download()

    # 2. Ask for Title & Hashtags
    await message.reply("\ud83d\udcdd Please send the title for your video.")
    title_msg = await app.listen(message.chat.id, timeout=60)
    await message.reply("\ud83c\udffd Now send hashtags (e.g. #funny #reel).")
    tags_msg = await app.listen(message.chat.id, timeout=60)

    caption = f"{title_msg.text.strip()}\n\n{tags_msg.text.strip()}"

    # 3. Show Preview
    await message.reply(f"\ud83e\uddfe Preview:\n\n{caption}\n\nSend \u2705 to confirm or \u274c to cancel.")
    confirm = await app.listen(message.chat.id, timeout=30)

    if confirm.text.strip() != "\u2705":
        await message.reply("\u274c Cancelled.")
        return

    # 4. Upload
    insta_client.clip_upload(path, caption)
    await message.reply("\u2705 Uploaded to Instagram!")

    # 5. Delay if needed
    await asyncio.sleep(30)

except Exception as e:
    await message.reply(f"\u26a0\ufe0f Error: {e}")

=== RUN ===

app.run()

