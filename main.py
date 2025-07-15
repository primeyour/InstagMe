import os
import threading
import concurrent.futures
from http.server import HTTPServer, BaseHTTPRequestHandler
import logging
import json
import time
import subprocess
from datetime import datetime
import sys # For sys.exit()

from pyrogram import Client, filters, enums
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
import requests

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === LOAD ENV ===
load_dotenv()
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# === MongoDB Configuration ===
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://cristi7jjr:tRjSVaoSNQfeZ0Ik@cluster0.kowid.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
DB_NAME = "YtBot"

# === Admin and Log Channel Configuration ===
OWNER_ID = int(os.getenv("OWNER_ID", "7577977996"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "-1002779117737"))
ADMIN_TOM_USERNAME = "CjjTom"
CHANNEL_LINK = "https://t.me/KeralaCaptain"
CHANNEL_PHOTO_URL = "https://i.postimg.cc/SXDxJ92z/x.jpg"

# === Facebook API Configuration (unchanged) ===
FACEBOOK_APP_ID = os.getenv("FACEBOOK_APP_ID", "")
FACEBOOK_APP_SECRET = os.getenv("FACEBOOK_APP_SECRET", "")
FACEBOOK_PAGE_ID = os.getenv("FACEBOOK_PAGE_ID", "")
FACEBOOK_PAGE_ACCESS_TOKEN = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN", "")

# === GLOBAL CLIENTS AND DB ===
app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
users_collection = db["users"]

# --- IMPORTANT: Ensure there is NO unique index on a field named 'user_id' ---
# The primary key '_id' is already unique. If you manually created a unique index
# on a separate 'user_id' field, it will cause 'DuplicateKeyError' if that field
# is ever 'null'. Please remove any such index in MongoDB Atlas.

# === KEYBOARDS ===
main_menu_user = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📤 Upload Video (Facebook)")],
        [KeyboardButton("⚙️ Settings")]
    ],
    resize_keyboard=True
)

main_menu_admin = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📤 Upload Video (Facebook)")],
        [KeyboardButton("⚙️ Settings"), KeyboardButton("👤 Admin Panel")]
    ],
    resize_keyboard=True
)

def get_general_settings_inline_keyboard(user_id):
    keyboard = []
    if is_premium_user(user_id) or is_admin(user_id):
         keyboard.append([InlineKeyboardButton("User Settings", callback_data='settings_user_menu_inline')])
    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("Bot Status", callback_data='settings_bot_status_inline')])

    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_main_menu_reply')])
    return InlineKeyboardMarkup(keyboard)

Admin_markup = InlineKeyboardMarkup([
    [InlineKeyboardButton("👥 Users List", callback_data="admin_users_list")],
    [InlineKeyboardButton("➕ Add User", callback_data="admin_add_user_prompt")],
    [InlineKeyboardButton("➖ Remove User", callback_data="admin_remove_user_prompt")],
    [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast_prompt")],
    [InlineKeyboardButton("🔄 Restart Bot", callback_data='admin_restart_bot')],
    [InlineKeyboardButton("📤 Admin Upload Video (Facebook)", callback_data='admin_upload_fb')],
    [InlineKeyboardButton("🔙 Back to General Settings", callback_data="settings_main_menu_inline")]
])

user_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🎵 Tik Settings", callback_data='settings_tiktok')],
        [InlineKeyboardButton("📘 Fb Settings", callback_data='settings_facebook')],
        [InlineKeyboardButton("▶️ YT Settings", callback_data='settings_youtube')],
        [InlineKeyboardButton("⬅️ Back to General Settings", callback_data='settings_user_menu_inline')]
    ]
)

tiktok_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🔑 Login", callback_data='tiktok_login')],
        [InlineKeyboardButton("📝 Set Caption", callback_data='tiktok_set_caption')],
        [InlineKeyboardButton("🏷️ Set Tag", callback_data='tiktok_set_tag')],
        [InlineKeyboardButton("🎥 Video Type (Aspect Ratio)", callback_data='tiktok_video_type')],
        [InlineKeyboardButton("📄 Set Description", callback_data='tiktok_set_description')],
        [InlineKeyboardButton("ℹ️ Check Account Info", callback_data='tiktok_check_account_info')],
        [InlineKeyboardButton("⬅️ Back to User Settings", callback_data='settings_user_menu_inline')]
    ]
)

facebook_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🔑 Facebook Login", callback_data='fb_login_prompt')],
        [InlineKeyboardButton("📝 Set Title", callback_data='fb_set_title')],
        [InlineKeyboardButton("🏷️ Set Tag", callback_data='fb_set_tag')],
        [InlineKeyboardButton("📄 Set Description", callback_data='fb_set_description')],
        [InlineKeyboardButton("🎥 Video Type (Reels/Video)", callback_data='fb_video_type')],
        [InlineKeyboardButton("⏰ Set Schedule Time", callback_data='fb_set_schedule_time')],
        [InlineKeyboardButton("🔒 Set Private/Public", callback_data='fb_set_privacy')],
        [InlineKeyboardButton("🗓️ Check Expiry Date", callback_data='fb_check_expiry_date')],
        [InlineKeyboardButton("⬅️ Back to User Settings", callback_data='settings_user_menu_inline')]
    ]
)

youtube_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🔑 YouTube Login", callback_data='yt_login_prompt')],
        [InlineKeyboardButton("📝 Set Title", callback_data='yt_set_title')],
        [InlineKeyboardButton("🏷️ Set Tag", callback_data='yt_set_tag')],
        [InlineKeyboardButton("📄 Set Description", callback_data='yt_set_description')],
        [InlineKeyboardButton("🎥 Video Type (Shorts/Video)", callback_data='yt_video_type')],
        [InlineKeyboardButton("⏰ Set Schedule Time", callback_data='yt_set_schedule_time')],
        [InlineKeyboardButton("🔒 Set Private/Public", callback_data='yt_set_privacy')],
        [InlineKeyboardButton("🗓️ Check Expiry Date", callback_data='yt_check_expiry_date')],
        [InlineKeyboardButton("⬅️ Back to User Settings", callback_data='settings_user_menu_inline')]
    ]
)

tiktok_video_type_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("1:1 Aspect Ratio", callback_data='tiktok_aspect_ratio_1_1')],
        [InlineKeyboardButton("9:16 Aspect Ratio", callback_data='tiktok_aspect_ratio_9_16')],
        [InlineKeyboardButton("⬅️ Back to Tik Settings", callback_data='settings_tiktok')]
    ]
)

facebook_video_type_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("Reels", callback_data='fb_video_type_reels')],
        [InlineKeyboardButton("Video", callback_data='fb_video_type_video')],
        [InlineKeyboardButton("⬅️ Back to Fb Settings", callback_data='settings_facebook')]
    ]
)

youtube_video_type_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("Shorts", callback_data='yt_video_type_shorts')],
        [InlineKeyboardButton("Video", callback_data='yt_video_type_video')],
        [InlineKeyboardButton("⬅️ Back to YT Settings", callback_data='settings_youtube')]
    ]
)

def get_privacy_inline_menu(platform):
    keyboard = [
        [InlineKeyboardButton("Public", callback_data=f'{platform}_privacy_public')],
        [InlineKeyboardButton("Private", callback_data=f'{platform}_privacy_private')],
    ]
    if platform == 'yt':
        keyboard.append([InlineKeyboardButton("Unlisted", callback_data='yt_privacy_unlisted')])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f'settings_{platform}')])
    return InlineKeyboardMarkup(keyboard)


# === USER STATES ===
user_states = {}

# === CONVERSATION STATES (for text input) ===
AWAITING_TIKTOK_CAPTION = "awaiting_tiktok_caption"
AWAITING_TIKTOK_TAG = "awaiting_tiktok_tag"
AWAITING_TIKTOK_DESCRIPTION = "awaiting_tiktok_description"

AWAITING_FB_TITLE = "awaiting_fb_title"
AWAITING_FB_TAG = "awaiting_fb_tag"
AWAITING_FB_DESCRIPTION = "awaiting_fb_description"
AWAITING_FB_SCHEDULE_TIME = "awaiting_fb_schedule_time"
AWAITING_FB_ACCESS_TOKEN = "awaiting_fb_access_token"

AWAITING_YT_TITLE = "awaiting_yt_title"
AWAITING_YT_TAG = "awaiting_yt_tag"
AWAITING_YT_DESCRIPTION = "awaiting_yt_description"
AWAITING_YT_SCHEDULE_TIME = "awaiting_yt_schedule_time"
AWAITING_YT_ACCESS_TOKEN = "awaiting_yt_access_token"

AWAITING_TIKTOK_ACCESS_TOKEN = "awaiting_tiktok_access_token"

# === HELPERS ===
def get_user_data(user_id):
    """Retrieves user data from MongoDB using _id."""
    return users_collection.find_one({"_id": user_id})

def update_user_data(user_id, data):
    """Updates user data in MongoDB using _id for upsert."""
    users_collection.update_one({"_id": user_id}, {"$set": data}, upsert=True)

def is_admin(user_id):
    """Checks if a user is an admin."""
    user_doc = get_user_data(user_id)
    return user_doc and user_doc.get("role") == "admin"

def is_premium_user(user_id):
    """Checks if a user is a premium user (checks 'is_premium' boolean)."""
    user_doc = get_user_data(user_id)
    return user_doc and user_doc.get("is_premium", False)

async def log_to_channel(client, message_text):
    """Sends a message to the designated log channel."""
    try:
        await client.send_message(LOG_CHANNEL_ID, f"**Bot Log:**\n\n{message_text}")
    except Exception as e:
        logger.error(f"Failed to send message to log channel (ID: {LOG_CHANNEL_ID}): {e}")

def get_facebook_access_token_for_user(user_id):
    """Retrieves Facebook access token from user data."""
    user_doc = get_user_data(user_id)
    return user_doc.get("facebook_access_token")

def store_facebook_access_token_for_user(user_id, token):
    """Stores Facebook access token in user data."""
    update_user_data(user_id, {"facebook_access_token": token})

def upload_facebook_video(file_path, title, description, access_token, page_id, visibility="PUBLISHED", schedule_time=None):
    """Uploads a video to Facebook Page using Graph API."""
    if not all([file_path, title, description, access_token, page_id]):
        raise ValueError("Missing required parameters for Facebook video upload.")

    post_url = f"https://graph-video.facebook.com/v19.0/{page_id}/videos"

    params = {
        'access_token': access_token,
        'title': title,
        'description': description,
    }

    if schedule_time:
        params['published'] = 'false'
        params['scheduled_publish_time'] = int(schedule_time.timestamp())
        params['status_type'] = 'SCHEDULED_PUBLISH'
        logger.info(f"Scheduling Facebook video for: {schedule_time}")
    else:
        params['published'] = 'true'
        if visibility == 'private' or visibility == 'draft':
            params['status_type'] = 'DRAFT'
            logger.info(f"Uploading Facebook video as DRAFT.")
        else: # 'public'
            params['status_type'] = 'PUBLISHED'
            logger.info(f"Uploading Facebook video as PUBLISHED.")

    with open(file_path, 'rb') as f:
        files = {'file': f}
        response = requests.post(post_url, params=params, files=files)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Facebook video upload result: {result}")
        return result

def convert_video_to_mp4(input_path, output_path):
    """
    Converts video to MP4 format, copying video and audio streams.
    Ensures output is MP4.
    """
    command = ["ffmpeg", "-i", input_path, "-c:v", "copy", "-c:a", "copy", "-map", "0", "-y", output_path]
    logger.info(f"FFmpeg: Converting video to MP4 for {input_path}")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"FFmpeg command successful for {input_path}. Output: {result.stdout}")
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg conversion failed for {input_path}. Command: {' '.join(e.cmd)}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        raise RuntimeError(f"FFmpeg conversion error: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("FFmpeg not found. Please install FFmpeg and ensure it's in your system's PATH.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during FFmpeg conversion: {e}")
        raise

# === PYROGRAM HANDLERS ===

@app.on_message(filters.command("start"))
async def start_command(client, message):
    user_id = message.from_user.id
    user_first_name = message.from_user.first_name or "there"
    user_username = message.from_user.username or "N/A"

    # Define base user data for new users or for updates
    # We are using '_id' as the unique identifier, which is the Telegram user_id.
    # We do not need a separate 'user_id' field unless there's a specific reason.
    # If a 'user_id' field was previously created with a unique index,
    # it must be removed from MongoDB as instructed above.
    user_data_to_set = {
        "first_name": user_first_name,
        "username": user_username,
        "last_active": datetime.now(),
        # Default settings if user is new or fields are missing
        "is_premium": False,
        "role": "user",
        "premium_platforms": [],
        "total_uploads": 0,
        "tiktok_settings": {
            "logged_in": False, "caption": "", "tag": "", "video_type": "", "description": ""
        },
        "facebook_settings": {
            "title": "", "tag": "", "description": "", "video_type": "", "schedule_time": None, "privacy": "", "expiry_date": ""
        },
        "youtube_settings": {
            "title": "", "tag": "", "description": "", "video_type": "", "schedule_time": None, "privacy": "", "expiry_date": ""
        }
    }

    # If owner, set role to admin and is_premium to True
    if user_id == OWNER_ID:
        user_data_to_set["role"] = "admin"
        user_data_to_set["is_premium"] = True

    # Use update_one with upsert=True to handle both insertion and update
    # This ensures that even if the document exists, it's updated, and if not, it's created.
    try:
        users_collection.update_one(
            {"_id": user_id},
            {"$set": user_data_to_set, "$setOnInsert": {"added_at": datetime.now(), "added_by": "self_start"}},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error during user data update/upsert for user {user_id}: {e}")
        await message.reply("An error occurred while initializing your account. Please try again later or contact support.")
        return # Prevent further execution if DB operation failed

    # Fetch the updated user document to ensure it reflects current state
    user_doc = get_user_data(user_id)
    if not user_doc: # This should ideally not happen after upsert, but as a safeguard
        logger.error(f"Could not retrieve user document for {user_id} after upsert.")
        await message.reply("Failed to retrieve your account data. Please try /start again.")
        return

    await log_to_channel(client, f"User `{user_id}` (`{user_username}` - `{user_first_name}`) performed /start. Role: `{user_doc.get('role')}`, Premium: `{user_doc.get('is_premium')}`.")

    if is_admin(user_id):
        welcome_msg = (
            f"🤖 **Welcome to Instagram Upload Bot, Admin {user_first_name}!**\n\n"
            "🛠 You have **full admin privileges**."
        )
        reply_markup = main_menu_admin
        await message.reply(welcome_msg, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)

    elif is_premium_user(user_id):
        welcome_msg = (
            f"🤖 **Welcome to Instagram Upload Bot, Premium User {user_first_name}!**\n\n"
            "⭐ You have **premium access** to all features.\n"
            "Ready to upload your Instagram Reels & Posts directly from Telegram."
        )
        reply_markup = main_menu_user
        await message.reply(welcome_msg, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)

    else: # Non-premium, non-admin user
        contact_admin_text = (
            f"👋 **Hi {user_first_name}!**\n\n"
            "**This Bot Lets You Upload Any Size Instagram Reels & Posts Directly From Telegram**.\n\n"
            "• **Unlock Full Premium Features for:**\n"
            "  • **YouTube (Shorts & Videos)**\n"
            "  • **Facebook (Reels & Posts)**\n"
            "  • **TikTok (Videos)**\n\n"
            "• **Enjoy Unlimited Video Uploads**\n"
            "• **Automatic Captions & Hashtags (Configurable)**\n"
            "• **Reel, Post, or Short Type Selection**\n\n"
            f"👤 Contact **[ADMIN TOM](https://t.me/{ADMIN_TOM_USERNAME})** **To Upgrade Your Access**.\n"
            "🔐 **Your Data Is Fully ✅Encrypted**\n\n"
            f"🆔 Your User ID: `{user_id}`"
        )

        join_channel_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅Join Our Channel✅", url=CHANNEL_LINK)]
        ])

        await client.send_photo(
            chat_id=message.chat.id,
            photo=CHANNEL_PHOTO_URL,
            caption=contact_admin_text,
            reply_markup=join_channel_markup,
            parse_mode=enums.ParseMode.MARKDOWN
        )

# --- Admin Commands ---
@app.on_message(filters.command("addadmin") & filters.user(OWNER_ID))
async def add_admin_command(client, message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2 or not args[1].isdigit():
            await message.reply("❗ Usage: `/addadmin <user_id>`")
            return

        target_user_id = int(args[1])
        # Ensure new user data is correctly set if they don't exist
        update_user_data(target_user_id, {"role": "admin", "is_premium": True}) # Admins are also premium
        await message.reply(f"✅ User `{target_user_id}` has been promoted to admin and premium.")
        try:
            await client.send_message(target_user_id, "🎉 You have been promoted to an admin! Use /start to see your new options.")
        except Exception:
            logger.warning(f"Could not notify user {target_user_id} about admin promotion.")
        await log_to_channel(client, f"User `{target_user_id}` promoted to admin by `{message.from_user.id}`.")

    except Exception as e:
        await message.reply(f"❌ Failed to add admin: {e}")
        logger.error(f"Failed to add admin: {e}")

@app.on_message(filters.command("removeadmin") & filters.user(OWNER_ID))
async def remove_admin_command(client, message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2 or not args[1].isdigit():
            await message.reply("❗ Usage: `/removeadmin <user_id>`")
            return

        target_user_id = int(args[1])
        user_doc = get_user_data(target_user_id)

        if target_user_id == OWNER_ID:
            await message.reply("❌ You cannot remove the owner's admin status.")
            return

        if user_doc and user_doc.get("role") == "admin":
            update_user_data(target_user_id, {"role": "user", "is_premium": False, "premium_platforms": []}) # Demote and remove premium
            await message.reply(f"✅ User `{target_user_id}` has been demoted to a regular user and removed from premium.")
            try:
                await client.send_message(target_user_id, "You have been demoted from admin status and your premium access has been revoked.")
            except Exception:
                logger.warning(f"Could not notify user {target_user_id} about admin demotion.")
            await log_to_channel(client, f"User `{target_user_id}` demoted from admin by `{message.from_user.id}`.")
        else:
            await message.reply(f"User `{target_user_id}` is not an admin or not found.")

    except Exception as e:
        await message.reply(f"❌ Failed to remove admin: {e}")
        logger.error(f"Failed to remove admin: {e}")

# --- Settings Menu Handlers (Reply Keyboard & Inline) ---

@app.on_message(filters.text & filters.regex("^⚙️ Settings$"))
async def show_main_settings_menu_reply(client, message):
    user_id = message.from_user.id
    # Ensure user data exists before trying to get settings keyboard
    user_doc = get_user_data(user_id)
    if not user_doc:
        # This shouldn't happen often with the /start update, but as a safeguard
        await message.reply("Please send /start first to initialize your account.")
        return
    await message.reply("⚙️ Choose your settings options:", reply_markup=get_general_settings_inline_keyboard(user_id))

@app.on_message(filters.text & filters.regex("^🔙 Main Menu$"))
async def back_to_main_menu_reply(client, message):
    user_id = message.from_user.id
    user_states.pop(user_id, None)
    if is_admin(user_id):
        await message.reply("Returning to Main Menu.", reply_markup=main_menu_admin)
    else:
        await message.reply("Returning to Main Menu.", reply_markup=main_menu_user)

# --- General Settings Inline Callbacks ---
@app.on_callback_query(filters.regex("^settings_main_menu_inline$"))
async def settings_main_menu_inline_callback(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    await callback_query.edit_message_text(
        "⚙️ Choose your settings options:",
        reply_markup=get_general_settings_inline_keyboard(user_id)
    )

@app.on_callback_query(filters.regex("^back_to_main_menu_reply$"))
async def back_to_main_menu_from_inline(client, callback_query):
    user_id = callback_query.from_user.id
    user_states.pop(user_id, None)
    await callback_query.answer("Returning to Main Menu.")
    if is_admin(user_id):
        await client.send_message(user_id, "Returning to Main Menu.", reply_markup=main_menu_admin)
    else:
        await client.send_message(user_id, "Returning to Main Menu.", reply_markup=main_menu_user)
    try:
        await callback_query.message.delete()
    except Exception as e:
        logger.warning(f"Could not delete inline message: {e}")

@app.on_callback_query(filters.regex("^settings_user_menu_inline$"))
async def settings_user_menu_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id):
        await callback_query.answer("You need a premium subscription to access user settings.", show_alert=True)
        return
    await callback_query.answer()
    await callback_query.edit_message_text(
        "Choose a platform to configure:",
        reply_markup=user_settings_inline_menu
    )

# --- Admin Panel Reply Button Handler ---
@app.on_message(filters.text & filters.regex("^👤 Admin Panel$") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_panel_menu_reply(client, message):
    user_id = message.from_user.id
    await message.reply("👋 Welcome to the Admin Panel!", reply_markup=Admin_markup)

# --- Admin Inline Callbacks (from Admin_markup) ---

@app.on_callback_query(filters.regex("^admin_users_list$"))
async def admin_users_list_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    await callback_query.answer("Fetching users list...")

    all_users = list(users_collection.find({}, {"_id": 1, "first_name": 1, "username": 1, "role": 1, "is_premium": 1}))
    user_list_text = "**👥 All Users:**\n\n"
    if not all_users:
        user_list_text += "No users found in the database."
    else:
        for user in all_users:
            role = user.get("role", "user").capitalize()
            premium_status = "⭐ Premium" if user.get("is_premium") else ""
            user_list_text += (
                f"ID: `{user['_id']}`\n"
                f"Name: `{user.get('first_name', 'N/A')}`\n"
                f"Username: `@{user.get('username', 'N/A')}`\n"
                f"Role: `{role}` {premium_status}\n\n"
            )

    await callback_query.edit_message_text(user_list_text, reply_markup=Admin_markup)
    await log_to_channel(client, f"Admin `{user_id}` viewed users list.")

@app.on_callback_query(filters.regex("^admin_add_user_prompt$"))
async def admin_add_user_prompt_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    await callback_query.answer()
    user_states[user_id] = {"step": "admin_awaiting_user_id_to_add"}
    await callback_query.edit_message_text(
        "Please send the Telegram User ID of the user you want to add as premium.\n"
        "Simply enter the ID.",
        reply_markup=Admin_markup
    )

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == "admin_awaiting_user_id_to_add") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_add_user_id_input(client, message):
    user_id = message.from_user.id
    target_user_id_str = message.text.strip()
    user_states.pop(user_id, None)

    try:
        target_user_id = int(target_user_id_str)
        # Always update with upsert=True
        update_user_data(target_user_id, {"is_premium": True})
        await message.reply(f"✅ User `{target_user_id}` has been marked as premium.", reply_markup=Admin_markup)
        try:
            await client.send_message(target_user_id, "🎉 Congratulations! Your account has been upgraded to premium! Use /start to see your new options.")
        except Exception:
            logger.warning(f"Could not notify user {target_user_id} about premium upgrade.")
        await log_to_channel(client, f"Admin `{user_id}` upgraded user `{target_user_id}` to premium.")

    except ValueError:
        await message.reply("❌ Invalid User ID. Please send a numeric ID.", reply_markup=Admin_markup)
    except Exception as e:
        await message.reply(f"❌ Failed to add user: {e}", reply_markup=Admin_markup)
        logger.error(f"Failed to add user for admin {user_id}: {e}")

@app.on_callback_query(filters.regex("^admin_remove_user_prompt$"))
async def admin_remove_user_prompt_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    await callback_query.answer()
    user_states[user_id] = {"step": "admin_awaiting_user_id_to_remove"}
    await callback_query.edit_message_text(
        "Please send the Telegram User ID of the user you want to remove from premium access.\n"
        "Simply enter the ID.",
        reply_markup=Admin_markup
    )

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == "admin_awaiting_user_id_to_remove") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_remove_user_id_input(client, message):
    user_id = message.from_user.id
    target_user_id_str = message.text.strip()
    user_states.pop(user_id, None)

    try:
        target_user_id = int(target_user_id_str)
        if target_user_id == OWNER_ID:
            await message.reply("❌ Cannot remove owner's premium status.", reply_markup=Admin_markup)
            return

        user_doc = get_user_data(target_user_id)

        if user_doc and user_doc.get("is_premium"):
            update_user_data(target_user_id, {"is_premium": False, "premium_platforms": []})
            await message.reply(f"✅ User `{target_user_id}` has been removed from premium access.", reply_markup=Admin_markup)
            try:
                await client.send_message(target_user_id, "❗ Your premium access has been revoked.")
            except Exception:
                logger.warning(f"Could not notify user {target_user_id} about premium revocation.")
            await log_to_channel(client, f"Admin `{user_id}` revoked premium for user `{target_user_id}`.")
        else:
            await message.reply(f"User `{target_user_id}` is not a premium user or not found.", reply_markup=Admin_markup)

    except ValueError:
        await message.reply("❌ Invalid User ID. Please send a numeric ID.", reply_markup=Admin_markup)
    except Exception as e:
        await message.reply(f"❌ Failed to remove user: {e}", reply_markup=Admin_markup)
        logger.error(f"Failed to remove user for admin {user_id}: {e}")

AWAITING_BROADCAST_MESSAGE = "awaiting_broadcast_message"

@app.on_callback_query(filters.regex("^admin_broadcast_prompt$"))
async def admin_broadcast_prompt_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_BROADCAST_MESSAGE}
    await callback_query.edit_message_text("Please send the message you want to broadcast to all users.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cancel Broadcast", callback_data="cancel_broadcast")]]))

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_BROADCAST_MESSAGE) & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def broadcast_message_handler(client, message):
    user_id = message.from_user.id
    text_to_broadcast = message.text
    user_states.pop(user_id, None)

    await message.reply("Starting broadcast...")
    await log_to_channel(client, f"Broadcast initiated by `{user_id}` with message: '{text_to_broadcast[:50]}...'")

    all_user_ids = [user["_id"] for user in users_collection.find({}, {"_id": 1})]
    success_count = 0
    fail_count = 0

    for target_user_id in all_user_ids:
        try:
            if target_user_id == user_id:
                continue
            await client.send_message(target_user_id, text_to_broadcast)
            success_count += 1
            time.sleep(0.1)
        except Exception as e:
            fail_count += 1
            logger.warning(f"Failed to send broadcast to user {target_user_id}: {e}")

    await message.reply(f"✅ Broadcast finished. Sent to {success_count} users, failed for {fail_count} users.", reply_markup=Admin_markup)
    await log_to_channel(client, f"Broadcast finished by `{user_id}`. Sent: {success_count}, Failed: {fail_count}.")

@app.on_callback_query(filters.regex("^cancel_broadcast$"))
async def cancel_broadcast_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if user_states.get(user_id, {}).get("step") == AWAITING_BROADCAST_MESSAGE:
        user_states.pop(user_id, None)
        await callback_query.answer("Broadcast cancelled.")
        await callback_query.edit_message_text("Broadcast cancelled.", reply_markup=Admin_markup)
        await log_to_channel(client, f"Admin `{user_id}` cancelled broadcast.")
    else:
        await callback_query.answer("No active broadcast to cancel.", show_alert=True)

@app.on_callback_query(filters.regex("^admin_restart_bot$"))
async def admin_restart_bot_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    await callback_query.answer("Bot is restarting...", show_alert=True)
    await callback_query.message.edit_text("🔄 Bot is restarting now. This may take a moment. Please send /start in a few seconds.", reply_markup=None)
    await log_to_channel(client, f"Admin `{user_id}` initiated bot restart.")
    sys.exit(0)

@app.on_callback_query(filters.regex("^admin_upload_fb$"))
async def admin_upload_fb_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("Unauthorized.", show_alert=True)
        return
    await callback_query.answer()
    await callback_query.message.edit_text(
        "Initiating Facebook video upload for admin. Please send the video file directly now.",
        reply_markup=None
    )
    await client.send_message(user_id, "You can use '🔙 Main Menu' to cancel the upload.", reply_markup=main_menu_admin)
    user_states[user_id] = {"step": "awaiting_video_facebook", "platform": "facebook"}

@app.on_callback_query(filters.regex("^settings_bot_status_inline$"))
async def settings_bot_status_inline_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("You are not authorized to access bot status.", show_alert=True)
        return

    await callback_query.answer()
    total_users = users_collection.count_documents({})
    admin_users = users_collection.count_documents({"role": "admin"})
    premium_users = users_collection.count_documents({"is_premium": True})

    total_fb_accounts = users_collection.count_documents({"facebook_access_token": {"$ne": None}})
    total_tiktok_accounts = users_collection.count_documents({"tiktok_settings.logged_in": True})
    total_youtube_accounts = users_collection.count_documents({"youtube_logged_in": True})

    total_uploads_count = sum(user.get("total_uploads", 0) for user in users_collection.find({}, {"total_uploads": 1}))

    stats_message = (
        f"**📊 Bot Status:**\n\n"
        f"**Users:**\n"
        f"Total Registered Users: `{total_users}`\n"
        f"Admins: `{admin_users}`\n"
        f"Premium Users: `{premium_users}`\n\n"
        f"**Connected Accounts:**\n"
        f"Facebook Accounts Logged In: `{total_fb_accounts}`\n"
        f"TikTok Accounts Logged In: `{total_tiktok_accounts}`\n"
        f"YouTube Accounts Logged In: `{total_youtube_accounts}`\n\n"
        f"**Activity:**\n"
        f"Total Uploads (across all users): `{total_uploads_count}`\n\n"
        f"_Note: 'People left' metric is not tracked directly by the bot._"
    )
    await callback_query.edit_message_text(stats_message, reply_markup=get_general_settings_inline_keyboard(user_id))
    await log_to_channel(client, f"Admin `{user_id}` viewed detailed bot status.")

# --- Platform Specific Settings Menus ---
@app.on_callback_query(filters.regex("^settings_tiktok$"))
async def show_tiktok_settings(client, callback_query):
    await callback_query.answer()
    await callback_query.edit_message_text("🎵 TikTok Settings:", reply_markup=tiktok_settings_inline_menu)

@app.on_callback_query(filters.regex("^settings_facebook$"))
async def show_facebook_settings(client, callback_query):
    await callback_query.answer()
    await callback_query.edit_message_text("📘 Facebook Settings:", reply_markup=facebook_settings_inline_menu)

@app.on_callback_query(filters.regex("^settings_youtube$"))
async def show_youtube_settings(client, callback_query):
    await callback_query.answer()
    await callback_query.edit_message_text("▶️ YouTube Settings:", reply_markup=youtube_settings_inline_menu)

# --- TikTok Settings Handlers ---
@app.on_callback_query(filters.regex("^tiktok_login$"))
async def tiktok_login_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id): # Ensure user is premium before allowing platform login setup
        await callback_query.answer("You need a premium subscription to connect platforms.", show_alert=True)
        return
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_TIKTOK_ACCESS_TOKEN}
    await callback_query.edit_message_text(
        "To log in to TikTok, you'll need to provide your **TikTok Access Token**.\n"
        "Please send it now: `/tiktoklogin <your_tiktok_access_token>`\n\n"
        "_Note: This is a placeholder for a real TikTok OAuth flow._"
    )

@app.on_message(filters.command("tiktoklogin") & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_TIKTOK_ACCESS_TOKEN))
async def tiktok_login_command(client, message):
    user_id = message.from_user.id
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.reply("❗ Usage: `/tiktoklogin <your_tiktok_access_token>`")
            return

        access_token = args[1].strip()
        if access_token:
            user_doc = get_user_data(user_id)
            premium_platforms = user_doc.get("premium_platforms", [])
            if "tiktok" not in premium_platforms:
                premium_platforms.append("tiktok")

            update_user_data(user_id, {
                "tiktok_settings.logged_in": True,
                "tiktok_access_token": access_token,
                "premium_platforms": premium_platforms,
                "is_premium": True # Ensure is_premium is true when they connect a platform
            })
            await message.reply("✅ TikTok login successful! (Token saved - simulation).", reply_markup=tiktok_settings_inline_menu)
            await log_to_channel(client, f"User `{user_id}` successfully 'logged into' TikTok and set as premium.")
        else:
            await message.reply("❌ TikTok login failed. Invalid token provided.", reply_markup=tiktok_settings_inline_menu)
            logger.error(f"TikTok token validation failed for user {user_id}: Empty token")
    except Exception as e:
        await message.reply(f"❌ Failed to process TikTok login: {e}", reply_markup=tiktok_settings_inline_menu)
        logger.error(f"Failed to process TikTok login for user {user_id}: {e}")
    finally:
        user_states.pop(user_id, None)

@app.on_callback_query(filters.regex("^tiktok_set_caption$"))
async def tiktok_set_caption_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_TIKTOK_CAPTION}
    await callback_query.edit_message_text("Please send the new TikTok caption:")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_TIKTOK_CAPTION))
async def tiktok_set_caption_save(client, message):
    user_id = message.from_user.id
    caption = message.text
    update_user_data(user_id, {"tiktok_settings.caption": caption})
    user_states.pop(user_id, None)
    await message.reply(f"✅ TikTok caption set to: '{caption}'", reply_markup=tiktok_settings_inline_menu)
    await log_to_channel(client, f"User `{user_id}` set TikTok caption.")

@app.on_callback_query(filters.regex("^tiktok_set_tag$"))
async def tiktok_set_tag_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_TIKTOK_TAG}
    await callback_query.edit_message_text("Please send the new TikTok tag (e.g., #myvideo #foryou):")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_TIKTOK_TAG))
async def tiktok_set_tag_save(client, message):
    user_id = message.from_user.id
    tag = message.text
    update_user_data(user_id, {"tiktok_settings.tag": tag})
    user_states.pop(user_id, None)
    await message.reply(f"✅ TikTok tag set to: '{tag}'", reply_markup=tiktok_settings_inline_menu)
    await log_to_channel(client, f"User `{user_id}` set TikTok tag.")

@app.on_callback_query(filters.regex("^tiktok_video_type$"))
async def tiktok_video_type_selection(client, callback_query):
    await callback_query.answer()
    await callback_query.edit_message_text("Select TikTok video type/aspect ratio:", reply_markup=tiktok_video_type_inline_menu)

@app.on_callback_query(filters.regex("^tiktok_aspect_ratio_"))
async def tiktok_set_aspect_ratio(client, callback_query):
    user_id = callback_query.from_user.id
    aspect_ratio = "1:1 Aspect Ratio" if '1_1' in callback_query.data else "9:16 Aspect Ratio"
    update_user_data(user_id, {"tiktok_settings.video_type": aspect_ratio})
    await callback_query.answer(f"TikTok aspect ratio set to: {aspect_ratio}", show_alert=True)
    await callback_query.edit_message_text(
        f"✅ TikTok video type set to: {aspect_ratio}",
        reply_markup=tiktok_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set TikTok aspect ratio to `{aspect_ratio}`.")

@app.on_callback_query(filters.regex("^tiktok_set_description$"))
async def tiktok_set_description_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_TIKTOK_DESCRIPTION}
    await callback_query.edit_message_text("Please send the new TikTok description:")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_TIKTOK_DESCRIPTION))
async def tiktok_set_description_save(client, message):
    user_id = message.from_user.id
    description = message.text
    update_user_data(user_id, {"tiktok_settings.description": description})
    user_states.pop(user_id, None)
    await message.reply(f"✅ TikTok description set to: '{description}'", reply_markup=tiktok_settings_inline_menu)
    await log_to_channel(client, f"User `{user_id}` set TikTok description.")

@app.on_callback_query(filters.regex("^tiktok_check_account_info$"))
async def tiktok_check_account_info(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_doc = get_user_data(user_id)
    tiktok_settings = user_doc.get("tiktok_settings", {})

    logged_in_status = "Logged In" if tiktok_settings.get("logged_in") else "Logged Out"
    caption = tiktok_settings.get("caption", "Not set")
    tag = tiktok_settings.get("tag", "Not set")
    video_type = tiktok_settings.get("video_type", "Not set")
    description = tiktok_settings.get("description", "Not set")

    info_text = (
        f"**🎵 TikTok Account Info:**\n"
        f"Status: {logged_in_status}\n"
        f"Caption: `{caption}`\n"
        f"Tag: `{tag}`\n"
        f"Video Type: `{video_type}`\n"
        f"Description: `{description}`"
    )
    await callback_query.edit_message_text(info_text, reply_markup=tiktok_settings_inline_menu)

# --- Facebook Settings Handlers ---
@app.on_callback_query(filters.regex("^fb_login_prompt$"))
async def prompt_facebook_login_from_settings(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id): # Ensure user is premium before allowing platform login setup
        await callback_query.answer("You need a premium subscription to connect platforms.", show_alert=True)
        return
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_FB_ACCESS_TOKEN}
    await callback_query.edit_message_text(
        "To log in to Facebook, you'll need to provide an **Access Token**.\n"
        "This token should ideally be a **Page Access Token** if you plan to upload to a Facebook Page, as user access tokens are short-lived.\n\n"
        "❗ **How to get a Page Access Token:**\n"
        "1. Go to Facebook Developers: `https://developers.facebook.com/`\n"
        "2. Create an App (if you don't have one).\n"
        "3. Get a User Access Token with `pages_show_list` and `pages_manage_posts` permissions.\n"
        "4. Use that User Access Token to get a Long-Lived Page Access Token for your specific Page.\n\n"
        "Once you have the token, send it using:\n"
        "```\n/fblogin <your_facebook_page_access_token>\n```\n"
        f"_Note: The bot uses a configured Page ID (`{FACEBOOK_PAGE_ID}`). Ensure your token is for that page._"
    )

@app.on_message(filters.command("fblogin") & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_ACCESS_TOKEN))
async def facebook_login_command(client, message):
    user_id = message.from_user.id
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.reply("❗ Usage: `/fblogin <your_facebook_page_access_token>`")
            return

        access_token = args[1].strip()

        test_url = f"https://graph.facebook.com/v19.0/me?access_token={access_token}"
        response = requests.get(test_url)
        response_data = response.json()

        if response.status_code == 200 and 'id' in response_data:
            user_doc = get_user_data(user_id)
            premium_platforms = user_doc.get("premium_platforms", [])
            if "facebook" not in premium_platforms:
                premium_platforms.append("facebook")

            update_user_data(user_id, {
                "facebook_access_token": access_token,
                "premium_platforms": premium_platforms,
                "is_premium": True # Ensure is_premium is true when they connect a platform
            })
            await message.reply("✅ Facebook login successful! Access token saved.", reply_markup=facebook_settings_inline_menu)
            await log_to_channel(client, f"User `{user_id}` successfully logged into Facebook and set as premium.")
        else:
            error_message = response_data.get('error', {}).get('message', 'Unknown error')
            await message.reply(f"❌ Facebook login failed. Invalid or expired token. Error: `{error_message}`", reply_markup=facebook_settings_inline_menu)
            logger.error(f"Facebook token validation failed for user {user_id}: {response_data}")

    except Exception as e:
        await message.reply(f"❌ Failed to process Facebook login: {e}", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Failed to process Facebook login for user {user_id}: {e}")
    finally:
        user_states.pop(user_id, None)

@app.on_callback_query(filters.regex("^fb_set_title$"))
async def fb_set_title_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_FB_TITLE}
    await callback_query.edit_message_text("Please send the new Facebook video title:")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_TITLE))
async def fb_set_title_save(client, message):
    user_id = message.from_user.id
    title = message.text
    update_user_data(user_id, {"facebook_settings.title": title})
    user_states.pop(user_id, None)
    await message.reply(f"✅ Facebook title set to: '{title}'", reply_markup=facebook_settings_inline_menu)
    await log_to_channel(client, f"User `{user_id}` set Facebook title.")

@app.on_callback_query(filters.regex("^fb_set_tag$"))
async def fb_set_tag_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_FB_TAG}
    await callback_query.edit_message_text("Please send the new Facebook tag (e.g., #reels #video):")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_TAG))
async def fb_set_tag_save(client, message):
    user_id = message.from_user.id
    tag = message.text
    update_user_data(user_id, {"facebook_settings.tag": tag})
    user_states.pop(user_id, None)
    await message.reply(f"✅ Facebook tag set to: '{tag}'", reply_markup=facebook_settings_inline_menu)
    await log_to_channel(client, f"User `{user_id}` set Facebook tag.")

@app.on_callback_query(filters.regex("^fb_set_description$"))
async def fb_set_description_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_FB_DESCRIPTION}
    await callback_query.edit_message_text("Please send the new Facebook description:")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_DESCRIPTION))
async def fb_set_description_save(client, message):
    user_id = message.from_user.id
    description = message.text
    update_user_data(user_id, {"facebook_settings.description": description})
    user_states.pop(user_id, None)
    await message.reply(f"✅ Facebook description set to: '{description}'", reply_markup=facebook_settings_inline_menu)
    await log_to_channel(client, f"User `{user_id}` set Facebook description.")

@app.on_callback_query(filters.regex("^fb_video_type$"))
async def fb_video_type_selection(client, callback_query):
    await callback_query.answer()
    await callback_query.edit_message_text("Select Facebook video type:", reply_markup=facebook_video_type_inline_menu)

@app.on_callback_query(filters.regex("^fb_video_type_"))
async def fb_set_video_type(client, callback_query):
    user_id = callback_query.from_user.id
    video_type = "Reels" if 'reels' in callback_query.data else "Video"
    update_user_data(user_id, {"facebook_settings.video_type": video_type})
    await callback_query.answer(f"Facebook video type set to: {video_type}", show_alert=True)
    await callback_query.edit_message_text(
        f"✅ Facebook video type set to: {video_type}",
        reply_markup=facebook_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set Facebook video type to `{video_type}`.")

@app.on_callback_query(filters.regex("^fb_set_schedule_time$"))
async def fb_set_schedule_time_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_FB_SCHEDULE_TIME}
    await callback_query.edit_message_text(
        "Please send the schedule date and time in `YYYY-MM-DD HH:MM` format (e.g., `2025-07-20 14:30`).\n"
        "_Time will be interpreted in UTC._"
    )

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_SCHEDULE_TIME))
async def fb_set_schedule_time_save(client, message):
    user_id = message.from_user.id
    schedule_str = message.text.strip()
    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")
        if schedule_dt <= datetime.utcnow():
            await message.reply("❌ Schedule time must be in the future. Please try again.")
            return

        update_user_data(user_id, {"facebook_settings.schedule_time": schedule_dt.isoformat()})
        user_states.pop(user_id, None)
        await message.reply(f"✅ Facebook schedule time set to: '{schedule_str}'", reply_markup=facebook_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set Facebook schedule time to `{schedule_str}`.")
    except ValueError:
        await message.reply("❌ Invalid date/time format. Please use `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`).")

@app.on_callback_query(filters.regex("^fb_set_privacy$"))
async def fb_set_privacy_selection(client, callback_query):
    await callback_query.answer()
    await callback_query.edit_message_text("Select Facebook privacy setting:", reply_markup=get_privacy_inline_menu('fb'))

@app.on_callback_query(filters.regex("^fb_privacy_"))
async def fb_set_privacy(client, callback_query):
    user_id = callback_query.from_user.id
    privacy = "Public" if 'public' in callback_query.data else "Private"
    update_user_data(user_id, {"facebook_settings.privacy": privacy})
    await callback_query.answer(f"Facebook privacy set to: {privacy}", show_alert=True)
    await callback_query.edit_message_text(
        f"✅ Facebook privacy set to: {privacy}",
        reply_markup=facebook_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set Facebook privacy to `{privacy}`.")

@app.on_callback_query(filters.regex("^fb_check_expiry_date$"))
async def fb_check_expiry_date(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_doc = get_user_data(user_id)
    fb_settings = user_doc.get("facebook_settings", {})
    expiry_date = fb_settings.get("expiry_date", "Not set (Requires real API integration)")
    await callback_query.edit_message_text(f"🗓️ Facebook expiry date: `{expiry_date}`", reply_markup=facebook_settings_inline_menu)

# --- YouTube Settings Handlers ---
@app.on_callback_query(filters.regex("^yt_login_prompt$"))
async def yt_login_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id): # Ensure user is premium before allowing platform login setup
        await callback_query.answer("You need a premium subscription to connect platforms.", show_alert=True)
        return
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_YT_ACCESS_TOKEN}
    await callback_query.edit_message_text(
        "To log in to YouTube, you'll need to provide your **YouTube Access Token**.\n"
        "Please send it now: `/youtubelogin <your_youtube_access_token>`\n\n"
        "_Note: This is a placeholder for a real YouTube OAuth flow._"
    )

@app.on_message(filters.command("youtubelogin") & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_ACCESS_TOKEN))
async def youtube_login_command(client, message):
    user_id = message.from_user.id
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.reply("❗ Usage: `/youtubelogin <your_youtube_access_token>`")
            return

        access_token = args[1].strip()
        if access_token:
            user_doc = get_user_data(user_id)
            premium_platforms = user_doc.get("premium_platforms", [])
            if "youtube" not in premium_platforms:
                premium_platforms.append("youtube")

            update_user_data(user_id, {
                "youtube_logged_in": True,
                "youtube_access_token": access_token,
                "premium_platforms": premium_platforms,
                "is_premium": True # Ensure is_premium is true when they connect a platform
            })
            await message.reply("✅ YouTube login successful! (Token saved - simulation).", reply_markup=youtube_settings_inline_menu)
            await log_to_channel(client, f"User `{user_id}` successfully 'logged into' YouTube and set as premium.")
        else:
            await message.reply("❌ YouTube login failed. Invalid token provided.", reply_markup=youtube_settings_inline_menu)
            logger.error(f"YouTube token validation failed for user {user_id}: Empty token")
    except Exception as e:
        await message.reply(f"❌ Failed to process YouTube login: {e}", reply_markup=youtube_settings_inline_menu)
        logger.error(f"Failed to process YouTube login for user {user_id}: {e}")
    finally:
        user_states.pop(user_id, None)

@app.on_callback_query(filters.regex("^yt_set_title$"))
async def yt_set_title_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_YT_TITLE}
    await callback_query.edit_message_text("Please send the new YouTube video title:")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_TITLE))
async def yt_set_title_save(client, message):
    user_id = message.from_user.id
    title = message.text
    update_user_data(user_id, {"youtube_settings.title": title})
    user_states.pop(user_id, None)
    await message.reply(f"✅ YouTube title set to: '{title}'", reply_markup=youtube_settings_inline_menu)
    await log_to_channel(client, f"User `{user_id}` set YouTube title.")

@app.on_callback_query(filters.regex("^yt_set_tag$"))
async def yt_set_tag_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_YT_TAG}
    await callback_query.edit_message_text("Please send the new YouTube tag (e.g., #shorts #video):")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_TAG))
async def yt_set_tag_save(client, message):
    user_id = message.from_user.id
    tag = message.text
    update_user_data(user_id, {"youtube_settings.tag": tag})
    user_states.pop(user_id, None)
    await message.reply(f"✅ YouTube tag set to: '{tag}'", reply_markup=youtube_settings_inline_menu)
    await log_to_channel(client, f"User `{user_id}` set YouTube tag.")

@app.on_callback_query(filters.regex("^yt_set_description$"))
async def yt_set_description_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_YT_DESCRIPTION}
    await callback_query.edit_message_text("Please send the new YouTube description:")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_DESCRIPTION))
async def yt_set_description_save(client, message):
    user_id = message.from_user.id
    description = message.text
    update_user_data(user_id, {"youtube_settings.description": description})
    user_states.pop(user_id, None)
    await message.reply(f"✅ YouTube description set to: '{description}'", reply_markup=youtube_settings_inline_menu)
    await log_to_channel(client, f"User `{user_id}` set YouTube description.")

@app.on_callback_query(filters.regex("^yt_video_type$"))
async def yt_video_type_selection(client, callback_query):
    await callback_query.answer()
    await callback_query.edit_message_text("Select YouTube video type:", reply_markup=youtube_video_type_inline_menu)

@app.on_callback_query(filters.regex("^yt_video_type_"))
async def yt_set_video_type(client, callback_query):
    user_id = callback_query.from_user.id
    video_type = "Shorts" if 'shorts' in callback_query.data else "Video"
    update_user_data(user_id, {"youtube_settings.video_type": video_type})
    await callback_query.answer(f"YouTube video type set to: {video_type}", show_alert=True)
    await callback_query.edit_message_text(
        f"✅ YouTube video type set to: {video_type}",
        reply_markup=youtube_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set YouTube video type to `{video_type}`.")

@app.on_callback_query(filters.regex("^yt_set_schedule_time$"))
async def yt_set_schedule_time_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_states[user_id] = {"step": AWAITING_YT_SCHEDULE_TIME}
    await callback_query.edit_message_text(
        "Please send the schedule date and time in `YYYY-MM-DD HH:MM` format (e.g., `2025-07-20 14:30`).\n"
        "_Time will be interpreted in UTC._"
    )

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_SCHEDULE_TIME))
async def yt_set_schedule_time_save(client, message):
    user_id = message.from_user.id
    schedule_str = message.text.strip()
    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")
        if schedule_dt <= datetime.utcnow():
            await message.reply("❌ Schedule time must be in the future. Please try again.")
            return

        update_user_data(user_id, {"youtube_settings.schedule_time": schedule_dt.isoformat()})
        user_states.pop(user_id, None)
        await message.reply(f"✅ YouTube schedule time set to: '{schedule_str}'", reply_markup=youtube_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set YouTube schedule time to `{schedule_str}`.")
    except ValueError:
        await message.reply("❌ Invalid date/time format. Please use `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`).")

@app.on_callback_query(filters.regex("^yt_set_privacy$"))
async def yt_set_privacy_selection(client, callback_query):
    await callback_query.answer()
    await callback_query.edit_message_text("Select YouTube privacy setting:", reply_markup=get_privacy_inline_menu('yt'))

@app.on_callback_query(filters.regex("^yt_privacy_"))
async def yt_set_privacy(client, callback_query):
    user_id = callback_query.from_user.id
    privacy = ""
    if 'public' in callback_query.data:
        privacy = "Public"
    elif 'private' in callback_query.data:
        privacy = "Private"
    elif 'unlisted' in callback_query.data:
        privacy = "Unlisted"

    update_user_data(user_id, {"youtube_settings.privacy": privacy})
    await callback_query.answer(f"YouTube privacy set to: {privacy}", show_alert=True)
    await callback_query.edit_message_text(
        f"✅ YouTube privacy set to: {privacy}",
        reply_markup=youtube_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set YouTube privacy to `{privacy}`.")

@app.on_callback_query(filters.regex("^yt_check_expiry_date$"))
async def yt_check_expiry_date(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer()
    user_doc = get_user_data(user_id)
    yt_settings = user_doc.get("youtube_settings", {})
    expiry_date = yt_settings.get("expiry_date", "Not set (Requires real API integration)")
    await callback_query.edit_message_text(f"🗓️ YouTube expiry date: `{expiry_date}`", reply_markup=youtube_settings_inline_menu)

# --- Upload Flow Handlers ---

@app.on_message(filters.text & filters.regex("^📤 Upload Video (Facebook)$"))
async def upload_facebook_video_prompt(client, message):
    user_id = message.chat.id
    user_doc = get_user_data(user_id)
    if not user_doc:
        await message.reply("Please send /start first to initialize your account.")
        return

    if not is_premium_user(user_id) and not is_admin(user_id):
        await message.reply("❌ You need premium access to use the Facebook upload feature. Please contact the admin to upgrade.")
        return

    fb_access_token = get_facebook_access_token_for_user(user_id)
    if not fb_access_token:
        await message.reply("❌ You are not logged into Facebook. Please use the '🔑 Facebook Login' button in Facebook Settings to provide your access token first.")
        return

    if not FACEBOOK_PAGE_ID or not FACEBOOK_PAGE_ACCESS_TOKEN:
        await message.reply("Bot's Facebook Page ID or Access Token is not configured. Please contact the admin.")
        return

    user_states[user_id] = {"step": "awaiting_video_facebook", "platform": "facebook"}
    await message.reply("🎥 Send your video for Facebook now.")

@app.on_message(filters.video)
async def handle_video_upload(client, message):
    user_id = message.chat.id
    if not get_user_data(user_id):
        await message.reply("⛔ Please send /start first to initialize your account.")
        return

    state = user_states.get(user_id)
    if not state or (state.get("step") != "awaiting_video_facebook"):
        await message.reply("❗ Please click an upload button (e.g., '📤 Upload Video (Facebook)') first.")
        return

    if not is_premium_user(user_id) and not is_admin(user_id):
        await message.reply("❌ You need premium access to upload videos. Please contact the admin.")
        user_states.pop(user_id, None)
        return

    if not os.path.exists("downloads"):
        os.makedirs("downloads")

    initial_status_msg = await message.reply("⏳ Downloading your video... This might take a while for large files.")
    try:
        file_path = await message.download(file_name=f"downloads/{user_id}_{message.video.file_id}.mp4")
        user_states[user_id]["file_path"] = file_path
        user_states[user_id]["step"] = f"awaiting_title_{state['platform']}"
        await initial_status_msg.edit_text("📝 Now send the title for your video.")
    except Exception as e:
        await initial_status_msg.edit_text(f"❌ Failed to download video: {e}")
        logger.error(f"Failed to download video for user {user_id}: {e}")
        user_states.pop(user_id, None)

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step", "").startswith("awaiting_title_")))
async def handle_upload_title(client, message):
    user_id = message.chat.id
    platform = user_states[user_id]["platform"]
    user_states[user_id]["title"] = message.text
    user_states[user_id]["step"] = f"awaiting_caption_or_description_{platform}"

    if platform == "facebook":
        await message.reply("📝 Now send a description for your Facebook video.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step", "").startswith("awaiting_caption_or_description_")))
async def handle_upload_caption_or_description(client, message):
    user_id = message.chat.id
    platform = user_states[user_id]["platform"]
    caption_or_description = message.text.strip()

    user_states[user_id]["caption_or_description"] = caption_or_description
    user_states[user_id]["step"] = f"awaiting_visibility_{platform}"

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Public", callback_data="visibility_public")],
            [InlineKeyboardButton("Private", callback_data="visibility_private")],
            [InlineKeyboardButton("Draft", callback_data="visibility_draft")]
        ]
    )
    await message.reply("🌐 Select video visibility:", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^visibility_"))
async def handle_visibility_selection(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or not state.get("step", "").startswith("awaiting_visibility_"):
        await callback_query.answer("Please start an upload process first.", show_alert=True)
        return

    platform = state["platform"]
    visibility_choice = callback_query.data.split("_")[1]

    user_states[user_id]["visibility"] = visibility_choice
    user_states[user_id]["step"] = f"awaiting_schedule_{platform}"

    if platform == "facebook":
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Publish Now", callback_data="schedule_now")],
                [InlineKeyboardButton("Schedule Later", callback_data="schedule_later")]
            ]
        )
        await callback_query.message.edit_text("⏰ Do you want to publish now or schedule for later?", reply_markup=keyboard)
    else:
        await callback_query.message.edit_text("⏳ Processing your video and preparing for upload... Please wait.")
        await callback_query.answer("Processing initiated.")
        await initiate_upload(client, callback_query.message, user_id)

@app.on_callback_query(filters.regex("^schedule_"))
async def handle_schedule_selection(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or not state.get("step", "").startswith("awaiting_schedule_"):
        await callback_query.answer("Please start an upload process first.", show_alert=True)
        return

    schedule_choice = callback_query.data.split("_")[1]

    if schedule_choice == "now":
        user_states[user_id]["schedule_time"] = None
        await callback_query.message.edit_text("⏳ Processing your video and preparing for upload... Please wait.")
        await callback_query.answer("Processing initiated.")
        await initiate_upload(client, callback_query.message, user_id)
    elif schedule_choice == "later":
        user_states[user_id]["step"] = f"awaiting_schedule_datetime_{state['platform']}"
        await callback_query.message.edit_text(
            "📅 Please send the schedule date and time in `YYYY-MM-DD HH:MM` format (e.g., `2025-07-20 14:30`).\n"
            "_Time will be interpreted in UTC._"
        )
        await callback_query.answer("Awaiting schedule time.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step", "").startswith("awaiting_schedule_datetime_")))
async def handle_schedule_datetime_input(client, message):
    user_id = message.chat.id
    state = user_states.get(user_id)

    try:
        schedule_str = message.text.strip()
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")

        if schedule_dt <= datetime.utcnow():
            await message.reply("❌ Schedule time must be in the future. Please try again.")
            return

        user_states[user_id]["schedule_time"] = schedule_dt
        await message.reply("⏳ Processing your video and preparing for upload... Please wait.")
        await initiate_upload(client, message, user_id)

    except ValueError:
        await message.reply("❌ Invalid date/time format. Please use `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`).")
    except Exception as e:
        await message.reply(f"❌ An error occurred while parsing schedule time: {e}")
        logger.error(f"Error parsing schedule time for user {user_id}: {e}")

async def initiate_upload(client, message, user_id):
    state = user_states.get(user_id)
    if not state:
        await client.send_message(user_id, "❌ Upload process interrupted. Please start again.")
        return

    platform = state["platform"]
    file_path = state["file_path"]
    title = state["title"]
    caption_or_description = state["caption_or_description"]
    visibility = state.get("visibility", "public")
    schedule_time = state.get("schedule_time")

    user_states[user_id]["step"] = "processing_and_uploading"
    await client.send_chat_action(user_id, "upload_video")
    await log_to_channel(client, f"User `{user_id}` initiating upload for {platform}. File: `{os.path.basename(file_path)}`. Visibility: {visibility}. Schedule: {schedule_time}")

    processed_file_path = file_path

    try:
        input_ext = os.path.splitext(file_path)[1].lower()
        output_ext = ".mp4"

        if input_ext != output_ext:
            processed_file_path = f"downloads/processed_{user_id}_{os.path.basename(file_path).replace(input_ext, output_ext)}"
            await client.send_message(user_id, f"Converting video to {output_ext.upper()} format...")
            await client.send_chat_action(user_id, "upload_video")
            def do_processing_sync():
                return convert_video_to_mp4(file_path, processed_file_path)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(do_processing_sync)
                processed_file_path = future.result(timeout=600)
            await client.send_message(user_id, "✅ Video format conversion complete.")
            await log_to_channel(client, f"User `{user_id}` video converted. Output: `{os.path.basename(processed_file_path)}`")

        if platform == "facebook":
            fb_access_token = get_facebook_access_token_for_user(user_id)
            if not fb_access_token:
                await client.send_message(user_id, "❌ Error: Facebook access token not found. Please re-authenticate via Facebook Settings.")
                return

            await client.send_message(user_id, "📤 Uploading to Facebook...")
            await client.send_chat_action(user_id, "upload_video")

            def upload_to_facebook_sync():
                return upload_facebook_video(processed_file_path, title, caption_or_description, fb_access_token, FACEBOOK_PAGE_ID, visibility=visibility, schedule_time=schedule_time)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(upload_to_facebook_sync)
                fb_result = future.result(timeout=900)

            if fb_result and 'id' in fb_result:
                await client.send_message(user_id, f"✅ Uploaded to Facebook! Video ID: `{fb_result['id']}`")
                users_collection.update_one({"_id": user_id}, {"$inc": {"total_uploads": 1}})
                await log_to_channel(client, f"User `{user_id}` successfully uploaded to Facebook. Video ID: `{fb_result['id']}`. File: `{os.path.basename(processed_file_path)}`")
            else:
                await client.send_message(user_id, f"❌ Facebook upload failed: `{fb_result}`")
                logger.error(f"Facebook upload failed for user {user_id}: {fb_result}")

    except concurrent.futures.TimeoutError:
        await client.send_message(user_id, "❌ Upload/processing timed out. The file might be too large or the network is slow. Please try again with a smaller file or better connection.")
        logger.error(f"Upload/processing timeout for user {user_id}. Original file: {file_path}")
    except RuntimeError as re:
        await client.send_message(user_id, f"❌ Processing/Upload Error: `{re}`\n\n_Ensure FFmpeg is installed and your video file is not corrupted._")
        logger.error(f"Processing/Upload Error for user {user_id}: {re}")
    except requests.exceptions.RequestException as req_e:
        await client.send_message(user_id, f"❌ Network/API Error during upload: `{req_e}`\n\n_Please check your internet connection or Facebook API settings._")
        logger.error(f"Network/API Error for user {user_id}: {req_e}")
    except Exception as e:
        await client.send_message(user_id, f"❌ Upload failed: An unexpected error occurred: `{e}`")
        logger.error(f"Upload failed for user {user_id}: {e}", exc_info=True)
    finally:
        if 'file_path' in user_states.get(user_id, {}) and os.path.exists(user_states[user_id]['file_path']):
            os.remove(user_states[user_id]['file_path'])
            logger.info(f"Cleaned up original file: {user_states[user_id]['file_path']}")
        if processed_file_path != file_path and os.path.exists(processed_file_path):
            os.remove(processed_file_path)
            logger.info(f"Cleaned up processed file: {processed_file_path}")
        user_states.pop(user_id, None)

# === KEEP ALIVE SERVER ===
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_server():
    httpd = HTTPServer(('0.0.0.0', 8080), Handler)
    httpd.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# === START BOT ===
if __name__ == "__main__":
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Created 'downloads' directory.")

    logger.info("Bot starting...")
    app.run()
