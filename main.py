import os
import threading
import concurrent.futures
from http.server import HTTPServer, BaseHTTPRequestHandler
import logging
import json
import time
import subprocess
from datetime import datetime, timedelta
import sys

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

# === Facebook API Configuration (APP ID and SECRET needed for token exchange if implemented fully) ===
FACEBOOK_APP_ID = os.getenv("FACEBOOK_APP_ID", "")
FACEBOOK_APP_SECRET = os.getenv("FACEBOOK_APP_SECRET", "")
# FACEBOOK_PAGE_ID and FACEBOOK_PAGE_ACCESS_TOKEN are now specific to user's choice,
# but might be used as a fallback or for admin's direct uploads.
# For dynamic selection, these are less critical for end-user functionality.
# Keeping them for potential admin-level bot-managed page or fallback.
FACEBOOK_PAGE_ID = os.getenv("FACEBOOK_PAGE_ID", "") # This will be for admin or fallback
FACEBOOK_PAGE_ACCESS_TOKEN = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN", "") # This will be for admin or fallback


# === GLOBAL CLIENTS AND DB ===
app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
users_collection = db["users"]

# --- IMPORTANT: MongoDB Index Check/Creation ---
try:
    for index_info in users_collection.index_information().values():
        if index_info.get('unique') and index_info.get('key') == [('user_id', 1)]:
            logger.warning("Found a problematic 'user_id_1' unique index. Attempting to drop it.")
            users_collection.drop_index("user_id_1")
            logger.info("Successfully dropped 'user_id_1' unique index.")
            break
except Exception as e:
    logger.error(f"Error checking/dropping problematic user_id index: {e}")


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
    """Returns the general settings inline keyboard based on user role."""
    keyboard = []
    if is_premium_user(user_id) or is_admin(user_id):
         keyboard.append([InlineKeyboardButton("User Settings", callback_data='settings_user_menu_inline')])
    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("Bot Status", callback_data='settings_bot_status_inline')])

    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_main_menu_reply_from_inline')])
    return InlineKeyboardMarkup(keyboard)

Admin_markup = InlineKeyboardMarkup([
    [InlineKeyboardButton("👥 Users List", callback_data="admin_users_list")],
    [InlineKeyboardButton("➕ Add Premium User", callback_data="admin_add_user_prompt")],
    [InlineKeyboardButton("➖ Remove Premium User", callback_data="admin_remove_user_prompt")],
    [InlineKeyboardButton("📢 Broadcast Message", callback_data="admin_broadcast_prompt")],
    [InlineKeyboardButton("🔄 Restart Bot", callback_data='admin_restart_bot')],
    [InlineKeyboardButton("📤 Admin Upload Video (Facebook)", callback_data='admin_upload_fb')],
    [InlineKeyboardButton("🔙 Back to General Settings", callback_data="settings_main_menu_inline")]
])

user_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🎵 TikTok Settings", callback_data='settings_tiktok')],
        [InlineKeyboardButton("📘 Facebook Settings", callback_data='settings_facebook')],
        [InlineKeyboardButton("▶️ YouTube Settings", callback_data='settings_youtube')],
        [InlineKeyboardButton("⬅️ Back to General Settings", callback_data='settings_main_menu_inline')]
    ]
)

tiktok_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🔑 TikTok Login", callback_data='tiktok_login')],
        [InlineKeyboardButton("📝 Set Caption", callback_data='tiktok_set_caption')],
        [InlineKeyboardButton("🏷️ Set Tag", callback_data='tiktok_set_tag')],
        [InlineKeyboardButton("🎥 Video Type (Aspect Ratio)", callback_data='tiktok_video_type')],
        [InlineKeyboardButton("📄 Set Description", callback_data='tiktok_set_description')],
        [InlineKeyboardButton("ℹ️ Check Account Info", callback_data='tiktok_check_account_info')],
        [InlineKeyboardButton("⬅️ Back to User Settings", callback_data='settings_user_menu_inline')]
    ]
)

# New: Inline keyboard for Facebook pages for settings/info
def get_facebook_page_selection_markup(user_id, for_upload=False):
    user_doc = get_user_data(user_id)
    linked_pages = user_doc.get("facebook_linked_pages", [])
    
    keyboard = []
    if linked_pages:
        for page in linked_pages:
            page_name = page.get("name", "Unnamed Page")
            # Optionally, add a checkmark if this page is the 'active' one in user_states
            # For this implementation, we'll just list them.
            callback_data = f'select_fb_page_{page["id"]}'
            keyboard.append([InlineKeyboardButton(page_name, callback_data=callback_data)])
    
    # Add a 'Login/Link New Page' button
    keyboard.append([InlineKeyboardButton("🔑 Link/Refresh Facebook Pages", callback_data='fb_login_prompt')])
    
    # Add settings buttons if not for upload flow
    if not for_upload:
        keyboard.extend([
            [InlineKeyboardButton("📝 Set Title", callback_data='fb_set_title')],
            [InlineKeyboardButton("🏷️ Set Tag", callback_data='fb_set_tag')],
            [InlineKeyboardButton("📄 Set Description", callback_data='fb_set_description')],
            [InlineKeyboardButton("🎥 Video Type (Reels/Video)", callback_data='fb_video_type')],
            [InlineKeyboardButton("⏰ Set Schedule Time", callback_data='fb_set_schedule_time')],
            [InlineKeyboardButton("🔒 Set Private/Public", callback_data='fb_set_privacy')],
            [InlineKeyboardButton("🗓️ Check Expiry Date", callback_data='fb_check_expiry_date')],
        ])

    keyboard.append([InlineKeyboardButton("⬅️ Back to User Settings", callback_data='settings_user_menu_inline')])
    return InlineKeyboardMarkup(keyboard)

# Existing Facebook settings inline menu will now primarily link to page management or general settings
facebook_settings_inline_menu_main = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🗂️ Manage Linked Pages", callback_data='fb_manage_pages')], # New button
        [InlineKeyboardButton("📝 Set Global Title", callback_data='fb_set_title')], # Renamed for clarity
        [InlineKeyboardButton("🏷️ Set Global Tag", callback_data='fb_set_tag')], # Renamed for clarity
        [InlineKeyboardButton("📄 Set Global Description", callback_data='fb_set_description')], # Renamed for clarity
        [InlineKeyboardButton("🎥 Set Global Video Type", callback_data='fb_video_type')], # Renamed for clarity
        [InlineKeyboardButton("⏰ Set Global Schedule Time", callback_data='fb_set_schedule_time')], # Renamed for clarity
        [InlineKeyboardButton("🔒 Set Global Private/Public", callback_data='fb_set_privacy')], # Renamed for clarity
        [InlineKeyboardButton("🗓️ Check Expiry Date (Page Token)", callback_data='fb_check_expiry_date')],
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
        [InlineKeyboardButton("1:1 Aspect Ratio (Square)", callback_data='tiktok_aspect_ratio_1_1')],
        [InlineKeyboardButton("9:16 Aspect Ratio (Vertical)", callback_data='tiktok_aspect_ratio_9_16')],
        [InlineKeyboardButton("⬅️ Back to Tik Settings", callback_data='settings_tiktok')]
    ]
)

facebook_video_type_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("Reels (Short Vertical Video)", callback_data='fb_video_type_reels')],
        [InlineKeyboardButton("Video (Standard Horizontal/Square)", callback_data='fb_video_type_video')],
        [InlineKeyboardButton("⬅️ Back to Fb Settings", callback_data='settings_facebook')]
    ]
)

youtube_video_type_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("Shorts (Short Vertical Video)", callback_data='yt_video_type_shorts')],
        [InlineKeyboardButton("Video (Standard Horizontal/Square)", callback_data='yt_video_type_video')],
        [InlineKeyboardButton("⬅️ Back to YT Settings", callback_data='settings_youtube')]
    ]
)

def get_privacy_inline_menu(platform):
    """Generates privacy inline keyboard for different platforms."""
    keyboard = [
        [InlineKeyboardButton("Public", callback_data=f'{platform}_privacy_public')],
        [InlineKeyboardButton("Private", callback_data=f'{platform}_privacy_private')],
    ]
    if platform == 'yt':
        keyboard.append([InlineKeyboardButton("Unlisted", callback_data='yt_privacy_unlisted')])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f'settings_{platform}')])
    return InlineKeyboardMarkup(keyboard)


# === USER STATES (for sequential conversation flows) ===
user_states = {}

# === CONVERSATION STATES (for specific text input steps) ===
AWAITING_TIKTOK_CAPTION = "awaiting_tiktok_caption"
AWAITING_TIKTOK_TAG = "awaiting_tiktok_tag"
AWAITING_TIKTOK_DESCRIPTION = "awaiting_tiktok_description"
AWAITING_TIKTOK_LOGIN_DETAILS = "awaiting_tiktok_login_details"

AWAITING_FB_TITLE = "awaiting_fb_title"
AWAITING_FB_TAG = "awaiting_fb_tag"
AWAITING_FB_DESCRIPTION = "awaiting_fb_description"
AWAITING_FB_SCHEDULE_TIME = "awaiting_fb_schedule_time"
AWAITING_FB_ACCESS_TOKEN = "awaiting_fb_access_token" # State for initial token input
AWAITING_FB_PAGE_SELECTION_UPLOAD = "awaiting_fb_page_selection_upload" # New state for page selection before upload
AWAITING_FB_PAGE_SELECTION_SETTINGS = "awaiting_fb_page_selection_settings" # New state for page selection in settings


AWAITING_YT_TITLE = "awaiting_yt_title"
AWAITING_YT_TAG = "awaiting_yt_tag"
AWAITING_YT_DESCRIPTION = "awaiting_yt_description"
AWAITING_YT_SCHEDULE_TIME = "awaiting_yt_schedule_time"
AWAITING_YT_ACCESS_TOKEN = "awaiting_yt_access_token"

AWAITING_BROADCAST_MESSAGE = "awaiting_broadcast_message"

# --- Upload Flow Specific States ---
AWAITING_UPLOAD_VIDEO = "awaiting_upload_video"
AWAITING_UPLOAD_TITLE = "awaiting_upload_title"
AWAITING_UPLOAD_DESCRIPTION = "awaiting_upload_description"
AWAITING_UPLOAD_VISIBILITY = "awaiting_upload_visibility"
AWAITING_UPLOAD_SCHEDULE = "awaiting_upload_schedule"
AWAITING_UPLOAD_SCHEDULE_DATETIME = "awaiting_upload_schedule_datetime"


# === HELPERS ===
def get_user_data(user_id):
    """Retrieves user data from MongoDB using _id. Returns None if not found."""
    return users_collection.find_one({"_id": user_id})

def update_user_data(user_id, data):
    """Updates user data in MongoDB using _id for upsert. Handles potential errors."""
    try:
        users_collection.update_one({"_id": user_id}, {"$set": data}, upsert=True)
        logger.info(f"User {user_id} data updated/upserted successfully.")
    except Exception as e:
        logger.error(f"Failed to update user {user_id} data: {e}")

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
        await client.send_message(LOG_CHANNEL_ID, f"**Bot Log:**\n\n{message_text}", parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Failed to send message to log channel (ID: {LOG_CHANNEL_ID}): {e}")

# This function is now deprecated for dynamic page selection
# def get_facebook_access_token_for_user(user_id):
#     """Retrieves Facebook access token from user data."""
#     user_doc = get_user_data(user_id)
#     return user_doc.get("facebook_access_token")

# This function is now deprecated for dynamic page selection
# def store_facebook_access_token_for_user(user_id, token):
#     """Stores Facebook access token in user data."""
#     update_user_data(user_id, {"facebook_access_token": token})

def upload_facebook_video(file_path, title, description, page_access_token, page_id, visibility="PUBLISHED", schedule_time=None):
    """Uploads a video to a specific Facebook Page using Graph API and its Page Access Token."""
    if not all([file_path, title, description, page_access_token, page_id]):
        raise ValueError("Missing required parameters for Facebook video upload.")

    post_url = f"https://graph-video.facebook.com/v19.0/{page_id}/videos"

    params = {
        'access_token': page_access_token, # Use page_access_token
        'title': title,
        'description': description,
    }

    if schedule_time:
        params['published'] = 'false'
        params['scheduled_publish_time'] = int(schedule_time.timestamp())
        params['status_type'] = 'SCHEDULED_PUBLISH'
        logger.info(f"Scheduling Facebook video for: {schedule_time} on page {page_id}")
    else:
        if visibility.lower() == 'private' or visibility.lower() == 'draft':
            params['published'] = 'false'
            params['status_type'] = 'DRAFT'
            logger.info(f"Uploading Facebook video as DRAFT (visibility: {visibility}) on page {page_id}.")
        else:
            params['published'] = 'true'
            params['status_type'] = 'PUBLISHED'
            logger.info(f"Uploading Facebook video as PUBLISHED (visibility: {visibility}) on page {page_id}.")

    with open(file_path, 'rb') as f:
        files = {'file': f}
        response = requests.post(post_url, params=params, files=files)
        response.raise_for_status()
        result = response.json()
        logger.info(f"Facebook video upload result for page {page_id}: {result}")
        return result

def convert_video_to_mp4(input_path, output_path):
    """
    Converts video to MP4 format, copying video and audio streams.
    Ensures output is MP4.
    """
    command = ["ffmpeg", "-i", input_path, "-c:v", "copy", "-c:a", "copy", "-map", "0", "-y", output_path]
    logger.info(f"[FFmpeg] Initiating video conversion for {input_path}")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=600)
        logger.info(f"[FFmpeg] Conversion successful for {input_path}. Output: {result.stdout}")
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"[FFmpeg] Conversion failed for {input_path}. Command: {' '.join(e.cmd)}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        raise RuntimeError(f"FFmpeg conversion error: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("FFmpeg not found. Please install FFmpeg and ensure it's in your system's PATH.")
    except subprocess.TimeoutExpired:
        raise RuntimeError("FFmpeg conversion timed out. Video might be too large or complex.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during FFmpeg conversion: {e}")
        raise

# === PYROGRAM HANDLERS ===

@app.on_message(filters.command("start"))
async def start_command(client, message):
    """Handles the /start command, initializes/updates user data."""
    user_id = message.from_user.id
    user_first_name = message.from_user.first_name or "Unknown User"
    user_username = message.from_user.username or "N/A"

    user_data_to_set = {
        "first_name": user_first_name,
        "username": user_username,
        "last_active": datetime.now(),
        "is_premium": False,
        "role": "user",
        "premium_platforms": [],
        "total_uploads": 0,
        "tiktok_settings": {
            "logged_in": False, "caption": "Default TikTok Caption", "tag": "#tiktok #video #fyp", "video_type": "9:16 Aspect Ratio (Vertical)", "description": "Default TikTok Description"
        },
        "facebook_settings": {
            "title": "Default Facebook Title", "tag": "#facebook #video #reels", "description": "Default Facebook Description", "video_type": "Video (Standard Horizontal/Square)", "schedule_time": None, "privacy": "Public"
        },
        "facebook_linked_pages": [], # NEW: To store multiple linked Facebook pages
        "youtube_settings": {
            "title": "Default YouTube Title", "tag": "#youtube #video #shorts", "description": "Default YouTube Description", "video_type": "Video (Standard Horizontal/Square)", "schedule_time": None, "privacy": "Public"
        }
    }

    if user_id == OWNER_ID:
        user_data_to_set["role"] = "admin"
        user_data_to_set["is_premium"] = True

    try:
        users_collection.update_one(
            {"_id": user_id},
            {"$set": user_data_to_set, "$setOnInsert": {"added_at": datetime.now(), "added_by": "self_start"}},
            upsert=True
        )
        logger.info(f"User {user_id} account initialized/updated successfully.")
    except Exception as e:
        logger.error(f"Error during user data update/upsert for user {user_id}: {e}")
        await message.reply("🚨 **System Alert!** An error occurred while initializing your account. Please try again later or contact support.")
        return

    user_doc = get_user_data(user_id)
    if not user_doc:
        logger.error(f"Could not retrieve user document for {user_id} after upsert. This is unexpected.")
        await message.reply("❌ **Error!** Failed to retrieve your account data after setup. Please try `/start` again.")
        return

    await log_to_channel(client, f"User `{user_id}` (`{user_username}` - `{user_first_name}`) performed `/start`. Role: `{user_doc.get('role')}`, Premium: `{user_doc.get('is_premium')}`.")

    if is_admin(user_id):
        welcome_msg = (
            f"🤖 **Welcome to the Upload Bot, Administrator {user_first_name}!**\n\n"
            "🛠 You have **full system access and privileges**.\n"
            "Ready to command the digital frontier!"
        )
        reply_markup = main_menu_admin
    elif is_premium_user(user_id):
        welcome_msg = (
            f"🤖 **Welcome to the Upload Bot, Premium User {user_first_name}!**\n\n"
            "⭐ You have **premium access** to all features. Unleash your creativity!\n"
            "Ready to upload your content directly from Telegram."
        )
        reply_markup = main_menu_user
    else:
        contact_admin_text = (
            f"👋 **Greetings, {user_first_name}!**\n\n"
            "This bot is your gateway to **effortless video uploads** directly from Telegram.\n\n"
            "• **Unlock Full Premium Features for:**\n"
            "  • **YouTube (Shorts & Videos)**\n"
            "  • **Facebook (Reels & Posts)**\n"
            "  • **TikTok (Videos)**\n\n"
            "• **Enjoy Unlimited Video Uploads & Advanced Options!**\n"
            "• **Automatic/Customizable Captions, Titles, & Hashtags**\n"
            "• **Flexible Content Type Selection (Reel, Post, Short, etc.)**\n\n"
            f"👤 Contact **[ADMIN TOM](https://t.me/{ADMIN_TOM_USERNAME})** **To Upgrade Your Access**.\n"
            "🔐 **Your Data Is Fully ✅Encrypted**\n\n"
            f"🆔 Your System User ID: `{user_id}`"
        )

        join_channel_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅Join Our Digital Hub✅", url=CHANNEL_LINK)]
        ])

        await client.send_photo(
            chat_id=message.chat.id,
            photo=CHANNEL_PHOTO_URL,
            caption=contact_admin_text,
            reply_markup=join_channel_markup,
            parse_mode=enums.ParseMode.MARKDOWN
        )
        return

    await message.reply(welcome_msg, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)
    logger.info(f"Start command completed for user {user_id}. Showing {'admin' if is_admin(user_id) else 'premium' if is_premium_user(user_id) else 'regular'} menu.")

# --- Admin Commands ---
@app.on_message(filters.command("addadmin") & filters.user(OWNER_ID))
async def add_admin_command(client, message):
    """Promotes a user to admin status."""
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2 or not args[1].isdigit():
            await message.reply("❗ **Syntax Error:** Usage: `/addadmin <user_id>`")
            return

        target_user_id = int(args[1])
        update_user_data(target_user_id, {"role": "admin", "is_premium": True})
        await message.reply(f"✅ **Success!** User `{target_user_id}` has been promoted to **ADMIN** and **PREMIUM** status.")
        try:
            await client.send_message(target_user_id, "🎉 **System Notification!** You have been promoted to an administrator! Use `/start` to access your new command interface.")
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id} about admin promotion: {e}")
        await log_to_channel(client, f"User `{target_user_id}` promoted to admin by `{message.from_user.id}` (`{message.from_user.username}`).")

    except Exception as e:
        await message.reply(f"❌ **Error!** Failed to add administrator: `{e}`")
        logger.error(f"Failed to add admin for {message.from_user.id}: {e}")

@app.on_message(filters.command("removeadmin") & filters.user(OWNER_ID))
async def remove_admin_command(client, message):
    """Demotes a user from admin status."""
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2 or not args[1].isdigit():
            await message.reply("❗ **Syntax Error:** Usage: `/removeadmin <user_id>`")
            return

        target_user_id = int(args[1])
        user_doc = get_user_data(target_user_id)

        if target_user_id == OWNER_ID:
            await message.reply("❌ **Access Denied!** You cannot remove the owner's administrator status.")
            return

        if user_doc and user_doc.get("role") == "admin":
            update_user_data(target_user_id, {"role": "user", "is_premium": False, "premium_platforms": []})
            await message.reply(f"✅ **Success!** User `{target_user_id}` has been demoted to a regular user and removed from premium access.")
            try:
                await client.send_message(target_user_id, "❗ **System Notification!** Your administrator status has been revoked, and premium access removed.")
            except Exception as e:
                logger.warning(f"Could not notify user {target_user_id} about admin demotion: {e}")
            await log_to_channel(client, f"User `{target_user_id}` demoted from admin by `{message.from_user.id}` (`{message.from_user.username}`).")
        else:
            await message.reply(f"User `{target_user_id}` is not an administrator or not found in the system.")

    except Exception as e:
        await message.reply(f"❌ **Error!** Failed to remove administrator: `{e}`")
        logger.error(f"Failed to remove admin for {message.from_user.id}: {e}")

# --- Settings Menu Handlers (Reply Keyboard & Inline) ---

@app.on_message(filters.text & filters.regex("^⚙️ Settings$"))
async def show_main_settings_menu_reply(client, message):
    """Displays the main settings menu via inline keyboard."""
    user_id = message.from_user.id
    user_doc = get_user_data(user_id)
    if not user_doc:
        await message.reply("⛔ **Access Denied!** Please send `/start` first to initialize your account in the system.")
        return
    await message.reply("⚙️ **System Configuration Interface:**\n\nChoose your settings options:", reply_markup=get_general_settings_inline_keyboard(user_id))
    logger.info(f"User {user_id} accessed main settings menu.")

@app.on_message(filters.text & filters.regex("^🔙 Main Menu$"))
async def back_to_main_menu_reply(client, message):
    """Handles the 'Back to Main Menu' reply button."""
    user_id = message.from_user.id
    user_states.pop(user_id, None)
    if is_admin(user_id):
        await message.reply("✅ **Returning to Command Center.**", reply_markup=main_menu_admin)
    else:
        await message.reply("✅ **Returning to Main System Interface.**", reply_markup=main_menu_user)
    logger.info(f"User {user_id} returned to main menu via reply button.")


# --- General Settings Inline Callbacks ---
@app.on_callback_query(filters.regex("^settings_main_menu_inline$"))
async def settings_main_menu_inline_callback(client, callback_query):
    """Callback for navigating to the general settings inline menu."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Accessing settings...")
    await callback_query.edit_message_text(
        "⚙️ **System Configuration Interface:**\n\nChoose your settings options:",
        reply_markup=get_general_settings_inline_keyboard(user_id)
    )
    logger.info(f"User {user_id} navigated to general settings via inline button.")

@app.on_callback_query(filters.regex("^back_to_main_menu_reply_from_inline$"))
async def back_to_main_menu_from_inline(client, callback_query):
    """Handles 'Back to Main Menu' from an inline keyboard, switches to reply keyboard."""
    user_id = callback_query.from_user.id
    user_states.pop(user_id, None)
    await callback_query.answer("System redirection initiated...")

    if is_admin(user_id):
        await client.send_message(user_id, "✅ **Returning to Command Center.**", reply_markup=main_menu_admin)
    else:
        await client.send_message(user_id, "✅ **Returning to Main System Interface.**", reply_markup=main_menu_user)
    try:
        await callback_query.message.delete()
        logger.info(f"Deleted inline message for user {user_id}.")
    except Exception as e:
        logger.warning(f"Could not delete inline message for user {user_id}: {e}")
    logger.info(f"User {user_id} returned to main menu via inline back button.")

@app.on_callback_query(filters.regex("^settings_user_menu_inline$"))
async def settings_user_menu_callback(client, callback_query):
    """Callback for navigating to user-specific platform settings."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** You need a premium subscription to access user-specific configuration.", show_alert=True)
        logger.info(f"User {user_id} attempted to access user settings without premium.")
        return
    await callback_query.answer("Accessing user configurations...")
    await callback_query.edit_message_text(
        "⚙️ **User Account Settings:**\n\nChoose a platform to configure:",
        reply_markup=user_settings_inline_menu
    )
    logger.info(f"User {user_id} accessed user settings menu.")

# --- Admin Panel Reply Button Handler ---
@app.on_message(filters.text & filters.regex("^👤 Admin Panel$") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_panel_menu_reply(client, message):
    """Displays the admin panel menu."""
    user_id = message.from_user.id
    await message.reply("👋 **Welcome to the Administrator Command Center!**", reply_markup=Admin_markup)
    logger.info(f"Admin {user_id} accessed the Admin Panel.")

# --- Admin Inline Callbacks (from Admin_markup) ---

@app.on_callback_query(filters.regex("^admin_users_list$"))
async def admin_users_list_inline(client, callback_query):
    """Displays a list of all registered users for admin."""
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        logger.warning(f"Non-admin user {user_id} attempted to access admin_users_list.")
        return
    await callback_query.answer("Fetching system user directory...")

    all_users = list(users_collection.find({}, {"_id": 1, "first_name": 1, "username": 1, "role": 1, "is_premium": 1}))
    user_list_text = "**👥 Registered System Users:**\n\n"
    if not all_users:
        user_list_text += "No user records found in the system database."
    else:
        for user in all_users:
            role = user.get("role", "user").capitalize()
            premium_status = "⭐ Premium" if user.get("is_premium") else ""
            user_list_text += (
                f"• ID: `{user['_id']}`\n"
                f"  Name: `{user.get('first_name', 'N/A')}`\n"
                f"  Username: `@{user.get('username', 'N/A')}`\n"
                f"  Status: `{role}` {premium_status}\n\n"
            )

    await callback_query.edit_message_text(user_list_text, reply_markup=Admin_markup, parse_mode=enums.ParseMode.MARKDOWN)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) viewed system users list.")

@app.on_callback_query(filters.regex("^admin_add_user_prompt$"))
async def admin_add_user_prompt_inline(client, callback_query):
    """Prompts admin to send user ID to add premium."""
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("Initiating user upgrade protocol...")
    user_states[user_id] = {"step": "admin_awaiting_user_id_to_add"}
    await callback_query.edit_message_text(
        "Please transmit the **Telegram User ID** of the user you wish to grant **PREMIUM ACCESS**.\n"
        "Input the numeric ID now.",
        reply_markup=Admin_markup
    )
    logger.info(f"Admin {user_id} prompted to add premium user.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == "admin_awaiting_user_id_to_add") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_add_user_id_input(client, message):
    """Handles input of user ID for adding premium status."""
    user_id = message.from_user.id
    target_user_id_str = message.text.strip()
    user_states.pop(user_id, None)

    try:
        target_user_id = int(target_user_id_str)
        update_user_data(target_user_id, {"is_premium": True, "role": "user"})
        await message.reply(f"✅ **Success!** User `{target_user_id}` has been granted **PREMIUM ACCESS**.", reply_markup=Admin_markup)
        try:
            await client.send_message(target_user_id, "🎉 **System Notification!** Your account has been upgraded to **PREMIUM** status! Use `/start` to access your enhanced features.")
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id} about premium upgrade: {e}")
        await log_to_channel(client, f"Admin `{user_id}` (`{message.from_user.username}`) upgraded user `{target_user_id}` to premium.")

    except ValueError:
        await message.reply("❌ **Input Error.** Invalid User ID detected. Please transmit a numeric ID.", reply_markup=Admin_markup)
        logger.warning(f"Admin {user_id} provided invalid user ID '{target_user_id_str}' for adding premium.")
    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during user premium assignment: `{e}`", reply_markup=Admin_markup)
        logger.error(f"Failed to add premium user for admin {user_id}: {e}")

@app.on_callback_query(filters.regex("^admin_remove_user_prompt$"))
async def admin_remove_user_prompt_inline(client, callback_query):
    """Prompts admin to send user ID to remove premium."""
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("Initiating user downgrade protocol...")
    user_states[user_id] = {"step": "admin_awaiting_user_id_to_remove"}
    await callback_query.edit_message_text(
        "Please transmit the **Telegram User ID** of the user you wish to revoke **PREMIUM ACCESS** from.\n"
        "Input the numeric ID now.",
        reply_markup=Admin_markup
    )
    logger.info(f"Admin {user_id} prompted to remove premium user.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == "admin_awaiting_user_id_to_remove") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_remove_user_id_input(client, message):
    """Handles input of user ID for removing premium status."""
    user_id = message.from_user.id
    target_user_id_str = message.text.strip()
    user_states.pop(user_id, None)

    try:
        target_user_id = int(target_user_id_str)
        if target_user_id == OWNER_ID:
            await message.reply("❌ **Security Alert!** Cannot revoke owner's premium status.", reply_markup=Admin_markup)
            return

        user_doc = get_user_data(target_user_id)

        if user_doc and user_doc.get("is_premium"):
            update_user_data(target_user_id, {"is_premium": False, "premium_platforms": []})
            await message.reply(f"✅ **Success!** User `{target_user_id}` has been revoked from **PREMIUM ACCESS**.", reply_markup=Admin_markup)
            try:
                await client.send_message(target_user_id, "❗ **System Notification!** Your premium access has been revoked.")
            except Exception as e:
                logger.warning(f"Could not notify user {target_user_id} about premium revocation: {e}")
            await log_to_channel(client, f"Admin `{user_id}` (`{message.from_user.username}`) revoked premium for user `{target_user_id}`.")
        else:
            await message.reply(f"User `{target_user_id}` is not a premium user or no record found in the system.", reply_markup=Admin_markup)

    except ValueError:
        await message.reply("❌ **Input Error.** Invalid User ID detected. Please transmit a numeric ID.", reply_markup=Admin_markup)
        logger.warning(f"Admin {user_id} provided invalid user ID '{target_user_id_str}' for removing premium.")
    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during user premium revocation: `{e}`", reply_markup=Admin_markup)
        logger.error(f"Failed to remove user for admin {user_id}: {e}")

@app.on_callback_query(filters.regex("^admin_broadcast_prompt$"))
async def admin_broadcast_prompt_inline(client, callback_query):
    """Prompts admin to send message for broadcast."""
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("Initiating broadcast transmission protocol...")
    user_states[user_id] = {"step": AWAITING_BROADCAST_MESSAGE}
    await callback_query.edit_message_text(
        "Please transmit the **message payload** you wish to broadcast to all active system users.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Terminate Broadcast", callback_data="cancel_broadcast")]])
    )
    logger.info(f"Admin {user_id} prompted for broadcast message.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_BROADCAST_MESSAGE) & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def broadcast_message_handler(client, message):
    """Handles the broadcast message input and sends it to all users."""
    user_id = message.from_user.id
    text_to_broadcast = message.text
    user_states.pop(user_id, None)

    await message.reply("📡 **Initiating Global Transmission...**")
    await log_to_channel(client, f"Broadcast initiated by `{user_id}` (`{message.from_user.username}`). Message preview: '{text_to_broadcast[:50]}...'")

    all_user_ids = [user["_id"] for user in users_collection.find({}, {"_id": 1})]
    success_count = 0
    fail_count = 0

    for target_user_id in all_user_ids:
        try:
            if target_user_id == user_id:
                continue
            await client.send_message(target_user_id, f"📢 **ADMIN BROADCAST MESSAGE:**\n\n{text_to_broadcast}")
            success_count += 1
            time.sleep(0.05)
        except Exception as e:
            fail_count += 1
            logger.warning(f"Failed to send broadcast to user {target_user_id}: {e}")

    await message.reply(f"✅ **Broadcast Transmission Complete.** Sent to `{success_count}` users, `{fail_count}` transmissions failed.", reply_markup=Admin_markup)
    await log_to_channel(client, f"Broadcast finished by `{user_id}`. Transmitted: {success_count}, Failed: {fail_count}.")

@app.on_callback_query(filters.regex("^cancel_broadcast$"))
async def cancel_broadcast_callback(client, callback_query):
    """Cancels an ongoing broadcast message input."""
    user_id = callback_query.from_user.id
    if user_states.get(user_id, {}).get("step") == AWAITING_BROADCAST_MESSAGE:
        user_states.pop(user_id, None)
        await callback_query.answer("Broadcast sequence terminated.")
        await callback_query.message.edit_text("🛑 **Broadcast Protocol Terminated.**", reply_markup=Admin_markup)
        await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) cancelled broadcast.")
    else:
        await callback_query.answer("No active broadcast protocol to terminate.", show_alert=True)

@app.on_callback_query(filters.regex("^admin_restart_bot$"))
async def admin_restart_bot_callback(client, callback_query):
    """Restarts the bot (exits the process, assuming a process manager will restart it)."""
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("System reboot sequence initiated...", show_alert=True)
    await callback_query.message.edit_text("🔄 **System Rebooting...** This may take a moment. Please send `/start` in a few seconds to re-establish connection.", reply_markup=None)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) initiated bot restart.")
    sys.exit(0)

@app.on_callback_query(filters.regex("^admin_upload_fb$"))
async def admin_upload_fb_callback(client, callback_query):
    """Initiates Facebook video upload flow for admin."""
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("Initiating Facebook upload protocol for Administrator...")
    # Admin upload will use the globally configured FACEBOOK_PAGE_ID and FACEBOOK_PAGE_ACCESS_TOKEN
    # This is a simplification; for a full admin feature, you might want to allow selecting any user's page.
    if not FACEBOOK_PAGE_ID or not FACEBOOK_PAGE_ACCESS_TOKEN:
         await callback_query.message.edit_text("❌ **System Error.** Admin Facebook Page ID or Access Token is not configured in environment variables. Cannot proceed with admin upload.", reply_markup=Admin_markup)
         await log_to_channel(client, f"Admin {user_id} attempted admin FB upload but bot's global FACEBOOK_PAGE_ID/TOKEN is missing.")
         return

    user_states[user_id] = {
        "step": AWAITING_UPLOAD_VIDEO,
        "platform": "facebook",
        "fb_page_id": FACEBOOK_PAGE_ID, # Pre-set for admin upload
        "fb_page_access_token": FACEBOOK_PAGE_ACCESS_TOKEN # Pre-set for admin upload
    }
    await callback_query.message.edit_text(
        "🎥 **Facebook Upload Protocol Active.** Please transmit the video data file directly now.",
        reply_markup=None
    )
    await client.send_message(user_id, "You can use '🔙 Main Menu' to abort the transmission.", reply_markup=main_menu_admin)
    logger.info(f"Admin {user_id} initiated Facebook upload.")

@app.on_callback_query(filters.regex("^settings_bot_status_inline$"))
async def settings_bot_status_inline_callback(client, callback_query):
    """Displays bot status and statistics for admin."""
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Access Restricted.** You are not authorized to access system diagnostics.", show_alert=True)
        return

    await callback_query.answer("Fetching system diagnostics...")
    total_users = users_collection.count_documents({})
    admin_users = users_collection.count_documents({"role": "admin"})
    premium_users = users_collection.count_documents({"is_premium": True})

    total_fb_accounts = users_collection.count_documents({"facebook_linked_pages": {"$exists": True, "$ne": []}}) # Check for non-empty array
    total_tiktok_accounts = users_collection.count_documents({"tiktok_settings.logged_in": True})
    total_youtube_accounts = users_collection.count_documents({"youtube_logged_in": True})

    total_uploads_count = sum(user.get("total_uploads", 0) for user in users_collection.find({}, {"total_uploads": 1}))

    stats_message = (
        f"**📊 System Diagnostics & Statistics:**\n\n"
        f"**User Matrix:**\n"
        f"• Total Registered Users: `{total_users}`\n"
        f"• System Administrators: `{admin_users}`\n"
        f"• Premium Access Users: `{premium_users}`\n\n"
        f"**Integrated Accounts:**\n"
        f"• Facebook Accounts Synced: `{total_fb_accounts}`\n"
        f"• TikTok Accounts Synced: `{total_tiktok_accounts}`\n"
        f"• YouTube Accounts Synced: `{total_youtube_accounts}`\n\n"
        f"**Operational Metrics:**\n"
        f"• Total Content Transmissions: `{total_uploads_count}`\n\n"
        f"_Note: 'People left' metric is not tracked directly by this system._"
    )
    await callback_query.edit_message_text(stats_message, reply_markup=get_general_settings_inline_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) viewed detailed system status.")

# --- Platform Specific Settings Menus ---
@app.on_callback_query(filters.regex("^settings_tiktok$"))
async def show_tiktok_settings(client, callback_query):
    """Displays TikTok settings menu."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing TikTok configurations...")
    await callback_query.edit_message_text("🎵 **TikTok Configuration Module:**", reply_markup=tiktok_settings_inline_menu)
    logger.info(f"User {user_id} accessed TikTok settings.")

@app.on_callback_query(filters.regex("^settings_facebook$"))
async def show_facebook_settings(client, callback_query):
    """Displays Facebook settings menu."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing Facebook configurations...")
    await callback_query.edit_message_text("📘 **Facebook Configuration Module:**", reply_markup=facebook_settings_inline_menu_main) # Use the new main menu
    logger.info(f"User {user_id} accessed Facebook settings.")

# New: Handle 'Manage Linked Pages' button
@app.on_callback_query(filters.regex("^fb_manage_pages$"))
async def fb_manage_pages_callback(client, callback_query):
    user_id = callback_query.from_user.id
    user_doc = get_user_data(user_id)
    linked_pages = user_doc.get("facebook_linked_pages", [])

    if not linked_pages:
        await callback_query.answer("No Facebook pages currently linked.", show_alert=True)
        await callback_query.message.edit_text(
            "🗂️ **Facebook Linked Pages:**\n\n"
            "No pages are currently associated with your account. "
            "Please use '🔑 Link/Refresh Facebook Pages' to add or update your linked pages.",
            reply_markup=get_facebook_page_selection_markup(user_id)
        )
    else:
        await callback_query.answer("Displaying linked Facebook pages.")
        await callback_query.message.edit_text(
            "🗂️ **Facebook Linked Pages:**\n\n"
            "Select a page to view/manage settings, or link a new one.",
            reply_markup=get_facebook_page_selection_markup(user_id)
        )
    user_states[user_id] = {"step": AWAITING_FB_PAGE_SELECTION_SETTINGS} # Set state for page selection in settings
    logger.info(f"User {user_id} accessing Facebook linked pages management.")

# Handle selection of a specific Facebook page from settings
@app.on_callback_query(filters.regex("^select_fb_page_"))
async def select_fb_page_for_settings(client, callback_query):
    user_id = callback_query.from_user.id
    page_id = callback_query.data.split("_")[3] # Extract page ID

    user_doc = get_user_data(user_id)
    linked_pages = user_doc.get("facebook_linked_pages", [])
    selected_page_info = next((page for page in linked_pages if page["id"] == page_id), None)

    if selected_page_info:
        # Store selected page info in user_states for current interaction context
        user_states[user_id] = {
            "step": "fb_page_selected_for_settings",
            "selected_fb_page_id": selected_page_info["id"],
            "selected_fb_page_name": selected_page_info["name"],
            "selected_fb_page_access_token": selected_page_info["access_token"]
        }
        await callback_query.answer(f"Selected page: {selected_page_info['name']}", show_alert=True)
        await callback_query.message.edit_text(
            f"✅ **Page Selected:** `{selected_page_info['name']}`\n\n"
            f"Now, any settings (title, description, etc.) you configure will apply to this page. "
            f"You can also proceed with content transmission to this page.",
            reply_markup=get_facebook_page_selection_markup(user_id, for_upload=False) # Show page settings options
        )
        logger.info(f"User {user_id} selected Facebook page {page_id} ({selected_page_info['name']}) for settings.")
    else:
        await callback_query.answer("Error: Page not found in your linked accounts.", show_alert=True)
        logger.warning(f"User {user_id} attempted to select non-existent FB page {page_id}.")
        await callback_query.message.edit_text(
            "❌ **Page Not Found.** Please try linking your pages again.",
            reply_markup=get_facebook_page_selection_markup(user_id)
        )


@app.on_callback_query(filters.regex("^settings_youtube$"))
async def show_youtube_settings(client, callback_query):
    """Displays YouTube settings menu."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing YouTube configurations...")
    await callback_query.edit_message_text("▶️ **YouTube Configuration Module:**", reply_markup=youtube_settings_inline_menu)
    logger.info(f"User {user_id} accessed YouTube settings.")

# --- TikTok Settings Handlers ---
@app.on_callback_query(filters.regex("^tiktok_login$"))
async def tiktok_login_prompt(client, callback_query):
    """Prompts for TikTok login details (placeholder for direct login)."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** You need a premium subscription to connect platforms.", show_alert=True)
        return
    await callback_query.answer("Initiating TikTok login procedure...")
    user_states[user_id] = {"step": AWAITING_TIKTOK_LOGIN_DETAILS}
    await callback_query.edit_message_text(
        "🔑 **TikTok Authentication Protocol:**\n\n"
        "To perform a **direct TikTok login**, please transmit your credentials using the format below.\n"
        "**Format:** `/tiktoklogin <username_or_email> <password> [optional_proxy_url]`\n\n"
        "**Example:**\n"
        "`/tiktoklogin mytiktokuser MyS3cur3P@ss http://user:pass@proxy.example.com:8080`\n"
        "`/tiktoklogin mytiktokemail@example.com MyS3cur3P@ss`\n\n"
        "_**System Warning:** Direct login functionality is simulated in this version and for demonstration. Real-world direct login for TikTok is complex, often changes, and may violate platform terms of service. For robust integration, consider official OAuth APIs or dedicated third-party libraries for web automation._"
    )
    logger.info(f"User {user_id} prompted for TikTok direct login.")

@app.on_message(filters.command("tiktoklogin") & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_TIKTOK_LOGIN_DETAILS))
async def tiktok_login_command(client, message):
    """Handles TikTok direct login command (simulated)."""
    user_id = message.from_user.id
    user_states.pop(user_id, None)

    try:
        args = message.text.split(maxsplit=3)
        if not (3 <= len(args) <= 4):
            await message.reply("❗ **Syntax Error.** Usage: `/tiktoklogin <username_or_email> <password> [optional_proxy_url]`", reply_markup=tiktok_settings_inline_menu)
            return

        username_or_email = args[1].strip()
        password = args[2].strip()
        proxy_url = args[3].strip() if len(args) == 4 else None

        if not username_or_email or not password:
            await message.reply("❌ **Authentication Failed.** Both username/email and password are required.", reply_markup=tiktok_settings_inline_menu)
            return

        user_doc = get_user_data(user_id)
        premium_platforms = user_doc.get("premium_platforms", [])
        if "tiktok" not in premium_platforms:
            premium_platforms.append("tiktok")

        update_user_data(user_id, {
            "tiktok_settings.logged_in": True,
            "tiktok_username_or_email": username_or_email,
            "tiktok_proxy": proxy_url,
            "premium_platforms": premium_platforms,
            "is_premium": True
        })
        await message.reply("✅ **TikTok Login Simulated Successfully!** Your credentials and proxy (if provided) have been recorded. You can now use TikTok upload features.", reply_markup=tiktok_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) successfully 'logged into' TikTok (simulated, proxy: {proxy_url}). Set as premium.")

    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during TikTok login simulation: `{e}`", reply_markup=tiktok_settings_inline_menu)
        logger.error(f"Failed to process TikTok login for user {user_id}: {e}", exc_info=True)

@app.on_callback_query(filters.regex("^tiktok_set_caption$"))
async def tiktok_set_caption_prompt(client, callback_query):
    """Prompts user to set TikTok caption."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting caption input...")
    user_states[user_id] = {"step": AWAITING_TIKTOK_CAPTION}
    await callback_query.edit_message_text(
        "📝 **TikTok Caption Input Module:**\n\nPlease transmit the new TikTok caption for your uploads.\n"
        "_(Type 'skip' to use the default caption.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_tiktok')]])
    )
    logger.info(f"User {user_id} prompted for TikTok caption.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_TIKTOK_CAPTION))
async def tiktok_set_caption_save(client, message):
    """Saves the provided TikTok caption or uses default."""
    user_id = message.from_user.id
    caption = message.text.strip()
    user_states.pop(user_id, None)

    if caption.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_caption = user_doc.get("tiktok_settings", {}).get("caption", "Default TikTok Caption")
        await message.reply(f"✅ **TikTok Caption Skipped.** Using default: '{default_caption}'", reply_markup=tiktok_settings_inline_menu)
        logger.info(f"User {user_id} skipped TikTok caption, using default.")
    else:
        update_user_data(user_id, {"tiktok_settings.caption": caption})
        await message.reply(f"✅ **TikTok Caption Configured.** New caption set to: '{caption}'", reply_markup=tiktok_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set TikTok caption.")
    logger.info(f"User {user_id} saved TikTok caption.")


@app.on_callback_query(filters.regex("^tiktok_set_tag$"))
async def tiktok_set_tag_prompt(client, callback_query):
    """Prompts user to set TikTok tags."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting tag input...")
    user_states[user_id] = {"step": AWAITING_TIKTOK_TAG}
    await callback_query.edit_message_text(
        "🏷️ **TikTok Tag Input Module:**\n\nPlease transmit the new TikTok tags (e.g., `#myvideo #foryou`). Separate with spaces.\n"
        "_(Type 'skip' to use default tags.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_tiktok')]])
    )
    logger.info(f"User {user_id} prompted for TikTok tags.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_TIKTOK_TAG))
async def tiktok_set_tag_save(client, message):
    """Saves the provided TikTok tags or uses default."""
    user_id = message.from_user.id
    tag = message.text.strip()
    user_states.pop(user_id, None)

    if tag.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_tag = user_doc.get("tiktok_settings", {}).get("tag", "#tiktok #video #fyp")
        await message.reply(f"✅ **TikTok Tags Skipped.** Using default: '{default_tag}'", reply_markup=tiktok_settings_inline_menu)
        logger.info(f"User {user_id} skipped TikTok tags, using default.")
    else:
        update_user_data(user_id, {"tiktok_settings.tag": tag})
        await message.reply(f"✅ **TikTok Tags Configured.** New tags set to: '{tag}'", reply_markup=tiktok_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set TikTok tag.")
    logger.info(f"User {user_id} saved TikTok tags.")

@app.on_callback_query(filters.regex("^tiktok_video_type$"))
async def tiktok_video_type_selection(client, callback_query):
    """Displays options for TikTok video type/aspect ratio."""
    await callback_query.answer("Awaiting video type selection...")
    await callback_query.edit_message_text("🎥 **TikTok Video Type Selector:**\n\nSelect TikTok video type/aspect ratio:", reply_markup=tiktok_video_type_inline_menu)
    logger.info(f"User {callback_query.from_user.id} accessed TikTok video type selection.")

@app.on_callback_query(filters.regex("^tiktok_aspect_ratio_"))
async def tiktok_set_aspect_ratio(client, callback_query):
    """Sets the TikTok video aspect ratio."""
    user_id = callback_query.from_user.id
    aspect_ratio = "1:1 Aspect Ratio (Square)" if '1_1' in callback_query.data else "9:16 Aspect Ratio (Vertical)"
    update_user_data(user_id, {"tiktok_settings.video_type": aspect_ratio})
    await callback_query.answer(f"TikTok aspect ratio set to: {aspect_ratio}", show_alert=True)
    await callback_query.edit_message_text(
        f"✅ **TikTok Video Type Configured.** Set to: {aspect_ratio}",
        reply_markup=tiktok_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set TikTok aspect ratio to `{aspect_ratio}`.")
    logger.info(f"User {user_id} set TikTok aspect ratio to {aspect_ratio}.")

@app.on_callback_query(filters.regex("^tiktok_set_description$"))
async def tiktok_set_description_prompt(client, callback_query):
    """Prompts user to set TikTok description."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting description input...")
    user_states[user_id] = {"step": AWAITING_TIKTOK_DESCRIPTION}
    await callback_query.edit_message_text(
        "📄 **TikTok Description Input Module:**\n\nPlease transmit the new TikTok description for your uploads.\n"
        "_(Type 'skip' to use the default description.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_tiktok')]])
    )
    logger.info(f"User {user_id} prompted for TikTok description.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_TIKTOK_DESCRIPTION))
async def tiktok_set_description_save(client, message):
    """Saves the provided TikTok description or uses default."""
    user_id = message.from_user.id
    description = message.text.strip()
    user_states.pop(user_id, None)

    if description.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_description = user_doc.get("tiktok_settings", {}).get("description", "Default TikTok Description")
        await message.reply(f"✅ **TikTok Description Skipped.** Using default: '{default_description}'", reply_markup=tiktok_settings_inline_menu)
        logger.info(f"User {user_id} skipped TikTok description, using default.")
    else:
        update_user_data(user_id, {"tiktok_settings.description": description})
        await message.reply(f"✅ **TikTok Description Configured.** New description set to: '{description}'", reply_markup=tiktok_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set TikTok description.")
    logger.info(f"User {user_id} saved TikTok description.")

@app.on_callback_query(filters.regex("^tiktok_check_account_info$"))
async def tiktok_check_account_info(client, callback_query):
    """Displays current TikTok account settings."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Retrieving TikTok account status...")
    user_doc = get_user_data(user_id)
    tiktok_settings = user_doc.get("tiktok_settings", {})

    logged_in_status = "✅ Active Session" if tiktok_settings.get("logged_in") else "❌ Logged Out"
    username_or_email = tiktok_settings.get("tiktok_username_or_email", "N/A")
    proxy_info = tiktok_settings.get("tiktok_proxy", "None")
    caption = tiktok_settings.get("caption", "Not set")
    tag = tiktok_settings.get("tag", "Not set")
    video_type = tiktok_settings.get("video_type", "Not set")
    description = tiktok_settings.get("description", "Not set")

    info_text = (
        f"**🎵 TikTok Account Diagnostics:**\n"
        f"Status: {logged_in_status}\n"
        f"Registered Account: `{username_or_email}`\n"
        f"Proxy Configured: `{proxy_info}`\n"
        f"Default Caption: `{caption}`\n"
        f"Default Tags: `{tag}`\n"
        f"Default Video Type: `{video_type}`\n"
        f"Default Description: `{description}`"
    )
    await callback_query.edit_message_text(info_text, reply_markup=tiktok_settings_inline_menu, parse_mode=enums.ParseMode.MARKDOWN)
    logger.info(f"User {user_id} checked TikTok account info.")


# --- Facebook Settings Handlers ---
@app.on_callback_query(filters.regex("^fb_login_prompt$"))
async def prompt_facebook_login_from_settings(client, callback_query):
    """Prompts for Facebook login access token (User Access Token or Page Access Token)."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** You need a premium subscription to connect platforms.", show_alert=True)
        return
    await callback_query.answer("Initiating Facebook authentication protocol...")
    user_states[user_id] = {"step": AWAITING_FB_ACCESS_TOKEN}
    await callback_query.message.edit_text(
        "🔑 **Facebook Access Token Input Module:**\n\n"
        "To link your Facebook account and manage pages, please transmit your **User Access Token** "
        "(with `pages_show_list` and `pages_manage_posts` permissions) or a **Page Access Token**.\n\n"
        "**Format:** `/fblogin <your_facebook_access_token>`\n\n"
        "**Recommendation:** For full page management, acquire a User Access Token first, then exchange it for long-lived Page Access Tokens.\n"
        "_**System Note:** The bot will attempt to fetch all pages linked to the provided token and store them._",
        parse_mode=enums.ParseMode.MARKDOWN
    )
    logger.info(f"User {user_id} prompted for Facebook access token.")

@app.on_message(filters.command("fblogin") & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_ACCESS_TOKEN))
async def facebook_login_command(client, message):
    """Handles Facebook access token input, fetches pages, and saves them."""
    user_id = message.from_user.id
    user_states.pop(user_id, None)

    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.reply("❗ **Syntax Error.** Usage: `/fblogin <your_facebook_access_token>`", reply_markup=facebook_settings_inline_menu_main)
            return

        provided_access_token = args[1].strip()

        # Attempt to get pages using the provided token. This token could be a User Access Token or a Page Access Token.
        # If it's a Page Access Token, it will likely only return that one page's info.
        # If it's a User Access Token, it should list all managed pages.
        pages_url = f"https://graph.facebook.com/v19.0/me/accounts?access_token={provided_access_token}"
        response = requests.get(pages_url)
        response_data = response.json()

        if response.status_code == 200 and 'data' in response_data:
            linked_pages = []
            for page_data in response_data['data']:
                # Ensure we get the page-specific access token, which is better for publishing
                # The 'access_token' field in page_data is the page access token
                if 'access_token' in page_data and 'id' in page_data and 'name' in page_data:
                    linked_pages.append({
                        "id": page_data["id"],
                        "name": page_data["name"],
                        "access_token": page_data["access_token"]
                    })
            
            if not linked_pages:
                # If 'me/accounts' returned nothing, try to validate if the token is a direct Page Access Token
                # by fetching info about that page itself (assuming the ID from FACEBOOK_PAGE_ID for example)
                # This fallback is for when a user provides just a Page Access Token for a single page.
                # This part is complex without knowing the page ID first. A simpler approach is to tell them to provide a token that gives pages_show_list.
                # However, for the purpose of the request, if they input a valid page token, we can just save it as one page.
                
                # Let's try to get info about the token itself to see if it's a valid page token
                debug_token_url = f"https://graph.facebook.com/debug_token?input_token={provided_access_token}&access_token={FACEBOOK_APP_ID}|{FACEBOOK_APP_SECRET}"
                debug_response = requests.get(debug_token_url)
                debug_data = debug_response.json()

                if debug_response.status_code == 200 and debug_data.get('data', {}).get('is_valid') and debug_data.get('data', {}).get('type') == 'PAGE':
                    page_id = debug_data['data']['page_id']
                    # Try to fetch page name with this token
                    page_info_url = f"https://graph.facebook.com/v19.0/{page_id}?fields=name&access_token={provided_access_token}"
                    page_info_response = requests.get(page_info_url)
                    page_info_data = page_info_response.json()

                    if page_info_response.status_code == 200 and 'name' in page_info_data:
                        linked_pages.append({
                            "id": page_id,
                            "name": page_info_data["name"],
                            "access_token": provided_access_token
                        })
                        await message.reply("✅ **Facebook Page Linked!** Your page has been successfully added. You can now transmit content.", reply_markup=facebook_settings_inline_menu_main)
                        await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) linked single Facebook page: {page_info_data['name']}.")
                    else:
                        await message.reply("❌ **Facebook Token Invalid.** The provided token is valid but could not retrieve page information or it's not a page token for a known page.", reply_markup=facebook_settings_inline_menu_main)
                        logger.error(f"User {user_id} provided valid FB token, but could not get page info: {page_info_data}")
                        return
                else:
                    await message.reply("❌ **Facebook Token Invalid.** No pages found or token is not a valid User Access Token with `pages_show_list` permission, nor a valid Page Access Token. Please ensure the token has the necessary permissions.", reply_markup=facebook_settings_inline_menu_main)
                    logger.error(f"Facebook token validation failed for user {user_id}. No pages found or debug_token invalid: {response_data}, Debug: {debug_data}")
                    return

            user_doc = get_user_data(user_id)
            premium_platforms = user_doc.get("premium_platforms", [])
            if "facebook" not in premium_platforms:
                premium_platforms.append("facebook")

            update_user_data(user_id, {
                "facebook_linked_pages": linked_pages, # Store the list of pages
                "premium_platforms": premium_platforms,
                "is_premium": True
            })

            if len(linked_pages) > 0:
                 await message.reply(f"✅ **Facebook Linked Pages Synced!** Found {len(linked_pages)} page(s) linked to your account. You can now manage them.", reply_markup=facebook_settings_inline_menu_main)
                 await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) successfully linked {len(linked_pages)} Facebook pages. Set as premium.")
            else:
                 await message.reply("⚠️ **Facebook Link Warning:** Token was valid but no pages could be retrieved. Ensure it has `pages_show_list` permission.", reply_markup=facebook_settings_inline_menu_main)

        else:
            error_message = response_data.get('error', {}).get('message', 'Unknown API error')
            await message.reply(f"❌ **Authentication Failed.** Invalid or expired Facebook token. Error Code: `{response_data.get('error', {}).get('code', 'N/A')}`, Message: `{error_message}`", reply_markup=facebook_settings_inline_menu_main)
            logger.error(f"Facebook token validation failed for user {user_id}: {response_data}")

    except requests.exceptions.RequestException as req_e:
        await message.reply(f"❌ **Network Error.** Could not connect to Facebook API: `{req_e}`", reply_markup=facebook_settings_inline_menu_main)
        logger.error(f"Network error during FB login for user {user_id}: {req_e}", exc_info=True)
    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during Facebook login procedure: `{e}`", reply_markup=facebook_settings_inline_menu_main)
        logger.error(f"Failed to process Facebook login for user {user_id}: {e}", exc_info=True)


@app.on_callback_query(filters.regex("^fb_set_title$"))
async def fb_set_title_prompt(client, callback_query):
    """Prompts user to set Facebook title."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting title input...")
    user_states[user_id] = {"step": AWAITING_FB_TITLE}
    await callback_query.message.edit_text(
        "📝 **Facebook Title Input Module:**\n\nPlease transmit the new Facebook video title.\n"
        "_(Type 'skip' to use the default title.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook title.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_TITLE))
async def fb_set_title_save(client, message):
    """Saves the provided Facebook title or uses default."""
    user_id = message.from_user.id
    title = message.text.strip()
    user_states.pop(user_id, None)

    if title.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_title = user_doc.get("facebook_settings", {}).get("title", "Default Facebook Title")
        await message.reply(f"✅ **Facebook Title Skipped.** Using default: '{default_title}'", reply_markup=facebook_settings_inline_menu_main)
        logger.info(f"User {user_id} skipped Facebook title, using default.")
    else:
        update_user_data(user_id, {"facebook_settings.title": title})
        await message.reply(f"✅ **Facebook Title Configured.** New title set to: '{title}'", reply_markup=facebook_settings_inline_menu_main)
        await log_to_channel(client, f"User `{user_id}` set Facebook title.")
    logger.info(f"User {user_id} saved Facebook title.")

@app.on_callback_query(filters.regex("^fb_set_tag$"))
async def fb_set_tag_prompt(client, callback_query):
    """Prompts user to set Facebook tags."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting tag input...")
    user_states[user_id] = {"step": AWAITING_FB_TAG}
    await callback_query.message.edit_text(
        "🏷️ **Facebook Tag Input Module:**\n\nPlease transmit the new Facebook tags (e.g., `#reels #video`). Separate with spaces.\n"
        "_(Type 'skip' to use default tags.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook tags.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_TAG))
async def fb_set_tag_save(client, message):
    """Saves the provided Facebook tags or uses default."""
    user_id = message.from_user.id
    tag = message.text.strip()
    user_states.pop(user_id, None)

    if tag.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_tag = user_doc.get("facebook_settings", {}).get("tag", "#facebook #video #reels")
        await message.reply(f"✅ **Facebook Tags Skipped.** Using default: '{default_tag}'", reply_markup=facebook_settings_inline_menu_main)
        logger.info(f"User {user_id} skipped Facebook tags, using default.")
    else:
        update_user_data(user_id, {"facebook_settings.tag": tag})
        await message.reply(f"✅ **Facebook Tags Configured.** New tags set to: '{tag}'", reply_markup=facebook_settings_inline_menu_main)
        await log_to_channel(client, f"User `{user_id}` set Facebook tag.")
    logger.info(f"User {user_id} saved Facebook tags.")

@app.on_callback_query(filters.regex("^fb_set_description$"))
async def fb_set_description_prompt(client, callback_query):
    """Prompts user to set Facebook description."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting description input...")
    user_states[user_id] = {"step": AWAITING_FB_DESCRIPTION}
    await callback_query.message.edit_text(
        "📄 **Facebook Description Input Module:**\n\nPlease transmit the new Facebook description for your uploads.\n"
        "_(Type 'skip' to use the default description.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook description.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_DESCRIPTION))
async def fb_set_description_save(client, message):
    """Saves the provided Facebook description or uses default."""
    user_id = message.from_user.id
    description = message.text.strip()
    user_states.pop(user_id, None)

    if description.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_description = user_doc.get("facebook_settings", {}).get("description", "Default Facebook Description")
        await message.reply(f"✅ **Facebook Description Skipped.** Using default: '{default_description}'", reply_markup=facebook_settings_inline_menu_main)
        logger.info(f"User {user_id} skipped Facebook description, using default.")
    else:
        update_user_data(user_id, {"facebook_settings.description": description})
        await message.reply(f"✅ **Facebook Description Configured.** New description set to: '{description}'", reply_markup=facebook_settings_inline_menu_main)
        await log_to_channel(client, f"User `{user_id}` set Facebook description.")
    logger.info(f"User {user_id} saved Facebook description.")


@app.on_callback_query(filters.regex("^fb_video_type$"))
async def fb_video_type_selection(client, callback_query):
    """Displays options for Facebook video type."""
    await callback_query.answer("Awaiting video type selection...")
    await callback_query.message.edit_text("🎥 **Facebook Video Type Selector:**\n\nSelect Facebook content type:", reply_markup=facebook_video_type_inline_menu)
    logger.info(f"User {callback_query.from_user.id} accessed Facebook video type selection.")

@app.on_callback_query(filters.regex("^fb_video_type_"))
async def fb_set_video_type(client, callback_query):
    """Sets the Facebook video type."""
    user_id = callback_query.from_user.id
    video_type = "Reels (Short Vertical Video)" if 'reels' in callback_query.data else "Video (Standard Horizontal/Square)"
    update_user_data(user_id, {"facebook_settings.video_type": video_type})
    await callback_query.answer(f"Facebook video type set to: {video_type}", show_alert=True)
    await callback_query.message.edit_text(
        f"✅ **Facebook Video Type Configured.** Set to: {video_type}",
        reply_markup=facebook_settings_inline_menu_main
    )
    await log_to_channel(client, f"User `{user_id}` set Facebook video type to `{video_type}`.")
    logger.info(f"User {user_id} set Facebook video type to {video_type}.")

@app.on_callback_query(filters.regex("^fb_set_schedule_time$"))
async def fb_set_schedule_time_prompt(client, callback_query):
    """Prompts user to set Facebook schedule time."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting schedule time input...")
    user_states[user_id] = {"step": AWAITING_FB_SCHEDULE_TIME}
    await callback_query.message.edit_text(
        "⏰ **Facebook Schedule Configuration Module:**\n\nPlease transmit the desired schedule date and time.\n"
        "**Format:** `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`)\n"
        "_**System Note:** Time will be interpreted in UTC._\n"
        "_(Type 'clear' to remove any existing schedule.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook schedule time.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_SCHEDULE_TIME))
async def fb_set_schedule_time_save(client, message):
    """Saves the provided Facebook schedule time or clears it."""
    user_id = message.from_user.id
    schedule_str = message.text.strip()
    user_states.pop(user_id, None)

    if schedule_str.lower() == "clear":
        update_user_data(user_id, {"facebook_settings.schedule_time": None})
        await message.reply("✅ **Facebook Schedule Cleared.** Your uploads will now publish immediately (unless privacy is Draft).", reply_markup=facebook_settings_inline_menu_main)
        await log_to_channel(client, f"User `{user_id}` cleared Facebook schedule time.")
        logger.info(f"User {user_id} cleared Facebook schedule time.")
        return

    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")
        if schedule_dt <= datetime.utcnow() + timedelta(minutes=5):
            await message.reply("❌ **Time Constraint Violation.** Schedule time must be at least 5 minutes in the future. Please try again with a later time.")
            return

        update_user_data(user_id, {"facebook_settings.schedule_time": schedule_dt.isoformat()})
        await message.reply(f"✅ **Facebook Schedule Configured.** Content set for transmission at: '{schedule_str}' (UTC)", reply_markup=facebook_settings_inline_menu_main)
        await log_to_channel(client, f"User `{user_id}` set Facebook schedule time to `{schedule_str}`.")
        logger.info(f"User {user_id} set Facebook schedule time to {schedule_str}.")
    except ValueError:
        await message.reply("❌ **Input Error.** Invalid date/time format. Please use `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`).")
        logger.warning(f"User {user_id} provided invalid Facebook schedule time format: {schedule_str}")
    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** An error occurred while parsing schedule time: `{e}`")
        logger.error(f"Error parsing Facebook schedule time for user {user_id}: {e}")

@app.on_callback_query(filters.regex("^fb_set_privacy$"))
async def fb_set_privacy_selection(client, callback_query):
    """Displays options for Facebook privacy setting."""
    await callback_query.answer("Awaiting privacy selection...")
    await callback_query.message.edit_text("🔒 **Facebook Privacy Configuration Module:**\n\nSelect Facebook privacy setting:", reply_markup=get_privacy_inline_menu('fb'))
    logger.info(f"User {callback_query.from_user.id} accessed Facebook privacy selection.")

@app.on_callback_query(filters.regex("^fb_privacy_"))
async def fb_set_privacy(client, callback_query):
    """Sets the Facebook privacy setting."""
    user_id = callback_query.from_user.id
    privacy = "Public" if 'public' in callback_query.data else ("Private" if 'private' in callback_query.data else "Draft")
    update_user_data(user_id, {"facebook_settings.privacy": privacy})
    await callback_query.answer(f"Facebook privacy set to: {privacy}", show_alert=True)
    await callback_query.message.edit_text(
        f"✅ **Facebook Privacy Configured.** Set to: {privacy}",
        reply_markup=facebook_settings_inline_menu_main
    )
    await log_to_channel(client, f"User `{user_id}` set Facebook privacy to `{privacy}`.")
    logger.info(f"User {user_id} set Facebook privacy to {privacy}.")

@app.on_callback_query(filters.regex("^fb_check_expiry_date$"))
async def fb_check_expiry_date(client, callback_query):
    """Displays current Facebook token expiry date (placeholder)."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Retrieving Facebook token expiry data...")
    user_doc = get_user_data(user_id)
    # This now checks for linked pages and mentions general token validity for them
    linked_pages = user_doc.get("facebook_linked_pages", [])
    
    info_text = "**🗓️ Facebook Token Expiry Status:**\n\n"
    if linked_pages:
        info_text += "Your linked Page Access Tokens are generally long-lived and typically do not expire unless:\n"
        info_text += "• The user changes their password.\n"
        info_text += "• The user revokes the app's permissions.\n"
        info_text += "• The user's account is compromised.\n\n"
        info_text += "To refresh or re-check, you can use '🔑 Link/Refresh Facebook Pages'."
    else:
        info_text += "_No Facebook pages are currently linked to your account. No token expiry information available._"

    await callback_query.message.edit_text(info_text, reply_markup=facebook_settings_inline_menu_main, parse_mode=enums.ParseMode.MARKDOWN)
    logger.info(f"User {user_id} checked Facebook expiry date.")


# --- YouTube Settings Handlers ---
@app.on_callback_query(filters.regex("^yt_login_prompt$"))
async def yt_login_prompt(client, callback_query):
    """Prompts for YouTube login access token (placeholder)."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** You need a premium subscription to connect platforms.", show_alert=True)
        return
    await callback_query.answer("Initiating YouTube authentication protocol...")
    user_states[user_id] = {"step": AWAITING_YT_ACCESS_TOKEN}
    await callback_query.message.edit_text(
        "🔑 **YouTube Access Token Input Module:**\n\n"
        "To establish a connection to YouTube, please transmit your **YouTube Access Token**.\n\n"
        "**Format:** `/youtubelogin <your_youtube_access_token>`\n\n"
        "_**System Note:** This is a placeholder for a real YouTube OAuth 2.0 flow. Obtaining YouTube access tokens securely usually involves a multi-step web authentication process._",
        parse_mode=enums.ParseMode.MARKDOWN
    )
    logger.info(f"User {user_id} prompted for YouTube access token.")

@app.on_message(filters.command("youtubelogin") & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_ACCESS_TOKEN))
async def youtube_login_command(client, message):
    """Handles YouTube access token input and saves it (simulated)."""
    user_id = message.from_user.id
    user_states.pop(user_id, None)

    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.reply("❗ **Syntax Error.** Usage: `/youtubelogin <your_youtube_access_token>`", reply_markup=youtube_settings_inline_menu)
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
                "is_premium": True
            })
            await message.reply("✅ **YouTube Login Simulated Successfully!** Token recorded. Connection established.", reply_markup=youtube_settings_inline_menu)
            await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) successfully 'logged into' YouTube (simulated). Set as premium.")
        else:
            await message.reply("❌ **Authentication Failed.** Invalid token provided.", reply_markup=youtube_settings_inline_menu)
            logger.error(f"YouTube token validation failed for user {user_id}: Empty token provided.")
    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during YouTube login procedure: `{e}`", reply_markup=youtube_settings_inline_menu)
        logger.error(f"Failed to process YouTube login for user {user_id}: {e}", exc_info=True)

@app.on_callback_query(filters.regex("^yt_set_title$"))
async def yt_set_title_prompt(client, callback_query):
    """Prompts user to set YouTube title."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting title input...")
    user_states[user_id] = {"step": AWAITING_YT_TITLE}
    await callback_query.message.edit_text(
        "📝 **YouTube Title Input Module:**\n\nPlease transmit the new YouTube video title.\n"
        "_(Type 'skip' to use the default title.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_youtube')]])
    )
    logger.info(f"User {user_id} prompted for YouTube title.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_TITLE))
async def yt_set_title_save(client, message):
    """Saves the provided YouTube title or uses default."""
    user_id = message.from_user.id
    title = message.text.strip()
    user_states.pop(user_id, None)

    if title.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_title = user_doc.get("youtube_settings", {}).get("title", "Default YouTube Title")
        await message.reply(f"✅ **YouTube Title Skipped.** Using default: '{default_title}'", reply_markup=youtube_settings_inline_menu)
        logger.info(f"User {user_id} skipped YouTube title, using default.")
    else:
        update_user_data(user_id, {"youtube_settings.title": title})
        await message.reply(f"✅ **YouTube Title Configured.** New title set to: '{title}'", reply_markup=youtube_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set YouTube title.")
    logger.info(f"User {user_id} saved YouTube title.")

@app.on_callback_query(filters.regex("^yt_set_tag$"))
async def yt_set_tag_prompt(client, callback_query):
    """Prompts user to set YouTube tags."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting tag input...")
    user_states[user_id] = {"step": AWAITING_YT_TAG}
    await callback_query.message.edit_text(
        "🏷️ **YouTube Tag Input Module:**\n\nPlease transmit the new YouTube tags (e.g., `#shorts #video`). Separate with spaces.\n"
        "_(Type 'skip' to use default tags.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_youtube')]])
    )
    logger.info(f"User {user_id} prompted for YouTube tags.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_TAG))
async def yt_set_tag_save(client, message):
    """Saves the provided YouTube tags or uses default."""
    user_id = message.from_user.id
    tag = message.text.strip()
    user_states.pop(user_id, None)

    if tag.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_tag = user_doc.get("youtube_settings", {}).get("tag", "#youtube #video #shorts")
        await message.reply(f"✅ **YouTube Tags Skipped.** Using default: '{default_tag}'", reply_markup=youtube_settings_inline_menu)
        logger.info(f"User {user_id} skipped YouTube tags, using default.")
    else:
        update_user_data(user_id, {"youtube_settings.tag": tag})
        await message.reply(f"✅ **YouTube Tags Configured.** New tags set to: '{tag}'", reply_markup=youtube_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set YouTube tag.")
    logger.info(f"User {user_id} saved YouTube tags.")

@app.on_callback_query(filters.regex("^yt_set_description$"))
async def yt_set_description_prompt(client, callback_query):
    """Prompts user to set YouTube description."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting description input...")
    user_states[user_id] = {"step": AWAITING_YT_DESCRIPTION}
    await callback_query.message.edit_text(
        "📄 **YouTube Description Input Module:**\n\nPlease transmit the new YouTube description.\n"
        "_(Type 'skip' to use the default description.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_youtube')]])
    )
    logger.info(f"User {user_id} prompted for YouTube description.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_DESCRIPTION))
async def yt_set_description_save(client, message):
    """Saves the provided YouTube description or uses default."""
    user_id = message.from_user.id
    description = message.text.strip()
    user_states.pop(user_id, None)

    if description.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_description = user_doc.get("youtube_settings", {}).get("description", "Default YouTube Description")
        await message.reply(f"✅ **YouTube Description Skipped.** Using default: '{default_description}'", reply_markup=youtube_settings_inline_menu)
        logger.info(f"User {user_id} skipped YouTube description, using default.")
    else:
        update_user_data(user_id, {"youtube_settings.description": description})
        await message.reply(f"✅ **YouTube Description Configured.** New description set to: '{description}'", reply_markup=youtube_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set YouTube description.")
    logger.info(f"User {user_id} saved YouTube description.")

@app.on_callback_query(filters.regex("^yt_video_type$"))
async def yt_video_type_selection(client, callback_query):
    """Displays options for YouTube video type."""
    await callback_query.answer("Awaiting video type selection...")
    await callback_query.message.edit_text("🎥 **YouTube Video Type Selector:**\n\nSelect YouTube content type:", reply_markup=youtube_video_type_inline_menu)
    logger.info(f"User {callback_query.from_user.id} accessed YouTube video type selection.")

@app.on_callback_query(filters.regex("^yt_video_type_"))
async def yt_set_video_type(client, callback_query):
    """Sets the YouTube video type."""
    user_id = callback_query.from_user.id
    video_type = "Shorts (Short Vertical Video)" if 'shorts' in callback_query.data else "Video (Standard Horizontal/Square)"
    update_user_data(user_id, {"youtube_settings.video_type": video_type})
    await callback_query.answer(f"YouTube video type set to: {video_type}", show_alert=True)
    await callback_query.message.edit_text(
        f"✅ **YouTube Video Type Configured.** Set to: {video_type}",
        reply_markup=youtube_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set YouTube video type to `{video_type}`.")
    logger.info(f"User {user_id} set YouTube video type to {video_type}.")

@app.on_callback_query(filters.regex("^yt_set_schedule_time$"))
async def yt_set_schedule_time_prompt(client, callback_query):
    """Prompts user to set YouTube schedule time."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting schedule time input...")
    user_states[user_id] = {"step": AWAITING_YT_SCHEDULE_TIME}
    await callback_query.message.edit_text(
        "⏰ **YouTube Schedule Configuration Module:**\n\nPlease transmit the desired schedule date and time.\n"
        "**Format:** `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`)\n"
        "_**System Note:** Time will be interpreted in UTC._\n"
        "_(Type 'clear' to remove any existing schedule.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_youtube')]])
    )
    logger.info(f"User {user_id} prompted for YouTube schedule time.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_SCHEDULE_TIME))
async def yt_set_schedule_time_save(client, message):
    """Saves the provided YouTube schedule time or clears it."""
    user_id = message.from_user.id
    schedule_str = message.text.strip()
    user_states.pop(user_id, None)

    if schedule_str.lower() == "clear":
        update_user_data(user_id, {"youtube_settings.schedule_time": None})
        await message.reply("✅ **YouTube Schedule Cleared.** Your uploads will now publish immediately (unless privacy is Private/Unlisted).", reply_markup=youtube_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` cleared YouTube schedule time.")
        logger.info(f"User {user_id} cleared YouTube schedule time.")
        return

    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")
        if schedule_dt <= datetime.utcnow() + timedelta(minutes=5):
            await message.reply("❌ **Time Constraint Violation.** Schedule time must be at least 5 minutes in the future. Please try again with a later time.")
            return

        update_user_data(user_id, {"youtube_settings.schedule_time": schedule_dt.isoformat()})
        await message.reply(f"✅ **YouTube Schedule Configured.** Content set for transmission at: '{schedule_str}' (UTC)", reply_markup=youtube_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set YouTube schedule time to `{schedule_str}`.")
        logger.info(f"User {user_id} set YouTube schedule time to {schedule_str}.")
    except ValueError:
        await message.reply("❌ **Input Error.** Invalid date/time format. Please use `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`).")
        logger.warning(f"User {user_id} provided invalid YouTube schedule time format: {schedule_str}")
    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** An error occurred while parsing schedule time: `{e}`")
        logger.error(f"Error parsing YouTube schedule time for user {user_id}: {e}")

@app.on_callback_query(filters.regex("^yt_set_privacy$"))
async def yt_set_privacy_selection(client, callback_query):
    """Displays options for YouTube privacy setting."""
    await callback_query.answer("Awaiting privacy selection...")
    await callback_query.message.edit_text("🔒 **YouTube Privacy Configuration Module:**\n\nSelect YouTube privacy setting:", reply_markup=get_privacy_inline_menu('yt'))
    logger.info(f"User {callback_query.from_user.id} accessed YouTube privacy selection.")

@app.on_callback_query(filters.regex("^yt_privacy_"))
async def yt_set_privacy(client, callback_query):
    """Sets the YouTube privacy setting."""
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
    await callback_query.message.edit_text(
        f"✅ **YouTube Privacy Configured.** Set to: {privacy}",
        reply_markup=youtube_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set YouTube privacy to `{privacy}`.")
    logger.info(f"User {user_id} set YouTube privacy to {privacy}.")

@app.on_callback_query(filters.regex("^yt_check_expiry_date$"))
async def yt_check_expiry_date(client, callback_query):
    """Displays current YouTube token expiry date (placeholder)."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Retrieving YouTube token expiry data...")
    user_doc = get_user_data(user_id)
    yt_settings = user_doc.get("youtube_settings", {})
    expiry_date = yt_settings.get("expiry_date", "Not Integrated (Requires OAuth Token Refresh)")
    await callback_query.message.edit_text(f"🗓️ **YouTube Token Expiry Status:** `{expiry_date}`\n\n_**System Note:** YouTube OAuth 2.0 access tokens are typically short-lived and require a refresh token mechanism for persistent access. This is not fully implemented._", reply_markup=youtube_settings_inline_menu, parse_mode=enums.ParseMode.MARKDOWN)
    logger.info(f"User {user_id} checked YouTube expiry date.")


# --- Upload Flow Handlers ---

@app.on_message(filters.text & filters.regex("^📤 Upload Video (Facebook)$"))
async def upload_facebook_video_prompt(client, message):
    """Initiates the Facebook video upload process, first by checking pages."""
    user_id = message.chat.id
    user_doc = get_user_data(user_id)
    if not user_doc:
        await message.reply("⛔ **Access Denied!** Please send `/start` first to initialize your account in the system.")
        return

    if not is_premium_user(user_id) and not is_admin(user_id):
        await message.reply("❌ **Access Restricted.** You need **PREMIUM ACCESS** to use the Facebook upload feature. Please contact the administrator to upgrade your privileges.")
        return

    linked_pages = user_doc.get("facebook_linked_pages", [])
    if not linked_pages:
        await message.reply("❌ **Authentication Required.** No Facebook pages are linked to your account. Please navigate to `⚙️ Settings` -> `📘 Facebook Settings` -> `🔑 Link/Refresh Facebook Pages` to link your pages first.",
                            reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        return

    # If there are linked pages, prompt the user to select one
    user_states[user_id] = {"step": AWAITING_FB_PAGE_SELECTION_UPLOAD, "platform": "facebook"}
    
    # Create dynamic buttons for linked pages
    page_buttons = []
    for page in linked_pages:
        page_buttons.append([InlineKeyboardButton(page["name"], callback_data=f"upload_select_fb_page_{page['id']}")])
    page_buttons.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_main_menu_reply_from_inline')])

    await message.reply(
        "🗂️ **Facebook Page Selection.** Please select the Facebook page to which you wish to transmit your content:",
        reply_markup=InlineKeyboardMarkup(page_buttons)
    )
    logger.info(f"User {user_id} prompted to select Facebook page for upload.")

@app.on_callback_query(filters.regex("^upload_select_fb_page_"))
async def select_fb_page_for_upload(client, callback_query):
    """Handles selection of a Facebook page for video upload."""
    user_id = callback_query.from_user.id
    page_id_to_upload = callback_query.data.split("_")[4] # Extract page ID

    user_doc = get_user_data(user_id)
    linked_pages = user_doc.get("facebook_linked_pages", [])
    selected_page_info = next((page for page in linked_pages if page["id"] == page_id_to_upload), None)

    if selected_page_info:
        # Store selected page info in user_states for the current upload session
        user_states[user_id] = {
            "step": AWAITING_UPLOAD_VIDEO,
            "platform": "facebook",
            "fb_page_id": selected_page_info["id"],
            "fb_page_name": selected_page_info["name"],
            "fb_page_access_token": selected_page_info["access_token"]
        }
        await callback_query.answer(f"Selected page: {selected_page_info['name']}", show_alert=True)
        await callback_query.message.edit_text(
            f"✅ **Page Selected:** `{selected_page_info['name']}`\n\n"
            "🎥 **Content Transmission Protocol Active.** Please transmit your video file for Facebook now.",
            reply_markup=None # Remove inline keyboard
        )
        await client.send_message(user_id, "You can use '🔙 Main Menu' to abort the transmission.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin) # Keep reply keyboard
        logger.info(f"User {user_id} selected Facebook page {page_id_to_upload} ({selected_page_info['name']}) for upload.")
    else:
        await callback_query.answer("Error: Selected page not found in your linked accounts. Please try linking your pages again.", show_alert=True)
        logger.warning(f"User {user_id} attempted to select non-existent FB page {page_id_to_upload} for upload.")
        # Re-prompt page selection or go back to main menu
        await callback_query.message.edit_text(
            "❌ **Page Not Found.** Please select an existing page or link a new one.",
            reply_markup=get_facebook_page_selection_markup(user_id, for_upload=True)
        )


@app.on_message(filters.video)
async def handle_video_upload(client, message):
    """Handles incoming video files for upload."""
    user_id = message.chat.id
    if not get_user_data(user_id):
        await message.reply("⛔ **Access Denied!** Please send `/start` first to initialize your account in the system.")
        return

    state = user_states.get(user_id)
    # Ensure the state is AWAITING_UPLOAD_VIDEO and platform is Facebook
    if not state or not (state.get("step") == AWAITING_UPLOAD_VIDEO and state.get("platform") == "facebook"):
        await message.reply("❗ **Invalid Operation.** Please initiate an upload process by clicking a dedicated upload button (e.g., '📤 Upload Video (Facebook)') first and select a page.",
                            reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        logger.warning(f"User {user_id} sent video without active Facebook upload state or selected page.")
        return

    if not is_premium_user(user_id) and not is_admin(user_id):
        await message.reply("❌ **Access Restricted.** You need **PREMIUM ACCESS** to upload video content. Please contact the administrator.")
        user_states.pop(user_id, None)
        logger.warning(f"Non-premium user {user_id} attempted video upload.")
        return

    # Critical check: Ensure page details are in state
    if not state.get("fb_page_id") or not state.get("fb_page_access_token"):
        await message.reply("❌ **Facebook Page Not Selected.** Please restart the Facebook upload process and select a page.",
                            reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        user_states.pop(user_id, None)
        logger.error(f"User {user_id} attempting upload without selected Facebook page details in state.")
        return

    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Created 'downloads' directory for video processing.")

    initial_status_msg = await message.reply("⏳ **Data Acquisition In Progress...** Downloading your video. This operation may require significant processing time for large data files.")
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        file_extension = os.path.splitext(message.video.file_name or "video")[1]
        download_filename = f"downloads/{user_id}_{timestamp}{file_extension}"
        
        file_path = await message.download(file_name=download_filename)
        
        user_states[user_id]["file_path"] = file_path
        user_states[user_id]["step"] = AWAITING_UPLOAD_TITLE
        
        user_doc = get_user_data(user_id)
        default_title = user_doc.get(f"{state['platform']}_settings", {}).get("title", "Default Title")
        
        await initial_status_msg.edit_text(
            f"📝 **Metadata Input Required.** Now, transmit the **title** for your `{state['platform'].capitalize()}` video.\n"
            f"_(Type 'skip' to use your default title: '{default_title}')_"
        )
        logger.info(f"User {user_id} video downloaded to {file_path}. Awaiting title for {state.get('fb_page_name', 'selected page')}.")
    except Exception as e:
        await initial_status_msg.edit_text(f"❌ **Data Acquisition Failed.** Error downloading video: `{e}`")
        logger.error(f"Failed to download video for user {user_id}: {e}", exc_info=True)
        user_states.pop(user_id, None)

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_TITLE))
async def handle_upload_title(client, message):
    """Handles input for video title."""
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state:
        await message.reply("❌ **Session Interrupted.** Please restart the upload process.")
        return

    title_input = message.text.strip()
    platform = state["platform"]
    user_doc = get_user_data(user_id)
    
    if title_input.lower() == "skip":
        state["title"] = user_doc.get(f"{platform}_settings", {}).get("title", "Default Title")
        await message.reply(f"✅ **Title Input Skipped.** Using default title: '{state['title']}'.")
        logger.info(f"User {user_id} skipped title input for {platform}.")
    else:
        state["title"] = title_input
        await message.reply(f"✅ **Title Recorded.** New title: '{title_input}'.")
        logger.info(f"User {user_id} provided title for {platform}: '{title_input}'.")

    user_states[user_id]["step"] = AWAITING_UPLOAD_DESCRIPTION

    default_description = user_doc.get(f"{platform}_settings", {}).get("description", "Default Description")

    await message.reply(
        f"📝 **Metadata Input Required.** Now, transmit a **description** for your `{platform.capitalize()}` video.\n"
        f"_(Type 'skip' to use your default description: '{default_description}')_"
    )
    logger.info(f"User {user_id} awaiting description for {platform}.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_DESCRIPTION))
async def handle_upload_description(client, message):
    """Handles input for video description."""
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state:
        await message.reply("❌ **Session Interrupted.** Please restart the upload process.")
        return

    description_input = message.text.strip()
    platform = state["platform"]
    user_doc = get_user_data(user_id)

    if description_input.lower() == "skip":
        state["description"] = user_doc.get(f"{platform}_settings", {}).get("description", "Default Description")
        await message.reply(f"✅ **Description Input Skipped.** Using default description: '{state['description']}'.")
        logger.info(f"User {user_id} skipped description input for {platform}.")
    else:
        state["description"] = description_input
        await message.reply(f"✅ **Description Recorded.** New description: '{description_input}'.")
        logger.info(f"User {user_id} provided description for {platform}: '{description_input}'.")

    user_states[user_id]["step"] = AWAITING_UPLOAD_VISIBILITY

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Public", callback_data="visibility_public")],
            [InlineKeyboardButton("Private", callback_data="visibility_private")],
            [InlineKeyboardButton("Draft", callback_data="visibility_draft")]
        ]
    )
    await message.reply("🌐 **Visibility Configuration Module.** Select content visibility:", reply_markup=keyboard)
    logger.info(f"User {user_id} awaiting visibility choice for {platform}.")


@app.on_callback_query(filters.regex("^visibility_"))
async def handle_visibility_selection(client, callback_query):
    """Handles selection for video visibility."""
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or state.get("step") != AWAITING_UPLOAD_VISIBILITY:
        await callback_query.answer("❗ **Invalid Operation.** Please ensure you are in an active upload sequence.", show_alert=True)
        return

    platform = state["platform"]
    visibility_choice = callback_query.data.split("_")[1]

    state["visibility"] = visibility_choice
    user_states[user_id]["step"] = AWAITING_UPLOAD_SCHEDULE

    await callback_query.answer(f"Visibility set to: {visibility_choice.capitalize()}", show_alert=True)
    logger.info(f"User {user_id} set visibility for {platform} to {visibility_choice}.")

    if platform == "facebook":
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Publish Now", callback_data="schedule_now")],
                [InlineKeyboardButton("Schedule Later", callback_data="schedule_later")]
            ]
        )
        await callback_query.message.edit_text("⏰ **Content Release Protocol.** Do you wish to publish now or schedule for later?", reply_markup=keyboard)
        logger.info(f"User {user_id} awaiting schedule choice for {platform}.")
    else:
        await callback_query.message.edit_text("⏳ **Data Processing Initiated...** Preparing your content for transmission. Please standby.")
        await callback_query.answer("Processing initiated.")
        await initiate_upload(client, callback_query.message, user_id)
        logger.info(f"User {user_id} skipping schedule for {platform}, initiating upload.")


@app.on_callback_query(filters.regex("^schedule_"))
async def handle_schedule_selection(client, callback_query):
    """Handles selection for video scheduling (now or later)."""
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or state.get("step") != AWAITING_UPLOAD_SCHEDULE:
        await callback_query.answer("❗ **Invalid Operation.** Please ensure you are in an active upload sequence.", show_alert=True)
        return

    schedule_choice = callback_query.data.split("_")[1]
    platform = state["platform"]

    if schedule_choice == "now":
        state["schedule_time"] = None
        await callback_query.answer("Publishing now selected.", show_alert=True)
        await callback_query.message.edit_text("⏳ **Data Processing Initiated...** Preparing your content for immediate transmission. Please standby.")
        await initiate_upload(client, callback_query.message, user_id)
        logger.info(f"User {user_id} chose 'publish now' for {platform}.")
    elif schedule_choice == "later":
        user_states[user_id]["step"] = AWAITING_UPLOAD_SCHEDULE_DATETIME
        await callback_query.answer("Awaiting schedule time input...", show_alert=True)
        await callback_query.message.edit_text(
            "📅 **Temporal Configuration Module.** Please transmit the desired schedule date and time.\n"
            "**Format:** `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`)\n"
            "_**System Note:** Time will be interpreted in UTC._"
        )
        logger.info(f"User {user_id} chose 'schedule later' for {platform}.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_SCHEDULE_DATETIME))
async def handle_schedule_datetime_input(client, message):
    """Handles input for specific schedule date and time."""
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state:
        await message.reply("❌ **Session Interrupted.** Please restart the upload process.")
        return

    schedule_str = message.text.strip()
    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")

        if schedule_dt <= datetime.utcnow() + timedelta(minutes=5):
            await message.reply("❌ **Time Constraint Violation.** Schedule time must be at least 5 minutes in the future. Please transmit a later time.")
            return

        state["schedule_time"] = schedule_dt
        user_states[user_id]["step"] = "processing_and_uploading"
        await message.reply("⏳ **Data Processing Initiated...** Preparing your content for scheduled transmission. Please standby.")
        await initiate_upload(client, message, user_id)
        logger.info(f"User {user_id} provided schedule datetime for {state['platform']}: {schedule_str}.")

    except ValueError:
        await message.reply("❌ **Input Error.** Invalid date/time format. Please use `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`).")
        logger.warning(f"User {user_id} provided invalid schedule datetime format: {schedule_str}")
    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** An error occurred while processing schedule time: `{e}`")
        logger.error(f"Error processing schedule time for user {user_id}: {e}")


async def initiate_upload(client, message, user_id):
    """Initiates the actual video upload to the chosen platform."""
    state = user_states.get(user_id)
    if not state:
        await client.send_message(user_id, "❌ **Upload Process Aborted.** Session state lost. Please re-initiate the content transmission protocol.")
        logger.error(f"Upload initiated without valid state for user {user_id}.")
        return

    platform = state["platform"]
    file_path = state.get("file_path")
    title = state.get("title")
    description = state.get("description")
    visibility = state.get("visibility", "public")
    schedule_time = state.get("schedule_time")

    if not all([file_path, title, description]):
        await client.send_message(user_id, "❌ **Upload Protocol Failure.** Missing essential content metadata (file, title, or description). Please restart the upload sequence.")
        logger.error(f"Missing essential upload data for user {user_id}. State: {state}")
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        user_states.pop(user_id, None)
        return

    user_states[user_id]["step"] = "processing_and_uploading"
    await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO)
    await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) initiating upload for {platform}. File: `{os.path.basename(file_path)}`. Visibility: `{visibility}`. Schedule: `{schedule_time}`.")

    processed_file_path = file_path

    try:
        input_ext = os.path.splitext(file_path)[1].lower()
        
        if input_ext not in [".mp4", ".mov", ".mkv"]:
            target_ext = ".mp4"
            processed_file_path = f"downloads/processed_{user_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}{target_ext}"
            await client.send_message(user_id, f"🔄 **Data Conversion Protocol.** Converting video to {target_ext.upper()} format. Please standby...")
            await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO)
            
            def do_processing_sync():
                return convert_video_to_mp4(file_path, processed_file_path)
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(do_processing_sync)
                processed_file_path = future.result(timeout=600)
            
            await client.send_message(user_id, "✅ **Video Data Conversion Complete.**")
            await log_to_channel(client, f"User `{user_id}` video converted. Original: `{os.path.basename(file_path)}`, Processed: `{os.path.basename(processed_file_path)}`.")
        else:
            await client.send_message(user_id, "✅ **Video Format Verified.** Proceeding with direct transmission.")
            logger.info(f"User {user_id} video already suitable format for {platform}. Skipping conversion.")

        if platform == "facebook":
            # Retrieve the specific page_id and page_access_token from the user_states
            fb_page_id = state.get("fb_page_id")
            fb_page_access_token = state.get("fb_page_access_token")

            if not fb_page_id or not fb_page_access_token:
                await client.send_message(user_id, "❌ **Facebook Page Details Missing.** Please restart the Facebook upload process and ensure a page is selected.",
                                            reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
                user_states.pop(user_id, None) # Clear state as critical info is missing
                return # Exit early

            await client.send_message(user_id, f"📤 **Initiating Facebook Content Transmission to '{state.get('fb_page_name', 'selected page')}'...**")
            await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO)

            def upload_to_facebook_sync():
                return upload_facebook_video(processed_file_path, title, description, fb_page_access_token, fb_page_id, visibility=visibility, schedule_time=schedule_time)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(upload_to_facebook_sync)
                fb_result = future.result(timeout=900)

            if fb_result and 'id' in fb_result:
                status_text = "Scheduled" if schedule_time else "Published"
                await client.send_message(user_id, f"✅ **Facebook Content Transmitted!** Video ID: `{fb_result['id']}`. Status: `{status_text}` on page `{state.get('fb_page_name', 'selected page')}`.")
                users_collection.update_one({"_id": user_id}, {"$inc": {"total_uploads": 1}})
                await log_to_channel(client, f"User `{user_id}` successfully uploaded to Facebook page `{state.get('fb_page_name', 'selected page')}`. Video ID: `{fb_result['id']}`. Status: `{status_text}`. File: `{os.path.basename(processed_file_path)}`.")
            else:
                await client.send_message(user_id, f"❌ **Facebook Transmission Failed.** Response: `{json.dumps(fb_result, indent=2)}`")
                logger.error(f"Facebook upload failed for user {user_id} on page {state.get('fb_page_name', 'selected page')}. Result: {fb_result}")

    except concurrent.futures.TimeoutError:
        await client.send_message(user_id, "❌ **Operation Timed Out.** Content processing or transmission exceeded time limits. The data file might be too large or the network connection is unstable. Please retry with a smaller file or a more robust connection.")
        logger.error(f"Upload/processing timeout for user {user_id}. Original file: {file_path}")
    except RuntimeError as re:
        await client.send_message(user_id, f"❌ **Processing Error:** `{re}`\n\n_**System Note:** Ensure FFmpeg is correctly installed and accessible in your system's PATH, and verify your video data file is not corrupted._")
        logger.error(f"Processing/Upload Error for user {user_id}: {re}", exc_info=True)
    except requests.exceptions.RequestException as req_e:
        await client.send_message(user_id, f"❌ **Network/API Error during Transmission:** `{req_e}`\n\n_**System Note:** Please verify your internet connection or inspect the configured Facebook API parameters._")
        logger.error(f"Network/API Error for user {user_id}: {req_e}", exc_info=True)
    except Exception as e:
        await client.send_message(user_id, f"❌ **Critical Transmission Failure.** An unexpected system error occurred: `{e}`")
        logger.error(f"Upload failed for user {user_id}: {e}", exc_info=True)
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up original file: {file_path}")
        if processed_file_path != file_path and os.path.exists(processed_file_path):
            os.remove(processed_file_path)
            logger.info(f"Cleaned up processed file: {processed_file_path}")
        user_states.pop(user_id, None)
        await client.send_chat_action(user_id, enums.ChatAction.CANCEL)


# === KEEP ALIVE SERVER ===
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_server():
    httpd = HTTPServer(('0.0.0.0', 8080), Handler)
    logger.info("Keep-alive HTTP server started on port 8080.")
    httpd.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# === START BOT ===
if __name__ == "__main__":
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Created 'downloads' directory.")

    logger.info("Bot system initiating...")
    app.run()

