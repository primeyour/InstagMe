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
import re

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
        [KeyboardButton("⬆️ Upload Content")],
        [KeyboardButton("⚙️ Settings")]
    ],
    resize_keyboard=True
)

main_menu_admin = ReplyKeyboardMarkup(
    [
        [KeyboardButton("⬆️ Upload Content")],
        [KeyboardButton("⚙️ Settings"), KeyboardButton("👤 Admin Panel")]
    ],
    resize_keyboard=True
)

def get_general_settings_inline_keyboard(user_id):
    keyboard = []
    if is_premium_user(user_id) or is_admin(user_id):
        keyboard.append([InlineKeyboardButton("User Settings", callback_data='settings_user_menu_inline')])
    
    keyboard.append([InlineKeyboardButton("🎞️ Video Compression", callback_data='settings_video_compression')])
        
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
    [InlineKeyboardButton("🔙 Back to General Settings", callback_data="settings_main_menu_inline")]
])

user_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("📘 Facebook Settings", callback_data='settings_facebook')],
        [InlineKeyboardButton("▶️ YouTube Settings", callback_data='settings_youtube')],
        [InlineKeyboardButton("⬅️ Back to General Settings", callback_data='settings_main_menu_inline')]
    ]
)

facebook_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🔑 Facebook API Login", callback_data='fb_api_login_prompt')],
        [InlineKeyboardButton("📝 Set Title", callback_data='fb_set_title')],
        [InlineKeyboardButton("🏷️ Set Tag", callback_data='fb_set_tag')],
        [InlineKeyboardButton("📄 Set Description", callback_data='fb_set_description')],
        [InlineKeyboardButton("🎥 Default Upload Type", callback_data='fb_default_upload_type')],
        [InlineKeyboardButton("⏰ Set Schedule Time", callback_data='fb_set_schedule_time')],
        [InlineKeyboardButton("🔒 Set Private/Public", callback_data='fb_set_privacy')],
        [InlineKeyboardButton("🗓️ Check Token Info", callback_data='fb_check_token_info')],
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
        [InlineKeyboardButton("🗓️ Check Login Status", callback_data='yt_check_login_status')],
        [InlineKeyboardButton("⬅️ Back to User Settings", callback_data='settings_user_menu_inline')]
    ]
)

facebook_upload_type_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("Reels (Short Video)", callback_data='fb_upload_type_reels')],
        [InlineKeyboardButton("Video (Standard Post)", callback_data='fb_upload_type_video')],
        [InlineKeyboardButton("Photo (Image Post)", callback_data='fb_upload_type_photo')],
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
    keyboard = [
        [InlineKeyboardButton("Public", callback_data=f'{platform}_privacy_public')],
        [InlineKeyboardButton("Private", callback_data=f'{platform}_privacy_private')],
    ]
    if platform == 'yt':
        keyboard.append([InlineKeyboardButton("Unlisted", callback_data='yt_privacy_unlisted')])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f'settings_{platform}')])
    return InlineKeyboardMarkup(keyboard)

platform_selection_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("📘 Upload to Facebook", callback_data='upload_select_facebook')],
        [InlineKeyboardButton("▶️ Upload to YouTube", callback_data='upload_select_youtube')]
    ]
)

def get_video_compression_inline_keyboard(user_id):
    user_doc = get_user_data(user_id)
    compression_enabled = user_doc.get("compression_enabled", True)
    
    on_text = "✅ On (Compress Video)" if compression_enabled else "On (Compress Video)"
    off_text = "Off (Original Video) ✅" if not compression_enabled else "Off (Original Video)"

    keyboard = [
        [InlineKeyboardButton(on_text, callback_data='compression_set_on')],
        [InlineKeyboardButton(off_text, callback_data='compression_set_off')],
        [InlineKeyboardButton("⬅️ Back to General Settings", callback_data='settings_main_menu_inline')]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_facebook_page_selection_menu(pages, is_direct_login=False):
    page_buttons = []
    if pages:
        for page in pages:
            # We need the page-specific access token for future API calls
            page_buttons.append([InlineKeyboardButton(page['name'], callback_data=f"select_fb_page_{page['id']}_{page['access_token']}")])
        
    page_buttons.append([InlineKeyboardButton("🔄 Refresh Pages", callback_data="refresh_fb_pages")])
    page_buttons.append([InlineKeyboardButton("⬅️ Back to FB Settings", callback_data="settings_facebook")])
        
    return InlineKeyboardMarkup(page_buttons)


# === USER STATES (for sequential conversation flows) ===
user_states = {}

# === CONVERSATION STATES (for specific text input steps) ===
AWAITING_FB_TITLE = "awaiting_fb_title"
AWAITING_FB_TAG = "awaiting_fb_tag"
AWAITING_FB_DESCRIPTION = "awaiting_fb_description"
AWAITING_FB_SCHEDULE_TIME = "awaiting_fb_schedule_time"

AWAITING_FB_APP_ID = "awaiting_fb_app_id"
AWAITING_FB_APP_SECRET = "awaiting_fb_app_secret"
AWAITING_FB_PAGE_TOKEN = "awaiting_fb_page_token"
AWAITING_FB_USER_TOKEN = "awaiting_fb_user_token"

AWAITING_FB_PAGE_SELECTION = "awaiting_fb_page_selection"

AWAITING_YT_TITLE = "awaiting_yt_title"
AWAITING_YT_TAG = "awaiting_yt_tag"
AWAITING_YT_DESCRIPTION = "awaiting_yt_description"
AWAITING_YT_SCHEDULE_TIME = "awaiting_yt_schedule_time"
AWAITING_YT_CLIENT_ID = "awaiting_yt_client_id"
AWAITING_YT_CLIENT_SECRET = "awaiting_yt_client_secret"
AWAITING_YT_AUTH_CODE = "awaiting_yt_auth_code"

AWAITING_BROADCAST_MESSAGE = "awaiting_broadcast_message"

# --- Upload Flow Specific States ---
AWAITING_UPLOAD_FILE = "awaiting_upload_file"
AWAITING_UPLOAD_TITLE = "awaiting_upload_title"
AWAITING_UPLOAD_DESCRIPTION = "awaiting_upload_description"
AWAITING_UPLOAD_VISIBILITY = "awaiting_upload_visibility"
AWAITING_UPLOAD_SCHEDULE = "awaiting_upload_schedule"
AWAITING_UPLOAD_SCHEDULE_DATETIME = "awaiting_upload_schedule_datetime"
AWAITING_UPLOAD_TYPE_SELECTION = "awaiting_upload_type_selection"

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

def get_facebook_tokens_for_user(user_id):
    """Retrieves Facebook access token and selected page ID from user data."""
    user_doc = get_user_data(user_id)
    if not user_doc:
        return None, None
    return user_doc.get("facebook_page_access_token"), user_doc.get("facebook_selected_page_id")

def get_facebook_pages(user_access_token, user_id=None):
    """
    Fetches list of Facebook pages managed by the user token.
    Can optionally use a proxy.
    """
    proxies = None
    if user_id:
        user_doc = get_user_data(user_id)
        if user_doc and user_doc.get("facebook_proxy_url"):
            proxy_url = user_doc["facebook_proxy_url"]
            proxies = {"http": proxy_url, "https": proxy_url}
            logger.info(f"Using proxy {proxy_url} for Facebook API request for user {user_id}")
    
    # We now fetch pages using the user's access token stored in a temp state or the database.
    try:
        pages_url = f"https://graph.facebook.com/v19.0/me/accounts?access_token={user_access_token}"
        response = requests.get(pages_url, proxies=proxies)
        response.raise_for_status()
        return response.json().get('data', [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Facebook pages (user {user_id}): {e}")
        return []

def upload_facebook_content(file_path, content_type, title, description, access_token, page_id, visibility="PUBLISHED", schedule_time=None, user_id=None):
    """
    Uploads content (video, reels, photo) to Facebook Page using Graph API.
    Can optionally use a proxy.
    """
    if not all([file_path, content_type, access_token, page_id]):
        raise ValueError("Missing required parameters for Facebook content upload.")

    proxies = None
    if user_id:
        user_doc = get_user_data(user_id)
        if user_doc and user_doc.get("facebook_proxy_url"):
            proxy_url = user_doc["facebook_proxy_url"]
            proxies = {"http": proxy_url, "https": proxy_url}
            logger.info(f"Using proxy {proxy_url} for Facebook upload request for user {user_id}")

    params = {
        'access_token': access_token,
        'published': 'true' if not schedule_time and visibility.lower() != 'draft' else 'false',
    }

    if content_type == "video":
        post_url = f"https://graph-video.facebook.com/v19.0/{page_id}/videos"
        params['title'] = title
        params['description'] = description
        if schedule_time:
            params['scheduled_publish_time'] = int(schedule_time.timestamp())
            params['status_type'] = 'SCHEDULED_PUBLISH'
            logger.info(f"Scheduling Facebook video for: {schedule_time}")
        elif visibility.lower() == 'private' or visibility.lower() == 'draft':
            params['status_type'] = 'DRAFT'
            logger.info(f"Uploading Facebook video as DRAFT (visibility: {visibility}).")
        else:
            params['status_type'] = 'PUBLISHED'
            logger.info(f"Uploading Facebook video as PUBLISHED (visibility: {visibility}).")
        
        with open(file_path, 'rb') as f:
            files = {'file': f}
            response = requests.post(post_url, params=params, files=files, proxies=proxies)

    elif content_type == "reels":
        post_url = f"https://graph.facebook.com/v19.0/{page_id}/video_reels"
        params['video_file_chunk_upload_start'] = 1
        params['title'] = title
        params['description'] = description
        
        try:
            init_url = f"https://graph.facebook.com/v19.0/{page_id}/video_reels?access_token={access_token}&upload_phase=start"
            init_response = requests.post(init_url, proxies=proxies)
            init_response.raise_for_status()
            init_data = init_response.json()
            upload_session_id = init_data['upload_session_id']
            upload_url = init_data['upload_url']

            with open(file_path, 'rb') as f:
                upload_headers = {'file_offset': '0', 'X-Entity-Length': str(os.path.getsize(file_path))}
                upload_response = requests.post(upload_url, headers=upload_headers, data=f, proxies=proxies)
                upload_response.raise_for_status()
                
            publish_url = f"https://graph.facebook.com/v19.0/{page_id}/video_reels?access_token={access_token}&upload_phase=finish"
            publish_params = {
                'upload_session_id': upload_session_id,
                'video_state': 'PUBLISHED' if visibility.lower() == 'public' else 'DRAFT',
                'title': title,
                'description': description
            }
            if schedule_time:
                publish_params['video_state'] = 'SCHEDULED'
                publish_params['scheduled_publish_time'] = int(schedule_time.timestamp())
                logger.warning("Facebook Reels scheduling is complex and may not be directly supported via API as for regular videos. Simulating schedule.")

            response = requests.post(publish_url, json=publish_params, proxies=proxies)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error during Facebook Reels upload for file {file_path}: {e}")
            raise RuntimeError(f"Facebook Reels API error: {e}")

    elif content_type == "photo":
        post_url = f"https://graph.facebook.com/v19.0/{page_id}/photos"
        params['caption'] = description if description else title
        
        if schedule_time:
            params['scheduled_publish_time'] = int(schedule_time.timestamp())
            params['published'] = 'false'
            logger.info(f"Scheduling Facebook photo for: {schedule_time}")
        elif visibility.lower() == 'private' or visibility.lower() == 'draft':
            params['published'] = 'false'
            logger.info(f"Uploading Facebook photo as DRAFT (visibility: {visibility}).")
        else:
            params['published'] = 'true'
            logger.info(f"Uploading Facebook photo as PUBLISHED (visibility: {visibility}).")

        with open(file_path, 'rb') as f:
            files = {'source': f}
            response = requests.post(post_url, params=params, files=files, proxies=proxies)
            
    else:
        raise ValueError(f"Unsupported Facebook content type: {content_type}")

    response.raise_for_status()
    result = response.json()
    logger.info(f"Facebook {content_type} upload result: {result}")
    return result

def convert_media_for_facebook(input_path, output_type, target_format):
    """
    Converts media to suitable format for Facebook upload.
    """
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_path = f"downloads/processed_{base_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.{target_format}"

    command = []
    if output_type in ["video", "reels"]:
        if output_type == "reels":
            command = ["ffmpeg", "-i", input_path, "-vf", "scale='min(iw,ih*9/16)':'min(ih,iw*16/9)',pad='ih*9/16':ih:(ow-iw)/2:(oh-ih)/2", "-c:v", "libx264", "-preset", "medium", "-crf", "23", "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-y", output_path]
            logger.info(f"[FFmpeg] Attempting conversion for Reels to 9:16 for {input_path}")
        else:
            command = ["ffmpeg", "-i", input_path, "-c:v", "copy", "-c:a", "copy", "-map", "0", "-y", output_path]
            logger.info(f"[FFmpeg] Initiating video conversion for {input_path}")
    elif output_type == "photo":
        command = ["ffmpeg", "-i", input_path, "-vframes", "1", "-q:v", "2", "-y", output_path]
        logger.info(f"[FFmpeg] Converting video frame to photo for {input_path}")
    else:
        raise ValueError(f"Unsupported output type for conversion: {output_type}")

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=900)
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
        raise RuntimeError("FFmpeg conversion timed out. Media might be too large or complex.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during FFmpeg conversion: {e}")
        raise

def compress_video_ffmpeg(input_path):
    """
    Compresses a video using FFmpeg.
    """
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_path = f"downloads/{base_name}_compressed_{datetime.now().strftime('%Y%m%d%H%M%S')}.mp4"
    
    command = ["ffmpeg", "-i", input_path, "-c:v", "libx264", "-crf", "28", "-preset", "medium", "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-y", output_path]
    logger.info(f"[FFmpeg] Initiating video compression for {input_path}")

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=1200)
        logger.info(f"[FFmpeg] Compression successful for {input_path}. Output: {result.stdout}")
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"[FFmpeg] Compression failed for {input_path}. Command: {' '.join(e.cmd)}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        raise RuntimeError(f"FFmpeg compression error: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("FFmpeg not found. Please install FFmpeg and ensure it's in your system's PATH.")
    except subprocess.TimeoutExpired:
        raise RuntimeError("FFmpeg compression timed out. Video might be too large or complex for standard compression settings.")
    except Exception as e:
    logger.error(f"An unexpected error occurred during FFmpeg compression: {e}")


# === PYROGRAM HANDLERS ===

@app.on_message(filters.command("start"))
async def start_command(client, message):
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
        "compression_enabled": True,
        "facebook_settings": {
            "title": "Default Facebook Title", "tag": "#facebook #video #reels", "description": "Default Facebook Description", "upload_type": "Video", "schedule_time": None, "privacy": "Public", "proxy_url": None
        },
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
            "  • **Facebook (Reels, Videos & Photos)**\n\n"
            "• **Enjoy Unlimited Content Uploads & Advanced Options!**\n"
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

@app.on_message(filters.text & filters.regex("^⚙️ Settings$"))
async def show_main_settings_menu_reply(client, message):
    user_id = message.from_user.id
    user_doc = get_user_data(user_id)
    if not user_doc:
        await message.reply("⛔ **Access Denied!** Please send `/start` first to initialize your account in the system.")
        return
    await message.reply("⚙️ **System Configuration Interface:**\n\nChoose your settings options:", reply_markup=get_general_settings_inline_keyboard(user_id))
    logger.info(f"User {user_id} accessed main settings menu.")

@app.on_message(filters.text & filters.regex("^🔙 Main Menu$"))
async def back_to_main_menu_reply(client, message):
    user_id = message.from_user.id
    user_states.pop(user_id, None)
    if is_admin(user_id):
        await message.reply("✅ **Returning to Command Center.**", reply_markup=main_menu_admin)
    else:
        await message.reply("✅ **Returning to Main System Interface.**", reply_markup=main_menu_user)
    logger.info(f"User {user_id} returned to main menu via reply button.")

@app.on_callback_query(filters.regex("^settings_main_menu_inline$"))
async def settings_main_menu_inline_callback(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Accessing settings...")
    await callback_query.message.edit_text(
        "⚙️ **System Configuration Interface:**\n\nChoose your settings options:",
        reply_markup=get_general_settings_inline_keyboard(user_id)
    )
    logger.info(f"User {user_id} navigated to general settings via inline button.")

@app.on_callback_query(filters.regex("^back_to_main_menu_reply_from_inline$"))
async def back_to_main_menu_from_inline(client, callback_query):
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
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** You need a premium subscription to access user-specific configuration.", show_alert=True)
        logger.info(f"User {user_id} attempted to access user settings without premium.")
        return
    await callback_query.answer("Accessing user configurations...")
    await callback_query.message.edit_text(
        "⚙️ **User Account Settings:**\n\nChoose a platform to configure:",
        reply_markup=user_settings_inline_menu
    )
    logger.info(f"User {user_id} accessed user settings menu.")

@app.on_callback_query(filters.regex("^settings_video_compression$"))
async def show_video_compression_settings(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Accessing video compression settings...")
    await callback_query.message.edit_text(
        "🎞️ **Video Compression Module:**\n\n"
        "Configure whether to compress videos before uploading.\n"
        "**'On'** will apply compression to reduce file size (recommended for faster uploads).\n"
        "**'Off'** will attempt to upload the original video without additional compression (may be slower for large files).",
        reply_markup=get_video_compression_inline_keyboard(user_id)
    )
    logger.info(f"User {user_id} accessed video compression settings.")

@app.on_callback_query(filters.regex("^compression_set_"))
async def set_video_compression(client, callback_query):
    user_id = callback_query.from_user.id
    action = callback_query.data.split("_")[-1]
    compression_enabled = True if action == 'on' else False
    update_user_data(user_id, {"compression_enabled": compression_enabled})
    status_text = "enabled" if compression_enabled else "disabled"
    await callback_query.answer(f"Video compression {status_text}.", show_alert=True)
    await callback_query.message.edit_text(
        f"✅ **Video Compression Configured.** Compression is now **{status_text.upper()}**.",
        reply_markup=get_video_compression_inline_keyboard(user_id)
    )
    await log_to_channel(client, f"User `{user_id}` set video compression to `{status_text}`.")
    logger.info(f"User {user_id} set video compression to {status_text}.")

@app.on_message(filters.text & filters.regex("^👤 Admin Panel$") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_panel_menu_reply(client, message):
    user_id = message.from_user.id
    await message.reply("👋 **Welcome to the Administrator Command Center!**", reply_markup=Admin_markup)
    logger.info(f"Admin {user_id} accessed the Admin Panel.")

@app.on_callback_query(filters.regex("^admin_users_list$"))
async def admin_users_list_inline(client, callback_query):
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
    await callback_query.message.edit_text(user_list_text, reply_markup=Admin_markup, parse_mode=enums.ParseMode.MARKDOWN)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) viewed system users list.")

@app.on_callback_query(filters.regex("^admin_add_user_prompt$"))
async def admin_add_user_prompt_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("Initiating user upgrade protocol...")
    user_states[user_id] = {"step": "admin_awaiting_user_id_to_add"}
    await callback_query.message.edit_text(
        "Please transmit the **Telegram User ID** of the user you wish to grant **PREMIUM ACCESS**.\n"
        "Input the numeric ID now.",
        reply_markup=Admin_markup
    )
    logger.info(f"Admin {user_id} prompted to add premium user.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == "admin_awaiting_user_id_to_add") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_add_user_id_input(client, message):
    user_id = message.from_user.id
    target_user_id_str = message.text.strip()
    user_states.pop(user_id, None)
    try:
        target_user_id = int(target_user_id_str)
        update_user_data(target_user_id, {"is_premium": True, "role": "user"})
        await message.reply(f"✅ **Success!** User `{target_user_id}` has been granted **PREMIUM ACCESS**.", reply_markup=Admin_markup)
        try:
            await client.send_message(target_user_id, "🎉 **Congratulations!** Your account has been upgraded to **PREMIUM** status! Use `/start` to access your enhanced features.")
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
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("Initiating user downgrade protocol...")
    user_states[user_id] = {"step": "admin_awaiting_user_id_to_remove"}
    await callback_query.message.edit_text(
        "Please transmit the **Telegram User ID** of the user you wish to revoke **PREMIUM ACCESS** from.\n"
        "Input the numeric ID now.",
        reply_markup=Admin_markup
    )
    logger.info(f"Admin {user_id} prompted to remove premium user.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == "admin_awaiting_user_id_to_remove") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_remove_user_id_input(client, message):
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
            update_user_data(target_user_id, {"is_premium": False, "premium_platforms": [], "facebook_app_id": None, "facebook_app_secret": None, "facebook_page_access_token": None, "facebook_selected_page_id": None, "facebook_selected_page_name": None, "youtube_logged_in": False, "youtube_client_id": None, "youtube_client_secret": None, "youtube_access_token": None, "youtube_refresh_token": None})
            await message.reply(f"✅ **Success!** User `{target_user_id}` has been revoked from **PREMIUM ACCESS**.", reply_markup=Admin_markup)
            try:
                await client.send_message(target_user_id, "❗ **System Notification!** Your premium access has been revoked.")
            except Exception as e:
                logger.warning(f"Could not notify user {target_user_id} about premium revocation: {e}")
            await log_to_channel(client, f"User `{target_user_id}` demoted from admin by `{message.from_user.id}` (`{message.from_user.username}`).")
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
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("Initiating broadcast transmission protocol...")
    user_states[user_id] = {"step": AWAITING_BROADCAST_MESSAGE}
    await callback_query.message.edit_text(
        "Please transmit the **message payload** you wish to broadcast to all active system users.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Terminate Broadcast", callback_data="cancel_broadcast")]])
    )
    logger.info(f"Admin {user_id} prompted for broadcast message.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_BROADCAST_MESSAGE) & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def broadcast_message_handler(client, message):
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
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("System reboot sequence initiated...", show_alert=True)
    await callback_query.message.edit_text("🔄 **System Rebooting...** This may take a moment. Please send `/start` in a few seconds to re-establish connection.", reply_markup=None)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) initiated bot restart.")
    sys.exit(0)

@app.on_callback_query(filters.regex("^settings_bot_status_inline$"))
async def settings_bot_status_inline_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Access Restricted.** You are not authorized to access system diagnostics.", show_alert=True)
        return
    await callback_query.answer("Fetching system diagnostics...")
    total_users = users_collection.count_documents({})
    admin_users = users_collection.count_documents({"role": "admin"})
    premium_users = users_collection.count_documents({"is_premium": True})
    total_fb_accounts = users_collection.count_documents({"facebook_page_access_token": {"$ne": None}})
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
        f"• YouTube Accounts Synced: `{total_youtube_accounts}`\n\n"
        f"**Operational Metrics:**\n"
        f"• Total Content Transmissions: `{total_uploads_count}`\n\n"
        f"_Note: 'People left' metric is not tracked directly by this system._"
    )
    await callback_query.message.edit_text(stats_message, reply_markup=get_general_settings_inline_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) viewed detailed system status.")

@app.on_callback_query(filters.regex("^settings_facebook$"))
async def show_facebook_settings(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing Facebook configurations...")
    await callback_query.message.edit_text("📘 **Facebook Configuration Module:**", reply_markup=facebook_settings_inline_menu)
    logger.info(f"User {user_id} accessed Facebook settings.")

@app.on_callback_query(filters.regex("^settings_youtube$"))
async def show_youtube_settings(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing YouTube configurations...")
    await callback_query.message.edit_text("▶️ **YouTube Configuration Module:**", reply_markup=youtube_settings_inline_menu)
    logger.info(f"User {user_id} accessed YouTube settings.")

@app.on_callback_query(filters.regex("^settings_video_compression$"))
async def show_video_compression_settings(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Accessing video compression settings...")
    await callback_query.message.edit_text(
        "🎞️ **Video Compression Module:**\n\n"
        "Configure whether to compress videos before uploading.\n"
        "**'On'** will apply compression to reduce file size (recommended for faster uploads).\n"
        "**'Off'** will attempt to upload the original video without additional compression (may be slower for large files).",
        reply_markup=get_video_compression_inline_keyboard(user_id)
    )
    logger.info(f"User {user_id} accessed video compression settings.")

@app.on_callback_query(filters.regex("^compression_set_"))
async def set_video_compression(client, callback_query):
    user_id = callback_query.from_user.id
    action = callback_query.data.split("_")[-1]
    compression_enabled = True if action == 'on' else False
    update_user_data(user_id, {"compression_enabled": compression_enabled})
    status_text = "enabled" if compression_enabled else "disabled"
    await callback_query.answer(f"Video compression {status_text}.", show_alert=True)
    await callback_query.message.edit_text(
        f"✅ **Video Compression Configured.** Compression is now **{status_text.upper()}**.",
        reply_markup=get_video_compression_inline_keyboard(user_id)
    )
    await log_to_channel(client, f"User `{user_id}` set video compression to `{status_text}`.")
    logger.info(f"User {user_id} set video compression to {status_text}.")


# --- NEW COMMAND: Facebook Direct Login ---
@app.on_message(filters.command("fblogin"))
async def fb_direct_login_command(client, message):
    user_id = message.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await message.reply("❌ **Access Restricted.** This feature requires premium access. Please contact the administrator.")
        return

    try:
        args = message.text.split(maxsplit=2)
        if len(args) != 3:
            await message.reply("❗ **Syntax Error.** Usage: `/fblogin <username> <password>` or `/fblogin <email> <password>`. Proxy is optional, add it like so: ` /fblogin username password my_proxy_url`.")
            return

        username = args[1]
        password = args[2]

        await message.reply("⏳ **Initiating Facebook Direct Authentication (Simulated)...** Please standby.")
        await client.send_chat_action(user_id, enums.ChatAction.TYPING)

        # --- SIMULATED DIRECT LOGIN ---
        # Simulate success and store a mock user access token.
        simulated_user_access_token = f"SIMULATED_USER_TOKEN_{user_id}_{datetime.now().timestamp()}"
        
        # We don't save username/password for security reasons.
        # We only save the token that represents a "successful" login.
        update_user_data(user_id, {
            "facebook_user_access_token": simulated_user_access_token,
            "is_premium": True,
            "$addToSet": {"premium_platforms": "facebook"}
        })

        await message.reply("✅ **Login successfully completed ✅**")
        await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) successfully 'logged into' Facebook directly (simulated).")

    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during Facebook direct login procedure: `{e}`", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Failed to process Facebook direct login for user {user_id}: {e}", exc_info=True)


# --- NEW COMMAND: Display Facebook Pages ---
@app.on_message(filters.command("fbpagedetails"))
async def fb_page_details_command(client, message):
    user_id = message.from_user.id
    user_doc = get_user_data(user_id)
    if not user_doc or not user_doc.get("facebook_user_access_token"):
        await message.reply("❌ **Authentication Required.** You are not logged into Facebook. Please use `/fblogin` or configure your API access.")
        return
    
    await message.reply("⏳ **Fetching your Facebook pages...** Please standby.")

    user_access_token = user_doc["facebook_user_access_token"]
    
    # We will fetch a list of pages. This is still a simulation.
    dummy_pages = [
        {'id': '100000000000001', 'name': 'My Page A', 'access_token': f'DUMMY_PAGE_TOKEN_A_{user_id}'},
        {'id': '100000000000002', 'name': 'My Page B', 'access_token': f'DUMMY_PAGE_TOKEN_B_{user_id}'},
        {'id': '100000000000003', 'name': 'My Other Page C', 'access_token': f'DUMMY_PAGE_TOKEN_C_{user_id}'},
    ]

    if not dummy_pages:
        await message.reply("❌ **No Pages Found.** Could not retrieve any accessible Facebook Pages with your token. Please ensure your account manages at least one page and has necessary permissions.")
        return

    user_states[user_id] = {"step": AWAITING_FB_PAGE_SELECTION, "fb_pages_data": dummy_pages}
    
    await message.reply(
        "✅ **Pages Found!** Now, please select the Facebook Page you wish to use for uploads:",
        reply_markup=get_facebook_page_selection_menu(dummy_pages)
    )
    logger.info(f"User {user_id} requested and received Facebook page list.")


@app.on_message(filters.command("fbproxy"))
async def set_facebook_proxy_command(client, message):
    user_id = message.from_user.id
    if not is_premium_user(user_id):
        await message.reply("❌ **Access Restricted.** This feature requires premium access.")
        return

    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply("❗ **Syntax Error.** Usage: `/fbproxy <proxy_url>` or `/fbproxy clear` to remove it.")
            return

        proxy_input = args[1].strip()

        if proxy_input.lower() == "clear":
            update_user_data(user_id, {"facebook_proxy_url": None})
            await message.reply("✅ **Facebook Proxy Cleared.** Direct connection will be used.", reply_markup=facebook_settings_inline_menu)
        else:
            if not re.match(r"^(http|https|socks5|socks4):\/\/(.*?)(:\d+)?(\/.*)?$", proxy_input):
                await message.reply("❌ **Input Error.** Invalid proxy URL format. Please provide a valid URL (e.g., `http://host:port` or `socks5://user:pass@host:port`).", reply_markup=facebook_settings_inline_menu)
                return

            update_user_data(user_id, {"facebook_proxy_url": proxy_input})
            await message.reply(f"✅ **Facebook Proxy Configured.** New proxy set.", reply_markup=facebook_settings_inline_menu)
        
        await log_to_channel(client, f"User `{user_id}` set/cleared Facebook proxy.")

    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** An error occurred: `{e}`", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Failed to set Facebook proxy for user {user_id}: {e}", exc_info=True)


# --- Facebook API Login Flow ---
@app.on_callback_query(filters.regex("^fb_api_login_prompt$"))
async def fb_api_login_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Initiating Facebook API login protocol...")
    user_states[user_id] = {"step": AWAITING_FB_APP_ID}
    await callback_query.message.edit_text(
        "🔑 **Facebook API Login - Step 1/3 (App ID):**\n\n"
        "Please transmit your Facebook App ID.\n"
        "_(e.g., `12345678901234567`)_",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to FB Settings", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook App ID.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_APP_ID))
async def handle_fb_app_id_input(client, message):
    user_id = message.from_user.id
    app_id = message.text.strip()
    user_states[user_id]["fb_app_id"] = app_id
    user_states[user_id]["step"] = AWAITING_FB_APP_SECRET
    await message.reply(
        "🔑 **Facebook API Login - Step 2/3 (App Secret):**\n\n"
        "Please transmit your Facebook App Secret.\n"
        "_(e.g., `a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6`)_",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to FB Settings", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} provided Facebook App ID, awaiting App Secret.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_APP_SECRET))
async def handle_fb_app_secret_input(client, message):
    user_id = message.from_user.id
    app_secret = message.text.strip()
    user_states[user_id]["fb_app_secret"] = app_secret
    user_states[user_id]["step"] = AWAITING_FB_PAGE_TOKEN
    await message.reply(
        "🔑 **Facebook API Login - Step 3/3 (Page Access Token):**\n\n"
        "Please transmit the **Page Access Token** for the page you want to manage.\n"
        "This token should be obtained from the Facebook Developer portal and have the necessary permissions.\n"
        "_(e.g., `EAA...`)_",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to FB Settings", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} provided Facebook App Secret, awaiting Page Access Token.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_PAGE_TOKEN))
async def handle_fb_page_token_input(client, message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    page_access_token = message.text.strip()
    user_states.pop(user_id, None)
    app_id = state.get("fb_app_id")
    app_secret = state.get("fb_app_secret")
    await message.reply("⏳ **Validating Facebook credentials...** Please standby.")
    await client.send_chat_action(user_id, enums.ChatAction.TYPING)

    try:
        page_details_url = f"https://graph.facebook.com/v19.0/me?fields=id,name&access_token={page_access_token}"
        response = requests.get(page_details_url)
        response.raise_for_status()
        page_data = response.json()
        
        if 'id' not in page_data or 'name' not in page_data:
            raise ValueError("Invalid Page Access Token. Could not retrieve page ID and name.")
        
        page_id = page_data['id']
        page_name = page_data['name']
        
        update_user_data(user_id, {
            "facebook_app_id": app_id,
            "facebook_app_secret": app_secret,
            "facebook_page_access_token": page_access_token,
            "facebook_selected_page_id": page_id,
            "facebook_selected_page_name": page_name,
            "is_premium": True,
            "$addToSet": {"premium_platforms": "facebook"}
        })

        success_message = (
            f"✅ **Login success**\n\n"
            f"**User:** `{user_id}`\n"
            f"**App ID:** `{app_id}`\n"
            f"**App Secret:** `{app_secret[:5]}...{app_secret[-5:]}`\n"
            f"**Token:** `{page_access_token[:10]}...`\n"
            f"**Page:** `{page_name}`"
        )
        await message.reply(success_message, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=facebook_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) successfully logged into Facebook via API. Page: `{page_name}` ({page_id}).")

    except requests.exceptions.RequestException as e:
        await message.reply(f"❌ **Authentication Failed.** Invalid Page Access Token or network error. Please ensure the token is correct and has the required permissions. Error: `{e}`", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Failed to validate Facebook Page Access Token for user {user_id}: {e}", exc_info=True)
    except Exception as e:
        await message.reply(f"❌ **Authentication Failed.** An unexpected error occurred: `{e}`", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Failed to process Facebook API login for user {user_id}: {e}", exc_info=True)


@app.on_callback_query(filters.regex("^select_fb_page_"))
async def select_facebook_page(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or state.get("step") != AWAITING_FB_PAGE_SELECTION:
        await callback_query.answer("❗ **Invalid Operation.** Please re-initiate the Facebook login process.", show_alert=True)
        return

    try:
        parts = callback_query.data.split('_', 3)
        if len(parts) < 5:
            await callback_query.answer("❌ Invalid page selection data.", show_alert=True)
            return
        
        selected_page_id = parts[3]
        page_access_token = parts[4]

        page_name = "Unknown Page"
        if "fb_pages_data" in state:
            for page in state["fb_pages_data"]:
                if page.get('id') == selected_page_id:
                    page_name = page.get('name', page_name)
                    break
        
        update_user_data(user_id, {
            "facebook_page_access_token": page_access_token,
            "facebook_selected_page_id": selected_page_id,
            "facebook_selected_page_name": page_name,
            "is_premium": True,
            "$addToSet": {"premium_platforms": "facebook"}
        })
        user_states.pop(user_id, None)

        await callback_query.answer(f"Facebook Page '{page_name}' selected!", show_alert=True)
        await callback_query.message.edit_text(
            f"✅ **Facebook Page Configured!**\n"
            f"You are now ready to upload content to Page: **{page_name}** (`{selected_page_id}`).",
            reply_markup=facebook_settings_inline_menu,
            parse_mode=enums.ParseMode.MARKDOWN
        )
        await log_to_channel(client, f"User `{user_id}` (`{callback_query.from_user.username}`) selected Facebook Page '{page_name}' ({selected_page_id}).")

    except Exception as e:
        await callback_query.answer("❌ **Error selecting page.** Please try again.", show_alert=True)
        await callback_query.message.edit_text(f"❌ **Operation Failed.** Error selecting Facebook page: `{e}`", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Failed to select Facebook page for user {user_id}: {e}", exc_info=True)


@app.on_callback_query(filters.regex("^refresh_fb_pages$"))
async def refresh_facebook_pages(client, callback_query):
    user_id = callback_query.from_user.id
    user_doc = get_user_data(user_id)
    
    user_access_token = user_doc.get("facebook_page_access_token") # Use stored page token for refresh
    if not user_access_token:
        # If no page token is stored, check for a user access token from direct login
        user_access_token = user_doc.get("facebook_user_access_token")

    if not user_access_token:
        await callback_query.answer("No active Facebook login session to refresh.", show_alert=True)
        await callback_query.message.edit_text("❗ **Session Expired.** Please re-initiate Facebook login.", reply_markup=facebook_settings_inline_menu)
        return

    await callback_query.answer("Refreshing Facebook pages...")
    
    # We will fetch a list of pages. This is still a simulation.
    dummy_pages = [
        {'id': '100000000000001', 'name': 'My Page A (Refreshed)', 'access_token': f'DUMMY_PAGE_TOKEN_A_{user_id}'},
        {'id': '100000000000002', 'name': 'My Page B (Refreshed)', 'access_token': f'DUMMY_PAGE_TOKEN_B_{user_id}'},
    ]

    if not dummy_pages:
        await callback_query.message.edit_text("❌ **No Pages Found.** Could not retrieve any accessible Facebook Pages. Please ensure your token is valid and has `pages_show_list` permission.", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Facebook page refresh failed for user {user_id}: No pages found or token invalid.")
        return
    
    # Store the pages temporarily in state for selection
    user_states[user_id] = {"step": AWAITING_FB_PAGE_SELECTION, "fb_pages_data": dummy_pages}

    await callback_query.message.edit_text(
        "✅ **Facebook Pages Refreshed!** Select the Facebook Page you wish to manage for uploads:",
        reply_markup=get_facebook_page_selection_menu(dummy_pages)
    )
    logger.info(f"User {user_id} refreshed Facebook pages.")


@app.on_callback_query(filters.regex("^fb_set_title$"))
async def fb_set_title_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting title input...")
    user_states[user_id] = {"step": AWAITING_FB_TITLE}
    await callback_query.message.edit_text(
        "📝 **Facebook Title Input Module:**\n\nPlease transmit the new Facebook video/post title.\n"
        "_(Type 'skip' to use the default title.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook title.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_TITLE))
async def fb_set_title_save(client, message):
    user_id = message.from_user.id
    title = message.text.strip()
    user_states.pop(user_id, None)
    if title.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_title = user_doc.get("facebook_settings", {}).get("title", "Default Facebook Title")
        await message.reply(f"✅ **Facebook Title Skipped.** Using default: '{default_title}'", reply_markup=facebook_settings_inline_menu)
        logger.info(f"User {user_id} skipped Facebook title, using default.")
    else:
        update_user_data(user_id, {"facebook_settings.title": title})
        await message.reply(f"✅ **Facebook Title Configured.** New title set to: '{title}'", reply_markup=facebook_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set Facebook title.")
    logger.info(f"User {user_id} saved Facebook title.")

@app.on_callback_query(filters.regex("^fb_set_tag$"))
async def fb_set_tag_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting tag input...")
    user_states[user_id] = {"step": AWAITING_FB_TAG}
    await callback_query.message.edit_text(
        "🏷️ **Facebook Tag Input Module:**\n\nPlease transmit the new Facebook tags (e.g., `#reels #video #photo`). Separate with spaces.\n"
        "_(Type 'skip' to use default tags.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook tags.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_TAG))
async def fb_set_tag_save(client, message):
    user_id = message.from_user.id
    tag = message.text.strip()
    user_states.pop(user_id, None)
    if tag.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_tag = user_doc.get("facebook_settings", {}).get("tag", "#facebook #content #post")
        await message.reply(f"✅ **Facebook Tags Skipped.** Using default: '{default_tag}'", reply_markup=facebook_settings_inline_menu)
        logger.info(f"User {user_id} skipped Facebook tags, using default.")
    else:
        update_user_data(user_id, {"facebook_settings.tag": tag})
        await message.reply(f"✅ **Facebook Tags Configured.** New tags set to: '{tag}'", reply_markup=facebook_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set Facebook tag.")
    logger.info(f"User {user_id} saved Facebook tags.")

@app.on_callback_query(filters.regex("^fb_set_description$"))
async def fb_set_description_prompt(client, callback_query):
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
    user_id = message.from_user.id
    description = message.text.strip()
    user_states.pop(user_id, None)
    if description.lower() == "skip":
        user_doc = get_user_data(user_id)
        default_description = user_doc.get("facebook_settings", {}).get("description", "Default Facebook Description")
        await message.reply(f"✅ **Facebook Description Skipped.** Using default: '{default_description}'", reply_markup=facebook_settings_inline_menu)
        logger.info(f"User {user_id} skipped Facebook description, using default.")
    else:
        update_user_data(user_id, {"facebook_settings.description": description})
        await message.reply(f"✅ **Facebook Description Configured.** New description set to: '{description}'", reply_markup=facebook_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set Facebook description.")
    logger.info(f"User {user_id} saved Facebook description.")

@app.on_callback_query(filters.regex("^fb_default_upload_type$"))
async def fb_default_upload_type_selection(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting default upload type selection...")
    await callback_query.message.edit_text("🎥 **Facebook Default Upload Type Selector:**\n\nSelect the default content type for your Facebook uploads:", reply_markup=facebook_upload_type_inline_menu)
    logger.info(f"User {callback_query.from_user.id} accessed Facebook default upload type selection.")

@app.on_callback_query(filters.regex("^fb_upload_type_"))
async def fb_set_default_upload_type(client, callback_query):
    user_id = callback_query.from_user.id
    upload_type = callback_query.data.split("_")[-1].capitalize()
    update_user_data(user_id, {"facebook_settings.upload_type": upload_type})
    await callback_query.answer(f"Default Facebook upload type set to: {upload_type}", show_alert=True)
    await callback_query.message.edit_text(
        f"✅ **Facebook Default Upload Type Configured.** Set to: {upload_type}",
        reply_markup=facebook_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set Facebook default upload type to `{upload_type}`.")
    logger.info(f"User {user_id} set Facebook default upload type to {upload_type}.")

@app.on_callback_query(filters.regex("^fb_set_schedule_time$"))
async def fb_set_schedule_time_prompt(client, callback_query):
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
    user_id = message.from_user.id
    schedule_str = message.text.strip()
    user_states.pop(user_id, None)
    if schedule_str.lower() == "clear":
        update_user_data(user_id, {"facebook_settings.schedule_time": None})
        await message.reply("✅ **Facebook Schedule Cleared.** Your uploads will now publish immediately (unless privacy is Draft).", reply_markup=facebook_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` cleared Facebook schedule time.")
        logger.info(f"User {user_id} cleared Facebook schedule time.")
        return
    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")
        if schedule_dt <= datetime.utcnow() + timedelta(minutes=5):
            await message.reply("❌ **Time Constraint Violation.** Schedule time must be at least 5 minutes in the future. Please try again with a later time.")
            return
        update_user_data(user_id, {"facebook_settings.schedule_time": schedule_dt.isoformat()})
        await message.reply(f"✅ **Facebook Schedule Configured.** Content set for transmission at: '{schedule_str}' (UTC)", reply_markup=facebook_settings_inline_menu)
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
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting privacy selection...")
    await callback_query.message.edit_text("🔒 **Facebook Privacy Configuration Module:**\n\nSelect Facebook privacy setting:", reply_markup=get_privacy_inline_menu('fb'))
    logger.info(f"User {callback_query.from_user.id} accessed Facebook privacy selection.")

@app.on_callback_query(filters.regex("^fb_privacy_"))
async def fb_set_privacy(client, callback_query):
    user_id = callback_query.from_user.id
    privacy = "Public" if 'public' in callback_query.data else ("Private" if 'private' in callback_query.data else "Draft")
    update_user_data(user_id, {"facebook_settings.privacy": privacy})
    await callback_query.answer(f"Facebook privacy set to: {privacy}", show_alert=True)
    await callback_query.message.edit_text(
        f"✅ **Facebook Privacy Configured.** Set to: {privacy}",
        reply_markup=facebook_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set Facebook privacy to `{privacy}`.")
    logger.info(f"User {user_id} set Facebook privacy to {privacy}.")

@app.on_callback_query(filters.regex("^fb_check_token_info$"))
async def fb_check_token_info(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Retrieving Facebook token and page info...")
    user_doc = get_user_data(user_id)
    
    fb_app_id = user_doc.get("facebook_app_id", "Not Set")
    fb_app_secret = user_doc.get("facebook_app_secret", "Not Set")
    fb_page_access_token = user_doc.get("facebook_page_access_token", "Not Set")
    fb_selected_page_id = user_doc.get("facebook_selected_page_id", "Not Set")
    fb_selected_page_name = user_doc.get("facebook_selected_page_name", "None Selected")
    fb_proxy_url = user_doc.get("facebook_proxy_url", "None")
    
    if fb_page_access_token and "SIMULATED" in fb_page_access_token:
        token_status = "⚠️ Active (Simulated)"
    elif fb_page_access_token and fb_page_access_token != "Not Set":
        token_status = "✅ Active (Stored)"
    else:
        token_status = "❌ Not Stored"
    
    if fb_app_secret and fb_app_secret != "Not Set":
        display_secret = f"{fb_app_secret[:5]}...{fb_app_secret[-5:]}"
    else:
        display_secret = "Not Set"
    
    info_text = (
        f"**📘 Facebook Account Diagnostics:**\n"
        f"Login Status: `{token_status}`\n"
        f"App ID: `{fb_app_id}`\n"
        f"App Secret: `{display_secret}`\n"
        f"Page Access Token: `{fb_page_access_token[:10]}...`\n"
        f"Selected Page Name: `{fb_selected_page_name}`\n"
        f"Selected Page ID: `{fb_selected_page_id}`\n"
        f"Configured Proxy: `{fb_proxy_url}`\n\n"
        f"_**System Note:** For security, the full token is not displayed here. Tokens are long-lived but can be invalidated by Facebook._"
    )
    await callback_query.message.edit_text(info_text, reply_markup=facebook_settings_inline_menu, parse_mode=enums.ParseMode.MARKDOWN)
    logger.info(f"User {user_id} checked Facebook token info.")

@app.on_callback_query(filters.regex("^yt_login_prompt$"))
async def yt_login_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Initiating YouTube authentication protocol...")
    user_states[user_id] = {"step": AWAITING_YT_CLIENT_ID}
    await callback_query.message.edit_text(
        "🔑 **YouTube OAuth 2.0 Configuration - Step 1/3 (Client ID):**\n\n"
        "To connect your YouTube account, you need to provide your Google Cloud Project's **Client ID**.\n"
        "Please transmit your **Client ID** now.\n",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to YT Settings", callback_data='settings_youtube')]])
    )
    logger.info(f"User {user_id} prompted for YouTube Client ID.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_CLIENT_ID))
async def handle_yt_client_id_input(client, message):
    user_id = message.from_user.id
    client_id = message.text.strip()
    if not re.match(r"^\d+-[a-zA-Z0-9]+\.apps\.googleusercontent\.com$", client_id):
        await message.reply("❌ **Input Error.** Invalid Client ID format. Please ensure it looks like `123...-xyz...apps.googleusercontent.com`.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to YT Settings", callback_data='settings_youtube')]]))
        return
    user_states[user_id]["yt_client_id"] = client_id
    user_states[user_id]["step"] = AWAITING_YT_CLIENT_SECRET
    await message.reply(
        "🔑 **YouTube OAuth 2.0 Configuration - Step 2/3 (Client Secret):**\n\n"
        "Now, transmit your Google Cloud Project's **Client Secret**.\n",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to YT Settings", callback_data='settings_youtube')]])
    )
    logger.info(f"User {user_id} provided YouTube Client ID, awaiting Client Secret.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_CLIENT_SECRET))
async def handle_yt_client_secret_input(client, message):
    user_id = message.from_user.id
    client_secret = message.text.strip()
    state = user_states.get(user_id)
    client_id = state.get("yt_client_id")
    if not client_id or not client_secret:
        await message.reply("❌ **Configuration Failed.** Missing Client ID or Secret. Please restart the YouTube login process.", reply_markup=youtube_settings_inline_menu)
        user_states.pop(user_id, None)
        return
    update_user_data(user_id, {"youtube_client_id": client_id, "youtube_client_secret": client_secret})
    user_states[user_id]["yt_client_secret"] = client_secret
    user_states[user_id]["step"] = AWAITING_YT_AUTH_CODE
    simulated_auth_url = "https://accounts.google.com/o/oauth2/auth?response_type=code&" + f"client_id={client_id}&" + "redirect_uri=urn:ietf:wg:oauth:2.0:oob&" + "scope=https://www.googleapis.com/auth/youtube.upload%20https://www.googleapis.com/auth/youtube&" + "access_type=offline&" + "prompt=consent"
    await message.reply(
        "🔑 **YouTube OAuth 2.0 Configuration - Step 3/3 (Authorization Code):**\n\n"
        "Click the link below, authorize the application with your YouTube account, "
        "and then **copy the authorization code** provided by Google.\n\n"
        f"[Click here to authorize your YouTube account]({simulated_auth_url})\n\n"
        "Once you have the code, transmit it using the following command structure:\n"
        "```\n/ytcode <your_authorization_code>\n```\n",
        parse_mode=enums.ParseMode.MARKDOWN,
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to YT Settings", callback_data='settings_youtube')]])
    )
    logger.info(f"User {user_id} provided YouTube Client Secret, awaiting Auth Code.")

@app.on_message(filters.command("ytcode") & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_AUTH_CODE))
async def youtube_auth_code_command(client, message):
    user_id = message.from_user.id
    user_states.pop(user_id, None)
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.reply("❗ **Syntax Error.** Usage: `/ytcode <your_authorization_code>`", reply_markup=youtube_settings_inline_menu)
            return
        auth_code = args[1].strip()
        user_doc = get_user_data(user_id)
        client_id = user_doc.get("youtube_client_id")
        client_secret = user_doc.get("youtube_client_secret")
        if not client_id or not client_secret:
            await message.reply("❌ **Authentication Failed.** Client ID or Client Secret missing. Please restart the YouTube login process from `Settings`.", reply_markup=youtube_settings_inline_menu)
            return
        simulated_access_token = f"SIMULATED_YT_ACCESS_TOKEN_{user_id}_{datetime.now().timestamp()}"
        simulated_refresh_token = f"SIMULATED_YT_REFRESH_TOKEN_{user_id}_{datetime.now().timestamp()}"
        simulated_expiry_date = datetime.utcnow() + timedelta(hours=1)
        update_user_data(user_id, {
            "youtube_logged_in": True,
            "youtube_access_token": simulated_access_token,
            "youtube_refresh_token": simulated_refresh_token,
            "youtube_token_expiry": simulated_expiry_date.isoformat(),
            "is_premium": True,
            "$addToSet": {"premium_platforms": "youtube"}
        })
        success_message = (
            f"✅ **YouTube Login Simulated Successfully!**\n\n"
            f"**User ID:** `{user_id}`\n"
            f"**Client ID:** `{client_id}`\n"
            f"**Client Secret:** `{client_secret[:5]}...{client_secret[-5:]}`\n"
            f"**Expiry Date:** `{simulated_expiry_date.strftime('%Y-%m-%d %H:%M UTC')}`\n\n"
            "You are now ready to upload content to YouTube (simulated)."
        )
        await message.reply(success_message, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=youtube_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) successfully 'logged into' YouTube (simulated). Set as premium.")
    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during YouTube login procedure: `{e}`. Please ensure your authorization code is correct or try again from `Settings`.", reply_markup=youtube_settings_inline_menu)
        logger.error(f"Failed to process YouTube auth code for user {user_id}: {e}", exc_info=True)


@app.on_callback_query(filters.regex("^yt_set_title$"))
async def yt_set_title_prompt(client, callback_query):
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
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting video type selection...")
    await callback_query.message.edit_text("🎥 **YouTube Video Type Selector:**\n\nSelect YouTube content type:", reply_markup=youtube_video_type_inline_menu)
    logger.info(f"User {callback_query.from_user.id} accessed YouTube video type selection.")

@app.on_callback_query(filters.regex("^yt_video_type_"))
async def yt_set_video_type(client, callback_query):
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
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting privacy selection...")
    await callback_query.message.edit_text("🔒 **YouTube Privacy Configuration Module:**\n\nSelect YouTube privacy setting:", reply_markup=get_privacy_inline_menu('yt'))
    logger.info(f"User {callback_query.from_user.id} accessed YouTube privacy selection.")

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
    await callback_query.message.edit_text(
        f"✅ **YouTube Privacy Configured.** Set to: {privacy}",
        reply_markup=youtube_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set YouTube privacy to `{privacy}`.")
    logger.info(f"User {user_id} set YouTube privacy to {privacy}.")

@app.on_callback_query(filters.regex("^yt_check_login_status$"))
async def yt_check_login_status(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Retrieving YouTube login status...")
    user_doc = get_user_data(user_id)
    logged_in = user_doc.get("youtube_logged_in", False)
    client_id = user_doc.get("youtube_client_id", "Not Set")
    client_secret = user_doc.get("youtube_client_secret", "Not Set")
    token_expiry = user_doc.get("youtube_token_expiry", "Not Set")
    status_text = "✅ Logged In (Simulated)" if logged_in else "❌ Not Logged In"
    if client_secret and client_secret != "Not Set":
        display_secret = f"{client_secret[:5]}...{client_secret[-5:]}"
    else:
        display_secret = "Not Set"
    info_message = (
        f"▶️ **YouTube Account Diagnostics:**\n"
        f"Login Status: `{status_text}`\n"
        f"Client ID: `{client_id}`\n"
        f"Client Secret: `{display_secret}`\n"
        f"Simulated Token Expiry: `{token_expiry}`\n\n"
        "_**System Note:** Full YouTube OAuth 2.0 and token refresh is a complex web-based process, "
        "and this implementation simulates the credential storage. For production, consider using "
        "Google API client libraries for Python._"
    )
    await callback_query.message.edit_text(info_message, reply_markup=youtube_settings_inline_menu, parse_mode=enums.ParseMode.MARKDOWN)
    logger.info(f"User {user_id} checked YouTube login status.")


@app.on_message(filters.text & filters.regex("^⬆️ Upload Content$"))
async def prompt_platform_for_upload(client, message):
    user_id = message.chat.id
    user_doc = get_user_data(user_id)
    if not user_doc:
        await message.reply("⛔ **Access Denied!** Please send `/start` first to initialize your account in the system.")
        return
    if not is_premium_user(user_id) and not is_admin(user_id):
        await message.reply("❌ **Access Restricted.** You need **PREMIUM ACCESS** to use upload features. Please contact the administrator to upgrade your privileges.")
        return
    await message.reply(
        "🚀 **Content Transmission Platform Selection.**\n\n"
        "Please select the target platform for your upload:",
        reply_markup=platform_selection_inline_menu
    )
    logger.info(f"User {user_id} initiated generic upload flow, prompted for platform selection.")

@app.on_callback_query(filters.regex("^upload_select_"))
async def handle_upload_platform_selection(client, callback_query):
    user_id = callback_query.from_user.id
    platform = callback_query.data.split("_")[-1]
    await callback_query.answer(f"Selected {platform.capitalize()} for upload.", show_alert=True)
    user_doc = get_user_data(user_id)
    if platform == "facebook":
        fb_access_token, fb_selected_page_id = get_facebook_tokens_for_user(user_id)
        if not fb_access_token or not fb_selected_page_id:
            await callback_query.message.edit_text("❌ **Authentication Required.** You are not logged into Facebook or haven't selected a page. Please use `/fbpagedetails` to select a page before uploading.")
            return
        user_states[user_id] = {"step": AWAITING_UPLOAD_TYPE_SELECTION, "platform": "facebook"}
        await callback_query.message.edit_text(
            "What type of content are you transmitting to Facebook?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎥 Video", callback_data="upload_type_fb_video")],
                [InlineKeyboardButton("🎞️ Reel", callback_data="upload_type_fb_reels")],
                [InlineKeyboardButton("🖼️ Photo", callback_data="upload_type_fb_photo")]
            ])
        )
        logger.info(f"User {user_id} selected Facebook for upload, prompted for content type.")
    elif platform == "youtube":
        if not user_doc.get("youtube_logged_in") or not user_doc.get("youtube_client_id") or not user_doc.get("youtube_client_secret"):
            await callback_query.message.edit_text("❌ **Authentication Required.** You are not fully logged into YouTube (missing credentials). Please navigate to `⚙️ Settings` -> `▶️ YouTube Settings` to configure your account first.")
            return
        user_states[user_id] = {"step": AWAITING_UPLOAD_FILE, "platform": "youtube"}
        await callback_query.message.edit_text(
            "🎥 **Content Transmission Protocol Active.** Please transmit your video file for YouTube now.",
            reply_markup=None
        )
        await client.send_message(user_id, "You can use '🔙 Main Menu' to abort the transmission.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        logger.info(f"User {user_id} selected YouTube for upload, awaiting file.")

@app.on_callback_query(filters.regex("^upload_type_fb_"))
async def handle_facebook_upload_type_selection(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)
    if not state or state.get("step") != AWAITING_UPLOAD_TYPE_SELECTION:
        await callback_query.answer("❗ **Invalid Operation.** Please restart the upload process.", show_alert=True)
        return
    upload_type = callback_query.data.split("_")[-1]
    state["upload_type"] = upload_type
    user_states[user_id]["step"] = AWAITING_UPLOAD_FILE
    await callback_query.answer(f"Selected Facebook {upload_type.capitalize()} upload.", show_alert=True)
    await callback_query.message.edit_text(
        f"🎥 **Content Transmission Protocol Active.** Please transmit your {'video' if upload_type != 'photo' else 'image'} file for Facebook now.",
        reply_markup=None
    )
    await client.send_message(user_id, "You can use '🔙 Main Menu' to abort the transmission.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
    logger.info(f"User {user_id} selected Facebook upload type '{upload_type}', awaiting file.")

@app.on_message(filters.video | filters.photo)
async def handle_media_upload(client, message):
    user_id = message.chat.id
    if not get_user_data(user_id):
        await message.reply("⛔ **Access Denied!** Please send `/start` first to initialize your account in the system.")
        return
    state = user_states.get(user_id)
    if not state or (state.get("step") != AWAITING_UPLOAD_FILE):
        if message.video:
            await message.reply("❗ **Invalid Operation.** Please initiate a video upload process by clicking '⬆️ Upload Content' first.")
        elif message.photo:
            await message.reply("❗ **Invalid Operation.** Please initiate a photo upload process by clicking '⬆️ Upload Content' first.")
        logger.warning(f"User {user_id} sent media without active upload state.")
        return
    if not is_premium_user(user_id) and not is_admin(user_id):
        await message.reply("❌ **Access Restricted.** You need **PREMIUM ACCESS** to upload content. Please contact the administrator.")
        user_states.pop(user_id, None)
        logger.warning(f"Non-premium user {user_id} attempted media upload.")
        return
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Created 'downloads' directory for media processing.")
    initial_status_msg = await message.reply("⏳ **Data Acquisition In Progress...** Downloading your content. This operation may require significant processing time for large data files.")
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        if message.video:
            file_extension = os.path.splitext(message.video.file_name or "video.mp4")[1]
        elif message.photo:
            file_extension = ".jpg"
        download_filename = f"downloads/{user_id}_{timestamp}{file_extension}"
        file_path = await message.download(file_name=download_filename)
        user_states[user_id]["file_path"] = file_path
        user_states[user_id]["step"] = AWAITING_UPLOAD_TITLE
        user_doc = get_user_data(user_id)
        platform = state["platform"]
        default_title = user_doc.get(f"{platform}_settings", {}).get("title", "Default Title")
        await initial_status_msg.edit_text(
            f"📝 **Metadata Input Required.** Now, transmit the **title** for your `{platform.capitalize()}` content.\n"
            f"_(Type 'skip' to use your default title: '{default_title}')_"
        )
        logger.info(f"User {user_id} media downloaded to {file_path}. Awaiting title.")
    except Exception as e:
        await initial_status_msg.edit_text(f"❌ **Data Acquisition Failed.** Error downloading media: `{e}`")
        logger.error(f"Failed to download media for user {user_id}: {e}", exc_info=True)
        user_states.pop(user_id, None)

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_TITLE))
async def handle_upload_title(client, message):
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
        f"📝 **Metadata Input Required.** Now, transmit a **description** for your `{platform.capitalize()}` content.\n"
        f"_(Type 'skip' to use your default description: '{default_description}')_"
    )
    logger.info(f"User {user_id} awaiting description for {platform}.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_DESCRIPTION))
async def handle_upload_description(client, message):
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
    if platform == "youtube":
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Public", callback_data="visibility_public")], [InlineKeyboardButton("Private", callback_data="visibility_private")], [InlineKeyboardButton("Unlisted", callback_data="visibility_unlisted")]]
        )
    else:
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Public", callback_data="visibility_public")], [InlineKeyboardButton("Private (Draft)", callback_data="visibility_draft")]]
        )
    await message.reply("🌐 **Visibility Configuration Module.** Select content visibility:", reply_markup=keyboard)
    logger.info(f"User {user_id} awaiting visibility choice for {platform}.")

@app.on_callback_query(filters.regex("^visibility_"))
async def handle_visibility_selection(client, callback_query):
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
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Publish Now", callback_data="schedule_now")], [InlineKeyboardButton("Schedule Later", callback_data="schedule_later")]]
    )
    await callback_query.message.edit_text("⏰ **Content Release Protocol.** Do you wish to publish now or schedule for later?", reply_markup=keyboard)
    logger.info(f"User {user_id} awaiting schedule choice for {platform}.")

@app.on_callback_query(filters.regex("^schedule_"))
async def handle_schedule_selection(client, callback_query):
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
        logger.error(f"Error processing schedule time for user {user_id}: {e}", exc_info=True)


async def initiate_upload(client, message, user_id):
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
    upload_type = state.get("upload_type", "video")
    if not all([file_path, title, description]):
        await client.send_message(user_id, "❌ **Upload Protocol Failure.** Missing essential content metadata (file, title, or description). Please restart the upload sequence.")
        logger.error(f"Missing essential upload data for user {user_id}. State: {state}")
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        user_states.pop(user_id, None)
        return
    user_states[user_id]["step"] = "processing_and_uploading"
    await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO)
    await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) initiating upload for {platform}. Type: `{upload_type}`. File: `{os.path.basename(file_path)}`. Visibility: `{visibility}`. Schedule: `{schedule_time}`.")
    processed_file_path = file_path
    user_doc = get_user_data(user_id)
    compression_enabled = user_doc.get("compression_enabled", True)
    try:
        if message.video:
            if compression_enabled:
                await client.send_message(user_id, "🎞️ **Video Compression Protocol.** Compressing your video for optimal transmission. Please standby...")
                await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO)
                def do_compression_sync():
                    return compress_video_ffmpeg(file_path)
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(do_compression_sync)
                    processed_file_path = future.result(timeout=1200)
                await client.send_message(user_id, "✅ **Video Compression Complete.**")
                await log_to_channel(client, f"User `{user_id}` video compressed. Original: `{os.path.basename(file_path)}`, Compressed: `{os.path.basename(processed_file_path)}`.")
            else:
                await client.send_message(user_id, "✅ **Video Compression Skipped.** Uploading original video.")
                logger.info(f"User {user_id} chose to skip video compression.")
        if platform == "facebook":
            target_format = "mp4" if upload_type in ["video", "reels"] else "jpg"
            current_file_ext = os.path.splitext(processed_file_path)[1].lower()
            if (target_format == "mp4" and current_file_ext != ".mp4") or (target_format == "jpg" and current_file_ext not in [".jpg", ".jpeg", ".png"]):
                await client.send_message(user_id, f"🔄 **Data Conversion Protocol.** Converting content to {target_format.upper()} format for Facebook {upload_type}. Please standby...")
                await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO)
                def do_conversion_sync():
                    return convert_media_for_facebook(processed_file_path, upload_type, target_format)
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(do_conversion_sync)
                    processed_file_path = future.result(timeout=900)
                await client.send_message(user_id, "✅ **Content Data Conversion Complete.**")
                await log_to_channel(client, f"User `{user_id}` content converted. Original: `{os.path.basename(file_path)}`, Processed: `{os.path.basename(processed_file_path)}`.")
            else:
                await client.send_message(user_id, "✅ **Content Format Verified.** Proceeding with direct transmission to Facebook.")
                logger.info(f"User {user_id} content already suitable format for Facebook {upload_type}. Skipping additional conversion.")
        elif platform == "youtube":
            if os.path.splitext(processed_file_path)[1].lower() not in [".mp4", ".mov", ".avi", ".webm"]:
                await client.send_message(user_id, f"🔄 **Data Conversion Protocol.** Converting video to MP4 format for YouTube. Please standby...")
                await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO)
                def do_conversion_sync_yt():
                    return convert_media_for_facebook(processed_file_path, "video", "mp4")
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(do_conversion_sync_yt)
                    processed_file_path = future.result(timeout=600)
                await client.send_message(user_id, "✅ **Video Data Conversion Complete.**")
                await log_to_channel(client, f"User `{user_id}` video converted for YouTube. Original: `{os.path.basename(file_path)}`, Processed: `{os.path.basename(processed_file_path)}`.")
            else:
                await client.send_message(user_id, "✅ **Video Format Verified.** Proceeding with direct transmission to YouTube.")
                logger.info(f"User {user_id} video already suitable format for YouTube. Skipping additional conversion.")
        if platform == "facebook":
            fb_access_token, fb_selected_page_id = get_facebook_tokens_for_user(user_id)
            if not fb_access_token or not fb_selected_page_id:
                await client.send_message(user_id, "❌ **Authentication Required.** You are not logged into Facebook or haven't selected a page. Please use `/fbpagedetails` to select a page before uploading.")
                return
            await client.send_message(user_id, f"📤 **Initiating Facebook {upload_type.capitalize()} Transmission...**")
            await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO if upload_type != 'photo' else enums.ChatAction.UPLOAD_PHOTO)
            def upload_to_facebook_sync():
                return upload_facebook_content(processed_file_path, upload_type.lower(), title, description, fb_access_token, fb_selected_page_id, visibility=visibility, schedule_time=schedule_time, user_id=user_id)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(upload_to_facebook_sync)
                fb_result = future.result(timeout=1200)
            if fb_result and ('id' in fb_result or 'post_id' in fb_result):
                content_id = fb_result.get('id') or fb_result.get('post_id')
                status_text = "Scheduled" if schedule_time else ("Draft" if visibility.lower() == 'draft' else "Published")
                await client.send_message(user_id, f"✅ **Facebook Content Transmitted!** {upload_type.capitalize()} ID: `{content_id}`. Status: `{status_text}`.")
                users_collection.update_one({"_id": user_id}, {"$inc": {"total_uploads": 1}})
                await log_to_channel(client, f"User `{user_id}` successfully uploaded {upload_type} to Facebook. ID: `{content_id}`. Status: `{status_text}`. File: `{os.path.basename(processed_file_path)}`.")
            else:
                await client.send_message(user_id, f"❌ **Facebook Transmission Failed.** Response: `{json.dumps(fb_result, indent=2)}`")
                logger.error(f"Facebook upload failed for user {user_id}. Result: {fb_result}")
        elif platform == "youtube":
            await client.send_message(user_id, "🚧 **YouTube Upload Feature Under Development.**\n\n_**System Note:** The YouTube upload functionality requires advanced Google API integration (OAuth 2.0, YouTube Data API V3) which is currently not fully implemented in this system. Your video was processed, but not uploaded._")
            await log_to_channel(client, f"User `{user_id}` attempted YouTube upload (currently simulated). File: `{os.path.basename(processed_file_path)}`.")
            users_collection.update_one({"_id": user_id}, {"$inc": {"total_uploads": 1}})
    except concurrent.futures.TimeoutError:
        await client.send_message(user_id, "❌ **Operation Timed Out.** Content processing or transmission exceeded time limits. The data file might be too large or the network connection is unstable. Please retry with a smaller file or a more robust connection.")
        logger.error(f"Upload/processing timeout for user {user_id}. Original file: {file_path}")
    except RuntimeError as re:
        await client.send_message(user_id, f"❌ **Processing Error:** `{re}`\n\n_**System Note:** Ensure FFmpeg is correctly installed and accessible in your system's PATH, and verify your media data file is not corrupted._")
        logger.error(f"Processing/Upload Error for user {user_id}: {re}", exc_info=True)
    except requests.exceptions.RequestException as req_e:
        await client.send_message(user_id, f"❌ **Network/API Error during Transmission:** `{req_e}`\n\n_**System Note:** Please verify your internet connection or inspect the configured API parameters for Facebook._")
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
