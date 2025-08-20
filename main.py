import os
import threading
import concurrent.futures
from http.server import HTTPServer, BaseHTTPRequestHandler
import logging
import json
import time
import subprocess
from datetime import datetime, timedelta, timezone
import sys
import base64
from urllib.parse import urlencode, parse_qs
import random
import asyncio
from functools import wraps
import re
import psutil
from uuid import uuid4

# Import for Google OAuth and YouTube API
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import Flow

from pyrogram import Client, filters, enums
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.objectid import ObjectId

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
PAYMENT_QR_CODE = "https://i.postimg.cc/SXDxJ92z/x.jpg"

# ⚠️ CRITICAL CONFIGURATION ⚠️
# The REDIRECT_URI MUST be your public server URL, not 127.0.0.1.
# Example: https://your-app-name.koyeb.app/oauth2callback
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://127.0.0.1:8080/oauth2callback")
GOOGLE_API_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/userinfo.email"
]
FB_API_SCOPES = [
    "pages_show_list", "pages_read_engagement", "pages_manage_posts", "pages_manage_metadata"
]

# === GLOBAL CLIENTS AND DB ===
app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
users_collection = db["users"]
jobs_collection = db["jobs"]
settings_collection = db["settings"]

# --- MongoDB Index Check/Creation ---
try:
    for index_info in users_collection.index_information().values():
        if index_info.get('unique') and index_info.get('key') == [('user_id', 1)]:
            logger.warning("Found a problematic 'user_id_1' unique index. Attempting to drop it.")
            users_collection.drop_index("user_id_1")
            logger.info("Successfully dropped 'user_id_1' unique index.")
            break
except Exception as e:
    logger.error(f"Error checking/dropping problematic user_id index: {e}")


# === PREMIUM PLANS & QUOTAS ===
PREMIUM_PLANS = {
    "free": {"duration_days": 1, "upload_quota": 1},
    "trial": {"duration_days": 0.25, "upload_quota": 10},
    "3_days": {"duration_days": 3, "upload_quota": 30, "price": "100 INR"},
    "7_days": {"duration_days": 7, "upload_quota": 70, "price": "200 INR"},
    "15_days": {"duration_days": 15, "upload_quota": 150, "price": "350 INR"},
    "1_month": {"duration_days": 30, "upload_quota": 300, "price": "600 INR"},
    "2_months": {"duration_days": 60, "upload_quota": 600, "price": "1000 INR"},
    "6_months": {"duration_days": 180, "upload_quota": 1800, "price": "2500 INR"},
    "1_year": {"duration_days": 365, "upload_quota": 3650, "price": "4500 INR"},
    "lifetime": {"duration_days": 36500, "upload_quota": 99999, "price": "Negotiable"}
}


# === CONCURRENCY & SETTINGS ===
upload_semaphore = None

def initialize_global_settings():
    """Initializes global bot settings in the database if they don't exist."""
    if not settings_collection.find_one({"_id": "global_config"}):
        settings_collection.insert_one({
            "_id": "global_config",
            "concurrent_uploads": 5,
            "payment_qr_code": "https://i.postimg.cc/SXDxJ92z/x.jpg",
            "payment_info": f"To proceed with the payment, please contact the administrator: @{ADMIN_TOM_USERNAME}"
        })
        logger.info("Initialized global bot settings in the database.")

def load_upload_semaphore():
    global upload_semaphore
    settings = settings_collection.find_one({"_id": "global_config"})
    limit = settings.get("concurrent_uploads", 5) if settings else 5
    upload_semaphore = threading.Semaphore(limit)
    logger.info(f"Upload semaphore initialized with a limit of {limit}.")

# === FONT HELPER (NEW) ===
SMALL_CAPS_MAP = str.maketrans(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ" +
    "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ"
)
def to_small_caps(s: str) -> str:
    return s.translate(SMALL_CAPS_MAP)


# === KEYBOARDS (REVISED) ===
def get_main_menu(user_id):
    """Generates the main reply keyboard with small-caps and conditional buttons."""
    kb = []
    
    # Premium-gated upload buttons
    if is_premium_user(user_id, "youtube"):
        kb.append([KeyboardButton(to_small_caps("▶️ Upload To YouTube"))])
    if is_premium_user(user_id, "facebook"):
        kb.append([KeyboardButton(to_small_caps("📘 Upload To Facebook"))])

    # Premium Plans always visible, Settings is conditional
    settings_button = KeyboardButton(to_small_caps("⚙️ Settings")) if (is_premium_user(user_id) or is_admin(user_id)) else None
    
    row = [KeyboardButton(to_small_caps("⭐ Premium Plans"))]
    if settings_button:
        row.append(settings_button)
    kb.append(row)

    if is_admin(user_id):
        kb.append([KeyboardButton(to_small_caps("👤 Admin Panel"))])

    return ReplyKeyboardMarkup(kb, resize_keyboard=True)


def get_general_settings_inline_keyboard(user_id):
    keyboard = []
    if is_premium_user(user_id) or is_admin(user_id):
        keyboard.append([InlineKeyboardButton("User Settings", callback_data='settings_user_menu_inline')])
    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("Bot Status", callback_data='settings_bot_status_inline')])

    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_main_menu_reply_from_inline')])
    return InlineKeyboardMarkup(keyboard)

admin_markup = InlineKeyboardMarkup([
    [InlineKeyboardButton("👥 Users List", callback_data="admin_users_list")],
    [InlineKeyboardButton("🕵️ Get User Info", callback_data="admin_get_user_info_prompt")],
    [InlineKeyboardButton("➕ Add Premium", callback_data="admin_add_user_prompt")],
    [InlineKeyboardButton("➖ Remove Premium", callback_data="admin_remove_user_prompt")],
    [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast_prompt")],
    [InlineKeyboardButton("⚙️ Bot Settings", callback_data="admin_bot_settings")],
    [InlineKeyboardButton("💳 Payment Settings", callback_data="admin_payment_settings")],
    [InlineKeyboardButton("🔄 Restart Bot", callback_data='admin_restart_bot')],
    [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="back_to_main_menu_reply_from_inline")]
])

user_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("📘 Facebook Settings", callback_data='settings_facebook')],
        [InlineKeyboardButton("▶️ YouTube Settings", callback_data='settings_youtube')],
        [InlineKeyboardButton("👑 My Plan", callback_data='settings_my_plan')],
        [InlineKeyboardButton("⬅️ Back to General Settings", callback_data='settings_main_menu_inline')]
    ]
)

facebook_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🔑 Manage Accounts", callback_data='fb_manage_accounts')],
        [InlineKeyboardButton("📝 Set Title", callback_data='fb_set_title')],
        [InlineKeyboardButton("🏷️ Set Tag", callback_data='fb_set_tag')],
        [InlineKeyboardButton("📄 Set Description", callback_data='fb_set_description')],
        [InlineKeyboardButton("🎥 Default Upload Type", callback_data='fb_default_upload_type')],
        [InlineKeyboardButton("⏰ Set Schedule Time", callback_data='fb_set_schedule_time')],
        [InlineKeyboardButton("🔒 Set Privacy", callback_data='fb_set_privacy')],
        [InlineKeyboardButton("⬅️ Back to User Settings", callback_data='settings_user_menu_inline')]
    ]
)

youtube_settings_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🔑 Manage Accounts", callback_data='yt_manage_accounts')],
        [InlineKeyboardButton("📝 Set Title", callback_data='yt_set_title')],
        [InlineKeyboardButton("🏷️ Set Tag", callback_data='yt_set_tag')],
        [InlineKeyboardButton("📄 Set Description", callback_data='yt_set_description')],
        [InlineKeyboardButton("🎥 Video Type (Shorts/Video)", callback_data='yt_video_type')],
        [InlineKeyboardButton("⏰ Set Schedule Time", callback_data='yt_set_schedule_time')],
        [InlineKeyboardButton("🔒 Set Private/Public", callback_data='yt_set_privacy')],
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

def get_premium_plan_keyboard():
    keyboard = []
    for plan, details in PREMIUM_PLANS.items():
        if plan in ["free", "trial"]: continue
        keyboard.append([InlineKeyboardButton(f"👑 {plan.replace('_', ' ').capitalize()} - {details.get('price')}", callback_data=f"select_plan_{plan}")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_main_menu_reply_from_inline')])
    return InlineKeyboardMarkup(keyboard)

def get_platform_selection_keyboard(action):
    keyboard = [
        [InlineKeyboardButton("📘 Facebook", callback_data=f"{action}_platform_facebook")],
        [InlineKeyboardButton("▶️ YouTube", callback_data=f"{action}_platform_youtube")],
        [InlineKeyboardButton("⬅️ Back", callback_data="settings_my_plan")]
    ]
    return InlineKeyboardMarkup(keyboard)


# === USER STATES (for sequential conversation flows) ===
user_states = {}

# Upload Flow States (Revised for new flow)
AWAITING_UPLOAD_TYPE_SELECTION = "awaiting_upload_type_selection"
AWAITING_UPLOAD_FILE = "awaiting_upload_file"
AWAITING_UPLOAD_TITLE = "awaiting_upload_title"
AWAITING_UPLOAD_DESCRIPTION = "awaiting_upload_description"
AWAITING_UPLOAD_TAGS = "awaiting_upload_tags"
AWAITING_UPLOAD_THUMBNAIL = "awaiting_upload_thumbnail"
AWAITING_CUSTOM_THUMBNAIL = "awaiting_custom_thumbnail"
AWAITING_UPLOAD_VISIBILITY = "awaiting_upload_visibility"
AWAITING_UPLOAD_SCHEDULE = "awaiting_upload_schedule"
AWAITING_UPLOAD_SCHEDULE_DATETIME = "awaiting_upload_schedule_datetime"

# Settings & Login States
AWAITING_FB_TITLE = "awaiting_fb_title"
AWAITING_FB_TAG = "awaiting_fb_tag"
AWAITING_FB_DESCRIPTION = "awaiting_fb_description"
AWAITING_FB_SCHEDULE_TIME = "awaiting_fb_schedule_time"
AWAITING_FB_OAUTH_TOKEN = "awaiting_fb_oauth_token"
AWAITING_FB_PAGE_SELECTION = "awaiting_fb_page_selection"
AWAITING_YT_TITLE = "awaiting_yt_title"
AWAITING_YT_TAG = "awaiting_yt_tag"
AWAITING_YT_DESCRIPTION = "awaiting_yt_description"
AWAITING_YT_SCHEDULE_TIME = "awaiting_yt_schedule_time"
AWAITING_YT_CLIENT_ID = "awaiting_yt_client_id"
AWAITING_YT_CLIENT_SECRET = "awaiting_yt_client_secret"
AWAITING_YT_AUTH_CODE = "awaiting_yt_auth_code"

# Admin States
AWAITING_BROADCAST_MESSAGE = "awaiting_broadcast_message"
AWAITING_ADD_USER = "admin_awaiting_user_id_to_add"
AWAITING_REMOVE_USER = "admin_awaiting_user_id_to_remove"
AWAITING_ADD_PREMIUM = "awaiting_add_premium_details"
AWAITING_NEW_CONCURRENT_UPLOADS = "awaiting_new_concurrent_uploads"
AWAITING_NEW_PAYMENT_QR = "awaiting_new_payment_qr"
AWAITING_NEW_PAYMENT_INFO = "awaiting_new_payment_info"
AWAITING_USER_ID_FOR_INSPECTION = "awaiting_user_id_for_inspection"


# === CONCURRENCY LOCKS ===
user_locks = {}
upload_semaphore = None

def with_user_lock(func):
    @wraps(func)
    async def wrapper(client, message, *args, **kwargs):
        user_id = message.from_user.id
        if user_id in user_locks:
            await message.reply("⚠️ **Please wait.** Another operation is already in progress. Please finish the current task or send `/start` to cancel.")
            return
        user_locks[user_id] = True
        try:
            return await func(client, message, *args, **kwargs)
        finally:
            if user_id in user_locks:
                del user_locks[user_id]
    return wrapper

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

def set_premium_plan(user_id, platform, plan_tier):
    """Grants a premium plan to a user for a specific platform."""
    user_doc = get_user_data(user_id)
    if not user_doc:
        # If user doesn't exist, we can't add a plan.
        # This could happen if an admin tries to add premium to an ID that never started the bot.
        logger.error(f"Attempted to set premium for non-existent user {user_id}")
        return False
        
    current_expiry_str = user_doc.get("premium", {}).get(platform, {}).get("until")
    
    # Start from now if no current plan or it has expired
    start_date = datetime.utcnow()
    if current_expiry_str:
        try:
            current_expiry = datetime.fromisoformat(current_expiry_str)
            if current_expiry > start_date:
                start_date = current_expiry # Stack plan duration on top of existing one
        except ValueError:
            logger.warning(f"Could not parse expiry date '{current_expiry_str}' for user {user_id}. Resetting.")

    plan_info = PREMIUM_PLANS.get(plan_tier)
    if not plan_info:
        logger.error(f"Invalid plan tier '{plan_tier}' for user {user_id}.")
        return False
    
    new_expiry = start_date + timedelta(days=plan_info['duration_days'])
    
    premium_data = {
        f"premium.{platform}.type": plan_tier,
        f"premium.{platform}.until": new_expiry.isoformat(),
        f"premium.{platform}.status": "active"
    }
    
    update_user_data(user_id, premium_data)
    logger.info(f"User {user_id} granted premium '{plan_tier}' for {platform}.")
    return True

def get_user_plan_and_expiry(user_id, platform=None):
    """
    Retrieves user's premium plan and expiry, checking if it's still active.
    If no platform is specified, checks for ANY active premium plan.
    """
    user_doc = get_user_data(user_id)
    if not user_doc:
        return "free", None, 0
    
    premium_data = user_doc.get("premium", {})
    now = datetime.utcnow()

    platforms_to_check = [platform] if platform else premium_data.keys()

    for p_name in platforms_to_check:
        p_data = premium_data.get(p_name, {})
        if p_data.get("status") == "active":
            try:
                expiry_date = datetime.fromisoformat(p_data["until"])
                if expiry_date > now:
                    return p_data["type"], expiry_date, user_doc.get("uploads_today", {}).get(p_name, 0)
                else:
                    # Plan has expired, update status in DB
                    update_user_data(user_id, {f"premium.{p_name}.status": "expired"})
            except (ValueError, TypeError):
                # Invalid date format, treat as expired
                update_user_data(user_id, {f"premium.{p_name}.status": "expired"})

    return "free", None, 0

def is_premium_user(user_id, platform=None):
    """Checks if a user has an active premium plan for a specific platform or any platform."""
    plan, _, _ = get_user_plan_and_expiry(user_id, platform)
    return plan not in ["free", "trial"]

def has_upload_quota(user_id, platform):
    """Checks if a user has remaining upload quota for the day."""
    user_doc = get_user_data(user_id)
    if not user_doc: return False
    
    plan_tier, _, _ = get_user_plan_and_expiry(user_id, platform)
    uploads_today = user_doc.get("uploads_today", {}).get(platform, 0)
    last_upload_date_iso = user_doc.get("last_upload_date", {}).get(platform)

    if last_upload_date_iso:
        last_upload_date = datetime.fromisoformat(last_upload_date_iso)
        if last_upload_date.date() < datetime.utcnow().date():
            # It's a new day, reset the quota
            users_collection.update_one(
                {"_id": user_id},
                {"$set": {f"uploads_today.{platform}": 0}}
            )
            uploads_today = 0
    
    quota = PREMIUM_PLANS.get(plan_tier, {}).get("upload_quota", 0)
    return uploads_today < quota


async def log_to_channel(client, message_text):
    """Sends a message to the designated log channel."""
    try:
        await client.send_message(LOG_CHANNEL_ID, f"**Bot Log:**\n\n{message_text}", parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Failed to send message to log channel (ID: {LOG_CHANNEL_ID}): {e}")

def get_facebook_page_info(user_id):
    """Retrieves Facebook access token and selected page info from user data."""
    user_doc = get_user_data(user_id)
    if not user_doc:
        return None, None
    selected_page_id = user_doc.get("facebook_selected_page_id")
    if not selected_page_id:
        return None, None
    
    for page in user_doc.get("facebook_pages", []):
        if page['id'] == selected_page_id:
            return page['access_token'], page['id']
            
    return None, None

def get_facebook_pages_from_token(user_access_token):
    """Fetches list of Facebook pages managed by the user token."""
    try:
        pages_url = f"https://graph.facebook.com/v19.0/me/accounts?access_token={user_access_token}"
        response = requests.get(pages_url)
        response.raise_for_status()
        return response.json().get('data', [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Facebook pages: {e}")
        return []

def get_youtube_credentials(user_id):
    """Retrieves and refreshes credentials for the ACTIVE YouTube account."""
    user_doc = get_user_data(user_id)
    if not user_doc: return None

    active_account_id = user_doc.get("youtube_active_account_id")
    if not active_account_id: return None

    yt_account = next((acc for acc in user_doc.get("youtube_accounts", []) if acc.get("account_id") == active_account_id), None)
    if not yt_account or not yt_account.get('refresh_token'): return None
    
    try:
        creds = Credentials(
            token=yt_account.get('access_token'),
            refresh_token=yt_account.get('refresh_token'),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=yt_account.get('client_id'),
            client_secret=yt_account.get('client_secret'),
            scopes=GOOGLE_API_SCOPES
        )

        if creds.expired and creds.refresh_token:
            creds.refresh(requests.Request())
            users_collection.update_one(
                {"_id": user_id, "youtube_accounts.account_id": active_account_id},
                {
                    "$set": {
                        "youtube_accounts.$.access_token": creds.token,
                        "youtube_accounts.$.token_expiry": creds.expiry.isoformat()
                    }
                }
            )
            logger.info(f"Refreshed YouTube token for user {user_id}, account {active_account_id}.")
        return creds
    except Exception as e:
        logger.error(f"Failed to refresh/get YouTube credentials for user {user_id}: {e}")
        return None

def process_video_for_upload(input_path):
    """Processes video to a web-optimized MP4 format using FFmpeg."""
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_path = f"downloads/processed_{base_name}_{uuid4().hex[:6]}.mp4"
    
    command = [
        "ffmpeg", "-y", "-i", input_path,
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        output_path
    ]
    
    try:
        logger.info(f"Starting video processing for: {input_path}")
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=900)
        logger.info(f"Successfully processed video. Output: {output_path}")
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"[FFmpeg] Processing failed for {input_path}. STDERR: {e.stderr}")
        raise RuntimeError(f"FFmpeg processing error: {e.stderr}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during FFmpeg processing: {e}")
        raise e

def upload_facebook_content(file_path, content_type, title, description, access_token, page_id, visibility="PUBLISHED", schedule_time=None, tags=[], location=None):
    """Uploads content to Facebook Page using Graph API."""
    if not all([file_path, content_type, access_token, page_id]):
        raise ValueError("Missing required parameters for Facebook content upload.")

    params = {
        'access_token': access_token,
        'published': 'true' if not schedule_time and visibility.lower() != 'draft' else 'false',
    }

    if content_type in ["video", "reels"]:
        post_url = f"https://graph-video.facebook.com/v19.0/{page_id}/videos"
        params['title'] = title
        params['description'] = description
        if schedule_time:
            params['scheduled_publish_time'] = int(schedule_time.timestamp())
            params['published'] = 'false'
        
        with open(file_path, 'rb') as f:
            files = {'source': f}
            response = requests.post(post_url, data=params, files=files)

    elif content_type == "photo":
        post_url = f"https://graph.facebook.com/v19.0/{page_id}/photos"
        params['caption'] = description if description else title
        if schedule_time:
            params['scheduled_publish_time'] = int(schedule_time.timestamp())
            params['published'] = 'false'
        
        with open(file_path, 'rb') as f:
            files = {'source': f}
            response = requests.post(post_url, data=params, files=files)
            
    else:
        raise ValueError(f"Unsupported Facebook content type: {content_type}")

    response.raise_for_status()
    result = response.json()
    logger.info(f"Facebook {content_type} upload result: {result}")
    return result

def generate_thumbnail(video_path):
    """Generates a thumbnail from a video."""
    base_name = os.path.splitext(os.path.basename(video_path))[0]
    output_path = f"downloads/thumb_{base_name}.jpg"
    
    try:
        # Tries to find a visually interesting frame
        command = [
            "ffmpeg", "-i", video_path, "-vf", "select='gt(scene,0.4)',scale=1280:-1",
            "-frames:v", "1", "-q:v", "2", "-y", output_path
        ]
        # Fallback to a fixed timestamp if the smart select fails
        fallback_command = [
            "ffmpeg", "-ss", "5", "-i", video_path, "-vframes", "1", 
            "-q:v", "2", "-y", output_path
        ]

        try:
            subprocess.run(command, check=True, capture_output=True, text=True, timeout=60)
        except subprocess.CalledProcessError:
            logger.warning("Smart thumbnail generation failed, falling back to fixed timestamp.")
            subprocess.run(fallback_command, check=True, capture_output=True, text=True, timeout=60)
        
        return output_path
    except Exception as e:
        logger.error(f"Failed to generate thumbnail for {video_path}: {e}")
        return None

def apply_template(template, user_data, job_data):
    """Applies template variables to a string."""
    if not template: return ""
    replacements = {
        '{filename}': job_data.get('source_filename', ''),
        '{duration}': str(timedelta(seconds=int(job_data.get('duration', 0)))),
        '{date}': datetime.now().strftime('%Y-%m-%d'),
        '{platform}': job_data.get('platform', '').capitalize(),
        '{brand}': 'Auto Uploader Pro',
        '{hashtags}': ' '.join(user_data.get(f'{job_data.get("platform", "")}_settings', {}).get('tag', '').split()),
    }
    for key, value in replacements.items():
        template = template.replace(key, str(value))
    return template

async def download_progress_callback(current, total, *args):
    """Sends progress updates during file download."""
    client, message, start_time = args
    try:
        now = time.time()
        # Update every 2 seconds to avoid being rate-limited
        if not hasattr(message, "last_update_time") or now - message.last_update_time > 2:
            percentage = int((current / total) * 100)
            
            time_elapsed = now - start_time
            if time_elapsed > 0:
                speed = current / time_elapsed
                eta_seconds = (total - current) / speed if speed > 0 else 0
                
                progress_str = (
                    f"⬇️ **Downloading...**\n"
                    f"`{current / 1024 / 1024:.2f} MB` of `{total / 1024 / 1024:.2f} MB` (`{percentage}%`)\n"
                    f"Speed: `{speed / 1024:.2f} KB/s` | ETA: `{timedelta(seconds=int(eta_seconds))}`"
                )
                await message.edit_text(progress_str)
                message.last_update_time = now
    except Exception as e:
        logger.debug(f"Failed to update download progress message: {e}")

async def upload_progress_callback(status_msg, progress):
    """Asynchronously edits a message to show upload progress."""
    try:
        now = time.time()
        if not hasattr(status_msg, "last_upload_update_time") or now - status_msg.last_upload_update_time > 2:
            percentage = int(progress.progress() * 100)
            await status_msg.edit_text(f"📤 **Uploading to YouTube...** `{percentage}%`")
            status_msg.last_upload_update_time = now
    except Exception as e:
        logger.debug(f"Could not update upload progress message: {e}")

# === PYROGRAM HANDLERS ===
@app.on_message(filters.command("start"))
@with_user_lock
async def start_command(client, message):
    user_id = message.from_user.id
    user_first_name = message.from_user.first_name or "User"
    user_username = message.from_user.username or "N/A"
    existing_user_doc = get_user_data(user_id)

    if not existing_user_doc:
        user_data = {
            "_id": user_id, "first_name": user_first_name, "username": user_username,
            "last_active": datetime.utcnow(), "role": "user", "premium": {}, "uploads_today": {},
            "last_upload_date": {}, "facebook_pages": [], "youtube_accounts": [],
            "facebook_selected_page_id": None, "youtube_active_account_id": None,
            "facebook_settings": {"title": "Default Title", "tag": "#video", "description": "Uploaded via Bot"},
            "youtube_settings": {"title": "Default Title", "tag": "video", "description": "Uploaded via Bot"},
            "added_at": datetime.utcnow()
        }
        user_data["premium"]["facebook"] = {"type": "trial", "until": (datetime.utcnow() + timedelta(days=PREMIUM_PLANS["trial"]["duration_days"])).isoformat(), "status": "active"}
        user_data["premium"]["youtube"] = {"type": "trial", "until": (datetime.utcnow() + timedelta(days=PREMIUM_PLANS["trial"]["duration_days"])).isoformat(), "status": "active"}
        if user_id == OWNER_ID:
            user_data["role"] = "admin"
            user_data["premium"]["facebook"] = {"type": "lifetime", "until": (datetime.utcnow() + timedelta(days=365*100)).isoformat(), "status": "active"}
            user_data["premium"]["youtube"] = {"type": "lifetime", "until": (datetime.utcnow() + timedelta(days=365*100)).isoformat(), "status": "active"}
        users_collection.insert_one(user_data)
        logger.info(f"New user {user_id} created.")
    else:
        update_user_data(user_id, {"first_name": user_first_name, "username": user_username, "last_active": datetime.utcnow()})
        logger.info(f"User {user_id} updated their info.")

    if is_admin(user_id):
        welcome_msg = f"🤖 **Welcome back, Administrator {user_first_name}!**\n\nYou have full system access."
    else:
        welcome_msg = (
            f"👋 **Greetings, {user_first_name}!**\n\n"
            "This bot helps you upload videos effortlessly.\n"
            f"Contact **@{ADMIN_TOM_USERNAME}** to upgrade your access.\n\n"
            f"🆔 Your System User ID: `{user_id}`"
        )
    
    reply_markup = get_main_menu(user_id)
    # Send welcome image only to non-premium new users
    if not is_premium_user(user_id) and not existing_user_doc:
        await client.send_photo(
            chat_id=message.chat.id, photo=CHANNEL_PHOTO_URL, caption=welcome_msg,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Join Our Channel ✅", url=CHANNEL_LINK)]]),
            parse_mode=enums.ParseMode.MARKDOWN
        )
        await message.reply("Please use the menu below to navigate.", reply_markup=reply_markup)
    else:
        await message.reply(welcome_msg, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)

# === NEW COMMANDS ===
@app.on_message(filters.command("premiumplan"))
async def cmd_premium_plan(client, message):
    await message.reply(
        "👑 **Elevate Your Experience**\n\n"
        "Unlock limitless uploads and advanced features with our premium plans.\n\n"
        "**Choose a plan:**",
        reply_markup=get_premium_plan_keyboard()
    )

@app.on_message(filters.command("premiumdetails"))
async def cmd_premium_details(client, message):
    user_id = message.from_user.id
    user_doc = get_user_data(user_id)
    if not user_doc:
        return await message.reply("Please send /start first to register.")

    message_text = "👑 **Your Current Subscription Status:**\n\n"
    has_active_plan = False
    
    platforms = ["facebook", "youtube"]
    for platform in platforms:
        plan_tier, expiry, _ = get_user_plan_and_expiry(user_id, platform)
        if plan_tier not in ["free"]:
            has_active_plan = True
            expiry_str = "Never" if plan_tier == 'lifetime' else expiry.strftime('%Y-%m-%d %H:%M:%S UTC')
            remaining = "∞" if plan_tier == 'lifetime' else str(expiry - datetime.utcnow()).split('.')[0]
            
            message_text += f"**{platform.capitalize()} Plan:** `{plan_tier.replace('_', ' ').capitalize()}`\n"
            message_text += f"  - **Expires On:** `{expiry_str}`\n"
            message_text += f"  - **Time Remaining:** `{remaining}`\n\n"

    if not has_active_plan:
        message_text += "You are currently on the **Free** plan.\n"
        message_text += "Use /premiumplan to see upgrade options."

    await message.reply(message_text, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command(["fblogin", "flogin"]))
async def cmd_fb_login(client, message):
    await fb_oauth_login_prompt(client, message)

@app.on_message(filters.command(["ytlogin", "ylogin"]))
async def cmd_yt_login(client, message):
    await yt_login_prompt(client, message)

@app.on_message(filters.command("skip"))
async def cmd_skip(client, message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    if not state: return

    step = state.get("step")
    platform = state.get("platform")
    user_doc = get_user_data(user_id)

    if step == AWAITING_UPLOAD_TITLE:
        state["title"] = apply_template(user_doc.get(f"{platform}_settings", {}).get("title", "Default Title"), user_doc, state)
        await message.reply(f"✅ **Title Skipped.** Using default: '{state['title']}'.")
        # This is the crucial part: start the download after getting the title
        await start_download_and_processing(client, message, user_id)
    
    elif step == AWAITING_UPLOAD_DESCRIPTION:
        state["description"] = apply_template(user_doc.get(f"{platform}_settings", {}).get("description", "Default Description"), user_doc, state)
        await message.reply(f"✅ **Description Skipped.** Using default description.")
        await prompt_tags(client, message, user_id, platform)

    elif step == AWAITING_UPLOAD_TAGS:
        state["tags"] = user_doc.get(f"{platform}_settings", {}).get("tag", "Default Tags").split()
        await message.reply(f"✅ **Tags Skipped.** Using default tags.")
        await prompt_thumbnail(client, message)
        
    else:
        await message.reply("`/skip` is not applicable at this stage.")


# === NEW: Central router for reply keyboard buttons ===
@app.on_message(filters.text & filters.private)
async def route_reply_buttons(client, message):
    text = (message.text or "").lower()
    user_id = message.from_user.id

    # Check if user is in a state first to avoid interrupting flows
    if user_states.get(user_id):
        # Allow specific text inputs for states
        step = user_states[user_id].get("step")
        if step in [
            AWAITING_UPLOAD_TITLE, AWAITING_UPLOAD_DESCRIPTION, AWAITING_UPLOAD_TAGS,
            AWAITING_YT_CLIENT_ID, AWAITING_YT_CLIENT_SECRET, AWAITING_YT_AUTH_CODE,
            AWAITING_FB_OAUTH_TOKEN, AWAITING_USER_ID_FOR_INSPECTION, AWAITING_ADD_USER,
            AWAITING_REMOVE_USER, AWAITING_BROADCAST_MESSAGE,
        ]:
            # These states expect text input, so let their handlers process it
            return
        
    # If not in a text-expecting state, process as a button press
    if "upload to youtube" in text:
        await prompt_youtube_upload(client, message)
    elif "upload to facebook" in text:
        await prompt_facebook_upload(client, message)
    elif "premium plans" in text:
        await premium_plans_reply(client, message)
    elif "settings" in text:
        await show_main_settings_menu_reply(client, message)
    elif "admin panel" in text and is_admin(user_id):
        await admin_panel_menu_reply(client, message)
    elif "main menu" in text:
        await back_to_main_menu_reply(client, message)
    # Fallback for other text messages if they don't match a state or button
    # You might want to add a message here like "I don't understand that command."


async def show_main_settings_menu_reply(client, message):
    user_id = message.from_user.id
    user_doc = get_user_data(user_id)
    if not user_doc:
        return await message.reply("⛔ **Access Denied!** Please send `/start` first.")
    if not is_premium_user(user_id) and not is_admin(user_id):
        return await message.reply("⛔ **Access Restricted.** Settings are available for premium users and admins.")
    
    await message.reply("⚙️ **System Configuration Interface:**\n\nChoose your settings options:", reply_markup=get_general_settings_inline_keyboard(user_id))
    logger.info(f"User {user_id} accessed main settings menu.")

async def back_to_main_menu_reply(client, message):
    user_id = message.from_user.id
    user_states.pop(user_id, None)
    await message.reply("✅ **Returning to Main System Interface.**", reply_markup=get_main_menu(user_id))
    logger.info(f"User {user_id} returned to main menu via reply button.")

async def premium_plans_reply(client, message):
    await message.reply(
        "👑 **Elevate Your Experience**\n\n"
        "Unlock limitless uploads and advanced features with our premium plans.\n\n"
        "**Choose a plan:**",
        reply_markup=get_premium_plan_keyboard()
    )

@app.on_callback_query(filters.regex("^select_plan_"))
async def premium_plan_selection(client, callback_query):
    plan = callback_query.data.split('_')[-1]
    
    await callback_query.message.edit_text(
        f"You have selected the **{plan.replace('_', ' ').capitalize()}** plan.\n\n"
        "Please choose the platform you want to apply this plan to:",
        reply_markup=get_platform_selection_keyboard('buy_premium')
    )
    user_states[callback_query.from_user.id] = {"step": "awaiting_platform_for_purchase", "plan": plan}

@app.on_callback_query(filters.regex("^buy_premium_platform_"))
async def handle_premium_purchase(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id, {})
    plan = state.get("plan")
    platform = callback_query.data.split('_')[-1]

    if not plan:
        return await callback_query.answer("❌ Session expired. Please restart from the Premium Plans menu.", show_alert=True)

    await callback_query.answer("Generating payment details...")
    
    settings = settings_collection.find_one({"_id": "global_config"})
    payment_info = settings.get("payment_info", f"To proceed, please contact the administrator: @{ADMIN_TOM_USERNAME}")
    payment_qr = settings.get("payment_qr_code", "https://i.postimg.cc/SXDxJ92z/x.jpg")
    
    message_text = (
        f"**💰 Payment for {platform.capitalize()} Premium ({plan.replace('_', ' ').capitalize()} Plan)**\n\n"
        f"**Price:** {PREMIUM_PLANS[plan]['price']}\n\n"
        f"{payment_info}"
    )
    
    await client.send_photo(
        chat_id=user_id,
        photo=payment_qr,
        caption=message_text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ I Have Paid", callback_data="payment_confirm")]
        ])
    )
    user_states.pop(user_id, None)

@app.on_callback_query(filters.regex("^settings_main_menu_inline$"))
async def settings_main_menu_inline_callback(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Accessing settings...")
    await callback_query.edit_message_text(
        "⚙️ **System Configuration Interface:**\n\nChoose your settings options:",
        reply_markup=get_general_settings_inline_keyboard(user_id)
    )
    logger.info(f"User {user_id} navigated to general settings via inline button.")

@app.on_callback_query(filters.regex("^back_to_main_menu_reply_from_inline$"))
async def back_to_main_menu_from_inline(client, callback_query):
    user_id = callback_query.from_user.id
    user_states.pop(user_id, None)
    await callback_query.answer("System redirection initiated...")

    await client.send_message(user_id, "✅ **Returning to Main System Interface.**", reply_markup=get_main_menu(user_id))
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
    await callback_query.edit_message_text(
        "⚙️ **User Account Settings:**\n\nChoose a platform to configure:",
        reply_markup=user_settings_inline_menu
    )
    logger.info(f"User {user_id} accessed user settings menu.")

@app.on_callback_query(filters.regex("^settings_my_plan$"))
async def show_my_plan(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Fetching plan details...")
    
    # Reuse the logic from the /premiumdetails command
    details_message = await get_premium_details_text(user_id)
    
    await callback_query.message.edit_text(details_message, reply_markup=user_settings_inline_menu, parse_mode=enums.ParseMode.MARKDOWN)

async def get_premium_details_text(user_id):
    """Helper function to generate premium details text for reuse."""
    user_doc = get_user_data(user_id)
    if not user_doc:
        return "User not found. Please /start the bot."

    message_text = "👑 **Your Current Subscription Status:**\n\n"
    has_active_plan = False
    
    platforms = ["facebook", "youtube"]
    for platform in platforms:
        plan_tier, expiry, _ = get_user_plan_and_expiry(user_id, platform)
        if plan_tier not in ["free"]:
            has_active_plan = True
            expiry_str = "Never" if plan_tier == 'lifetime' else expiry.strftime('%Y-%m-%d %H:%M:%S UTC')
            remaining = "∞" if plan_tier == 'lifetime' else str(expiry - datetime.utcnow()).split('.')[0]
            
            message_text += f"**{platform.capitalize()} Plan:** `{plan_tier.replace('_', ' ').capitalize()}`\n"
            message_text += f"  - **Expires On:** `{expiry_str}`\n"
            message_text += f"  - **Time Remaining:** `{remaining}`\n\n"

    if not has_active_plan:
        message_text += "You are currently on the **Free** plan.\n"
        message_text += "Upgrade to a full plan for unlimited access!"

    return message_text


# --- Admin Panel Handlers ---
async def admin_panel_menu_reply(client, message):
    user_id = message.from_user.id
    await message.reply("👋 **Welcome to the Administrator Command Center!**", reply_markup=admin_markup)
    logger.info(f"Admin {user_id} accessed the Admin Panel.")

@app.on_callback_query(filters.regex("^admin_users_list$"))
async def admin_users_list_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        return await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
    await callback_query.answer("Fetching system user directory...")
    all_users = list(users_collection.find({}, {"_id": 1, "first_name": 1, "username": 1, "role": 1, "premium": 1}))
    user_list_text = "**👥 Registered System Users:**\n\n"
    if not all_users:
        user_list_text += "No user records found in the system database."
    else:
        for user in all_users:
            role = user.get("role", "user").capitalize()
            premium_status = []
            for platform, data in user.get("premium", {}).items():
                if data.get("status") == "active":
                    try:
                        expiry = datetime.fromisoformat(data['until'])
                        expiry_info = f"Expires: `{expiry.strftime('%Y-%m-%d')}`" if data['type'] != 'lifetime' else "Lifetime"
                        premium_status.append(f"{platform.capitalize()}: {data['type'].replace('_', ' ').capitalize()} ({expiry_info})")
                    except (ValueError, TypeError):
                        premium_status.append(f"{platform.capitalize()}: Invalid Date")
            
            premium_info = "\n  " + "\n  ".join(premium_status) if premium_status else " Free"
            
            user_list_text += (
                f"• ID: `{user['_id']}`\n"
                f"  Name: `{user.get('first_name', 'N/A')}`\n"
                f"  Username: `@{user.get('username', 'N/A')}`\n"
                f"  Status: `{role}`, Premium: `{premium_info}`\n\n"
            )
    await callback_query.edit_message_text(user_list_text, reply_markup=admin_markup, parse_mode=enums.ParseMode.MARKDOWN)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) viewed system users list.")

@app.on_callback_query(filters.regex("^admin_get_user_info_prompt$"))
async def admin_get_user_info_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        return await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
    
    await callback_query.answer("Ready to inspect user.")
    user_states[user_id] = {"step": AWAITING_USER_ID_FOR_INSPECTION}
    await callback_query.edit_message_text(
        "**🕵️ User Inspector**\n\n"
        "Please send the **Telegram User ID** of the user you wish to inspect.",
        reply_markup=admin_markup
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.from_user.id, {}).get("step") == AWAITING_USER_ID_FOR_INSPECTION))
@with_user_lock
async def admin_inspect_user(client, message):
    admin_id = message.from_user.id
    if not is_admin(admin_id): return

    try:
        target_user_id = int(message.text.strip())
    except ValueError:
        return await message.reply("❌ **Invalid ID.** Please provide a numeric Telegram User ID.", reply_markup=admin_markup)
    
    user_doc = get_user_data(target_user_id)
    
    if not user_doc:
        return await message.reply(f"❌ **User Not Found.** No user with ID `{target_user_id}` exists in the database.", reply_markup=admin_markup)

    info_text = f"**🕵️ User Profile: `{target_user_id}`**\n\n"
    info_text += f"**Name:** `{user_doc.get('first_name', 'N/A')}`\n"
    info_text += f"**Username:** `@{user_doc.get('username', 'N/A')}`\n"
    info_text += f"**Role:** `{user_doc.get('role', 'user').capitalize()}`\n"
    
    join_date = user_doc.get('added_at')
    if join_date:
        info_text += f"**Date Joined:** `{join_date.strftime('%Y-%m-%d %H:%M')} UTC`\n"

    info_text += "\n**⭐ Premium Status:**\n"
    premium_details_text = await get_premium_details_text(target_user_id)
    info_text += premium_details_text.replace("👑 **Your Current Subscription Status:**\n\n", "")

    info_text += "\n**📊 Upload Stats (Today):**\n"
    uploads_today = user_doc.get('uploads_today', {})
    info_text += f"  - Facebook: `{uploads_today.get('facebook', 0)}`\n"
    info_text += f"  - YouTube: `{uploads_today.get('youtube', 0)}`\n"

    info_text += "\n**🔗 Linked Accounts:**\n"
    fb_pages = user_doc.get('facebook_pages', [])
    if fb_pages:
        info_text += "**Facebook Pages:**\n"
        for page in fb_pages:
            active_marker = " (Active)" if page.get('id') == user_doc.get('facebook_selected_page_id') else ""
            info_text += f"  - `{page.get('name', 'N/A')}`{active_marker}\n"
    else:
        info_text += "  - No Facebook pages linked.\n"

    yt_accounts = user_doc.get('youtube_accounts', [])
    if yt_accounts:
        info_text += "**YouTube Channels:**\n"
        for acc in yt_accounts:
            active_marker = " (Active)" if acc.get('account_id') == user_doc.get('youtube_active_account_id') else ""
            info_text += f"  - `{acc.get('channel_name', 'N/A')}`{active_marker}\n"
    else:
        info_text += "  - No YouTube channels linked.\n"

    await message.reply(info_text, reply_markup=admin_markup, parse_mode=enums.ParseMode.MARKDOWN)
    user_states.pop(admin_id, None)


@app.on_callback_query(filters.regex("^admin_add_user_prompt$"))
async def admin_add_user_prompt_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        return await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
    await callback_query.answer("Initiating user upgrade protocol...")
    user_states[user_id] = {"step": AWAITING_ADD_PREMIUM}
    
    keyboard = []
    for platform in ["facebook", "youtube"]:
        for plan, details in PREMIUM_PLANS.items():
            if plan in ["free", "trial"]: continue
            keyboard.append([InlineKeyboardButton(f"Add {platform.capitalize()} {plan.replace('_', ' ').capitalize()}", callback_data=f"admin_add_user_plan_{platform}_{plan}")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="back_to_admin_panel")])
    
    await callback_query.edit_message_text(
        "Please select the **plan** and **platform** you wish to grant to a user.\n"
        "After selection, I will ask for the user ID.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    logger.info(f"Admin {user_id} prompted to add premium user.")

@app.on_callback_query(filters.regex("^admin_add_user_plan_"))
async def admin_add_user_plan_selection(client, callback_query):
    user_id = callback_query.from_user.id
    parts = callback_query.data.split('_')
    platform = parts[4]
    plan = '_'.join(parts[5:]) # Handle plans with underscores like "1_month"
    
    user_states[user_id].update({"platform": platform, "plan": plan, "step": AWAITING_ADD_USER})

    await callback_query.answer(f"Selected {platform.capitalize()} {plan.replace('_', ' ').capitalize()} plan.")
    await callback_query.edit_message_text(
        "Please transmit the **Telegram User ID** of the user you wish to grant this plan.\n"
        "Example: `123456789`",
        reply_markup=admin_markup
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_ADD_USER) & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
@with_user_lock
async def admin_add_user_id_input(client, message):
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    target_user_id_str = message.text.strip()
    
    try:
        target_user_id = int(target_user_id_str)
        if set_premium_plan(target_user_id, state["platform"], state["plan"]):
            await message.reply(f"✅ **Success!** User `{target_user_id}` has been granted **PREMIUM** status for **{state['platform'].capitalize()}**.", reply_markup=admin_markup)
            try:
                await client.send_message(target_user_id, f"🎉 **System Notification!** Your premium access for {state['platform'].capitalize()} has been granted! Use /start to access your enhanced features.")
            except Exception as e:
                logger.warning(f"Could not notify user {target_user_id} about premium extension: {e}")
            await log_to_channel(client, f"Admin `{message.from_user.id}` granted premium for `{state['platform']}` to user `{target_user_id}`.")
        else:
            await message.reply("❌ **Error!** Failed to set premium plan. The user might not exist (they need to /start the bot once).", reply_markup=admin_markup)
    except (ValueError, KeyError):
        await message.reply("❌ **Input Error.** Invalid format or session data lost. Please use the buttons to restart the process.", reply_markup=admin_markup)
    finally:
        user_states.pop(user_id, None)

@app.on_callback_query(filters.regex("^admin_remove_user_prompt$"))
async def admin_remove_user_prompt_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        return await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
    await callback_query.answer("Initiating user downgrade protocol...")
    user_states[user_id] = {"step": AWAITING_REMOVE_USER}
    await callback_query.edit_message_text(
        "Please transmit the **Telegram User ID** of the user you wish to revoke **ALL PREMIUM ACCESS** from.",
        reply_markup=admin_markup
    )
    logger.info(f"Admin {user_id} prompted to remove premium user.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_REMOVE_USER) & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
@with_user_lock
async def admin_remove_user_id_input(client, message):
    user_id = message.from_user.id
    target_user_id_str = message.text.strip()
    user_states.pop(user_id, None)

    try:
        target_user_id = int(target_user_id_str)
        if target_user_id == OWNER_ID:
            return await message.reply("❌ **Security Alert!** Cannot revoke owner's premium status.", reply_markup=admin_markup)

        update_user_data(target_user_id, {"premium": {}})
        await message.reply(f"✅ **Success!** User `{target_user_id}` has been revoked from **PREMIUM ACCESS** on all platforms.", reply_markup=admin_markup)
        try:
            await client.send_message(target_user_id, "❗ **System Notification!** Your premium access has been revoked.")
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id} about premium revocation: {e}")
        await log_to_channel(client, f"Admin `{message.from_user.id}` revoked premium from user `{target_user_id}`.")

    except ValueError:
        await message.reply("❌ **Input Error.** Invalid User ID detected. Please transmit a numeric ID.", reply_markup=admin_markup)
    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during user premium revocation: `{e}`", reply_markup=admin_markup)
        logger.error(f"Failed to remove user for admin {user_id}: {e}")

@app.on_callback_query(filters.regex("^admin_broadcast_prompt$"))
async def admin_broadcast_prompt_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        return await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
    await callback_query.answer("Initiating broadcast transmission protocol...")
    user_states[user_id] = {"step": AWAITING_BROADCAST_MESSAGE}
    await callback_query.edit_message_text(
        "Please transmit the **message payload** you wish to broadcast to all active system users.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Terminate Broadcast", callback_data="cancel_broadcast")]])
    )
    logger.info(f"Admin {user_id} prompted for broadcast message.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_BROADCAST_MESSAGE) & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
@with_user_lock
async def broadcast_message_handler(client, message):
    user_id = message.from_user.id
    text_to_broadcast = message.text
    user_states.pop(user_id, None)

    await message.reply("📡 **Initiating Global Transmission...**")
    await log_to_channel(client, f"Broadcast initiated by `{user_id}`. Message preview: '{text_to_broadcast[:50]}...'")

    all_user_ids = [user["_id"] for user in users_collection.find({}, {"_id": 1})]
    success_count = 0
    fail_count = 0

    for target_user_id in all_user_ids:
        try:
            if target_user_id == user_id: continue
            await client.send_message(target_user_id, f"📢 **ADMIN BROADCAST MESSAGE:**\n\n{text_to_broadcast}")
            success_count += 1
            await asyncio.sleep(0.05) # To avoid hitting rate limits
        except Exception as e:
            fail_count += 1
            logger.warning(f"Failed to send broadcast to user {target_user_id}: {e}")

    await message.reply(f"✅ **Broadcast Transmission Complete.** Sent to `{success_count}` users, `{fail_count}` transmissions failed.", reply_markup=admin_markup)
    await log_to_channel(client, f"Broadcast finished by `{user_id}`. Transmitted: {success_count}, Failed: {fail_count}.")

@app.on_callback_query(filters.regex("^cancel_broadcast$"))
async def cancel_broadcast_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if user_states.get(user_id, {}).get("step") == AWAITING_BROADCAST_MESSAGE:
        user_states.pop(user_id, None)
        await callback_query.answer("Broadcast sequence terminated.")
        await callback_query.message.edit_text("🛑 **Broadcast Protocol Terminated.**", reply_markup=admin_markup)
    else:
        await callback_query.answer("No active broadcast protocol to terminate.", show_alert=True)

# ... Other admin handlers for bot settings, payment, restart, status ... (unchanged, kept for completeness)
@app.on_callback_query(filters.regex("^admin_bot_settings$"))
async def admin_bot_settings(client, callback_query):
    if not is_admin(callback_query.from_user.id): return
    settings = settings_collection.find_one({"_id": "global_config"})
    limit = settings.get("concurrent_uploads", 5)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Concurrent Uploads: {limit}", callback_data="admin_set_concurrent_uploads")],
        [InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="back_to_admin_panel")]
    ])
    await callback_query.edit_message_text("🔧 **Bot Settings:**\n\nConfigure global settings for the bot.", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^back_to_admin_panel$"))
async def back_to_admin_panel(client, callback_query):
    if not is_admin(callback_query.from_user.id): return
    await callback_query.edit_message_text("👋 **Administrator Command Center**", reply_markup=admin_markup)

@app.on_callback_query(filters.regex("^admin_payment_settings$"))
async def admin_payment_settings(client, callback_query):
    if not is_admin(callback_query.from_user.id): return
    settings = settings_collection.find_one({"_id": "global_config"})
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Set Payment QR Code URL", callback_data="admin_set_payment_qr")],
        [InlineKeyboardButton("Set Payment Info Text", callback_data="admin_set_payment_info")],
        [InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="back_to_admin_panel")]
    ])
    await callback_query.edit_message_text(f"💳 **Payment Settings**\n\n**Current QR URL:** `{settings.get('payment_qr_code')}`\n**Current Info:** `{settings.get('payment_info')}`", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^admin_set_concurrent_uploads$"))
async def admin_set_concurrent_uploads_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id): return
    user_states[user_id] = {"step": AWAITING_NEW_CONCURRENT_UPLOADS}
    await callback_query.edit_message_text("Please enter the new limit for concurrent uploads (e.g., 5).")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.from_user.id, {}).get("step") == AWAITING_NEW_CONCURRENT_UPLOADS))
async def admin_set_concurrent_uploads_save(client, message):
    user_id = message.from_user.id
    if not is_admin(user_id): return
    try:
        new_limit = int(message.text.strip())
        if not 1 <= new_limit <= 20: raise ValueError("Limit must be between 1 and 20.")
        settings_collection.update_one({"_id": "global_config"}, {"$set": {"concurrent_uploads": new_limit}})
        load_upload_semaphore()
        await message.reply(f"✅ Concurrent upload limit updated to **{new_limit}**.")
    except (ValueError, TypeError) as e:
        await message.reply(f"❌ Invalid input. Please provide a number between 1 and 20. Error: {e}")
    finally:
        user_states.pop(user_id, None)

@app.on_callback_query(filters.regex("^admin_set_payment_qr$"))
async def admin_set_payment_qr_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id): return
    user_states[user_id] = {"step": AWAITING_NEW_PAYMENT_QR}
    await callback_query.edit_message_text("Please send the new URL for the Payment QR Code image.")

@app.on_callback_query(filters.regex("^admin_set_payment_info$"))
async def admin_set_payment_info_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id): return
    user_states[user_id] = {"step": AWAITING_NEW_PAYMENT_INFO}
    await callback_query.edit_message_text("Please send the new text for payment instructions.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.from_user.id, {}).get("step") == AWAITING_NEW_PAYMENT_QR))
async def admin_set_payment_qr_save(client, message):
    user_id = message.from_user.id
    if not is_admin(user_id): return
    new_qr_url = message.text.strip()
    settings_collection.update_one({"_id": "global_config"}, {"$set": {"payment_qr_code": new_qr_url}})
    await message.reply(f"✅ Payment QR Code URL has been updated successfully.")
    user_states.pop(user_id, None)

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.from_user.id, {}).get("step") == AWAITING_NEW_PAYMENT_INFO))
async def admin_set_payment_info_save(client, message):
    user_id = message.from_user.id
    if not is_admin(user_id): return
    new_payment_info = message.text.strip()
    settings_collection.update_one({"_id": "global_config"}, {"$set": {"payment_info": new_payment_info}})
    await message.reply(f"✅ Payment Info text has been updated successfully.")
    user_states.pop(user_id, None)

@app.on_callback_query(filters.regex("^admin_restart_bot$"))
async def admin_restart_bot_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        return await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
    await callback_query.answer("System reboot sequence initiated...", show_alert=True)
    await callback_query.message.edit_text("🔄 **System Rebooting...** This may take a moment. Please send `/start` in a few seconds to re-establish connection.", reply_markup=None)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) initiated bot restart.")
    os.execv(sys.executable, ['python'] + sys.argv)

@app.on_callback_query(filters.regex("^settings_bot_status_inline$"))
async def settings_bot_status_inline_callback(client, callback_query):
    if not is_admin(callback_query.from_user.id): return
    await callback_query.answer("Fetching system diagnostics...")
    total_users = users_collection.count_documents({})
    premium_fb = users_collection.count_documents({"premium.facebook.status": "active", "premium.facebook.type": {"$nin": ["free", "trial"]}})
    premium_yt = users_collection.count_documents({"premium.youtube.status": "active", "premium.youtube.type": {"$nin": ["free", "trial"]}})
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    disk = psutil.disk_usage('/').percent
    stats_message = (
        f"**📊 System Diagnostics**\n\n"
        f"**👤 User Base:**\n"
        f"  - Total Users: `{total_users}`\n"
        f"  - FB Premium: `{premium_fb}` | YT Premium: `{premium_yt}`\n\n"
        f"**⚙️ System Health:**\n"
        f"  - CPU Usage: `{cpu}%`\n"
        f"  - RAM Usage: `{ram}%`\n"
        f"  - Disk Usage: `{disk}%`"
    )
    await callback_query.edit_message_text(stats_message, reply_markup=admin_markup, parse_mode=enums.ParseMode.MARKDOWN)

# --- Settings Handlers ---
@app.on_callback_query(filters.regex("^settings_facebook$"))
async def show_facebook_settings(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id, 'facebook') and not is_admin(user_id):
        return await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
    await callback_query.answer("Accessing Facebook configurations...")
    await callback_query.message.edit_text(
        "📘 **Facebook Configuration Module:**",
        reply_markup=facebook_settings_inline_menu
    )
    logger.info(f"User {user_id} accessed Facebook settings.")

@app.on_callback_query(filters.regex("^settings_youtube$"))
async def show_youtube_settings(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id, 'youtube') and not is_admin(user_id):
        return await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
    await callback_query.answer("Accessing YouTube configurations...")
    await callback_query.message.edit_text("▶️ **YouTube Configuration Module:**", reply_markup=youtube_settings_inline_menu)
    logger.info(f"User {user_id} accessed YouTube settings.")
    
# ... (All other setting handlers for Title, Tags, Description etc. are unchanged and kept for completeness)
# ... These will be triggered by the inline keyboards and work as before.
@app.on_callback_query(filters.regex("^fb_set_title$"))
async def fb_set_title_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting title input...")
    user_states[user_id] = {"step": AWAITING_FB_TITLE}
    await callback_query.edit_message_text(
        "📝 Please transmit the new default Facebook title.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_TITLE))
async def fb_set_title_save(client, message):
    user_id = message.from_user.id
    title = message.text.strip()
    user_states.pop(user_id, None)
    update_user_data(user_id, {"facebook_settings.title": title})
    await message.reply(f"✅ **Facebook Title Configured.** New default title set to: '{title}'", reply_markup=facebook_settings_inline_menu)

# ... The rest of the settings handlers for fb/yt tags, description, privacy, etc. remain the same.

# --- Account Login/Management Handlers ---
async def yt_login_prompt(client, message_or_callback):
    """Handles both /ytlogin command and settings button."""
    user_id = message_or_callback.from_user.id
    if not is_premium_user(user_id, 'youtube') and not is_admin(user_id):
        reply_func = getattr(message_or_callback, "answer", message_or_callback.reply)
        return await reply_func("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)

    if isinstance(message_or_callback, types.CallbackQuery):
        await message_or_callback.answer("Initiating YouTube OAuth 2.0 flow...")
        msg_func = message_or_callback.message.edit_text
    else:
        msg_func = message_or_callback.reply

    user_states[user_id] = {"step": AWAITING_YT_CLIENT_ID}
    await msg_func(
        "🔑 **YouTube OAuth 2.0 Setup (Step 1/3):**\n\n"
        "Please provide your **Google Client ID** from your Google Cloud Console's 'Credentials' page.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_youtube')]])
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_CLIENT_ID))
@with_user_lock
async def yt_get_client_id(client, message):
    user_id = message.from_user.id
    user_states[user_id]["client_id"] = message.text.strip()
    user_states[user_id]["step"] = AWAITING_YT_CLIENT_SECRET
    await message.reply(
        "**Step 2/3:** Now, please provide your **Google Client Secret**."
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_CLIENT_SECRET))
@with_user_lock
async def yt_get_client_secret(client, message):
    user_id = message.from_user.id
    client_secret = message.text.strip()
    client_id = user_states[user_id].get("client_id")
    
    if not client_id:
        user_states.pop(user_id, None)
        return await message.reply("❌ **Error:** Client ID was not found. Please restart the login process.", reply_markup=youtube_settings_inline_menu)

    try:
        temp_creds_json = {
            "web": {
                "client_id": client_id, "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [REDIRECT_URI], "scopes": GOOGLE_API_SCOPES
            }
        }
        
        flow = Flow.from_client_config(temp_creds_json, scopes=GOOGLE_API_SCOPES)
        flow.redirect_uri = REDIRECT_URI
        
        auth_url, state = flow.authorization_url(access_type='offline', include_granted_scopes='true')
        
        user_states[user_id].update({
            "client_secret": client_secret, "state": state,
            "step": AWAITING_YT_AUTH_CODE, "flow_config": temp_creds_json
        })
        
        await message.reply(
            f"**Step 3/3:** Click the link below to grant access to your YouTube channel:\n\n"
            f"[**Click here to authenticate with Google**]({auth_url})\n\n"
            "After you grant access, **copy the full URL from your browser's address bar** and paste it here.",
            parse_mode=enums.ParseMode.MARKDOWN, disable_web_page_preview=True
        )

    except Exception as e:
        user_states.pop(user_id, None)
        await message.reply(f"❌ **Error during URL generation:** `{e}`\nPlease check your Client ID and Secret, then restart the process.", reply_markup=youtube_settings_inline_menu)

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.from_user.id, {}).get("step") == AWAITING_YT_AUTH_CODE))
@with_user_lock
async def yt_get_auth_code(client, message):
    user_id = message.from_user.id
    state_data = user_states.get(user_id)
    if not state_data: return await message.reply("Session expired. Please restart the login process.")
    try:
        flow = Flow.from_client_config(state_data["flow_config"], scopes=GOOGLE_API_SCOPES, state=state_data["state"])
        flow.redirect_uri = REDIRECT_URI
        flow.fetch_token(authorization_response=message.text.strip())
        creds = flow.credentials
        
        youtube_api = build('youtube', 'v3', credentials=creds)
        channels_response = youtube_api.channels().list(part='snippet', mine=True).execute()
        channel_name = channels_response['items'][0]['snippet']['title'] if channels_response.get('items') else f"Account-{str(uuid4())[:4]}"
        
        new_account_id = str(uuid4())
        youtube_account_data = {
            "account_id": new_account_id, "channel_name": channel_name, "logged_in": True,
            "access_token": creds.token, "refresh_token": creds.refresh_token,
            "token_expiry": creds.expiry.isoformat(), "client_id": creds.client_id, "client_secret": creds.client_secret
        }
        
        users_collection.update_one(
            {"_id": user_id},
            {"$push": {"youtube_accounts": youtube_account_data}, "$set": {"youtube_active_account_id": new_account_id}},
            upsert=True
        )
        await message.reply(f"✅ **Success!** Channel **'{channel_name}'** has been linked and set as active.", reply_markup=youtube_settings_inline_menu)
    except Exception as e:
        await message.reply(f"❌ **Authentication Failed:** `{e}`. Please check the URL and try again.")
    finally:
        user_states.pop(user_id, None)


@app.on_callback_query(filters.regex("^yt_manage_accounts$"))
async def yt_manage_accounts_inline(client, callback_query):
    user_id = callback_query.from_user.id
    user_doc = get_user_data(user_id)
    accounts = user_doc.get("youtube_accounts", [])
    active_id = user_doc.get("youtube_active_account_id")
    keyboard = []
    if not accounts:
        await callback_query.answer("No YouTube accounts linked yet.", show_alert=True)
    else:
        for acc in accounts:
            is_active = "✅ " if acc.get("account_id") == active_id else ""
            channel_name = acc.get("channel_name", "Unknown Channel")
            keyboard.append([InlineKeyboardButton(f"{is_active}{channel_name}", callback_data=f"yt_switch_account_{acc['account_id']}")])
    keyboard.append([InlineKeyboardButton("🔗 Link New Account", callback_data="yt_login_prompt_from_menu")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Settings", callback_data="settings_youtube")])
    await callback_query.edit_message_text("**🔑 Manage Your YouTube Accounts:**\n\nSelect an account to make it active for uploads.", reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_callback_query(filters.regex("^yt_login_prompt_from_menu$"))
async def yt_login_prompt_from_menu(client, callback_query):
    await yt_login_prompt(client, callback_query)

@app.on_callback_query(filters.regex("^yt_switch_account_"))
async def yt_switch_account(client, callback_query):
    user_id = callback_query.from_user.id
    new_active_id = callback_query.data.split('_')[-1]
    update_user_data(user_id, {"youtube_active_account_id": new_active_id})
    await callback_query.answer("✅ Active YouTube account switched!", show_alert=True)
    await yt_manage_accounts_inline(client, callback_query)


async def fb_oauth_login_prompt(client, message_or_callback):
    """Handles both /fblogin command and settings button."""
    user_id = message_or_callback.from_user.id
    if not is_premium_user(user_id, 'facebook') and not is_admin(user_id):
        reply_func = getattr(message_or_callback, "answer", message_or_callback.reply)
        return await reply_func("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)

    if isinstance(message_or_callback, types.CallbackQuery):
        await message_or_callback.answer("Starting Facebook OAuth Login...")
        msg_func = message_or_callback.message.edit_text
    else:
        msg_func = message_or_callback.reply
        
    user_states[user_id] = {"step": AWAITING_FB_OAUTH_TOKEN}
    await msg_func(
        "🔑 **Facebook Page Access Token Setup:**\n\n"
        "Please paste your long-lived **Page Access Token** here. This will be used to manage uploads to your Facebook page.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_OAUTH_TOKEN))
@with_user_lock
async def fb_get_oauth_token(client, message):
    user_id = message.from_user.id
    page_access_token = message.text.strip()
    user_states.pop(user_id, None)

    if not page_access_token:
        return await message.reply("❌ Login failed. No token provided.", reply_markup=facebook_settings_inline_menu)

    try:
        await client.send_chat_action(user_id, enums.ChatAction.TYPING)
        response = requests.get(f"https://graph.facebook.com/me?fields=id,name&access_token={page_access_token}")
        response.raise_for_status()
        page_data = response.json()

        if 'id' in page_data and 'name' in page_data:
            page_info = {
                "id": page_data['id'], "name": page_data['name'],
                "access_token": page_access_token, "is_active": True
            }
            
            user_doc = get_user_data(user_id)
            pages = user_doc.get("facebook_pages", [])
            page_exists = False
            for p in pages:
                p["is_active"] = (p['id'] == page_info['id'])
                if p['id'] == page_info['id']:
                    p.update(page_info)
                    page_exists = True
            
            if not page_exists:
                pages.append(page_info)
            
            update_user_data(user_id, {
                "facebook_pages": pages,
                "facebook_selected_page_id": page_data['id']
            })
            await message.reply(
                f"✅ **Facebook Login Successful!**\n"
                f"You are now logged in to page: **{page_data['name']}**.\n"
                f"It has been set as your active page for uploads.",
                reply_markup=facebook_settings_inline_menu
            )
        else:
            await message.reply("❌ Invalid Page Access Token. Could not retrieve page info.", reply_markup=facebook_settings_inline_menu)
    except requests.exceptions.RequestException as e:
        await message.reply(f"❌ **API Error:** `{e}`\nPlease check your token and try again.", reply_markup=facebook_settings_inline_menu)
    except Exception as e:
        await message.reply(f"❌ **An unexpected error occurred:** `{e}`. Please try again.", reply_markup=facebook_settings_inline_menu)


@app.on_callback_query(filters.regex("^fb_manage_accounts$"))
async def fb_manage_accounts_inline(client, callback_query):
    user_id = callback_query.from_user.id
    user_doc = get_user_data(user_id)
    pages = user_doc.get("facebook_pages", [])
    active_page_id = user_doc.get("facebook_selected_page_id")
    
    if not pages:
        return await callback_query.answer("No Facebook accounts linked. Please log in first.", show_alert=True)
    
    keyboard = []
    for page in pages:
        is_active = (page["id"] == active_page_id)
        checkmark = "✅ " if is_active else ""
        button_text = f"{checkmark}{page['name']}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"fb_switch_account_{page['id']}")])
    
    keyboard.append([InlineKeyboardButton("🔑 Link New Account", callback_data="fb_oauth_login_prompt_from_menu")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to Settings", callback_data="settings_facebook")])
    
    await callback_query.message.edit_text("Select an active Facebook account to use for uploads:", reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_callback_query(filters.regex("^fb_oauth_login_prompt_from_menu$"))
async def fb_login_prompt_from_menu(client, callback_query):
    await fb_oauth_login_prompt(client, callback_query)

@app.on_callback_query(filters.regex("^fb_switch_account_"))
async def fb_switch_account(client, callback_query):
    user_id = callback_query.from_user.id
    new_page_id = callback_query.data.split('_')[-1]
    
    update_user_data(user_id, {"facebook_selected_page_id": new_page_id})
    
    user_doc = get_user_data(user_id)
    selected_page_name = next((p['name'] for p in user_doc.get("facebook_pages", []) if p['id'] == new_page_id), "N/A")
    
    await callback_query.answer(f"Switched to account: {selected_page_name}", show_alert=True)
    await fb_manage_accounts_inline(client, callback_query)


# --- UPLOAD FLOW (REVISED) ---
async def prompt_facebook_upload(client, message):
    user_id = message.chat.id
    if not is_premium_user(user_id, 'facebook') and not is_admin(user_id):
        return await message.reply("❌ **Access Restricted.** You need a premium plan for Facebook uploads.")

    if not has_upload_quota(user_id, 'facebook') and not is_admin(user_id):
        return await message.reply("❌ **Quota Exceeded.** You have reached your daily upload limit for Facebook.")
        
    user_doc = get_user_data(user_id)
    if not user_doc.get("facebook_pages"):
        return await message.reply("❌ **Authentication Required.** Please log into Facebook via `⚙️ Settings` first.")

    user_states[user_id] = {"step": AWAITING_UPLOAD_TYPE_SELECTION, "platform": "facebook"}
    await message.reply(
        "What type of content are you transmitting to Facebook?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎥 Video", callback_data="upload_type_fb_video")],
            [InlineKeyboardButton("🎞️ Reel", callback_data="upload_type_fb_reels")],
            [InlineKeyboardButton("🖼️ Photo", callback_data="upload_type_fb_photo")],
            [InlineKeyboardButton("⬅️ Cancel", callback_data="cancel_upload")]
        ])
    )

async def prompt_youtube_upload(client, message):
    user_id = message.chat.id
    if not is_premium_user(user_id, 'youtube') and not is_admin(user_id):
        return await message.reply("❌ **Access Restricted.** You need a premium plan for YouTube uploads.")

    if not has_upload_quota(user_id, 'youtube') and not is_admin(user_id):
        return await message.reply("❌ **Quota Exceeded.** You have reached your daily upload limit for YouTube.")
    
    user_doc = get_user_data(user_id)
    if not user_doc.get("youtube_accounts"):
        return await message.reply("❌ **Authentication Required.** Please log into YouTube via `⚙️ Settings` first.")

    user_states[user_id] = {"step": AWAITING_UPLOAD_TYPE_SELECTION, "platform": "youtube"}
    await message.reply(
        "What type of video are you transmitting to YouTube?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎬 Standard Video", callback_data="upload_type_yt_video")],
            [InlineKeyboardButton("📱 Short", callback_data="upload_type_yt_shorts")],
            [InlineKeyboardButton("⬅️ Cancel", callback_data="cancel_upload")]
        ])
    )

@app.on_callback_query(filters.regex("^upload_type_"))
async def handle_upload_type_selection(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)
    if not state or state.get("step") != AWAITING_UPLOAD_TYPE_SELECTION:
        return await callback_query.answer("❗ **Invalid Operation.** Please restart.", show_alert=True)

    parts = callback_query.data.split("_")
    platform_code = parts[2]
    upload_type = parts[3]
    
    state["upload_type"] = upload_type
    state["step"] = AWAITING_UPLOAD_FILE

    await callback_query.answer(f"Selected {upload_type.capitalize()} upload.")
    await callback_query.message.edit_text(
        f"✅ **Type Selected.**\n\nPlease send your {'video' if upload_type != 'photo' else 'image'} file now.",
    )

@app.on_message((filters.video | filters.document | filters.photo) & filters.private & filters.create(lambda _, __, m: user_states.get(m.from_user.id, {}).get("step") == AWAITING_UPLOAD_FILE))
@with_user_lock
async def handle_media_file(client, message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    if not state: return

    media = message.video or message.document or message.photo
    if media.file_size > 4 * 1024 * 1024 * 1024:
        return await message.reply("❌ **File Size Limit Exceeded.** Maximum file size is 4GB.")
    
    # Cache file ID and ask for title (DO NOT DOWNLOAD YET)
    state["tg_file_id"] = media.file_id
    state["source_filename"] = getattr(media, "file_name", f"media_{int(time.time())}")
    state["step"] = AWAITING_UPLOAD_TITLE
    
    await message.reply(
        "✅ **File Received.**\n\n"
        "Now, please send the **title** for your upload.\n"
        "Or, send /skip to use your default title."
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_TITLE))
async def handle_upload_title(client, message):
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state: return

    state["title"] = message.text.strip()
    await message.reply(f"✅ **Title Recorded:** '{state['title']}'.")
    
    # Now that we have the title, start the download
    await start_download_and_processing(client, message, user_id)

async def start_download_and_processing(client, message, user_id):
    state = user_states.get(user_id)
    status_msg = await client.send_message(user_id, "⏳ **Preparing file for download...**")
    download_path, processed_path = None, None
    try:
        start_time = time.time()
        download_path = await client.download_media(
            state["tg_file_id"],
            progress=download_progress_callback,
            progress_args=(client, status_msg, start_time)
        )
        
        # Process video, but keep photo as is
        if state["upload_type"] != 'photo':
            await status_msg.edit_text("⏳ **Processing video...** This may take a moment.")
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as pool:
                processed_path = await loop.run_in_executor(pool, process_video_for_upload, download_path)
        else:
            processed_path = download_path

        state["file_path"] = processed_path
        await status_msg.edit_text("✅ **Processing Complete.**")
        
        # Move to the next step: Description
        await prompt_description(client, message, user_id, state['platform'])

    except Exception as e:
        await status_msg.edit_text(f"❌ **Error during file handling:** `{e}`")
        logger.error(f"Error during media handling for user {user_id}: {e}", exc_info=True)
        user_states.pop(user_id, None)
    finally:
        # Clean up the original downloaded file if a processed version was created
        if download_path and processed_path and download_path != processed_path and os.path.exists(download_path):
            os.remove(download_path)

async def prompt_description(client, message, user_id, platform):
    user_doc = get_user_data(user_id)
    user_states[user_id]["step"] = AWAITING_UPLOAD_DESCRIPTION
    default_desc = user_doc.get(f"{platform}_settings", {}).get("description", "")
    await client.send_message(
        user_id,
        f"📝 Now, send a **description** for your content.\n"
        f"Or, send /skip to use your default:\n`{default_desc[:100]}...`"
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_DESCRIPTION))
async def handle_upload_description(client, message):
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state: return

    state["description"] = message.text.strip()
    await message.reply("✅ **Description Recorded.**")
    await prompt_tags(client, message, user_id, state['platform'])

async def prompt_tags(client, message, user_id, platform):
    user_doc = get_user_data(user_id)
    user_states[user_id]["step"] = AWAITING_UPLOAD_TAGS
    default_tags = user_doc.get(f"{platform}_settings", {}).get("tag", "")
    await client.send_message(
        user_id,
        f"🏷️ Please send a list of **tags** (separated by spaces).\n"
        f"Or, send /skip to use your default: `{default_tags}`"
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_TAGS))
async def handle_upload_tags(client, message):
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state: return

    state["tags"] = message.text.strip().split()
    await message.reply("✅ **Tags Recorded.**")
    await prompt_thumbnail(client, message)

async def prompt_thumbnail(client, message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    if not state: return

    # Photos don't need thumbnails
    if state.get("upload_type") == 'photo':
        state['thumbnail_path'] = None
        await prompt_visibility_selection(client, message, user_id, state['platform'])
        return
        
    user_states[user_id]["step"] = AWAITING_UPLOAD_THUMBNAIL
    await message.reply(
        "🖼️ **Thumbnail Selection:**",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Upload Custom Thumbnail", callback_data="thumb_custom")],
            [InlineKeyboardButton("Auto-Generate from Video", callback_data="thumb_auto_generate")]
        ])
    )
    
@app.on_callback_query(filters.regex("^thumb_"))
async def handle_thumbnail_selection(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)
    if not state or state.get("step") != AWAITING_UPLOAD_THUMBNAIL:
        return await callback_query.answer("❗ Invalid Operation.", show_alert=True)
    
    choice = callback_query.data.split("_")[1]
    
    if choice == "custom":
        state["thumbnail_choice"] = "custom"
        await callback_query.message.edit_text("Please send the **image file** for the thumbnail.")
        user_states[user_id]["step"] = AWAITING_CUSTOM_THUMBNAIL
    elif choice == "auto_generate":
        state["thumbnail_choice"] = "auto_generate"
        await callback_query.message.edit_text("⏳ **Generating thumbnail...**")
        
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            thumbnail_path = await loop.run_in_executor(pool, generate_thumbnail, state["file_path"])
        state["thumbnail_path"] = thumbnail_path

        if state["thumbnail_path"]:
            await callback_query.message.delete()
            await prompt_visibility_selection(client, callback_query.message, user_id, state["platform"])
        else:
            await callback_query.message.edit_text("❌ **Thumbnail Generation Failed.**")
            user_states.pop(user_id, None)

@app.on_message(filters.photo & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_CUSTOM_THUMBNAIL))
async def handle_custom_thumbnail(client, message):
    user_id = message.chat.id
    state = user_states.get(user_id)
    
    status_msg = await message.reply("⏳ **Downloading thumbnail...**")
    
    try:
        thumb_path = await client.download_media(message.photo)
        state["thumbnail_path"] = thumb_path
        await status_msg.delete()
        await prompt_visibility_selection(client, message, user_id, state["platform"])
    except Exception as e:
        await status_msg.edit_text(f"❌ **Thumbnail Download Failed:** `{e}`.")
        user_states.pop(user_id, None)

# ... The rest of the upload flow (visibility, schedule, initiate_upload) is largely unchanged and works with the new flow.
async def prompt_visibility_selection(client, message, user_id, platform):
    """Sends the visibility selection prompt."""
    user_states[user_id]["step"] = AWAITING_UPLOAD_VISIBILITY
    
    if platform == "youtube":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Public", callback_data="visibility_public")],
            [InlineKeyboardButton("Private", callback_data="visibility_private")],
            [InlineKeyboardButton("Unlisted", callback_data="visibility_unlisted")]
        ])
    else: # Facebook
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Public", callback_data="visibility_public")],
            [InlineKeyboardButton("Private (Draft)", callback_data="visibility_private")]
        ])
    await client.send_message(user_id, "🌐 **Select content visibility:**", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^visibility_"))
async def handle_visibility_selection(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)
    if not state or state.get("step") != AWAITING_UPLOAD_VISIBILITY:
        return await callback_query.answer("❗ Invalid Operation.", show_alert=True)
    
    state["visibility"] = callback_query.data.split("_")[1]
    user_states[user_id]["step"] = AWAITING_UPLOAD_SCHEDULE

    await callback_query.answer(f"Visibility set to: {state['visibility'].capitalize()}", show_alert=True)
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Publish Now", callback_data="schedule_now")],
        [InlineKeyboardButton("Schedule for Later", callback_data="schedule_later")]
    ])
    await callback_query.message.edit_text("⏰ **Do you wish to publish now or schedule for later?**", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^schedule_"))
async def handle_schedule_selection(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)
    if not state or state.get("step") != AWAITING_UPLOAD_SCHEDULE:
        return await callback_query.answer("❗ Invalid Operation.", show_alert=True)
    
    schedule_choice = callback_query.data.split("_")[1]
    
    if schedule_choice == "now":
        state["schedule_time"] = None
        await callback_query.answer("Publishing now selected.")
        await callback_query.message.edit_text("⏳ **Finalizing...** Preparing your content for immediate transmission.")
        await initiate_upload(client, callback_query.message, user_id)
    elif schedule_choice == "later":
        user_states[user_id]["step"] = AWAITING_UPLOAD_SCHEDULE_DATETIME
        await callback_query.answer("Awaiting schedule time input...")
        await callback_query.message.edit_text(
            "📅 Please send the schedule date and time.\n"
            "**Format:** `YYYY-MM-DD HH:MM` (e.g., `2025-12-25 14:30`)\n"
            "**Note:** Time is in UTC."
        )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_SCHEDULE_DATETIME))
async def handle_schedule_datetime_input(client, message):
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state: return

    schedule_str = message.text.strip()
    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")
        if schedule_dt <= datetime.utcnow() + timedelta(minutes=10):
            return await message.reply("❌ **Time Constraint Violation.** Schedule time must be at least 10 minutes in the future.")

        state["schedule_time"] = schedule_dt
        await message.reply("⏳ **Finalizing...** Preparing your content for scheduled transmission.")
        await initiate_upload(client, message, user_id)

    except ValueError:
        await message.reply("❌ **Input Error.** Invalid date/time format. Please use `YYYY-MM-DD HH:MM`.")


async def initiate_upload(client, message, user_id):
    state = user_states.get(user_id)
    if not state:
        return await client.send_message(user_id, "❌ **Upload Aborted.** Session state lost.")

    file_path = state.get("file_path")
    thumbnail_path = state.get("thumbnail_path")
    platform = state["platform"]
    
    try:
        if not all([file_path, state.get("title")]):
            return await client.send_message(user_id, "❌ **Upload Failure.** Missing essential data. Please restart.")

        job_id = jobs_collection.insert_one({
            "user_id": user_id, "platform": platform,
            "status": "queued", "start_time": datetime.utcnow()
        }).inserted_id
        
        status_msg = await client.send_message(user_id, f"✅ **All data received.** Your upload is now in the queue.")

        async with upload_semaphore:
            logger.info(f"Starting upload for job {job_id} (User: {user_id})")
            jobs_collection.update_one({"_id": job_id}, {"$set": {"status": "uploading"}})
            
            upload_successful = False
            last_error = "Unknown error"

            try:
                if platform == "facebook":
                    fb_access_token, fb_page_id = get_facebook_page_info(user_id)
                    if not fb_access_token: raise RuntimeError("Facebook login required.")
                    
                    def fb_upload_task():
                        return upload_facebook_content(
                            file_path=file_path, content_type=state.get("upload_type", "video"),
                            title=state.get("title"), description=state.get("description"),
                            access_token=fb_access_token, page_id=fb_page_id,
                            visibility=state.get("visibility"), schedule_time=state.get("schedule_time")
                        )
                    
                    await status_msg.edit_text(f"📤 **Uploading to Facebook...**")
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        fb_result = await asyncio.get_event_loop().run_in_executor(executor, fb_upload_task)

                    if fb_result and 'id' in fb_result:
                        await status_msg.edit_text(f"✅ **Facebook Upload Complete!**\nPost ID: `{fb_result['id']}`")
                        upload_successful = True
                    else:
                        raise RuntimeError(f"Facebook API did not return an ID. Response: {fb_result}")

                elif platform == "youtube":
                    creds = get_youtube_credentials(user_id)
                    if not creds: raise RuntimeError("YouTube login required.")
                    
                    def yt_upload_task(status_message):
                        youtube = build('youtube', 'v3', credentials=creds)
                        body = {
                            'snippet': {'title': state.get('title'), 'description': state.get('description'), 'tags': state.get('tags', [])},
                            'status': {'privacyStatus': state.get('visibility')}
                        }
                        if state.get('schedule_time'):
                            body['status']['publishAt'] = state['schedule_time'].isoformat("T") + "Z"

                        media_file = MediaFileUpload(file_path, chunksize=-1, resumable=True)
                        
                        request = youtube.videos().insert(part=",".join(body.keys()), body=body, media_body=media_file)
                        
                        response = None
                        while response is None:
                            status, response = request.next_chunk()
                            if status:
                                asyncio.run_coroutine_threadsafe(upload_progress_callback(status_message, status), client.loop)
                        return response

                    await status_msg.edit_text(f"📤 **Uploading to YouTube...**")
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        yt_result = await asyncio.get_event_loop().run_in_executor(executor, yt_upload_task, status_msg)
                    
                    if yt_result and 'id' in yt_result:
                        await status_msg.edit_text(f"✅ **YouTube Upload Complete!**\nVideo ID: `{yt_result['id']}`")
                        upload_successful = True
                    else:
                        raise RuntimeError(f"YouTube API did not return an ID. Response: {yt_result}")

            except Exception as e:
                last_error = str(e)
                logger.error(f"Upload failed for job {job_id}: {e}", exc_info=True)
            
            if not upload_successful:
                await status_msg.edit_text(f"❌ **Upload Failed.**\nReason: `{last_error}`")
                jobs_collection.update_one({"_id": job_id}, {"$set": {"status": "failed", "error": last_error, "end_time": datetime.utcnow()}})
            else:
                users_collection.update_one(
                    {"_id": user_id},
                    {
                        "$inc": {f"uploads_today.{platform}": 1},
                        "$set": {f"last_upload_date.{platform}": datetime.utcnow().isoformat()}
                    }
                )
                jobs_collection.update_one({"_id": job_id}, {"$set": {"status": "success", "end_time": datetime.utcnow()}})

    finally:
        # Final cleanup
        if file_path and os.path.exists(file_path): os.remove(file_path)
        if thumbnail_path and os.path.exists(thumbnail_path): os.remove(thumbnail_path)
        user_states.pop(user_id, None)


# === KEEP ALIVE SERVER ===
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/oauth2callback'):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            html_content = """
            <html>
            <head><title>Authentication Successful</title></head>
            <body style='font-family: sans-serif; text-align: center; padding-top: 50px;'>
                <h1>✅ Authentication Successful!</h1>
                <p>You can now return to the Telegram bot.</p>
                <p>Please copy the full URL from your browser's address bar and paste it into the bot if you haven't already.</p>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode('utf-8'))
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Bot is alive.")

def run_server():
    httpd = HTTPServer(('0.0.0.0', 8080), Handler)
    logger.info("Keep-alive HTTP server started on port 8080.")
    httpd.serve_forever()

if __name__ == "__main__":
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
    
    initialize_global_settings()
    load_upload_semaphore()
    
    threading.Thread(target=run_server, daemon=True).start()
    
    logger.info("Bot system initiating...")
    app.run()
