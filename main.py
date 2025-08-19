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

# ⚠️ CRITICAL CONFIGURATION ⚠️
# The REDIRECT_URI MUST be your public server URL, not 127.0.0.1.
# Example: https://absent-dulcea-primeyour-bcdf24ed.koyeb.app/oauth2callback
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

# === PREMIUM PLANS & QUOTAS ===
PREMIUM_PLANS = {
    "free": {"duration_days": 1, "upload_quota": 1},
    "2day": {"duration_days": 2, "upload_quota": 20, "price": 10},
    "5day": {"duration_days": 5, "upload_quota": 50, "price": 30},
    "8day": {"duration_days": 8, "upload_quota": 80, "price": 50},
    "20day": {"duration_days": 20, "upload_quota": 200, "price": 70},
    "30day": {"duration_days": 30, "upload_quota": 300, "price": 100},
    "2mo": {"duration_days": 60, "upload_quota": 600, "price": 150},
    "6mo": {"duration_days": 180, "upload_quota": 1800, "price": 300},
    "1year": {"duration_days": 365, "upload_quota": 3650, "price": "Negotiable"},
    "lifetime": {"duration_days": 36500, "upload_quota": 99999, "price": "Negotiable"}
}

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
    [InlineKeyboardButton("📊 Bot Stats", callback_data='settings_bot_status_inline')],
    [InlineKeyboardButton("⬅️ Back to Settings", callback_data="settings_main_menu_inline")]
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
        [InlineKeyboardButton("🔑 Facebook Login (OAuth)", callback_data='fb_oauth_login_prompt')],
        [InlineKeyboardButton("🗓️ Check Status", callback_data='fb_stats_status')],
        [InlineKeyboardButton("🔄 Refresh Pages List", callback_data='fb_refresh_pages_list')],
        [InlineKeyboardButton("📝 Set Title", callback_data='fb_set_title')],
        [InlineKeyboardButton("🏷️ Set Tag", callback_data='fb_set_tag')],
        [InlineKeyboardButton("📄 Set Description", callback_data='fb_set_description')],
        [InlineKeyboardButton("🎥 Default Upload Type", callback_data='fb_default_upload_type')],
        [InlineKeyboardButton("⏰ Set Schedule Time", callback_data='fb_set_schedule_time')],
        [InlineKeyboardButton("🔒 Set Private/Public", callback_data='fb_set_privacy')],
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
    """Generates privacy inline keyboard for different platforms."""
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

# === USER STATES (for sequential conversation flows) ===
user_states = {}

AWAITING_UPLOAD_FILE = "awaiting_upload_file"
AWAITING_UPLOAD_TITLE = "awaiting_upload_title"
AWAITING_UPLOAD_DESCRIPTION = "awaiting_upload_description"
AWAITING_UPLOAD_VISIBILITY = "awaiting_upload_visibility"
AWAITING_UPLOAD_SCHEDULE = "awaiting_upload_schedule"
AWAITING_UPLOAD_SCHEDULE_DATETIME = "awaiting_upload_schedule_datetime"
AWAITING_UPLOAD_TYPE_SELECTION = "awaiting_upload_type_selection"
AWAITING_UPLOAD_TAGS = "awaiting_upload_tags"
AWAITING_UPLOAD_THUMBNAIL = "awaiting_upload_thumbnail"

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

AWAITING_BROADCAST_MESSAGE = "awaiting_broadcast_message"
AWAITING_ADD_USER = "admin_awaiting_user_id_to_add"
AWAITING_REMOVE_USER = "admin_awaiting_user_id_to_remove"

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

def get_user_plan_and_expiry(user_id):
    """Retrieves user's premium plan and expiry, checking if it's still active."""
    user_doc = get_user_data(user_id)
    if not user_doc or not user_doc.get("is_premium", False):
        return None, None, None

    plan = user_doc.get("plan_tier")
    expiry_date_str = user_doc.get("premium_expiry")
    if not plan or not expiry_date_str:
        return None, None, None

    expiry_date = datetime.fromisoformat(expiry_date_str)
    if expiry_date < datetime.now(timezone.utc).replace(tzinfo=None): # Use timezone-aware comparison
        # Premium expired, downgrade user
        update_user_data(user_id, {"is_premium": False, "plan_tier": "free", "premium_expiry": None})
        return "free", None, None

    return plan, expiry_date, user_doc.get("daily_uploads", 0)

def is_premium_user(user_id):
    """Checks if a user has an active premium plan."""
    plan, expiry, _ = get_user_plan_and_expiry(user_id)
    return plan is not None and plan != "free"

def has_upload_quota(user_id):
    """Checks if a user has remaining upload quota for the day."""
    user_doc = get_user_data(user_id)
    plan_tier = user_doc.get("plan_tier", "free")
    uploads_today = user_doc.get("uploads_today", 0)
    last_upload_date = user_doc.get("last_upload_date")

    # Reset daily count if the day has changed
    if last_upload_date and last_upload_date.date() < datetime.now(timezone.utc).date():
        users_collection.update_one({"_id": user_id}, {"$set": {"uploads_today": 0, "last_upload_date": datetime.now(timezone.utc)}})
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
    """
    Retrieves and refreshes YouTube credentials from MongoDB.
    Returns a Credentials object or None.
    """
    user_doc = get_user_data(user_id)
    yt_data = user_doc.get("youtube", {}) if user_doc else {}

    if not yt_data.get('logged_in') or not yt_data.get('refresh_token'):
        return None

    try:
        creds = Credentials(
            token=yt_data.get('access_token'),
            refresh_token=yt_data.get('refresh_token'),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=yt_data.get('client_id'),
            client_secret=yt_data.get('client_secret'),
            scopes=GOOGLE_API_SCOPES
        )

        if creds.expired and creds.refresh_token:
            creds.refresh(requests.Request())
            update_user_data(user_id, {
                "youtube.access_token": creds.token,
                "youtube.token_expiry": creds.expiry.isoformat()
            })
            logger.info(f"Refreshed YouTube token for user {user_id}.")

        return creds
    except Exception as e:
        logger.error(f"Failed to refresh/get YouTube credentials for user {user_id}: {e}")
        return None

def get_video_metadata(file_path):
    """
    Uses ffprobe to get video metadata like duration, resolution, and audio streams.
    Returns a dictionary of metadata or raises an exception on failure.
    """
    try:
        command = [
            "ffprobe", "-v", "error", 
            "-show_entries", "stream=duration,width,height,codec_type,tags:format=duration",
            "-of", "json", file_path
        ]
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
        metadata = json.loads(result.stdout)
        
        duration = float(metadata['format']['duration'])
        video_stream = next((s for s in metadata.get('streams', []) if s.get('codec_type') == 'video'), None)
        audio_streams = [s for s in metadata.get('streams', []) if s.get('codec_type') == 'audio']
        
        width = video_stream['width'] if video_stream else 0
        height = video_stream['height'] if video_stream else 0
        
        return {
            "duration": duration,
            "width": width,
            "height": height,
            "aspect_ratio": width / height if height > 0 else 0,
            "audio_streams": audio_streams
        }
    except FileNotFoundError:
        raise RuntimeError("FFprobe not found. Please install FFmpeg.")
    except Exception as e:
        logger.error(f"Failed to get video metadata for {file_path}: {e}")
        return None

def process_audio(input_path, output_path, audio_streams):
    """
    Processes audio by selecting a preferred stream or mixing down multiple streams.
    This is a basic implementation of the audio policy.
    """
    command = ["ffmpeg", "-y", "-i", input_path]
    
    # Simple logic: prefer 'mal' or 'eng', otherwise use the first stream
    mal_stream = next((s for s in audio_streams if s.get('tags', {}).get('language') == 'mal'), None)
    eng_stream = next((s for s in audio_streams if s.get('tags', {}).get('language') == 'eng'), None)
    
    if mal_stream:
        command.extend(["-map", f"0:{mal_stream['index']}"])
    elif eng_stream:
        command.extend(["-map", f"0:{eng_stream['index']}"])
    else:
        first_audio_stream = next((s for s in audio_streams if s.get('codec_type') == 'audio'), None)
        if first_audio_stream:
            command.extend(["-map", f"0:{first_audio_stream['index']}"])
        else:
            command.extend(["-an"])
    
    command.extend([
        "-c:v", "copy",
        "-c:a", "aac",
        "-b:a", "192k",
        "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
        output_path
    ])
    
    try:
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=900)
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"[FFmpeg Audio] Processing failed for {input_path}. STDOUT: {e.stdout}, STDERR: {e.stderr}")
        raise RuntimeError(f"FFmpeg audio processing error: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("FFmpeg not found. Please install FFmpeg.")
    except subprocess.TimeoutExpired:
        raise RuntimeError("FFmpeg audio processing timed out.")

def upload_facebook_content(file_path, content_type, title, description, access_token, page_id, visibility="PUBLISHED", schedule_time=None):
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
            params['status_type'] = 'SCHEDULED_PUBLISH'
        elif visibility.lower() == 'private' or visibility.lower() == 'draft':
            params['status_type'] = 'DRAFT'
        else:
            params['status_type'] = 'PUBLISHED'
        
        with open(file_path, 'rb') as f:
            files = {'file': f}
            response = requests.post(post_url, params=params, files=files)

    elif content_type == "photo":
        post_url = f"https://graph.facebook.com/v19.0/{page_id}/photos"
        params['caption'] = description if description else title
        if schedule_time:
            params['scheduled_publish_time'] = int(schedule_time.timestamp())
            params['published'] = 'false'
        elif visibility.lower() == 'private' or visibility.lower() == 'draft':
            params['published'] = 'false'
        else:
            params['published'] = 'true'
        
        with open(file_path, 'rb') as f:
            files = {'source': f}
            response = requests.post(post_url, params=params, files=files)
            
    else:
        raise ValueError(f"Unsupported Facebook content type: {content_type}")

    response.raise_for_status()
    result = response.json()
    logger.info(f"Facebook {content_type} upload result: {result}")
    return result

def convert_media_for_facebook(input_path, output_type, target_format):
    """
    Converts media to a suitable format for Facebook upload.
    Adds a placeholder for audio mixdown logic.
    """
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_path = f"downloads/processed_{base_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.{target_format}"
    command = []
    
    if output_type in ["video", "reels"]:
        command = ["ffmpeg", "-i", input_path, "-c:v", "libx264", "-preset", "medium", "-crf", "23", "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-y", output_path]
        if output_type == "reels":
            command.insert(2, "-vf")
            command.insert(3, "scale='min(iw,ih*9/16)':'min(ih,iw*16/9)',pad='ih*9/16':ih:(ow-iw)/2:(oh-ih)/2")
    elif output_type == "photo":
        command = ["ffmpeg", "-i", input_path, "-vframes", "1", "-q:v", "2", "-y", output_path]
    else:
        raise ValueError(f"Unsupported output type for conversion: {output_type}")

    try:
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=900)
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"[FFmpeg] Conversion failed for {input_path}. STDOUT: {e.stdout}, STDERR: {e.stderr}")
        raise RuntimeError(f"FFmpeg conversion error: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("FFmpeg not found. Please install FFmpeg.")
    except subprocess.TimeoutExpired:
        raise RuntimeError("FFmpeg conversion timed out.")

def generate_thumbnail(video_path):
    """
    Generates a thumbnail from a video using a more intelligent FFmpeg command.
    It finds a visually complex frame instead of just a fixed timestamp.
    """
    base_name = os.path.splitext(os.path.basename(video_path))[0]
    output_path = f"downloads/thumb_{base_name}.jpg"
    
    try:
        command = [
            "ffmpeg", "-i", video_path, "-vf", "select='gt(scene,0.4)',scale=1280:-1",
            "-frames:v", "1", "-q:v", "2", "-y", output_path
        ]
        fallback_command = [
            "ffmpeg", "-i", video_path, "-ss", "00:00:05", "-vframes", "1", 
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
    replacements = {
        '{filename}': job_data.get('source_filename', ''),
        '{duration}': str(timedelta(seconds=int(job_data.get('duration', 0)))),
        '{date}': datetime.now().strftime('%Y-%m-%d'),
        '{platform}': job_data.get('platform', ''),
        '{brand}': 'Auto Uploader Pro',
        '{hashtags}': ' '.join(user_data.get(f'{job_data["platform"]}_settings', {}).get('tag', '').split()),
    }
    for key, value in replacements.items():
        template = template.replace(key, str(value))
    return template

async def download_progress_callback(current, total, *args):
    """Sends progress updates during file download."""
    client, message = args
    try:
        # Avoid spamming Telegram APIs by tracking last percentage
        last_percentage = message.meta.get("last_percentage", 0)
        percentage = int((current / total) * 100)
        if percentage > last_percentage:
            progress_str = f"⬇️ Downloading: {percentage}%"
            await message.edit(progress_str)
            message.meta["last_percentage"] = percentage
    except Exception as e:
        logger.debug(f"Failed to update download progress message: {e}")

# === PYROGRAM HANDLERS ===
# FIXED: The entire start_command is rewritten to avoid the MongoDB path conflict error.
@app.on_message(filters.command("start"))
async def start_command(client, message):
    user_id = message.from_user.id
    user_first_name = message.from_user.first_name or "Unknown User"
    user_username = message.from_user.username or "N/A"

    # Start with a single dictionary for default user data
    user_data = {
        "first_name": user_first_name,
        "username": user_username,
        "last_active": datetime.now(timezone.utc),
        "is_premium": False,
        "role": "user",
        "plan_tier": "free",
        "total_uploads": 0,
        "uploads_today": 0,
        "last_upload_date": datetime.now(timezone.utc),
        "premium_expiry": None,
        "facebook_settings": {
            "title": "Default Facebook Title", "tag": "#facebook #video #reels", "description": "Default Facebook Description", "upload_type": "Video", "schedule_time": None, "privacy": "Public"
        },
        "youtube_settings": {
            "title": "Default YouTube Title", "tag": "#youtube #video #shorts", "description": "Default YouTube Description", "video_type": "Video (Standard Horizontal/Square)", "schedule_time": None, "privacy": "Public"
        }
    }

    # If the user is the owner, override the default values
    if user_id == OWNER_ID:
        user_data["role"] = "admin"
        user_data["is_premium"] = True
        user_data["plan_tier"] = "lifetime"
        user_data["premium_expiry"] = (datetime.now(timezone.utc) + timedelta(days=365*10)).isoformat()
    
    # Prepare the query: $set will handle all fields, $setOnInsert only for the creation time.
    try:
        users_collection.update_one(
            {"_id": user_id},
            {
                "$set": user_data,
                "$setOnInsert": {"added_at": datetime.now(timezone.utc)}
            },
            upsert=True
        )
        logger.info(f"User {user_id} account initialized/updated successfully.")
    except Exception as e:
        logger.error(f"Error during user data update/upsert for user {user_id}: {e}")
        await message.reply("🚨 **System Alert!** An error occurred while initializing your account. Please try again later or contact support.")
        return

    # Continue with the rest of the original logic
    user_doc = get_user_data(user_id)
    if not user_doc:
        logger.error(f"Could not retrieve user document for {user_id} after upsert.")
        await message.reply("❌ **Error!** Failed to retrieve your account data after setup. Please try `/start` again.")
        return

    plan_tier = user_doc.get("plan_tier", "free").capitalize()
    await log_to_channel(client, f"User `{user_id}` (`{user_username}` - `{user_first_name}`) performed `/start`. Role: `{user_doc.get('role')}`, Plan: `{plan_tier}`.")

    if is_admin(user_id):
        welcome_msg = (
            f"🤖 **Welcome to the Upload Bot, Administrator {user_first_name}!**\n\n"
            "🛠 You have **full system access and privileges**.\n"
            "Ready to command the digital frontier!"
        )
        reply_markup = main_menu_admin
    else:
        welcome_msg = (
            f"👋 **Greetings, {user_first_name}!**\n\n"
            "This bot is your gateway to **efforless video uploads** directly from Telegram.\n\n"
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
        reply_markup = main_menu_user
        if not is_premium_user(user_id):
            join_channel_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅Join Our Digital Hub✅", url=CHANNEL_LINK)]
            ])
            await client.send_photo(
                chat_id=message.chat.id,
                photo=CHANNEL_PHOTO_URL,
                caption=welcome_msg,
                reply_markup=join_channel_markup,
                parse_mode=enums.ParseMode.MARKDOWN
            )
            return

    await message.reply(welcome_msg, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)
    logger.info(f"Start command completed for user {user_id}.")


@app.on_message(filters.command("addpremium") & filters.user(OWNER_ID))
async def add_premium_command(client, message):
    try:
        args = message.text.split(maxsplit=2)
        if len(args) != 3 or not args[1].isdigit() or not args[2].isdigit():
            await message.reply("❗ **Syntax Error:** Usage: `/addpremium <user_id> <days>`")
            return

        target_user_id = int(args[1])
        days = int(args[2])
        expiry_date = datetime.now(timezone.utc) + timedelta(days=days)

        user_data_to_set = {
            "is_premium": True,
            "plan_tier": f"{days}day",
            "premium_expiry": expiry_date.isoformat()
        }

        update_user_data(target_user_id, user_data_to_set)
        await message.reply(f"✅ **Success!** User `{target_user_id}` has been granted **PREMIUM** status for **{days} days**.")
        try:
            await client.send_message(target_user_id, f"🎉 **System Notification!** Your premium access has been extended by **{days} days**! Use `/start` to access your enhanced features.")
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id} about premium extension: {e}")
        await log_to_channel(client, f"Admin `{message.from_user.id}` granted premium for `{days}` days to user `{target_user_id}`.")

    except Exception as e:
        await message.reply(f"❌ **Error!** Failed to add premium user: `{e}`")

@app.on_message(filters.command("removepremium") & filters.user(OWNER_ID))
async def remove_premium_command(client, message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2 or not args[1].isdigit():
            await message.reply("❗ **Syntax Error:** Usage: `/removepremium <user_id>`")
            return

        target_user_id = int(args[1])
        if target_user_id == OWNER_ID:
            await message.reply("❌ **Access Denied!** You cannot remove the owner's premium status.")
            return

        user_doc = get_user_data(target_user_id)
        if user_doc and user_doc.get("is_premium"):
            update_user_data(target_user_id, {"is_premium": False, "plan_tier": "free", "premium_expiry": None})
            await message.reply(f"✅ **Success!** User `{target_user_id}` has been revoked from **PREMIUM ACCESS**.")
            try:
                await client.send_message(target_user_id, "❗ **System Notification!** Your premium access has been revoked.")
            except Exception as e:
                logger.warning(f"Could not notify user {target_user_id} about premium revocation: {e}")
            await log_to_channel(client, f"Admin `{message.from_user.id}` revoked premium from user `{target_user_id}`.")
        else:
            await message.reply(f"User `{target_user_id}` is not a premium user or no record found in the system.")

    except Exception as e:
        await message.reply(f"❌ **Error!** Failed to remove premium user: `{e}`")

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
    await callback_query.edit_message_text(
        "⚙️ **User Account Settings:**\n\nChoose a platform to configure:",
        reply_markup=user_settings_inline_menu
    )
    logger.info(f"User {user_id} accessed user settings menu.")

@app.on_callback_query(filters.regex("^settings_my_plan$"))
async def show_my_plan(client, callback_query):
    user_id = callback_query.from_user.id
    plan, expiry, uploads = get_user_plan_and_expiry(user_id)
    user_doc = get_user_data(user_id)
    
    if plan == "free" or plan is None:
        message_text = "🆓 **Your Current Plan:** Free\n"
        message_text += "You have a basic plan with limited features."
    else:
        uploads_today = user_doc.get("uploads_today", 0)
        quota = PREMIUM_PLANS.get(plan, {}).get("upload_quota")

        message_text = f"👑 **Your Current Plan:** {plan.capitalize()}\n"
        if expiry:
            message_text += f"**Expires On:** `{expiry.strftime('%Y-%m-%d %H:%M:%S')} UTC`\n"
        if quota:
            message_text += f"**Daily Quota:** `{uploads_today}` / `{quota}` uploads today."
    
    await callback_query.answer("Fetching plan details...")
    await callback_query.message.edit_text(message_text, reply_markup=user_settings_inline_menu, parse_mode=enums.ParseMode.MARKDOWN)

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
    all_users = list(users_collection.find({}, {"_id": 1, "first_name": 1, "username": 1, "role": 1, "plan_tier": 1, "premium_expiry": 1}))
    user_list_text = "**👥 Registered System Users:**\n\n"
    if not all_users:
        user_list_text += "No user records found in the system database."
    else:
        for user in all_users:
            role = user.get("role", "user").capitalize()
            plan_tier = user.get("plan_tier", "free").capitalize()
            expiry_date_str = user.get("premium_expiry")
            expiry_info = f", Expires: `{datetime.fromisoformat(expiry_date_str).strftime('%Y-%m-%d')}`" if expiry_date_str else ""
            user_list_text += (
                f"• ID: `{user['_id']}`\n"
                f"  Name: `{user.get('first_name', 'N/A')}`\n"
                f"  Username: `@{user.get('username', 'N/A')}`\n"
                f"  Status: `{role}`, Plan: `{plan_tier}`{expiry_info}\n\n"
            )
    await callback_query.edit_message_text(user_list_text, reply_markup=Admin_markup, parse_mode=enums.ParseMode.MARKDOWN)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) viewed system users list.")

@app.on_callback_query(filters.regex("^admin_add_user_prompt$"))
async def admin_add_user_prompt_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("Initiating user upgrade protocol...")
    user_states[user_id] = {"step": AWAITING_ADD_USER}
    await callback_query.edit_message_text(
        "Please transmit the **Telegram User ID** and **plan duration (in days)**, separated by a space.\n"
        "Example: `123456789 30`",
        reply_markup=Admin_markup
    )
    logger.info(f"Admin {user_id} prompted to add premium user.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_ADD_USER) & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_add_user_id_input(client, message):
    user_id = message.from_user.id
    try:
        target_user_id_str, days_str = message.text.strip().split()
        target_user_id = int(target_user_id_str)
        days = int(days_str)
        expiry_date = datetime.now(timezone.utc) + timedelta(days=days)
        plan_tier = f"{days}day" if days in [2, 5, 8, 20, 30] else ("2mo" if days == 60 else ("6mo" if days == 180 else "1year"))

        user_data_to_set = {
            "is_premium": True,
            "plan_tier": plan_tier,
            "premium_expiry": expiry_date.isoformat(),
            "uploads_today": 0
        }

        update_user_data(target_user_id, user_data_to_set)
        await message.reply(f"✅ **Success!** User `{target_user_id}` has been granted **{plan_tier.capitalize()}** status expiring in **{days} days**.", reply_markup=Admin_markup)
        try:
            await client.send_message(target_user_id, f"🎉 **System Notification!** Your premium access has been extended by **{days} days**! Use `/start` to access your enhanced features.")
        except Exception as e:
            logger.warning(f"Could not notify user {target_user_id} about premium extension: {e}")
        await log_to_channel(client, f"Admin `{message.from_user.id}` granted premium for `{days}` days to user `{target_user_id}`.")
    except (ValueError, IndexError):
        await message.reply("❌ **Input Error.** Invalid format. Please use `User ID days` (e.g., `123456789 30`).", reply_markup=Admin_markup)
    finally:
        user_states.pop(user_id, None)

@app.on_callback_query(filters.regex("^admin_remove_user_prompt$"))
async def admin_remove_user_prompt_inline(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    await callback_query.answer("Initiating user downgrade protocol...")
    user_states[user_id] = {"step": AWAITING_REMOVE_USER}
    await callback_query.edit_message_text(
        "Please transmit the **Telegram User ID** of the user you wish to revoke **PREMIUM ACCESS** from.\n"
        "Input the numeric ID now.",
        reply_markup=Admin_markup
    )
    logger.info(f"Admin {user_id} prompted to remove premium user.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_REMOVE_USER) & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
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
            update_user_data(target_user_id, {"is_premium": False, "plan_tier": "free", "premium_expiry": None})
            await message.reply(f"✅ **Success!** User `{target_user_id}` has been revoked from **PREMIUM ACCESS**.", reply_markup=Admin_markup)
            try:
                await client.send_message(target_user_id, "❗ **System Notification!** Your premium access has been revoked.")
            except Exception as e:
                logger.warning(f"Could not notify user {target_user_id} about premium revocation: {e}")
            await log_to_channel(client, f"Admin `{message.from_user.id}` revoked premium from user `{target_user_id}`.")
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
    await callback_query.edit_message_text(
        "Please transmit the **message payload** you wish to broadcast to all active system users.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Terminate Broadcast", callback_data="cancel_broadcast")]])
    )
    logger.info(f"Admin {user_id} prompted for broadcast message.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_BROADCAST_MESSAGE) & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
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
            await asyncio.sleep(0.05) # Use asyncio.sleep in async functions
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
    # A more graceful way to exit for process managers
    os.execv(sys.executable, ['python'] + sys.argv)

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
    active_jobs = jobs_collection.count_documents({"status": {"$in": ["downloading", "processing", "transcoding", "uploading"]}})
    failed_jobs_count = jobs_collection.count_documents({"status": "failed"})

    total_fb_accounts = users_collection.count_documents({"facebook_pages": {"$ne": []}})
    total_youtube_accounts = users_collection.count_documents({"youtube.logged_in": True})

    total_uploads_count_agg = list(users_collection.aggregate([{"$group": {"_id": None, "total": {"$sum": "$total_uploads"}}}]))
    total_uploads_count = total_uploads_count_agg[0]['total'] if total_uploads_count_agg else 0

    stats_message = (
        f"**📊 System Diagnostics & Statistics:**\n\n"
        f"**User Matrix:**\n"
        f"• Total Registered Users: `{total_users}`\n"
        f"• System Administrators: `{admin_users}`\n"
        f"• Premium Access Users: `{premium_users}`\n\n"
        f"**Operational Metrics:**\n"
        f"• Active Upload Jobs: `{active_jobs}`\n"
        f"• Failed Jobs (Total): `{failed_jobs_count}`\n"
        f"• Total Content Transmissions: `{total_uploads_count}`\n"
        f"• Facebook Accounts Synced: `{total_fb_accounts}`\n"
        f"• YouTube Accounts Synced: `{total_youtube_accounts}`\n\n"
    )
    await callback_query.edit_message_text(stats_message, reply_markup=get_general_settings_inline_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) viewed detailed system status.")

# --- All other functions (Facebook, YouTube settings, etc.) remain the same ---
# ... (The rest of the code from the previous turn) ...
# To save space, only the changed functions are shown here. 
# You should paste the rest of your unchanged code below this point.
# The following is a placeholder for the rest of the functions from the previous provided code.
# The complete code is very long, so I am including only the changed parts.

# [PASTE THE REST OF YOUR UNCHANGED CODE HERE, STARTING FROM `show_facebook_settings` all the way to the end]
# The functions below this comment are the same as the last version I sent.

@app.on_callback_query(filters.regex("^settings_facebook$"))
async def show_facebook_settings(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing Facebook configurations...")
    await callback_query.message.edit_text(
        "🚀 **Facebook Login Panel – Choose Your Method:**",
        reply_markup=facebook_settings_inline_menu
    )
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

# --- NEW YouTube Login Flow (OAuth 2.0) ---
@app.on_callback_query(filters.regex("^yt_login_prompt$"))
async def yt_login_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Initiating YouTube OAuth 2.0 flow...")
    user_states[user_id] = {"step": AWAITING_YT_CLIENT_ID}
    await callback_query.message.edit_text(
        "🔑 **YouTube OAuth 2.0 Setup:**\n\n"
        "**Step 1:** Please provide your **Google Client ID**.\n"
        "You can find this in your Google Cloud Console's 'Credentials' page.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_youtube')]])
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_CLIENT_ID))
async def yt_get_client_id(client, message):
    user_id = message.from_user.id
    user_states[user_id]["client_id"] = message.text.strip()
    user_states[user_id]["step"] = AWAITING_YT_CLIENT_SECRET
    await message.reply(
        "**Step 2:** Now, please provide your **Google Client Secret**."
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_CLIENT_SECRET))
async def yt_get_client_secret(client, message):
    user_id = message.from_user.id
    client_secret = message.text.strip()
    client_id = user_states[user_id].get("client_id")
    
    if not client_id:
        user_states.pop(user_id, None)
        await message.reply("❌ **Error:** Client ID was not found. Please restart the login process.", reply_markup=youtube_settings_inline_menu)
        return

    try:
        temp_creds_json = {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [REDIRECT_URI],
                "scopes": GOOGLE_API_SCOPES
            }
        }
        
        flow = Flow.from_client_config(temp_creds_json, scopes=GOOGLE_API_SCOPES)
        flow.redirect_uri = REDIRECT_URI
        
        auth_url, state = flow.authorization_url(access_type='offline', include_granted_scopes='true')
        
        user_states[user_id].update({
            "client_secret": client_secret,
            "state": state,
            "step": AWAITING_YT_AUTH_CODE,
            "flow_config": temp_creds_json
        })
        
        await message.reply(
            f"**Step 3:** Please click the link below to grant access to your YouTube channel:\n\n"
            f"[**Click here to authenticate with Google**]({auth_url})\n\n"
            "After you grant access, **copy the full URL from your browser's address bar** and paste it here.",
            parse_mode=enums.ParseMode.MARKDOWN,
            disable_web_page_preview=True
        )

    except Exception as e:
        user_states.pop(user_id, None)
        await message.reply(f"❌ **Error during URL generation:** `{e}`\nPlease check your Client ID and Secret, then restart the process.", reply_markup=youtube_settings_inline_menu)

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_AUTH_CODE))
async def yt_get_auth_code(client, message):
    user_id = message.from_user.id
    state_data = user_states.get(user_id)
    if not state_data:
        await message.reply("❌ **Session Expired.** Please restart the YouTube login flow.")
        return
    
    redirect_response_url = message.text.strip()
    try:
        temp_creds_json = state_data["flow_config"]
        flow = Flow.from_client_config(temp_creds_json, scopes=GOOGLE_API_SCOPES, state=state_data["state"])
        flow.redirect_uri = REDIRECT_URI
        
        flow.fetch_token(authorization_response=redirect_response_url)
        
        creds = flow.credentials
        
        youtube_data = {
            "logged_in": True,
            "access_token": creds.token,
            "refresh_token": creds.refresh_token,
            "token_expiry": creds.expiry.isoformat(),
            "client_id": creds.client_id,
            "client_secret": creds.client_secret
        }
        
        update_user_data(user_id, {"youtube": youtube_data})
        
        await message.reply(
            f"✅ **YouTube Login Successful!**",
            reply_markup=youtube_settings_inline_menu
        )
        await log_to_channel(client, f"User `{user_id}` successfully linked YouTube.")
        
    except Exception as e:
        await message.reply(f"❌ **Authentication Failed.** An error occurred: `{e}`. Please ensure you copied the full URL and that your `REDIRECT_URI` is set correctly.", reply_markup=youtube_settings_inline_menu)
        logger.error(f"YouTube auth code exchange failed for user {user_id}: {e}")
    finally:
        user_states.pop(user_id, None)

@app.on_callback_query(filters.regex("^yt_check_expiry_date$"))
async def yt_check_expiry_date(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Retrieving YouTube token expiry data...")
    user_doc = get_user_data(user_id)
    yt_data = user_doc.get("youtube", {})
    
    if not yt_data.get("logged_in"):
        await callback_query.message.edit_text("❌ **No YouTube Account Linked.**", reply_markup=youtube_settings_inline_menu)
        return
        
    creds = get_youtube_credentials(user_id)
    
    status_text = ""
    if creds:
        expiry_date = creds.expiry
        if expiry_date > datetime.now(timezone.utc):
            status_text = f"✅ Valid until: `{expiry_date.strftime('%Y-%m-%d %H:%M:%S')} UTC`"
        else:
            status_text = "❌ Expired and refresh failed. Please log in again."
    else:
        status_text = "❌ Not Logged In or invalid credentials."
        
    info_text = f"🗓️ **YouTube Token Expiry Status:**\n{status_text}\n\n_**Note:** Tokens are refreshed automatically._"
    
    await callback_query.message.edit_text(info_text, reply_markup=youtube_settings_inline_menu, parse_mode=enums.ParseMode.MARKDOWN)



# --- NEW conversational Facebook Login Flow (OAuth) ---
@app.on_callback_query(filters.regex("^fb_oauth_login_prompt$"))
async def fb_oauth_login_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Starting Facebook OAuth Login...")
    user_states[user_id] = {"step": AWAITING_FB_OAUTH_TOKEN}
    await callback_query.message.edit_text(
        "🔑 **Facebook Page Access Token Setup:**\n\n"
        "**1.** Generate a long-lived **User Access Token** from Meta's Graph API Explorer.\n"
        "**2.** Use that token to get a long-lived **Page Access Token** for your page.\n"
        "**3.** Please paste your **long-lived Page Access Token** here.\n\n"
        "This token will be used to manage uploads to your Facebook page.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_OAUTH_TOKEN))
async def fb_get_oauth_token(client, message):
    user_id = message.from_user.id
    page_access_token = message.text.strip()
    user_states.pop(user_id, None)

    if not page_access_token:
        await message.reply("❌ Login failed. No token provided. Please restart the process.", reply_markup=facebook_settings_inline_menu)
        return

    try:
        await client.send_chat_action(user_id, enums.ChatAction.TYPING)
        response = requests.get(f"https://graph.facebook.com/me?access_token={page_access_token}")
        response.raise_for_status()
        page_data = response.json()

        if 'id' in page_data and 'name' in page_data:
            page_info = {
                "id": page_data['id'],
                "name": page_data['name'],
                "access_token": page_access_token
            }
            update_user_data(user_id, {
                "facebook_pages": [page_info],
                "facebook_selected_page_id": page_data['id'],
                "facebook_selected_page_name": page_data['name'],
            })
            await message.reply(
                f"✅ **Facebook Login Successful!**\n"
                f"You are now logged in to page: **{page_data['name']}**.\n"
                f"Page ID: `{page_data['id']}`",
                reply_markup=facebook_settings_inline_menu,
                parse_mode=enums.ParseMode.MARKDOWN
            )
            await log_to_channel(client, f"User `{user_id}` successfully linked Facebook with OAuth token.")
        else:
            await message.reply("❌ Invalid Page Access Token. Could not retrieve page info.", reply_markup=facebook_settings_inline_menu)
    except requests.exceptions.RequestException as e:
        await message.reply(f"❌ **API Error:** `{e}`\nPlease check your token and try again.", reply_markup=facebook_settings_inline_menu)
    except Exception as e:
        await message.reply(f"❌ **An unexpected error occurred:** `{e}`. Please try again.", reply_markup=facebook_settings_inline_menu)

@app.on_callback_query(filters.regex("^fb_stats_status$"))
async def fb_stats_status(client, callback_query):
    user_id = callback_query.from_user.id
    user_doc = get_user_data(user_id)
    page_name = user_doc.get("facebook_selected_page_name", "Not Logged In")
    page_id = user_doc.get("facebook_selected_page_id", "N/A")

    message_text = (
        "📈 **Facebook Account Status**\n\n"
        f"**Account Status:** {'✅ Logged In' if page_name != 'Not Logged In' else '❌ Not Logged In'}\n"
        f"**Page Name:** `{page_name}`\n"
        f"**Page ID:** `{page_id}`\n"
        f"**Token Status:** The token is stored and will be used for uploads.\n"
    )
    await callback_query.answer("Fetching account status...")
    await callback_query.message.edit_text(message_text, reply_markup=facebook_settings_inline_menu, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^fb_refresh_pages_list$"))
async def refresh_facebook_pages(client, callback_query):
    user_id = callback_query.from_user.id
    user_doc = get_user_data(user_id)
    user_access_token = user_doc.get("facebook_long_lived_token")
    
    await callback_query.answer("This feature requires a user access token and is currently unavailable.", show_alert=True)

@app.on_callback_query(filters.regex("^fb_set_title$"))
async def fb_set_title_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting title input...")
    user_states[user_id] = {"step": AWAITING_FB_TITLE}
    await callback_query.edit_message_text(
        "📝 **Facebook Title Input Module:**\n\nPlease transmit the new Facebook video/post title.\n"
        "_(Type 'skip' to use the default title.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook title.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_TITLE))
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
    await callback_query.edit_message_text(
        "🏷️ **Facebook Tag Input Module:**\n\nPlease transmit the new Facebook tags (e.g., `#reels #video #photo`). Separate with spaces.\n"
        "_(Type 'skip' to use default tags.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook tags.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_TAG))
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
    await callback_query.edit_message_text(
        "📄 **Facebook Description Input Module:**\n\nPlease transmit the new Facebook description for your uploads.\n"
        "_(Type 'skip' to use the default description.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook description.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_DESCRIPTION))
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
    await callback_query.answer("Awaiting default upload type selection...")
    await callback_query.edit_message_text("🎥 **Facebook Default Upload Type Selector:**\n\nSelect the default content type for your Facebook uploads:", reply_markup=facebook_upload_type_inline_menu)
    logger.info(f"User {callback_query.from_user.id} accessed Facebook default upload type selection.")

@app.on_callback_query(filters.regex("^fb_upload_type_"))
async def fb_set_default_upload_type(client, callback_query):
    user_id = callback_query.from_user.id
    upload_type = callback_query.data.split("_")[-1].capitalize()
    update_user_data(user_id, {"facebook_settings.upload_type": upload_type})
    await callback_query.answer(f"Default Facebook upload type set to: {upload_type}", show_alert=True)
    await callback_query.edit_message_text(
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
    await callback_query.edit_message_text(
        "⏰ **Facebook Schedule Configuration Module:**\n\nPlease transmit the desired schedule date and time.\n"
        "**Format:** `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`)\n"
        "_**System Note:** Time will be interpreted in UTC._\n"
        "_(Type 'clear' to remove any existing schedule.)_",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]])
    )
    logger.info(f"User {user_id} prompted for Facebook schedule time.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_SCHEDULE_TIME))
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
        if schedule_dt <= datetime.utcnow() + timedelta(minutes=10):
            await message.reply("❌ **Time Constraint Violation.** Schedule time must be at least 10 minutes in the future. Please try again with a later time.")
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
    await callback_query.answer("Awaiting privacy selection...")
    await callback_query.edit_message_text("🔒 **Facebook Privacy Configuration Module:**\n\nSelect Facebook privacy setting:", reply_markup=get_privacy_inline_menu('fb'))
    logger.info(f"User {callback_query.from_user.id} accessed Facebook privacy selection.")

@app.on_callback_query(filters.regex("^fb_privacy_"))
async def fb_set_privacy(client, callback_query):
    user_id = callback_query.from_user.id
    privacy = "Public" if 'public' in callback_query.data else ("Private" if 'private' in callback_query.data else "Draft")
    update_user_data(user_id, {"facebook_settings.privacy": privacy})
    await callback_query.answer(f"Facebook privacy set to: {privacy}", show_alert=True)
    await callback_query.edit_message_text(
        f"✅ **Facebook Privacy Configured.** Set to: {privacy}",
        reply_markup=facebook_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set Facebook privacy to `{privacy}`.")
    logger.info(f"User {user_id} set Facebook privacy to {privacy}.")

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

    if not has_upload_quota(user_id) and not is_admin(user_id):
        await message.reply("❌ **Quota Exceeded.** You have reached your daily upload limit. Please upgrade your plan or try again tomorrow.")
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

    await callback_query.answer(f"Selected {platform.capitalize()} for upload.")
    
    user_doc = get_user_data(user_id)
    
    if platform == "facebook":
        if not user_doc.get("facebook_pages"):
            await callback_query.message.edit_text("❌ **Authentication Required.** You are not logged into Facebook or haven't selected a page. Please navigate to `⚙️ Settings` -> `📘 Facebook Settings` to configure your account first.")
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
        if not user_doc.get("youtube", {}).get("logged_in"):
            await callback_query.message.edit_text("❌ **Authentication Required.** You are not logged into YouTube. Please navigate to `⚙️ Settings` -> `▶️ YouTube Settings` to configure your account first.")
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

    await callback_query.answer(f"Selected Facebook {upload_type.capitalize()} upload.")
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
        # Allow media sending even without a state, but don't process it.
        # This prevents the bot from erroring out when a user sends a random video.
        logger.warning(f"User {user_id} sent media without an active upload state. Ignoring.")
        return

    if not is_premium_user(user_id) and not is_admin(user_id):
        await message.reply("❌ **Access Restricted.** You need **PREMIUM ACCESS** to upload content. Please contact the administrator.")
        user_states.pop(user_id, None)
        logger.warning(f"Non-premium user {user_id} attempted media upload.")
        return
    
    file_info = message.video or message.photo
    if file_info.file_size > 3 * 1024 * 1024 * 1024: # 3 GB Limit
        await message.reply("❌ **File Size Limit Exceeded.** The maximum allowed file size is 3GB.")
        user_states.pop(user_id, None)
        return

    if not os.path.exists("downloads"):
        os.makedirs("downloads")
    
    initial_status_msg = await message.reply("⏳ **Data Acquisition In Progress...** Downloading your content.")
    
    job_id = jobs_collection.insert_one({
        "user_id": user_id,
        "platform": state["platform"],
        "status": "downloading",
        "start_time": datetime.utcnow()
    }).inserted_id
    user_states[user_id]["job_id"] = str(job_id)

    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        file_extension = os.path.splitext(file_info.file_name or "video.mp4")[1] if message.video else ".jpg"
        download_filename = f"downloads/{user_id}_{timestamp}{file_extension}"
        
        file_path = await client.download_media(
            message,
            file_name=download_filename,
            progress=download_progress_callback,
            progress_args=(client, initial_status_msg)
        )
        
        user_states[user_id]["file_path"] = file_path
        user_states[user_id]["step"] = AWAITING_UPLOAD_TITLE
        
        user_doc = get_user_data(user_id)
        platform = state["platform"]
        default_title = user_doc.get(f"{platform}_settings", {}).get("title", "Default Title")
        
        video_metadata = get_video_metadata(file_path) if message.video else None
        
        if video_metadata:
            is_shorts_candidate = video_metadata['duration'] <= 60 and video_metadata.get('aspect_ratio', 1) < 1.0
            
            summary_msg = f"🎬 **File: {file_info.file_name or 'video'}** | ⏱️ {video_metadata['duration']:.0f}s | 📐 {video_metadata['width']}x{video_metadata['height']}\n\n"
            
            if platform == 'youtube' and is_shorts_candidate:
                summary_msg += "Detected: **YouTube Shorts Candidate**"
                state["upload_intent"] = "shorts"
            else:
                summary_msg += "Detected: **Normal Video**"
                state["upload_intent"] = "video"

            await initial_status_msg.edit_text(summary_msg)
            await asyncio.sleep(1)

            jobs_collection.update_one({"_id": job_id}, {"$set": {"analysis": video_metadata, "upload_intent": state.get("upload_intent")}})

        await client.send_message(
            user_id,
            f"✅ **Download Complete.**\n\n"
            f"📝 **Metadata Input Required.** Now, transmit the **title** for your `{platform.capitalize()}` content.\n"
            f"_(Type 'skip' to use your default title: '{default_title}')_"
        )
        logger.info(f"User {user_id} media downloaded to {file_path}. Awaiting title.")
    except Exception as e:
        await initial_status_msg.edit_text(f"❌ **Data Acquisition Failed.** Error downloading media: `{e}`")
        logger.error(f"Failed to download media for user {user_id}: {e}", exc_info=True)
        jobs_collection.update_one({"_id": job_id}, {"$set": {"status": "failed", "error": str(e)}})
        user_states.pop(user_id, None)

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_TITLE))
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
        state["title"] = apply_template(user_doc.get(f"{platform}_settings", {}).get("title", "Default Title"), user_doc, state)
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

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_DESCRIPTION))
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
        state["description"] = apply_template(user_doc.get(f"{platform}_settings", {}).get("description", "Default Description"), user_doc, state)
        await message.reply(f"✅ **Description Input Skipped.** Using default description: '{state['description']}'.")
        logger.info(f"User {user_id} skipped description input for {platform}.")
    else:
        state["description"] = description_input
        await message.reply(f"✅ **Description Recorded.** New description: '{description_input}'.")
        logger.info(f"User {user_id} provided description for {platform}: '{description_input}'.")

    user_states[user_id]["step"] = AWAITING_UPLOAD_TAGS
    default_tags = user_doc.get(f"{platform}_settings", {}).get("tag", "Default Tags")

    await message.reply(
        f"🏷️ **Hashtag Configuration Module.** Please transmit a list of **hashtags**.\n"
        f"_(Type 'skip' to use your default tags: '{default_tags}')_"
    )
    logger.info(f"User {user_id} awaiting tags for {platform}.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_TAGS))
async def handle_upload_tags(client, message):
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state:
        await message.reply("❌ **Session Interrupted.** Please restart the upload process.")
        return

    tags_input = message.text.strip()
    platform = state["platform"]
    user_doc = get_user_data(user_id)
    
    if tags_input.lower() == "skip":
        state["tags"] = user_doc.get(f"{platform}_settings", {}).get("tag", "Default Tags").split()
        await message.reply(f"✅ **Tags Input Skipped.** Using default tags.")
        logger.info(f"User {user_id} skipped tags input for {platform}.")
    else:
        state["tags"] = tags_input.split()
        await message.reply(f"✅ **Tags Recorded.** New tags: '{tags_input}'.")
        logger.info(f"User {user_id} provided tags for {platform}: '{tags_input}'.")

    user_states[user_id]["step"] = AWAITING_UPLOAD_THUMBNAIL
    await message.reply(
        "🖼️ **Thumbnail Selection Module.** Choose an option for your thumbnail:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Upload Custom", callback_data="thumb_custom")],
            [InlineKeyboardButton("Auto-generate", callback_data="thumb_auto_generate")]
        ])
    )

@app.on_callback_query(filters.regex("^thumb_"))
async def handle_thumbnail_selection(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)
    if not state or state.get("step") != AWAITING_UPLOAD_THUMBNAIL:
        await callback_query.answer("❗ **Invalid Operation.** Please ensure you are in an active upload sequence.", show_alert=True)
        return
    
    choice = callback_query.data.split("_")[1]
    
    if choice == "custom":
        state["thumbnail_choice"] = "custom"
        await callback_query.message.edit_text("Please send the **image file** you want to use as a thumbnail.")
        user_states[user_id]["step"] = "awaiting_custom_thumbnail"
    elif choice == "auto_generate":
        state["thumbnail_choice"] = "auto_generate"
        await callback_query.message.edit_text("⏳ **Generating thumbnail...** This might take a moment.")
        
        # FIXED: Run blocking I/O in an executor to prevent freezing the bot
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            thumbnail_path = await loop.run_in_executor(
                pool, generate_thumbnail, state["file_path"]
            )
        state["thumbnail_path"] = thumbnail_path

        if state["thumbnail_path"]:
            await callback_query.message.delete() # Clean up the "Generating..." message
            user_states[user_id]["step"] = AWAITING_UPLOAD_VISIBILITY
            await prompt_visibility_selection(client, callback_query.message, user_id, state["platform"])
        else:
            await callback_query.message.edit_text("❌ **Thumbnail Generation Failed.** Please try uploading a custom one or check the logs.")
            user_states.pop(user_id, None)

@app.on_message(filters.photo & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == "awaiting_custom_thumbnail"))
async def handle_custom_thumbnail(client, message):
    user_id = message.chat.id
    state = user_states.get(user_id)
    
    status_msg = await message.reply("⏳ **Downloading thumbnail...**")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        thumb_path = f"downloads/thumb_{user_id}_{timestamp}.jpg"
        await client.download_media(message.photo, file_name=thumb_path)
        state["thumbnail_path"] = thumb_path
        user_states[user_id]["step"] = AWAITING_UPLOAD_VISIBILITY
        await status_msg.delete()
        await prompt_visibility_selection(client, message, user_id, state["platform"])
    except Exception as e:
        await status_msg.edit_text(f"❌ **Thumbnail Download Failed.** Error: `{e}`. Please try again.")
        logger.error(f"Failed to download thumbnail for user {user_id}: {e}", exc_info=True)
        user_states.pop(user_id, None)

async def prompt_visibility_selection(client, message, user_id, platform):
    """Sends the visibility selection prompt."""
    if platform == "youtube":
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Public", callback_data="visibility_public")],
                [InlineKeyboardButton("Private", callback_data="visibility_private")],
                [InlineKeyboardButton("Unlisted", callback_data="visibility_unlisted")]
            ]
        )
    else: # Facebook
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Public", callback_data="visibility_public")],
                [InlineKeyboardButton("Private (Draft)", callback_data="visibility_private")]
            ]
        )
    await client.send_message(user_id, "🌐 **Visibility Configuration Module.** Select content visibility:", reply_markup=keyboard)
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
        [
            [InlineKeyboardButton("Publish Now", callback_data="schedule_now")],
            [InlineKeyboardButton("Schedule Later", callback_data="schedule_later")]
        ]
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
        await callback_query.answer("Publishing now selected.")
        await callback_query.message.edit_text("⏳ **Data Processing Initiated...** Preparing your content for immediate transmission. Please standby.")
        await initiate_upload(client, callback_query.message, user_id)
        logger.info(f"User {user_id} chose 'publish now' for {platform}.")
    elif schedule_choice == "later":
        user_states[user_id]["step"] = AWAITING_UPLOAD_SCHEDULE_DATETIME
        await callback_query.answer("Awaiting schedule time input...")
        await callback_query.message.edit_text(
            "📅 **Temporal Configuration Module.** Please transmit the desired schedule date and time.\n"
            "**Format:** `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`)\n"
            "_**System Note:** Time will be interpreted in UTC._"
        )
        logger.info(f"User {user_id} chose 'schedule later' for {platform}.")

@app.on_message(filters.text & filters.private & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_SCHEDULE_DATETIME))
async def handle_schedule_datetime_input(client, message):
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state:
        await message.reply("❌ **Session Interrupted.** Please restart the upload process.")
        return

    schedule_str = message.text.strip()
    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")

        if schedule_dt <= datetime.utcnow() + timedelta(minutes=10):
            await message.reply("❌ **Time Constraint Violation.** Schedule time must be at least 10 minutes in the future. Please transmit a later time.")
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

# FIXED: Complete function rewrite to fix SyntaxError and improve robustness.
async def initiate_upload(client, message, user_id):
    """Initiates the actual content upload with a proper try/finally block for cleanup."""
    state = user_states.get(user_id)
    if not state:
        await client.send_message(user_id, "❌ **Upload Process Aborted.** Session state lost.")
        logger.error(f"Upload initiated without valid state for user {user_id}.")
        return

    # Extract state variables early
    file_path = state.get("file_path")
    thumbnail_path = state.get("thumbnail_path")
    processed_file_path = file_path  # Default to original file path

    try:
        # Check for essential data
        if not all([file_path, state.get("title"), state.get("description")]):
            await client.send_message(user_id, "❌ **Upload Failure.** Missing essential metadata. Please restart.")
            logger.error(f"Missing essential upload data for user {user_id}. State: {state}")
            return

        job_id = ObjectId(state["job_id"])
        jobs_collection.update_one({"_id": job_id}, {"$set": {"status": "processing"}})
        status_msg = await client.send_message(user_id, "⏳ **Data Processing Initiated...**")

        await log_to_channel(client, f"User `{user_id}` initiating upload for {state['platform']}.")

        retries = 3
        upload_successful = False
        last_error = "Unknown error"

        for attempt in range(retries):
            try:
                # --- Platform-specific logic ---
                if state["platform"] == "facebook":
                    # (Facebook upload logic as before)
                    # This section is kept concise for the example. Your full logic goes here.
                    fb_access_token, fb_page_id = get_facebook_page_info(user_id)
                    if not fb_access_token: raise RuntimeError("Facebook login required.")

                    def fb_upload_task():
                        return upload_facebook_content(
                            file_path=processed_file_path,
                            content_type=state.get("upload_type", "video").lower(),
                            title=state.get("title"),
                            description=state.get("description"),
                            access_token=fb_access_token,
                            page_id=fb_page_id,
                            visibility=state.get("visibility"),
                            schedule_time=state.get("schedule_time")
                        )
                    
                    await status_msg.edit_text(f"📤 **Uploading to Facebook...** (Attempt {attempt + 1}/{retries})")
                    jobs_collection.update_one({"_id": job_id}, {"$set": {"status": "uploading"}})

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        fb_result = await asyncio.get_event_loop().run_in_executor(executor, fb_upload_task)

                    if fb_result and 'id' in fb_result:
                        await status_msg.edit_text(f"✅ **Facebook Upload Complete!** ID: `{fb_result['id']}`")
                        upload_successful = True
                        break
                    else:
                        raise RuntimeError(f"Facebook API did not return an ID. Response: {fb_result}")

                elif state["platform"] == "youtube":
                    # (YouTube upload logic as before)
                    # This section is kept concise for the example. Your full logic goes here.
                    creds = get_youtube_credentials(user_id)
                    if not creds: raise RuntimeError("YouTube login required.")
                    
                    def yt_upload_task():
                        youtube = build('youtube', 'v3', credentials=creds)
                        body = {
                            'snippet': {'title': state.get('title'), 'description': state.get('description'), 'tags': state.get('tags', [])},
                            'status': {'privacyStatus': state.get('visibility')}
                        }
                        if state.get('schedule_time'): body['status']['publishAt'] = state['schedule_time'].isoformat() + "Z"

                        media_file = MediaFileUpload(processed_file_path, chunksize=-1, resumable=True)
                        request = youtube.videos().insert(part=",".join(body.keys()), body=body, media_body=media_file)
                        
                        response = None
                        while response is None:
                            status, response = request.next_chunk()
                            if status:
                                logger.info(f"Uploaded {int(status.progress() * 100)}% to YouTube.")
                        return response

                    await status_msg.edit_text(f"📤 **Uploading to YouTube...** (Attempt {attempt + 1}/{retries})")
                    jobs_collection.update_one({"_id": job_id}, {"$set": {"status": "uploading"}})

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        yt_result = await asyncio.get_event_loop().run_in_executor(executor, yt_upload_task)
                    
                    if yt_result and 'id' in yt_result:
                        await status_msg.edit_text(f"✅ **YouTube Upload Complete!** Video ID: `{yt_result['id']}`")
                        upload_successful = True
                        break
                    else:
                        raise RuntimeError(f"YouTube API did not return an ID. Response: {yt_result}")

            except Exception as e:
                last_error = str(e)
                logger.error(f"Upload attempt {attempt + 1} for user {user_id} failed: {e}", exc_info=True)
                if attempt < retries - 1:
                    await status_msg.edit_text(f"❌ **Upload Failed.** Retrying in 5 seconds...")
                    await asyncio.sleep(5)
        
        # After the loop
        if not upload_successful:
            await status_msg.edit_text(f"❌ **Upload Failed After All Retries.** Reason: `{last_error}`")
            jobs_collection.update_one({"_id": job_id}, {"$set": {"status": "failed", "error": last_error, "end_time": datetime.utcnow()}})
        else:
            users_collection.update_one({"_id": user_id}, {"$inc": {"total_uploads": 1, "uploads_today": 1}})
            jobs_collection.update_one({"_id": job_id}, {"$set": {"status": "success", "end_time": datetime.utcnow()}})

    finally:
        # This cleanup block will ALWAYS run, regardless of success or failure.
        logger.info(f"Cleaning up files for job {state.get('job_id')}")
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Removed original file: {file_path}")
        if processed_file_path and processed_file_path != file_path and os.path.exists(processed_file_path):
            os.remove(processed_file_path)
            logger.info(f"Removed processed file: {processed_file_path}")
        if thumbnail_path and os.path.exists(thumbnail_path):
            os.remove(thumbnail_path)
            logger.info(f"Removed thumbnail: {thumbnail_path}")
        
        user_states.pop(user_id, None) # Clear the user's state
        logger.info(f"State cleared for user {user_id}")

# === KEEP ALIVE SERVER ===
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/oauth2callback'):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            # Provide a simple, user-friendly response page
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

threading.Thread(target=run_server, daemon=True).start()

# === START BOT ===
if __name__ == "__main__":
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Created 'downloads' directory.")

    logger.info("Bot system initiating...")
    app.run()
