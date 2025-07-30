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
import mimetypes # New import for better MIME type detection

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
    # Ensure the _id field is always primary key and unique.
    # The original check was for 'user_id_1' which might be from old migration.
    # We want to ensure _id is unique which it is by default.
    # If a custom unique index was created for 'user_id' named 'user_id_1',
    # and it conflicts with _id, this might be problematic.
    # For robust ID management, stick to Pyrogram's `message.from_user.id` as `_id`.
    # Let's verify _id is used for querying. It is.
    # This block can largely be removed if _id is consistently used as primary key.
    # If there was a custom 'user_id' index, this would be the correct place to handle it.
    
    # Check if a custom unique index named 'user_id_1' exists and drop it if it's not on _id
    index_info = users_collection.index_information()
    if 'user_id_1' in index_info and index_info['user_id_1'].get('unique') and index_info['user_id_1'].get('key') == [('user_id', 1)]:
        logger.warning("Found a problematic 'user_id_1' unique index. Attempting to drop it.")
        users_collection.drop_index("user_id_1")
        logger.info("Successfully dropped 'user_id_1' unique index.")
    
except Exception as e:
    logger.error(f"Error checking/dropping problematic user_id index: {e}")


# === KEYBOARDS ===
main_menu_user = ReplyKeyboardMarkup(
    [
        [KeyboardButton("⬆️ Upload Content")], # Changed to a generic upload button
        [KeyboardButton("⚙️ Settings")]
    ],
    resize_keyboard=True
)

main_menu_admin = ReplyKeyboardMarkup(
    [
        [KeyboardButton("⬆️ Upload Content")], # Changed to a generic upload button
        [KeyboardButton("⚙️ Settings"), KeyboardButton("👤 Admin Panel")]
    ],
    resize_keyboard=True
)

def get_general_settings_inline_keyboard(user_id):
    """Returns the general settings inline keyboard based on user role."""
    keyboard = []
    # Only show user settings if user is premium or admin
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
        [InlineKeyboardButton("🔑 Facebook Login (Token)", callback_data='fb_login_token_prompt')], # New button for token login
        [InlineKeyboardButton("📝 Set Title", callback_data='fb_set_title')],
        [InlineKeyboardButton("🏷️ Set Tag", callback_data='fb_set_tag')],
        [InlineKeyboardButton("📄 Set Description", callback_data='fb_set_description')],
        [InlineKeyboardButton("🎥 Default Upload Type", callback_data='fb_default_upload_type')], # Changed for Reels/Video/Photo
        [InlineKeyboardButton("⏰ Set Schedule Time", callback_data='fb_set_schedule_time')],
        [InlineKeyboardButton("🔒 Set Private/Public", callback_data='fb_set_privacy')],
        [InlineKeyboardButton("🗓️ Check Token Info", callback_data='fb_check_token_info')], # Changed from expiry date
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

# New Facebook Upload Type Menu
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

# New inline keyboard for platform selection after "Upload Content"
platform_selection_inline_menu = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("📘 Upload to Facebook", callback_data='upload_select_facebook')],
        [InlineKeyboardButton("▶️ Upload to YouTube", callback_data='upload_select_youtube')]
    ]
)

# === USER STATES (for sequential conversation flows) ===
# This dictionary holds temporary user state information for multi-step processes.
# Example: user_states[user_id] = {"step": "awaiting_fb_title", "file_path": "/tmp/video.mp4"}
user_states = {}

# === CONVERSATION STATES (for specific text input steps) ===
AWAITING_FB_TITLE = "awaiting_fb_title"
AWAITING_FB_TAG = "awaiting_fb_tag"
AWAITING_FB_DESCRIPTION = "awaiting_fb_description"
AWAITING_FB_SCHEDULE_TIME = "awaiting_fb_schedule_time"
AWAITING_FB_ACCESS_TOKEN = "awaiting_fb_access_token"
# AWAITING_FB_USERNAME = "awaiting_fb_username" # Removed direct login
# AWAITING_FB_PASSWORD = "awaiting_fb_password" # Removed direct login
AWAITING_FB_PAGE_SELECTION = "awaiting_fb_page_selection" # New state for page selection

AWAITING_YT_TITLE = "awaiting_yt_title"
AWAITING_YT_TAG = "awaiting_yt_tag"
AWAITING_YT_DESCRIPTION = "awaiting_yt_description"
AWAITING_YT_SCHEDULE_TIME = "awaiting_yt_schedule_time"
AWAITING_YT_ACCESS_TOKEN = "awaiting_yt_access_token"

AWAITING_BROADCAST_MESSAGE = "awaiting_broadcast_message"

# --- Upload Flow Specific States ---
AWAITING_UPLOAD_FILE = "awaiting_upload_file" # Generic for video/photo
AWAITING_UPLOAD_TITLE = "awaiting_upload_title"
AWAITING_UPLOAD_DESCRIPTION = "awaiting_upload_description"
AWAITING_UPLOAD_VISIBILITY = "awaiting_upload_visibility"
AWAITING_UPLOAD_SCHEDULE = "awaiting_upload_schedule"
AWAITING_UPLOAD_SCHEDULE_DATETIME = "awaiting_upload_schedule_datetime"
AWAITING_UPLOAD_TYPE_SELECTION = "awaiting_upload_type_selection" # For FB Reels/Video/Photo

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
    return user_doc.get("facebook_access_token"), user_doc.get("facebook_selected_page_id")

def store_facebook_access_token_for_user(user_id, token):
    """Stores Facebook access token in user data."""
    update_user_data(user_id, {"facebook_access_token": token})

# Removed direct login related functions
# def store_facebook_user_login_details(user_id, username, password):
#     """Stores Facebook user login details (for direct login simulation)."""
#     update_user_data(user_id, {"facebook_username": username, "facebook_password": password})

def store_facebook_selected_page_id(user_id, page_id, page_name):
    """Stores the selected Facebook page ID for uploads."""
    update_user_data(user_id, {"facebook_selected_page_id": page_id, "facebook_selected_page_name": page_name})

def get_facebook_pages(user_access_token):
    """Fetches list of Facebook pages managed by the user token."""
    try:
        # It's crucial to get a User Access Token first, then query for pages
        # The /me/accounts endpoint typically lists pages accessible by a User Access Token
        # Each page object in the response will contain its own page access token.
        # This function should ideally take a long-lived USER access token.
        # Then, it will return pages, and the bot selects the PAGE access token for uploads.
        pages_url = f"https://graph.facebook.com/v19.0/me/accounts?access_token={user_access_token}"
        response = requests.get(pages_url)
        response.raise_for_status()
        return response.json().get('data', [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Facebook pages: {e}")
        return []

def upload_facebook_content(file_path, content_type, title, description, access_token, page_id, visibility="PUBLISHED", schedule_time=None):
    """Uploads content (video, reels, photo) to Facebook Page using Graph API."""
    if not all([file_path, content_type, access_token, page_id]):
        raise ValueError("Missing required parameters for Facebook content upload.")

    params = {
        'access_token': access_token,
        # 'published': 'true' if not schedule_time and visibility.lower() != 'draft' else 'false',
        # 'published' param is handled differently for each type below.
    }
    
    # For photo, default to using caption as description.
    # For video/reels, title and description are separate.
    effective_title = title if title else "Untitled Post"
    effective_description = description if description else "No description provided."

    if content_type == "video":
        post_url = f"https://graph-video.facebook.com/v19.0/{page_id}/videos"
        params['title'] = effective_title
        params['description'] = effective_description
        
        # Facebook video statuses: PUBLISHED, SCHEDULED, DRAFT, UNPUBLISHED
        if schedule_time:
            params['scheduled_publish_time'] = int(schedule_time.timestamp())
            params['status_type'] = 'SCHEDULED_PUBLISH'
            params['published'] = 'false' # Must be false if scheduled
            logger.info(f"Scheduling Facebook video for: {schedule_time}")
        elif visibility.lower() == 'private' or visibility.lower() == 'draft':
            params['status_type'] = 'DRAFT'
            params['published'] = 'false'
            logger.info(f"Uploading Facebook video as DRAFT (visibility: {visibility}).")
        else:
            params['status_type'] = 'PUBLISHED'
            params['published'] = 'true'
            logger.info(f"Uploading Facebook video as PUBLISHED (visibility: {visibility}).")
        
        with open(file_path, 'rb') as f:
            files = {'file': f}
            response = requests.post(post_url, params=params, files=files)

    elif content_type == "reels":
        # Reels API is somewhat different and requires chunked uploads.
        # This is a simplified example focusing on getting a video to upload.
        # A full implementation would involve initializing an upload, sending chunks, then finalizing.
        # For simplicity, we'll try to use the regular video endpoint for now as a fallback/demo,
        # or simulate a Reels specific flow. The provided code has a good attempt for Reels upload.
        # Let's refine the provided Reels logic.
        
        # Reels API requires an initial call to get an upload session
        # Then upload, then publish.
        
        try:
            # Step 1: Initialize upload session
            init_url = f"https://graph.facebook.com/v19.0/{page_id}/video_reels?access_token={access_token}&upload_phase=start"
            init_response = requests.post(init_url)
            init_response.raise_for_status()
            init_data = init_response.json()
            upload_session_id = init_data['upload_session_id']
            upload_url = init_data['upload_url']
            
            # Step 2: Upload video file (simplified - often requires chunking for large files)
            file_size = os.path.getsize(file_path)
            with open(file_path, 'rb') as f:
                upload_headers = {'file_offset': '0', 'X-Entity-Length': str(file_size)}
                # Using PUT for the actual file upload
                upload_response = requests.put(upload_url, headers=upload_headers, data=f)
                upload_response.raise_for_status()
            
            # Step 3: Publish the Reel
            publish_url = f"https://graph.facebook.com/v19.0/{page_id}/video_reels?access_token={access_token}&upload_phase=finish"
            publish_params = {
                'upload_session_id': upload_session_id,
                'video_state': 'PUBLISHED' if visibility.lower() == 'public' else 'DRAFT', # Reels typically Public or Draft
                'title': effective_title,
                'description': effective_description
            }
            if schedule_time:
                # Facebook Reels API does not directly support scheduled publish time in the same way as regular videos.
                # Scheduling for Reels often means setting it to 'SCHEDULED' state and then relying on Facebook's scheduler,
                # or manually publishing after the scheduled time. For this example, we'll log a warning.
                # If scheduling is a hard requirement for Reels, a more complex approach (e.g., cron job to publish drafts) is needed.
                logger.warning("Facebook Reels scheduling is complex and may not be directly supported via API as for regular videos. Consider manual publish after scheduling or check Facebook's API for recent updates.")
                # We won't set scheduled_publish_time here, as it's not a standard parameter for Reels finish phase.
                # If you must schedule, set as DRAFT, and then a separate process would publish it.
                publish_params['video_state'] = 'DRAFT' # Set to draft if scheduled, user must publish later.
                await app.send_message(OWNER_ID, f"**ATTENTION: REELS SCHEDULING NOTE**\n\nUser {page_id} attempted to schedule a Reel. Facebook's Reels API currently lacks direct scheduling. The Reel has been uploaded as a DRAFT. It must be manually published at {schedule_time.strftime('%Y-%m-%d %H:%M UTC')}.")

            response = requests.post(publish_url, json=publish_params)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error during Facebook Reels upload for file {file_path}: {e}")
            if e.response:
                logger.error(f"Facebook API Error Response: {e.response.text}")
            raise RuntimeError(f"Facebook Reels API error: {e}")

    elif content_type == "photo":
        post_url = f"https://graph.facebook.com/v19.0/{page_id}/photos"
        params['caption'] = effective_description if effective_description else effective_title # Use description as caption, fall back to title
        
        if schedule_time:
            params['scheduled_publish_time'] = int(schedule_time.timestamp())
            params['published'] = 'false' # Must be false if scheduled
            logger.info(f"Scheduling Facebook photo for: {schedule_time}")
        elif visibility.lower() == 'private' or visibility.lower() == 'draft':
            params['published'] = 'false' # Draft
            logger.info(f"Uploading Facebook photo as DRAFT (visibility: {visibility}).")
        else:
            params['published'] = 'true' # Public
            logger.info(f"Uploading Facebook photo as PUBLISHED (visibility: {visibility}).")

        with open(file_path, 'rb') as f:
            files = {'source': f}
            response = requests.post(post_url, params=params, files=files)
            
    else:
        raise ValueError(f"Unsupported Facebook content type: {content_type}")

    response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
    result = response.json()
    logger.info(f"Facebook {content_type} upload result: {result}")
    return result

def get_video_info(file_path):
    """Gets video information using ffprobe."""
    try:
        cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0",
               "-show_entries", "stream=width,height,duration_ts,codec_name,pix_fmt",
               "-of", "json", file_path]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=60)
        video_info = json.loads(result.stdout)
        return video_info.get("streams", [{}])[0]
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Could not get video info for {file_path}: {e}")
        return {}

def convert_media_for_facebook(input_path, output_type, target_format):
    """
    Converts media to suitable format for Facebook upload, avoiding re-compression if possible.
    output_type can be 'video', 'reels', 'photo'.
    target_format can be 'mp4' for video/reels or 'jpg'/'png' for photo.
    """
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_path = f"downloads/processed_{base_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.{target_format}"

    input_extension = os.path.splitext(input_path)[1].lower()
    input_mime_type = mimetypes.guess_type(input_path)[0] or ''

    command = []

    if output_type in ["video", "reels"]:
        # Check if input is already a video and in target_format (MP4)
        if input_extension == ".mp4" and input_mime_type.startswith("video/"):
            logger.info(f"[FFmpeg] Input {input_path} is already MP4 video. Copying directly.")
            # Use -c copy to avoid re-encoding and preserve quality
            command = ["ffmpeg", "-i", input_path, "-c", "copy", "-map", "0", "-y", output_path]
        else:
            # If not MP4 or not a video, convert to MP4 (h264, aac)
            # Use lower CRF for better quality, faster preset for speed.
            logger.info(f"[FFmpeg] Converting video to MP4 for Facebook {output_type} for {input_path}")
            # Ensure correct pixel format for broader compatibility, and faststart for streaming.
            command = ["ffmpeg", "-i", input_path, "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                       "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-y", output_path]
            
            # For reels, typically vertical aspect ratio (9:16) is preferred.
            # This complex filter ensures video is within bounds and potentially padded, not aggressively cropped.
            # However, direct copies are better if possible. If re-encoding is needed, and aspect ratio needs adjustment,
            # this filter can be considered, but it might still alter the original.
            # For now, let's keep it simple and avoid aggressive aspect ratio changes unless strictly required
            # by a platform (which Facebook API doesn't strictly enforce for upload, only for display).
            # if output_type == "reels":
            #     command = ["ffmpeg", "-i", input_path, "-vf", "scale='min(iw,ih*9/16)':-1,pad='ih*9/16':ih:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
            #                "-c:v", "libx264", "-preset", "medium", "-crf", "23", "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-y", output_path]
            #     logger.info(f"[FFmpeg] Attempting conversion for Reels to 9:16 (padded) for {input_path}")
            
    elif output_type == "photo":
        # Check if input is already an image in target_format (JPG or PNG)
        if input_mime_type.startswith("image/") and input_extension in [".jpg", ".jpeg", ".png"]:
            logger.info(f"[FFmpeg] Input {input_path} is already a suitable image. Copying directly.")
            import shutil
            shutil.copy(input_path, output_path)
            return output_path
        else:
            # Convert video frame to image or other image formats to JPG
            logger.info(f"[FFmpeg] Converting to photo (JPG) for {input_path}")
            command = ["ffmpeg", "-i", input_path, "-vframes", "1", "-q:v", "2", "-y", output_path]
    else:
        raise ValueError(f"Unsupported output type for conversion: {output_type}")

    try:
        if not command: # Should not happen if logic above is sound, but a safeguard
            raise RuntimeError("No FFmpeg command generated for conversion.")
            
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=900) # Increased timeout
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

def convert_video_to_mp4(input_path, output_path):
    """
    Converts a video to MP4 format using FFmpeg, avoiding re-encoding if already MP4.
    Prioritizes copying streams if possible to avoid quality loss.
    """
    input_extension = os.path.splitext(input_path)[1].lower()
    input_mime_type = mimetypes.guess_type(input_path)[0] or ''

    if input_extension == ".mp4" and input_mime_type.startswith("video/"):
        logger.info(f"[FFmpeg] Input {input_path} is already MP4. Copying directly.")
        import shutil
        shutil.copy(input_path, output_path)
        return output_path
    
    logger.info(f"[FFmpeg] Converting video to MP4 (h264, aac) for {input_path}")
    command = ["ffmpeg", "-i", input_path, "-c:v", "libx264", "-preset", "fast", "-crf", "23",
               "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-y", output_path]

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=600) # 10 min timeout
        logger.info(f"[FFmpeg] Video conversion successful for {input_path}. Output: {result.stdout}")
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"[FFmpeg] Video conversion failed for {input_path}. Command: {' '.join(e.cmd)}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        raise RuntimeError(f"FFmpeg video conversion error: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("FFmpeg not found. Please install FFmpeg and ensure it's in your system's PATH.")
    except subprocess.TimeoutExpired:
        raise RuntimeError("FFmpeg video conversion timed out. Video might be too large or complex.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during FFmpeg video conversion: {e}")
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
        "facebook_settings": {
            "title": "Default Facebook Title", "tag": "#facebook #video #reels", "description": "Default Facebook Description", "upload_type": "Video", "schedule_time": None, "privacy": "Public"
        },
        "youtube_settings": {
            "title": "Default YouTube Title", "tag": "#youtube #video #shorts", "description": "Default YouTube Description", "video_type": "Video (Standard Horizontal/Square)", "schedule_time": None, "privacy": "Public"
        }
    }

    # Ensure owner is always admin and premium
    if user_id == OWNER_ID:
        user_data_to_set["role"] = "admin"
        user_data_to_set["is_premium"] = True
        if "facebook" not in user_data_to_set["premium_platforms"]:
            user_data_to_set["premium_platforms"].append("facebook")
        if "youtube" not in user_data_to_set["premium_platforms"]:
            user_data_to_set["premium_platforms"].append("youtube")

    try:
        # Use $setOnInsert for fields that should only be set on initial creation
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

    # Fetch the updated user doc to ensure latest state is used
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
    """Promotes a user to admin status."""
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2 or not args[1].isdigit():
            await message.reply("❗ **Syntax Error:** Usage: `/addadmin <user_id>`")
            return

        target_user_id = int(args[1])
        update_user_data(target_user_id, {"role": "admin", "is_premium": True, "$addToSet": {"premium_platforms": "facebook", "premium_platforms": "youtube"}})
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
            update_user_data(target_user_id, {"role": "user", "is_premium": False, "premium_platforms": [], "facebook_access_token": None, "facebook_selected_page_id": None, "facebook_selected_page_name": None, "youtube_logged_in": False, "youtube_access_token": None})
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
    user_states.pop(user_id, None) # Clear state immediately after receiving input

    try:
        target_user_id = int(target_user_id_str)
        update_user_data(target_user_id, {"is_premium": True, "role": "user", "$addToSet": {"premium_platforms": "facebook", "premium_platforms": "youtube"}})
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
    user_states.pop(user_id, None) # Clear state immediately after receiving input

    try:
        target_user_id = int(target_user_id_str)
        if target_user_id == OWNER_ID:
            await message.reply("❌ **Security Alert!** Cannot revoke owner's premium status.", reply_markup=Admin_markup)
            return

        user_doc = get_user_data(target_user_id)

        if user_doc and user_doc.get("is_premium"):
            update_user_data(target_user_id, {"is_premium": False, "premium_platforms": [], "facebook_access_token": None, "facebook_selected_page_id": None, "facebook_selected_page_name": None, "youtube_logged_in": False, "youtube_access_token": None}) # Revoke premium and clear platform connections
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
            if target_user_id == user_id: # Don't send broadcast to the admin who initiated it
                continue
            await client.send_message(target_user_id, f"📢 **ADMIN BROADCAST MESSAGE:**\n\n{text_to_broadcast}")
            success_count += 1
            time.sleep(0.05) # Small delay to avoid hitting Telegram API limits too quickly
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

    total_fb_accounts = users_collection.count_documents({"facebook_access_token": {"$ne": None}})
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
    await callback_query.edit_message_text(stats_message, reply_markup=get_general_settings_inline_keyboard(user_id), parse_mode=enums.ParseMode.MARKDOWN)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) viewed detailed system status.")

# --- Platform Specific Settings Menus ---
@app.on_callback_query(filters.regex("^settings_facebook$"))
async def show_facebook_settings(client, callback_query):
    """Displays Facebook settings menu."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing Facebook configurations...")
    await callback_query.edit_message_text("📘 **Facebook Configuration Module:**", reply_markup=facebook_settings_inline_menu)
    logger.info(f"User {user_id} accessed Facebook settings.")

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

# --- Facebook Settings Handlers ---

# Handler for Facebook Token Login
@app.on_callback_query(filters.regex("^fb_login_token_prompt$"))
async def prompt_facebook_token_login_from_settings(client, callback_query):
    """Prompts for Facebook login access token."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** You need a premium subscription to connect platforms.", show_alert=True)
        return
    await callback_query.answer("Initiating Facebook authentication protocol (Token)...")
    user_states[user_id] = {"step": AWAITING_FB_ACCESS_TOKEN}
    await callback_query.edit_message_text(
        "🔑 **Facebook Page Access Token Input Module:**\n\n"
        "To establish a connection to your Facebook Page, you must transmit your **Page Access Token**.\n"
        "This token enables the system to publish content to your designated Facebook Page.\n\n"
        "❗ **Acquisition Protocol (Page Access Token):**\n"
        "1.  Navigate to Facebook Developers Portal: `https://developers.facebook.com/`\n"
        "2.  Create or Select an existing Application.\n"
        "3.  Acquire a **User Access Token** with `pages_show_list` and `pages_manage_posts` permissions.\n"
        "4.  Utilize this User Access Token to procure a **Long-Lived Page Access Token** for your desired Page.\n\n"
        "Once the token is acquired, transmit it using the following command structure:\n"
        "```\n/fbtoken <your_facebook_page_access_token>\n```\n"
        "\nOr, if you have a User Access Token (not page token), try it. The bot will try to list pages for you to pick.",
        parse_mode=enums.ParseMode.MARKDOWN
    )
    logger.info(f"User {user_id} prompted for Facebook access token (via /fbtoken).")

@app.on_message(filters.command("fbtoken") & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_ACCESS_TOKEN))
async def facebook_token_login_command(client, message):
    """Handles Facebook access token input and saves it, then prompts for page selection."""
    user_id = message.from_user.id
    user_states.pop(user_id, None) # Clear state immediately

    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.reply("❗ **Syntax Error.** Usage: `/fbtoken <your_facebook_page_access_token>`", reply_markup=facebook_settings_inline_menu)
            return

        access_token = args[1].strip()

        # Try to fetch pages with this token to validate and allow selection
        pages = get_facebook_pages(access_token)
        if not pages:
            await message.reply("❌ **Authentication Failed.** Invalid or expired Facebook token, or no pages found. Please ensure your token has `pages_show_list` and `pages_manage_posts` permissions. If it's a User Access Token, ensure it's long-lived.", reply_markup=facebook_settings_inline_menu)
            logger.error(f"Facebook token validation failed for user {user_id}: No pages found or token invalid.")
            return

        # Store the token temporarily in state for page selection
        # The `access_token` here might be a User Access Token or a Page Access Token.
        # When selecting a page, we will use the page's specific access token from `pages` list.
        user_states[user_id] = {"step": AWAITING_FB_PAGE_SELECTION, "fb_temp_user_token": access_token, "fb_pages_data": pages}

        page_buttons = []
        for page in pages:
            # It's crucial to use page['access_token'] here, as this is the page-specific token.
            page_buttons.append([InlineKeyboardButton(page['name'], callback_data=f"select_fb_page_{page['id']}_{page['access_token']}")])
        
        await message.reply(
            "✅ **Facebook Token Validated!** Now, please select the Facebook Page you wish to manage for uploads:",
            reply_markup=InlineKeyboardMarkup(page_buttons)
        )
        await log_to_channel(client, f"User {user_id} token validated. Prompting for Facebook page selection.")

    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during Facebook token login procedure: `{e}`", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Failed to process Facebook token login for user {user_id}: {e}", exc_info=True)

@app.on_callback_query(filters.regex("^select_fb_page_"))
async def select_facebook_page(client, callback_query):
    """Handles selection of a Facebook page for uploads."""
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or state.get("step") != AWAITING_FB_PAGE_SELECTION:
        await callback_query.answer("❗ **Invalid Operation.** Please re-initiate the Facebook login process.", show_alert=True)
        return

    try:
        # The callback_data is 'select_fb_page_<page_id>_<page_access_token>'
        # We need to correctly parse page_id and page_access_token.
        # Splitting by '_' and taking last two parts should work if page_access_token doesn't contain underscores.
        # Better to join the parts after a certain index, as tokens can contain underscores.
        parts = callback_query.data.split('_')
        selected_page_id = parts[3]
        selected_page_token = "_".join(parts[4:]) # Rejoin parts in case token has underscores

        # Find the selected page in the temporarily stored data to get its name
        page_name = "Unknown Page"
        if "fb_pages_data" in state:
            for page in state["fb_pages_data"]:
                if page['id'] == selected_page_id:
                    page_name = page.get('name', 'Unknown Page')
                    break
        else:
            # Fallback if fb_pages_data is not in state (e.g., bot restarted or old state)
            # Try to fetch page name directly using the token
            try:
                page_name_url = f"https://graph.facebook.com/v19.0/{selected_page_id}?access_token={selected_page_token}"
                response = requests.get(page_name_url)
                response.raise_for_status()
                page_data = response.json()
                page_name = page_data.get('name', 'Unknown Page')
            except Exception as e:
                logger.warning(f"Could not fetch page name for {selected_page_id} with token. Error: {e}")


        update_user_data(user_id, {
            "facebook_access_token": selected_page_token, # Store the page-specific token
            "facebook_selected_page_id": selected_page_id,
            "facebook_selected_page_name": page_name,
            "is_premium": True, # Grant premium status if successful login
            "$addToSet": {"premium_platforms": "facebook"}
        })
        user_states.pop(user_id, None) # Clear state

        await callback_query.answer(f"Facebook Page '{page_name}' selected!", show_alert=True)
        await callback_query.message.edit_text(
            f"✅ **Facebook Page Configured!**\n"
            f"You are now ready to upload content to Page: **{page_name}** (`{selected_page_id}`).",
            reply_markup=facebook_settings_inline_menu,
            parse_mode=enums.ParseMode.MARKDOWN
        )
        await log_to_channel(client, f"User `{user_id}` (`{callback_query.from_user.username}`) selected Facebook Page '{page_name}' ({selected_page_id}).")

    except Exception as e:
        await callback_query.answer("❌ **Error selecting page.** Please try again or provide a valid token.", show_alert=True)
        await callback_query.message.edit_text(f"❌ **Operation Failed.** Error selecting Facebook page: `{e}`", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Failed to select Facebook page for user {user_id}: {e}", exc_info=True)
    
# Removed direct login (username/password) prompts and handlers for security and feasibility reasons.
# @app.on_callback_query(filters.regex("^fb_direct_login_prompt$"))
# async def prompt_facebook_direct_login(client, callback_query):
#    # ... (removed for this refined version as it's not a secure or reliable method) ...

@app.on_callback_query(filters.regex("^fb_set_title$"))
async def fb_set_title_prompt(client, callback_query):
    """Prompts user to set Facebook title."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting title input...")
    user_states[user_id] = {"step": AWAITING_FB_TITLE}
    await callback_query.edit_message_text(
        "📝 **Facebook Title Input Module:**\n\nPlease transmit the new Facebook video/post title.\n"
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
        await message.reply(f"✅ **Facebook Title Skipped.** Using default: '{default_title}'", reply_markup=facebook_settings_inline_menu)
        logger.info(f"User {user_id} skipped Facebook title, using default.")
    else:
        update_user_data(user_id, {"facebook_settings.title": title})
        await message.reply(f"✅ **Facebook Title Configured.** New title set to: '{title}'", reply_markup=facebook_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set Facebook title.")
    logger.info(f"User {user_id} saved Facebook title.")

@app.on_callback_query(filters.regex("^fb_set_tag$"))
async def fb_set_tag_prompt(client, callback_query):
    """Prompts user to set Facebook tags."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting tag input...")
    user_states[user_id] = {"step": AWAITING_FB_TAG}
    await callback_query.edit_message_text(
        "🏷️ **Facebook Tag Input Module:**\n\nPlease transmit the new Facebook tags (e.g., `#reels #video #photo`). Separate with spaces.\n"
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
    """Prompts user to set Facebook description."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting description input...")
    user_states[user_id] = {"step": AWAITING_FB_DESCRIPTION}
    await callback_query.edit_message_text(
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
        await message.reply(f"✅ **Facebook Description Skipped.** Using default: '{default_description}'", reply_markup=facebook_settings_inline_menu)
        logger.info(f"User {user_id} skipped Facebook description, using default.")
    else:
        update_user_data(user_id, {"facebook_settings.description": description})
        await message.reply(f"✅ **Facebook Description Configured.** New description set to: '{description}'", reply_markup=facebook_settings_inline_menu)
        await log_to_channel(client, f"User `{user_id}` set Facebook description.")
    logger.info(f"User {user_id} saved Facebook description.")


@app.on_callback_query(filters.regex("^fb_default_upload_type$"))
async def fb_default_upload_type_selection(client, callback_query):
    """Displays options for Facebook default upload type (Reels, Video, Photo)."""
    await callback_query.answer("Awaiting default upload type selection...")
    await callback_query.edit_message_text("🎥 **Facebook Default Upload Type Selector:**\n\nSelect the default content type for your Facebook uploads:", reply_markup=facebook_upload_type_inline_menu)
    logger.info(f"User {callback_query.from_user.id} accessed Facebook default upload type selection.")

@app.on_callback_query(filters.regex("^fb_upload_type_"))
async def fb_set_default_upload_type(client, callback_query):
    """Sets the Facebook default upload type."""
    user_id = callback_query.from_user.id
    upload_type = callback_query.data.split("_")[-1].capitalize() # 'reels', 'video', 'photo'
    
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
    """Prompts user to set Facebook schedule time."""
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

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_SCHEDULE_TIME))
async def fb_set_schedule_time_save(client, message):
    """Saves the provided Facebook schedule time or clears it."""
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
        # Ensure schedule time is in UTC for consistency
        # Assuming current machine time is UTC or handling conversion if local.
        # For simplicity, we'll treat the input as UTC directly.
        
        # Facebook API requires scheduled time to be at least 10 minutes in the future from publish.
        # Let's set a buffer, e.g., 5 minutes from now.
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
    """Displays options for Facebook privacy setting."""
    await callback_query.answer("Awaiting privacy selection...")
    # Modified to include 'Draft' option explicitly for Facebook
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Public", callback_data='fb_privacy_public')],
            [InlineKeyboardButton("Private (Draft)", callback_data='fb_privacy_draft')],
            [InlineKeyboardButton("⬅️ Back", callback_data='settings_facebook')]
        ]
    )
    await callback_query.edit_message_text("🔒 **Facebook Privacy Configuration Module:**\n\nSelect Facebook privacy setting:", reply_markup=keyboard)
    logger.info(f"User {callback_query.from_user.id} accessed Facebook privacy selection.")

@app.on_callback_query(filters.regex("^fb_privacy_"))
async def fb_set_privacy(client, callback_query):
    """Sets the Facebook privacy setting."""
    user_id = callback_query.from_user.id
    # 'public', 'private', 'draft'
    privacy = callback_query.data.split("_")[-1].capitalize() 
    
    # Facebook API mostly uses 'PUBLISHED' or 'DRAFT' concepts.
    # 'Private' for a post generally means DRAFT or UNPUBLISHED.
    # So map 'private' from UI to 'Draft' for API.
    if privacy.lower() == 'private':
        api_privacy_setting = 'Draft'
        display_privacy = 'Private (Draft)'
    else:
        api_privacy_setting = privacy
        display_privacy = privacy

    update_user_data(user_id, {"facebook_settings.privacy": api_privacy_setting})
    await callback_query.answer(f"Facebook privacy set to: {display_privacy}", show_alert=True)
    await callback_query.edit_message_text(
        f"✅ **Facebook Privacy Configured.** Set to: {display_privacy}",
        reply_markup=facebook_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set Facebook privacy to `{api_privacy_setting}`.")
    logger.info(f"User {user_id} set Facebook privacy to {api_privacy_setting}.")

@app.on_callback_query(filters.regex("^fb_check_token_info$"))
async def fb_check_token_info(client, callback_query):
    """Displays current Facebook token and selected page information."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Retrieving Facebook token and page info...")
    user_doc = get_user_data(user_id)
    fb_access_token = user_doc.get("facebook_access_token")
    fb_selected_page_id = user_doc.get("facebook_selected_page_id")
    fb_selected_page_name = user_doc.get("facebook_selected_page_name")

    # For security, do not display the full token in chat. Just indicate presence.
    token_status = "✅ Active (Stored)" if fb_access_token else "❌ Not Stored"
    page_status = f"✅ Selected: **{fb_selected_page_name}** (`{fb_selected_page_id}`)" if fb_selected_page_id else "❌ No Page Selected"

    info_text = (
        f"**📘 Facebook Account Diagnostics:**\n"
        f"Page Access Token Status: `{token_status}`\n"
        f"Selected Page Info: {page_status}\n\n"
        f"_**System Note:** For security, the full token is not displayed here. Facebook Page Access Tokens are generally long-lived, but dependent on the originating User Access Token's validity and permissions._"
    )
    await callback_query.edit_message_text(info_text, reply_markup=facebook_settings_inline_menu, parse_mode=enums.ParseMode.MARKDOWN)
    logger.info(f"User {user_id} checked Facebook token info.")

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
    await callback_query.edit_message_text(
        "🔑 **YouTube Access Token Input Module:**\n\n"
        "To establish a connection to YouTube, please transmit your **YouTube Access Token**.\n\n"
        "**Format:** `/youtubelogin <your_youtube_access_token>`\n\n"
        "_**System Note:** This is a placeholder for a real YouTube OAuth 2.0 flow. Obtaining YouTube access tokens securely usually involves a multi-step web authentication process and a separate API client setup. This bot does not currently implement a full YouTube API upload, but stores the token for future integration._",
        parse_mode=enums.ParseMode.MARKDOWN
    )
    logger.info(f"User {user_id} prompted for YouTube access token.")

@app.on_message(filters.command("youtubelogin") & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_YT_ACCESS_TOKEN))
async def youtube_login_command(client, message):
    """Handles YouTube access token input and saves it (simulated)."""
    user_id = message.from_user.id
    user_states.pop(user_id, None) # Clear state immediately

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
                "is_premium": True # Grant premium status if successful login (even simulated)
            })
            await message.reply("✅ **YouTube Login Simulated Successfully!** Token recorded. Connection established (for future use).", reply_markup=youtube_settings_inline_menu)
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
    await callback_query.edit_message_text(
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
    await callback_query.edit_message_text(
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
    await callback_query.edit_message_text(
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
    await callback_query.edit_message_text("🎥 **YouTube Video Type Selector:**\n\nSelect YouTube content type:", reply_markup=youtube_video_type_inline_menu)
    logger.info(f"User {callback_query.from_user.id} accessed YouTube video type selection.")

@app.on_callback_query(filters.regex("^yt_video_type_"))
async def yt_set_video_type(client, callback_query):
    """Sets the YouTube video type."""
    user_id = callback_query.from_user.id
    video_type = "Shorts (Short Vertical Video)" if 'shorts' in callback_query.data else "Video (Standard Horizontal/Square)"
    update_user_data(user_id, {"youtube_settings.video_type": video_type})
    await callback_query.answer(f"YouTube video type set to: {video_type}", show_alert=True)
    await callback_query.edit_message_text(
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
    await callback_query.edit_message_text(
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
    await callback_query.edit_message_text("🔒 **YouTube Privacy Configuration Module:**\n\nSelect YouTube privacy setting:", reply_markup=get_privacy_inline_menu('yt'))
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
    await callback_query.edit_message_text(
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
    # For a real YouTube integration, you'd store and display the actual token expiry and refresh status.
    # This is a placeholder as actual OAuth token refreshing is not implemented.
    expiry_date_info = "Not Tracked (Requires full OAuth implementation)"
    if user_doc.get("youtube_logged_in"):
        expiry_date_info = "Logged in (Token status based on placeholder. Real expiry not monitored)"

    await callback_query.edit_message_text(f"🗓️ **YouTube Token Expiry Status:** `{expiry_date_info}`\n\n_**System Note:** YouTube OAuth 2.0 access tokens are typically short-lived and require a refresh token mechanism for persistent access. This is not fully implemented here._", reply_markup=youtube_settings_inline_menu, parse_mode=enums.ParseMode.MARKDOWN)
    logger.info(f"User {user_id} checked YouTube expiry date (simulated).")


# --- Generic Upload Flow Handler ---
@app.on_message(filters.text & filters.regex("^⬆️ Upload Content$"))
async def prompt_platform_for_upload(client, message):
    """Prompts the user to select a platform for upload."""
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
    """Handles selection of upload platform (Facebook or YouTube)."""
    user_id = callback_query.from_user.id
    platform = callback_query.data.split("_")[-1] # 'facebook' or 'youtube'

    await callback_query.answer(f"Selected {platform.capitalize()} for upload.", show_alert=True)
    
    user_doc = get_user_data(user_id)
    
    if platform == "facebook":
        fb_access_token, fb_selected_page_id = get_facebook_tokens_for_user(user_id)
        if not fb_access_token or not fb_selected_page_id:
            await callback_query.message.edit_text("❌ **Authentication Required.** You are not logged into Facebook or haven't selected a page. Please navigate to `⚙️ Settings` -> `📘 Facebook Settings` to configure your account first.")
            return

        # Use the default upload type from settings, but still offer selection in case user wants to override
        user_fb_settings = user_doc.get("facebook_settings", {})
        default_fb_upload_type = user_fb_settings.get("upload_type", "Video").lower()

        user_states[user_id] = {"step": AWAITING_UPLOAD_FILE, "platform": "facebook", "upload_type": default_fb_upload_type}
        
        # We can ask for file directly, or still offer the type selection.
        # Original code offered type selection via `AWAITING_UPLOAD_TYPE_SELECTION`.
        # Let's keep that flow for explicit choice.
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
        # Placeholder for YouTube authentication check
        if not user_doc.get("youtube_logged_in"):
            await callback_query.message.edit_text("❌ **Authentication Required.** You are not logged into YouTube. Please navigate to `⚙️ Settings` -> `▶️ YouTube Settings` to configure your account first.")
            return

        user_states[user_id] = {"step": AWAITING_UPLOAD_FILE, "platform": "youtube"} # YouTube handles video/shorts based on later settings
        await callback_query.message.edit_text(
            "🎥 **Content Transmission Protocol Active.** Please transmit your video file for YouTube now.",
            reply_markup=None
        )
        await client.send_message(user_id, "You can use '🔙 Main Menu' to abort the transmission.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        logger.info(f"User {user_id} selected YouTube for upload, awaiting file.")

@app.on_callback_query(filters.regex("^upload_type_fb_"))
async def handle_facebook_upload_type_selection(client, callback_query):
    """Handles selection of specific Facebook upload type (Video, Reel, Photo)."""
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or state.get("step") != AWAITING_UPLOAD_TYPE_SELECTION:
        await callback_query.answer("❗ **Invalid Operation.** Please restart the upload process.", show_alert=True)
        return

    upload_type = callback_query.data.split("_")[-1] # 'video', 'reels', 'photo'
    state["upload_type"] = upload_type
    user_states[user_id]["step"] = AWAITING_UPLOAD_FILE # Next step is file input

    await callback_query.answer(f"Selected Facebook {upload_type.capitalize()} upload.", show_alert=True)
    await callback_query.message.edit_text(
        f"🎥 **Content Transmission Protocol Active.** Please transmit your {'video' if upload_type != 'photo' else 'image'} file for Facebook now.",
        reply_markup=None
    )
    await client.send_message(user_id, "You can use '🔙 Main Menu' to abort the transmission.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
    logger.info(f"User {user_id} selected Facebook upload type '{upload_type}', awaiting file.")


@app.on_message(filters.video | filters.photo | filters.document) # Added filters.document for broader media support
async def handle_media_upload(client, message):
    """Handles incoming video or photo files for upload."""
    user_id = message.chat.id
    if not get_user_data(user_id):
        await message.reply("⛔ **Access Denied!** Please send `/start` first to initialize your account in the system.")
        return

    state = user_states.get(user_id)
    if not state or (state.get("step") != AWAITING_UPLOAD_FILE):
        if message.video or message.photo or (message.document and message.document.mime_type and (message.document.mime_type.startswith('video/') or message.document.mime_type.startswith('image/'))):
            await message.reply("❗ **Invalid Operation.** Please initiate a content upload process by clicking '⬆️ Upload Content' first.")
        else:
            await message.reply("❗ **Unsupported Media Type.** Please transmit a video or image file to proceed.")
        logger.warning(f"User {user_id} sent media without active upload state or unsupported media type.")
        return

    if not is_premium_user(user_id) and not is_admin(user_id):
        await message.reply("❌ **Access Restricted.** You need **PREMIUM ACCESS** to upload content. Please contact the administrator.")
        user_states.pop(user_id, None)
        logger.warning(f"Non-premium user {user_id} attempted media upload.")
        return

    # Determine file type and extension for download
    file_info = None
    if message.video:
        file_info = message.video
        file_extension = os.path.splitext(file_info.file_name or "video.mp4")[1]
    elif message.photo:
        # Pyrogram's message.photo selects the highest quality photo by default
        file_info = message.photo.file_id # Use file_id for photo download, filename irrelevant for photos in this context
        file_extension = ".jpg" # Photos typically downloaded as JPG by Pyrogram
    elif message.document and message.document.mime_type and (message.document.mime_type.startswith('video/') or message.document.mime_type.startswith('image/')):
        file_info = message.document
        file_extension = os.path.splitext(file_info.file_name or "document")[1]
    else:
        await message.reply("❗ **Unsupported Media Type.** Please transmit a video or image file to proceed.")
        logger.warning(f"User {user_id} sent an unhandled document type.")
        user_states.pop(user_id, None)
        return

    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Created 'downloads' directory for media processing.")

    initial_status_msg = await message.reply("⏳ **Data Acquisition In Progress...** Downloading your content. This operation may require significant processing time for large data files.")
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        download_filename = f"downloads/{user_id}_{timestamp}{file_extension}"
        
        file_path = await client.download_media(file_info, file_name=download_filename)
        
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
    """Handles input for content title."""
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
    """Handles input for content description."""
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

    # Adjust privacy options based on platform
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
                [InlineKeyboardButton("Private (Draft)", callback_data="visibility_draft")]
            ]
        )
    await message.reply("🌐 **Visibility Configuration Module.** Select content visibility:", reply_markup=keyboard)
    logger.info(f"User {user_id} awaiting visibility choice for {platform}.")


@app.on_callback_query(filters.regex("^visibility_"))
async def handle_visibility_selection(client, callback_query):
    """Handles selection for content visibility."""
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or state.get("step") != AWAITING_UPLOAD_VISIBILITY:
        await callback_query.answer("❗ **Invalid Operation.** Please ensure you are in an active upload sequence.", show_alert=True)
        return

    platform = state["platform"]
    visibility_choice = callback_query.data.split("_")[1]

    # Map 'draft' to 'private' for consistent API handling if needed, but keep 'draft' for Facebook.
    if visibility_choice.lower() == 'draft' and platform != 'facebook':
        # This case handles if a "draft" option was accidentally presented for YT
        state["visibility"] = "private"
    else:
        state["visibility"] = visibility_choice
    
    user_states[user_id]["step"] = AWAITING_UPLOAD_SCHEDULE

    await callback_query.answer(f"Visibility set to: {visibility_choice.replace('_', ' ').capitalize()}", show_alert=True)
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
    """Handles selection for content scheduling (now or later)."""
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
        # Note: 'processing_and_uploading' is not an actual state to be set.
        # The `initiate_upload` call should handle the process end.
        await message.reply("⏳ **Data Processing Initiated...** Preparing your content for scheduled transmission. Please standby.")
        await initiate_upload(client, message, user_id)
        logger.info(f"User {user_id} provided schedule datetime for {state['platform']}: {schedule_str}.")

    except ValueError:
        await message.reply("❌ **Input Error.** Invalid date/time format. Please use `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`).")
        logger.warning(f"User {user_id} provided invalid schedule datetime format: {schedule_str}")
    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** An error occurred while processing schedule time: `{e}`")
        logger.error(f"Error processing schedule time for user {user_id}: {e}", exc_info=True)


async def initiate_upload(client, message_obj, user_id): # Renamed message to message_obj to avoid confusion with Pyrogram's message object.
    """Initiates the actual content upload to the chosen platform."""
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
    upload_type = state.get("upload_type", "video") # Default to video for Facebook, or just generic for YouTube

    if not all([file_path, title, description]):
        await client.send_message(user_id, "❌ **Upload Protocol Failure.** Missing essential content metadata (file, title, or description). Please restart the upload sequence.")
        logger.error(f"Missing essential upload data for user {user_id}. State: {state}")
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        user_states.pop(user_id, None)
        return

    # Set chat action and log
    await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO) # Keep generic for now
    await log_to_channel(client, f"User `{user_id}` (`{message_obj.from_user.username}`) initiating upload for {platform}. Type: `{upload_type}`. File: `{os.path.basename(file_path)}`. Visibility: `{visibility}`. Schedule: `{schedule_time}`.")

    processed_file_path = file_path # Assume original is fine until conversion needed

    try:
        # --- Media Conversion / Pre-processing ---
        if platform == "facebook":
            # Determine target format based on selected upload_type
            if upload_type == "photo":
                target_format = "jpg"
            elif upload_type in ["video", "reels"]:
                target_format = "mp4"
            else:
                raise ValueError(f"Unsupported Facebook upload type: {upload_type}")

            await client.send_message(user_id, f"🔄 **Data Pre-processing Protocol.** Analyzing content for Facebook {upload_type} compatibility. Please standby...")
            await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO if upload_type != 'photo' else enums.ChatAction.UPLOAD_PHOTO)
                
            def do_processing_sync():
                return convert_media_for_facebook(file_path, upload_type, target_format)
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(do_processing_sync)
                processed_file_path = future.result(timeout=900) # Timeout for conversion
            
            await client.send_message(user_id, "✅ **Content Data Conversion/Verification Complete.**")
            await log_to_channel(client, f"User `{user_id}` content processed. Original: `{os.path.basename(file_path)}`, Processed: `{os.path.basename(processed_file_path)}`.")
        
        elif platform == "youtube":
            # YouTube typically accepts various video formats, but MP4 (H264/AAC) is universal.
            # Convert to MP4 only if necessary, preferring copy.
            input_ext = os.path.splitext(file_path)[1].lower()
            if input_ext not in [".mp4", ".mov", ".avi", ".webm", ".mkv"]: # Common video formats
                await client.send_message(user_id, f"🔄 **Data Pre-processing Protocol.** Converting video to MP4 format for YouTube. Please standby...")
                await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO)
                
                def do_processing_sync():
                    return convert_video_to_mp4(file_path, f"downloads/processed_{os.path.basename(file_path)}.mp4")
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(do_processing_sync)
                    processed_file_path = future.result(timeout=600) # Timeout for conversion
                
                await client.send_message(user_id, "✅ **Video Data Conversion Complete.**")
                await log_to_channel(client, f"User `{user_id}` video converted for YouTube. Original: `{os.path.basename(file_path)}`, Processed: `{os.path.basename(processed_file_path)}`.")
            else:
                await client.send_message(user_id, "✅ **Video Format Verified.** Proceeding with direct transmission.")
                logger.info(f"User {user_id} video already suitable format for YouTube. Skipping re-encoding.")

        # --- Upload to Platform ---
        if platform == "facebook":
            fb_access_token, fb_selected_page_id = get_facebook_tokens_for_user(user_id)
            if not fb_access_token or not fb_selected_page_id:
                await client.send_message(user_id, "❌ **Authentication Required.** Facebook access token or selected page not found. Please re-authenticate via `⚙️ Settings` -> `📘 Facebook Settings`.")
                return

            await client.send_message(user_id, f"📤 **Initiating Facebook {upload_type.capitalize()} Transmission...**")
            await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO if upload_type != 'photo' else enums.ChatAction.UPLOAD_PHOTO)

            def upload_to_facebook_sync():
                # Pass the actual visibility string ('public', 'draft') as Facebook API handles this.
                # 'private' from UI is mapped to 'draft' for FB.
                actual_visibility = visibility
                if platform == "facebook" and visibility.lower() == "private":
                    actual_visibility = "draft"
                
                return upload_facebook_content(processed_file_path, upload_type.lower(), title, description, fb_access_token, fb_selected_page_id, visibility=actual_visibility, schedule_time=schedule_time)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(upload_to_facebook_sync)
                fb_result = future.result(timeout=1200) # Increased timeout for FB upload

            if fb_result and ('id' in fb_result or 'post_id' in fb_result): # Reels might return 'id', videos 'post_id'
                # Check for post_id or id in result for success confirmation
                post_id = fb_result.get('id') or fb_result.get('post_id')
                status_text = "Scheduled" if schedule_time else ("Draft" if visibility.lower() == 'draft' or visibility.lower() == 'private' else "Published")
                await client.send_message(user_id, f"✅ **Facebook Content Transmitted!** {upload_type.capitalize()} ID: `{post_id}`. Status: `{status_text}`.")
                users_collection.update_one({"_id": user_id}, {"$inc": {"total_uploads": 1}})
                await log_to_channel(client, f"User `{user_id}` successfully uploaded {upload_type} to Facebook. ID: `{post_id}`. Status: `{status_text}`. File: `{os.path.basename(processed_file_path)}`.")
            else:
                await client.send_message(user_id, f"❌ **Facebook Transmission Failed.** Response: `{json.dumps(fb_result, indent=2)}`")
                logger.error(f"Facebook upload failed for user {user_id}. Result: {fb_result}")

        elif platform == "youtube":
            # YouTube Upload Logic (placeholder - requires Google API client setup)
            await client.send_message(user_id, "🚧 **YouTube Upload Feature Under Development.**\n\n_**System Note:** The YouTube upload functionality requires advanced Google API integration (OAuth 2.0, YouTube Data API V3) which is currently not fully implemented in this system. Your video was processed, but not uploaded._")
            await log_to_channel(client, f"User `{user_id}` attempted YouTube upload (currently simulated). File: `{os.path.basename(processed_file_path)}`.")
            users_collection.update_one({"_id": user_id}, {"$inc": {"total_uploads": 1}}) # Still increment for attempt
            # In a real scenario, you'd use google-api-python-client here.

    except concurrent.futures.TimeoutError:
        await client.send_message(user_id, "❌ **Operation Timed Out.** Content processing or transmission exceeded time limits. The data file might be too large or the network connection is unstable. Please retry with a smaller file or a more robust connection.")
        logger.error(f"Upload/processing timeout for user {user_id}. Original file: {file_path}")
    except RuntimeError as re:
        await client.send_message(user_id, f"❌ **Processing Error:** `{re}`\n\n_**System Note:** Ensure FFmpeg is correctly installed and accessible in your system's PATH, and verify your media data file is not corrupted._")
        logger.error(f"Processing/Upload Error for user {user_id}: {re}", exc_info=True)
    except requests.exceptions.RequestException as req_e:
        error_msg = f"❌ **Network/API Error during Transmission:** `{req_e}`"
        if req_e.response is not None:
            try:
                error_details = req_e.response.json()
                error_msg += f"\nAPI Details: `{json.dumps(error_details, indent=2)}`"
            except json.JSONDecodeError:
                error_msg += f"\nAPI Response: `{req_e.response.text[:200]}`"
        error_msg += "\n\n_**System Note:** Please verify your internet connection or inspect the configured API parameters for Facebook._"
        await client.send_message(user_id, error_msg)
        logger.error(f"Network/API Error for user {user_id}: {req_e}", exc_info=True)
    except Exception as e:
        await client.send_message(user_id, f"❌ **Critical Transmission Failure.** An unexpected system error occurred: `{e}`")
        logger.error(f"Upload failed for user {user_id}: {e}", exc_info=True)
    finally:
        # Clean up files
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Cleaned up original file: {file_path}")
            except OSError as e:
                logger.error(f"Error deleting original file {file_path}: {e}")
        if processed_file_path and processed_file_path != file_path and os.path.exists(processed_file_path):
            try:
                os.remove(processed_file_path)
                logger.info(f"Cleaned up processed file: {processed_file_path}")
            except OSError as e:
                logger.error(f"Error deleting processed file {processed_file_path}: {e}")
        
        user_states.pop(user_id, None) # Always clear state at the end of upload attempt
        await client.send_chat_action(user_id, enums.ChatAction.CANCEL)


# === KEEP ALIVE SERVER ===
class Handler(BaseHTTPRequestHandler):
    """Simple HTTP server to keep the bot alive on platforms like Render/Railway."""
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_server():
    """Runs a simple HTTP server in a separate thread."""
    httpd = HTTPServer(('0.0.0.0', 8080), Handler)
    logger.info("Keep-alive HTTP server started on port 8080.")
    httpd.serve_forever()

# Start the keep-alive server in a separate thread, ensuring the bot doesn't block.
threading.Thread(target=run_server, daemon=True).start()

# === START BOT ===
if __name__ == "__main__":
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Created 'downloads' directory.")

    logger.info("Bot system initiating...")
    app.run()

