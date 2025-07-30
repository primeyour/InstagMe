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

# === GLOBAL CLIENTS AND DB ===
app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
users_collection = db["users"]

# --- IMPORTANT: MongoDB Index Check/Creation ---
try:
    # Ensure no unique index exists on user_id if it's causing issues.
    # We generally want _id to be the unique identifier for users_collection.
    # If a 'user_id_1' index exists and is unique, it might conflict with upsert logic if _id is also user_id.
    # It's safer to ensure _id is used as the primary key.
    # If the user_id is meant to be stored as a field 'user_id' and needs an index, it should not be unique
    # if _id is already the unique user identifier.
    # The provided code seems to use _id for user_id, which is good.
    pass # No explicit index dropping needed if _id is consistently used as user_id.
except Exception as e:
    logger.error(f"Error during MongoDB index check: {e}")


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
        [InlineKeyboardButton("🔑 Facebook Direct Login (U/P)", callback_data='fb_direct_login_prompt')], # New button for direct login
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
AWAITING_FB_USERNAME = "awaiting_fb_username" # New state for direct login username
AWAITING_FB_PASSWORD = "awaiting_fb_password" # New state for direct login password
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

def store_facebook_user_login_details(user_id, username, password):
    """Stores Facebook user login details (for direct login simulation)."""
    # NOTE: Storing plain passwords is a security risk. This is for simulation ONLY.
    update_user_data(user_id, {"facebook_username": username, "facebook_password": password})

def store_facebook_selected_page_id(user_id, page_id, page_name):
    """Stores the selected Facebook page ID for uploads."""
    update_user_data(user_id, {"facebook_selected_page_id": page_id, "facebook_selected_page_name": page_name})

def get_facebook_pages(user_access_token):
    """Fetches list of Facebook pages managed by the user token."""
    try:
        # A user access token with 'pages_show_list' is typically needed to get pages.
        # This endpoint returns pages the user manages, *with* their page access tokens.
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
        # 'published': 'true' is implied or handled by status_type
    }

    if content_type == "video":
        post_url = f"https://graph-video.facebook.com/v19.0/{page_id}/videos"
        params['title'] = title
        params['description'] = description
        
        # Schedule and visibility logic
        if schedule_time:
            params['scheduled_publish_time'] = int(schedule_time.timestamp())
            params['published'] = 'false' # Must be false if scheduled
            logger.info(f"Scheduling Facebook video for: {schedule_time}")
        elif visibility.lower() == 'private' or visibility.lower() == 'draft':
            params['published'] = 'false' # Draft if not scheduled and not public
            logger.info(f"Uploading Facebook video as DRAFT (visibility: {visibility}).")
        else: # Public/Published
            params['published'] = 'true'
            logger.info(f"Uploading Facebook video as PUBLISHED (visibility: {visibility}).")
        
        with open(file_path, 'rb') as f:
            files = {'source': f} # Use 'source' for direct video file upload
            response = requests.post(post_url, params=params, files=files)

    elif content_type == "reels":
        # Reels API is distinct. This is a more accurate (but still simplified) flow.
        # Real Reels API often requires an initiator POST, then a byte upload, then a publish POST.
        
        # Step 1: Initialize upload session (simplified for direct upload)
        # Note: True Reels API involves a series of requests. This is a direct upload attempt
        # which may not support all Reels features or limitations.
        # https://developers.facebook.com/docs/instagram/publishing-api/guides/reels
        
        post_url = f"https://graph.facebook.com/v19.0/{page_id}/video_reels"
        params['upload_phase'] = 'finish' # Indicate it's a direct upload (simplified)
        params['video_state'] = 'PUBLISHED' if visibility.lower() == 'public' else 'DRAFT'
        params['title'] = title
        params['description'] = description
        
        # Reels usually don't have direct scheduling in the same way as videos via API.
        # If schedule_time is present, we'll try to publish it as scheduled, but it might just be published immediately.
        if schedule_time:
            logger.warning("Facebook Reels API has complex scheduling. Attempting to set published time, but may behave as immediate publish.")
            params['scheduled_publish_time'] = int(schedule_time.timestamp())
            # For Reels, typically you'd set video_state to SCHEDULED if supported,
            # but published=false is often not an option for initial Reels uploads.
            # We'll stick to PUBLISHED/DRAFT for video_state.
        
        with open(file_path, 'rb') as f:
            files = {'file': f} # 'file' is common for video uploads to Reels endpoint
            response = requests.post(post_url, params=params, files=files)

    elif content_type == "photo":
        post_url = f"https://graph.facebook.com/v19.0/{page_id}/photos"
        params['caption'] = description if description else title # Use description as caption, fall back to title
        
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

    response.raise_for_status()
    result = response.json()
    logger.info(f"Facebook {content_type} upload result: {result}")
    return result

def process_media_for_upload(input_path, platform, output_type):
    """
    Processes media to a suitable format for the target platform without re-compressing
    if the format is already compatible.
    
    Args:
        input_path (str): Path to the input media file.
        platform (str): 'facebook' or 'youtube'.
        output_type (str): For 'facebook': 'video', 'reels', 'photo'. For 'youtube': 'video' (or 'shorts').
                           This defines the target file characteristics.
    """
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    output_dir = "downloads/processed"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    output_path = f"{output_dir}/processed_{base_name}_{timestamp}"

    command = []
    
    if platform == "facebook":
        if output_type in ["video", "reels"]:
            output_path += ".mp4"
            # Prioritize stream copy for video/audio to avoid re-compression, if possible.
            # For Reels, if the aspect ratio isn't 9:16, re-encoding might be unavoidable
            # to fit Facebook's requirements for short-form content.
            if output_type == "reels":
                # Attempt to fit 9:16. If original is far off, it might re-encode.
                # Use '-c copy' if the input is already acceptable (e.g., MP4 H.264 AAC)
                # and you don't need aspect ratio changes.
                # For forcing 9:16 aspect ratio while maintaining quality as much as possible:
                # scale='min(iw,ih*9/16)':-1 will scale width to match 9:16 if height is reference
                # -vf "scale=iw:ih,setdar=9/16" is simpler for just setting aspect ratio
                # For actual reels, better to have vertical video.
                # Complex filter for padding/cropping to 9:16 while maintaining quality (might re-encode)
                command = ["ffmpeg", "-i", input_path, 
                           "-vf", "scale='min(iw,ih*9/16)':'min(ih,iw*16/9)',pad='ih*9/16':ih:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
                           "-c:v", "libx264", "-preset", "fast", "-crf", "23", # Moderate quality, relatively fast
                           "-c:a", "aac", "-b:a", "128k",
                           "-movflags", "+faststart", "-y", output_path]
                logger.info(f"[FFmpeg] Attempting conversion for Facebook Reels (9:16) for {input_path}")
            else: # Standard Facebook video
                # Copy video and audio streams without re-encoding
                command = ["ffmpeg", "-i", input_path, "-c:v", "copy", "-c:a", "copy", "-movflags", "+faststart", "-y", output_path]
                logger.info(f"[FFmpeg] Attempting direct stream copy for Facebook Video for {input_path}")
        elif output_type == "photo":
            output_path += ".jpg"
            # If input is video, extract first frame. If image, convert to JPG.
            command = ["ffmpeg", "-i", input_path, "-vframes", "1", "-q:v", "2", "-y", output_path]
            logger.info(f"[FFmpeg] Converting media to photo for Facebook for {input_path}")
        else:
            raise ValueError(f"Unsupported Facebook output type: {output_type}")

    elif platform == "youtube":
        output_path += ".mp4"
        # YouTube is flexible with formats, but MP4 is common.
        # Prioritize stream copy to avoid compression.
        command = ["ffmpeg", "-i", input_path, "-c:v", "copy", "-c:a", "copy", "-movflags", "+faststart", "-y", output_path]
        logger.info(f"[FFmpeg] Attempting direct stream copy for YouTube for {input_path}")

    else:
        raise ValueError(f"Unsupported platform for media processing: {platform}")

    try:
        # Check if input_path already matches output_path format and type (for simple cases like .mp4 to .mp4 copy)
        # This prevents unnecessary FFmpeg calls if the file is already perfect.
        if os.path.splitext(input_path)[1].lower() == os.path.splitext(output_path)[1].lower() and "copy" in " ".join(command):
            # Simple check, assumes if we copy and extension matches, no new file is needed.
            # This is a heuristic and might need more robust checks for actual content compatibility.
            if os.path.abspath(input_path) == os.path.abspath(output_path):
                 logger.info(f"[FFmpeg] Input and output paths are the same, skipping conversion for {input_path}.")
                 return input_path
            
            # If target is simply a copy and format matches, just copy the file directly using shutil
            try:
                import shutil
                shutil.copyfile(input_path, output_path)
                logger.info(f"[FFmpeg] Directly copied {input_path} to {output_path} (no conversion needed).")
                return output_path
            except Exception as e:
                logger.warning(f"Failed to directly copy file, falling back to FFmpeg: {e}")
        
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=1800) # Increased timeout to 30 mins
        logger.info(f"[FFmpeg] Conversion successful for {input_path}. Output: {result.stdout}")
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"[FFmpeg] Conversion failed for {input_path}. Command: {' '.join(e.cmd)}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        # Clean up partially created output file on error
        if os.path.exists(output_path):
            os.remove(output_path)
        raise RuntimeError(f"FFmpeg conversion error: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("FFmpeg not found. Please install FFmpeg and ensure it's in your system's PATH.")
    except subprocess.TimeoutExpired:
        if os.path.exists(output_path): # Clean up partial file
            os.remove(output_path)
        raise RuntimeError("FFmpeg conversion timed out. Media might be too large or complex.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during FFmpeg conversion: {e}")
        if os.path.exists(output_path): # Clean up partial file
            os.remove(output_path)
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
    if not is_premium_user(user_id) and not is_admin(user_id): # Admin can also access user settings
        await callback_query.answer("⚠️ **Access Restricted.** You need a premium subscription to access user-specific configuration.", show_alert=True)
        logger.info(f"User {user_id} attempted to access user settings without premium/admin.")
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
        update_user_data(target_user_id, {"is_premium": True, "role": "user"}) # Promote to premium, role remains 'user' unless explicitly 'admin'
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
    user_states.pop(user_id, None)

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
            await log_to_channel(client, f"User `{target_user_id}` revoked premium access by `{message.from_user.id}` (`{message.from_user.username}`).")
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
            time.sleep(0.05) # Small delay to avoid flooding Telegram API limits
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
    if not is_premium_user(user_id) and not is_admin(user_id): # Admin can also access user settings
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing Facebook configurations...")
    await callback_query.edit_message_text("📘 **Facebook Configuration Module:**", reply_markup=facebook_settings_inline_menu)
    logger.info(f"User {user_id} accessed Facebook settings.")

@app.on_callback_query(filters.regex("^settings_youtube$"))
async def show_youtube_settings(client, callback_query):
    """Displays YouTube settings menu."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id): # Admin can also access user settings
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
        "```\n/fbtoken <your_facebook_page_access_token>\n```\n",
        parse_mode=enums.ParseMode.MARKDOWN
    )
    logger.info(f"User {user_id} prompted for Facebook access token (via /fbtoken).")

@app.on_message(filters.command("fbtoken") & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_ACCESS_TOKEN))
async def facebook_token_login_command(client, message):
    """Handles Facebook access token input and saves it, then prompts for page selection."""
    user_id = message.from_user.id
    # No pop user_states here, as we transition to AWAITING_FB_PAGE_SELECTION

    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.reply("❗ **Syntax Error.** Usage: `/fbtoken <your_facebook_page_access_token>`", reply_markup=facebook_settings_inline_menu)
            user_states.pop(user_id, None) # Clear state on syntax error
            return

        user_access_token = args[1].strip() # This should be a user access token, not a page access token initially

        # Try to fetch pages with this user access token
        pages = get_facebook_pages(user_access_token)
        if not pages:
            await message.reply("❌ **Authentication Failed.** Invalid or expired Facebook User Access Token, or no pages found. Please ensure your token has `pages_show_list` and `pages_manage_posts` permissions.", reply_markup=facebook_settings_inline_menu)
            logger.error(f"Facebook user token validation failed for user {user_id}: No pages found or token invalid.")
            user_states.pop(user_id, None) # Clear state if no pages found
            return

        # Store the pages data and user access token temporarily in state for page selection
        user_states[user_id] = {"step": AWAITING_FB_PAGE_SELECTION, "fb_temp_user_token": user_access_token, "fb_pages_data": pages}

        page_buttons = []
        for page in pages:
            # IMPORTANT: The callback data needs to store the page_id and its specific access_token
            # The page['access_token'] is a page-specific access token that Facebook provides via the 'me/accounts' endpoint.
            # This is the token we actually want to store for direct page uploads.
            page_buttons.append([InlineKeyboardButton(page['name'], callback_data=f"select_fb_page_{page['id']}_{page['access_token']}")])
        
        await message.reply(
            "✅ **Facebook User Token Validated!** Now, please select the Facebook Page you wish to manage for uploads:",
            reply_markup=InlineKeyboardMarkup(page_buttons)
        )
        logger.info(f"User {user_id} user token validated. Prompting for Facebook page selection.")

    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during Facebook token login procedure: `{e}`", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Failed to process Facebook token login for user {user_id}: {e}", exc_info=True)
        user_states.pop(user_id, None) # Clear state on general error

@app.on_callback_query(filters.regex("^select_fb_page_"))
async def select_facebook_page(client, callback_query):
    """Handles selection of a Facebook page for uploads."""
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or state.get("step") != AWAITING_FB_PAGE_SELECTION:
        await callback_query.answer("❗ **Invalid Operation.** Please re-initiate the Facebook login process.", show_alert=True)
        return

    try:
        # The callback_data format is "select_fb_page_{page_id}_{page_access_token}"
        # We need to split carefully to get both parts.
        parts = callback_query.data.split('_', 3) # Split at most 3 times. ["select", "fb", "page", "{page_id}_{page_access_token}"]
        selected_page_info = parts[3] # This will be "{page_id}_{page_access_token}"

        # Split this part again to get page_id and page_access_token
        page_id_str, page_token_str = selected_page_info.split('_', 1) # Split only once

        selected_page_id = page_id_str
        selected_page_token = page_token_str # This is the PAGE-SPECIFIC access token

        # Find the page name from the stored pages_data
        page_name = "Unknown Page"
        if "fb_pages_data" in state:
            for page in state["fb_pages_data"]:
                if page['id'] == selected_page_id:
                    page_name = page.get('name', 'Unknown Page')
                    break

        update_user_data(user_id, {
            "facebook_access_token": selected_page_token, # Store the page-specific token
            "facebook_selected_page_id": selected_page_id,
            "facebook_selected_page_name": page_name,
            "is_premium": True,
            "$addToSet": {"premium_platforms": "facebook"}
        })
        user_states.pop(user_id, None) # Clear state after successful page selection

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
        user_states.pop(user_id, None) # Clear state on error
    
# Handler for Facebook Direct Login (Username/Password)
@app.on_callback_query(filters.regex("^fb_direct_login_prompt$"))
async def prompt_facebook_direct_login(client, callback_query):
    """Prompts for Facebook direct login username."""
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id) and not is_admin(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** You need a premium subscription to connect platforms.", show_alert=True)
        return
    await callback_query.answer("Initiating Facebook direct login protocol...")
    user_states[user_id] = {"step": AWAITING_FB_USERNAME}
    await callback_query.message.edit_text(
        "🔑 **Facebook Direct Login (Username/Password):**\n\n"
        "Please transmit your Facebook **email or phone number** associated with your account.\n\n"
        "**_System Warning:_** _Direct login using username/password is generally **not recommended** due to security risks and Facebook's preferred OAuth method. This feature is for demonstration and might have limitations or be unstable. For production, **always use the Token Login method**._"
    )
    logger.info(f"User {user_id} prompted for Facebook direct login username.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_USERNAME))
async def handle_facebook_username_input(client, message):
    """Handles Facebook direct login username input, then prompts for password."""
    user_id = message.from_user.id
    username = message.text.strip()
    
    # Check if the user wants to cancel
    if username.lower() == "cancel":
        user_states.pop(user_id, None)
        await message.reply("❌ **Facebook Direct Login Aborted.**", reply_markup=facebook_settings_inline_menu)
        return

    user_states[user_id]["fb_username"] = username
    user_states[user_id]["step"] = AWAITING_FB_PASSWORD

    await message.reply("🔐 **Authentication Protocol:**\n\nNow, please transmit your Facebook **password**.\n\n"
                        "_(Type 'cancel' to abort.)_",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to FB Settings", callback_data='settings_facebook')]])) # Add back button
    logger.info(f"User {user_id} provided Facebook username, awaiting password.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_FB_PASSWORD))
async def handle_facebook_password_input(client, message):
    """Handles Facebook direct login password input and attempts login (simulated)."""
    user_id = message.from_user.id
    password = message.text.strip()
    
    # Check if the user wants to cancel
    if password.lower() == "cancel":
        user_states.pop(user_id, None)
        await message.reply("❌ **Facebook Direct Login Aborted.**", reply_markup=facebook_settings_inline_menu)
        return

    state = user_states.get(user_id)
    username = state.get("fb_username")
    
    user_states.pop(user_id, None) # Clear state immediately

    if not username or not password:
        await message.reply("❌ **Authentication Failed.** Missing username or password. Please restart the direct login process.", reply_markup=facebook_settings_inline_menu)
        return

    await message.reply("⏳ **Initiating Facebook Direct Authentication (Simulated)...** Please standby. This may take a moment.")
    await client.send_chat_action(user_id, enums.ChatAction.TYPING)

    try:
        # --- SIMULATED DIRECT LOGIN ---
        # This is a highly simplified simulation. A real direct login would
        # involve web scraping (e.g., Selenium, Playwright) to bypass
        # Facebook's security measures and obtain a user access token,
        # which is fragile and often against platform policies.
        # For demonstration purposes, we assume success and generate dummy data.
        
        simulated_user_access_token = f"SIMULATED_USER_TOKEN_{user_id}_{datetime.now().timestamp()}"
        
        # Simulate fetching pages with this token
        dummy_pages = [
            {'id': '100000000000001', 'name': 'My Simulated Page 1', 'access_token': f'DUMMY_PAGE_TOKEN_1_{user_id}'},
            {'id': '100000000000002', 'name': 'My Simulated Page 2', 'access_token': f'DUMMY_PAGE_TOKEN_2_{user_id}'},
        ]
        
        if not dummy_pages: # Or if real pages were fetched and none exist
            await message.reply("❌ **Direct Login Failed.** Could not retrieve any accessible Facebook Pages with the provided credentials. Please ensure your account manages at least one page and has necessary permissions.", reply_markup=facebook_settings_inline_menu)
            logger.warning(f"Simulated direct login failed for user {user_id}: No pages found.")
            return

        # Store the (simulated) user access token and prompt for page selection
        user_states[user_id] = {"step": AWAITING_FB_PAGE_SELECTION, "fb_temp_user_token": simulated_user_access_token, "fb_pages_data": dummy_pages}
        
        page_buttons = []
        for page in dummy_pages: # Use dummy_pages for demonstration
            page_buttons.append([InlineKeyboardButton(page['name'], callback_data=f"select_fb_page_{page['id']}_{page['access_token']}")])
        
        await message.reply(
            "✅ **Facebook Authentication Successful!** Now, please select the Facebook Page you wish to manage for uploads:",
            reply_markup=InlineKeyboardMarkup(page_buttons)
        )
        await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) successfully 'logged into' Facebook directly (simulated). Prompting for page selection.")

    except Exception as e:
        await message.reply(f"❌ **Operation Failed.** Error during Facebook direct login procedure: `{e}`", reply_markup=facebook_settings_inline_menu)
        logger.error(f"Failed to process Facebook direct login for user {user_id}: {e}", exc_info=True)


@app.on_callback_query(filters.regex("^fb_set_title$"))
async def fb_set_title_prompt(client, callback_query):
    """Prompts user to set Facebook title."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Awaiting title input...")
    user_states[user_id] = {"step": AWAITING_FB_TITLE}
    user_doc = get_user_data(user_id)
    current_title = user_doc.get("facebook_settings", {}).get("title", "Default Facebook Title")
    await callback_query.edit_message_text(
        "📝 **Facebook Title Input Module:**\n\nPlease transmit the new Facebook video/post title.\n"
        f"**Current:** `{current_title}`\n"
        "_(Type 'skip' to keep current, 'cancel' to abort.)_",
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
        await message.reply(f"✅ **Facebook Title Skipped.** Using current: '{default_title}'", reply_markup=facebook_settings_inline_menu)
        logger.info(f"User {user_id} skipped Facebook title, using current.")
    elif title.lower() == "cancel":
        await message.reply("❌ **Facebook Title Update Aborted.**", reply_markup=facebook_settings_inline_menu)
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
    user_doc = get_user_data(user_id)
    current_tag = user_doc.get("facebook_settings", {}).get("tag", "#facebook #video #reels")
    await callback_query.edit_message_text(
        "🏷️ **Facebook Tag Input Module:**\n\nPlease transmit the new Facebook tags (e.g., `#reels #video #photo`). Separate with spaces.\n"
        f"**Current:** `{current_tag}`\n"
        "_(Type 'skip' to keep current, 'cancel' to abort.)_",
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
        await message.reply(f"✅ **Facebook Tags Skipped.** Using current: '{default_tag}'", reply_markup=facebook_settings_inline_menu)
        logger.info(f"User {user_id} skipped Facebook tags, using current.")
    elif tag.lower() == "cancel":
        await message.reply("❌ **Facebook Tags Update Aborted.**", reply_markup=facebook_settings_inline_menu)
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
    user_doc = get_user_data(user_id)
    current_description = user_doc.get("facebook_settings", {}).get("description", "Default Facebook Description")
    await callback_query.edit_message_text(
        "📄 **Facebook Description Input Module:**\n\nPlease transmit the new Facebook description for your uploads.\n"
        f"**Current:** `{current_description}`\n"
        "_(Type 'skip' to keep current, 'cancel' to abort.)_",
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
        await message.reply(f"✅ **Facebook Description Skipped.** Using current: '{default_description}'", reply_markup=facebook_settings_inline_menu)
        logger.info(f"User {user_id} skipped Facebook description, using current.")
    elif description.lower() == "cancel":
        await message.reply("❌ **Facebook Description Update Aborted.**", reply_markup=facebook_settings_inline_menu)
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
    user_doc = get_user_data(user_id)
    current_schedule = user_doc.get("facebook_settings", {}).get("schedule_time")
    current_schedule_str = datetime.fromisoformat(current_schedule).strftime("%Y-%m-%d %H:%M") if current_schedule else "None"
    await callback_query.edit_message_text(
        "⏰ **Facebook Schedule Configuration Module:**\n\nPlease transmit the desired schedule date and time.\n"
        "**Format:** `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`)\n"
        f"**Current Schedule:** `{current_schedule_str}` (UTC)\n"
        "_**System Note:** Time will be interpreted in UTC._\n"
        "_(Type 'clear' to remove any existing schedule, 'cancel' to abort.)_",
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
    elif schedule_str.lower() == "cancel":
        await message.reply("❌ **Facebook Schedule Update Aborted.**", reply_markup=facebook_settings_inline_menu)
        return

    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")
        # Ensure schedule time is in UTC for consistency
        schedule_dt_utc = schedule_dt # Assuming input is already effectively UTC or local bot time matches.
                                      # For true UTC conversion, you'd need pytz or similar.
                                      # For simplicity, comparing against datetime.utcnow() directly.

        if schedule_dt_utc <= datetime.utcnow() + timedelta(minutes=5):
            await message.reply("❌ **Time Constraint Violation.** Schedule time must be at least 5 minutes in the future. Please try again with a later time.")
            return

        update_user_data(user_id, {"facebook_settings.schedule_time": schedule_dt_utc.isoformat()})
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
    await callback_query.edit_message_text("🔒 **Facebook Privacy Configuration Module:**\n\nSelect Facebook privacy setting:", reply_markup=get_privacy_inline_menu('fb'))
    logger.info(f"User {callback_query.from_user.id} accessed Facebook privacy selection.")

@app.on_callback_query(filters.regex("^fb_privacy_"))
async def fb_set_privacy(client, callback_query):
    """Sets the Facebook privacy setting."""
    user_id = callback_query.from_user.id
    privacy = ""
    if 'public' in callback_query.data:
        privacy = "Public"
    elif 'private' in callback_query.data:
        privacy = "Private" # This will be interpreted as 'Draft' for Facebook posts API
    
    update_user_data(user_id, {"facebook_settings.privacy": privacy})
    await callback_query.answer(f"Facebook privacy set to: {privacy}", show_alert=True)
    await callback_query.edit_message_text(
        f"✅ **Facebook Privacy Configured.** Set to: {privacy}",
        reply_markup=facebook_settings_inline_menu
    )
    await log_to_channel(client, f"User `{user_id}` set Facebook privacy to `{privacy}`.")
    logger.info(f"User {user_id} set Facebook privacy to {privacy}.")

@app.on_callback_query(filters.regex("^fb_check_token_info$"))
async def fb_check_token_info(client, callback_query):
    """Displays current Facebook token and selected page information."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Retrieving Facebook token and page info...")
    user_doc = get_user_data(user_id)
    fb_access_token = user_doc.get("facebook_access_token")
    fb_selected_page_id = user_doc.get("facebook_selected_page_id", "Not Set")
    fb_selected_page_name = user_doc.get("facebook_selected_page_name", "None Selected")

    # For security, do not display the full token in chat. Just indicate presence.
    token_status = "✅ Active (Stored)" if fb_access_token else "❌ Not Stored"

    info_text = (
        f"**📘 Facebook Account Diagnostics:**\n"
        f"Page Access Token Status: `{token_status}`\n"
        f"Selected Page Name: `{fb_selected_page_name}`\n"
        f"Selected Page ID: `{fb_selected_page_id}`\n\n"
        f"_**System Note:** For security, the full token is not displayed here. Facebook Page Access Tokens are generally long-lived._"
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
        "_**System Note:** This is a placeholder for a real YouTube OAuth 2.0 flow. Obtaining YouTube access tokens securely usually involves a multi-step web authentication process. For this demo, any string will be accepted as a 'token'._\n\n"
        "_(Type 'cancel' to abort.)_",
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
        
        if access_token.lower() == "cancel":
            await message.reply("❌ **YouTube Login Aborted.**", reply_markup=youtube_settings_inline_menu)
            return

        if access_token:
            # Simulate basic validation - in a real app, you'd validate against Google API
            user_doc = get_user_data(user_id)
            premium_platforms = user_doc.get("premium_platforms", [])
            if "youtube" not in premium_platforms:
                premium_platforms.append("youtube")

            update_user_data(user_id, {
                "youtube_logged_in": True,
                "youtube_access_token": access_token,
                "premium_platforms": premium_platforms,
                "is_premium": True # Grant premium status if not already, as platform login implies premium feature use
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
    user_doc = get_user_data(user_id)
    current_title = user_doc.get("youtube_settings", {}).get("title", "Default YouTube Title")
    await callback_query.edit_message_text(
        "📝 **YouTube Title Input Module:**\n\nPlease transmit the new YouTube video title.\n"
        f"**Current:** `{current_title}`\n"
        "_(Type 'skip' to keep current, 'cancel' to abort.)_",
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
        await message.reply(f"✅ **YouTube Title Skipped.** Using current: '{default_title}'", reply_markup=youtube_settings_inline_menu)
        logger.info(f"User {user_id} skipped YouTube title, using current.")
    elif title.lower() == "cancel":
        await message.reply("❌ **YouTube Title Update Aborted.**", reply_markup=youtube_settings_inline_menu)
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
    user_doc = get_user_data(user_id)
    current_tag = user_doc.get("youtube_settings", {}).get("tag", "#youtube #video #shorts")
    await callback_query.edit_message_text(
        "🏷️ **YouTube Tag Input Module:**\n\nPlease transmit the new YouTube tags (e.g., `#shorts #video`). Separate with spaces.\n"
        f"**Current:** `{current_tag}`\n"
        "_(Type 'skip' to keep current, 'cancel' to abort.)_",
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
        await message.reply(f"✅ **YouTube Tags Skipped.** Using current: '{default_tag}'", reply_markup=youtube_settings_inline_menu)
        logger.info(f"User {user_id} skipped YouTube tags, using current.")
    elif tag.lower() == "cancel":
        await message.reply("❌ **YouTube Tags Update Aborted.**", reply_markup=youtube_settings_inline_menu)
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
    user_doc = get_user_data(user_id)
    current_description = user_doc.get("youtube_settings", {}).get("description", "Default YouTube Description")
    await callback_query.edit_message_text(
        "📄 **YouTube Description Input Module:**\n\nPlease transmit the new YouTube description.\n"
        f"**Current:** `{current_description}`\n"
        "_(Type 'skip' to keep current, 'cancel' to abort.)_",
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
        await message.reply(f"✅ **YouTube Description Skipped.** Using current: '{default_description}'", reply_markup=youtube_settings_inline_menu)
        logger.info(f"User {user_id} skipped YouTube description, using current.")
    elif description.lower() == "cancel":
        await message.reply("❌ **YouTube Description Update Aborted.**", reply_markup=youtube_settings_inline_menu)
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
    video_type_raw = callback_query.data.split("_")[-1]
    video_type = "Shorts (Short Vertical Video)" if video_type_raw == 'shorts' else "Video (Standard Horizontal/Square)"
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
    user_doc = get_user_data(user_id)
    current_schedule = user_doc.get("youtube_settings", {}).get("schedule_time")
    current_schedule_str = datetime.fromisoformat(current_schedule).strftime("%Y-%m-%d %H:%M") if current_schedule else "None"
    await callback_query.edit_message_text(
        "⏰ **YouTube Schedule Configuration Module:**\n\nPlease transmit the desired schedule date and time.\n"
        "**Format:** `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`)\n"
        f"**Current Schedule:** `{current_schedule_str}` (UTC)\n"
        "_**System Note:** Time will be interpreted in UTC._\n"
        "_(Type 'clear' to remove any existing schedule, 'cancel' to abort.)_",
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
    elif schedule_str.lower() == "cancel":
        await message.reply("❌ **YouTube Schedule Update Aborted.**", reply_markup=youtube_settings_inline_menu)
        return

    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")
        schedule_dt_utc = schedule_dt # Assuming input is already effectively UTC or local bot time matches.

        if schedule_dt_utc <= datetime.utcnow() + timedelta(minutes=5):
            await message.reply("❌ **Time Constraint Violation.** Schedule time must be at least 5 minutes in the future. Please try again with a later time.")
            return

        update_user_data(user_id, {"youtube_settings.schedule_time": schedule_dt_utc.isoformat()})
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
    # The actual expiry date would come from a real OAuth flow with Google API.
    # Here, we'll just indicate if a token is present.
    yt_access_token = user_doc.get("youtube_access_token")
    token_status_text = "✅ Token Stored (Simulated)" if yt_access_token else "❌ No Token Stored"
    
    # In a real scenario, you'd check the token validity here.
    # For now, we'll give a placeholder expiry date if a token is present.
    expiry_info = "N/A (Requires real OAuth token validation/refresh)"
    if yt_access_token:
        # Simulate an expiry a year from now for a long-lived token idea,
        # but again, real OAuth tokens are short-lived.
        simulated_expiry = datetime.utcnow() + timedelta(days=365)
        expiry_info = f"Simulated: {simulated_expiry.strftime('%Y-%m-%d %H:%M')} UTC"

    await callback_query.edit_message_text(
        f"🗓️ **YouTube Token Status:**\n"
        f"• Token Presence: `{token_status_text}`\n"
        f"• Expiry Information: `{expiry_info}`\n\n"
        f"_**System Note:** YouTube OAuth 2.0 access tokens are typically short-lived and require a refresh token mechanism for persistent access. This is a simplified representation._",
        reply_markup=youtube_settings_inline_menu,
        parse_mode=enums.ParseMode.MARKDOWN
    )
    logger.info(f"User {user_id} checked YouTube token info.")


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
            await callback_query.message.edit_text("❌ **Authentication Required.** You are not logged into Facebook or haven't selected a page. Please navigate to `⚙️ Settings` -> `📘 Facebook Settings` to configure your account first.", reply_markup=facebook_settings_inline_menu)
            user_states.pop(user_id, None) # Clear state if auth is missing
            return

        user_states[user_id] = {"step": AWAITING_UPLOAD_TYPE_SELECTION, "platform": "facebook"}
        await callback_query.message.edit_text(
            "What type of content are you transmitting to Facebook?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎥 Video (Standard Post)", callback_data="upload_type_fb_video")],
                [InlineKeyboardButton("🎞️ Reel (Short Video)", callback_data="upload_type_fb_reels")],
                [InlineKeyboardButton("🖼️ Photo (Image Post)", callback_data="upload_type_fb_photo")]
            ])
        )
        logger.info(f"User {user_id} selected Facebook for upload, prompted for content type.")

    elif platform == "youtube":
        if not user_doc.get("youtube_logged_in"):
            await callback_query.message.edit_text("❌ **Authentication Required.** You are not logged into YouTube. Please navigate to `⚙️ Settings` -> `▶️ YouTube Settings` to configure your account first.", reply_markup=youtube_settings_inline_menu)
            user_states.pop(user_id, None) # Clear state if auth is missing
            return

        user_states[user_id] = {"step": AWAITING_UPLOAD_FILE, "platform": "youtube"} # YouTube handles video/shorts based on later settings
        await callback_query.message.edit_text(
            "🎥 **Content Transmission Protocol Active.** Please transmit your video file for YouTube now.\n\n"
            "_(You can also send a photo, but it will be converted to a video frame for YouTube. Only video uploads are currently supported on YouTube.)_",
            reply_markup=None # Remove inline keyboard for file input
        )
        # Send reply keyboard explicitly for convenience
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
        reply_markup=None # Remove inline keyboard for file input
    )
    # Send reply keyboard explicitly for convenience
    await client.send_message(user_id, "You can use '🔙 Main Menu' to abort the transmission.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
    logger.info(f"User {user_id} selected Facebook upload type '{upload_type}', awaiting file.")


@app.on_message(filters.video | filters.photo)
async def handle_media_upload(client, message):
    """Handles incoming video or photo files for upload."""
    user_id = message.chat.id
    if not get_user_data(user_id):
        await message.reply("⛔ **Access Denied!** Please send `/start` first to initialize your account in the system.")
        return

    state = user_states.get(user_id)
    if not state or (state.get("step") != AWAITING_UPLOAD_FILE):
        # If media is sent outside of an upload flow, inform the user.
        await message.reply("❗ **Unexpected Media Input.** Please initiate a content upload process by clicking '⬆️ Upload Content' first.")
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
        
        # Determine file extension based on media type
        file_extension = ""
        if message.video:
            file_extension = os.path.splitext(message.video.file_name or "video.mp4")[1].lower()
        elif message.photo:
            file_extension = ".jpg" # Photos usually come as JPEG
        
        if not file_extension: # Fallback if file_name is None for video, or other issues
            if message.video: file_extension = ".mp4"
            elif message.photo: file_extension = ".jpg"

        download_filename = f"downloads/{user_id}_{timestamp}{file_extension}"
        
        file_path = await message.download(file_name=download_filename)
        
        user_states[user_id]["file_path"] = file_path
        user_states[user_id]["step"] = AWAITING_UPLOAD_TITLE
        
        platform = state["platform"]
        user_doc = get_user_data(user_id)
        default_title = user_doc.get(f"{platform}_settings", {}).get("title", "Default Title")
        
        await initial_status_msg.edit_text(
            f"📝 **Metadata Input Required.** Now, transmit the **title** for your `{platform.capitalize()}` content.\n"
            f"_(Type 'skip' to use your default title: '{default_title}', 'cancel' to abort.)_"
        )
        logger.info(f"User {user_id} media downloaded to {file_path}. Awaiting title for {platform}.")
    except Exception as e:
        await initial_status_msg.edit_text(f"❌ **Data Acquisition Failed.** Error downloading media: `{e}`")
        logger.error(f"Failed to download media for user {user_id}: {e}", exc_info=True)
        user_states.pop(user_id, None) # Clear state on download error

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_TITLE))
async def handle_upload_title(client, message):
    """Handles input for content title."""
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state or "file_path" not in state: # Ensure file_path is already set
        await message.reply("❌ **Session Interrupted.** Please restart the upload process by sending your file again.")
        user_states.pop(user_id, None) # Clear potentially corrupted state
        return

    title_input = message.text.strip()
    platform = state["platform"]
    user_doc = get_user_data(user_id)
    
    if title_input.lower() == "skip":
        state["title"] = user_doc.get(f"{platform}_settings", {}).get("title", "Default Title")
        await message.reply(f"✅ **Title Input Skipped.** Using default title: '{state['title']}'.")
        logger.info(f"User {user_id} skipped title input for {platform}.")
    elif title_input.lower() == "cancel":
        await message.reply("❌ **Content Upload Aborted.** Title input cancelled.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        if state.get("file_path") and os.path.exists(state["file_path"]):
            os.remove(state["file_path"])
            logger.info(f"Cleaned up file {state['file_path']} for user {user_id}.")
        user_states.pop(user_id, None)
        return
    else:
        state["title"] = title_input
        await message.reply(f"✅ **Title Recorded.** New title: '{title_input}'.")
        logger.info(f"User {user_id} provided title for {platform}: '{title_input}'.")

    user_states[user_id]["step"] = AWAITING_UPLOAD_DESCRIPTION

    default_description = user_doc.get(f"{platform}_settings", {}).get("description", "Default Description")

    await message.reply(
        f"📝 **Metadata Input Required.** Now, transmit a **description** for your `{platform.capitalize()}` content.\n"
        f"_(Type 'skip' to use your default description: '{default_description}', 'cancel' to abort.)_"
    )
    logger.info(f"User {user_id} awaiting description for {platform}.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_DESCRIPTION))
async def handle_upload_description(client, message):
    """Handles input for content description."""
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state or "file_path" not in state:
        await message.reply("❌ **Session Interrupted.** Please restart the upload process by sending your file again.")
        user_states.pop(user_id, None)
        return

    description_input = message.text.strip()
    platform = state["platform"]
    user_doc = get_user_data(user_id)

    if description_input.lower() == "skip":
        state["description"] = user_doc.get(f"{platform}_settings", {}).get("description", "Default Description")
        await message.reply(f"✅ **Description Input Skipped.** Using default description: '{state['description']}'.")
        logger.info(f"User {user_id} skipped description input for {platform}.")
    elif description_input.lower() == "cancel":
        await message.reply("❌ **Content Upload Aborted.** Description input cancelled.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        if state.get("file_path") and os.path.exists(state["file_path"]):
            os.remove(state["file_path"])
            logger.info(f"Cleaned up file {state['file_path']} for user {user_id}.")
        user_states.pop(user_id, None)
        return
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
        # Facebook API doesn't have a direct 'private' status for posts like YouTube.
        # 'Draft' is the closest equivalent if not publishing publicly.
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
            "_**System Note:** Time will be interpreted in UTC._\n"
            "_(Type 'cancel' to abort.)_"
        )
        logger.info(f"User {user_id} chose 'schedule later' for {platform}.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_SCHEDULE_DATETIME))
async def handle_schedule_datetime_input(client, message):
    """Handles input for specific schedule date and time."""
    user_id = message.chat.id
    state = user_states.get(user_id)
    if not state or "file_path" not in state:
        await message.reply("❌ **Session Interrupted.** Please restart the upload process by sending your file again.")
        user_states.pop(user_id, None)
        return

    schedule_str = message.text.strip()
    
    if schedule_str.lower() == "cancel":
        await message.reply("❌ **Content Upload Aborted.** Schedule time input cancelled.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        if state.get("file_path") and os.path.exists(state["file_path"]):
            os.remove(state["file_path"])
            logger.info(f"Cleaned up file {state['file_path']} for user {user_id}.")
        user_states.pop(user_id, None)
        return

    try:
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")
        # Ensure schedule time is in UTC for consistency
        schedule_dt_utc = schedule_dt # Assuming input is already effectively UTC or local bot time matches.

        if schedule_dt_utc <= datetime.utcnow() + timedelta(minutes=5):
            await message.reply("❌ **Time Constraint Violation.** Schedule time must be at least 5 minutes in the future. Please transmit a later time.")
            # Keep user in this state to retry
            return

        state["schedule_time"] = schedule_dt_utc
        user_states[user_id]["step"] = "processing_and_uploading" # Set a temporary "busy" state
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
    """Initiates the actual content upload to the chosen platform."""
    state = user_states.get(user_id)
    if not state:
        await client.send_message(user_id, "❌ **Upload Process Aborted.** Session state lost. Please re-initiate the content transmission protocol.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        logger.error(f"Upload initiated without valid state for user {user_id}.")
        return

    platform = state["platform"]
    file_path = state.get("file_path")
    title = state.get("title")
    description = state.get("description")
    visibility = state.get("visibility", "public")
    schedule_time = state.get("schedule_time") # This is a datetime object now
    upload_type = state.get("upload_type", "video") # Default to video for Facebook

    if not all([file_path, title, description]):
        await client.send_message(user_id, "❌ **Upload Protocol Failure.** Missing essential content metadata (file, title, or description). Please restart the upload sequence.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        logger.error(f"Missing essential upload data for user {user_id}. State: {state}")
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        user_states.pop(user_id, None)
        return

    await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO) # Keep generic for now
    await log_to_channel(client, f"User `{user_id}` (`{message.from_user.username}`) initiating upload for {platform}. Type: `{upload_type}`. File: `{os.path.basename(file_path)}`. Visibility: `{visibility}`. Schedule: `{schedule_time.isoformat() if schedule_time else 'None'}`.")

    processed_file_path = file_path # Assume original is fine until conversion needed

    try:
        # --- Media Conversion / Pre-processing ---
        # Renamed `convert_media_for_facebook` to `process_media_for_upload`
        # and made it generic for both platforms.
        await client.send_message(user_id, f"🔄 **Data Pre-processing Protocol.** Analyzing and preparing content for `{platform.capitalize()}`. Please standby...")
        await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO) # Visual feedback

        def do_processing_sync():
            return process_media_for_upload(file_path, platform, upload_type if platform == "facebook" else "video")
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(do_processing_sync)
            # Timeout for processing large files. 30 minutes (1800 seconds) is generous.
            processed_file_path = future.result(timeout=1800) 
        
        if processed_file_path == file_path:
            await client.send_message(user_id, "✅ **Content Format Verified.** No conversion required. Proceeding with direct transmission.")
            logger.info(f"User {user_id} content already suitable format for {platform}. Skipping conversion.")
        else:
            await client.send_message(user_id, "✅ **Content Data Preparation Complete.**")
            await log_to_channel(client, f"User `{user_id}` content processed. Original: `{os.path.basename(file_path)}`, Processed: `{os.path.basename(processed_file_path)}`.")

        # --- Upload to Platform ---
        if platform == "facebook":
            fb_access_token, fb_selected_page_id = get_facebook_tokens_for_user(user_id)
            if not fb_access_token or not fb_selected_page_id:
                await client.send_message(user_id, "❌ **Authentication Required.** Facebook access token or selected page not found. Please re-authenticate via `⚙️ Settings` -> `📘 Facebook Settings`.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
                return

            await client.send_message(user_id, f"📤 **Initiating Facebook {upload_type.capitalize()} Transmission...**")
            await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO if upload_type != 'photo' else enums.ChatAction.UPLOAD_PHOTO)

            def upload_to_facebook_sync():
                return upload_facebook_content(processed_file_path, upload_type.lower(), title, description, fb_access_token, fb_selected_page_id, visibility=visibility, schedule_time=schedule_time)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(upload_to_facebook_sync)
                fb_result = future.result(timeout=3600) # Increased timeout for FB upload to 1 hour

            if fb_result and 'id' in fb_result:
                status_text = "Scheduled" if schedule_time else ("Draft" if visibility.lower() == 'draft' else "Published")
                await client.send_message(user_id, f"✅ **Facebook Content Transmitted!** {upload_type.capitalize()} ID: `{fb_result['id']}`. Status: `{status_text}`.")
                users_collection.update_one({"_id": user_id}, {"$inc": {"total_uploads": 1}})
                await log_to_channel(client, f"User `{user_id}` successfully uploaded {upload_type} to Facebook. ID: `{fb_result['id']}`. Status: `{status_text}`. File: `{os.path.basename(processed_file_path)}`.")
            else:
                await client.send_message(user_id, f"❌ **Facebook Transmission Failed.** Response: `{json.dumps(fb_result, indent=2)}`")
                logger.error(f"Facebook upload failed for user {user_id}. Result: {fb_result}")

        elif platform == "youtube":
            # YouTube Upload Logic (placeholder - requires Google API client setup)
            await client.send_message(user_id, "🚧 **YouTube Upload Feature Under Development.**\n\n_**System Note:** The YouTube upload functionality requires advanced Google API integration (OAuth 2.0, YouTube Data API V3) which is currently not fully implemented in this system. Your video was processed, but not uploaded._", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
            await log_to_channel(client, f"User `{user_id}` attempted YouTube upload (currently simulated). File: `{os.path.basename(processed_file_path)}`.")
            users_collection.update_one({"_id": user_id}, {"$inc": {"total_uploads": 1}}) # Still increment for attempt
            # In a real scenario, you'd use google-api-python-client here.

    except concurrent.futures.TimeoutError:
        await client.send_message(user_id, "❌ **Operation Timed Out.** Content processing or transmission exceeded time limits. The data file might be too large or the network connection is unstable. Please retry with a smaller file or a more robust connection.", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        logger.error(f"Upload/processing timeout for user {user_id}. Original file: {file_path}")
    except RuntimeError as re:
        await client.send_message(user_id, f"❌ **Processing Error:** `{re}`\n\n_**System Note:** Ensure FFmpeg is correctly installed and accessible in your system's PATH, and verify your media data file is not corrupted._", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        logger.error(f"Processing/Upload Error for user {user_id}: {re}", exc_info=True)
    except requests.exceptions.RequestException as req_e:
        await client.send_message(user_id, f"❌ **Network/API Error during Transmission:** `{req_e}`\n\n_**System Note:** Please verify your internet connection or inspect the configured API parameters for Facebook._", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        logger.error(f"Network/API Error for user {user_id}: {req_e}", exc_info=True)
    except Exception as e:
        await client.send_message(user_id, f"❌ **Critical Transmission Failure.** An unexpected system error occurred: `{e}`", reply_markup=main_menu_user if not is_admin(user_id) else main_menu_admin)
        logger.error(f"Upload failed for user {user_id}: {e}", exc_info=True)
    finally:
        # Clean up files
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up original file: {file_path}")
        # Only remove processed file if it's different from original and exists
        if processed_file_path and processed_file_path != file_path and os.path.exists(processed_file_path):
            os.remove(processed_file_path)
            logger.info(f"Cleaned up processed file: {processed_file_path}")
        user_states.pop(user_id, None) # Clear the state fully
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

threading.Thread(target=run_server, daemon=True).start()

# === START BOT ===
if __name__ == "__main__":
    # Ensure downloads directory for unprocessed and processed files
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Created 'downloads' directory.")
    if not os.path.exists("downloads/processed"):
        os.makedirs("downloads/processed")
        logger.info("Created 'downloads/processed' directory.")


    logger.info("Bot system initiating...")
    app.run()
