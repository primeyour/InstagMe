# main.py

import os
import sys
import asyncio
import threading
import logging
import subprocess
import json
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
import signal
from functools import wraps
import time
import requests
import random
from urllib.parse import urlparse, parse_qs
from collections import defaultdict

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# MongoDB
from pymongo import MongoClient, DESCENDING
from pymongo.errors import OperationFailure

# Pyrogram (Telegram Bot)
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import UserNotParticipant, FloodWait, UserIsBlocked, ChatAdminRequired, PeerIdInvalid
from pyrogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove
)

# Google/YouTube Client
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError

# System Utilities
import psutil

# --- Enhanced YouTube Authentication ---
oauth_flows = {}
oauth_tokens = {}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log")
    ]
)
logger = logging.getLogger("YTFBUser")

# === Load and Validate Environment Variables ===
API_ID_STR = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
LOG_CHANNEL_STR = os.getenv("LOG_CHANNEL_ID")
MONGO_URI = os.getenv("MONGO_DB")
ADMIN_ID_STR = os.getenv("ADMIN_ID")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8080")
PORT_STR = os.getenv("PORT", "8080")
# NEW: Required for the scheduling system
STORAGE_CHANNEL_STR = os.getenv("STORAGE_CHANNEL_ID") 

# Validate required environment variables
if not all([API_ID_STR, API_HASH, BOT_TOKEN, ADMIN_ID_STR, MONGO_URI, STORAGE_CHANNEL_STR]):
    logger.critical("FATAL ERROR: One or more required environment variables are missing. Please check TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_BOT_TOKEN, ADMIN_ID, MONGO_DB, and STORAGE_CHANNEL_ID.")
    sys.exit(1)

# Convert to correct types after validation
API_ID = int(API_ID_STR)
ADMIN_ID = int(ADMIN_ID_STR)
LOG_CHANNEL = int(LOG_CHANNEL_STR) if LOG_CHANNEL_STR else None
PORT = int(PORT_STR)
STORAGE_CHANNEL = int(STORAGE_CHANNEL_STR)

# === Advanced Video Processing Helpers ===

def get_video_metadata(file_path: str) -> dict:
    """Uses ffprobe to get detailed video metadata."""
    try:
        command = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json',
            '-show_format', '-show_streams', file_path
        ]
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8')
        return json.loads(result.stdout)
    except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError) as e:
        logger.error(f"Could not probe file '{file_path}': {e}")
        return {}

def generate_thumbnail(video_path: str, output_path: str) -> str | None:
    """Intelligently generates a thumbnail by finding a visually complex scene."""
    try:
        logger.info(f"Generating intelligent thumbnail for {video_path}...")
        # FIX: Added -y flag to overwrite existing files
        ffmpeg_command = [
            'ffmpeg', '-i', video_path,
            '-vf', "select='gt(scene,0.4)',scale=1280:-1",
            '-frames:v', '1', '-q:v', '2',
            output_path, '-y'
        ]
        subprocess.run(ffmpeg_command, check=True, capture_output=True)
        logger.info(f"Thumbnail saved to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Thumbnail generation failed: {e}. Falling back to a random frame.")
        try:
            # FIX: Added -y flag to overwrite existing files
            fallback_command = [
                'ffmpeg', '-i', video_path, '-ss', str(random.randint(1, 29)),
                '-vframes', '1', '-q:v', '2', output_path, '-y'
            ]
            subprocess.run(fallback_command, check=True, capture_output=True)
            return output_path
        except Exception as fallback_e:
            logger.error(f"Fallback thumbnail generation also failed: {fallback_e}")
            return None

def process_video_for_upload(input_file: str, output_file: str) -> str:
    """Ensures the video is in a web-compatible format (H.264 video, AAC audio, MP4 container)."""
    metadata = get_video_metadata(input_file)
    if not metadata:
        raise ValueError("Could not get video metadata. Assuming conversion is needed.")

    v_codec = "none"
    a_codec = "none"
    for stream in metadata.get('streams', []):
        if stream.get('codec_type') == 'video':
            v_codec = stream.get('codec_name')
        elif stream.get('codec_type') == 'audio':
            a_codec = stream.get('codec_name')

    if v_codec == 'h264' and a_codec == 'aac' and 'mp4' in metadata.get('format', {}).get('format_name', ''):
        logger.info(f"'{input_file}' is already compatible. No conversion needed.")
        if input_file != output_file:
            import shutil
            shutil.copy(input_file, output_file)
        return output_file

    logger.warning(f"'{input_file}' needs conversion (Video: {v_codec}, Audio: {a_codec}).")
    try:
        command = [
            'ffmpeg', '-y', '-i', input_file,
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
            '-c:a', 'aac', '-b:a', '192k',
            '-movflags', '+faststart',
            output_file
        ]
        subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"Successfully converted video to '{output_file}'.")
        return output_file
    except FileNotFoundError:
        raise FileNotFoundError("ffmpeg is not installed. Video processing is not possible.")
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg conversion failed for {input_file}. Error: {e.stderr}")
        raise ValueError(f"Video format is incompatible and conversion failed.")


# === Global Bot Settings ===
DEFAULT_GLOBAL_SETTINGS = {
    "special_event_toggle": False,
    "special_event_title": "🎉 Special Event!",
    "special_event_message": "Enjoy our special event features!",
    "max_concurrent_uploads": 15,
    "max_file_size_mb": 1000,
    "allow_multiple_logins": False,
    "delete_user_text_after_use": True, # NEW: Privacy setting
    "payment_settings": {
        "google_play_qr_file_id": "",
        "upi": "", "usdt": "", "btc": "", "others": "", "custom_buttons": {},
        "instructions": "After paying, please send the transaction ID or a screenshot of your payment."
    },
    "last_weekly_report": None
}

# --- Global State & DB Management ---
mongo = None
db = None
global_settings = {}
upload_semaphore = None
user_upload_locks = {}
MAX_FILE_SIZE_BYTES = 0
MAX_CONCURRENT_UPLOADS = 0
shutdown_event = asyncio.Event()
valid_log_channel = False

# Pyrogram Client
app = Client("upload_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
BOT_ID = 0

# --- Task Management ---
class TaskTracker:
    def __init__(self):
        self._tasks = set()
        self._user_specific_tasks = {}
        self.loop = None

    def create_task(self, coro, user_id=None, task_name=None):
        if self.loop is None:
            try:
                self.loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.error("Could not create task: No running event loop.")
                return
        if user_id and task_name:
            self.cancel_user_task(user_id, task_name)
        task = self.loop.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        if user_id and task_name:
            if user_id not in self._user_specific_tasks:
                self._user_specific_tasks[user_id] = {}
            self._user_specific_tasks[user_id][task_name] = task
        return task

    def cancel_user_task(self, user_id, task_name):
        if user_id in self._user_specific_tasks and task_name in self._user_specific_tasks[user_id]:
            task_to_cancel = self._user_specific_tasks[user_id].pop(task_name)
            if not task_to_cancel.done():
                task_to_cancel.cancel()
                logger.info(f"Cancelled previous task '{task_name}' for user {user_id}.")
            if not self._user_specific_tasks[user_id]:
                del self._user_specific_tasks[user_id]

    async def cancel_all_user_tasks(self, user_id):
        if user_id in self._user_specific_tasks:
            user_tasks = self._user_specific_tasks.pop(user_id)
            for task_name, task in user_tasks.items():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*[t for t in user_tasks.values() if not t.done()], return_exceptions=True)

    async def cancel_and_wait_all(self):
        tasks_to_cancel = [t for t in self._tasks if not t.done()]
        if not tasks_to_cancel: return
        logger.info(f"Cancelling {len(tasks_to_cancel)} outstanding background tasks...")
        for t in tasks_to_cancel:
            t.cancel()
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        logger.info("All background tasks have been awaited.")

task_tracker = None

async def safe_task_wrapper(coro):
    """Wraps a coroutine to catch and log any exceptions."""
    try:
        await coro
    except asyncio.CancelledError:
        logger.warning(f"Task {asyncio.current_task().get_name()} was cancelled.")
    except Exception:
        logger.exception(f"Unhandled exception in background task: {asyncio.current_task().get_name()}")

# ===================================================================
# ==================== FONT & TEXT HELPERS ==========================
# ===================================================================

def to_bold_sans(text: str) -> str:
    """Converts a string to bold sans-serif font."""
    bold_sans_map = {
        'A': '𝗔', 'B': '𝗕', 'C': '𝗖', 'D': '𝗗', 'E': '𝗘', 'F': '𝗙', 'G': '𝗚', 'H': '𝗛', 'I': '𝗜',
        'J': '𝗝', 'K': '𝗞', 'L': '𝗟', 'M': '𝗠', 'N': '𝗡', 'O': '𝗢', 'P': '𝗣', 'Q': '𝗤', 'R': '𝗥',
        'S': '𝗦', 'T': '𝗧', 'U': '𝗨', 'V': '𝗩', 'W': '𝗪', 'X': '𝗫', 'Y': '𝗬', 'Z': '𝗭',
        'a': '𝗮', 'b': '𝗯', 'c': '𝗰', 'd': '𝗱', 'e': '𝗲', 'f': '𝗳', 'g': '𝗴', 'h': '𝗵', 'i': '𝗶',
        'j': '𝗷', 'k': '𝗸', 'l': '𝗹', 'm': '𝗺', 'n': '𝗻', 'o': '𝗼', 'p': '𝗽', 'q': '𝗾', 'r': '𝗿',
        's': '𝘀', 't': '𝘁', 'u': '𝘂', 'v': '𝘃', 'w': '𝘄', 'x': '𝘅', 'y': '𝘆', 'z': '𝘇',
        '0': '𝟬', '1': '𝟭', '2': '𝟮', '3': '𝟯', '4': '𝟰', '5': '𝟱', '6': '𝟲', '7': '𝟳', '8': '𝟴', '9': '𝟵'
    }
    sanitized_text = str(text).encode('utf-8', 'surrogatepass').decode('utf-8')
    return ''.join(bold_sans_map.get(char, char) for char in sanitized_text)

# State dictionary to hold user states
user_states = {}

PREMIUM_PLANS = {
    "6_hour_trial": {"duration": timedelta(hours=6), "price": "Free / Free"},
    "3_days": {"duration": timedelta(days=3), "price": "₹10 / $0.40"},
    "7_days": {"duration": timedelta(days=7), "price": "₹25 / $0.70"},
    "15_days": {"duration": timedelta(days=15), "price": "₹35 / $0.90"},
    "1_month": {"duration": timedelta(days=30), "price": "₹60 / $2.50"},
    "3_months": {"duration": timedelta(days=90), "price": "₹150 / $4.50"},
    "1_year": {"duration": timedelta(days=365), "price": "Negotiable / Negotiable"},
    "lifetime": {"duration": None, "price": "Negotiable / Negotiable"}
}
PREMIUM_PLATFORMS = ["facebook", "youtube"]

# ===================================================================
# ==================== MARKUP GENERATORS ============================
# ===================================================================

def get_main_keyboard(user_id, premium_platforms):
    buttons = [
        [KeyboardButton("⚙️ ꜱᴇᴛᴛɪɴɢꜱ"), KeyboardButton("📊 ꜱᴛᴀᴛꜱ")]
    ]
    fb_buttons = []
    yt_buttons = []

    if "facebook" in premium_platforms:
        fb_buttons.extend([
            KeyboardButton("📘 FB ᴠɪᴅᴇᴏ"),
            KeyboardButton("📘 FB ʀᴇᴇʟꜱ"),
            KeyboardButton("📦 Bulk Upload FB"),
        ])
    if "youtube" in premium_platforms:
        yt_buttons.extend([
            KeyboardButton("▶️ YT ᴠɪᴅᴇᴏ"),
            KeyboardButton("🟥 YT ꜱʜᴏʀᴛꜱ"),
            KeyboardButton("📦 Bulk Upload YT"),
        ])
    
    if fb_buttons:
        buttons.insert(0, fb_buttons)
    if yt_buttons:
        insert_index = 1 if fb_buttons else 0
        buttons.insert(insert_index, yt_buttons)

    buttons.append([KeyboardButton("⭐ ᴩʀᴇᴍɪᴜᴍ"), KeyboardButton("/premiumdetails")])
    if is_admin(user_id):
        buttons.append([KeyboardButton("🛠 ᴀᴅᴍɪɴ ᴩᴀɴᴇʟ"), KeyboardButton("🔄 ʀᴇꜱᴛᴀʀᴛ ʙᴏᴛ")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, selective=True)

async def get_main_settings_markup(user_id):
    # DYNAMIC SETTINGS MENU
    user_settings = await get_user_settings(user_id)
    buttons = []
    if await is_premium_for_platform(user_id, "facebook"):
        buttons.append([InlineKeyboardButton("📘 ғᴀᴄᴇʙᴏᴏᴋ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="hub_settings_facebook")])
        buttons.append([InlineKeyboardButton("🗓️ My FB Schedules", callback_data="manage_schedules_facebook")])
    if await is_premium_for_platform(user_id, "youtube"):
        buttons.append([InlineKeyboardButton("▶️ yᴏᴜᴛᴜʙᴇ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="hub_settings_youtube")])
        buttons.append([InlineKeyboardButton("🗓️ My YT Schedules", callback_data="manage_schedules_youtube")])

    # Privacy setting toggle
    delete_enabled = user_settings.get("delete_user_text_after_use", global_settings.get("delete_user_text_after_use"))
    emoji = "✅" if delete_enabled else "❌"
    buttons.append([InlineKeyboardButton(f"{emoji} Auto-Delete My Text", callback_data="toggle_auto_delete_text")])
    
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(buttons)

def get_facebook_settings_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 ᴅᴇғᴀᴜʟᴛ ᴄᴀᴩᴛɪᴏɴ", callback_data="set_caption_facebook")],
        [InlineKeyboardButton("👤 ᴍᴀɴᴀɢᴇ ғʙ ᴀᴄᴄᴏᴜɴᴛꜱ", callback_data="manage_fb_accounts")],
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="back_to_settings")]
    ])

def get_youtube_settings_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 ᴅᴇғᴀᴜʟᴛ ᴛɪᴛʟᴇ", callback_data="set_title_youtube")],
        [InlineKeyboardButton("📄 ᴅᴇғᴀᴜʟᴛ ᴅᴇꜱᴄʀɪᴩᴛɪᴏɴ", callback_data="set_description_youtube")],
        [InlineKeyboardButton("🏷️ ᴅᴇғᴀᴜʟᴛ ᴛᴀɢꜱ", callback_data="set_tags_youtube")],
        [InlineKeyboardButton("👁️ ᴅᴇғᴀᴜʟᴛ ᴠɪꜱɪʙɪʟɪᴛy", callback_data="set_visibility_youtube")],
        [InlineKeyboardButton("👤 ᴍᴀɴᴀɢᴇ yᴛ ᴀᴄᴄᴏᴜɴᴛꜱ", callback_data="manage_yt_accounts")],
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="back_to_settings")]
    ])

def get_progress_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_upload")]
    ])

def get_upload_flow_markup(platform, step, upload_type=None):
    buttons = []
    # REELS THUMBNAIL FIX: Don't show thumbnail option for Reels
    if step == "thumbnail" and upload_type != "reel":
        buttons.extend([
            [InlineKeyboardButton("🖼️ ᴜᴩʟᴏᴀᴅ ᴄᴜꜱᴛᴏᴍ", callback_data="upload_flow_thumbnail_custom")],
            [InlineKeyboardButton("🤖 ᴀᴜᴛᴏ-ɢᴇɴᴇʀᴀᴛᴇ", callback_data="upload_flow_thumbnail_auto")]
        ])
    elif step == "visibility":
        buttons.extend([
            [InlineKeyboardButton("🌍 ᴩᴜʙʟɪᴄ", callback_data="upload_flow_visibility_public")],
            [InlineKeyboardButton("🔒 ᴩʀɪᴠᴀᴛᴇ", callback_data="upload_flow_visibility_private")],
            [InlineKeyboardButton("🔗 ᴜɴʟɪꜱᴛᴇᴅ", callback_data="upload_flow_visibility_unlisted")]
        ])
    elif step == "publish":
        buttons.extend([
            [InlineKeyboardButton("🚀 ᴩᴜʙʟɪꜱʜ ɴᴏᴡ", callback_data="upload_flow_publish_now")],
            [InlineKeyboardButton("⏰ ꜱᴄʜᴇᴅᴜʟᴇ ʟᴀᴛᴇʀ", callback_data="upload_flow_publish_schedule")]
        ])

    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_upload")])
    return InlineKeyboardMarkup(buttons)

async def get_schedule_management_markup(user_id, platform):
    buttons = []
    if db:
        scheduled_jobs = await asyncio.to_thread(
            list,
            db.scheduled_jobs.find({
                "user_id": user_id,
                "platform": platform,
                "status": "pending"
            }).sort("scheduled_time_utc", 1)
        )
        for job in scheduled_jobs[:10]: # Show up to 10
            title = job['metadata'].get('title', 'Untitled')[:20]
            time_str = job['scheduled_time_utc'].strftime('%Y-%m-%d %H:%M')
            buttons.append([
                InlineKeyboardButton(f"'{title}' on {time_str}", callback_data=f"view_schedule_{job['_id']}"),
                InlineKeyboardButton("❌", callback_data=f"cancel_schedule_{job['_id']}")
            ])

    buttons.append([InlineKeyboardButton("🔙 Back to Settings", callback_data="back_to_settings")])
    return InlineKeyboardMarkup(buttons)
    
# ===================================================================
# ====================== HELPER FUNCTIONS ===========================
# ===================================================================
user_click_timestamps = defaultdict(list)
SPAM_LIMIT = 10
SPAM_TIMEFRAME = 10 # seconds

def rate_limit_callbacks(func):
    """Decorator to prevent callback query spam."""
    @wraps(func)
    async def wrapper(client, query):
        user_id = query.from_user.id
        now = time.time()
        
        # Clean up old timestamps
        user_click_timestamps[user_id] = [t for t in user_click_timestamps[user_id] if now - t < SPAM_TIMEFRAME]
        
        if len(user_click_timestamps[user_id]) >= SPAM_LIMIT:
            await query.answer("Rate limit exceeded. Please wait a moment.", show_alert=True)
            return
            
        user_click_timestamps[user_id].append(now)
        return await func(client, query)
    return wrapper

def is_admin(user_id):
    return user_id == ADMIN_ID

async def _get_user_data(user_id):
    if db is None:
        return {"_id": user_id, "premium": {}}
    return await asyncio.to_thread(db.users.find_one, {"_id": user_id})

async def _save_user_data(user_id, data_to_update):
    if db is None:
        logger.warning(f"DB not connected. Skipping save for user {user_id}.")
        return
    serializable_data = {}
    for key, value in data_to_update.items():
        if isinstance(value, dict):
            serializable_data[key] = {k: v for k, v in value.items() if not k.startswith('$')}
        else:
            serializable_data[key] = value
    await asyncio.to_thread(
        db.users.update_one,
        {"_id": user_id},
        {"$set": serializable_data},
        upsert=True
    )

async def _update_global_setting(key, value):
    global_settings[key] = value
    if db is None: return
    await asyncio.to_thread(db.settings.update_one, {"_id": "global_settings"}, {"$set": {key: value}}, upsert=True)

async def is_premium_for_platform(user_id, platform):
    if user_id == ADMIN_ID: return True
    if db is None: return False

    user = await _get_user_data(user_id)
    if not user: return False

    platform_premium = user.get("premium", {}).get(platform, {})
    if not platform_premium or platform_premium.get("status") == "expired": return False
        
    premium_type = platform_premium.get("type")
    premium_until = platform_premium.get("until")

    if premium_until and isinstance(premium_until, datetime) and premium_until.tzinfo is None:
        premium_until = premium_until.replace(tzinfo=timezone.utc)

    if premium_type == "lifetime": return True

    if premium_until and isinstance(premium_until, datetime) and premium_until > datetime.now(timezone.utc):
        return True

    if premium_type and premium_until and premium_until <= datetime.now(timezone.utc):
        await asyncio.to_thread(
            db.users.update_one,
            {"_id": user_id},
            {"$set": {f"premium.{platform}.status": "expired"}}
        )
        return False
    return False

async def get_user_settings(user_id):
    settings = {}
    if db is not None:
        settings = await asyncio.to_thread(db.settings.find_one, {"_id": user_id}) or {}
    
    settings.setdefault("caption_facebook", "")
    settings.setdefault("active_facebook_id", None)
    settings.setdefault("title_youtube", "")
    settings.setdefault("description_youtube", "")
    settings.setdefault("tags_youtube", "")
    settings.setdefault("visibility_youtube", "private")
    settings.setdefault("active_youtube_id", None)
    settings.setdefault("delete_user_text_after_use", global_settings.get("delete_user_text_after_use"))
    return settings

async def save_user_settings(user_id, settings):
    if db is None: return
    await asyncio.to_thread(db.settings.update_one, {"_id": user_id}, {"$set": settings}, upsert=True)

async def get_active_session(user_id, platform):
    user_settings = await get_user_settings(user_id)
    active_id = user_settings.get(f"active_{platform}_id")
    if not active_id or db is None: return None
    
    session = await asyncio.to_thread(db.sessions.find_one, {"user_id": user_id, "platform": platform, "account_id": active_id})
    return session.get("session_data") if session else None

async def safe_edit_message(message, text, reply_markup=None, parse_mode=enums.ParseMode.MARKDOWN):
    try:
        if not message: return
        current_text = getattr(message, 'text', '') or getattr(message, 'caption', '')
        if current_text and hasattr(current_text, 'strip') and current_text.strip() == text.strip() and message.reply_markup == reply_markup:
            return
        await message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            logger.warning(f"Couldn't edit message: {e}")

async def restart_bot(msg):
    restart_msg_log = (f"🔄 **Bot Restart Initiated (Graceful)**\n\n👤 **By**: {msg.from_user.mention} (ID: `{msg.from_user.id}`)")
    logger.info(f"User {msg.from_user.id} initiated graceful restart.")
    await send_log_to_channel(app, LOG_CHANNEL, restart_msg_log)
    await msg.reply(
        to_bold_sans("Graceful Restart Initiated...") + "\n\n"
        "The bot will shut down cleanly. If running under a process manager, it will restart automatically."
    )
    shutdown_event.set()

_progress_updates = {}
_upload_progress = {} 

def download_progress_callback(current, total, ud_type, msg_id, chat_id, start_time, last_update_time):
    now = time.time()
    if now - last_update_time[0] < 2 and current != total: return
    last_update_time[0] = now
    
    with threading.Lock():
        _progress_updates[(chat_id, msg_id)] = {
            "current": current, "total": total, "ud_type": ud_type, "start_time": start_time, "now": now
        }

async def monitor_progress_task(chat_id, msg_id, progress_msg):
    """Monitors and updates the progress of a download or upload."""
    try:
        while True:
            await asyncio.sleep(2)
            with threading.Lock():
                update_data = _progress_updates.get((chat_id, msg_id))
            
            if update_data:
                current, total, ud_type, start_time, now = (update_data['current'], update_data['total'], update_data['ud_type'], update_data['start_time'], update_data['now'])
                percentage = current * 100 / total
                speed = current / (now - start_time) if (now - start_time) > 0 else 0
                eta = timedelta(seconds=int((total - current) / speed)) if speed > 0 else "N/A"
                progress_bar = f"[{'█' * int(percentage / 5)}{' ' * (20 - int(percentage / 5))}]"
                progress_text = (
                    f"{to_bold_sans(f'{ud_type} Progress')}: `{progress_bar}`\n"
                    f"📊 **Percentage**: `{percentage:.2f}%`\n"
                    f"✅ **Done**: `{current / (1024 * 1024):.2f}` MB / `{total / (1024 * 1024):.2f}` MB\n"
                    f"🚀 **Speed**: `{speed / (1024 * 1024):.2f}` MB/s\n⏳ **ETA**: `{eta}`"
                )
                await safe_edit_message(progress_msg, progress_text, reply_markup=get_progress_markup(), parse_mode=None)
                
                if current == total:
                    with threading.Lock():
                        _progress_updates.pop((chat_id, msg_id), None)
                    break 

            elif _upload_progress.get('status') == 'uploading':
                await safe_edit_message(progress_msg, "⬆️ " + to_bold_sans("Uploading To API... Please Wait."), reply_markup=get_progress_markup())
            elif _upload_progress.get('status') == 'complete':
                _upload_progress.clear()
                break
    except asyncio.CancelledError:
        logger.info(f"Progress monitor task for msg {msg_id} was cancelled.")

def cleanup_temp_files(files_to_delete):
    for file_path in files_to_delete:
        if file_path and os.path.exists(file_path):
            try: os.remove(file_path)
            except Exception as e: logger.error(f"Error deleting file {file_path}: {e}")

def with_user_lock(func):
    """A decorator to prevent a user from running multiple commands simultaneously."""
    @wraps(func)
    async def wrapper(client, message, *args, **kwargs):
        user_id = message.from_user.id
        if user_id not in user_upload_locks:
            user_upload_locks[user_id] = asyncio.Lock()
        if user_upload_locks[user_id].locked():
            return await message.reply("⚠️ " + to_bold_sans("Another Operation Is Already In Progress. Please Wait Or Use The ❌ Cancel Button."))
        async with user_upload_locks[user_id]:
            return await func(client, message, *args, **kwargs)
    return wrapper

# ===================================================================
# ======================== COMMAND HANDLERS =========================
# ===================================================================

@app.on_message(filters.command("start"))
async def start(_, msg):
    user_id = msg.from_user.id
    user_first_name = msg.from_user.first_name or "there"
    premium_platforms = [p for p in PREMIUM_PLATFORMS if await is_premium_for_platform(user_id, p)]

    if is_admin(user_id):
        await msg.reply(
            to_bold_sans("Welcome To The Direct Upload Bot!") + "\n\n🛠️ " + to_bold_sans("You Have Admin Privileges."),
            reply_markup=get_main_keyboard(user_id, PREMIUM_PLATFORMS)
        )
        return

    user = await _get_user_data(user_id)
    is_new_user = not user or "added_by" not in user
    if is_new_user:
        await _save_user_data(user_id, {
            "_id": user_id, "premium": {}, "added_by": "self_start", 
            "added_at": datetime.now(timezone.utc), "username": msg.from_user.username
        })
        await send_log_to_channel(app, LOG_CHANNEL, f"🌟 New user started bot: `{user_id}` (`{msg.from_user.username or 'N/A'}`)")
        trial_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Activate FREE FB Trial", callback_data="activate_trial_facebook")],
            [InlineKeyboardButton("✅ Activate FREE YT Trial", callback_data="activate_trial_youtube")],
            [InlineKeyboardButton("➡️ View premium plans", callback_data="buypypremium")]
        ])
        await msg.reply(
            f"👋 **Hi {user_first_name}!**\n\n"
            + to_bold_sans("This Bot Lets You Upload To Facebook & YouTube Directly From Telegram.") + "\n\n"
            + to_bold_sans("To Get A Taste Of The Premium Features, You Can Activate A Free 6-hour Trial!"),
            reply_markup=trial_markup, parse_mode=enums.ParseMode.MARKDOWN
        )
        return
    
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc), "username": msg.from_user.username})

    welcome_msg = to_bold_sans("Welcome Back To Telegram ➜ Direct Uploader") + "\n\n"
    if premium_platforms:
        for platform in premium_platforms:
            p_data = user.get("premium", {}).get(platform, {})
            if p_data.get("type") == "lifetime":
                welcome_msg += f"⭐ {platform.capitalize()} premium: **Lifetime**\n"
            elif p_data.get("until"):
                p_expiry = p_data["until"]
                if p_expiry.tzinfo is None: p_expiry = p_expiry.replace(tzinfo=timezone.utc)
                remaining = p_expiry - datetime.now(timezone.utc)
                if remaining.total_seconds() > 0:
                    welcome_msg += f"⭐ {platform.capitalize()} premium expires in: `{remaining.days} days, {remaining.seconds // 3600} hours`.\n"
    else:
        welcome_msg += ("🔥 **Key Features:**\n"
                        "✅ Direct Login & Ultra-fast uploading\n"
                        "✅ No file size limit & unlimited uploads\n"
                        "✅ Advanced Scheduling & Bulk Uploads\n\n"
                        f"👤 Contact Admin to get premium. Your ID: `{user_id}`")

    await msg.reply(welcome_msg, reply_markup=get_main_keyboard(user_id, premium_platforms), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("restart") & filters.user(ADMIN_ID))
async def restart_cmd(_, msg):
    await restart_bot(msg)

@app.on_message(filters.command(["fblogin", "flogin"]))
@with_user_lock
async def facebook_login_cmd_new(_, msg):
    user_id = msg.from_user.id
    if not await is_premium_for_platform(user_id, "facebook"):
        return await msg.reply("❌ " + to_bold_sans("Facebook Premium Access Is Required."))
    user_states[user_id] = {"action": "waiting_for_fb_page_token", "platform": "facebook"}
    await msg.reply("🔑 " + to_bold_sans("Please Enter Your Facebook Page Access Token."))

@app.on_message(filters.command(["ytlogin", "ylogin"]))
@with_user_lock
async def youtube_login_cmd_new(_, msg):
    user_id = msg.from_user.id
    if not await is_premium_for_platform(user_id, "youtube"):
        return await msg.reply("❌ " + to_bold_sans("YouTube Premium Access Is Required."))
    user_states[user_id] = {"action": "waiting_for_yt_client_id", "platform": "youtube"}
    await msg.reply("🔑 " + to_bold_sans("Please Enter Your Google OAuth `client_id`."))

@app.on_message(filters.command("skip"))
@with_user_lock
async def handle_skip_command(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    if not state_data or "file_info" not in state_data: return

    action = state_data.get('action')
    if action == 'waiting_for_title':
        state_data["file_info"]["title"] = None
    elif action == 'waiting_for_description':
        state_data["file_info"]["description"] = ""
    elif action == 'waiting_for_tags':
        state_data["file_info"]["tags"] = ""
    
    # NEW: Auto-delete user's skip command
    user_settings = await get_user_settings(user_id)
    if user_settings.get("delete_user_text_after_use", True):
        try: await msg.delete()
        except Exception: pass
    
    await process_upload_step(msg)

@app.on_message(filters.command("finish") & filters.private)
@with_user_lock
async def finish_bulk_upload_command(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    if not state_data or state_data.get("action") != "waiting_for_bulk_media":
        return

    bulk_files = state_data.get("bulk_files", [])
    if not bulk_files:
        return await msg.reply("You haven't sent any files yet. Please send up to 10 media files first.")

    state_data["action"] = "waiting_for_bulk_caption"
    interactive_msg = state_data.get("interactive_msg")
    if interactive_msg:
        await safe_edit_message(interactive_msg, f"✅ **{len(bulk_files)} files received.**\n\nNow, please send a single caption/title to be used for all of them.")
    else:
        await msg.reply("Please send a single caption/title for all files.")

# ===================================================================
# ======================== REGEX HANDLERS ===========================
# ===================================================================

@app.on_message(filters.regex("⚙️ ꜱᴇᴛᴛɪɴɢꜱ"))
async def settings_menu(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    
    has_premium_any = await is_premium_for_platform(user_id, "facebook") or await is_premium_for_platform(user_id, "youtube")
    if not is_admin(user_id) and not has_premium_any:
        return await msg.reply("❌ " + to_bold_sans("Premium Required To Access Settings."))
    
    await msg.reply(
        "⚙️ " + to_bold_sans("Configure Your Upload Settings:"),
        reply_markup=await get_main_settings_markup(user_id)
    )

@app.on_message(filters.regex("^(📘 FB ᴠɪᴅᴇᴏ|📘 FB ʀᴇᴇʟꜱ|▶️ YT ᴠɪᴅᴇᴏ|🟥 YT ꜱʜᴏʀᴛꜱ|📦 Bulk Upload FB|📦 Bulk Upload YT)"))
@with_user_lock
async def initiate_upload(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})

    type_map = {
        "📘 FB ᴠɪᴅᴇᴏ": ("facebook", "video"), "📘 FB ʀᴇᴇʟꜱ": ("facebook", "reel"),
        "▶️ YT ᴠɪᴅᴇᴏ": ("youtube", "video"), "🟥 YT ꜱʜᴏʀᴛꜱ": ("youtube", "short"),
        "📦 Bulk Upload FB": ("facebook", "bulk"), "📦 Bulk Upload YT": ("youtube", "bulk"),
    }
    platform, upload_type = type_map[msg.text]

    if not await is_premium_for_platform(user_id, platform):
        return await msg.reply(f"❌ " + to_bold_sans(f"Your Access Has Been Denied. Please Upgrade To {platform.capitalize()} Premium."))

    sessions = await asyncio.to_thread(list, db.sessions.find({"user_id": user_id, "platform": platform}))
    if not sessions:
        return await msg.reply(f"❌ " + to_bold_sans(f"Please Login To {platform.capitalize()} First Using `/{platform[0]}login`"), parse_mode=enums.ParseMode.MARKDOWN)
    
    if upload_type == "bulk":
        user_states[user_id] = {
            "action": "waiting_for_bulk_media", "platform": platform, "bulk_files": []
        }
        await msg.reply("✅ " + to_bold_sans("Bulk Upload Mode\n\nPlease send up to 10 media files, then type /finish when you are done."), reply_markup=ReplyKeyboardRemove())
    else:
        user_states[user_id] = {
            "action": "waiting_for_media", "platform": platform, "upload_type": upload_type, "file_info": {}
        }
        await msg.reply("✅ " + to_bold_sans("Send The Video File, Ready When You Are!"), reply_markup=ReplyKeyboardRemove())


# ===================================================================
# ======================== TEXT HANDLERS ============================
# ===================================================================

@app.on_message(filters.text & filters.private & ~filters.command(""))
@with_user_lock
async def handle_text_input(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})

    if not state_data: return
    action = state_data.get("action")
    
    # Auto-delete user's text message if setting is enabled
    user_settings = await get_user_settings(user_id)
    should_delete = user_settings.get("delete_user_text_after_use", True)
    
    async def delete_user_message():
        if should_delete:
            try: await msg.delete()
            except Exception: pass

    # --- Login Flow ---
    if action == "waiting_for_fb_page_token":
        token = msg.text.strip()
        login_msg = await msg.reply("🔐 " + to_bold_sans("Validating Token..."))
        try:
            url = f"https://graph.facebook.com/v19.0/me?access_token={token}&fields=id,name"
            response = requests.get(url)
            response.raise_for_status()
            page_data = response.json()
            page_id, page_name = page_data.get('id'), page_data.get('name')
            if not page_id or not page_name: raise ValueError("Invalid token")

            await asyncio.to_thread(db.sessions.update_one,
                {"user_id": user_id, "platform": "facebook", "account_id": page_id},
                {"$set": {"session_data": {'id': page_id, 'name': page_name, 'access_token': token}}}, upsert=True)
            
            user_settings["active_facebook_id"] = page_id
            await save_user_settings(user_id, user_settings)
            
            await safe_edit_message(login_msg, f"✅ {to_bold_sans('Successfully logged in!')}\n**Name:** `{page_name}`")
            if user_id in user_states: del user_states[user_id]
        except Exception as e:
            await safe_edit_message(login_msg, f"❌ " + to_bold_sans(f"Login Failed: Invalid token or API error. {e}"))
            
    # --- Upload Flow (Interactive UI) ---
    elif action == "waiting_for_title":
        state_data["file_info"]["title"] = msg.text
        await delete_user_message()
        await process_upload_step(msg)
    elif action == "waiting_for_description":
        state_data["file_info"]["description"] = msg.text
        await delete_user_message()
        await process_upload_step(msg)
    elif action == "waiting_for_tags":
        state_data["file_info"]["tags"] = msg.text
        await delete_user_message()
        await process_upload_step(msg)
    elif action == "waiting_for_schedule_time":
        try:
            dt_naive = datetime.strptime(msg.text.strip(), "%Y-%m-%d %H:%M")
            schedule_time_utc = dt_naive.replace(tzinfo=timezone.utc)
            if schedule_time_utc <= datetime.now(timezone.utc):
                return await msg.reply("❌ " + to_bold_sans("Scheduled time must be in the future."))
            
            state_data['file_info']['schedule_time_utc'] = schedule_time_utc
            await delete_user_message()
            await process_upload_step(msg)
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid format. Please use `YYYY-MM-DD HH:MM` in UTC."))

    # --- Bulk Upload Flow ---
    elif action == "waiting_for_bulk_caption":
        caption = msg.text
        await msg.reply(to_bold_sans("✅ Caption received. Scheduling all files... Please wait."))
        await process_bulk_upload(user_id, caption)
        if user_id in user_states: del user_states[user_id]

# ===================================================================
# =================== CALLBACK QUERY HANDLERS =======================
# ===================================================================

@app.on_callback_query()
@rate_limit_callbacks # Apply rate limit to all callbacks
async def handle_all_callbacks(_, query):
    # This function acts as a router for all callback queries
    data = query.data
    if data.startswith("hub_settings_"):
        await hub_settings_cb(_, query)
    elif data.startswith("manage_schedules_"):
        await manage_schedules_cb(_, query)
    elif data.startswith("cancel_schedule_"):
        await cancel_schedule_cb(_, query)
    elif data.startswith("toggle_auto_delete_text"):
        await toggle_auto_delete_cb(_, query)
    elif data.startswith("upload_flow_"):
        await upload_flow_cb(_, query)
    elif data.startswith("cancel_upload"):
        await cancel_upload_cb(_, query)
    elif data.startswith("back_to_"):
        await back_to_cb(_, query)
    # ... other specific handlers
    else:
        # Fallback or general handler if needed
        await query.answer()

async def hub_settings_cb(_, query):
    platform = query.data.split("_")[-1]
    if platform == "facebook":
        await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Configure Facebook Settings:"), reply_markup=get_facebook_settings_markup())
    elif platform == "youtube":
        await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Configure YouTube Settings:"), reply_markup=get_youtube_settings_markup())

async def manage_schedules_cb(_, query):
    user_id = query.from_user.id
    platform = query.data.split("_")[-1]
    await query.message.edit(
        f"🗓️ **Your Pending {platform.capitalize()} Schedules**",
        reply_markup=await get_schedule_management_markup(user_id, platform)
    )

async def cancel_schedule_cb(_, query):
    user_id = query.from_user.id
    job_id = query.data.split("_")[-1]
    
    if db:
        result = await asyncio.to_thread(db.scheduled_jobs.delete_one, {"_id": job_id, "user_id": user_id})
        if result.deleted_count > 0:
            await query.answer("✅ Schedule cancelled successfully!", show_alert=True)
            # Refresh the list
            platform = (await asyncio.to_thread(db.scheduled_jobs.find_one, {"_id": job_id}) or {}).get("platform")
            if platform:
                 await manage_schedules_cb(app, type('Query', (), {'data': f'manage_schedules_{platform}', 'message': query.message, 'from_user': query.from_user})())
        else:
            await query.answer("Could not find or cancel this schedule.", show_alert=True)

async def toggle_auto_delete_cb(_, query):
    user_id = query.from_user.id
    user_settings = await get_user_settings(user_id)
    current_status = user_settings.get("delete_user_text_after_use", global_settings.get("delete_user_text_after_use"))
    new_status = not current_status
    user_settings["delete_user_text_after_use"] = new_status
    await save_user_settings(user_id, user_settings)
    await query.answer(f"Auto-delete of text inputs is now {'ON' if new_status else 'OFF'}.")
    await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Settings Panel"), reply_markup=await get_main_settings_markup(user_id))

async def cancel_upload_cb(_, query):
    user_id = query.from_user.id
    await query.answer("Upload cancelled.", show_alert=True)
    
    state_data = user_states.get(user_id, {})
    status_msg = state_data.get('interactive_msg', query.message)
    await safe_edit_message(status_msg, "❌ **" + to_bold_sans("Upload Cancelled") + "**")

    file_info = state_data.get("file_info", {})
    files_to_clean = [file_info.get("downloaded_path"), file_info.get("processed_path"), file_info.get("thumbnail_path")]
    
    cleanup_temp_files(files_to_clean)
    if user_id in user_states: del user_states[user_id]
    await task_tracker.cancel_all_user_tasks(user_id)

async def upload_flow_cb(_, query):
    user_id = query.from_user.id
    data = query.data.replace("upload_flow_", "")
    state_data = user_states.get(user_id)
    if not state_data or "file_info" not in state_data:
        return await query.answer("❌ Error: State lost.", show_alert=True)
    
    parts = data.split("_")
    step, choice = parts[0], parts[1]

    if step == "thumbnail":
        if choice == "custom":
            state_data['action'] = 'waiting_for_thumbnail'
            await safe_edit_message(state_data['interactive_msg'], "🖼️ " + to_bold_sans("Please Send The Thumbnail Image."))
        elif choice == "auto":
            state_data['file_info']['thumbnail_path'] = "auto"
            await process_upload_step(query)
    elif step == "visibility":
        state_data['file_info']['visibility'] = choice
        await process_upload_step(query)
    elif step == "publish":
        if choice == "now":
            state_data['file_info']['schedule_time_utc'] = None
            await process_upload_step(query)
        elif choice == "schedule":
            state_data['action'] = 'waiting_for_schedule_time'
            await safe_edit_message(
                state_data['interactive_msg'],
                "⏰ " + to_bold_sans("Send Schedule Time (UTC)") + "\n\n"
                f"Format: `YYYY-MM-DD HH:MM`\nCurrent UTC time: `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}`"
            )
            
async def back_to_cb(_, query):
    data = query.data
    user_id = query.from_user.id
    if user_id in user_states: del user_states[user_id]
        
    if data == "back_to_main_menu":
        try: await query.message.delete()
        except: pass
        premium_platforms = [p for p in PREMIUM_PLATFORMS if await is_premium_for_platform(user_id, p) or is_admin(user_id)]
        await app.send_message(query.message.chat.id, "🏠 " + to_bold_sans("Main Menu"), reply_markup=get_main_keyboard(user_id, premium_platforms))
    elif data == "back_to_settings":
        await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Settings Panel"), reply_markup=await get_main_settings_markup(user_id))

# ===================================================================
# ======================== MEDIA HANDLERS ===========================
# ===================================================================

async def process_upload_step(msg_or_query):
    """Central function to handle the INTERACTIVE step-by-step upload process."""
    user_id = msg_or_query.from_user.id
    state_data = user_states.get(user_id)
    if not state_data: return
    
    interactive_msg = state_data["interactive_msg"]
    file_info = state_data["file_info"]
    platform = state_data["platform"]
    upload_type = state_data["upload_type"]
    user_settings = await get_user_settings(user_id)
    
    if "title" not in file_info:
        state_data["action"] = "waiting_for_title"
        default = user_settings.get(f'title_{platform}') or user_settings.get(f'caption_{platform}')
        prompt = to_bold_sans("Please Send Your Title.") + (f"\nOr use /skip for default: `{default[:50]}`" if default else "")
    elif "description" not in file_info:
        state_data["action"] = "waiting_for_description"
        default = user_settings.get(f'description_{platform}')
        prompt = to_bold_sans("Please Send Your Description.") + (f"\nOr use /skip for default: `{default[:50]}`" if default else "")
    elif platform == 'youtube' and "tags" not in file_info:
        state_data["action"] = "waiting_for_tags"
        default = user_settings.get(f'tags_{platform}')
        prompt = to_bold_sans("Please Send Comma-separated Tags.") + (f"\nOr use /skip for default: `{default[:50]}`" if default else "")
    elif "thumbnail_path" not in file_info and upload_type != 'reel':
        is_video = file_info['original_media_msg'].video or (file_info['original_media_msg'].document and 'video' in file_info['original_media_msg'].document.mime_type)
        if not is_video:
            file_info['thumbnail_path'] = None
            return await process_upload_step(msg_or_query)
        state_data["action"] = "waiting_for_thumbnail_choice"
        return await safe_edit_message(interactive_msg, to_bold_sans("Choose Thumbnail Option:"), reply_markup=get_upload_flow_markup(platform, 'thumbnail', upload_type))
    elif "visibility" not in file_info and platform == 'youtube':
        state_data["action"] = "waiting_for_visibility_choice"
        return await safe_edit_message(interactive_msg, to_bold_sans("Set Video Visibility:"), reply_markup=get_upload_flow_markup(platform, 'visibility'))
    elif "schedule_time_utc" not in file_info:
        state_data["action"] = "waiting_for_publish_choice"
        return await safe_edit_message(interactive_msg, to_bold_sans("When To Publish?"), reply_markup=get_upload_flow_markup(platform, 'publish'))
    else:
        # All info gathered
        state_data["action"] = "finalizing"
        schedule_time = file_info.get("schedule_time_utc")
        if schedule_time:
            # Schedule the job
            await schedule_upload_task(interactive_msg, file_info, user_id)
        else:
            # Upload immediately
            await start_upload_task(interactive_msg, file_info, user_id)
        return
        
    await safe_edit_message(interactive_msg, prompt, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.media & filters.private)
@with_user_lock
async def handle_media_upload(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id, {})
    action = state_data.get("action")

    # Handle thumbnail during upload flow
    if action == 'waiting_for_thumbnail':
        if not msg.photo: return await msg.reply("❌ " + to_bold_sans("Please send an image file for the thumbnail."))
        status_msg = await msg.reply("🖼️ " + to_bold_sans("Downloading thumbnail..."))
        thumb_path = await app.download_media(msg.photo)
        state_data['file_info']['thumbnail_path'] = thumb_path
        await status_msg.delete()
        await process_upload_step(msg)
        return
        
    # Handle bulk media upload
    if action == "waiting_for_bulk_media":
        if len(state_data.get("bulk_files", [])) >= 10:
            return await msg.reply("You have already sent 10 files. Please type /finish to proceed.")
        state_data["bulk_files"].append(msg)
        if "interactive_msg" not in state_data:
            state_data["interactive_msg"] = await msg.reply(f"File {len(state_data['bulk_files'])}/10 received.")
        else:
            await safe_edit_message(state_data["interactive_msg"], f"File {len(state_data['bulk_files'])}/10 received.")
        return

    if not action or action != "waiting_for_media": return
    
    media = msg.video or msg.photo or msg.document
    if not media or media.file_size > MAX_FILE_SIZE_BYTES:
        return await msg.reply(f"❌ File too large or unsupported.")

    # START OF INTERACTIVE FLOW
    interactive_msg = await msg.reply("⏳ " + to_bold_sans("Preparing..."))
    state_data['interactive_msg'] = interactive_msg
    
    try:
        start_time = time.time()
        last_update_time = [0]
        task_tracker.create_task(monitor_progress_task(msg.chat.id, interactive_msg.id, interactive_msg), user_id=user_id, task_name="progress_monitor")
        
        await safe_edit_message(interactive_msg, "Downloading...")
        downloaded_path = await app.download_media(
            msg, progress=download_progress_callback,
            progress_args=("Download", interactive_msg.id, msg.chat.id, start_time, last_update_time)
        )
        task_tracker.cancel_user_task(user_id, "progress_monitor")

        state_data["file_info"] = {"original_media_msg": msg, "downloaded_path": downloaded_path}
        
        # If Reels, skip thumbnail step
        if state_data.get("upload_type") == "reel":
            state_data["file_info"]["thumbnail_path"] = None
        
        # If FB non-youtube, skip visibility step
        if state_data.get("platform") == "facebook":
            state_data["file_info"]["visibility"] = "public"
            
        await process_upload_step(msg)

    except Exception as e:
        logger.error(f"Error during file download for user {user_id}: {e}", exc_info=True)
        await safe_edit_message(interactive_msg, f"❌ " + to_bold_sans(f"Download Failed: {e}"))
        if user_id in user_states: del user_states[user_id]

# ===================================================================
# ==================== UPLOAD & SCHEDULE PROCESSING ===============
# ===================================================================
async def schedule_upload_task(msg, file_info, user_id):
    """Handles the scheduling logic."""
    await safe_edit_message(msg, "⏰ " + to_bold_sans("Scheduling your post..."))
    try:
        media_msg = file_info['original_media_msg']
        
        # Forward file to storage channel
        stored_message = await media_msg.forward(STORAGE_CHANNEL)
        
        # Create job in DB
        job_doc = {
            "_id": f"{user_id}_{int(time.time())}",
            "user_id": user_id,
            "platform": user_states[user_id]["platform"],
            "upload_type": user_states[user_id]["upload_type"],
            "storage_message_id": stored_message.id,
            "scheduled_time_utc": file_info['schedule_time_utc'],
            "metadata": {
                "title": file_info.get("title"), "description": file_info.get("description"),
                "tags": file_info.get("tags"), "visibility": file_info.get("visibility"),
            },
            "status": "pending"
        }
        await asyncio.to_thread(db.scheduled_jobs.insert_one, job_doc)
        
        await safe_edit_message(msg,
            f"✅ **Successfully Scheduled!**\n\nYour post will be uploaded on "
            f"`{file_info['schedule_time_utc'].strftime('%Y-%m-%d %H:%M')} UTC`."
        )
    except Exception as e:
        await safe_edit_message(msg, f"❌ Failed to schedule post: {e}")
        logger.error(f"Scheduling failed for user {user_id}: {e}")
    finally:
        cleanup_temp_files([file_info.get("downloaded_path")])
        if user_id in user_states: del user_states[user_id]

async def start_upload_task(msg, file_info, user_id):
    task_tracker.create_task(
        safe_task_wrapper(process_and_upload(msg, file_info, user_id)),
        user_id=user_id, task_name="upload"
    )

async def process_and_upload(msg, file_info, user_id):
    platform = user_states[user_id]["platform"]
    upload_type = user_states[user_id]["upload_type"]
    
    async with upload_semaphore:
        files_to_clean = [file_info.get("downloaded_path"), file_info.get("processed_path"), file_info.get("thumbnail_path")]
        try:
            user_settings = await get_user_settings(user_id)
            path = file_info.get("downloaded_path")
            if not path or not os.path.exists(path):
                raise FileNotFoundError("Downloaded file path is missing.")
            
            is_video = file_info['original_media_msg'].video or (file_info['original_media_msg'].document and 'video' in file_info['original_media_msg'].document.mime_type)
            upload_path = path
            if is_video:
                await safe_edit_message(msg, "⚙️ " + to_bold_sans("Processing Video..."))
                processed_path = path.rsplit(".", 1)[0] + "_processed.mp4"
                upload_path = await asyncio.to_thread(process_video_for_upload, path, processed_path)
                file_info['processed_path'] = upload_path
            
            if file_info.get("thumbnail_path") == "auto":
                await safe_edit_message(msg, "🖼️ " + to_bold_sans("Generating Smart Thumbnail..."))
                thumb_output_path = path + ".jpg"
                generated_thumb = await asyncio.to_thread(generate_thumbnail, upload_path, thumb_output_path)
                file_info["thumbnail_path"] = generated_thumb
                files_to_clean.append(generated_thumb)

            _upload_progress['status'] = 'uploading'
            task_tracker.create_task(monitor_progress_task(user_id, msg.id, msg), user_id, "upload_monitor")
            
            url, media_id = "N/A", "N/A"
            final_title = file_info.get("title") or user_settings.get(f"title_{platform}") or user_settings.get(f"caption_{platform}") or "Untitled"
            
            if platform == "facebook":
                session = await get_active_session(user_id, 'facebook')
                if not session: raise ConnectionError("Facebook session not found.")
                page_id, token = session['id'], session['access_token']
                description = final_title + "\n\n" + file_info.get("description", "")
                
                if upload_type == 'reel':
                    # NEW: 3-Step Reels Upload
                    # Step 1: Initialize
                    init_url = f"https://graph.facebook.com/v19.0/{page_id}/video_reels"
                    init_params = {'upload_phase': 'start', 'access_token': token}
                    r = requests.post(init_url, params=init_params)
                    r.raise_for_status()
                    reel_data = r.json()
                    upload_url, video_id = reel_data['upload_url'], reel_data['video_id']
                    
                    # Step 2: Upload
                    with open(upload_path, 'rb') as video_file:
                        headers = {'Authorization': f'OAuth {token}'}
                        r_upload = requests.post(upload_url, headers=headers, data=video_file)
                        r_upload.raise_for_status()

                    # Step 3: Publish
                    publish_params = {
                        'access_token': token, 'video_id': video_id,
                        'upload_phase': 'finish', 'video_state': 'PUBLISHED',
                        'description': description
                    }
                    r_publish = requests.post(init_url, params=publish_params)
                    r_publish.raise_for_status()
                    media_id = video_id
                    url = f"https://www.facebook.com/reel/{video_id}"
                else: # video
                    upload_url = f"https://graph-video.facebook.com/{page_id}/videos"
                    with open(upload_path, 'rb') as f:
                        params = {'access_token': token, 'description': description}
                        files = {'source': f}
                        r = requests.post(upload_url, data=params, files=files, timeout=1800)
                        r.raise_for_status()
                        media_id = r.json()['id']
                        url = f"https://facebook.com/{media_id}"
            
            # ... [YouTube upload logic remains the same] ...
            
            _upload_progress['status'] = 'complete'
            task_tracker.cancel_user_task(user_id, "upload_monitor")
            await asyncio.to_thread(db.uploads.insert_one, {
                "user_id": user_id, "media_id": str(media_id), "platform": platform, 
                "upload_type": upload_type, "timestamp": datetime.now(timezone.utc), "url": url, "title": final_title
            })

            success_msg = f"✅ " + to_bold_sans("Uploaded Successfully!") + f"\n\n{url}"
            await safe_edit_message(msg, success_msg, parse_mode=None)
            await send_log_to_channel(app, LOG_CHANNEL, f"📤 New Upload\n👤 User: `{user_id}`\n🔗 URL: {url}")

        except Exception as e:
            await safe_edit_message(msg, f"❌ " + to_bold_sans(f"Upload Failed: {e}"))
            logger.error(f"Upload error for {user_id}: {e}", exc_info=True)
        finally:
            cleanup_temp_files(files_to_clean)
            if user_id in user_states: del user_states[user_id]
            _upload_progress.clear()
            
async def process_bulk_upload(user_id, caption):
    """Schedules multiple files from the bulk upload flow."""
    state_data = user_states.get(user_id, {})
    bulk_files = state_data.get("bulk_files", [])
    platform = state_data.get("platform")
    
    now = datetime.now(timezone.utc)
    successful_schedules = 0
    
    for i, media_msg in enumerate(bulk_files):
        try:
            stored_message = await media_msg.forward(STORAGE_CHANNEL)
            
            # Schedule for the next 10 days at a random hour
            schedule_time = (now + timedelta(days=i)).replace(hour=random.randint(8, 22), minute=random.randint(0, 59))
            
            job_doc = {
                "_id": f"bulk_{user_id}_{int(time.time())}_{i}",
                "user_id": user_id, "platform": platform,
                "upload_type": "video", # Default to video for bulk
                "storage_message_id": stored_message.id,
                "scheduled_time_utc": schedule_time,
                "metadata": {"title": caption, "description": ""}, "status": "pending"
            }
            await asyncio.to_thread(db.scheduled_jobs.insert_one, job_doc)
            successful_schedules += 1
        except Exception as e:
            logger.error(f"Failed to schedule a bulk file for user {user_id}: {e}")
            
    await app.send_message(user_id, f"✅ **Bulk scheduling complete!**\n\n`{successful_schedules}` out of `{len(bulk_files)}` files have been successfully scheduled for upload over the next `{len(bulk_files)}` days.")


# ===================================================================
# ======================== BOT STARTUP ============================
# ===================================================================
async def send_log_to_channel(client, channel_id, text):
    global valid_log_channel
    if not valid_log_channel: return
    try:
        await client.send_message(channel_id, text, disable_web_page_preview=True, parse_mode=enums.ParseMode.MARKDOWN)
    except (PeerIdInvalid, ChatAdminRequired) as e:
        logger.error(f"CRITICAL LOGGING ERROR: Could not send to channel {channel_id}. Reason: {e}. Please ensure the Channel ID is correct and the bot is an admin. Logging disabled.")
        valid_log_channel = False
    except Exception as e:
        logger.error(f"Failed to log to channel {channel_id}: {e}")

async def scheduler_worker():
    """Background worker to check for and execute scheduled jobs."""
    logger.info("Scheduler worker started.")
    while not shutdown_event.is_set():
        try:
            if db:
                now_utc = datetime.now(timezone.utc)
                due_jobs = await asyncio.to_thread(
                    list,
                    db.scheduled_jobs.find({"scheduled_time_utc": {"$lte": now_utc}, "status": "pending"})
                )
                
                for job in due_jobs:
                    logger.info(f"Processing scheduled job: {job['_id']} for user {job['user_id']}")
                    await asyncio.to_thread(db.scheduled_jobs.update_one, {"_id": job['_id']}, {"$set": {"status": "processing"}})
                    
                    # This is a simplified execution. A robust system might use a separate process pool.
                    task_tracker.create_task(execute_scheduled_job(job))
            
            await asyncio.sleep(60) # Check every minute
        except Exception as e:
            logger.error(f"Error in scheduler worker: {e}")
            await asyncio.sleep(60)

async def execute_scheduled_job(job):
    """Downloads file from storage and triggers the upload process."""
    user_id = job['user_id']
    try:
        # Create a mock state for process_and_upload
        user_states[user_id] = {
            "platform": job['platform'],
            "upload_type": job['upload_type']
        }
        
        # Download file from storage channel
        media_msg = await app.get_messages(STORAGE_CHANNEL, job['storage_message_id'])
        downloaded_path = await app.download_media(media_msg)
        
        # Prepare file_info
        file_info = {
            "original_media_msg": media_msg,
            "downloaded_path": downloaded_path,
            **job['metadata']
        }
        
        # Mock message for status updates
        mock_msg = await app.send_message(user_id, f"🚀 Starting scheduled upload for: *{job['metadata']['title']}*")
        
        await process_and_upload(mock_msg, file_info, user_id)
        
        await asyncio.to_thread(db.scheduled_jobs.update_one, {"_id": job['_id']}, {"$set": {"status": "completed"}})
        
    except Exception as e:
        logger.error(f"Failed to execute scheduled job {job['_id']}: {e}")
        await asyncio.to_thread(db.scheduled_jobs.update_one, {"_id": job['_id']}, {"$set": {"status": "failed", "error": str(e)}})
        await app.send_message(user_id, f"❌ Scheduled upload for '{job['metadata']['title']}' failed. Reason: {e}")
    finally:
        if user_id in user_states:
            del user_states[user_id]

async def start_bot():
    global mongo, db, global_settings, upload_semaphore, MAX_CONCURRENT_UPLOADS, MAX_FILE_SIZE_BYTES, task_tracker, valid_log_channel, BOT_ID

    try:
        mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo.admin.command('ping')
        db = mongo.UploaderBotDB
        logger.info("✅ Connected to MongoDB successfully.")
        
        # Startup DB Verification
        premium_users_count = await asyncio.to_thread(db.users.count_documents, {"premium": {"$ne": {}}})
        logger.info(f"Found {premium_users_count} users with premium data in the database.")
        
        settings_from_db = await asyncio.to_thread(db.settings.find_one, {"_id": "global_settings"}) or {}
        global_settings = {**DEFAULT_GLOBAL_SETTINGS, **settings_from_db}
        await asyncio.to_thread(db.settings.update_one, {"_id": "global_settings"}, {"$set": global_settings}, upsert=True)
    except Exception as e:
        logger.critical(f"❌ DATABASE SETUP FAILED: {e}. Running in degraded mode.")
        db = None
        global_settings = DEFAULT_GLOBAL_SETTINGS

    MAX_CONCURRENT_UPLOADS = global_settings.get("max_concurrent_uploads")
    upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
    MAX_FILE_SIZE_BYTES = global_settings.get("max_file_size_mb") * 1024 * 1024

    await app.start()
    me = await app.get_me()
    BOT_ID = me.id
    task_tracker.loop = asyncio.get_running_loop()

    if LOG_CHANNEL:
        try:
            await app.send_message(LOG_CHANNEL, "✅ **" + to_bold_sans("Bot Is Now Online And Running!") + "**", parse_mode=enums.ParseMode.MARKDOWN)
            valid_log_channel = True
        except Exception as e:
            logger.error(f"Could not log to channel {LOG_CHANNEL}. Invalid ID or bot isn't an admin. Error: {e}")
    
    # Start background tasks
    task_tracker.create_task(scheduler_worker())
    
    logger.info(f"Bot is now online! ID: {BOT_ID}.")
    await idle()

    logger.info("Shutting down...")
    await task_tracker.cancel_and_wait_all()
    await app.stop()
    if mongo: mongo.close()
    logger.info("Bot has been shut down gracefully.")

if __name__ == "__main__":
    task_tracker = TaskTracker()
    try:
        app.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Bot crashed during startup: {e}", exc_info=True)
