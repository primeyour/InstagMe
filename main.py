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
from functools import wraps, partial
import re
import time
import requests
import random
import string
from urllib.parse import urlparse, parse_qs
from collections import defaultdict


# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# MongoDB
from pymongo import MongoClient, DESCENDING
from pymongo.errors import OperationFailure
from bson import ObjectId


# Pyrogram (Telegram Bot)
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import UserNotParticipant, FloodWait, UserIsBlocked, PeerIdInvalid
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
# This dictionary will temporarily store a user's OAuth flow object, keyed by a unique state.
oauth_flows = {}
# Simple in-memory store for auth codes received by the local server.
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
LOG_CHANNEL_STR = os.getenv("LOG_CHANNEL_ID") # e.g., -1001234567890
MONGO_URI = os.getenv("MONGO_DB")
ADMIN_ID_STR = os.getenv("ADMIN_ID")
# NEW: Required for the secure YouTube OAuth flow
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8080")
PORT_STR = os.getenv("PORT", "8080") # Port for the local redirect server
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
STORAGE_CHANNEL = int(STORAGE_CHANNEL_STR)
PORT = int(PORT_STR)

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
        # Find a visually complex scene to use for the thumbnail
        # BUG FIX: Added -y flag
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
            # Fallback to a random frame in the first 30 seconds if scene detection fails
            # BUG FIX: Added -y flag
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
    """
    Ensures the video is in a web-compatible format (H.264 video, AAC audio, MP4 container).
    This function replaces the old `fix_video_format` and `needs_conversion`.
    """
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

    # If already compatible, just copy the file to avoid re-encoding
    if v_codec == 'h264' and a_codec == 'aac' and 'mp4' in metadata.get('format', {}).get('format_name', ''):
        logger.info(f"'{input_file}' is already compatible. No conversion needed.")
        if input_file != output_file:
            import shutil
            shutil.copy(input_file, output_file)
        return output_file

    logger.warning(f"'{input_file}' needs conversion (Video: {v_codec}, Audio: {a_codec}).")
    try:
        # BUG FIX: Added -y flag
        command = [
            'ffmpeg', '-y', '-i', input_file,
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '22', # Good quality H.264
            '-c:a', 'aac', '-b:a', '192k', # Standard AAC audio
            '-movflags', '+faststart', # For web streaming
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
    "max_file_size_mb": 1000, # New default
    "allow_multiple_logins": False, # New setting
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
BOT_ID = 0 # Will be fetched on startup

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
        # This is expected, so we log it as a warning
        logger.warning(f"Task {getattr(asyncio.current_task(), 'get_name', lambda: 'N/A')()} was cancelled.")
    except Exception:
        logger.exception(f"Unhandled exception in background task: {getattr(asyncio.current_task(), 'get_name', lambda: 'N/A')()}")


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
    # Sanitize text to handle potential emoji or special characters gracefully
    sanitized_text = text.encode('utf-8', 'surrogatepass').decode('utf-8')
    return ''.join(bold_sans_map.get(char, char) for char in sanitized_text)

# State dictionary to hold user states
user_states = {}

# NEW: Button Spam Protection
user_clicks = defaultdict(lambda: {'count': 0, 'time': 0})
SPAM_LIMIT = 10  # Clicks
SPAM_WINDOW = 10  # Seconds

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
    bulk_buttons = []

    if "facebook" in premium_platforms:
        fb_buttons.extend([
            KeyboardButton("📘 FB ᴩᴏꜱᴛ"),
            KeyboardButton("📘 FB ᴠɪᴅᴇᴏ"),
            KeyboardButton("📘 FB ʀᴇᴇʟꜱ"),
        ])
        bulk_buttons.append(KeyboardButton("Bulk Upload FB"))
    if "youtube" in premium_platforms:
        yt_buttons.extend([
            KeyboardButton("▶️ YT ᴠɪᴅᴇᴏ"),
            KeyboardButton("🟥 YT ꜱʜᴏʀᴛꜱ"),
        ])
        bulk_buttons.append(KeyboardButton("Bulk Upload YT"))
    
    if fb_buttons:
        buttons.insert(0, fb_buttons)
    if yt_buttons:
        # Insert YouTube buttons after Facebook buttons if they exist
        insert_index = 1 if fb_buttons else 0
        buttons.insert(insert_index, yt_buttons)
    if bulk_buttons:
        buttons.append(bulk_buttons)


    buttons.append([KeyboardButton("⭐ ᴩʀᴇᴍɪᴜᴍ"), KeyboardButton("/premiumdetails")])
    if is_admin(user_id):
        buttons.append([KeyboardButton("🛠 ᴀᴅᴍɪɴ ᴩᴀɴᴇʟ"), KeyboardButton("🔄 ʀᴇꜱᴛᴀʀᴛ ʙᴏᴛ")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, selective=True)

async def get_main_settings_markup(user_id):
    buttons = []
    user_settings = await get_user_settings(user_id)
    
    # NEW: Conditional Settings Menu
    if await is_premium_for_platform(user_id, "facebook"):
        buttons.append([InlineKeyboardButton("📘 ғᴀᴄᴇʙᴏᴏᴋ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="hub_settings_facebook")])
    if await is_premium_for_platform(user_id, "youtube"):
        buttons.append([InlineKeyboardButton("▶️ yᴏᴜᴛᴜʙᴇ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="hub_settings_youtube")])

    # NEW: Auto-Delete Toggle
    auto_delete_status = "✅" if user_settings.get("auto_delete_text", False) else "❌"
    buttons.append([InlineKeyboardButton(f"Auto-Delete My Text {auto_delete_status}", callback_data="toggle_auto_delete")])
    
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(buttons)


def get_facebook_settings_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 ᴅᴇғᴀᴜʟᴛ ᴄᴀᴩᴛɪᴏɴ", callback_data="set_caption_facebook")],
        [InlineKeyboardButton("👤 ᴍᴀɴᴀɢᴇ ғʙ ᴀᴄᴄᴏᴜɴᴛꜱ", callback_data="manage_fb_accounts")],
        [InlineKeyboardButton("🗓️ My FB Schedules", callback_data="manage_schedules_facebook")],
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="back_to_settings")]
    ])

def get_youtube_settings_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 ᴅᴇғᴀᴜʟᴛ ᴛɪᴛʟᴇ", callback_data="set_title_youtube")],
        [InlineKeyboardButton("📄 ᴅᴇғᴀᴜʟᴛ ᴅᴇꜱᴄʀɪᴩᴛɪᴏɴ", callback_data="set_description_youtube")],
        [InlineKeyboardButton("🏷️ ᴅᴇғᴀᴜʟᴛ ᴛᴀɢꜱ", callback_data="set_tags_youtube")],
        [InlineKeyboardButton("👁️ ᴅᴇғᴀᴜʟᴛ ᴠɪꜱɪʙɪʟɪᴛy", callback_data="set_visibility_youtube")],
        [InlineKeyboardButton("👤 ᴍᴀɴᴀɢᴇ yᴛ ᴀᴄᴄᴏᴜɴᴛꜱ", callback_data="manage_yt_accounts")],
        [InlineKeyboardButton("🗓️ My YT Schedules", callback_data="manage_schedules_youtube")],
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="back_to_settings")]
    ])

async def get_account_markup(user_id, platform, logged_in_accounts):
    buttons = []
    user_settings = await get_user_settings(user_id)
    active_account_id = user_settings.get(f"active_{platform}_id")

    for acc_id, acc_name in logged_in_accounts.items():
        emoji = "✅" if str(active_account_id) == str(acc_id) else "⬜"
        buttons.append([InlineKeyboardButton(f"{emoji} {acc_name}", callback_data=f"select_acc_{platform}_{acc_id}")])
    
    if active_account_id:
        buttons.append([InlineKeyboardButton("❌ ʟᴏɢᴏᴜᴛ ᴀᴄᴛɪᴠᴇ ᴀᴄᴄᴏᴜɴᴛ", callback_data=f"confirm_logout_{platform}_{active_account_id}")])

    buttons.append([InlineKeyboardButton("➕ ᴀᴅᴅ ɴᴇᴡ ᴀᴄᴄᴏᴜɴᴛ", callback_data=f"add_account_{platform}")])
    buttons.append([InlineKeyboardButton(f"🔙 ʙᴀᴄᴋ ᴛᴏ {platform.upper()} ꜱᴇᴛᴛɪɴɢꜱ", callback_data=f"hub_settings_{platform}")])
    return InlineKeyboardMarkup(buttons)

def get_logout_confirm_markup(platform, account_id, account_name):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ Yes, Logout {account_name}", callback_data=f"logout_acc_{platform}_{account_id}")],
        [InlineKeyboardButton("❌ No, Cancel", callback_data=f"manage_{'fb' if platform == 'facebook' else 'yt'}_accounts")]
    ])

admin_markup = InlineKeyboardMarkup([
    [InlineKeyboardButton("👥 ᴜꜱᴇʀꜱ ʟɪꜱᴛ", callback_data="users_list"), InlineKeyboardButton("👤 ᴜꜱᴇʀ ᴅᴇᴛᴀɪʟꜱ", callback_data="admin_user_details")],
    [InlineKeyboardButton("➕ ᴍᴀɴᴀɢᴇ ᴩʀᴇᴍɪᴜᴍ", callback_data="manage_premium")],
    [InlineKeyboardButton("📢 ʙʀᴏᴀᴅᴄᴀꜱᴛ", callback_data="broadcast_message")],
    [InlineKeyboardButton("⚙️ ɢʟᴏʙᴀʟ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="global_settings_panel")],
    [InlineKeyboardButton("📊 ꜱᴛᴀᴛꜱ ᴩᴀɴᴇʟ", callback_data="admin_stats_panel")],
    [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
])

def get_admin_global_settings_markup():
    event_status = "ON" if global_settings.get("special_event_toggle") else "OFF"
    multiple_logins_status = "Allowed" if global_settings.get("allow_multiple_logins") else "Blocked"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📢 Special Event ({event_status})", callback_data="toggle_special_event")],
        [InlineKeyboardButton("✏️ Set Event Title", callback_data="set_event_title")],
        [InlineKeyboardButton("💬 Set Event Message", callback_data="set_event_message")],
        [InlineKeyboardButton("⏫ Set Max Uploads", callback_data="set_max_uploads")],
        [InlineKeyboardButton("🗂️ Set Max File Size (MB)", callback_data="set_max_file_size")],
        [InlineKeyboardButton(f"👥 Multiple Logins ({multiple_logins_status})", callback_data="toggle_multiple_logins")],
        [InlineKeyboardButton("🗑️ Reset All Stats", callback_data="reset_stats")],
        [InlineKeyboardButton("💻 Show System Stats", callback_data="show_system_stats")],
        [InlineKeyboardButton("💰 Payment Settings", callback_data="payment_settings_panel")],
        [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]
    ])

payment_settings_markup = InlineKeyboardMarkup([
    [InlineKeyboardButton("🆕 ᴄʀᴇᴀᴛᴇ ᴩᴀyᴍᴇɴᴛ ʙᴜᴛᴛᴏɴ", callback_data="create_custom_payment_button")],
    [InlineKeyboardButton("✍️ Set Instructions", callback_data="set_payment_instructions")],
    [InlineKeyboardButton("ɢᴏᴏɢʟᴇ ᴩʟᴀy ǫʀ ᴄᴏᴅᴇ", callback_data="set_payment_google_play_qr")],
    [InlineKeyboardButton("ᴜᴩɪ", callback_data="set_payment_upi")],
    [InlineKeyboardButton("ᴜꜱᴅᴛ", callback_data="set_payment_usdt")],
    [InlineKeyboardButton("ʙᴛᴄ", callback_data="set_payment_btc")],
    [InlineKeyboardButton("ᴏᴛʜᴇʀꜱ", callback_data="set_payment_others")],
    [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ɢʟᴏʙᴀʟ", callback_data="global_settings_panel")]
])

def get_platform_selection_markup(user_id, current_selection=None):
    if current_selection is None:
        current_selection = {}
    buttons = []
    for platform in PREMIUM_PLATFORMS:
        emoji = "✅" if current_selection.get(platform) else "⬜"
        buttons.append([InlineKeyboardButton(f"{emoji} {platform.capitalize()}", callback_data=f"select_platform_{platform}")])
    buttons.append([InlineKeyboardButton("➡️ ᴄᴏɴᴛɪɴᴜᴇ ᴛᴏ ᴩʟᴀɴꜱ", callback_data="confirm_platform_selection")])
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ", callback_data="admin_panel")])
    return InlineKeyboardMarkup(buttons)

def get_premium_plan_markup(user_id):
    buttons = []
    # Don't show trial plan for manual granting or buying
    for key, value in PREMIUM_PLANS.items():
        if key != "6_hour_trial":
            buttons.append([InlineKeyboardButton(f"{key.replace('_', ' ').title()}", callback_data=f"show_plan_details_{key}")])
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(buttons)

def get_premium_details_markup(plan_key, is_admin_flow=False):
    plan_details = PREMIUM_PLANS[plan_key]
    buttons = []
    if is_admin_flow:
        buttons.append([InlineKeyboardButton(f"✅ Grant this Plan", callback_data=f"grant_plan_{plan_key}")])
    else:
        price_string = plan_details['price']
        buttons.append([InlineKeyboardButton(f"💰 ʙᴜy ɴᴏᴡ ({price_string})", callback_data="buy_now")])
        buttons.append([InlineKeyboardButton("➡️ ᴄʜᴇᴄᴋ ᴩᴀyᴍᴇɴᴛ ᴍᴇᴛʜᴏᴅꜱ", callback_data="show_payment_methods")])
    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴩʟᴀɴꜱ", callback_data="back_to_premium_plans")])
    return InlineKeyboardMarkup(buttons)

def get_payment_methods_markup():
    payment_buttons = []
    settings = global_settings.get("payment_settings", {})
    
    if settings.get("google_play_qr_file_id"):
        payment_buttons.append([InlineKeyboardButton("ɢᴏᴏɢʟᴇ ᴩʟᴀy ǫʀ ᴄᴏᴅᴇ", callback_data="show_payment_qr_google_play")])
    if settings.get("upi"):
        payment_buttons.append([InlineKeyboardButton("ᴜᴩɪ", callback_data="show_payment_details_upi")])
    if settings.get("usdt"):
        payment_buttons.append([InlineKeyboardButton("ᴜꜱᴅᴛ", callback_data="show_payment_details_usdt")])
    if settings.get("btc"):
        payment_buttons.append([InlineKeyboardButton("ʙᴛᴄ", callback_data="show_payment_details_btc")])
    if settings.get("others"):
        payment_buttons.append([InlineKeyboardButton("ᴏᴛʜᴇʀꜱ", callback_data="show_payment_details_others")])

    for btn_name in settings.get("custom_buttons", {}):
        payment_buttons.append([InlineKeyboardButton(btn_name.upper(), callback_data=f"show_custom_payment_{btn_name}")])

    payment_buttons.append([InlineKeyboardButton("🧾 I've Paid / Submit Proof", callback_data="submit_payment_proof")])
    payment_buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴩʀᴇᴍɪᴜᴍ ᴩʟᴀɴꜱ", callback_data="back_to_premium_plans")])
    return InlineKeyboardMarkup(payment_buttons)

def get_progress_markup():
    # Buttons now use normal text as requested
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_upload")]
    ])

# NEW: Unified markup for the entire upload flow
def get_upload_flow_markup(platform, step):
    buttons = []
    if step == "thumbnail":
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

# ===================================================================
# ====================== HELPER FUNCTIONS ===========================
# ===================================================================

def is_admin(user_id):
    return user_id == ADMIN_ID

async def _get_user_data(user_id):
    # BUG FIX: Changed `if db:` to `if db is not None:`
    if db is None:
        # Return a default structure if DB is offline
        return {"_id": user_id, "premium": {}}
    return await asyncio.to_thread(db.users.find_one, {"_id": user_id})

async def _save_user_data(user_id, data_to_update):
    # BUG FIX: Changed `if db:` to `if db is not None:`
    if db is None:
        logger.warning(f"DB not connected. Skipping save for user {user_id}.")
        return
    # Sanitize data before saving to prevent MongoDB errors
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
    # BUG FIX: Changed `if db:` to `if db is not None:`
    if db is None:
        logger.warning(f"DB not connected. Skipping save for global setting '{key}'.")
        return
    await asyncio.to_thread(db.settings.update_one, {"_id": "global_settings"}, {"$set": {key: value}}, upsert=True)

async def is_premium_for_platform(user_id, platform):
    if user_id == ADMIN_ID:
        return True
    
    # BUG FIX: Changed `if db:` to `if db is not None:`
    if db is None:
        return False

    user = await _get_user_data(user_id)
    if not user:
        return False

    platform_premium = user.get("premium", {}).get(platform, {})
    
    if not platform_premium or platform_premium.get("status") == "expired":
        return False
        
    premium_type = platform_premium.get("type")
    premium_until = platform_premium.get("until")

    # DATETIME FIX: Make naive datetimes from DB aware before comparing
    if premium_until and isinstance(premium_until, datetime) and premium_until.tzinfo is None:
        premium_until = premium_until.replace(tzinfo=timezone.utc)

    if premium_type == "lifetime":
        return True

    # Ensure premium_until is a datetime object before comparison
    if premium_until and isinstance(premium_until, datetime) and premium_until > datetime.now(timezone.utc):
        return True

    # If premium has expired, update the status in the DB
    if premium_type and premium_until and premium_until <= datetime.now(timezone.utc):
        await asyncio.to_thread(
            db.users.update_one,
            {"_id": user_id},
            {"$set": {f"premium.{platform}.status": "expired"}}
        )
        logger.info(f"Premium for {platform} expired for user {user_id}. Status updated in DB.")
        return False

    return False

async def save_platform_session(user_id, platform, session_data):
    # BUG FIX: Changed `if db:` to `if db is not None:`
    if db is None: return
    
    allow_multiple = global_settings.get("allow_multiple_logins", False)
    if not allow_multiple:
        # Delete all other sessions for this platform to enforce single login
        await asyncio.to_thread(db.sessions.delete_many, {"user_id": user_id, "platform": platform})

    account_id = session_data['id']
    await asyncio.to_thread(
        db.sessions.update_one,
        {"user_id": user_id, "platform": platform, "account_id": account_id},
        {"$set": {
            "session_data": session_data,
            "logged_in_at": datetime.now(timezone.utc)
        }},
        upsert=True
    )

async def load_platform_sessions(user_id, platform):
    # BUG FIX: Changed `if db:` to `if db is not None:`
    if db is None: return []
    sessions = await asyncio.to_thread(list, db.sessions.find({"user_id": user_id, "platform": platform}))
    return sessions

async def get_active_session(user_id, platform):
    user_settings = await get_user_settings(user_id)
    active_id = user_settings.get(f"active_{platform}_id")
    # BUG FIX: Changed `if not active_id or not db:` to `if not active_id or db is None:`
    if not active_id or db is None:
        return None
    
    session = await asyncio.to_thread(db.sessions.find_one, {"user_id": user_id, "platform": platform, "account_id": active_id})
    return session.get("session_data") if session else None

async def delete_platform_session(user_id, platform, account_id):
    # BUG FIX: Changed `if db:` to `if db is not None:`
    if db is None: return
    await asyncio.to_thread(db.sessions.delete_one, {"user_id": user_id, "platform": platform, "account_id": account_id})

async def save_user_settings(user_id, settings):
    # BUG FIX: Changed `if db:` to `if db is not None:`
    if db is None:
        logger.warning(f"DB not connected. Skipping user settings save for user {user_id}.")
        return
    await asyncio.to_thread(
        db.settings.update_one,
        {"_id": user_id},
        {"$set": settings},
        upsert=True
    )

async def get_user_settings(user_id):
    settings = {}
    if db is not None:
        settings = await asyncio.to_thread(db.settings.find_one, {"_id": user_id}) or {}
    
    # Set default values for all expected keys to avoid KeyErrors
    # Facebook defaults
    settings.setdefault("caption_facebook", "")
    settings.setdefault("active_facebook_id", None)
    # YouTube defaults
    settings.setdefault("title_youtube", "")
    settings.setdefault("description_youtube", "")
    settings.setdefault("tags_youtube", "")
    settings.setdefault("visibility_youtube", "private")
    settings.setdefault("active_youtube_id", None)
    # New settings
    settings.setdefault("auto_delete_text", False)
    
    return settings

async def safe_edit_message(message, text, reply_markup=None, parse_mode=enums.ParseMode.MARKDOWN):
    try:
        if not message:
            logger.warning("safe_edit_message called with a None message object.")
            return
        # Avoid editing if the message content is identical to prevent API errors
        current_text = getattr(message, 'text', '') or getattr(message, 'caption', '')
        if current_text and hasattr(current_text, 'strip') and current_text.strip() == text.strip() and message.reply_markup == reply_markup:
            return
        await message.edit_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
    except Exception as e:
        # Ignore "MESSAGE_NOT_MODIFIED" as it's not a critical error
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            logger.warning(f"Couldn't edit message: {e}")

async def safe_reply(message, text, **kwargs):
    """A helper to reply to a message, safely handling potential errors."""
    try:
        return await message.reply(text, **kwargs)
    except Exception as e:
        logger.error(f"Failed to reply to message {message.id}: {e}")
        # As a fallback, try sending a new message to the chat
        try:
            return await app.send_message(message.chat.id, text, **kwargs)
        except Exception as e2:
            logger.error(f"Fallback send_message also failed for chat {message.chat.id}: {e2}")
    return None


async def restart_bot(msg):
    restart_msg_log = (
        "🔄 **Bot Restart Initiated (Graceful)**\n\n"
        f"👤 **By**: {msg.from_user.mention} (ID: `{msg.from_user.id}`)"
    )
    logger.info(f"User {msg.from_user.id} initiated graceful restart.")
    await send_log_to_channel(app, LOG_CHANNEL, restart_msg_log)
    await msg.reply(
        to_bold_sans("Graceful Restart Initiated...") + "\n\n"
        "The bot will shut down cleanly. If running under a process manager "
        "(like Docker, Koyeb, or systemd), it will restart automatically."
    )
    shutdown_event.set()

_progress_updates = {}
_upload_progress = {} # For resumable API uploads

# Thread-safe progress callback for Pyrogram downloads
def download_progress_callback(current, total, ud_type, msg_id, chat_id, start_time, last_update_time):
    now = time.time()
    if now - last_update_time[0] < 2 and current != total:
        return
    last_update_time[0] = now
    
    # Use a lock to prevent race conditions when updating the shared dictionary
    with threading.Lock():
        _progress_updates[(chat_id, msg_id)] = {
            "current": current, "total": total, "ud_type": ud_type, "start_time": start_time, "now": now
        }

# Callback for YouTube resumable upload progress
def youtube_upload_progress(response):
    if response:
        # This function will be called by the Google API client library
        # `response` will be the video resource if the upload is complete.
        # It's None otherwise. We use this to signal completion.
        _upload_progress['status'] = 'complete'
    else:
        # You can access status.progress() if you need the percentage,
        # but for simplicity, we just show a generic "uploading" message.
        pass

async def monitor_progress_task(chat_id, msg_id, progress_msg):
    """Monitors and updates the progress of a download or upload."""
    try:
        while True:
            await asyncio.sleep(2)
            # Check for download progress
            with threading.Lock():
                update_data = _progress_updates.get((chat_id, msg_id))
            
            if update_data:
                current, total, ud_type, start_time, now = (
                    update_data['current'], update_data['total'], update_data['ud_type'],
                    update_data['start_time'], update_data['now']
                )
                percentage = current * 100 / total
                speed = current / (now - start_time) if (now - start_time) > 0 else 0
                eta_seconds = (total - current) / speed if speed > 0 else 0
                eta = timedelta(seconds=int(eta_seconds))
                progress_bar = f"[{'█' * int(percentage / 5)}{' ' * (20 - int(percentage / 5))}]"
                progress_text = (
                    f"{to_bold_sans(f'{ud_type} Progress')}: `{progress_bar}`\n"
                    f"📊 **Percentage**: `{percentage:.2f}%`\n"
                    f"✅ **Done**: `{current / (1024 * 1024):.2f}` MB / `{total / (1024 * 1024):.2f}` MB\n"
                    f"🚀 **Speed**: `{speed / (1024 * 1024):.2f}` MB/s\n"
                    f"⏳ **ETA**: `{eta}`"
                )
                await safe_edit_message(
                    progress_msg, progress_text,
                    reply_markup=get_progress_markup(),
                    parse_mode=None # Use None for better compatibility with special characters
                )
                
                if current == total:
                    with threading.Lock():
                        _progress_updates.pop((chat_id, msg_id), None)
                    break # Exit monitor loop once download is complete

            # Check for API upload progress (simplified)
            elif _upload_progress.get('status') == 'uploading':
                await safe_edit_message(
                    progress_msg,
                    "⬆️ " + to_bold_sans("Uploading To API... Please Wait."),
                    reply_markup=get_progress_markup()
                )
            elif _upload_progress.get('status') == 'complete':
                _upload_progress.clear()
                break

    except asyncio.CancelledError:
        logger.info(f"Progress monitor task for msg {msg_id} was cancelled.")

def cleanup_temp_files(files_to_delete):
    for file_path in files_to_delete:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")

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

def rate_limit_callbacks(func):
    """A decorator to prevent button spam."""
    @wraps(func)
    async def wrapper(client, query, *args, **kwargs):
        user_id = query.from_user.id
        now = time.time()
        
        # Reset counter if the window has passed
        if now - user_clicks[user_id]['time'] > SPAM_WINDOW:
            user_clicks[user_id]['count'] = 0
            user_clicks[user_id]['time'] = now

        user_clicks[user_id]['count'] += 1

        if user_clicks[user_id]['count'] > SPAM_LIMIT:
            await query.answer("Please don't click so fast!", show_alert=True)
            return

        return await func(client, query, *args, **kwargs)
    return wrapper


# ===================================================================
# ======================== COMMAND HANDLERS =========================
# ===================================================================

@app.on_message(filters.command("start"))
async def start(_, msg):
    user_id = msg.from_user.id
    user_first_name = msg.from_user.first_name or "there"
    
    premium_platforms = []
    if await is_premium_for_platform(user_id, "facebook") or is_admin(user_id):
        premium_platforms.append("facebook")
    if await is_premium_for_platform(user_id, "youtube") or is_admin(user_id):
        premium_platforms.append("youtube")

    if is_admin(user_id):
        welcome_msg = to_bold_sans("Welcome To The Direct Upload Bot!") + "\n\n"
        welcome_msg += "🛠️ " + to_bold_sans("You Have Admin Privileges.")
        await msg.reply(welcome_msg, reply_markup=get_main_keyboard(user_id, ["facebook", "youtube"]))
        return

    user = await _get_user_data(user_id)
    is_new_user = not user or "added_by" not in user
    if is_new_user:
        await _save_user_data(user_id, {
            "_id": user_id, "premium": {}, "added_by": "self_start", 
            "added_at": datetime.now(timezone.utc), "username": msg.from_user.username
        })
        logger.info(f"New user {user_id} added to database via start command.")
        await send_log_to_channel(app, LOG_CHANNEL, f"🌟 New user started bot: `{user_id}` (`{msg.from_user.username or 'N/A'}`)")
        welcome_msg = (
            f"👋 **Hi {user_first_name}!**\n\n"
            + to_bold_sans("This Bot Lets You Upload To Facebook & YouTube Directly From Telegram.") + "\n\n"
            + to_bold_sans("To Get A Taste Of The Premium Features, You Can Activate A Free 6-hour Trial!")
        )
        trial_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Activate FREE FB Trial", callback_data="activate_trial_facebook")],
            [InlineKeyboardButton("✅ Activate FREE YT Trial", callback_data="activate_trial_youtube")],
            [InlineKeyboardButton("➡️ View premium plans", callback_data="buypypremium")]
        ])
        await msg.reply(welcome_msg, reply_markup=trial_markup, parse_mode=enums.ParseMode.MARKDOWN)
        return
    else:
        # Update user info on every start
        await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc), "username": msg.from_user.username})

    event_toggle = global_settings.get("special_event_toggle", False)
    if event_toggle:
        event_title = global_settings.get("special_event_title", "🎉 Special Event!")
        event_message = global_settings.get("special_event_message", "Enjoy our special event features!")
        event_text = f"**{event_title}**\n\n{event_message}"
        await msg.reply(event_text, reply_markup=get_main_keyboard(user_id, premium_platforms), parse_mode=enums.ParseMode.MARKDOWN)
        return

    welcome_msg = to_bold_sans("Welcome Back To Telegram ➜ Direct Uploader") + "\n\n"
    premium_details_text = ""
    has_any_premium = False
    for platform in ["facebook", "youtube"]:
        if await is_premium_for_platform(user_id, platform):
            has_any_premium = True
            p_data = user.get("premium", {}).get(platform, {})
            p_expiry = p_data.get("until")

            # DATETIME FIX: Make naive datetimes from DB aware before comparing
            if p_expiry and isinstance(p_expiry, datetime) and p_expiry.tzinfo is None:
                p_expiry = p_expiry.replace(tzinfo=timezone.utc)

            if p_expiry:
                remaining = p_expiry - datetime.now(timezone.utc)
                if remaining.total_seconds() > 0:
                    premium_details_text += f"⭐ {platform.capitalize()} premium expires in: `{remaining.days} days, {remaining.seconds // 3600} hours`.\n"
            elif p_data.get("type") == "lifetime":
                premium_details_text += f"⭐ {platform.capitalize()} premium: **Lifetime**\n"


    if not has_any_premium:
        premium_details_text = (
            "🔥 **Key Features:**\n"
            "✅ Direct Login (No passwords needed for YouTube)\n"
            "✅ Ultra-fast uploading & High Quality\n"
            "✅ No file size limit & unlimited uploads\n"
            "✅ Facebook & YouTube Support\n\n"
            "👤 Contact Admin to get premium\n"
            f"🆔 Your ID: `{user_id}`"
        )
    welcome_msg += premium_details_text
    await msg.reply(welcome_msg, reply_markup=get_main_keyboard(user_id, premium_platforms), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("restart") & filters.user(ADMIN_ID))
async def restart_cmd(_, msg):
    await restart_bot(msg)

# NEW FACEBOOK LOGIN FLOW
@app.on_message(filters.command(["fblogin", "flogin"]))
@with_user_lock
async def facebook_login_cmd_new(_, msg):
    user_id = msg.from_user.id
    if not await is_premium_for_platform(user_id, "facebook"):
        return await msg.reply("❌ " + to_bold_sans("Facebook Premium Access Is Required. Use ") + "`/premiumplan`" + to_bold_sans(" To Upgrade."))
    
    user_states[user_id] = {"action": "waiting_for_fb_app_secret", "platform": "facebook"}
    await msg.reply("🔑 " + to_bold_sans("Please Enter Your Facebook App Secret."))

# NEW YOUTUBE LOGIN FLOW
@app.on_message(filters.command(["ytlogin", "ylogin"]))
@with_user_lock
async def youtube_login_cmd_new(_, msg):
    user_id = msg.from_user.id
    if not await is_premium_for_platform(user_id, "youtube"):
        return await msg.reply("❌ " + to_bold_sans("YouTube Premium Access Is Required. Use ") + "`/premiumplan`" + to_bold_sans(" To Upgrade."))
        
    user_states[user_id] = {"action": "waiting_for_yt_client_id", "platform": "youtube"}
    await msg.reply("🔑 " + to_bold_sans("Please Enter Your Google OAuth `client_id`."))


@app.on_message(filters.command(["buypypremium", "premiumplan"]))
@app.on_message(filters.regex("⭐ ᴩʀᴇᴍɪᴜᴍ"))
async def show_premium_options(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    premium_plans_text = (
        "⭐ " + to_bold_sans("Upgrade To Premium!") + " ⭐\n\n"
        + to_bold_sans("Unlock Full Features And Upload Unlimited Content Without Restrictions.") + "\n\n"
        "**Available Plans:**"
    )
    await msg.reply(premium_plans_text, reply_markup=get_premium_plan_markup(user_id), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("premiumdetails"))
async def premium_details_cmd(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    user = await _get_user_data(user_id)
    if not user:
        return await msg.reply(to_bold_sans("You Are Not Registered. Please Use /start."))
    if is_admin(user_id):
        return await msg.reply("👑 " + to_bold_sans("You Are The Admin. You Have Permanent Full Access!"), parse_mode=enums.ParseMode.MARKDOWN)

    status_text = "⭐ " + to_bold_sans("Your Premium Status:") + "\n\n"
    has_premium_any = False
    for platform in PREMIUM_PLATFORMS:
        if await is_premium_for_platform(user_id, platform):
            has_premium_any = True
            platform_premium = user.get("premium", {}).get(platform, {})
            premium_type = platform_premium.get("type")
            premium_until = platform_premium.get("until")

            # DATETIME FIX: Make naive datetimes from DB aware before comparing
            if premium_until and isinstance(premium_until, datetime) and premium_until.tzinfo is None:
                premium_until = premium_until.replace(tzinfo=timezone.utc)

            status_text += f"**{platform.capitalize()} Premium:** "
            if premium_type == "lifetime":
                status_text += "🎉 **Lifetime!**\n"
            elif premium_until:
                remaining_time = premium_until - datetime.now(timezone.utc)
                if remaining_time.total_seconds() > 0:
                    days, rem = divmod(remaining_time.total_seconds(), 86400)
                    hours, rem = divmod(rem, 3600)
                    minutes, _ = divmod(rem, 60)
                    status_text += (
                        f"`{premium_type.replace('_', ' ').title()}` expires on: "
                        f"`{premium_until.strftime('%Y-%m-%d %H:%M')} UTC`\n"
                        f"Time Remaining: `{int(days)}d, {int(hours)}h, {int(minutes)}m`\n"
                    )
            status_text += "\n"
    
    if not has_premium_any:
        status_text = "😔 " + to_bold_sans("You Have No Active Premium.") + "\n\n" + "Contact **Admin** to buy a plan."

    await msg.reply(status_text, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("skip"))
@with_user_lock
async def handle_skip_command(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    if not state_data: return

    # This command allows skipping optional steps in the upload flow
    action = state_data.get('action')
    if action == 'waiting_for_title':
        state_data["file_info"]["title"] = None # Signal to use default
        await process_upload_step(msg) # Move to next step
    elif action == 'waiting_for_description':
        state_data["file_info"]["description"] = "" # Use empty description
        await process_upload_step(msg)
    elif action == 'waiting_for_tags':
        state_data["file_info"]["tags"] = "" # Use empty tags
        await process_upload_step(msg)
        
# NEW: Leaderboard command
@app.on_message(filters.command("leaderboard"))
async def leaderboard_cmd(_, msg):
    # BUG FIX: Changed `if db:` to `if db is not None:`
    if db is None:
        return await msg.reply("⚠️ " + to_bold_sans("Database is currently unavailable."))

    pipeline = [
        {"$group": {"_id": "$user_id", "upload_count": {"$sum": 1}}},
        {"$sort": {"upload_count": -1}},
        {"$limit": 5}
    ]
    
    try:
        leaderboard_data = await asyncio.to_thread(list, db.uploads.aggregate(pipeline))
        
        if not leaderboard_data:
            return await msg.reply("🏆 " + to_bold_sans("Leaderboard is empty. No uploads recorded yet!"))
            
        leaderboard_text = "🏆 **" + to_bold_sans("Top 5 Uploaders") + "** 🏆\n\n"
        
        for i, user in enumerate(leaderboard_data):
            user_id = user['_id']
            upload_count = user['upload_count']
            
            try:
                user_info = await app.get_users(user_id)
                user_name = user_info.first_name
            except Exception:
                user_name = f"User ID: {user_id}"

            leaderboard_text += f"**{i+1}.** {user_name} - `{upload_count}` uploads\n"
            
        await msg.reply(leaderboard_text, parse_mode=enums.ParseMode.MARKDOWN)

    except OperationFailure as e:
        logger.error(f"Leaderboard aggregation failed: {e}")
        await msg.reply("⚠️ " + to_bold_sans("Could not fetch the leaderboard."))

@app.on_message(filters.command("finish") & filters.private)
async def finish_bulk_upload_cmd(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    if not state_data or state_data.get("action") != "waiting_for_bulk_media":
        return
    
    media_list = state_data.get("bulk_media", [])
    if not media_list:
        return await msg.reply("You haven't sent any media for bulk upload. Please send up to 10 files first.")
    
    state_data["action"] = "waiting_for_bulk_caption"
    await msg.reply(f"✅ Received {len(media_list)} files. Now, please send a single caption/title to be used for all of them.")



# ===================================================================
# ======================== REGEX HANDLERS ===========================
# ===================================================================

@app.on_message(filters.regex("🔄 ʀᴇꜱᴛᴀʀᴛ ʙᴏᴛ") & filters.user(ADMIN_ID))
async def restart_button_handler(_, msg):
    await restart_bot(msg)

@app.on_message(filters.regex("⚙️ ꜱᴇᴛᴛɪɴɢꜱ"))
async def settings_menu(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    
    has_premium_any = await is_premium_for_platform(user_id, "facebook") or \
                      await is_premium_for_platform(user_id, "youtube")
    
    if not is_admin(user_id) and not has_premium_any:
        return await msg.reply("❌ " + to_bold_sans("Premium Required To Access Settings. Use ") + "`/premiumplan`" + to_bold_sans(" To Upgrade."))
    
    await msg.reply(
        "⚙️ " + to_bold_sans("Configure Your Upload Settings:"),
        reply_markup=await get_main_settings_markup(user_id)
    )

@app.on_message(filters.regex("🛠 ᴀᴅᴍɪɴ ᴩᴀɴᴇʟ") & filters.user(ADMIN_ID))
async def admin_panel_button_handler(_, msg):
    await msg.reply(
        "🛠 " + to_bold_sans("Welcome To The Admin Panel!"),
        reply_markup=admin_markup,
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_message(filters.regex("📊 ꜱᴛᴀᴛꜱ"))
@with_user_lock
async def show_stats(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    # BUG FIX: Changed `if not db:` to `if db is None:`
    if db is None: return await msg.reply("⚠️ " + to_bold_sans("Database Is Currently Unavailable."))
    
    # Show personal stats for regular users
    if not is_admin(user_id):
        user_uploads = await asyncio.to_thread(db.uploads.count_documents, {'user_id': user_id})
        stats_text = (
            f"📊 **{to_bold_sans('Your Statistics:')}**\n\n"
            f"📈 **Total Uploads:** `{user_uploads}`\n"
        )
        for p in PREMIUM_PLATFORMS:
            platform_uploads = await asyncio.to_thread(db.uploads.count_documents, {'user_id': user_id, 'platform': p})
            stats_text += f"    - {p.capitalize()}: `{platform_uploads}`\n"
        await msg.reply(stats_text, parse_mode=enums.ParseMode.MARKDOWN)
        return

    # --- Admin Stats ---
    total_users = await asyncio.to_thread(db.users.count_documents, {})
    
    # Efficiently count premium users with an aggregation pipeline
    pipeline = [
        {"$project": {
            "is_premium": {"$or": [
                {"$or": [
                    {"$eq": [f"$premium.{p}.type", "lifetime"]},
                    {"$gt": [f"$premium.{p}.until", datetime.now(timezone.utc)]}
                ]} for p in PREMIUM_PLATFORMS
            ]},
            "platforms": {p: {"$or": [
                {"$eq": [f"$premium.{p}.type", "lifetime"]},
                {"$gt": [f"$premium.{p}.until", datetime.now(timezone.utc)]}
            ]} for p in PREMIUM_PLATFORMS}
        }},
        {"$group": {
            "_id": None,
            "total_premium": {"$sum": {"$cond": ["$is_premium", 1, 0]}},
            **{f"{p}_premium": {"$sum": {"$cond": [f"$platforms.{p}", 1, 0]}} for p in PREMIUM_PLATFORMS}
        }}
    ]
    
    try:
        result = await asyncio.to_thread(list, db.users.aggregate(pipeline))
    except OperationFailure as e:
        logger.error(f"Stats aggregation failed: {e}")
        return await msg.reply("⚠️ " + to_bold_sans("Could Not Fetch Bot Statistics."))

    total_premium_users = 0
    premium_counts = {p: 0 for p in PREMIUM_PLATFORMS}
    if result:
        total_premium_users = result[0].get('total_premium', 0)
        for p in PREMIUM_PLATFORMS:
            premium_counts[p] = result[0].get(f'{p}_premium', 0)
            
    total_uploads = await asyncio.to_thread(db.uploads.count_documents, {})
    
    stats_text = (
        f"📊 **{to_bold_sans('Bot Statistics:')}**\n\n"
        f"**Users**\n"
        f"👥 Total Users: `{total_users}`\n"
        f"⭐ Premium Users: `{total_premium_users}`\n"
    )
    for p in PREMIUM_PLATFORMS:
        stats_text += f"    - {p.capitalize()} Premium: `{premium_counts[p]}`\n"
        
    stats_text += (
        f"\n**Uploads**\n"
        f"📈 Total Uploads: `{total_uploads}`\n"
    )
    for p in PREMIUM_PLATFORMS:
        stats_text += f"    - {p.capitalize()}: `{await asyncio.to_thread(db.uploads.count_documents, {'platform': p})}`\n"

    stats_text += f"\n**Events**\n📢 Special Event Status: `{'ON' if global_settings.get('special_event_toggle') else 'OFF'}`"
    
    # This check is needed because the function can be called from a callback, which might not have a .reply method
    if hasattr(msg, 'reply_markup'):
        await msg.reply(stats_text, parse_mode=enums.ParseMode.MARKDOWN)
    else: # It's a mock message from a callback
        await msg.reply(stats_text, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]))


@app.on_message(filters.regex("^(📘 FB ᴩᴏꜱᴛ|📘 FB ᴠɪᴅᴇᴏ|📘 FB ʀᴇᴇʟꜱ|▶️ YT ᴠɪᴅᴇᴏ|🟥 YT ꜱʜᴏʀᴛꜱ|Bulk Upload FB|Bulk Upload YT)"))
@with_user_lock
async def initiate_upload(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})

    type_map = {
        "📘 FB ᴩᴏꜱᴛ": ("facebook", "post"),
        "📘 FB ᴠɪᴅᴇᴏ": ("facebook", "video"),
        "📘 FB ʀᴇᴇʟꜱ": ("facebook", "reel"),
        "▶️ YT ᴠɪᴅᴇᴏ": ("youtube", "video"),
        "🟥 YT ꜱʜᴏʀᴛꜱ": ("youtube", "short"),
        "Bulk Upload FB": ("facebook", "bulk"),
        "Bulk Upload YT": ("youtube", "bulk"),
    }
    platform, upload_type = type_map[msg.text]
    
    if upload_type == "bulk":
        user_states[user_id] = {
            "action": "waiting_for_bulk_media",
            "platform": platform,
            "bulk_media": []
        }
        await msg.reply(
            "✅ " + to_bold_sans("Starting Bulk Upload.") + "\n\n" +
            "Please send up to 10 video/photo files. When you are done, type /finish.",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if not await is_premium_for_platform(user_id, platform):
        return await msg.reply(f"❌ " + to_bold_sans(f"Your Access Has Been Denied. Please Upgrade To {platform.capitalize()} Premium."))

    sessions = await load_platform_sessions(user_id, platform)
    if not sessions:
        return await msg.reply(f"❌ " + to_bold_sans(f"Please Login To {platform.capitalize()} First Using `/{platform[0]}login`"), parse_mode=enums.ParseMode.MARKDOWN)
    
    action = f"waiting_for_media"
    user_states[user_id] = {
        "action": action,
        "platform": platform,
        "upload_type": upload_type,
        "file_info": {} # Initialize file_info here
    }
    
    media_type = "photo" if upload_type == "post" else "video"
    await msg.reply("✅ " + to_bold_sans(f"Send The {media_type} File, Ready When You Are!"), reply_markup=ReplyKeyboardRemove())


# ===================================================================
# ======================== TEXT HANDLERS ============================
# ===================================================================

@app.on_message(filters.text & filters.private & ~filters.command(""))
@with_user_lock
async def handle_text_input(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})

    if not state_data:
        return # Ignore random text if user is not in a specific state
    
    # NEW: Auto-delete user's message if enabled
    user_settings = await get_user_settings(user_id)
    if user_settings.get("auto_delete_text", False):
        try:
            await msg.delete()
        except Exception as e:
            logger.warning(f"Could not delete user message: {e}")


    action = state_data.get("action")

    # --- NEW Login Flows ---
    if action == "waiting_for_fb_app_secret":
        state_data["app_secret"] = msg.text.strip()
        state_data["action"] = "waiting_for_fb_app_id"
        await msg.reply("🔑 " + to_bold_sans("Please Enter Your Facebook App ID."))
    
    elif action == "waiting_for_fb_app_id":
        state_data["app_id"] = msg.text.strip()
        state_data["action"] = "waiting_for_fb_page_token"
        await msg.reply(
            "🔑 " + to_bold_sans("Please Enter Your Facebook Page API Token.") + "\n\n"
            + "This is a **Page Access Token**, not a User Token.\n"
            + "You can get it from the [Facebook Developer Dashboard](https://developers.facebook.com/tools/explorer/).",
            disable_web_page_preview=True
        )
        
    elif action == "waiting_for_fb_page_token":
        token = msg.text.strip()
        login_msg = await msg.reply("🔐 " + to_bold_sans("Validating Token And Fetching Page details..."))
        
        try:
            url = f"https://graph.facebook.com/v18.0/me?access_token={token}&fields=id,name"
            response = requests.get(url)
            response.raise_for_status()
            page_data = response.json()
            page_id = page_data.get('id')
            page_name = page_data.get('name')

            if not page_id or not page_name:
                return await safe_edit_message(login_msg, "❌ " + to_bold_sans("Invalid Page Token. Could not fetch Page ID and Name."))

            session_data = {
                'id': page_id,
                'name': page_name,
                'access_token': token
            }
            await save_platform_session(user_id, "facebook", session_data)
            
            user_settings = await get_user_settings(user_id)
            user_settings["active_facebook_id"] = page_id
            await save_user_settings(user_id, user_settings)
            
            await safe_edit_message(login_msg, 
                f"✅ {to_bold_sans('Successfully login!')}\n\n"
                f"**Name:** `{page_name}`\n"
                f"**ID:** `{page_id}`"
            )
            await send_log_to_channel(app, LOG_CHANNEL, f"📝 New Facebook Login: User `{user_id}`, Page: `{page_name}`")
        
        except requests.RequestException as e:
            await safe_edit_message(login_msg, f"❌ " + to_bold_sans(f"Login Failed: Invalid token or API error. {e}"))
        finally:
            if user_id in user_states: del user_states[user_id]
            
    elif action == "waiting_for_yt_client_id":
        state_data["client_id"] = msg.text.strip()
        state_data["action"] = "waiting_for_yt_client_secret"
        await msg.reply("🔑 " + to_bold_sans("Please Enter Your Google OAuth `client_secret`."))
    
    elif action == "waiting_for_yt_client_secret":
        state_data["client_secret"] = msg.text.strip()
        try:
            client_config = {
                "web": {
                    "client_id": state_data["client_id"],
                    "client_secret": state_data["client_secret"],
                    "redirect_uris": [REDIRECT_URI],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token"
                }
            }
            flow = Flow.from_client_config(
                client_config,
                scopes=['https://www.googleapis.com/auth/youtube.upload', 'https://www.googleapis.com/auth/youtube'],
                redirect_uri=REDIRECT_URI
            )
            auth_url, state = flow.authorization_url(access_type='offline', prompt='consent')
            oauth_flows[state] = flow
            state_data["oauth_state"] = state
            state_data["action"] = "waiting_for_yt_auth_code"

            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔗 Google OAuth Link", url=auth_url)],
                [InlineKeyboardButton("✅ I have the code", callback_data="yt_code_received")]
            ])
            await msg.reply(
                "⬇️ " + to_bold_sans("Click the link below, allow access, and copy the code from the URL.") + "\n\n"
                "After allowing, you'll be redirected to a page. Copy the **full URL** of that page and send it back to me, or just the `code` parameter value.",
                reply_markup=markup
            )
        except Exception as e:
            await msg.reply(f"❌ " + to_bold_sans(f"Failed to generate auth URL. Check your Client ID/Secret. Error: {e}"))
            if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_yt_auth_code":
        auth_code_or_url = msg.text.strip()
        auth_msg = await msg.reply("🔐 " + to_bold_sans("Exchanging code for tokens... please wait"))
        
        state = state_data.get("oauth_state")
        if not state or state not in oauth_flows:
            return await safe_edit_message(auth_msg, "❌ " + to_bold_sans("Invalid or expired authentication session. Please try /ytlogin again."))

        flow = oauth_flows[state]
        try:
            # Check if the user pasted the full URL or just the code
            if "localhost" in auth_code_or_url or "code=" in auth_code_or_url:
                await asyncio.to_thread(flow.fetch_token, authorization_response=auth_code_or_url)
            else:
                await asyncio.to_thread(flow.fetch_token, code=auth_code_or_url)
            
            credentials = flow.credentials
            
            youtube = build('youtube', 'v3', credentials=credentials)
            channels_response = await asyncio.to_thread(youtube.channels().list(part='snippet,contentDetails', mine=True).execute)
            
            if not channels_response.get('items'):
                return await safe_edit_message(auth_msg, "❌ " + to_bold_sans("No YouTube Channel Found For This Account."))
            
            channel = channels_response['items'][0]
            channel_id = channel['id']
            channel_name = channel['snippet']['title']
            uploads_playlist_id = channel['contentDetails']['relatedPlaylists']['uploads']
            
            session_data = {
                'id': channel_id,
                'name': channel_name,
                'credentials_json': credentials.to_json()
            }
            await save_platform_session(user_id, "youtube", session_data)
            
            user_settings = await get_user_settings(user_id)
            user_settings["active_youtube_id"] = channel_id
            await save_user_settings(user_id, user_settings)
            
            await safe_edit_message(
                auth_msg, 
                f"✅ {to_bold_sans('Successfully login!')}\n\n"
                f"**Channel Title:** `{channel_name}`\n"
                f"**Channel ID:** `{channel_id}`\n"
                f"**Uploads Playlist:** `{uploads_playlist_id}`"
            )
            await send_log_to_channel(app, LOG_CHANNEL, f"📝 New YouTube Login: User `{user_id}`, Channel: `{channel_name}`")
        except Exception as e:
            await safe_edit_message(auth_msg, 
                f"❌ {to_bold_sans('Login Failed.')}\n"
                f"Error: `{e}`\n\n"
                "**Note:** If you see 'Access blocked' or '401 invalid_client', make sure your app is **Published** in Google Cloud Console, and you have added your Google account email as a **Test User** in the OAuth Consent Screen settings."
            )
            logger.error(f"YouTube token exchange failed for {user_id}: {e}")
        finally:
            if state in oauth_flows: del oauth_flows[state]
            if user_id in user_states: del user_states[user_id]


    # --- Settings Flow ---
    elif action.startswith("waiting_for_caption_"):
        platform = action.split("_")[-1]
        settings = await get_user_settings(user_id)
        settings[f"caption_{platform}"] = msg.text
        await save_user_settings(user_id, settings)
        await msg.reply("✅ " + to_bold_sans(f"Default Caption For {platform.capitalize()} Has Been Set."))
        if user_id in user_states: del user_states[user_id]

    elif action.startswith("waiting_for_title_"):
        platform = action.split("_")[-1]
        settings = await get_user_settings(user_id)
        settings[f"title_{platform}"] = msg.text
        await save_user_settings(user_id, settings)
        await msg.reply("✅ " + to_bold_sans(f"Default Title For {platform.capitalize()} Has Been Set."))
        if user_id in user_states: del user_states[user_id]

    elif action.startswith("waiting_for_description_"):
        platform = action.split("_")[-1]
        settings = await get_user_settings(user_id)
        settings[f"description_{platform}"] = msg.text
        await save_user_settings(user_id, settings)
        await msg.reply("✅ " + to_bold_sans(f"Default Description For {platform.capitalize()} Has Been Set."))
        if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_tags_youtube":
        settings = await get_user_settings(user_id)
        settings["tags_youtube"] = msg.text
        await save_user_settings(user_id, settings)
        await msg.reply("✅ " + to_bold_sans("Default Tags For YouTube Have Been Set."))
        if user_id in user_states: del user_states[user_id]

    # --- Upload Flow ---
    elif action == "waiting_for_title":
        state_data["file_info"]["title"] = msg.text
        await process_upload_step(msg)
    elif action == "waiting_for_description":
        state_data["file_info"]["description"] = msg.text
        await process_upload_step(msg)
    elif action == "waiting_for_tags":
        state_data["file_info"]["tags"] = msg.text
        await process_upload_step(msg)
    elif action == "waiting_for_schedule_time":
        try:
            # Expecting format like "YYYY-MM-DD HH:MM"
            dt_naive = datetime.strptime(msg.text.strip(), "%Y-%m-%d %H:%M")
            # For simplicity, let's assume user inputs in UTC. For production, timezone handling should be more robust.
            schedule_time_utc = dt_naive.replace(tzinfo=timezone.utc)
            
            if schedule_time_utc <= datetime.now(timezone.utc):
                return await msg.reply("❌ " + to_bold_sans("Scheduled time must be in the future."))
            
            # YouTube API requires ISO 8601 format with 'Z' for UTC
            state_data['file_info']['schedule_time'] = schedule_time_utc
            await process_upload_step(msg)
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid format. Please use `YYYY-MM-DD HH:MM` in UTC."))
            
    # --- Bulk Upload Flow ---
    elif action == "waiting_for_bulk_caption":
        caption = msg.text
        platform = state_data["platform"]
        media_list = state_data["bulk_media"]

        confirm_msg = await msg.reply("⏳ " + to_bold_sans(f"Scheduling {len(media_list)} posts..."))

        today = datetime.now(timezone.utc)
        for i, media_msg_id in enumerate(media_list):
            # Schedule one per day for the next 10 days
            schedule_time = today + timedelta(days=i + 1, hours=random.randint(9, 21), minutes=random.randint(0, 59))
            
            job_details = {
                "user_id": user_id,
                "platform": platform,
                "storage_msg_id": media_msg_id,
                "schedule_time": schedule_time,
                "status": "pending",
                "created_at": datetime.now(timezone.utc),
                "metadata": {
                    "title": caption,
                    "description": "", # Empty for bulk
                    "tags": "",
                    "visibility": "public"
                }
            }
            if db is not None:
                await asyncio.to_thread(db.scheduled_jobs.insert_one, job_details)
        
        await confirm_msg.edit(f"✅ **Bulk Schedule Complete!**\n\n{len(media_list)} posts have been scheduled over the next {len(media_list)} days.")
        if user_id in user_states: del user_states[user_id]



    # --- Admin Flow ---
    elif action == "waiting_for_broadcast_message":
        if not is_admin(user_id): return
        
        # New broadcast logic to handle media and buttons
        if msg.text:
            await broadcast_message(msg, text=msg.text, reply_markup=msg.reply_markup)
        elif msg.photo:
            await broadcast_message(msg, photo=msg.photo.file_id, caption=msg.caption, reply_markup=msg.reply_markup)
        elif msg.video:
            await broadcast_message(msg, video=msg.video.file_id, caption=msg.caption, reply_markup=msg.reply_markup)
        else:
            await msg.reply("Unsupported broadcast format. Please send text, photo, or video.")
            
        if user_id in user_states: del user_states[user_id]


    elif action == "waiting_for_target_user_id_premium_management":
        if not is_admin(user_id): return
        try:
            target_user_id = int(msg.text)
            user_states[user_id] = {"action": "select_platforms_for_premium", "target_user_id": target_user_id, "selected_platforms": {}}
            await msg.reply(
                f"✅ " + to_bold_sans(f"User Id `{target_user_id}`. Select Platforms For Premium:"),
                reply_markup=get_platform_selection_markup(user_id, {}),
                parse_mode=enums.ParseMode.MARKDOWN
            )
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid User Id."))
            if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_user_id_for_details":
        if not is_admin(user_id): return
        try:
            target_user_id = int(msg.text)
            await show_user_details(msg, target_user_id)
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid User Id."))
        finally:
            if user_id in user_states: del user_states[user_id]
            
    elif action == "waiting_for_max_uploads":
        if not is_admin(user_id): return
        try:
            new_limit = int(msg.text)
            if new_limit <= 0: return await msg.reply("❌ " + to_bold_sans("Limit Must Be A Positive Integer."))
            await _update_global_setting("max_concurrent_uploads", new_limit)
            global upload_semaphore
            upload_semaphore = asyncio.Semaphore(new_limit)
            await msg.reply(f"✅ " + to_bold_sans(f"Max Concurrent Uploads Set To `{new_limit}`."))
            if user_id in user_states: del user_states[user_id]
            await show_global_settings_panel(msg)
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid Input. Please Send A Valid Number."))

    elif action == "waiting_for_max_file_size":
        if not is_admin(user_id): return
        try:
            new_limit = int(msg.text)
            if new_limit <= 0: return await msg.reply("❌ " + to_bold_sans("Limit Must Be A Positive Integer."))
            await _update_global_setting("max_file_size_mb", new_limit)
            global MAX_FILE_SIZE_BYTES
            MAX_FILE_SIZE_BYTES = new_limit * 1024 * 1024
            await msg.reply(f"✅ " + to_bold_sans(f"Max File Size Set To `{new_limit}` MB."))
            if user_id in user_states: del user_states[user_id]
            await show_global_settings_panel(msg)
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid Input. Please Send A Valid Number."))

    elif action in ["waiting_for_event_title", "waiting_for_event_message"]:
        if not is_admin(user_id): return
        setting_key = "special_event_title" if action == "waiting_for_event_title" else "special_event_message"
        await _update_global_setting(setting_key, msg.text)
        await msg.reply(f"✅ " + to_bold_sans(f"Special Event `{setting_key.split('_')[-1]}` Updated!"))
        if user_id in user_states: del user_states[user_id]
        await show_global_settings_panel(msg)

    elif action.startswith("waiting_for_payment_details_"):
        if not is_admin(user_id): return
        payment_method = action.replace("waiting_for_payment_details_", "")
        new_payment_settings = global_settings.get("payment_settings", {})
        new_payment_settings[payment_method] = msg.text
        await _update_global_setting("payment_settings", new_payment_settings)
        await msg.reply(f"✅ " + to_bold_sans(f"Payment Details For **{payment_method.upper()}** Updated."), reply_markup=payment_settings_markup, parse_mode=enums.ParseMode.MARKDOWN)
        if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_payment_instructions":
        if not is_admin(user_id): return
        new_payment_settings = global_settings.get("payment_settings", {})
        new_payment_settings["instructions"] = msg.text
        await _update_global_setting("payment_settings", new_payment_settings)
        await msg.reply(f"✅ " + to_bold_sans("Payment Instructions Updated."), reply_markup=payment_settings_markup)
        if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_custom_button_name":
        if not is_admin(user_id): return
        user_states[user_id]['button_name'] = msg.text.strip()
        user_states[user_id]['action'] = "waiting_for_custom_button_details"
        await msg.reply("✍️ " + to_bold_sans("Enter Payment Details (text / Number / Address / Link):"))

    elif action == "waiting_for_custom_button_details":
        if not is_admin(user_id): return
        button_name = state_data['button_name']
        button_details = msg.text.strip()
        payment_settings = global_settings.get("payment_settings", {})
        if "custom_buttons" not in payment_settings:
            payment_settings["custom_buttons"] = {}
        payment_settings["custom_buttons"][button_name] = button_details
        await _update_global_setting("payment_settings", payment_settings)
        await msg.reply(f"✅ " + to_bold_sans(f"Payment Button `{button_name}` Created."), reply_markup=payment_settings_markup)
        if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_payment_proof":
        if 'payment_proof_message' in user_states[user_id]:
            # This means the bot has already asked for proof and this is the user's reply
            await msg.forward(ADMIN_ID)
            await app.send_message(
                ADMIN_ID, 
                f"👆 Payment proof from user: `{user_id}` (@{msg.from_user.username or 'N/A'})"
            )
            await msg.reply("✅ Your proof has been sent to the admin for verification. Please wait for confirmation.")
            if user_id in user_states: del user_states[user_id]


# ===================================================================
# =================== CALLBACK QUERY HANDLERS =======================
# ===================================================================

# NEW CALLBACK HANDLER FOR PLATFORM SELECTION
@app.on_callback_query(filters.regex("^select_platform_"))
@rate_limit_callbacks
async def select_platform_for_premium_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id):
        return await query.answer("Admin only.", show_alert=True)

    state_data = user_states.get(user_id)
    if not state_data or state_data.get("action") != "select_platforms_for_premium":
        return await query.answer("Error: Invalid state. Please start again.", show_alert=True)
        
    platform = query.data.split("select_platform_")[1]
    
    # Toggle the selection status for the platform
    selected_platforms = state_data.get("selected_platforms", {})
    selected_platforms[platform] = not selected_platforms.get(platform, False)
    state_data["selected_platforms"] = selected_platforms
    
    # Edit the message to show the updated selection with checkmarks
    await safe_edit_message(
        query.message,
        text=f"✅ User Id `{state_data.get('target_user_id')}`. Select Platforms For Premium:",
        reply_markup=get_platform_selection_markup(user_id, selected_platforms)
    )

@app.on_callback_query(filters.regex("^confirm_platform_selection$"))
@rate_limit_callbacks
async def confirm_platform_selection_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id): return await query.answer("❌ Admin access required", show_alert=True)
    
    state_data = user_states.get(user_id)
    if not isinstance(state_data, dict) or state_data.get("action") != "select_platforms_for_premium":
        return await query.answer("Error: State lost. Please restart.", show_alert=True)
        
    selected_platforms = [p for p, selected in state_data.get("selected_platforms", {}).items() if selected]
    if not selected_platforms:
        return await query.answer("Please select at least one platform!", show_alert=True)
        
    state_data["action"] = "select_premium_plan_for_platforms"
    state_data["final_selected_platforms"] = selected_platforms
    user_states[user_id] = state_data
    
    await safe_edit_message(
        query.message,
        f"✅ Platforms Selected: `{', '.join(p.capitalize() for p in selected_platforms)}`.\n\nNow, select a premium plan for user `{state_data['target_user_id']}`:",
        reply_markup=get_premium_plan_markup(user_id),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^grant_plan_"))
@rate_limit_callbacks
async def grant_plan_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id): return await query.answer("❌ Admin access required", show_alert=True)
    # BUG FIX: Changed `if not db:` to `if db is None:`
    if db is None: return await query.answer("⚠️ Database unavailable.", show_alert=True)
    
    state_data = user_states.get(user_id)
    if not isinstance(state_data, dict) or state_data.get("action") != "select_premium_plan_for_platforms":
        return await query.answer("❌ Error: State lost. Please start over.", show_alert=True)
        
    target_user_id = state_data["target_user_id"]
    selected_platforms = state_data["final_selected_platforms"]
    premium_plan_key = query.data.split("grant_plan_")[1]
    
    plan_details = PREMIUM_PLANS.get(premium_plan_key)
    if not plan_details:
        return await query.answer("Invalid premium plan selected.", show_alert=True)
    
    target_user_data = await _get_user_data(target_user_id) or {"_id": target_user_id, "premium": {}}
    premium_data = target_user_data.get("premium", {})
    
    for platform in selected_platforms:
        new_premium_until = None
        if plan_details["duration"] is not None:
            new_premium_until = datetime.now(timezone.utc) + plan_details["duration"]
        
        platform_premium_data = {
            "type": premium_plan_key, "added_by": user_id, "added_at": datetime.now(timezone.utc), "status": "active"
        }
        if new_premium_until:
            platform_premium_data["until"] = new_premium_until
        
        premium_data[platform] = platform_premium_data
    
    await _save_user_data(target_user_id, {"premium": premium_data})
    
    admin_confirm_text = f"✅ Premium granted to user `{target_user_id}` for:\n"
    user_msg_text = "🎉 **Congratulations!** 🎉\n\nYou have been granted premium access for:\n"
    
    for platform in selected_platforms:
        p_data = premium_data.get(platform, {})
        line = f"**{platform.capitalize()}**: `{p_data.get('type', 'N/A').replace('_', ' ').title()}`"
        if p_data.get("until"):
            line += f" (Expires: `{p_data['until'].strftime('%Y-%m-%d %H:%M')}` UTC)"
        admin_confirm_text += f"- {line}\n"
        user_msg_text += f"- {line}\n"
    
    user_msg_text += "\nEnjoy your new features! ✨"
    
    await safe_edit_message(query.message, admin_confirm_text, reply_markup=admin_markup, parse_mode=enums.ParseMode.MARKDOWN)
    await query.answer("Premium granted!", show_alert=False)
    if user_id in user_states: del user_states[user_id]
        
    try:
        await app.send_message(target_user_id, user_msg_text, parse_mode=enums.ParseMode.MARKDOWN)
        await send_log_to_channel(app, LOG_CHANNEL,
            f"💰 Premium granted to `{target_user_id}` by admin `{user_id}`.\nPlatforms: `{', '.join(selected_platforms)}`, Plan: `{premium_plan_key}`"
        )
    except Exception as e:
        logger.error(f"Failed to notify user {target_user_id} about premium: {e}")
    
    # Answer the query to stop the loading animation on the button
    await query.answer()

@app.on_callback_query(filters.regex("^hub_settings_"))
@rate_limit_callbacks
async def hub_settings_cb(_, query):
    platform = query.data.split("_")[-1]
    if platform == "facebook":
        await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Configure Facebook Settings:"), reply_markup=get_facebook_settings_markup())
    elif platform == "youtube":
        await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Configure YouTube Settings:"), reply_markup=get_youtube_settings_markup())

# --- Account Management Callbacks ---
@app.on_callback_query(filters.regex("^manage_(fb|yt)_accounts$"))
@rate_limit_callbacks
async def manage_accounts_cb(_, query):
    user_id = query.from_user.id
    platform = "facebook" if "fb" in query.data else "youtube"
    
    sessions = await load_platform_sessions(user_id, platform)
    logged_in_accounts = {s['session_data']['id']: s['session_data']['name'] for s in sessions}
    
    if not logged_in_accounts:
        await query.answer(f"You have no {platform.capitalize()} accounts logged in. Let's add one.", show_alert=True)
        # We simulate a message object to call the handler directly
        class MockMessage:
            def __init__(self, user, chat):
                self.from_user = user
                self.chat = chat
            async def reply(self, *args, **kwargs):
                return await app.send_message(self.chat.id, *args, **kwargs)

        if platform == 'facebook':
            await facebook_login_cmd_new(app, MockMessage(query.from_user, query.message.chat))
        elif platform == 'youtube':
            await youtube_login_cmd_new(app, MockMessage(query.from_user, query.message.chat))
        await query.message.delete()
        return

    await safe_edit_message(query.message, "👤 " + to_bold_sans(f"Select Your Active {platform.capitalize()} Account"),
        reply_markup=await get_account_markup(user_id, platform, logged_in_accounts)
    )

@app.on_callback_query(filters.regex("^select_acc_"))
@rate_limit_callbacks
async def select_account_cb(_, query):
    user_id = query.from_user.id
    _, _, platform, acc_id = query.data.split("_")
    
    user_settings = await get_user_settings(user_id)
    user_settings[f"active_{platform}_id"] = acc_id
    await save_user_settings(user_id, user_settings)
    
    await query.answer(f"✅ Active account for {platform.capitalize()} has been updated.", show_alert=True)
    # Refresh the account management panel
    class MockQuery:
        def __init__(self, user, message, data):
            self.from_user = user
            self.message = message
            self.data = data
    await manage_accounts_cb(app, MockQuery(query.from_user, query.message, f'manage_{"fb" if platform == "facebook" else "yt"}_accounts'))

@app.on_callback_query(filters.regex("^confirm_logout_"))
@rate_limit_callbacks
async def confirm_logout_cb(_, query):
    _, _, platform, acc_id = query.data.split("_")
    sessions = await load_platform_sessions(query.from_user.id, platform)
    acc_name = next((s['session_data']['name'] for s in sessions if s['account_id'] == acc_id), "Account")
    
    await safe_edit_message(
        query.message,
        to_bold_sans(f"Logout {acc_name}? You Can Re-login Later."),
        reply_markup=get_logout_confirm_markup(platform, acc_id, acc_name)
    )

@app.on_callback_query(filters.regex("^logout_acc_"))
@rate_limit_callbacks
async def logout_account_cb(_, query):
    user_id = query.from_user.id
    _, _, platform, acc_id_to_logout = query.data.split("_")

    await delete_platform_session(user_id, platform, acc_id_to_logout)
    
    user_settings = await get_user_settings(user_id)
    if user_settings.get(f"active_{platform}_id") == acc_id_to_logout:
        sessions = await load_platform_sessions(user_id, platform)
        # Set the active account to the next available one, or None
        user_settings[f"active_{platform}_id"] = sessions[0]['session_data']['id'] if sessions else None
        await save_user_settings(user_id, user_settings)
    
    await query.answer(f"✅ Logged out successfully.", show_alert=True)
    class MockQuery:
        def __init__(self, user, message, data):
            self.from_user = user
            self.message = message
            self.data = data
    await manage_accounts_cb(app, MockQuery(query.from_user, query.message, f'manage_{"fb" if platform == "facebook" else "yt"}_accounts'))

@app.on_callback_query(filters.regex("^add_account_"))
@rate_limit_callbacks
async def add_account_cb(_, query):
    user_id = query.from_user.id
    platform = query.data.split("add_account_")[-1]
    
    if not await is_premium_for_platform(user_id, platform) and not is_admin(user_id):
        return await query.answer("❌ This is a premium feature.", show_alert=True)
    
    # We now trigger the new login flows using commands
    await query.message.delete()

    class MockMessage: # Create a mock message object to pass to the command handlers
        def __init__(self, user, chat):
            self.from_user = user
            self.chat = chat
        async def reply(self, *args, **kwargs):
            return await app.send_message(self.chat.id, *args, **kwargs)

    if platform == 'facebook':
        await facebook_login_cmd_new(app, MockMessage(query.from_user, query.message.chat))
    elif platform == 'youtube':
        await youtube_login_cmd_new(app, MockMessage(query.from_user, query.message.chat))


# --- General Callbacks ---
@app.on_callback_query(filters.regex("^cancel_upload$"))
@rate_limit_callbacks
async def cancel_upload_cb(_, query):
    user_id = query.from_user.id
    await query.answer("Upload cancelled.", show_alert=True)
    
    state_data = user_states.get(user_id, {})
    status_msg = state_data.get('status_msg', query.message)
    await safe_edit_message(status_msg, "❌ **" + to_bold_sans("Upload Cancelled") + "**\n\n" + to_bold_sans("Your Operation Has Been Successfully Cancelled."))

    file_info = state_data.get("file_info", {})
    files_to_clean = [file_info.get("downloaded_path"), file_info.get("processed_path"), file_info.get("thumbnail_path")]
    
    cleanup_temp_files(files_to_clean)
    if user_id in user_states: del user_states[user_id]
    await task_tracker.cancel_all_user_tasks(user_id)
    logger.info(f"User {user_id} cancelled their upload.")

# --- NEW: Upload Flow Callbacks ---
@app.on_callback_query(filters.regex("^upload_flow_"))
@rate_limit_callbacks
async def upload_flow_cb(_, query):
    user_id = query.from_user.id
    data = query.data.replace("upload_flow_", "")
    
    state_data = user_states.get(user_id)
    if not state_data or "file_info" not in state_data:
        return await query.answer("❌ Error: State lost, please start over.", show_alert=True)
    
    parts = data.split("_")
    step = parts[0]
    choice = parts[1]

    if step == "thumbnail":
        if choice == "custom":
            state_data['action'] = 'waiting_for_thumbnail'
            await safe_edit_message(query.message, "🖼️ " + to_bold_sans("Please Send The Thumbnail Image."))
        elif choice == "auto":
            state_data['file_info']['thumbnail_path'] = "auto" # Signal for auto-generation
            await process_upload_step(query)
    elif step == "visibility":
        state_data['file_info']['visibility'] = choice
        await process_upload_step(query)
    elif step == "publish":
        if choice == "now":
            state_data['file_info']['schedule_time'] = None
            await process_upload_step(query)
        elif choice == "schedule":
            state_data['action'] = 'waiting_for_schedule_time'
            await safe_edit_message(
                query.message,
                "⏰ " + to_bold_sans("Send Schedule Time (UTC)") + "\n\n"
                "Format: `YYYY-MM-DD HH:MM`\n"
                f"Current UTC time: `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}`"
            )

# --- Premium & Payment Callbacks ---
@app.on_callback_query(filters.regex("^buypypremium$"))
@rate_limit_callbacks
async def buypypremium_cb(_, query):
    user_id = query.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    
    premium_plans_text = (
        "⭐ " + to_bold_sans("Upgrade To Premium!") + " ⭐\n\n"
        + to_bold_sans("Unlock Full Features And Upload Unlimited Content Without Restrictions.") + "\n\n"
        "**Available Plans:**"
    )
    await safe_edit_message(query.message, premium_plans_text, reply_markup=get_premium_plan_markup(user_id), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^show_plan_details_"))
@rate_limit_callbacks
async def show_plan_details_cb(_, query):
    user_id = query.from_user.id
    plan_key = query.data.split("show_plan_details_")[1]
    
    state_data = user_states.get(user_id, {})
    is_admin_adding_premium = (is_admin(user_id) and state_data.get("action") == "select_premium_plan_for_platforms")
    
    plan_details = PREMIUM_PLANS[plan_key]
    plan_text = f"**{to_bold_sans(plan_key.replace('_', ' ').title() + ' Plan Details')}**\n\n**Duration**: "
    plan_text += f"{plan_details['duration'].days} days\n" if plan_details['duration'] else "Lifetime\n"
    plan_text += f"**Price**: {plan_details['price']}\n\n"
    
    if is_admin_adding_premium:
        target_user_id = state_data.get('target_user_id', 'Unknown User')
        plan_text += to_bold_sans(f"Click Below To Grant This Plan To User `{target_user_id}`.")
    else:
        plan_text += to_bold_sans("To Purchase, Click 'buy Now' Or Check The Available Payment Methods.")
        
    await safe_edit_message(
        query.message, plan_text,
        reply_markup=get_premium_details_markup(plan_key, is_admin_flow=is_admin_adding_premium),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^show_payment_methods$"))
@rate_limit_callbacks
async def show_payment_methods_cb(_, query):
    payment_methods_text = "**" + to_bold_sans("Available Payment Methods") + "**\n\n"
    payment_methods_text += to_bold_sans("Choose Your Preferred Method To Proceed With Payment.")
    await safe_edit_message(query.message, payment_methods_text, reply_markup=get_payment_methods_markup(), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^submit_payment_proof$"))
@rate_limit_callbacks
async def submit_payment_proof_cb(_, query):
    user_id = query.from_user.id
    instructions = global_settings.get("payment_settings", {}).get("instructions")
    user_states[user_id] = {"action": "waiting_for_payment_proof"}
    await query.answer()
    await safe_edit_message(query.message, f"🧾 **Submit Payment Proof**\n\n{instructions}")

@app.on_callback_query(filters.regex("^show_payment_qr_google_play$"))
@rate_limit_callbacks
async def show_payment_qr_google_play_cb(_, query):
    qr_file_id = global_settings.get("payment_settings", {}).get("google_play_qr_file_id")
    if not qr_file_id:
        await query.answer("QR code is not set by the admin yet.", show_alert=True)
        return
    
    caption_text = "**" + to_bold_sans("Scan & Pay") + "**\n\n" + \
                   "Send a screenshot to the Admin for activation."
    
    await query.message.reply_photo(
        photo=qr_file_id,
        caption=caption_text,
        parse_mode=enums.ParseMode.MARKDOWN
    )
    await query.answer()

@app.on_callback_query(filters.regex("^show_payment_details_"))
@rate_limit_callbacks
async def show_payment_details_cb(_, query):
    method = query.data.split("show_payment_details_")[1]
    payment_details = global_settings.get("payment_settings", {}).get(method, "No details available.")
    text = (
        f"**{to_bold_sans(f'{method.upper()} Payment Details')}**\n\n"
        f"`{payment_details}`\n\n"
        f"Contact the Admin with a screenshot for activation."
    )
    await safe_edit_message(query.message, text, reply_markup=get_payment_methods_markup(), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^show_custom_payment_"))
@rate_limit_callbacks
async def show_custom_payment_cb(_, query):
    button_name = query.data.split("show_custom_payment_")[1]
    payment_details = global_settings.get("payment_settings", {}).get("custom_buttons", {}).get(button_name, "No details available.")
    text = (
        f"**{to_bold_sans(f'{button_name.upper()} Payment Details')}**\n\n"
        f"`{payment_details}`\n\n"
        f"Contact the Admin with a screenshot for activation."
    )
    await safe_edit_message(query.message, text, reply_markup=get_payment_methods_markup(), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^buy_now$"))
@rate_limit_callbacks
async def buy_now_cb(_, query):
    text = (
        f"**{to_bold_sans('Purchase Confirmation')}**\n\n"
        f"Please contact the Admin to complete the payment."
    )
    await safe_edit_message(query.message, text, parse_mode=enums.ParseMode.MARKDOWN)

# --- Admin Panel Callbacks (ALL NEW AND FIXED) ---
@app.on_callback_query(filters.regex("^(admin_panel|users_list|admin_user_details|manage_premium|broadcast_message|admin_stats_panel)$"))
@rate_limit_callbacks
async def admin_panel_actions_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id):
        return await query.answer("❌ Admin access required", show_alert=True)
    
    action = query.data

    if action == "admin_panel":
        await safe_edit_message(
            query.message,
            "🛠 " + to_bold_sans("Welcome To The Admin Panel!"),
            reply_markup=admin_markup,
            parse_mode=enums.ParseMode.MARKDOWN
        )
    elif action == "users_list":
        # BUG FIX: Changed `if not db:` to `if db is None:`
        if db is None: return await query.answer("DB connection failed.", show_alert=True)
        await query.answer("Fetching users...")
        users = await asyncio.to_thread(list, db.users.find({}))
        user_list_text = f"👥 **Total Users: {len(users)}**\n\n"
        for i, user in enumerate(users[:50]): # Limit to 50 to avoid message overflow
            user_list_text += f"`{user['_id']}` - @{user.get('username', 'N/A')}\n"
        if len(users) > 50:
            user_list_text += "\n...and more."
        await safe_edit_message(query.message, user_list_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]))

    elif action == "admin_user_details":
        user_states[user_id] = {"action": "waiting_for_user_id_for_details"}
        await safe_edit_message(query.message, "👤 " + to_bold_sans("Please send the User ID to get their details."))
    
    elif action == "manage_premium":
        user_states[user_id] = {"action": "waiting_for_target_user_id_premium_management"}
        await safe_edit_message(query.message, "➕ " + to_bold_sans("Please send the User ID to manage their premium subscription."))

    elif action == "broadcast_message":
        user_states[user_id] = {"action": "waiting_for_broadcast_message"}
        await safe_edit_message(query.message, "📢 " + to_bold_sans("Please send the message (text, photo, or video) you want to broadcast to all users."))
    
    elif action == "admin_stats_panel":
        # MOCKMSG FIX: Pass the query object to create a compliant mock message
        class MockMsg:
            def __init__(self, q):
                self.from_user = q.from_user
                self.chat = q.message.chat
                self.id = q.message.id
            async def reply(self, text, **kwargs):
                await safe_edit_message(query.message, text, **kwargs)
        
        await show_stats(app, MockMsg(query))

async def show_user_details(message, target_user_id):
    """Helper function to fetch and display user details for the admin."""
    user_data = await _get_user_data(target_user_id)
    if not user_data:
        return await message.reply("❌ User not found in the database.")

    details_text = f"👤 **Details for User ID:** `{target_user_id}`\n"
    details_text += f"**Username:** @{user_data.get('username', 'N/A')}\n"
    
    joined_at = user_data.get('added_at', 'N/A')
    if isinstance(joined_at, datetime):
        details_text += f"**Joined:** {joined_at.strftime('%Y-%m-%d')}\n"
    else:
        details_text += f"**Joined:** {joined_at}\n"
        
    last_active = user_data.get('last_active', 'N/A')
    if isinstance(last_active, datetime):
        details_text += f"**Last Active:** {last_active.strftime('%Y-%m-%d %H:%M')}\n\n"
    else:
        details_text += f"**Last Active:** {last_active}\n\n"

    # Fetch upload stats
    total_uploads = await asyncio.to_thread(db.uploads.count_documents, {'user_id': target_user_id})
    last_upload = await asyncio.to_thread(db.uploads.find_one, {'user_id': target_user_id}, sort=[('timestamp', DESCENDING)])
    details_text += f"**Uploads:**\n- Total: `{total_uploads}`\n"
    if last_upload:
        details_text += f"- Last Upload: `{last_upload.get('title', 'N/A')}` on `{last_upload['timestamp'].strftime('%Y-%m-%d')}`\n\n"
    else:
        details_text += "- Last Upload: `None`\n\n"

    details_text += "**Premium Status:**\n"
    has_any_premium = False
    for platform in PREMIUM_PLATFORMS:
        if await is_premium_for_platform(target_user_id, platform):
            has_any_premium = True
            p_data = user_data.get("premium", {}).get(platform, {})
            p_type = p_data.get("type", "N/A").replace("_", " ").title()
            p_until = p_data.get("until")
            details_text += f"- **{platform.capitalize()}:** Active (`{p_type}`)\n"

            if p_until and isinstance(p_until, datetime):
                if p_until.tzinfo is None: p_until = p_until.replace(tzinfo=timezone.utc)
                details_text += f"  - Expires: `{p_until.strftime('%Y-%m-%d %H:%M UTC')}`\n"
    if not has_any_premium:
        details_text += "  - No active premium subscriptions.\n"

    await message.reply(details_text, parse_mode=enums.ParseMode.MARKDOWN)

async def show_global_settings_panel(message_or_query):
    """Helper function to display the global settings panel."""
    if hasattr(message_or_query, 'message'): # It's a query
        message = message_or_query.message
    else: # It's a message
        message = message_or_query

    settings_text = (
        "⚙️ **" + to_bold_sans("Global Bot Settings") + "**\n\n"
        f"**📢 Special Event:** `{global_settings.get('special_event_toggle', False)}`\n"
        f"**⏫ Max Concurrent Uploads:** `{global_settings.get('max_concurrent_uploads')}`\n"
        f"**🗂️ Max File Size:** `{global_settings.get('max_file_size_mb')}` MB\n"
        f"**👥 Multiple Logins:** `{'Allowed' if global_settings.get('allow_multiple_logins') else 'Blocked'}`"
    )
    await safe_edit_message(message, settings_text, reply_markup=get_admin_global_settings_markup(), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^(global_settings_panel|toggle_special_event|set_event_title|set_event_message|set_max_uploads|set_max_file_size|set_payment_instructions|toggle_multiple_logins|reset_stats|show_system_stats|confirm_reset_stats|payment_settings_panel|create_custom_payment_button|set_payment_google_play_qr|set_payment_upi|set_payment_usdt|set_payment_btc|set_payment_others)$"))
@rate_limit_callbacks
async def global_settings_actions_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id):
        return await query.answer("❌ Admin access required", show_alert=True)
        
    action = query.data
    
    if action == "global_settings_panel":
        await show_global_settings_panel(query)
    
    elif action == "toggle_special_event":
        current_status = global_settings.get("special_event_toggle", False)
        await _update_global_setting("special_event_toggle", not current_status)
        await query.answer(f"Special event turned {'OFF' if current_status else 'ON'}")
        await show_global_settings_panel(query)

    elif action == "toggle_multiple_logins":
        current_status = global_settings.get("allow_multiple_logins", False)
        await _update_global_setting("allow_multiple_logins", not current_status)
        await query.answer(f"Multiple logins {'Blocked' if current_status else 'Allowed'}")
        await show_global_settings_panel(query)

    elif action == "set_event_title":
        user_states[user_id] = {"action": "waiting_for_event_title"}
        await safe_edit_message(query.message, "✏️ " + to_bold_sans("Please send the new title for the special event."))
        
    elif action == "set_event_message":
        user_states[user_id] = {"action": "waiting_for_event_message"}
        await safe_edit_message(query.message, "💬 " + to_bold_sans("Please send the new message for the special event."))

    elif action == "set_max_uploads":
        user_states[user_id] = {"action": "waiting_for_max_uploads"}
        await safe_edit_message(query.message, "⏫ " + to_bold_sans(f"Current limit is {MAX_CONCURRENT_UPLOADS}. Send the new number for max concurrent uploads."))

    elif action == "set_max_file_size":
        user_states[user_id] = {"action": "waiting_for_max_file_size"}
        await safe_edit_message(query.message, "🗂️ " + to_bold_sans(f"Current limit is {global_settings.get('max_file_size_mb')} MB. Send the new number for max file size in MB."))

    elif action == "set_payment_instructions":
        user_states[user_id] = {"action": "waiting_for_payment_instructions"}
        await safe_edit_message(query.message, "✍️ " + to_bold_sans("Please send the new payment instructions for users."))

    elif action == "reset_stats":
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes, Reset All Stats", callback_data="confirm_reset_stats")],
            [InlineKeyboardButton("❌ No, Cancel", callback_data="global_settings_panel")]
        ])
        await safe_edit_message(query.message, "⚠️ " + to_bold_sans("Are you sure? This will delete all upload records permanently."), reply_markup=markup)

    elif action == "confirm_reset_stats":
        # BUG FIX: Changed `if not db:` to `if db is None:`
        if db is None: return await query.answer("DB connection failed.", show_alert=True)
        await asyncio.to_thread(db.uploads.delete_many, {})
        await query.answer("All upload stats have been reset.", show_alert=True)
        await send_log_to_channel(app, LOG_CHANNEL, f"🗑️ Admin `{user_id}` reset all upload stats.")
        await show_global_settings_panel(query)

    elif action == "show_system_stats":
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        stats_text = (
            f"💻 **{to_bold_sans('System Statistics')}**\n\n"
            f"**CPU:** `{cpu}%`\n"
            f"**RAM:** `{ram.percent}%` (Used: {ram.used / (1024**3):.2f} GB)\n"
            f"**Disk:** `{disk.percent}%` (Used: {disk.used / (1024**3):.2f} GB / {disk.total / (1024**3):.2f} GB)"
        )
        await safe_edit_message(query.message, stats_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Settings", callback_data="global_settings_panel")]]))
    
    elif action == "payment_settings_panel":
        await safe_edit_message(query.message, "💰 " + to_bold_sans("Manage Payment Settings:"), reply_markup=payment_settings_markup)

    elif action == "create_custom_payment_button":
        user_states[user_id] = {"action": "waiting_for_custom_button_name"}
        await query.message.edit("🆕 " + to_bold_sans("Enter the name for the new payment button (e.g., PayPal)."))

    elif action == "set_payment_google_play_qr":
        user_states[user_id] = {"action": "waiting_for_google_play_qr"}
        await query.message.edit("🖼️ " + to_bold_sans("Please send the QR code image for Google Play."))

    elif action in ["set_payment_upi", "set_payment_usdt", "set_payment_btc", "set_payment_others"]:
        method = action.split("set_payment_")[1]
        user_states[user_id] = {"action": f"waiting_for_payment_details_{method}"}
        await query.message.edit(f"✍️ " + to_bold_sans(f"Please send the payment details for {method.upper()}."))


# --- Back Callbacks ---
@app.on_callback_query(filters.regex("^back_to_"))
@rate_limit_callbacks
async def back_to_cb(_, query):
    data = query.data
    user_id = query.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    
    # Clean up state when going back
    await task_tracker.cancel_all_user_tasks(user_id)
    if user_id in user_states: del user_states[user_id]
        
    if data == "back_to_main_menu":
        try: await query.message.delete()
        except Exception: pass
        premium_platforms = [p for p in PREMIUM_PLATFORMS if await is_premium_for_platform(user_id, p) or is_admin(user_id)]
        await app.send_message(
            query.message.chat.id, "🏠 " + to_bold_sans("Main Menu"),
            reply_markup=get_main_keyboard(user_id, premium_platforms)
        )
    elif data == "back_to_settings":
        await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Settings Panel"), reply_markup=await get_main_settings_markup(user_id))
    elif data == "back_to_admin":
        await admin_panel_actions_cb(app, query)
    elif data == "back_to_premium_plans":
        state_data = user_states.get(user_id, {})
        if is_admin(user_id) and state_data.get("action") == "select_premium_plan_for_platforms":
            await safe_edit_message(query.message, "➡️ " + to_bold_sans("Please choose a plan to grant:"), reply_markup=get_premium_plan_markup(user_id))
        else:
            await buypypremium_cb(app, query)
    elif data == "back_to_global":
        await show_global_settings_panel(query)

# --- Trial Activation ---
@app.on_callback_query(filters.regex("^activate_trial_"))
@rate_limit_callbacks
async def activate_trial_cb(_, query):
    user_id = query.from_user.id
    platform = query.data.split("_")[-1]
    
    if await is_premium_for_platform(user_id, platform):
        return await query.answer(f"You already have an active premium/trial for {platform.capitalize()}!", show_alert=True)

    premium_until = datetime.now(timezone.utc) + timedelta(hours=6)
    user_data = await _get_user_data(user_id) or {}
    user_premium_data = user_data.get("premium", {})
    user_premium_data[platform] = {
        "type": "6_hour_trial", "added_by": "callback_trial",
        "added_at": datetime.now(timezone.utc), "until": premium_until,
        "status": "active"
    }
    await _save_user_data(user_id, {"premium": user_premium_data})

    logger.info(f"User {user_id} activated a 6-hour {platform} trial.")
    await send_log_to_channel(app, LOG_CHANNEL, f"✨ User `{user_id}` activated a 6-hour {platform.capitalize()} trial.")
    
    await query.answer(f"✅ Free 6-hour {platform.capitalize()} trial activated!", show_alert=True)
    
    welcome_msg = (
        f"🎉 **" + to_bold_sans(f"Congratulations!") + "**\n\n"
        + to_bold_sans(f"You Have Activated Your 6-hour Premium Trial For {platform.capitalize()}.") + "\n\n"
        + f"To get started, please log in with: `/{platform[0]}login`"
    )
    premium_platforms = [p for p in PREMIUM_PLATFORMS if await is_premium_for_platform(user_id, p) or is_admin(user_id)]
    await query.message.reply(welcome_msg, reply_markup=get_main_keyboard(user_id, premium_platforms), parse_mode=enums.ParseMode.MARKDOWN)
    await query.message.delete()

@app.on_callback_query(filters.regex("^set_visibility_"))
@rate_limit_callbacks
async def set_visibility_cb(_, query):
    user_id = query.from_user.id
    visibility = query.data.split("_")[-1]
    
    settings = await get_user_settings(user_id)
    settings["visibility_youtube"] = visibility
    await save_user_settings(user_id, settings)
    
    await query.answer(f"✅ Default YouTube visibility set to {visibility}.", show_alert=True)
    await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Configure YouTube Settings:"), reply_markup=get_youtube_settings_markup())

@app.on_callback_query(filters.regex("^yt_code_received$"))
@rate_limit_callbacks
async def yt_code_received_cb(_, query):
    user_id = query.from_user.id
    user_states[user_id]["action"] = "waiting_for_yt_auth_code"
    await query.answer("OK, now send me the code or URL.", show_alert=True)
    await safe_edit_message(query.message, "🔑 " + to_bold_sans("Please paste the verification code or the full redirect URL here:"))

@app.on_callback_query(filters.regex("^toggle_auto_delete$"))
@rate_limit_callbacks
async def toggle_auto_delete_cb(_, query):
    user_id = query.from_user.id
    settings = await get_user_settings(user_id)
    current_status = settings.get("auto_delete_text", False)
    settings["auto_delete_text"] = not current_status
    await save_user_settings(user_id, settings)
    await query.answer(f"Auto-delete of your messages is now {'ON' if not current_status else 'OFF'}.")
    await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Settings Panel"), reply_markup=await get_main_settings_markup(user_id))

# ===================================================================
# ======================== MEDIA HANDLERS ===========================
# ===================================================================
async def process_upload_step(msg_or_query):
    """Central function to handle the step-by-step upload process."""
    if hasattr(msg_or_query, 'message') and msg_or_query.message:
        user_id = msg_or_query.from_user.id
        status_msg = msg_or_query.message
    else:
        user_id = msg_or_query.from_user.id
        state = user_states.get(user_id, {})
        status_msg = state.get('status_msg', msg_or_query)

    state_data = user_states[user_id]
    file_info = state_data["file_info"]
    platform = state_data["platform"]
    upload_type = state_data["upload_type"]
    user_settings = await get_user_settings(user_id)

    # BUG FIX: Simplified Reels Flow (Corrected Logic)
    if upload_type == "reel":
        if "title" not in file_info:
            state_data["action"] = "waiting_for_title"
            default_caption = user_settings.get(f'caption_{platform}')
            prompt = to_bold_sans("Reel Received. Please Send Your Caption.")
            if default_caption:
                prompt += f"\n\nOr use /skip to use your default: `{default_caption[:50]}`"
            await safe_edit_message(status_msg, prompt, parse_mode=enums.ParseMode.MARKDOWN)
            return
        elif "schedule_time" not in file_info:
            file_info.update({'description': "", 'tags': "", 'thumbnail_path': None, 'visibility': 'public'})
            state_data["action"] = "waiting_for_publish_choice"
            await safe_edit_message(status_msg, to_bold_sans("When To Publish Reel?"), reply_markup=get_upload_flow_markup(platform, 'publish'))
            return
    
    if "title" not in file_info:
        state_data["action"] = "waiting_for_title"
        default_title = user_settings.get(f'title_{platform}') or user_settings.get(f'caption_{platform}')
        prompt = to_bold_sans("Media Received. First, Send Your Title.") + "\n\n"
        prompt += "• " + "Send Text Now"
        if default_title:
            prompt += f"\n• Or use /skip to use your default: `{default_title[:50]}`"
        else:
            prompt += "\n• Or use /skip for no title."
        
        if platform == 'youtube':
            metadata = get_video_metadata(file_info['downloaded_path'])
            duration = float(metadata.get('format', {}).get('duration', '61'))
            if duration < 60:
                prompt += "\n\nℹ️ This video may be published as a YouTube Short."
        await safe_edit_message(status_msg, prompt, parse_mode=enums.ParseMode.MARKDOWN)

    elif "description" not in file_info:
        state_data["action"] = "waiting_for_description"
        default_desc = user_settings.get(f'description_{platform}')
        prompt = to_bold_sans("Next, Send Your Description.") + "\n\n"
        if default_desc:
            prompt += f"• Or use /skip to use your default: `{default_desc[:50]}`"
        else:
            prompt += "• Or use /skip for no description."
        await safe_edit_message(status_msg, prompt, parse_mode=enums.ParseMode.MARKDOWN)

    elif platform == 'youtube' and "tags" not in file_info:
        state_data["action"] = "waiting_for_tags"
        default_tags = user_settings.get(f'tags_{platform}')
        prompt = to_bold_sans("Now, Send Comma-separated Tags.") + "\n\n"
        if default_tags:
            prompt += f"• Or use /skip to use your default: `{default_tags[:50]}`"
        else:
            prompt += "• Or use /skip for no tags."
        await safe_edit_message(status_msg, prompt, parse_mode=enums.ParseMode.MARKDOWN)

    elif platform == 'youtube' and "thumbnail_path" not in file_info:
        is_video = file_info['original_media_msg'].video or (file_info['original_media_msg'].document and 'video' in file_info['original_media_msg'].document.mime_type)
        if not is_video:
            file_info['thumbnail_path'] = None
            return await process_upload_step(msg_or_query)
        state_data["action"] = "waiting_for_thumbnail_choice"
        await safe_edit_message(status_msg, to_bold_sans("Choose Thumbnail Option:"), reply_markup=get_upload_flow_markup(platform, 'thumbnail'))

    elif platform == 'youtube' and "visibility" not in file_info:
        state_data["action"] = "waiting_for_visibility_choice"
        await safe_edit_message(status_msg, to_bold_sans("Set Video Visibility:"), reply_markup=get_upload_flow_markup(platform, 'visibility'))

    elif "schedule_time" not in file_info:
        # Default visibility for Facebook
        if platform == 'facebook':
            file_info['visibility'] = 'public'
        state_data["action"] = "waiting_for_publish_choice"
        await safe_edit_message(status_msg, to_bold_sans("When To Publish?"), reply_markup=get_upload_flow_markup(platform, 'publish'))

    else:
        schedule_time = file_info.get("schedule_time")
        if schedule_time:
            await safe_edit_message(status_msg, "⏳ " + to_bold_sans("Scheduling your post..."))
            try:
                stored_msg = await file_info['original_media_msg'].forward(STORAGE_CHANNEL)
                job_details = {
                    "user_id": user_id, "platform": platform, "upload_type": upload_type,
                    "storage_msg_id": stored_msg.id, "schedule_time": schedule_time,
                    "status": "pending", "created_at": datetime.now(timezone.utc),
                    "metadata": {k: file_info.get(k) for k in ["title", "description", "tags", "visibility"]}
                }
                if db is not None:
                    job_id = (await asyncio.to_thread(db.scheduled_jobs.insert_one, job_details)).inserted_id
                    schedule_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🗓️ Manage Schedules", callback_data=f"manage_schedules_{platform}")]])
                    await safe_edit_message(status_msg, f"✅ **Scheduled!**\n\nYour post will be uploaded on `{schedule_time.strftime('%Y-%m-%d %H:%M')} UTC`.", reply_markup=schedule_markup)
                else:
                    await safe_edit_message(status_msg, "❌ **Scheduling Failed:** Database is offline.")
            except Exception as e:
                logger.error(f"Failed to forward media to storage channel: {e}", exc_info=True)
                await safe_edit_message(status_msg, f"❌ **Scheduling Failed:** Could not store the media file. Error: {e}")
            finally:
                if user_id in user_states: del user_states[user_id]
        else:
            state_data["action"] = "finalizing"
            await start_upload_task(status_msg, file_info, user_id)


@app.on_message(filters.media & filters.private)
@with_user_lock
async def handle_media_upload(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    state_data = user_states.get(user_id, {})

    # Handle payment proof submission
    if state_data and state_data.get("action") == "waiting_for_payment_proof":
        await msg.forward(ADMIN_ID)
        await app.send_message(
            ADMIN_ID, 
            f"👆 Payment proof from user: `{user_id}` (@{msg.from_user.username or 'N/A'})"
        )
        await msg.reply("✅ Your proof has been sent to the admin for verification. Please wait for confirmation.")
        if user_id in user_states: del user_states[user_id]
        return

    # Handle admin settings media (e.g., QR code)
    if is_admin(user_id) and state_data and state_data.get("action") == "waiting_for_google_play_qr" and msg.photo:
        payment_settings = global_settings.get("payment_settings", {})
        payment_settings["google_play_qr_file_id"] = msg.photo.file_id
        await _update_global_setting("payment_settings", payment_settings)
        if user_id in user_states: del user_states[user_id]
        return await msg.reply("✅ " + to_bold_sans("Google Pay QR Code Image Saved!"), reply_markup=payment_settings_markup)

    # Handle thumbnail upload during the flow
    if state_data and state_data.get("action") == 'waiting_for_thumbnail':
        if not msg.photo:
            return await msg.reply("❌ " + to_bold_sans("Please send an image file for the thumbnail."))
        
        status_msg = await msg.reply("🖼️ " + to_bold_sans("Downloading thumbnail..."))
        thumb_path = await app.download_media(msg.photo)
        state_data['file_info']['thumbnail_path'] = thumb_path
        await status_msg.delete()
        await process_upload_step(msg) # Move to next step
        return
        
    # Handle bulk media upload
    if state_data and state_data.get("action") == 'waiting_for_bulk_media':
        media_list = state_data.get("bulk_media", [])
        if len(media_list) >= 10:
            return await msg.reply("You have already sent 10 files. Please type /finish to proceed.")
        
        # We need to forward to storage to get a permanent message_id
        try:
            stored_msg = await msg.forward(STORAGE_CHANNEL)
            state_data["bulk_media"].append(stored_msg.id)
            await msg.reply(f"✅ File {len(media_list)+1}/10 received. Send more files or type /finish.")
        except Exception as e:
            logger.error(f"Failed to forward bulk media to storage: {e}")
            await msg.reply(f"❌ Error storing file. Please check STORAGE_CHANNEL_ID. Error: {e}")
        return


    # Main media handler for starting an upload
    action = state_data.get("action")
    if not action or action != "waiting_for_media":
        return

    media = msg.video or msg.photo or msg.document
    if not media: return await msg.reply("❌ " + to_bold_sans("Unsupported Media Type."))

    if media.file_size > MAX_FILE_SIZE_BYTES:
        if user_id in user_states: del user_states[user_id]
        return await msg.reply(f"❌ " + to_bold_sans(f"File Size Exceeds The Limit Of `{MAX_FILE_SIZE_BYTES / (1024 * 1024):.0f}` Mb."))

    # Download the file immediately
    status_msg = await msg.reply("⏳ " + to_bold_sans("Starting Download..."))
    state_data['status_msg'] = status_msg
    try:
        start_time = time.time()
        last_update_time = [0]
        task_tracker.create_task(monitor_progress_task(msg.chat.id, status_msg.id, status_msg), user_id=user_id, task_name="progress_monitor")
        
        downloaded_path = await app.download_media(
            msg,
            progress=download_progress_callback,
            progress_args=("Download", status_msg.id, msg.chat.id, start_time, last_update_time)
        )
        
        task_tracker.cancel_user_task(user_id, "progress_monitor")

        state_data["file_info"] = {
            "original_media_msg": msg,
            "downloaded_path": downloaded_path
        }
        await process_upload_step(msg) # Start the step-by-step process

    except Exception as e:
        logger.error(f"Error during file download for user {user_id}: {e}", exc_info=True)
        await safe_edit_message(status_msg, f"❌ " + to_bold_sans(f"Download Failed: {e}"))
        if user_id in user_states: del user_states[user_id]


# ===================================================================
# ==================== UPLOAD PROCESSING ==========================
# ===================================================================

async def start_upload_task(msg, file_info, user_id):
    task_tracker.create_task(
        safe_task_wrapper(process_and_upload(msg, file_info, user_id)),
        user_id=user_id,
        task_name="upload"
    )

async def process_and_upload(msg, file_info, user_id, from_schedule=False, job_id=None):
    platform, upload_type, processing_msg = None, None, None
    if not from_schedule:
        state_data = user_states[user_id]
        platform = state_data["platform"]
        upload_type = state_data["upload_type"]
        processing_msg = state_data.get("status_msg") or msg
    else:
        job = await asyncio.to_thread(db.scheduled_jobs.find_one, {"_id": ObjectId(job_id)})
        platform = job['platform']
        upload_type = job.get('upload_type', 'video')
        processing_msg = await app.send_message(user_id, "⏳ " + to_bold_sans(f"Starting your scheduled {upload_type}..."))


    async with upload_semaphore:
        logger.info(f"Semaphore acquired for user {user_id}. Starting upload to {platform}.")
        files_to_clean = [file_info.get("downloaded_path"), file_info.get("processed_path"), file_info.get("thumbnail_path")]
        try:
            user_settings = await get_user_settings(user_id)
            
            path = file_info.get("downloaded_path")
            if not path or not os.path.exists(path):
                raise FileNotFoundError("Downloaded file path is missing or invalid.")
            
            is_video = 'video' in getattr(file_info.get('original_media_msg', {}), 'document', {}).get('mime_type', '') or getattr(file_info.get('original_media_msg', {}), 'video', None)

            upload_path = path
            if is_video:
                await safe_edit_message(processing_msg, "⚙️ " + to_bold_sans("Processing Video... This May Take A Moment."))
                processed_path = path.rsplit(".", 1)[0] + "_processed.mp4"
                upload_path = await asyncio.to_thread(process_video_for_upload, path, processed_path)
                files_to_clean.append(processed_path)
            
            if platform == 'youtube' and file_info.get("thumbnail_path") == "auto":
                await safe_edit_message(processing_msg, "🖼️ " + to_bold_sans("Generating Smart Thumbnail..."))
                thumb_output_path = upload_path + ".jpg"
                generated_thumb = await asyncio.to_thread(generate_thumbnail, upload_path, thumb_output_path)
                file_info["thumbnail_path"] = generated_thumb
                files_to_clean.append(generated_thumb)

            _upload_progress['status'] = 'uploading'
            task_tracker.create_task(monitor_progress_task(user_id, processing_msg.id, processing_msg), user_id, "upload_monitor")
            
            url, media_id = "N/A", "N/A"
            final_title = file_info.get("title") or user_settings.get(f"title_{platform}") or user_settings.get(f"caption_{platform}") or "Untitled"
            
            if platform == "facebook":
                session = await get_active_session(user_id, 'facebook')
                if not session: raise ConnectionError("Facebook session not found. Please /fblogin.")
                
                page_id = session['id']
                token = session['access_token']
                final_description = (file_info.get("title") or "") + "\n\n" + (file_info.get("description", "") or "")

                if upload_type == 'post':
                    with open(upload_path, 'rb') as f:
                        post_url = f"https://graph.facebook.com/{page_id}/photos"
                        payload = {'access_token': token, 'caption': final_description}
                        files = {'source': f}
                        r = requests.post(post_url, data=payload, files=files, timeout=600)
                        r.raise_for_status()
                        post_id = r.json().get('post_id', r.json().get('id', 'N/A'))
                        url = f"https://facebook.com/{post_id}"
                        media_id = post_id
                
                elif upload_type == 'video':
                    file_size = os.path.getsize(upload_path)
                    init_url = f"https://graph-video.facebook.com/v18.0/{page_id}/videos"
                    init_data = {'access_token': token, 'upload_phase': 'start', 'file_size': file_size}
                    init_response = requests.post(init_url, data=init_data)
                    init_response.raise_for_status()
                    init_data = init_response.json()
                    video_id = init_data['video_id']
                    upload_session_url = init_data['upload_url']

                    upload_headers = {'Authorization': f'OAuth {token}'}
                    with open(upload_path, 'rb') as f:
                        upload_response = requests.post(upload_session_url, headers=upload_headers, data=f)
                        upload_response.raise_for_status()
                        
                    finish_data = {'access_token': token, 'upload_phase': 'finish', 'description': final_description}
                    
                    for _ in range(15): # Poll for up to 2.5 minutes
                        await asyncio.sleep(10)
                        publish_response = requests.post(init_url, data={'video_id': video_id, **finish_data})
                        if publish_response.status_code == 200 and publish_response.json().get('success'):
                            logger.info(f"Video {video_id} published successfully.")
                            break
                    else:
                        raise Exception("Facebook video publishing timed out.")
                        
                    media_id = video_id
                    url = f"https://facebook.com/{video_id}"

                elif upload_type == 'reel':
                    init_url = f"https://graph.facebook.com/v18.0/{page_id}/video_reels"
                    init_data = {'upload_phase': 'start', 'access_token': token}
                    init_response = requests.post(init_url, data=init_data)
                    init_response.raise_for_status()
                    upload_data = init_response.json()
                    video_id = upload_data['video_id']
                    upload_session_url = upload_data['upload_url']

                    file_size = os.path.getsize(upload_path)
                    upload_headers = {'Authorization': f'OAuth {token}', 'offset': '0', 'file_size': str(file_size)}
                    with open(upload_path, 'rb') as f:
                        upload_response = requests.post(upload_session_url, headers=upload_headers, data=f)
                        upload_response.raise_for_status()

                    status_check_url = f"https://graph.facebook.com/v18.0/{video_id}"
                    status_params = {'access_token': token, 'fields': 'status'}
                    
                    for _ in range(12):
                        await asyncio.sleep(10)
                        status_response = requests.get(status_check_url, params=status_params)
                        status_data = status_response.json()
                        video_status = status_data.get('status', {}).get('video_status')
                        if video_status == 'ready':
                            logger.info(f"Reel {video_id} is processed and ready.")
                            break
                    else:
                        raise Exception("Facebook Reel processing timed out.")

                    publish_data = {'access_token': token, 'video_id': video_id, 'upload_phase': 'finish', 'description': final_title}
                    publish_response = requests.post(init_url, data=publish_data)
                    publish_response.raise_for_status()
                    media_id = video_id
                    url = f"https://www.facebook.com/reel/{video_id}"
            
            elif platform == "youtube":
                session = await get_active_session(user_id, 'youtube')
                if not session: raise ConnectionError("YouTube session not found. Please /ytlogin.")
                
                creds = Credentials.from_authorized_user_info(json.loads(session['credentials_json']))
                if creds.expired and creds.refresh_token:
                    try:
                        creds.refresh(Request())
                        session['credentials_json'] = creds.to_json()
                        await save_platform_session(user_id, "youtube", session)
                    except RefreshError as e:
                        raise ConnectionError(f"YouTube token expired and failed to refresh. Please /ytlogin again. Error: {e}")

                youtube = build('youtube', 'v3', credentials=creds)
                tags = (file_info.get("tags") or user_settings.get("tags_youtube", "")).split(',')
                visibility = file_info.get("visibility") or user_settings.get("visibility_youtube", "private")
                thumbnail = file_info.get("thumbnail_path")
                schedule_time = file_info.get("schedule_time")

                body = {
                    "snippet": {
                        "title": final_title,
                        "description": file_info.get("description") or user_settings.get("description_youtube", ""),
                        "tags": [tag.strip() for tag in tags if tag.strip()]
                    },
                    "status": {
                        "privacyStatus": "private" if schedule_time else visibility,
                        "selfDeclaredMadeForKids": False
                    }
                }
                if schedule_time and isinstance(schedule_time, datetime):
                    body["status"]["publishAt"] = schedule_time.isoformat().replace("+00:00", "Z")

                media_file = MediaFileUpload(upload_path, chunksize=-1, resumable=True)
                request = youtube.videos().insert(part=",".join(body.keys()), body=body, media_body=media_file)
                
                response = None
                while response is None:
                    status, response = await asyncio.to_thread(request.next_chunk)
                    if status:
                        logger.info(f"Uploaded {int(status.progress() * 100)}%")
                
                media_id = response['id']
                url = f"https://youtu.be/{media_id}"

                if thumbnail and os.path.exists(thumbnail):
                    await asyncio.to_thread(
                        youtube.thumbnails().set(videoId=media_id, media_body=MediaFileUpload(thumbnail)).execute
                    )

            _upload_progress['status'] = 'complete'
            task_tracker.cancel_user_task(user_id, "upload_monitor")
            
            if db is not None:
                if not from_schedule:
                    await asyncio.to_thread(db.uploads.insert_one, {
                        "user_id": user_id, "media_id": str(media_id),
                        "platform": platform, "upload_type": upload_type, "timestamp": datetime.now(timezone.utc),
                        "url": url, "title": final_title
                    })
                else:
                    await asyncio.to_thread(db.scheduled_jobs.update_one, {"_id": ObjectId(job_id)}, {"$set": {"status": "completed", "final_url": url}})
                    await app.send_message(user_id, f"✅ **Scheduled Upload Complete!**\n\nYour {upload_type} '{final_title}' has been published:\n{url}")

            log_msg = f"📤 New {platform.capitalize()} {upload_type.capitalize()} Upload\n" \
                      f"👤 User: `{user_id}`\n🔗 URL: {url}\n" \
                      f"📅 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}"
            success_msg = f"✅ " + to_bold_sans("Uploaded Successfully!") + f"\n\n{url}"
            
            await safe_edit_message(processing_msg, success_msg, parse_mode=None)
            await send_log_to_channel(app, LOG_CHANNEL, log_msg)

        except (ConnectionError, RefreshError, requests.RequestException, FileNotFoundError, ValueError) as e:
            error_msg = f"❌ " + to_bold_sans(f"Upload Failed: {e}")
            await safe_edit_message(processing_msg, error_msg)
            if from_schedule and db is not None:
                await asyncio.to_thread(db.scheduled_jobs.update_one, {"_id": ObjectId(job_id)}, {"$set": {"status": "failed", "error_message": str(e)}})
                await app.send_message(user_id, f"❌ Your scheduled upload for '{final_title}' failed. Error: {e}")
            logger.error(f"Upload error for {user_id}: {e}", exc_info=True)
        except Exception as e:
            error_msg = f"❌ " + to_bold_sans(f"An Unexpected Error Occurred: {str(e)}")
            await safe_edit_message(processing_msg, error_msg)
            if from_schedule and db is not None:
                await asyncio.to_thread(db.scheduled_jobs.update_one, {"_id": ObjectId(job_id)}, {"$set": {"status": "failed", "error_message": str(e)}})
                await app.send_message(user_id, f"❌ Your scheduled upload for '{final_title}' failed. Error: {e}")

            logger.error(f"General upload failed for {user_id} on {platform}: {e}", exc_info=True)
        finally:
            cleanup_temp_files(files_to_clean)
            if not from_schedule and user_id in user_states:
                del user_states[user_id]
            _upload_progress.clear()
            logger.info(f"Semaphore released for user {user_id}.")

# === HTTP Server for OAuth and Health Checks ===
class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        
        query_components = parse_qs(urlparse(self.path).query)
        state = query_components.get('state', [None])[0]
        
        if state:
            html_content = """
            <html>
                <head><title>Authentication Successful</title></head>
                <body>
                    <h1>✅ Authentication Successful!</h1>
                    <p>You can close this window and return to Telegram.</p>
                    <p>Please copy the full URL from your browser's address bar and paste it back to the bot.</p>
                </body>
            </html>
            """
            self.wfile.write(html_content.encode('utf-8'))
        else:
            self.wfile.write(b"Bot is running. This is the OAuth redirect endpoint.")

def run_server():
    try:
        server = HTTPServer(('0.0.0.0', PORT), OAuthHandler)
        logger.info(f"HTTP OAuth/health server started on port {PORT}.")
        server.serve_forever()
    except Exception as e:
        logger.error(f"HTTP server failed: {e}")

async def send_log_to_channel(client, channel_id, text):
    global valid_log_channel
    if not channel_id or not valid_log_channel:
        return
    try:
        await client.send_message(channel_id, text, disable_web_page_preview=True, parse_mode=enums.ParseMode.MARKDOWN)
    except PeerIdInvalid:
        logger.error(f"Failed to log to channel {channel_id}: The ID is invalid. Please ensure the bot is an admin in the correct channel and the ID is correct.")
        valid_log_channel = False
    except Exception as e:
        logger.error(f"Failed to log to channel {channel_id}: {e}")
        valid_log_channel = False

# ===================================================================
# ======================== BOT STARTUP ============================
# ===================================================================
async def start_bot():
    global mongo, db, global_settings, upload_semaphore, MAX_CONCURRENT_UPLOADS, MAX_FILE_SIZE_BYTES, task_tracker, valid_log_channel, BOT_ID

    try:
        mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo.admin.command('ping')
        db = mongo.UploaderBotDB
        logger.info("✅ Connected to MongoDB successfully.")
        
        await asyncio.to_thread(db.scheduled_jobs.create_index, [("schedule_time", 1), ("status", 1)])
        
        settings_from_db = await asyncio.to_thread(db.settings.find_one, {"_id": "global_settings"}) or {}
        
        def merge_dicts(d1, d2):
            for k, v in d2.items():
                if k in d1 and isinstance(d1[k], dict) and isinstance(v, dict):
                    merge_dicts(d1[k], v)
                else:
                    d1[k] = v
        
        global_settings = DEFAULT_GLOBAL_SETTINGS.copy()
        merge_dicts(global_settings, settings_from_db)

        await asyncio.to_thread(db.settings.update_one, {"_id": "global_settings"}, {"$set": global_settings}, upsert=True)
        logger.info("Global settings loaded and synchronized.")
    except Exception as e:
        logger.critical(f"❌ DATABASE SETUP FAILED: {e}. Running in degraded mode (no data persistence).")
        db = None
        global_settings = DEFAULT_GLOBAL_SETTINGS

    MAX_CONCURRENT_UPLOADS = global_settings.get("max_concurrent_uploads")
    upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
    MAX_FILE_SIZE_BYTES = global_settings.get("max_file_size_mb") * 1024 * 1024

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    await app.start()
    me = await app.get_me()
    BOT_ID = me.id
    
    task_tracker.loop = asyncio.get_running_loop()

    if LOG_CHANNEL:
        try:
            await app.get_chat(LOG_CHANNEL)
            await app.send_message(LOG_CHANNEL, "✅ **" + to_bold_sans("Bot Is Now Online And Running!") + "**", parse_mode=enums.ParseMode.MARKDOWN)
            valid_log_channel = True
        except Exception as e:
            logger.error(f"Could not log to channel {LOG_CHANNEL}. Invalid ID or bot isn't an admin. Error: {e}")
            valid_log_channel = False

    logger.info(f"Bot is now online! ID: {BOT_ID}. Waiting for tasks...")
    task_tracker.create_task(weekly_report_scheduler())
    task_tracker.create_task(schedule_checker_task())
    await idle()

    logger.info("Shutting down...")
    await task_tracker.cancel_and_wait_all()
    await app.stop()
    if mongo:
        mongo.close()
    logger.info("Bot has been shut down gracefully.")
    
# NEW: Enhanced broadcast function
async def broadcast_message(admin_msg, text=None, photo=None, video=None, reply_markup=None):
    if db is None:
        return await admin_msg.reply("DB connection failed, cannot get user list.")

    users_cursor = await asyncio.to_thread(db.users.find, {})
    users = await asyncio.to_thread(list, users_cursor)
    sent_count, failed_count = 0, 0
    status_msg = await admin_msg.reply(f"📢 {to_bold_sans('Starting Broadcast...')}")
    
    for user in users:
        try:
            user_id = user["_id"]
            if user_id == ADMIN_ID or user_id == BOT_ID:
                continue
            
            if text:
                await app.send_message(user_id, text, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)
            elif photo:
                await app.send_photo(user_id, photo, caption=text, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)
            elif video:
                await app.send_video(user_id, video, caption=text, reply_markup=reply_markup, parse_mode=enums.ParseMode.MARKDOWN)
                
            sent_count += 1
            await asyncio.sleep(0.1)
        except UserIsBlocked:
            failed_count += 1
            logger.warning(f"User {user_id} has blocked the bot. Skipping.")
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to send broadcast to user {user_id}: {e}")
            
    await status_msg.edit_text(f"✅ **Broadcast finished!**\nSent to `{sent_count}` users, failed for `{failed_count}` users.")
    await send_log_to_channel(app, LOG_CHANNEL,
        f"📢 Broadcast by admin `{admin_msg.from_user.id}`\n"
        f"Sent: `{sent_count}`, Failed: `{failed_count}`"
    )

# NEW: Weekly analytics report scheduler
async def weekly_report_scheduler():
    while not shutdown_event.is_set():
        await asyncio.sleep(3600) # Check every hour
        now = datetime.now(timezone.utc)
        last_report_str = global_settings.get("last_weekly_report")
        
        if last_report_str:
            last_report_time = datetime.fromisoformat(last_report_str)
            if now - last_report_time < timedelta(days=7):
                continue

        await send_weekly_report()
        await _update_global_setting("last_weekly_report", now.isoformat())

async def send_weekly_report():
    if db is None or not valid_log_channel:
        return

    one_week_ago = datetime.now(timezone.utc) - timedelta(days=7)

    new_users = await asyncio.to_thread(db.users.count_documents, {"added_at": {"$gte": one_week_ago}})
    total_uploads_week = await asyncio.to_thread(db.uploads.count_documents, {"timestamp": {"$gte": one_week_ago}})
    
    new_premium_fb_count = await asyncio.to_thread(db.users.count_documents, {"premium.facebook.added_at": {"$gte": one_week_ago}})
    new_premium_yt_count = await asyncio.to_thread(db.users.count_documents, {"premium.youtube.added_at": {"$gte": one_week_ago}})

    report_text = (
        "📊 **" + to_bold_sans("Weekly Analytics Report") + "** 📊\n\n"
        f"📅 **Period:** {one_week_ago.strftime('%Y-%m-%d')} to {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n\n"
        f"**New Users:** `{new_users}`\n"
        f"**Total Uploads This Week:** `{total_uploads_week}`\n"
        f"**New Premium Members:**\n"
        f"  - Facebook: `{new_premium_fb_count}`\n"
        f"  - YouTube: `{new_premium_yt_count}`\n"
    )

    await send_log_to_channel(app, LOG_CHANNEL, report_text)
    
# NEW: Background worker for scheduled posts
async def schedule_checker_task():
    logger.info("Scheduler worker started.")
    while not shutdown_event.is_set():
        if db is not None:
            try:
                now = datetime.now(timezone.utc)
                due_jobs_cursor = db.scheduled_jobs.find({
                    "schedule_time": {"$lte": now},
                    "status": "pending"
                })
                
                due_jobs = await asyncio.to_thread(list, due_jobs_cursor)

                for job in due_jobs:
                    job_id_str = str(job['_id'])
                    logger.info(f"Processing scheduled job: {job_id_str}")
                    
                    await asyncio.to_thread(db.scheduled_jobs.update_one, {"_id": job['_id']}, {"$set": {"status": "processing"}})
                    
                    try:
                        stored_msg = await app.get_messages(STORAGE_CHANNEL, job['storage_msg_id'])
                        if not stored_msg:
                            raise FileNotFoundError(f"Message {job['storage_msg_id']} not found in storage channel.")
                        
                        downloaded_path = await app.download_media(stored_msg)

                        file_info = {
                            "original_media_msg": stored_msg,
                            "downloaded_path": downloaded_path,
                            **job['metadata']
                        }
                        
                        task_tracker.create_task(
                            safe_task_wrapper(process_and_upload(None, file_info, job['user_id'], from_schedule=True, job_id=job_id_str))
                        )

                    except Exception as e:
                        logger.error(f"Failed to process scheduled job {job_id_str}: {e}", exc_info=True)
                        await asyncio.to_thread(db.scheduled_jobs.update_one, {"_id": job['_id']}, {"$set": {"status": "failed", "error_message": str(e)}})
                        try:
                            await app.send_message(job['user_id'], f"❌ Your scheduled upload for '{job['metadata']['title']}' failed. Error: {e}")
                        except Exception as notify_e:
                            logger.error(f"Failed to notify user {job['user_id']} about failed schedule: {notify_e}")

            except Exception as e:
                logger.error(f"Error in scheduler worker loop: {e}", exc_info=True)

        await asyncio.sleep(60) # Check every minute
    logger.info("Scheduler worker stopped.")
    
# NEW: Schedule Management UI
@app.on_callback_query(filters.regex("^manage_schedules_"))
@rate_limit_callbacks
async def manage_schedules_cb(_, query):
    user_id = query.from_user.id
    platform = query.data.split("_")[-1]
    
    if db is None:
        return await query.answer("Database is offline.", show_alert=True)
        
    jobs_cursor = db.scheduled_jobs.find({
        "user_id": user_id,
        "platform": platform,
        "status": "pending"
    }).sort("schedule_time", 1)
    
    jobs = await asyncio.to_thread(list, jobs_cursor)
    
    if not jobs:
        await safe_edit_message(query.message, f"You have no pending scheduled posts for {platform.capitalize()}.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"🔙 Back to {platform.capitalize()} Settings", callback_data=f"hub_settings_{platform}")]]))
        return

    text = f"🗓️ **Your Pending {platform.capitalize()} Schedules:**\n\n"
    buttons = []
    for job in jobs:
        title = (job['metadata'].get('title') or "Untitled")[:30]
        time_str = job['schedule_time'].strftime('%Y-%m-%d %H:%M')
        job_id = str(job['_id'])
        
        buttons.append([
            InlineKeyboardButton(f"'{title}' at {time_str}", callback_data=f"noop"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_schedule_{job_id}")
        ])
    
    buttons.append([InlineKeyboardButton(f"🔙 Back to {platform.capitalize()} Settings", callback_data=f"hub_settings_{platform}")])
    
    await safe_edit_message(query.message, text, reply_markup=InlineKeyboardMarkup(buttons))

@app.on_callback_query(filters.regex("^cancel_schedule_"))
@rate_limit_callbacks
async def cancel_schedule_cb(_, query):
    user_id = query.from_user.id
    job_id = query.data.split("_")[-1]

    if db is None:
        return await query.answer("Database is offline.", show_alert=True)
    
    job = await asyncio.to_thread(db.scheduled_jobs.find_one_and_delete, {"_id": ObjectId(job_id), "user_id": user_id})
    
    if job:
        await query.answer("Scheduled post cancelled successfully!", show_alert=True)
        platform = job.get("platform", "facebook")
        class MockQuery:
            def __init__(self, user, message, data):
                self.from_user = user
                self.message = message
                self.data = data
        await manage_schedules_cb(app, MockQuery(query.from_user, query.message, f'manage_schedules_{platform}'))
    else:
        await query.answer("Could not find the scheduled post or you don't have permission.", show_alert=True)


if __name__ == "__main__":
    task_tracker = TaskTracker()
    try:
        app.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Bot crashed during startup: {e}", exc_info=True)
