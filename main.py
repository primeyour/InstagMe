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
from pyrogram import Client, filters, enums, idle, types
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
REDIRECT_URI = os.getenv("REDIRECT_URI", "https://absent-dulcea-primeyour-bcdf24ed.koyeb.app/")
PORT_STR = os.getenv("PORT", "8080")


# Validate required environment variables
if not all([API_ID_STR, API_HASH, BOT_TOKEN, ADMIN_ID_STR, MONGO_URI]):
    logger.critical("FATAL ERROR: One or more required environment variables are missing.")
    sys.exit(1)

# Convert to correct types after validation
API_ID = int(API_ID_STR)
ADMIN_ID = int(ADMIN_ID_STR)
LOG_CHANNEL = int(LOG_CHANNEL_STR) if LOG_CHANNEL_STR else None
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
    """Intelligently generates a thumbnail."""
    try:
        logger.info(f"Generating intelligent thumbnail for {video_path}...")
        ffmpeg_command = [
            'ffmpeg', '-i', video_path,
            '-vf', "select='gt(scene,0.4)',scale=1280:-1",
            '-frames:v', '1', '-q:v', '2',
            output_path, '-y'
        ]
        subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True)
        logger.info(f"Thumbnail saved to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Thumbnail generation failed: {e}. Falling back to a random frame.")
        try:
            fallback_command = [
                'ffmpeg', '-i', video_path, '-ss', str(random.randint(1, 29)),
                '-vframes', '1', '-q:v', '2', output_path, '-y'
            ]
            subprocess.run(fallback_command, check=True, capture_output=True, text=True)
            return output_path
        except Exception as fallback_e:
            logger.error(f"Fallback thumbnail generation also failed: {fallback_e}")
            return None

def needs_conversion(input_file: str) -> bool:
    """Checks if a video file needs conversion to be web-compatible."""
    metadata = get_video_metadata(input_file)
    if not metadata:
        logger.warning("Could not get metadata, assuming conversion is needed.")
        return True

    v_codec = None
    a_codec = None
    container = metadata.get('format', {}).get('format_name', '')

    for stream in metadata.get('streams', []):
        if stream.get('codec_type') == 'video':
            v_codec = stream.get('codec_name')
        elif stream.get('codec_type') == 'audio':
            a_codec = stream.get('codec_name')

    if v_codec == 'h264' and a_codec == 'aac' and ('mp4' in container or 'mov' in container):
        logger.info(f"'{input_file}' is already compatible (H.264/AAC in {container}). No conversion needed.")
        return False
    
    logger.warning(f"'{input_file}' needs conversion (Video: {v_codec}, Audio: {a_codec}, Container: {container}).")
    return True

def process_video_for_upload(input_file: str, output_file: str) -> str:
    """
    Intelligently converts a video file. It stream-copies compatible tracks 
    and only re-encodes what is necessary.
    """
    metadata = get_video_metadata(input_file)
    if not metadata:
        raise ValueError("Could not get video metadata to perform conversion.")

    v_codec = None
    a_codec = None
    for stream in metadata.get('streams', []):
        if stream.get('codec_type') == 'video':
            v_codec = stream.get('codec_name')
        elif stream.get('codec_type') == 'audio':
            a_codec = stream.get('codec_name')

    # Build the FFmpeg command dynamically
    command = ['ffmpeg', '-y', '-i', input_file]

    # Video stream handling
    if v_codec == 'h264':
        logger.info("Video stream is compatible (h264). Copying without re-encoding.")
        command.extend(['-c:v', 'copy'])
    else:
        logger.warning(f"Video stream '{v_codec}' is not h264. Re-encoding.")
        command.extend(['-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23'])

    # Audio stream handling
    if a_codec == 'aac':
        logger.info("Audio stream is compatible (aac). Copying without re-encoding.")
        command.extend(['-c:a', 'copy'])
    else:
        logger.warning(f"Audio stream '{a_codec}' is not aac. Re-encoding.")
        command.extend(['-c:a', 'aac', '-b:a', '128k'])

    command.extend(['-movflags', '+faststart', output_file])

    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"Successfully processed video to '{output_file}'.")
        return output_file
    except FileNotFoundError:
        raise FileNotFoundError("ffmpeg is not installed. Video processing is not possible.")
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg conversion failed for {input_file}. Error: {e.stderr}")
        raise ValueError("Video conversion failed.")


# === Global Bot Settings ===
DEFAULT_GLOBAL_SETTINGS = {
    "special_event_toggle": False,
    "special_event_title": "🎉 Special Event!",
    "special_event_message": "Enjoy our special event features!",
    "max_concurrent_uploads": 15,
    "max_file_size_mb": 2000,
    "allow_multiple_logins": False,
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
    sanitized_text = text.encode('utf-8', 'surrogatepass').decode('utf-8')
    return ''.join(bold_sans_map.get(char, char) for char in sanitized_text)

# State dictionary to hold user states
user_states = {}

# Button Spam Protection
user_clicks = defaultdict(lambda: {'count': 0, 'time': 0})
SPAM_LIMIT = 10
SPAM_WINDOW = 10

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
        [KeyboardButton("📊 Dashboard"), KeyboardButton("👤 Account Info")],
        [KeyboardButton("⚙️ ꜱᴇᴛᴛɪɴɢꜱ")]
    ]
    fb_buttons = []
    yt_buttons = []
    
    if "facebook" in premium_platforms:
        fb_buttons.extend([
            KeyboardButton("📘 FB ᴩᴏꜱᴛ"),
            KeyboardButton("📘 FB ᴠɪᴅᴇᴏ"),
            KeyboardButton("📘 FB ʀᴇᴇʟꜱ"),
        ])
    if "youtube" in premium_platforms:
        yt_buttons.extend([
            KeyboardButton("▶️ YT ᴠɪᴅᴇᴏ"),
            KeyboardButton("🟥 YT ꜱʜᴏʀᴛꜱ"),
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
    buttons = []
    
    if await is_premium_for_platform(user_id, "facebook"):
        buttons.append([InlineKeyboardButton("📘 ғᴀᴄᴇʙᴏᴏᴋ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="hub_settings_facebook")])
    if await is_premium_for_platform(user_id, "youtube"):
        buttons.append([InlineKeyboardButton("▶️ yᴏᴜᴛᴜʙᴇ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="hub_settings_youtube")])

    buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")])
    return InlineKeyboardMarkup(buttons)


def get_facebook_settings_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 ᴅᴇғᴀᴜʟᴛ ᴄᴀᴩᴛɪᴏɴ", callback_data="set_caption_facebook")],
        [InlineKeyboardButton("📄 ᴅᴇғᴀᴜʟᴛ ᴅᴇꜱᴄʀɪᴩᴛɪᴏɴ", callback_data="set_description_facebook")],
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
        buttons.append([InlineKeyboardButton("❌ ʟᴏɢᴏᴜᴛ / ᴄʜᴀɴɢᴇ ᴀᴄᴄᴏᴜɴᴛ", callback_data=f"manage_logout_{platform}_{active_account_id}")])

    buttons.append([InlineKeyboardButton("➕ ᴀᴅᴅ ɴᴇᴡ ᴀᴄᴄᴏᴜɴᴛ", callback_data=f"add_account_{platform}")])
    buttons.append([InlineKeyboardButton(f"🔙 ʙᴀᴄᴋ ᴛᴏ {platform.upper()} ꜱᴇᴛᴛɪɴɢꜱ", callback_data=f"hub_settings_{platform}")])
    return InlineKeyboardMarkup(buttons)

def get_logout_options_markup(platform, account_id, account_name):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔑 Change Page Token Only", callback_data=f"change_page_token_{platform}_{account_id}")],
        [InlineKeyboardButton(f"🗑️ Complete Logout (App & Page)", callback_data=f"confirm_logout_{platform}_{account_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"manage_fb_accounts")]
    ])

def get_logout_confirm_markup(platform, account_id, account_name):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"✅ Yes, Logout Completely", callback_data=f"logout_acc_{platform}_{account_id}")],
        [InlineKeyboardButton("❌ No, Cancel", callback_data=f"manage_fb_accounts")]
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
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_upload")]
    ])

def get_upload_flow_markup(platform, step):
    buttons = []
    if step == "thumbnail":
        buttons.extend([
            [InlineKeyboardButton("🖼️ Upload Thumbnail", callback_data="upload_flow_thumbnail_custom")],
            [InlineKeyboardButton("🤖 Auto-Generate", callback_data="upload_flow_thumbnail_auto")]
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
    elif step == "input":
         buttons.append([InlineKeyboardButton("➡️ Skip", callback_data=f"upload_flow_input_skip")])


    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_upload")])
    return InlineKeyboardMarkup(buttons)

# ===================================================================
# ====================== HELPER FUNCTIONS ===========================
# ===================================================================

def check_fb_response(response):
    """Checks for HTTP and Facebook API errors in a requests response."""
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError(f"Facebook returned an invalid, non-JSON response: {response.text}")
    if 'error' in data:
        error_details = data['error']
        raise requests.RequestException(
            f"Facebook API Error ({error_details.get('code', 'N/A')}): {error_details.get('message', 'Unknown error')}"
        )
    return data

async def safe_edit_message(message, text, reply_markup=None):
    """Safely edits a message, ignoring 'message not modified' errors."""
    try:
        if not message:
            logger.warning("safe_edit_message called with a None message object.")
            return
        await message.edit_text(
            text=text, 
            reply_markup=reply_markup, 
            parse_mode=enums.ParseMode.MARKDOWN
        )
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            logger.warning(f"Couldn't edit message: {e}")

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
    if db is None:
        logger.warning(f"DB not connected. Skipping save for global setting '{key}'.")
        return
    await asyncio.to_thread(db.settings.update_one, {"_id": "global_settings"}, {"$set": {key: value}}, upsert=True)

async def is_premium_for_platform(user_id, platform):
    if user_id == ADMIN_ID:
        return True
    
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

    if premium_until and isinstance(premium_until, datetime) and premium_until.tzinfo is None:
        premium_until = premium_until.replace(tzinfo=timezone.utc)

    if premium_type == "lifetime":
        return True

    if premium_until and isinstance(premium_until, datetime) and premium_until > datetime.now(timezone.utc):
        return True

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
    if db is None: return
    
    if not is_admin(user_id):
        existing_sessions_count = await asyncio.to_thread(db.sessions.count_documents, {"user_id": user_id, "platform": platform})
        if existing_sessions_count >= 2:
            raise ValueError("Premium users can add a maximum of 2 accounts per platform.")

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
    if db is None: return []
    sessions = await asyncio.to_thread(list, db.sessions.find({"user_id": user_id, "platform": platform}))
    return sessions

async def get_active_session(user_id, platform):
    user_settings = await get_user_settings(user_id)
    active_id = user_settings.get(f"active_{platform}_id")
    if not active_id or db is None:
        return None
    
    session = await asyncio.to_thread(db.sessions.find_one, {"user_id": user_id, "platform": platform, "account_id": active_id})
    return session.get("session_data") if session else None

async def delete_platform_session(user_id, platform, account_id):
    if db is None: return
    await asyncio.to_thread(db.sessions.delete_one, {"user_id": user_id, "platform": platform, "account_id": account_id})

async def save_user_settings(user_id, settings):
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
    
    settings.setdefault("caption_facebook", "")
    settings.setdefault("description_facebook", "")
    settings.setdefault("active_facebook_id", None)
    settings.setdefault("title_youtube", "")
    settings.setdefault("description_youtube", "")
    settings.setdefault("tags_youtube", "")
    settings.setdefault("visibility_youtube", "private")
    settings.setdefault("active_youtube_id", None)
    
    return settings

async def safe_threaded_reply(original_media_message, new_text=None, new_markup=None, status_message=None):
    """Handles all replies and edits within the media's thread."""
    if not original_media_message:
        logger.error("safe_threaded_reply called without an original_media_message.")
        return None

    try:
        parse_mode = enums.ParseMode.MARKDOWN
        if status_message:
            await safe_edit_message(status_message, new_text, new_markup)
            return status_message
        else:
            return await original_media_message.reply(text=new_text, reply_markup=new_markup, parse_mode=parse_mode, quote=True)
    except Exception as e:
        logger.warning(f"Could not edit/reply in thread: {e}")
        return status_message

async def restart_bot(msg):
    restart_msg_log = (
        "🔄 **Bot Restart Initiated (Graceful)**\n\n"
        f"👤 **By**: {msg.from_user.mention} (ID: `{msg.from_user.id}`)"
    )
    logger.info(f"User {msg.from_user.id} initiated graceful restart.")
    await send_log_to_channel(app, LOG_CHANNEL, restart_msg_log)
    await msg.reply(
        to_bold_sans("Graceful Restart Initiated...") + "\n\n"
        "The bot will shut down cleanly. It should restart automatically if using a process manager."
    )
    shutdown_event.set()

_progress_updates = {}
_upload_progress = {}

def download_progress_callback(current, total, ud_type, msg_id, chat_id, start_time, last_update_time):
    now = time.time()
    if now - last_update_time[0] < 2 and current != total:
        return
    last_update_time[0] = now
    
    with threading.Lock():
        _progress_updates[(chat_id, msg_id)] = {
            "current": current, "total": total, "ud_type": ud_type, "start_time": start_time, "now": now
        }

async def monitor_progress_task(original_media_msg, status_msg, action_text="Downloading"):
    """Monitors and updates the progress of a download or upload."""
    try:
        while True:
            await asyncio.sleep(2)
            
            if action_text.startswith("Uploading to") and 'progress' in _upload_progress:
                percentage = _upload_progress['progress']
                progress_bar = f"[{'█' * int(percentage / 5)}{' ' * (20 - int(percentage / 5))}]"
                progress_text = (
                    f"⬆️ {to_bold_sans(action_text)}: `{progress_bar}`\n"
                    f"📊 **Percentage**: `{percentage:.2f}%`\n"
                )
                await safe_threaded_reply(original_media_msg, progress_text, get_progress_markup(), status_msg)
                if percentage >= 100:
                    break
                continue

            with threading.Lock():
                update_data = _progress_updates.get((status_msg.chat.id, status_msg.id))
            
            if update_data:
                current, total, _, start_time, now = (
                    update_data['current'], update_data['total'], update_data['ud_type'],
                    update_data['start_time'], update_data['now']
                )
                if total == 0: continue
                percentage = current * 100 / total
                speed = current / (now - start_time) if (now - start_time) > 0 else 0
                eta_seconds = (total - current) / speed if speed > 0 else 0
                eta = timedelta(seconds=int(eta_seconds))
                progress_bar = f"[{'█' * int(percentage / 5)}{' ' * (20 - int(percentage / 5))}]"
                progress_text = (
                    f"⬇️ {to_bold_sans(f'{action_text} Progress')}: `{progress_bar}`\n"
                    f"📊 **Percentage**: `{percentage:.2f}%`\n"
                    f"✅ **Done**: `{current / (1024 * 1024):.2f}` MB / `{total / (1024 * 1024):.2f}` MB\n"
                    f"🚀 **Speed**: `{speed / (1024 * 1024):.2f}` MB/s\n"
                    f"⏳ **ETA**: `{eta}`"
                )
                await safe_threaded_reply(original_media_msg, progress_text, get_progress_markup(), status_msg)
                
                if current == total:
                    with threading.Lock():
                        _progress_updates.pop((status_msg.chat.id, status_msg.id), None)
                    break

            elif _upload_progress.get('status') == 'complete':
                _upload_progress.clear()
                break

    except asyncio.CancelledError:
        logger.info(f"Progress monitor task for msg {status_msg.id} was cancelled.")
    finally:
        _upload_progress.clear()


def cleanup_temp_files(files_to_delete):
    for file_path in files_to_delete:
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")

def with_user_lock(func):
    """Decorator to prevent a user from running multiple commands simultaneously."""
    @wraps(func)
    async def wrapper(client, message, *args, **kwargs):
        user_id = message.from_user.id
        if user_id not in user_upload_locks:
            user_upload_locks[user_id] = asyncio.Lock()

        if user_upload_locks[user_id].locked():
            return await message.reply("⚠️ " + to_bold_sans("Another Operation Is Already In Progress. Please Wait Or Cancel."))
        
        async with user_upload_locks[user_id]:
            return await func(client, message, *args, **kwargs)
    return wrapper

def rate_limit_callbacks(func):
    """Decorator to prevent button spam."""
    @wraps(func)
    async def wrapper(client, query, *args, **kwargs):
        user_id = query.from_user.id
        now = time.time()
        
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
        logger.info(f"New user {user_id} added via /start")
        await send_log_to_channel(app, LOG_CHANNEL, f"🌟 New user: `{user_id}` (`{msg.from_user.username or 'N/A'}`)")
        welcome_msg = (
            f"👋 **Hi {user_first_name}!**\n\n"
            + to_bold_sans("This Bot Uploads To Facebook & YouTube Directly From Telegram.") + "\n\n"
            + to_bold_sans("Activate A Free 6-hour Trial To Test Premium Features!")
        )
        trial_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Activate FREE FB Trial", callback_data="activate_trial_facebook")],
            [InlineKeyboardButton("✅ Activate FREE YT Trial", callback_data="activate_trial_youtube")],
            [InlineKeyboardButton("➡️ View premium plans", callback_data="buypypremium")]
        ])
        await msg.reply(welcome_msg, reply_markup=trial_markup, parse_mode=enums.ParseMode.MARKDOWN)
        return
    else:
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

            if p_expiry and isinstance(p_expiry, datetime) and p_expiry.tzinfo is None:
                p_expiry = p_expiry.replace(tzinfo=timezone.utc)

            if p_expiry:
                remaining = p_expiry - datetime.now(timezone.utc)
                if remaining.total_seconds() > 0:
                    premium_details_text += f"⭐ {platform.capitalize()} premium expires in: `{remaining.days}d, {remaining.seconds // 3600}h`.\n"
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

@app.on_message(filters.command(["fblogin", "flogin"]))
@with_user_lock
async def facebook_login_cmd_new(_, msg):
    user_id = msg.from_user.id
    if not await is_premium_for_platform(user_id, "facebook"):
        return await msg.reply("❌ " + to_bold_sans("Facebook Premium Is Required. Use ") + "`/premiumplan`" + to_bold_sans(" To Upgrade."))
    
    prompt_msg = await msg.reply(to_bold_sans("🔑 Please Enter Your Facebook App ID."), quote=True)
    user_states[user_id] = {
        "action": "waiting_for_fb_app_id",
        "platform": "facebook",
        "login_data": {},
        "prompt_msg": prompt_msg,
        "secret_messages": [msg.id] 
    }

@app.on_message(filters.command(["ytlogin", "ylogin"]))
@with_user_lock
async def youtube_login_cmd_new(_, msg):
    user_id = msg.from_user.id
    if not await is_premium_for_platform(user_id, "youtube"):
        return await msg.reply("❌ " + to_bold_sans("YouTube Premium Is Required. Use ") + "`/premiumplan`" + to_bold_sans(" To Upgrade."))
    
    prompt_msg = await msg.reply(to_bold_sans("🔑 Please Enter Your YouTube OAuth Client ID."), quote=True)
    user_states[user_id] = {
        "action": "waiting_for_yt_client_id",
        "platform": "youtube",
        "login_data": {},
        "prompt_msg": prompt_msg,
        "secret_messages": [msg.id]
    }


@app.on_message(filters.command(["buypypremium", "premiumplan"]))
@app.on_message(filters.regex("⭐ ᴩʀᴇᴍɪᴜᴍ"))
async def show_premium_options(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    premium_plans_text = (
        "⭐ " + to_bold_sans("Upgrade To Premium!") + " ⭐\n\n"
        + to_bold_sans("Unlock Full Features And Upload Unlimited Content.") + "\n\n"
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
        return await msg.reply("👑 " + to_bold_sans("You Are The Admin. You Have Full Access!"))

    status_text = "⭐ " + to_bold_sans("Your Premium Status:") + "\n\n"
    has_premium_any = False
    for platform in PREMIUM_PLATFORMS:
        if await is_premium_for_platform(user_id, platform):
            has_premium_any = True
            platform_premium = user.get("premium", {}).get(platform, {})
            premium_type = platform_premium.get("type")
            premium_until = platform_premium.get("until")

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
    
@app.on_message(filters.command("leaderboard"))
async def leaderboard_cmd(_, msg):
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
        reply_markup=admin_markup
    )

@app.on_message(filters.regex("📊 Dashboard") | filters.command("stats"))
@with_user_lock
async def show_stats(_, msg_or_query):
    if hasattr(msg_or_query, 'message'): # It's a callback query
        user_id = msg_or_query.from_user.id
        message = msg_or_query.message
        is_callback = True
    else: # It's a regular message
        user_id = msg_or_query.from_user.id
        message = msg_or_query
        is_callback = False

    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    if db is None: 
        text = "⚠️ " + to_bold_sans("Database Is Currently Unavailable.")
        if is_callback:
            return await msg_or_query.answer(text, show_alert=True)
        else:
            return await message.reply(text)

    if not is_admin(user_id):
        user_uploads = await asyncio.to_thread(db.uploads.count_documents, {'user_id': user_id})
        stats_text = (
            f"📊 **{to_bold_sans('Your Dashboard:')}**\n\n"
            f"📈 **Total Uploads:** `{user_uploads}`\n"
        )
        for p in PREMIUM_PLATFORMS:
            platform_uploads = await asyncio.to_thread(db.uploads.count_documents, {'user_id': user_id, 'platform': p})
            stats_text += f"    - {p.capitalize()}: `{platform_uploads}`\n"
        await message.reply(stats_text)
        return

    # Admin Stats
    total_users = await asyncio.to_thread(db.users.count_documents, {})
    
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
        return await message.reply("⚠️ " + to_bold_sans("Could Not Fetch Bot Statistics."))

    total_premium_users = result[0].get('total_premium', 0) if result else 0
    premium_counts = {p: result[0].get(f'{p}_premium', 0) if result else 0 for p in PREMIUM_PLATFORMS}
    total_uploads = await asyncio.to_thread(db.uploads.count_documents, {})
    
    stats_text = (
        f"📊 **{to_bold_sans('Bot Dashboard:')}**\n\n"
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
    
    if is_callback:
        await safe_edit_message(message, stats_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]))
    else:
        await message.reply(stats_text)

@app.on_message(filters.regex("👤 Account Info"))
async def account_info_handler(_, msg):
    user_id = msg.from_user.id
    if is_admin(user_id):
        # For admin, this button can show their own info. Checking others is in admin panel.
        pass

    info_text = f"👤 **{to_bold_sans('Account Information')}**\n\n"
    has_any_session = False

    # Check Facebook Sessions
    fb_sessions = await load_platform_sessions(user_id, 'facebook')
    if fb_sessions:
        has_any_session = True
        info_text += "**Facebook Accounts:**\n"
        for session in fb_sessions:
            s_data = session.get('session_data', {})
            page_name = s_data.get('name', 'N/A')
            expiry = s_data.get('expires_at')
            info_text += f"  - **Page:** {page_name}\n"
            if expiry:
                expiry_dt = datetime.fromtimestamp(expiry, tz=timezone.utc)
                remaining = expiry_dt - datetime.now(timezone.utc)
                if remaining.total_seconds() > 0:
                    info_text += f"    - **Token Expires in:** `{remaining.days}d, {remaining.seconds // 3600}h`\n"
                else:
                    info_text += f"    - **Token:** `Expired`\n"
            else:
                info_text += f"    - **Token:** `Long-Lived (No Expiry Date Stored)`\n"

    # Check YouTube Sessions
    yt_sessions = await load_platform_sessions(user_id, 'youtube')
    if yt_sessions:
        has_any_session = True
        info_text += "\n**YouTube Accounts:**\n"
        for session in yt_sessions:
            s_data = session.get('session_data', {})
            channel_name = s_data.get('name', 'N/A')
            info_text += f"  - **Channel:** {channel_name}\n"
            try:
                creds = Credentials.from_authorized_user_info(json.loads(s_data['credentials_json']))
                expiry = creds.expiry
                remaining = expiry - datetime.now(timezone.utc)
                if remaining.total_seconds() > 0:
                    info_text += f"    - **Token Expires in:** `{remaining.days}d, {remaining.seconds // 3600}h`\n"
                else:
                    info_text += f"    - **Token:** `Expired` (Will attempt to refresh on next use)\n"
            except Exception as e:
                logger.error(f"Could not parse YouTube credentials for user {user_id}: {e}")
                info_text += f"    - **Token:** `Info Unavailable`\n"


    if not has_any_session:
        info_text += "You have not logged into any accounts yet. Use `/fblogin` or `/ytlogin` to get started."

    await msg.reply(info_text)


@app.on_message(filters.regex("^(📘 FB ᴩᴏꜱᴛ|📘 FB ᴠɪᴅᴇᴏ|📘 FB ʀᴇᴇʟꜱ|▶️ YT ᴠɪᴅᴇᴏ|🟥 YT ꜱʜᴏʀᴛꜱ)"))
@with_user_lock
async def initiate_upload(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})

    type_map = {
        "📘 FB ᴩᴏꜱᴛ": ("facebook", "post"),
        "📘 FB ᴠɪᴅᴇᴏ": ("facebook", "video"),
        "📘 FB ʀᴇᴇʟꜱ": ("facebook", "reels"),
        "▶️ YT ᴠɪᴅᴇᴏ": ("youtube", "video"),
        "🟥 YT ꜱʜᴏʀᴛꜱ": ("youtube", "short"),
    }
    platform, upload_type = type_map[msg.text]
    
    if not await is_premium_for_platform(user_id, platform):
        return await msg.reply(f"❌ " + to_bold_sans(f"Your Access Is Denied. Please Upgrade To {platform.capitalize()} Premium."))

    sessions = await load_platform_sessions(user_id, platform)
    if not sessions:
        return await msg.reply(f"❌ " + to_bold_sans(f"Please Login To {platform.capitalize()} First Using `/{'f' if platform == 'facebook' else 'y'}login`"), parse_mode=enums.ParseMode.MARKDOWN)
    
    action = f"waiting_for_media"
    user_states[user_id] = {
        "action": action,
        "platform": platform,
        "upload_type": upload_type,
        "file_info": {}
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
        return
    
    if "secret_messages" in state_data:
        state_data["secret_messages"].append(msg.id)

    action = state_data.get("action")
    prompt_msg = state_data.get("prompt_msg")
        
    if action == "waiting_for_fb_app_id":
        state_data["login_data"]["app_id"] = msg.text.strip()
        state_data["action"] = "waiting_for_fb_app_secret"
        await prompt_msg.edit(to_bold_sans("🔑 Please Enter Your Facebook App Secret."))
    
    elif action == "waiting_for_fb_app_secret":
        state_data["login_data"]["app_secret"] = msg.text.strip()
        state_data["action"] = "waiting_for_fb_page_token"
        await prompt_msg.edit(
            to_bold_sans("🔑 Please Enter Your Facebook Page API Token.") +
            "\n\nThis is a Page Access Token, not a User Token."
        )

    elif action == "waiting_for_fb_page_token":
        token = msg.text.strip()
        app_id = state_data["login_data"]["app_id"]
        app_secret = state_data["login_data"]["app_secret"]
        
        await prompt_msg.edit("🔐 " + to_bold_sans("Validating and extending token..."))
        
        try:
            # Exchange for a long-lived token
            exchange_url = (f"https://graph.facebook.com/v19.0/oauth/access_token?"
                            f"grant_type=fb_exchange_token&"
                            f"client_id={app_id}&"
                            f"client_secret={app_secret}&"
                            f"fb_exchange_token={token}")
            exchange_res = requests.get(exchange_url)
            token_data = check_fb_response(exchange_res)
            long_lived_token = token_data['access_token']
            expires_in = token_data.get('expires_in', 5184000) # Default to 60 days
            expires_at = int(time.time()) + expires_in

            # Get Page ID and Name
            page_url = f"https://graph.facebook.com/v19.0/me?access_token={long_lived_token}&fields=id,name,picture.type(large)"
            page_res = requests.get(page_url)
            page_data = check_fb_response(page_res)
            
            page_id = page_data.get('id')
            page_name = page_data.get('name')
            page_picture_url = page_data.get('picture', {}).get('data', {}).get('url')

            if not page_id or not page_name:
                raise ValueError("Could not fetch Page ID/Name. Please ensure it's a valid Page Access Token.")

            session_data = {
                'id': page_id, 'name': page_name, 'picture_url': page_picture_url,
                'app_id': app_id, 'app_secret': app_secret,
                'access_token': long_lived_token, 'expires_at': expires_at
            }
            await save_platform_session(user_id, "facebook", session_data)
            
            user_settings = await get_user_settings(user_id)
            user_settings["active_facebook_id"] = page_id
            await save_user_settings(user_id, user_settings)
            
            success_caption = (
                f"✅ **Login Successful!**\n\n"
                f"**Page Name:** `{page_name}`\n"
                f"**Page ID:** `{page_id}`\n\n"
                "This is now your active Facebook account."
            )
            await prompt_msg.delete()
            if state_data.get("secret_messages"):
                await app.delete_messages(user_id, state_data["secret_messages"])

            if page_picture_url:
                await msg.reply_photo(photo=page_picture_url, caption=success_caption)
            else:
                await msg.reply(success_caption)
            
            await send_log_to_channel(app, LOG_CHANNEL, f"📝 FB Login: User `{user_id}`, Page: `{page_name}`")
        
        except (requests.RequestException, ValueError) as e:
            await prompt_msg.edit(f"❌ **Login Failed:**\n`{e}`\n\nPlease try `/fblogin` again.")
        finally:
            if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_yt_client_id":
        state_data["login_data"]["client_id"] = msg.text.strip()
        state_data["action"] = "waiting_for_yt_client_secret"
        await prompt_msg.edit(to_bold_sans("🔑 Please Enter Your YouTube OAuth Client Secret."))

    elif action == "waiting_for_yt_client_secret":
        state_data["login_data"]["client_secret"] = msg.text.strip()
        client_id = state_data["login_data"]["client_id"]
        client_secret = state_data["login_data"]["client_secret"]
        
        try:
            client_config = { "web": {
                    "client_id": client_id, "client_secret": client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            }}
            
            flow = Flow.from_client_config(
                client_config,
                scopes=['https://www.googleapis.com/auth/youtube.upload', 'https://www.googleapis.com/auth/youtube'],
                redirect_uri=REDIRECT_URI
            )
            auth_url, state = flow.authorization_url(access_type='offline', prompt='consent')
            oauth_flows[state] = flow
            state_data["oauth_state"] = state
            state_data["action"] = "waiting_for_yt_auth_code"

            markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Google OAuth Link", url=auth_url)]])
            
            await prompt_msg.edit(
                "✅ " + to_bold_sans("Credentials accepted.") + "\n\n"
                "⬇️ " + to_bold_sans("Click the link, allow access, and copy the code/URL.") + "\n\n"
                "Copy the **full URL** from the page you are redirected to and send it back to me.\n\n"
                "**Note:** If you see 'Access blocked' or '401 invalid_client', ensure your app is **Published** in Google Cloud and you are a **Test User**.",
                reply_markup=markup
            )
        except Exception as e:
            await prompt_msg.edit(f"❌ " + to_bold_sans(f"Failed to process credentials. Error: {e}"))
            if user_id in user_states: del user_states[user_id]
            
    elif action == "waiting_for_yt_auth_code":
        auth_code_or_url = msg.text.strip()
        await prompt_msg.edit("🔐 " + to_bold_sans("Exchanging code for tokens..."))
        
        state = state_data.get("oauth_state")
        if not state or state not in oauth_flows:
            return await prompt_msg.edit("❌ " + to_bold_sans("Invalid/expired session. Please try /ytlogin again."))

        flow = oauth_flows[state]
        try:
            if "localhost" in auth_code_or_url or "code=" in auth_code_or_url:
                await asyncio.to_thread(flow.fetch_token, authorization_response=auth_code_or_url)
            else:
                await asyncio.to_thread(flow.fetch_token, code=auth_code_or_url)
            
            credentials = flow.credentials
            
            youtube = build('youtube', 'v3', credentials=credentials)
            channels_response = await asyncio.to_thread(youtube.channels().list(part='snippet,contentDetails', mine=True).execute)
            
            if not channels_response.get('items'):
                return await prompt_msg.edit("❌ " + to_bold_sans("No YouTube Channel Found For This Account."))
            
            channel = channels_response['items'][0]
            channel_id = channel['id']
            channel_name = channel['snippet']['title']
            
            session_data = {
                'id': channel_id, 'name': channel_name,
                'credentials_json': credentials.to_json(),
                'client_id': flow.client_config.get('client_id')
            }
            await save_platform_session(user_id, "youtube", session_data)
            
            user_settings = await get_user_settings(user_id)
            user_settings["active_youtube_id"] = channel_id
            await save_user_settings(user_id, user_settings)
            
            await prompt_msg.delete()
            if state_data.get("secret_messages"):
                await app.delete_messages(user_id, state_data["secret_messages"])

            await msg.reply(
                f"✅ {to_bold_sans('Successfully logged in!')}\n\n"
                f"**Channel Name:** `{channel_name}`\n"
                f"**Channel ID:** `{channel_id}`"
            )
            
            await send_log_to_channel(app, LOG_CHANNEL, f"📝 YT Login: User `{user_id}`, Channel: `{channel_name}`")
        except Exception as e:
            await prompt_msg.edit(
                f"❌ {to_bold_sans('Login Failed.')}\n"
                f"Error: `{e}`\n\n"
                "Please check your credentials and ensure your Google Cloud project is configured correctly."
            )
            logger.error(f"YouTube token exchange failed for {user_id}: {e}")
        finally:
            if state in oauth_flows: del oauth_flows[state]
            if user_id in user_states: del user_states[user_id]

    elif action.startswith("waiting_for_caption_"):
        platform = action.split("_")[-1]
        settings = await get_user_settings(user_id)
        settings[f"caption_{platform}"] = msg.text
        await save_user_settings(user_id, settings)
        await msg.reply("✅ " + to_bold_sans(f"Default Caption For {platform.capitalize()} Has Been Set."))
        if user_id in user_states: del user_states[user_id]

    elif action.startswith("waiting_for_description_"):
        platform = action.split("_")[-1]
        settings = await get_user_settings(user_id)
        settings[f"description_{platform}"] = msg.text
        await save_user_settings(user_id, settings)
        await msg.reply("✅ " + to_bold_sans(f"Default Description For {platform.capitalize()} Has Been Set."))
        if user_id in user_states: del user_states[user_id]

    elif action.startswith("waiting_for_title_"):
        platform = action.split("_")[-1]
        settings = await get_user_settings(user_id)
        settings[f"title_{platform}"] = msg.text
        await save_user_settings(user_id, settings)
        await msg.reply("✅ " + to_bold_sans(f"Default Title For {platform.capitalize()} Has Been Set."))
        if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_tags_youtube":
        settings = await get_user_settings(user_id)
        settings["tags_youtube"] = msg.text
        await save_user_settings(user_id, settings)
        await msg.reply("✅ " + to_bold_sans("Default Tags For YouTube Have Been Set."))
        if user_id in user_states: del user_states[user_id]

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
            dt_naive = datetime.strptime(msg.text.strip(), "%Y-%m-%d %H:%M")
            schedule_time_utc = dt_naive.replace(tzinfo=timezone.utc)
            
            if schedule_time_utc <= datetime.now(timezone.utc):
                await safe_threaded_reply(
                    state_data["file_info"]["original_media_msg"],
                    "❌ " + to_bold_sans("Scheduled time must be in the future."),
                    status_message=state_data.get("status_msg")
                )
                return
            
            state_data['file_info']['schedule_time'] = schedule_time_utc
            await process_upload_step(msg)
        except ValueError:
            await safe_threaded_reply(
                state_data["file_info"]["original_media_msg"],
                "❌ " + to_bold_sans("Invalid format. Please use `YYYY-MM-DD HH:MM` in UTC."),
                status_message=state_data.get("status_msg")
            )

    elif action in ["waiting_for_thumbnail_choice", "waiting_for_thumbnail"]:
        original_media_msg = state_data.get("file_info", {}).get("original_media_msg")
        correction_text = "❌ " + to_bold_sans("Invalid input. Please either click a button below or send a PHOTO for the thumbnail.")
        if original_media_msg:
            await original_media_msg.reply(correction_text)
        else:
            await msg.reply(correction_text)
        return
            
    elif action == "waiting_for_broadcast_message":
        if not is_admin(user_id): return
        
        if msg.text:
            await broadcast_message(msg, text=msg.text, reply_markup=msg.reply_markup)
        elif msg.photo:
            await broadcast_message(msg, photo=msg.photo.file_id, caption=msg.caption, reply_markup=msg.reply_markup)
        elif msg.video:
            await broadcast_message(msg, video=msg.video.file_id, caption=msg.caption, reply_markup=msg.reply_markup)
        else:
            await msg.reply("Unsupported broadcast format.")
            
        if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_target_user_id_premium_management":
        if not is_admin(user_id): return
        try:
            target_user_id = int(msg.text)
            user_states[user_id] = {"action": "select_platforms_for_premium", "target_user_id": target_user_id, "selected_platforms": {}}
            await msg.reply(
                f"✅ " + to_bold_sans(f"User `{target_user_id}`. Select Platforms:"),
                reply_markup=get_platform_selection_markup(user_id, {})
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
            if new_limit <= 0: return await msg.reply("❌ " + to_bold_sans("Must Be A Positive Integer."))
            await _update_global_setting("max_concurrent_uploads", new_limit)
            global upload_semaphore
            upload_semaphore = asyncio.Semaphore(new_limit)
            await msg.reply(f"✅ " + to_bold_sans(f"Max Concurrent Uploads Set To `{new_limit}`."))
            if user_id in user_states: del user_states[user_id]
            await show_global_settings_panel(msg)
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid Input."))

    elif action == "waiting_for_max_file_size":
        if not is_admin(user_id): return
        try:
            new_limit = int(msg.text)
            if new_limit <= 0: return await msg.reply("❌ " + to_bold_sans("Must Be A Positive Integer."))
            await _update_global_setting("max_file_size_mb", new_limit)
            global MAX_FILE_SIZE_BYTES
            MAX_FILE_SIZE_BYTES = new_limit * 1024 * 1024
            await msg.reply(f"✅ " + to_bold_sans(f"Max File Size Set To `{new_limit}` MB."))
            if user_id in user_states: del user_states[user_id]
            await show_global_settings_panel(msg)
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid Input."))

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
        await msg.reply(f"✅ " + to_bold_sans(f"Payment Details For **{payment_method.upper()}** Updated."), reply_markup=payment_settings_markup)
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
        await msg.reply("✍️ " + to_bold_sans("Enter Payment Details (text/Number/Address/Link):"))

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
            await msg.forward(ADMIN_ID)
            await app.send_message(
                ADMIN_ID, 
                f"👆 Payment proof from user: `{user_id}` (@{msg.from_user.username or 'N/A'})"
            )
            await msg.reply("✅ Your proof has been sent to the admin for verification.")
            if user_id in user_states: del user_states[user_id]


# ===================================================================
# =================== CALLBACK QUERY HANDLERS =======================
# ===================================================================

@app.on_callback_query(filters.regex("^select_platform_"))
@rate_limit_callbacks
async def select_platform_for_premium_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id):
        return await query.answer("Admin only.", show_alert=True)

    state_data = user_states.get(user_id)
    if not state_data or state_data.get("action") != "select_platforms_for_premium":
        return await query.answer("Error: Invalid state.", show_alert=True)
        
    platform = query.data.split("select_platform_")[1]
    
    selected_platforms = state_data.get("selected_platforms", {})
    selected_platforms[platform] = not selected_platforms.get(platform, False)
    state_data["selected_platforms"] = selected_platforms
    
    await safe_edit_message(
        query.message,
        text=f"✅ User `{state_data.get('target_user_id')}`. Select Platforms:",
        reply_markup=get_platform_selection_markup(user_id, selected_platforms)
    )

@app.on_callback_query(filters.regex("^confirm_platform_selection$"))
@rate_limit_callbacks
async def confirm_platform_selection_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id): return await query.answer("❌ Admin access required", show_alert=True)
    
    state_data = user_states.get(user_id)
    if not isinstance(state_data, dict) or state_data.get("action") != "select_platforms_for_premium":
        return await query.answer("Error: State lost.", show_alert=True)
        
    selected_platforms = [p for p, selected in state_data.get("selected_platforms", {}).items() if selected]
    if not selected_platforms:
        return await query.answer("Please select at least one platform!", show_alert=True)
        
    state_data["action"] = "select_premium_plan_for_platforms"
    state_data["final_selected_platforms"] = selected_platforms
    user_states[user_id] = state_data
    
    await safe_edit_message(
        query.message,
        f"✅ Platforms: `{', '.join(p.capitalize() for p in selected_platforms)}`.\n\nSelect a plan for user `{state_data['target_user_id']}`:",
        reply_markup=get_premium_plan_markup(user_id)
    )

@app.on_callback_query(filters.regex("^grant_plan_"))
@rate_limit_callbacks
async def grant_plan_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id): return await query.answer("❌ Admin access required", show_alert=True)
    if db is None: return await query.answer("⚠️ Database unavailable.", show_alert=True)
    
    state_data = user_states.get(user_id)
    if not isinstance(state_data, dict) or state_data.get("action") != "select_premium_plan_for_platforms":
        return await query.answer("❌ Error: State lost.", show_alert=True)
        
    target_user_id = state_data["target_user_id"]
    selected_platforms = state_data["final_selected_platforms"]
    premium_plan_key = query.data.split("grant_plan_")[1]
    
    plan_details = PREMIUM_PLANS.get(premium_plan_key)
    if not plan_details:
        return await query.answer("Invalid premium plan.", show_alert=True)
    
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
    
    await safe_edit_message(query.message, admin_confirm_text, reply_markup=admin_markup)
    await query.answer("Premium granted!", show_alert=False)
    if user_id in user_states: del user_states[user_id]
        
    try:
        await app.send_message(target_user_id, user_msg_text, parse_mode=enums.ParseMode.MARKDOWN)
        await send_log_to_channel(app, LOG_CHANNEL,
            f"💰 Premium granted to `{target_user_id}` by admin `{user_id}`.\nPlatforms: `{', '.join(selected_platforms)}`, Plan: `{premium_plan_key}`"
        )
    except Exception as e:
        logger.error(f"Failed to notify user {target_user_id} about premium: {e}")
    
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
    
    await query.answer(f"✅ Active account for {platform.capitalize()} updated.", show_alert=True)
    class MockQuery:
        def __init__(self, user, message, data):
            self.from_user = user
            self.message = message
            self.data = data
        async def answer(self, *args, **kwargs):
            pass
    await manage_accounts_cb(app, MockQuery(query.from_user, query.message, f'manage_{"fb" if platform == "facebook" else "yt"}_accounts'))

@app.on_callback_query(filters.regex("^manage_logout_"))
@rate_limit_callbacks
async def manage_logout_cb(_, query):
    _, _, platform, acc_id = query.data.split("_")
    sessions = await load_platform_sessions(query.from_user.id, platform)
    acc_name = next((s['session_data']['name'] for s in sessions if s['account_id'] == acc_id), "Account")
    
    await safe_edit_message(
        query.message,
        to_bold_sans(f"Logout Options for {acc_name}"),
        reply_markup=get_logout_options_markup(platform, acc_id, acc_name)
    )

@app.on_callback_query(filters.regex("^change_page_token_"))
@rate_limit_callbacks
async def change_page_token_cb(_, query):
    user_id = query.from_user.id
    _, _, platform, acc_id = query.data.split("_")
    
    session_doc = await asyncio.to_thread(db.sessions.find_one, {"user_id": user_id, "platform": platform, "account_id": acc_id})
    if not session_doc:
        return await query.answer("Account session not found.", show_alert=True)
        
    session_data = session_doc.get("session_data", {})
    
    prompt_msg = await query.message.edit(to_bold_sans("🔑 Please Enter Your New Facebook Page API Token."))
    user_states[user_id] = {
        "action": "waiting_for_fb_page_token", "platform": "facebook",
        "login_data": {
            "app_id": session_data.get('app_id'),
            "app_secret": session_data.get('app_secret')
        },
        "prompt_msg": prompt_msg, "secret_messages": []
    }
    await delete_platform_session(user_id, platform, acc_id)
    
    user_settings = await get_user_settings(user_id)
    if user_settings.get(f"active_{platform}_id") == acc_id:
        user_settings[f"active_{platform}_id"] = None
        await save_user_settings(user_id, user_settings)


@app.on_callback_query(filters.regex("^confirm_logout_"))
@rate_limit_callbacks
async def confirm_logout_cb(_, query):
    _, _, platform, acc_id = query.data.split("_")
    sessions = await load_platform_sessions(query.from_user.id, platform)
    acc_name = next((s['session_data']['name'] for s in sessions if s['account_id'] == acc_id), "Account")
    
    await safe_edit_message(
        query.message,
        to_bold_sans(f"Are you sure you want to completely logout from {acc_name}?"),
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
        user_settings[f"active_{platform}_id"] = sessions[0]['session_data']['id'] if sessions else None
        await save_user_settings(user_id, user_settings)
    
    await query.answer(f"✅ Logged out successfully.", show_alert=True)
    class MockQuery:
        def __init__(self, user, message, data):
            self.from_user = user
            self.message = message
            self.data = data
        async def answer(self, *args, **kwargs):
            pass
    await manage_accounts_cb(app, MockQuery(query.from_user, query.message, f'manage_{"fb" if platform == "facebook" else "yt"}_accounts'))

@app.on_callback_query(filters.regex("^add_account_"))
@rate_limit_callbacks
async def add_account_cb(_, query):
    user_id = query.from_user.id
    platform = query.data.split("add_account_")[-1]
    
    if not await is_premium_for_platform(user_id, platform) and not is_admin(user_id):
        return await query.answer("❌ This is a premium feature.", show_alert=True)
    
    await query.message.delete()

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


# --- General Callbacks ---
@app.on_callback_query(filters.regex("^cancel_upload$"))
@rate_limit_callbacks
async def cancel_upload_cb(_, query):
    user_id = query.from_user.id
    await query.answer("Upload cancelled.", show_alert=True)
    
    state_data = user_states.get(user_id, {})
    status_msg = state_data.get('status_msg')
    original_media_msg = state_data.get("file_info", {}).get("original_media_msg")

    await safe_threaded_reply(
        original_media_msg,
        "❌ **" + to_bold_sans("Upload Cancelled") + "**\n\n" + to_bold_sans("Your Operation Has Been Cancelled."),
        status_message=status_msg
    )

    file_info = state_data.get("file_info", {})
    files_to_clean = [file_info.get("downloaded_path"), file_info.get("processed_path"), file_info.get("thumbnail_path")]
    
    cleanup_temp_files(files_to_clean)
    if user_id in user_states: del user_states[user_id]
    await task_tracker.cancel_all_user_tasks(user_id)
    logger.info(f"User {user_id} cancelled their upload.")

@app.on_callback_query(filters.regex("^upload_flow_"))
@rate_limit_callbacks
async def upload_flow_cb(_, query):
    user_id = query.from_user.id
    data = query.data.replace("upload_flow_", "")
    
    state_data = user_states.get(user_id)
    if not state_data or "file_info" not in state_data:
        return await query.answer("❌ Error: State lost, please start over.", show_alert=True)
    
    state_data["status_msg"] = query.message

    parts = data.split("_")
    step = parts[0]
    choice = parts[1] if len(parts) > 1 else ""

    if step == "thumbnail":
        if choice == "custom":
            state_data['action'] = 'waiting_for_thumbnail'
            await safe_edit_message(query.message, "🖼️ " + to_bold_sans("Please send the thumbnail image now."))
        elif choice == "auto":
            state_data['file_info']['thumbnail_path'] = "auto"
            await process_upload_step(query)
            
    elif step == "visibility":
        state_data['file_info']['visibility'] = choice
        await process_upload_step(query)
        
    elif step == "publish":
        if choice == "now":
            # If "Publish Now" is clicked, set time to None and proceed
            state_data['file_info']['schedule_time'] = None
            await process_upload_step(query)
        elif choice == "schedule":
            # If "Schedule Later" is clicked, set the action...
            state_data['action'] = 'waiting_for_schedule_time'
            # ...and then ASK the user for the time directly.
            schedule_prompt = (
                "⏰ " + to_bold_sans("Please send the schedule time.") + "\n\n"
                "Use the format: `YYYY-MM-DD HH:MM`\n"
                "*(Time should be in UTC)*"
            )
            await safe_edit_message(query.message, schedule_prompt, reply_markup=get_progress_markup())
            
    elif step == "input" and choice == "skip":
        action = state_data.get('action')
        if action == "waiting_for_title":
            state_data["file_info"]["title"] = None
        elif action == "waiting_for_description":
            state_data["file_info"]["description"] = ""
        elif action == "waiting_for_tags":
            state_data["file_info"]["tags"] = ""
        await process_upload_step(query)


# --- Premium & Payment Callbacks ---
@app.on_callback_query(filters.regex("^buypypremium$"))
@rate_limit_callbacks
async def buypypremium_cb(_, query):
    user_id = query.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    
    premium_plans_text = (
        "⭐ " + to_bold_sans("Upgrade To Premium!") + " ⭐\n\n"
        + to_bold_sans("Unlock Full Features And Upload Unlimited Content.") + "\n\n"
        "**Available Plans:**"
    )
    await safe_edit_message(query.message, premium_plans_text, reply_markup=get_premium_plan_markup(user_id))

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
        plan_text += to_bold_sans("To Purchase, Click 'Buy Now' Or Check Payment Methods.")
        
    await safe_edit_message(
        query.message, plan_text,
        reply_markup=get_premium_details_markup(plan_key, is_admin_flow=is_admin_adding_premium)
    )

@app.on_callback_query(filters.regex("^show_payment_methods$"))
@rate_limit_callbacks
async def show_payment_methods_cb(_, query):
    payment_methods_text = "**" + to_bold_sans("Available Payment Methods") + "**\n\n"
    payment_methods_text += to_bold_sans("Choose Your Preferred Method To Proceed.")
    await safe_edit_message(query.message, payment_methods_text, reply_markup=get_payment_methods_markup())

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
        return await query.answer("QR code is not set by the admin.", show_alert=True)
    
    caption_text = "**" + to_bold_sans("Scan & Pay") + "**\n\n" + \
                    "Send a screenshot to the Admin for activation."
    
    await query.message.reply_photo(photo=qr_file_id, caption=caption_text)
    await query.answer()

@app.on_callback_query(filters.regex("^show_payment_details_"))
@rate_limit_callbacks
async def show_payment_details_cb(_, query):
    method = query.data.split("show_payment_details_")[1]
    payment_details = global_settings.get("payment_settings", {}).get(method, "No details available.")
    text = (
        f"**{to_bold_sans(f'{method.upper()} Payment Details')}**\n\n"
        f"`{payment_details}`\n\n"
        f"Contact Admin with a screenshot for activation."
    )
    await safe_edit_message(query.message, text, reply_markup=get_payment_methods_markup())

@app.on_callback_query(filters.regex("^show_custom_payment_"))
@rate_limit_callbacks
async def show_custom_payment_cb(_, query):
    button_name = query.data.split("show_custom_payment_")[1]
    payment_details = global_settings.get("payment_settings", {}).get("custom_buttons", {}).get(button_name, "No details available.")
    text = (
        f"**{to_bold_sans(f'{button_name.upper()} Payment Details')}**\n\n"
        f"`{payment_details}`\n\n"
        f"Contact Admin with a screenshot for activation."
    )
    await safe_edit_message(query.message, text, reply_markup=get_payment_methods_markup())

@app.on_callback_query(filters.regex("^buy_now$"))
@rate_limit_callbacks
async def buy_now_cb(_, query):
    text = (
        f"**{to_bold_sans('Purchase Confirmation')}**\n\n"
        f"Please contact the Admin to complete the payment."
    )
    await safe_edit_message(query.message, text)

# --- Admin Panel Callbacks ---
@app.on_callback_query(filters.regex("^(admin_panel|users_list|admin_user_details|manage_premium|broadcast_message|admin_stats_panel)$"))
@rate_limit_callbacks
async def admin_panel_actions_cb(_, query):
    user_id = query.from_user.id
    if not is_admin(user_id):
        return await query.answer("❌ Admin access required", show_alert=True)
    
    action = query.data

    if action == "admin_panel":
        await safe_edit_message(query.message, "🛠 " + to_bold_sans("Welcome To The Admin Panel!"), reply_markup=admin_markup)
    elif action == "users_list":
        if db is None: return await query.answer("DB connection failed.", show_alert=True)
        await query.answer("Fetching users...")
        users = await asyncio.to_thread(list, db.users.find({}))
        user_list_text = f"👥 **Total Users: {len(users)}**\n\n"
        for i, user in enumerate(users[:50]):
            user_list_text += f"`{user['_id']}` - @{user.get('username', 'N/A')}\n"
        if len(users) > 50:
            user_list_text += "\n...and more."
        await safe_edit_message(query.message, user_list_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]]))

    elif action == "admin_user_details":
        user_states[user_id] = {"action": "waiting_for_user_id_for_details"}
        await safe_edit_message(query.message, "👤 " + to_bold_sans("Please send the User ID to get their details."))
    
    elif action == "manage_premium":
        user_states[user_id] = {"action": "waiting_for_target_user_id_premium_management"}
        await safe_edit_message(query.message, "➕ " + to_bold_sans("Please send the User ID to manage their premium."))

    elif action == "broadcast_message":
        user_states[user_id] = {"action": "waiting_for_broadcast_message"}
        await safe_edit_message(query.message, "📢 " + to_bold_sans("Please send the message to broadcast."))
    
    elif action == "admin_stats_panel":
        await show_stats(app, query)

async def show_user_details(message, target_user_id):
    """Helper to fetch and display user details for the admin."""
    user_data = await _get_user_data(target_user_id)
    if not user_data:
        return await message.reply("❌ User not found in the database.")

    details_text = f"👤 **Details for User ID:** `{target_user_id}`\n"
    details_text += f"**Username:** @{user_data.get('username', 'N/A')}\n"
    
    joined_at = user_data.get('added_at')
    if isinstance(joined_at, datetime):
        details_text += f"**Joined:** {joined_at.strftime('%Y-%m-%d')}\n"
        
    last_active = user_data.get('last_active')
    if isinstance(last_active, datetime):
        details_text += f"**Last Active:** {last_active.strftime('%Y-%m-%d %H:%M')}\n\n"

    total_uploads = await asyncio.to_thread(db.uploads.count_documents, {'user_id': target_user_id})
    details_text += f"**Uploads:**\n- Total: `{total_uploads}`\n"

    details_text += "\n**Login & Token Status:**\n"
    yt_sessions = await load_platform_sessions(target_user_id, 'youtube')
    if yt_sessions:
        for session in yt_sessions:
            try:
                creds = Credentials.from_authorized_user_info(json.loads(session['session_data']['credentials_json']))
                expiry = creds.expiry.strftime('%Y-%m-%d %H:%M UTC')
                details_text += f"- **YouTube Channel:** {session['session_data'].get('name', 'N/A')}\n  - Token Expires: `{expiry}`\n"
            except:
                details_text += f"- **YouTube Channel:** {session['session_data'].get('name', 'N/A')} (Token info error)\n"
    else:
        details_text += "- No YouTube account logged in.\n"
    
    await message.reply(details_text, parse_mode=enums.ParseMode.MARKDOWN)

async def show_global_settings_panel(message_or_query):
    """Helper function to display the global settings panel."""
    message = message_or_query.message if hasattr(message_or_query, 'message') else message_or_query

    settings_text = (
        "⚙️ **" + to_bold_sans("Global Bot Settings") + "**\n\n"
        f"**📢 Special Event:** `{global_settings.get('special_event_toggle', False)}`\n"
        f"**⏫ Max Concurrent Uploads:** `{global_settings.get('max_concurrent_uploads')}`\n"
        f"**🗂️ Max File Size:** `{global_settings.get('max_file_size_mb')}` MB\n"
        f"**👥 Multiple Logins:** `{'Allowed' if global_settings.get('allow_multiple_logins') else 'Blocked'}`"
    )
    await safe_edit_message(message, settings_text, reply_markup=get_admin_global_settings_markup())

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
        await safe_edit_message(query.message, "✏️ " + to_bold_sans("Please send the new event title."))
        
    elif action == "set_event_message":
        user_states[user_id] = {"action": "waiting_for_event_message"}
        await safe_edit_message(query.message, "💬 " + to_bold_sans("Please send the new event message."))

    elif action == "set_max_uploads":
        user_states[user_id] = {"action": "waiting_for_max_uploads"}
        await safe_edit_message(query.message, "⏫ " + to_bold_sans(f"Current limit: {MAX_CONCURRENT_UPLOADS}. Send new number."))

    elif action == "set_max_file_size":
        user_states[user_id] = {"action": "waiting_for_max_file_size"}
        await safe_edit_message(query.message, "🗂️ " + to_bold_sans(f"Current limit: {global_settings.get('max_file_size_mb')} MB. Send new number in MB."))

    elif action == "set_payment_instructions":
        user_states[user_id] = {"action": "waiting_for_payment_instructions"}
        await safe_edit_message(query.message, "✍️ " + to_bold_sans("Please send the new payment instructions."))

    elif action == "reset_stats":
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes, Reset All Stats", callback_data="confirm_reset_stats")],
            [InlineKeyboardButton("❌ No, Cancel", callback_data="global_settings_panel")]
        ])
        await safe_edit_message(query.message, "⚠️ " + to_bold_sans("Are you sure? This will delete all upload records permanently."), reply_markup=markup)

    elif action == "confirm_reset_stats":
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
        await query.message.edit(f"✍️ " + to_bold_sans(f"Please send payment details for {method.upper()}."))


# --- Back Callbacks ---
@app.on_callback_query(filters.regex("^back_to_"))
@rate_limit_callbacks
async def back_to_cb(_, query):
    data = query.data
    user_id = query.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    
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
        await safe_edit_message(query.message, "🛠 " + to_bold_sans("Admin Panel"), reply_markup=admin_markup)
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
    await query.message.reply(welcome_msg, reply_markup=get_main_keyboard(user_id, premium_platforms))
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

# ===================================================================
# ======================== MEDIA HANDLERS ===========================
# ===================================================================
@app.on_message(filters.document)
async def handle_yt_json(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id, {})
    
    if state_data.get("action") == "waiting_for_yt_client_id":
        await msg.reply(
            "Login method has changed! Please enter your Client ID as text, not a file."
        )


async def process_upload_step(msg_or_query):
    """Central function to handle the step-by-step upload process."""
    if isinstance(msg_or_query, types.CallbackQuery):
        user_id = msg_or_query.from_user.id
    else:
        user_id = msg_or_query.from_user.id
    
    state_data = user_states.get(user_id)
    if not state_data: return

    file_info = state_data["file_info"]
    platform = state_data["platform"]
    upload_type = state_data["upload_type"]
    
    original_media_msg = file_info.get("original_media_msg")
    status_msg = state_data.get("status_msg")
    
    next_prompt_text, next_markup = None, None
    
    if "title" not in file_info:
        state_data["action"] = "waiting_for_title"
        next_prompt_text = to_bold_sans("Please send a Title for your post.")
        next_markup = get_upload_flow_markup(platform, 'input')

    elif "description" not in file_info:
        state_data["action"] = "waiting_for_description"
        next_prompt_text = to_bold_sans("Next, send a Description.")
        next_markup = get_upload_flow_markup(platform, 'input')

    elif platform == 'youtube' and "tags" not in file_info:
        state_data["action"] = "waiting_for_tags"
        next_prompt_text = to_bold_sans("Now, send comma-separated Tags.")
        next_markup = get_upload_flow_markup(platform, 'input')

    elif platform == 'youtube' and upload_type == 'video' and "thumbnail_path" not in file_info:
        state_data["action"] = "waiting_for_thumbnail_choice"
        next_prompt_text = to_bold_sans("A thumbnail is required for YouTube Videos. Please upload one or let the bot generate one.")
        next_markup = get_upload_flow_markup(platform, 'thumbnail')
    
    elif platform == 'youtube' and "visibility" not in file_info:
        state_data["action"] = "waiting_for_visibility_choice"
        next_prompt_text = to_bold_sans("Set Video Visibility:")
        next_markup = get_upload_flow_markup(platform, 'visibility')

    elif "schedule_time" not in file_info:
        if platform == 'facebook':
            file_info['visibility'] = 'public'
        state_data["action"] = "waiting_for_publish_choice"
        next_prompt_text = to_bold_sans("When To Publish?")
        next_markup = get_upload_flow_markup(platform, 'publish')

    else:
        # Final step: start upload/schedule
        schedule_time = file_info.get("schedule_time")
        if schedule_time:
            new_status_msg = await safe_threaded_reply(original_media_msg, "⏳ " + to_bold_sans("Scheduling your post..."), status_message=status_msg)
            state_data['status_msg'] = new_status_msg
            try:
                job_details = {
                    "user_id": user_id, "platform": platform, "upload_type": upload_type,
                    "original_chat_id": original_media_msg.chat.id,
                    "original_message_id": original_media_msg.id,
                    "schedule_time": schedule_time,
                    "status": "pending", "created_at": datetime.now(timezone.utc),
                    "metadata": {k: file_info.get(k) for k in ["title", "description", "tags", "visibility", "thumbnail_path"]}
                }
                if db is not None:
                    await asyncio.to_thread(db.scheduled_jobs.insert_one, job_details)
                    schedule_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🗓️ Manage Schedules", callback_data=f"manage_schedules_{platform}")]])
                    await safe_threaded_reply(original_media_msg, f"✅ **Scheduled!**\n\nYour post will be uploaded on `{schedule_time.strftime('%Y-%m-%d %H:%M')} UTC`.", schedule_markup, new_status_msg)
                else:
                    await safe_threaded_reply(original_media_msg, "❌ **Scheduling Failed:** Database is offline.", status_message=new_status_msg)
            except Exception as e:
                logger.error(f"Failed to schedule job: {e}", exc_info=True)
                await safe_threaded_reply(original_media_msg, f"❌ **Scheduling Failed:** Could not save the job. Error: {e}", status_message=new_status_msg)
            finally:
                if user_id in user_states: del user_states[user_id]
        else:
            state_data["action"] = "finalizing"
            await start_upload_task(status_msg, file_info, user_id)
            
    if next_prompt_text:
        new_status_msg = await safe_threaded_reply(original_media_msg, next_prompt_text, next_markup, status_msg)
        state_data["status_msg"] = new_status_msg


@app.on_message(filters.media & ~filters.document & filters.private)
async def handle_media_upload(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.now(timezone.utc)})
    state_data = user_states.get(user_id, {})

    if state_data and state_data.get("action") == "waiting_for_payment_proof":
        await msg.forward(ADMIN_ID)
        await app.send_message(
            ADMIN_ID, 
            f"👆 Payment proof from user: `{user_id}` (@{msg.from_user.username or 'N/A'})"
        )
        await msg.reply("✅ Your proof has been sent to the admin for verification.")
        if user_id in user_states: del user_states[user_id]
        return

    if is_admin(user_id) and state_data and state_data.get("action") == "waiting_for_google_play_qr" and msg.photo:
        payment_settings = global_settings.get("payment_settings", {})
        payment_settings["google_play_qr_file_id"] = msg.photo.file_id
        await _update_global_setting("payment_settings", payment_settings)
        if user_id in user_states: del user_states[user_id]
        return await msg.reply("✅ " + to_bold_sans("Google Pay QR Code Image Saved!"), reply_markup=payment_settings_markup)

    if state_data and state_data.get("action") == 'waiting_for_thumbnail':
        if not msg.photo:
            await safe_threaded_reply(
                state_data["file_info"]["original_media_msg"],
                "❌ " + to_bold_sans("That's not an image. Please send an image for the thumbnail."),
                status_message=state_data.get("status_msg")
            )
            return
        
        status_msg = state_data.get("status_msg")
        new_status_msg = await safe_threaded_reply(state_data["file_info"]["original_media_msg"], "🖼️ " + to_bold_sans("Downloading thumbnail..."), status_message=status_msg)
        state_data['status_msg'] = new_status_msg
        
        thumb_path = await app.download_media(msg.photo)
        state_data['file_info']['thumbnail_path'] = thumb_path
        await process_upload_step(msg)
        return

    action = state_data.get("action")
    if not action or action != "waiting_for_media":
        return

    media = msg.video or msg.photo or msg.document
    if not media: return await msg.reply("❌ " + to_bold_sans("Unsupported Media Type."))

    if media.file_size > MAX_FILE_SIZE_BYTES:
        if user_id in user_states: del user_states[user_id]
        return await msg.reply(f"❌ " + to_bold_sans(f"File Size Exceeds The Limit Of `{MAX_FILE_SIZE_BYTES / (1024 * 1024):.0f}` MB."))

    status_msg = await safe_threaded_reply(msg, "⏳ " + to_bold_sans("Starting Download..."))
    state_data['status_msg'] = status_msg
    state_data["file_info"] = { "original_media_msg": msg }

    try:
        start_time = time.time()
        last_update_time = [0]
        task_tracker.create_task(monitor_progress_task(msg, status_msg, action_text="Downloading"), user_id=user_id, task_name="progress_monitor")
        
        downloaded_path = await app.download_media(
            msg,
            progress=download_progress_callback,
            progress_args=("Download", status_msg.id, msg.chat.id, start_time, last_update_time)
        )
        
        task_tracker.cancel_user_task(user_id, "progress_monitor")
        
        state_data["file_info"]["downloaded_path"] = downloaded_path
        state_data["file_info"]["original_caption"] = msg.caption
        
        await process_upload_step(msg)

    except Exception as e:
        logger.error(f"Error during file download for user {user_id}: {e}", exc_info=True)
        await safe_threaded_reply(msg, f"❌ " + to_bold_sans(f"Download Failed: {e}"), status_message=status_msg)
        if user_id in user_states: del user_states[user_id]


# ===================================================================
# ==================== UPLOAD PROCESSING ==========================
# ===================================================================

async def start_upload_task(status_msg, file_info, user_id):
    task_tracker.create_task(
        safe_task_wrapper(process_and_upload(status_msg, file_info, user_id)),
        user_id=user_id,
        task_name="upload"
    )

async def process_and_upload(status_msg, file_info, user_id, from_schedule=False, job_id=None):
    platform, upload_type, final_title = None, None, "Untitled"
    original_media_msg = file_info.get('original_media_msg')

    if not from_schedule:
        state_data = user_states.get(user_id)
        if not state_data:
            logger.error(f"State not found for user {user_id} during direct upload.")
            if original_media_msg:
                await safe_threaded_reply(original_media_msg, "❌ " + to_bold_sans("An error occurred. Please start again."), status_message=status_msg)
            return
        platform = state_data["platform"]
        upload_type = state_data["upload_type"]
    else: # Scheduled job
        if db is None:
            logger.error("Cannot process scheduled job: DB is not connected.")
            return
        job = await asyncio.to_thread(db.scheduled_jobs.find_one, {"_id": ObjectId(job_id)})
        if not job:
            logger.error(f"Scheduled job with ID {job_id} not found.")
            return
        platform = job['platform']
        upload_type = job.get('upload_type', 'video')
        final_title = job.get('metadata', {}).get('title', 'Scheduled Upload')
        status_msg = await app.send_message(user_id, "⏳ " + to_bold_sans(f"Starting your scheduled {upload_type}..."))

    async with upload_semaphore:
        logger.info(f"Semaphore acquired for user {user_id}. Starting upload to {platform}.")
        files_to_clean = [file_info.get("downloaded_path"), file_info.get("processed_path"), file_info.get("thumbnail_path")]
        try:
            user_settings = await get_user_settings(user_id)
            
            path = file_info.get("downloaded_path")
            if not path or not os.path.exists(path):
                raise FileNotFoundError("Downloaded file path is missing or invalid.")

            is_video = upload_type in ['video', 'short', 'reels']
            upload_path = path

            if is_video:
                status_msg = await safe_threaded_reply(original_media_msg, "⚙️ " + to_bold_sans("Checking video format..."), status_message=status_msg)
                if await asyncio.to_thread(needs_conversion, path):
                    status_msg = await safe_threaded_reply(original_media_msg, "⚙️ " + to_bold_sans("Converting video... (This may take a while)"), status_message=status_msg)
                    processed_path = path.rsplit(".", 1)[0] + "_processed.mp4"
                    upload_path = await asyncio.to_thread(process_video_for_upload, path, processed_path)
                    files_to_clean.append(processed_path)
                    await safe_threaded_reply(original_media_msg, "ℹ️ **Conversion Info**: Video was re-encoded for compatibility.")
                else:
                    await safe_threaded_reply(original_media_msg, "✅ No conversion needed.")

            if platform == 'youtube' and upload_type == 'video' and file_info.get("thumbnail_path") == "auto":
                status_msg = await safe_threaded_reply(original_media_msg, "🖼️ " + to_bold_sans("Generating Smart Thumbnail..."), status_message=status_msg)
                thumb_output_path = upload_path + ".jpg"
                generated_thumb = await asyncio.to_thread(generate_thumbnail, upload_path, thumb_output_path)
                file_info["thumbnail_path"] = generated_thumb
                files_to_clean.append(generated_thumb)

            if file_info.get("title") is None:
                final_title = file_info.get("original_caption") or user_settings.get(f"title_{platform}") or user_settings.get(f"caption_{platform}") or "Untitled"
            else:
                final_title = file_info.get("title")
                
            final_description = file_info.get("description") or user_settings.get(f"description_{platform}") or ""
            
            if platform == "facebook":
                status_msg = await safe_threaded_reply(original_media_msg, "⬆️ " + to_bold_sans(f"Uploading {upload_type} to Facebook..."), status_message=status_msg)
                session = await get_active_session(user_id, 'facebook')
                if not session: raise ConnectionError("Facebook session not found. Please /fblogin.")
                
                page_id, token = session['id'], session['access_token']
                fb_caption = f"{final_title}\n\n{final_description}".strip()

                if upload_type == 'post':
                    with open(upload_path, 'rb') as f:
                        response = await asyncio.to_thread(requests.post, f"https://graph.facebook.com/v19.0/{page_id}/photos", data={'access_token': token, 'caption': fb_caption}, files={'source': f}, timeout=600)
                        post_data = check_fb_response(response)
                        post_id = post_data.get('post_id', post_data.get('id', 'N/A'))
                        url = f"https://facebook.com/{post_id}"

                elif upload_type in ['video', 'reels']:
                    with open(upload_path, 'rb') as f:
                        params = {'access_token': token, 'description': fb_caption}
                        r = await asyncio.to_thread(requests.post, f"https://graph-video.facebook.com/{page_id}/videos", data=params, files={'source': f}, timeout=3600)
                        video_data = check_fb_response(r)
                        media_id = video_data['id']
                        url = f"https://facebook.com/video.php?v={media_id}"
                        logger.info(f"Facebook {upload_type} {media_id} published.")
            
            elif platform == "youtube":
                if upload_type == 'short':
                    meta = get_video_metadata(upload_path)
                    v_stream = next((s for s in meta.get('streams', []) if s.get('codec_type') == 'video'), None)
                    duration = float(meta.get('format', {}).get('duration', '999'))
                    if v_stream and (v_stream.get('width', 0) > v_stream.get('height', 1) or duration > 60):
                        await safe_threaded_reply(original_media_msg, "⚠️ **Warning**: Video not vertical or >60s. Uploading as regular video.")
                    if "#shorts" not in final_description.lower() and "#short" not in final_title.lower():
                        final_description += " #shorts"

                status_msg = await safe_threaded_reply(original_media_msg, "⬆️ " + to_bold_sans("Uploading to YouTube..."), status_message=status_msg)
                task_tracker.create_task(monitor_progress_task(original_media_msg, status_msg, action_text="Uploading to YouTube"), user_id, "upload_monitor")
                session = await get_active_session(user_id, 'youtube')
                if not session: raise ConnectionError("YouTube session not found. Please /ytlogin.")
                
                creds = Credentials.from_authorized_user_info(json.loads(session['credentials_json']))
                if creds.expired and creds.refresh_token:
                    try:
                        creds.refresh(Request())
                        session['credentials_json'] = creds.to_json()
                        await save_platform_session(user_id, "youtube", session)
                    except RefreshError as e:
                        raise ConnectionError(f"YouTube token expired/failed to refresh. Please /ytlogin. Error: {e}")

                youtube = build('youtube', 'v3', credentials=creds)
                tags = (file_info.get("tags") or user_settings.get("tags_youtube", "")).split(',')
                visibility = file_info.get("visibility") or user_settings.get("visibility_youtube", "private")
                thumbnail = file_info.get("thumbnail_path")
                schedule_time = file_info.get("schedule_time")

                body = {
                    "snippet": { "title": final_title, "description": final_description, "tags": [tag.strip() for tag in tags if tag.strip()]},
                    "status": {"privacyStatus": "private" if schedule_time else visibility, "selfDeclaredMadeForKids": False }
                }
                if schedule_time:
                    body["status"]["publishAt"] = schedule_time.isoformat().replace("+00:00", "Z")

                media_file = MediaFileUpload(upload_path, chunksize=-1, resumable=True)
                request = youtube.videos().insert(part=",".join(body.keys()), body=body, media_body=media_file)
                
                response = None
                while response is None:
                    status, response = await asyncio.to_thread(request.next_chunk)
                    if status: 
                        _upload_progress['progress'] = int(status.progress() * 100)
                
                media_id = response['id']
                url = f"https://youtu.be/{media_id}"

                if thumbnail and os.path.exists(thumbnail):
                    await asyncio.to_thread(
                        youtube.thumbnails().set(videoId=media_id, media_body=MediaFileUpload(thumbnail)).execute
                    )

            _upload_progress['status'] = 'complete'
            task_tracker.cancel_user_task(user_id, "upload_monitor")
            
            if db is not None:
                db_payload = {
                    "user_id": user_id, "media_id": str(media_id), "platform": platform, 
                    "upload_type": upload_type, "timestamp": datetime.now(timezone.utc),
                    "url": url, "title": final_title
                }
                if not from_schedule:
                    await asyncio.to_thread(db.uploads.insert_one, db_payload)
                else:
                    await asyncio.to_thread(db.scheduled_jobs.update_one, {"_id": ObjectId(job_id)}, {"$set": {"status": "completed", "final_url": url}})
                    await app.send_message(user_id, f"✅ **Scheduled Upload Complete!**\n\nYour {upload_type} '{final_title}' is published:\n{url}")

            log_msg = f"📤 New {platform} {upload_type}\n👤 User: `{user_id}`\n🔗 URL: {url}"
            success_msg = f"✅ {to_bold_sans('Uploaded Successfully!')}\n\n**Title**: {final_title}\n**Link**: {url}"
            
            await safe_threaded_reply(original_media_msg, success_msg, status_message=status_msg)
            await send_log_to_channel(app, LOG_CHANNEL, log_msg)

        except Exception as e:
            error_msg = f"❌ " + to_bold_sans(f"An Unexpected Error Occurred: {str(e)}")
            await safe_threaded_reply(original_media_msg, error_msg, status_message=status_msg)
            if from_schedule and db is not None:
                await asyncio.to_thread(db.scheduled_jobs.update_one, {"_id": ObjectId(job_id)}, {"$set": {"status": "failed", "error_message": str(e)}})
            logger.error(f"Upload failed for user {user_id}: {e}", exc_info=True)
            
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
            <html><head><title>Authentication Successful</title></head>
            <body><h1>✅ Authentication Successful!</h1>
            <p>You can close this window. Please copy the full URL from your browser's address bar and paste it back to the bot.</p>
            </body></html>"""
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
    except Exception as e:
        logger.error(f"Failed to log to channel {channel_id}: {e}")


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
        logger.critical(f"❌ DATABASE SETUP FAILED: {e}. Running in degraded mode.")
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

    admin_dm_text = ""
    if LOG_CHANNEL:
        try:
            chat = await app.get_chat(LOG_CHANNEL)
            if hasattr(chat, 'is_public') and chat.is_public:
                raise ValueError("LOG_CHANNEL must be a private channel.")
            member = await app.get_chat_member(LOG_CHANNEL, BOT_ID)
            if member.status not in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]:
                raise PermissionError("Bot is not an admin in LOG_CHANNEL.")
            valid_log_channel = True
            await app.send_message(LOG_CHANNEL, "✅ **" + to_bold_sans("Bot Is Now Online!") + "**")
        except Exception as e:
            error = f"Could not access LOG_CHANNEL ({LOG_CHANNEL}). Logging disabled. Error: {e}"
            logger.error(error)
            admin_dm_text += f"**LOGGING ERROR**: {error}\n\n"
            valid_log_channel = False
    
    if admin_dm_text:
        try:
            await app.send_message(ADMIN_ID, "⚠️ **Configuration Issues Detected**\n\n" + admin_dm_text + "Please fix and restart.")
        except Exception as e:
            logger.error(f"Failed to send configuration error DM to admin: {e}")


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
                await app.send_message(user_id, text, reply_markup=reply_markup)
            elif photo:
                await app.send_photo(user_id, photo, caption=text, reply_markup=reply_markup)
            elif video:
                await app.send_video(user_id, video, caption=text, reply_markup=reply_markup)
                
            sent_count += 1
            await asyncio.sleep(0.1)
        except UserIsBlocked:
            failed_count += 1
            logger.warning(f"User {user_id} has blocked the bot.")
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to send broadcast to user {user_id}: {e}")
            
    await status_msg.edit_text(f"✅ **Broadcast finished!**\nSent to `{sent_count}` users, failed for `{failed_count}` users.")
    await send_log_to_channel(app, LOG_CHANNEL,
        f"📢 Broadcast by admin `{admin_msg.from_user.id}`\n"
        f"Sent: `{sent_count}`, Failed: `{failed_count}`"
    )

async def weekly_report_scheduler():
    while not shutdown_event.is_set():
        await asyncio.sleep(3600)
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
                        stored_msg = await app.get_messages(job['original_chat_id'], job['original_message_id'])
                        if not stored_msg:
                            raise FileNotFoundError(f"Message {job['original_message_id']} not found in chat {job['original_chat_id']}.")
                        
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

        await asyncio.sleep(60)
    logger.info("Scheduler worker stopped.")
    
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
            async def answer(self, *args, **kwargs):
                pass
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
