import os
import sys
import asyncio
import threading
import logging
import subprocess
import json
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
import signal
from functools import wraps, partial
import re
import time
import requests

# Load environment variables
from dotenv import load_dotenv

load_dotenv()
# MongoDB
from pymongo import MongoClient
from pymongo.errors import OperationFailure
# Pyrogram (Telegram Bot)
from pyrogram import Client, filters, enums, idle
from pyrogram.errors import UserNotParticipant, FloodWait
from pyrogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove
)
# Google/YouTube Client
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google.auth.exceptions import RefreshError
# System Utilities
import psutil
import GPUtil
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log")
    ]
)
logger = logging.getLogger("YtBot")

# === Load and Validate Environment Variables ===
API_ID_STR = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
LOG_CHANNEL_STR = os.getenv("LOG_CHANNEL_ID") # e.g., -1001234567890
MONGO_URI = os.getenv("MONGO_DB")
ADMIN_ID_STR = os.getenv("ADMIN_ID")

# Validate required environment variables
if not all([API_ID_STR, API_HASH, BOT_TOKEN, ADMIN_ID_STR, MONGO_URI]):
    logger.critical("FATAL ERROR: One or more required environment variables are missing. Please check TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_BOT_TOKEN, ADMIN_ID, and MONGO_DB.")
    sys.exit(1)

# Convert to correct types after validation
API_ID = int(API_ID_STR)
ADMIN_ID = int(ADMIN_ID_STR)
LOG_CHANNEL = int(LOG_CHANNEL_STR) if LOG_CHANNEL_STR else None

# === Video Conversion Helpers ===

def needs_conversion(input_file: str) -> bool:
    """
    Checks if a video file needs conversion to be compatible (MP4/AAC).
    Uses ffprobe to inspect the file's container and audio codec.
    """
    try:
        command = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json',
            '-show_format', '-show_streams', input_file
        ]
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8')
        data = json.loads(result.stdout)
        
        format_name = data.get('format', {}).get('format_name', '')
        is_compatible_container = any(x in format_name for x in ['mp4', 'mov', '3gp'])

        audio_codec = 'none'
        for stream in data.get('streams', []):
            if stream.get('codec_type') == 'audio':
                audio_codec = stream.get('codec_name')
                break
        
        is_compatible_audio = (audio_codec == 'aac' or audio_codec == 'none')

        if is_compatible_container and is_compatible_audio:
            logger.info(f"'{input_file}' is already compatible. No conversion needed.")
            return False
        else:
            logger.warning(f"'{input_file}' needs conversion (Container: {format_name}, Audio: {audio_codec}).")
            return True

    except FileNotFoundError:
        logger.error("ffprobe/ffmpeg is not installed. Assuming conversion is needed as a fallback.")
        return True
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        logger.error(f"Could not probe file '{input_file}'. Assuming conversion is needed.")
        return True

def fix_video_format(input_file: str, output_file: str) -> str:
    """
    Converts a video file to a web-compatible format (MP4 container, AAC audio).
    """
    try:
        logger.info(f"Converting '{input_file}' to compatible format...")
        command = [
            'ffmpeg', '-y', '-i', input_file, '-c:v', 'copy',
            '-c:a', 'aac', '-b:a', '192k', '-ar', '48000',
            '-movflags', '+faststart', output_file
        ]
        
        subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"Successfully converted video to '{output_file}'.")
        return output_file
        
    except FileNotFoundError:
        logger.critical("ffmpeg is not installed. Video conversion is not possible.")
        raise FileNotFoundError("ffmpeg is not installed. Cannot process video files.")
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg conversion failed for {input_file}. Error: {e.stderr}")
        raise ValueError(f"Video format is incompatible and conversion failed. Error: {e.stderr}")


# === Global Bot Settings ===
DEFAULT_GLOBAL_SETTINGS = {
    "special_event_toggle": False,
    "special_event_title": "🎉 Special Event!",
    "special_event_message": "Enjoy our special event features!",
    "max_concurrent_uploads": 15,
    "max_file_size_mb": 250,
    "payment_settings": {
        "google_play_qr_file_id": "",
        "upi": "", "usdt": "", "btc": "", "others": "", "custom_buttons": {}
    }
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
    sanitized_text = text.encode('utf-8', 'surrogatepass').decode('utf-8')
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
        buttons.insert(1, yt_buttons)

    buttons.append([KeyboardButton("⭐ ᴩʀᴇᴍɪᴜᴍ"), KeyboardButton("/premiumdetails")])
    if is_admin(user_id):
        buttons.append([KeyboardButton("🛠 ᴀᴅᴍɪɴ ᴩᴀɴᴇʟ"), KeyboardButton("🔄 ʀᴇꜱᴛᴀʀᴛ ʙᴏᴛ")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, selective=True)

def get_main_settings_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📘 ғᴀᴄᴇʙᴏᴏᴋ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="hub_settings_facebook")],
        [InlineKeyboardButton("▶️ yᴏᴜᴛᴜʙᴇ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="hub_settings_youtube")],
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ])

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
        [InlineKeyboardButton("❌ No, Cancel", callback_data=f"manage_{platform}_accounts")]
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
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📢 Special Event ({event_status})", callback_data="toggle_special_event")],
        [InlineKeyboardButton("✏️ Set Event Title", callback_data="set_event_title")],
        [InlineKeyboardButton("💬 Set Event Message", callback_data="set_event_message")],
        [InlineKeyboardButton("ᴍᴀx ᴜᴩʟᴏᴀᴅ ᴜꜱᴇʀꜱ", callback_data="set_max_uploads")],
        [InlineKeyboardButton("ʀᴇꜱᴇᴛ ꜱᴛᴀᴛꜱ", callback_data="reset_stats")],
        [InlineKeyboardButton("ꜱʜᴏᴡ ꜱyꜱᴛᴇᴍ ꜱᴛᴀᴛꜱ", callback_data="show_system_stats")],
        [InlineKeyboardButton("💰 ᴩᴀyᴍᴇɴᴛ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="payment_settings_panel")],
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ", callback_data="admin_panel")]
    ])

payment_settings_markup = InlineKeyboardMarkup([
    [InlineKeyboardButton("🆕 ᴄʀᴇᴀᴛᴇ ᴩᴀyᴍᴇɴᴛ ʙᴜᴛᴛᴏɴ", callback_data="create_custom_payment_button")],
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

    payment_buttons.append([InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ ᴩʀᴇᴍɪᴜᴍ ᴩʟᴀɴꜱ", callback_data="back_to_premium_plans")])
    return InlineKeyboardMarkup(payment_buttons)

def get_progress_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="cancel_upload")]
    ])

def get_upload_options_markup(platform):
    """Markup shown AFTER deferred download and title set."""
    buttons = []
    if platform == 'youtube':
        buttons.extend([
            [InlineKeyboardButton("📄 ᴀᴅᴅ ᴅᴇꜱᴄʀɪᴩᴛɪᴏɴ", callback_data="add_description_yt")],
            [InlineKeyboardButton("🏷️ ᴀᴅᴅ ᴛᴀɢꜱ", callback_data="add_tags_yt")],
            [InlineKeyboardButton("🖼️ ᴀᴅᴅ ᴛʜᴜᴍʙɴᴀɪʟ", callback_data="add_thumbnail_yt")]
        ])
    elif platform == 'facebook':
         buttons.extend([
            [InlineKeyboardButton("📄 ᴀᴅᴅ ᴅᴇꜱᴄʀɪᴩᴛɪᴏɴ", callback_data="add_description_fb")]
        ])

    buttons.append([InlineKeyboardButton("⬆️ ᴜᴩʟᴏᴀᴅ", callback_data="upload_now")])
    buttons.append([InlineKeyboardButton("❌ ᴄᴀɴᴄᴇʟ", callback_data="cancel_upload")])
    return InlineKeyboardMarkup(buttons)

def get_yt_visibility_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌍 ᴩᴜʙʟɪᴄ", callback_data="set_visibility_public")],
        [InlineKeyboardButton("🔒 ᴩʀɪᴠᴀᴛᴇ", callback_data="set_visibility_private")],
        [InlineKeyboardButton("🔗 ᴜɴʟɪꜱᴛᴇᴅ", callback_data="set_visibility_unlisted")],
        [InlineKeyboardButton("🔙 ʙᴀᴄᴋ ᴛᴏ yᴛ ꜱᴇᴛᴛɪɴɢꜱ", callback_data="hub_settings_youtube")]
    ])

# ===================================================================
# ====================== HELPER FUNCTIONS ===========================
# ===================================================================

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

    if premium_type == "lifetime":
        return True

    if premium_until and isinstance(premium_until, datetime) and premium_until > datetime.utcnow():
        return True

    if premium_type and premium_until and premium_until <= datetime.utcnow():
        await asyncio.to_thread(
            db.users.update_one,
            {"_id": user_id},
            {"$set": {f"premium.{platform}.status": "expired"}}
        )
        logger.info(f"Premium for {platform} expired for user {user_id}. Status updated in DB.")

    return False

async def save_platform_session(user_id, platform, session_data):
    if db is None: return
    # session_data must contain a unique 'id' and a 'name'
    account_id = session_data['id']
    await asyncio.to_thread(
        db.sessions.update_one,
        {"user_id": user_id, "platform": platform, "account_id": account_id},
        {"$set": {
            "session_data": session_data,
            "logged_in_at": datetime.utcnow()
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
    
    # Facebook defaults
    settings.setdefault("caption_facebook", "")
    settings.setdefault("active_facebook_id", None)
    # YouTube defaults
    settings.setdefault("title_youtube", "")
    settings.setdefault("description_youtube", "")
    settings.setdefault("tags_youtube", "")
    settings.setdefault("visibility_youtube", "private")
    settings.setdefault("active_youtube_id", None)
    
    return settings

async def safe_edit_message(message, text, reply_markup=None, parse_mode=enums.ParseMode.MARKDOWN):
    try:
        if not message:
            logger.warning("safe_edit_message called with a None message object.")
            return
        current_text = getattr(message, 'text', '') or getattr(message, 'caption', '')
        if current_text and hasattr(current_text, 'strip') and current_text.strip() == text.strip() and message.reply_markup == reply_markup:
            return
        await message.edit_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
    except Exception as e:
        if "MESSAGE_NOT_MODIFIED" not in str(e):
            logger.warning(f"Couldn't edit message: {e}")

async def safe_reply(message, text, **kwargs):
    """A helper to reply to a message, safely handling different message types."""
    try:
        await message.reply(text, **kwargs)
    except Exception as e:
        logger.error(f"Failed to reply to message {message.id}: {e}")
        try:
            await app.send_message(message.chat.id, text, **kwargs)
        except Exception as e2:
            logger.error(f"Fallback send_message also failed for chat {message.chat.id}: {e2}")

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

def progress_callback_threaded(current, total, ud_type, msg_id, chat_id, start_time, last_update_time):
    now = time.time()
    if now - last_update_time[0] < 2 and current != total:
        return
    last_update_time[0] = now
    
    with threading.Lock():
        _progress_updates[(chat_id, msg_id)] = {
            "current": current, "total": total, "ud_type": ud_type, "start_time": start_time, "now": now
        }

async def monitor_progress_task(chat_id, msg_id, progress_msg):
    try:
        while True:
            await asyncio.sleep(2)
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
                    f"✅ **Downloaded**: `{current / (1024 * 1024):.2f}` MB / `{total / (1024 * 1024):.2f}` MB\n"
                    f"🚀 **Speed**: `{speed / (1024 * 1024):.2f}` MB/s\n"
                    f"⏳ **ETA**: `{eta}`"
                )
                try:
                    await safe_edit_message(
                        progress_msg, progress_text,
                        reply_markup=get_progress_markup(),
                        parse_mode=None
                    )
                except Exception:
                    pass
            
            if update_data and update_data['current'] == update_data['total']:
                with threading.Lock():
                    _progress_updates.pop((chat_id, msg_id), None)
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
            "added_at": datetime.utcnow(), "username": msg.from_user.username
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
        await _save_user_data(user_id, {"last_active": datetime.utcnow(), "username": msg.from_user.username})

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
            if p_expiry:
                remaining = p_expiry - datetime.utcnow()
                premium_details_text += f"⭐ {platform.capitalize()} premium expires in: `{remaining.days} days, {remaining.seconds // 3600} hours`.\n"

    if not has_any_premium:
        premium_details_text = (
            "🔥 **Key Features:**\n"
            "✅ Direct Login (No passwords needed for YouTube)\n"
            "✅ Ultra-fast uploading & High Quality\n"
            "✅ No file size limit & unlimited uploads\n"
            "✅ Facebook & YouTube Support\n\n"
            "👤 Contact Admin → [Admin Tom](https://t.me/CjjTom) to get premium\n"
            f"🆔 Your ID: `{user_id}`"
        )
    welcome_msg += premium_details_text
    await msg.reply(welcome_msg, reply_markup=get_main_keyboard(user_id, premium_platforms), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("restart") & filters.user(ADMIN_ID))
async def restart_cmd(_, msg):
    await restart_bot(msg)

@app.on_message(filters.command(["fblogin", "flogin"]))
@with_user_lock
async def facebook_login_cmd(_, msg):
    user_id = msg.from_user.id
    if not await is_premium_for_platform(user_id, "facebook"):
        return await msg.reply("❌ " + to_bold_sans("Facebook Premium Access Is Required. Use /premiumplan To Upgrade."))
    
    user_states[user_id] = {"action": "waiting_for_fb_app_id"}
    await msg.reply("🆔 " + to_bold_sans("Please Send Your Facebook App ID."))

@app.on_message(filters.command(["ytlogin", "ylogin"]))
@with_user_lock
async def youtube_login_cmd(_, msg):
    user_id = msg.from_user.id
    if not await is_premium_for_platform(user_id, "youtube"):
        return await msg.reply("❌ " + to_bold_sans("YouTube Premium Access Is Required. Use /premiumplan To Upgrade."))
    
    user_states[user_id] = {"action": "waiting_for_yt_client_id"}
    await msg.reply("🆔 " + to_bold_sans("Please Send Your Google OAuth Client ID."))

@app.on_message(filters.command(["buypypremium", "premiumplan"]))
@app.on_message(filters.regex("⭐ ᴩʀᴇᴍɪᴜᴍ"))
async def show_premium_options(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    premium_plans_text = (
        "⭐ " + to_bold_sans("Upgrade To Premium!") + " ⭐\n\n"
        + to_bold_sans("Unlock Full Features And Upload Unlimited Content Without Restrictions.") + "\n\n"
        "**Available Plans:**"
    )
    await msg.reply(premium_plans_text, reply_markup=get_premium_plan_markup(user_id), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("premiumdetails"))
async def premium_details_cmd(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
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
            status_text += f"**{platform.capitalize()} Premium:** "
            if premium_type == "lifetime":
                status_text += "🎉 **Lifetime!**\n"
            elif premium_until:
                remaining_time = premium_until - datetime.utcnow()
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
        status_text = "😔 " + to_bold_sans("You Have No Active Premium.") + "\n\n" + "Contact **[Admin Tom](https://t.me/CjjTom)** to buy a plan."

    await msg.reply(status_text, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("broadcast") & filters.user(ADMIN_ID))
async def broadcast_cmd(_, msg):
    if db is None:
        return await msg.reply("⚠️ " + to_bold_sans("Database Is Unavailable."))
    if len(msg.text.split(maxsplit=1)) < 2:
        return await msg.reply("Usage: `/broadcast <your message>`", parse_mode=enums.ParseMode.MARKDOWN)
    
    broadcast_message = msg.text.split(maxsplit=1)[1]
    users_cursor = await asyncio.to_thread(db.users.find, {})
    users = await asyncio.to_thread(list, users_cursor)
    sent_count, failed_count = 0, 0
    status_msg = await msg.reply("📢 " + to_bold_sans("Starting Broadcast..."))
    
    for user in users:
        try:
            if user["_id"] == ADMIN_ID: continue
            await app.send_message(user["_id"], broadcast_message, parse_mode=enums.ParseMode.MARKDOWN)
            sent_count += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to send broadcast to user {user['_id']}: {e}")
            
    await status_msg.edit_text(f"✅ **Broadcast finished!**\nSent to `{sent_count}` users, failed for `{failed_count}` users.")
    await send_log_to_channel(app, LOG_CHANNEL,
        f"📢 Broadcast by admin `{msg.from_user.id}`\n"
        f"Sent: `{sent_count}`, Failed: `{failed_count}`"
    )

@app.on_message(filters.command("skip") & filters.private)
@with_user_lock
async def handle_skip_command(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    if not state_data: return

    action = state_data.get('action')
    if action == 'waiting_for_title':
        file_info = state_data.get("file_info", {})
        file_info["custom_title"] = None  # Signal to use default
        user_states[user_id]["file_info"] = file_info
        await _deferred_download_and_show_options(msg, file_info)
    elif action in ['waiting_for_yt_description', 'waiting_for_yt_tags', 'waiting_for_yt_thumbnail']:
        # This allows skipping optional steps
        await upload_now_cb(app, type('obj', (object,), {'from_user': msg.from_user, 'message': state_data.get('status_msg')})())


# ===================================================================
# ======================== REGEX HANDLERS ===========================
# ===================================================================

@app.on_message(filters.regex("🔄 ʀᴇꜱᴛᴀʀᴛ ʙᴏᴛ") & filters.user(ADMIN_ID))
async def restart_button_handler(_, msg):
    await restart_bot(msg)

@app.on_message(filters.regex("⚙️ ꜱᴇᴛᴛɪɴɢꜱ"))
async def settings_menu(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    
    has_premium_any = await is_premium_for_platform(user_id, "facebook") or \
                      await is_premium_for_platform(user_id, "youtube")
    
    if not is_admin(user_id) and not has_premium_any:
        return await msg.reply("❌ " + to_bold_sans("Premium Required To Access Settings. Use /premiumplan To Upgrade."))
    
    await msg.reply(
        "⚙️ " + to_bold_sans("Configure Your Upload Settings:"),
        reply_markup=get_main_settings_markup()
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
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    if db is None: return await msg.reply("⚠️ " + to_bold_sans("Database Is Currently Unavailable."))
    
    total_users = await asyncio.to_thread(db.users.count_documents, {})
    
    pipeline = [
        {"$project": {
            "is_premium": {"$or": [
                {"$or": [
                    {"$eq": [f"$premium.{p}.type", "lifetime"]},
                    {"$gt": [f"$premium.{p}.until", datetime.utcnow()]}
                ]} for p in PREMIUM_PLATFORMS
            ]},
            "platforms": {p: {"$or": [
                {"$eq": [f"$premium.{p}.type", "lifetime"]},
                {"$gt": [f"$premium.{p}.until", datetime.utcnow()]}
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

    await msg.reply(stats_text, parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.regex("^(📘 FB ᴩᴏꜱᴛ|📘 FB ᴠɪᴅᴇᴏ|📘 FB ʀᴇᴇʟꜱ|▶️ YT ᴠɪᴅᴇᴏ|🟥 YT ꜱʜᴏʀᴛꜱ)"))
@with_user_lock
async def initiate_upload(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})

    type_map = {
        "📘 FB ᴩᴏꜱᴛ": ("facebook", "post"),
        "📘 FB ᴠɪᴅᴇᴏ": ("facebook", "video"),
        "📘 FB ʀᴇᴇʟꜱ": ("facebook", "reel"),
        "▶️ YT ᴠɪᴅᴇᴏ": ("youtube", "video"),
        "🟥 YT ꜱʜᴏʀᴛꜱ": ("youtube", "short"),
    }
    platform, upload_type = type_map[msg.text]

    if not await is_premium_for_platform(user_id, platform):
        return await msg.reply(f"❌ " + to_bold_sans(f"Your Access Has Been Denied. Please Upgrade To {platform.capitalize()} Premium."))

    sessions = await load_platform_sessions(user_id, platform)
    if not sessions:
        return await msg.reply(f"❌ " + to_bold_sans(f"Please Login To {platform.capitalize()} First Using /{platform[0]}login"), parse_mode=enums.ParseMode.MARKDOWN)
    
    action = f"waiting_for_{platform}_{upload_type}"
    user_states[user_id] = {"action": action, "platform": platform, "upload_type": upload_type}
    
    media_type = "photo" if upload_type == "post" else "video"
    await msg.reply("✅ " + to_bold_sans(f"Send The {media_type} File, Ready When You Are!"))


# ===================================================================
# ======================== TEXT HANDLERS ============================
# ===================================================================

@app.on_message(filters.text & filters.private & ~filters.command(""))
@with_user_lock
async def handle_text_input(_, msg):
    user_id = msg.from_user.id
    state_data = user_states.get(user_id)
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})

    if not state_data:
        return await msg.reply(to_bold_sans("I Don't Understand That. Please Use The Menu Buttons."))

    action = state_data.get("action")

    # --- Login Flows ---
    if action == "waiting_for_fb_app_id":
        user_states[user_id]['app_id'] = msg.text.strip()
        user_states[user_id]['action'] = 'waiting_for_fb_app_secret'
        return await msg.reply("🤫 " + to_bold_sans("Please Send Your Facebook App Secret."))
    
    elif action == "waiting_for_fb_app_secret":
        user_states[user_id]['app_secret'] = msg.text.strip()
        user_states[user_id]['action'] = 'waiting_for_fb_token'
        return await msg.reply("🔑 " + to_bold_sans("Please Send Your Long-lived Page Access Token."))
    
    elif action == "waiting_for_fb_token":
        token = msg.text.strip()
        login_msg = await msg.reply("🔐 " + to_bold_sans("Validating Token And Fetching Pages..."))
        
        try:
            url = f"https://graph.facebook.com/v18.0/me/accounts?access_token={token}"
            response = requests.get(url)
            response.raise_for_status()
            pages = response.json().get('data', [])
            
            if not pages:
                return await safe_edit_message(login_msg, "❌ " + to_bold_sans("No Pages Found For This Token. Ensure It Has `pages_show_list` And `pages_manage_posts` Permissions."))

            user_states[user_id]['token'] = token
            user_states[user_id]['pages'] = pages
            user_states[user_id]['action'] = 'selecting_fb_page'
            
            buttons = [[InlineKeyboardButton(page['name'], callback_data=f"select_fb_page_{page['id']}")] for page in pages]
            await safe_edit_message(login_msg, "📄 " + to_bold_sans("Select The Page You Want To Use:"), reply_markup=InlineKeyboardMarkup(buttons))

        except requests.RequestException as e:
            await safe_edit_message(login_msg, f"❌ " + to_bold_sans(f"Login Failed: Invalid token or API error. {e}"))
            if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_yt_client_id":
        user_states[user_id]['client_id'] = msg.text.strip()
        user_states[user_id]['action'] = "waiting_for_yt_client_secret"
        return await msg.reply("🤫 " + to_bold_sans("Please Send Your Google OAuth Client Secret."))

    elif action == "waiting_for_yt_client_secret":
        client_id = user_states[user_id]['client_id']
        client_secret = msg.text.strip()

        # Save secrets to state
        user_states[user_id]['client_secret'] = client_secret
        
        try:
            flow = InstalledAppFlow.from_client_config(
                {"web": {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"]
                }},
                scopes=["https://www.googleapis.com/auth/youtube.upload", "https://www.googleapis.com/auth/youtube.readonly"]
            )
            flow.redirect_uri = "urn:ietf:wg:oauth:2.0:oob"
            auth_url, _ = flow.authorization_url(prompt='consent')
            
            user_states[user_id]['flow'] = flow
            user_states[user_id]['action'] = 'waiting_for_yt_code'
            await msg.reply(
                f"🔗 {to_bold_sans('Click The Link Below To Authorize Your Account:')}\n\n"
                f"`{auth_url}`\n\n"
                f"{to_bold_sans('After Authorizing, Copy The Code And Paste It Here.')}",
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"YouTube OAuth flow failed for {user_id}: {e}")
            await msg.reply(f"❌ {to_bold_sans('Failed To Start Login Process.')} Error: {e}")
            if user_id in user_states: del user_states[user_id]

    elif action == "waiting_for_yt_code":
        code = msg.text.strip()
        flow = user_states[user_id]['flow']
        login_msg = await msg.reply("🔐 " + to_bold_sans("Fetching Tokens And Channel Info..."))
        
        try:
            await asyncio.to_thread(flow.fetch_token, code=code)
            credentials = flow.credentials
            
            youtube = build('youtube', 'v3', credentials=credentials)
            channels_response = await asyncio.to_thread(youtube.channels().list(part='snippet', mine=True).execute)
            
            if not channels_response.get('items'):
                return await safe_edit_message(login_msg, "❌ " + to_bold_sans("No YouTube Channel Found For This Account."))
            
            channel = channels_response['items'][0]
            channel_id = channel['id']
            channel_name = channel['snippet']['title']
            
            session_data = {
                'id': channel_id,
                'name': channel_name,
                'credentials_json': credentials.to_json()
            }
            await save_platform_session(user_id, "youtube", session_data)
            
            user_settings = await get_user_settings(user_id)
            user_settings["active_youtube_id"] = channel_id
            await save_user_settings(user_id, user_settings)
            
            await safe_edit_message(login_msg, f"✅ " + to_bold_sans(f"YouTube Login Successful For Channel: {channel_name}!"))
            await send_log_to_channel(app, LOG_CHANNEL, f"📝 New YouTube Login: User `{user_id}`, Channel: `{channel_name}`")
            if user_id in user_states: del user_states[user_id]
        
        except Exception as e:
            await safe_edit_message(login_msg, f"❌ " + to_bold_sans(f"Login Failed. Please Try /ytlogin Again. Error: {e}"))
            logger.error(f"YouTube code exchange failed for {user_id}: {e}")
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
        file_info = state_data.get("file_info", {})
        file_info["custom_title"] = msg.text
        user_states[user_id]["file_info"] = file_info
        await _deferred_download_and_show_options(msg, file_info)
    
    elif action in ["waiting_for_yt_description", "waiting_for_fb_description"]:
        platform = "youtube" if "yt" in action else "facebook"
        file_info = state_data.get("file_info", {})
        file_info["description"] = msg.text
        user_states[user_id]["file_info"] = file_info
        await msg.reply("✅ " + to_bold_sans("Description Set! Choose Next Option Or Upload."))
        user_states[user_id]['action'] = "waiting_for_upload_options"
    
    elif action == "waiting_for_yt_tags":
        file_info = state_data.get("file_info", {})
        file_info["tags"] = msg.text
        user_states[user_id]["file_info"] = file_info
        await msg.reply("✅ " + to_bold_sans("Tags Set! Choose Next Option Or Upload."))
        user_states[user_id]['action'] = "waiting_for_upload_options"

    # --- Admin Flow ---
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
            
    # ... (Other admin text handlers like max_uploads, proxy, etc. are preserved below) ...
    elif action == "waiting_for_max_uploads":
        if not is_admin(user_id): return
        try:
            new_limit = int(msg.text)
            if new_limit <= 0: return await msg.reply("❌ " + to_bold_sans("Limit Must Be A Positive Integer."))
            await _update_global_setting("max_concurrent_uploads", new_limit)
            global upload_semaphore
            upload_semaphore = asyncio.Semaphore(new_limit)
            await msg.reply(f"✅ " + to_bold_sans(f"Max Concurrent Uploads Set To `{new_limit}`."), reply_markup=get_admin_global_settings_markup())
            if user_id in user_states: del user_states[user_id]
        except ValueError:
            await msg.reply("❌ " + to_bold_sans("Invalid Input. Please Send A Valid Number."))

    elif action in ["waiting_for_event_title", "waiting_for_event_message"]:
        if not is_admin(user_id): return
        setting_key = "special_event_title" if action == "waiting_for_event_title" else "special_event_message"
        await _update_global_setting(setting_key, msg.text)
        await msg.reply(f"✅ " + to_bold_sans(f"Special Event `{setting_key.split('_')[-1]}` Updated!"), reply_markup=get_admin_global_settings_markup())
        if user_id in user_states: del user_states[user_id]

    elif action.startswith("waiting_for_payment_details_"):
        if not is_admin(user_id): return
        payment_method = action.replace("waiting_for_payment_details_", "")
        new_payment_settings = global_settings.get("payment_settings", {})
        new_payment_settings[payment_method] = msg.text
        await _update_global_setting("payment_settings", new_payment_settings)
        await msg.reply(f"✅ " + to_bold_sans(f"Payment Details For **{payment_method.upper()}** Updated."), reply_markup=payment_settings_markup, parse_mode=enums.ParseMode.MARKDOWN)
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


# ===================================================================
# =================== CALLBACK QUERY HANDLERS =======================
# ===================================================================

@app.on_callback_query(filters.regex("^hub_settings_"))
async def hub_settings_cb(_, query):
    platform = query.data.split("_")[-1]
    if platform == "facebook":
        await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Configure Facebook Settings:"), reply_markup=get_facebook_settings_markup())
    elif platform == "youtube":
        await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Configure YouTube Settings:"), reply_markup=get_youtube_settings_markup())

# --- Account Management Callbacks ---
@app.on_callback_query(filters.regex("^manage_(fb|yt)_accounts$"))
async def manage_accounts_cb(_, query):
    user_id = query.from_user.id
    platform = "facebook" if "fb" in query.data else "youtube"
    
    sessions = await load_platform_sessions(user_id, platform)
    logged_in_accounts = {s['account_id']: s['session_data']['name'] for s in sessions}
    
    if not logged_in_accounts:
        await query.answer(f"You have no {platform.capitalize()} accounts logged in. Let's add one.", show_alert=True)
        # This will trigger the login flow
        await add_account_cb(app, type('obj', (object,), {'from_user': query.from_user, 'message': query.message, 'data': f'add_account_{platform}'})())
        return

    await safe_edit_message(query.message, "👤 " + to_bold_sans(f"Select Your Active {platform.capitalize()} Account"),
        reply_markup=await get_account_markup(user_id, platform, logged_in_accounts)
    )

@app.on_callback_query(filters.regex("^select_acc_"))
async def select_account_cb(_, query):
    user_id = query.from_user.id
    _, platform, acc_id = query.data.split("_")
    
    user_settings = await get_user_settings(user_id)
    user_settings[f"active_{platform}_id"] = acc_id
    await save_user_settings(user_id, user_settings)
    
    await query.answer(f"✅ Active account for {platform.capitalize()} has been updated.", show_alert=True)
    await manage_accounts_cb(app, type('obj', (object,), {'from_user': query.from_user, 'message': query.message, 'data': f'manage_{platform}_accounts'}()) ) # Refresh panel

@app.on_callback_query(filters.regex("^confirm_logout_"))
async def confirm_logout_cb(_, query):
    _, platform, acc_id = query.data.split("_")
    sessions = await load_platform_sessions(query.from_user.id, platform)
    acc_name = next((s['session_data']['name'] for s in sessions if s['account_id'] == acc_id), "Account")
    
    await safe_edit_message(
        query.message,
        to_bold_sans(f"Logout {acc_name}? You Can Re-login Later."),
        reply_markup=get_logout_confirm_markup(platform, acc_id, acc_name)
    )

@app.on_callback_query(filters.regex("^logout_acc_"))
async def logout_account_cb(_, query):
    user_id = query.from_user.id
    _, platform, acc_id_to_logout = query.data.split("_")

    await delete_platform_session(user_id, platform, acc_id_to_logout)
    
    user_settings = await get_user_settings(user_id)
    if user_settings.get(f"active_{platform}_id") == acc_id_to_logout:
        sessions = await load_platform_sessions(user_id, platform)
        user_settings[f"active_{platform}_id"] = sessions[0]['account_id'] if sessions else None
        await save_user_settings(user_id, user_settings)
    
    await query.answer(f"✅ Logged out successfully.", show_alert=True)
    await manage_accounts_cb(app, type('obj', (object,), {'from_user': query.from_user, 'message': query.message, 'data': f'manage_{platform}_accounts'}()) )

@app.on_callback_query(filters.regex("^add_account_"))
async def add_account_cb(_, query):
    user_id = query.from_user.id
    platform = query.data.split("add_account_")[-1]
    
    if not await is_premium_for_platform(user_id, platform) and not is_admin(user_id):
        return await query.answer("❌ This is a premium feature.", show_alert=True)
    
    if platform == "facebook":
        user_states[user_id] = {"action": "waiting_for_fb_app_id"}
        await safe_edit_message(query.message, "🆔 " + to_bold_sans("Please Send Your Facebook App ID."))
    elif platform == "youtube":
        user_states[user_id] = {"action": "waiting_for_yt_client_id"}
        await safe_edit_message(query.message, "🆔 " + to_bold_sans("Please Send Your Google OAuth Client ID."))

# --- General Callbacks ---
@app.on_callback_query(filters.regex("^cancel_upload$"))
async def cancel_upload_cb(_, query):
    user_id = query.from_user.id
    await query.answer("Upload cancelled.", show_alert=True)
    await safe_edit_message(query.message, "❌ **" + to_bold_sans("Upload Cancelled") + "**\n\n" + to_bold_sans("Your Operation Has Been Successfully Cancelled."))

    state_data = user_states.get(user_id, {})
    file_info = state_data.get("file_info", {})
    files_to_clean = [file_info.get("downloaded_path"), file_info.get("thumbnail_path")]
    
    cleanup_temp_files(files_to_clean)
    if user_id in user_states: del user_states[user_id]
    await task_tracker.cancel_all_user_tasks(user_id)
    logger.info(f"User {user_id} cancelled their upload.")

@app.on_callback_query(filters.regex("^upload_now$"))
async def upload_now_cb(_, query):
    user_id = query.from_user.id
    state_data = user_states.get(user_id)
    if not state_data or "file_info" not in state_data:
        return await query.answer("❌ Error: No upload process found.", show_alert=True)
    
    file_info = state_data["file_info"]
    status_msg = state_data.get("status_msg") or query.message
    await safe_edit_message(status_msg, "🚀 " + to_bold_sans("Starting Upload Now..."))
    await start_upload_task(status_msg, file_info, user_id=query.from_user.id)

@app.on_callback_query(filters.regex("^(add_description|add_tags|add_thumbnail)_(fb|yt)$"))
async def add_metadata_cb(_, query):
    user_id = query.from_user.id
    action, platform = query.data.split("_")
    
    state_data = user_states.get(user_id)
    if not state_data or 'file_info' not in state_data:
        return await query.answer("❌ Error: State lost, please start over.", show_alert=True)

    if action == 'add_description':
        user_states[user_id]['action'] = f'waiting_for_{platform}_description'
        await safe_edit_message(query.message, "📄 " + to_bold_sans("Please Send The Description."))
    elif action == 'add_tags':
        user_states[user_id]['action'] = 'waiting_for_yt_tags'
        await safe_edit_message(query.message, "🏷️ " + to_bold_sans("Please Send Comma-separated Tags."))
    elif action == 'add_thumbnail':
        user_states[user_id]['action'] = 'waiting_for_yt_thumbnail'
        await safe_edit_message(query.message, "🖼️ " + to_bold_sans("Please Send The Thumbnail Image."))

# --- Premium & Payment Callbacks (Largely Unchanged) ---
@app.on_callback_query(filters.regex("^buypypremium$"))
async def buypypremium_cb(_, query):
    user_id = query.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    
    premium_plans_text = (
        "⭐ " + to_bold_sans("Upgrade To Premium!") + " ⭐\n\n"
        + to_bold_sans("Unlock Full Features And Upload Unlimited Content Without Restrictions.") + "\n\n"
        "**Available Plans:**"
    )
    await safe_edit_message(query.message, premium_plans_text, reply_markup=get_premium_plan_markup(user_id), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^show_plan_details_"))
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
async def show_payment_methods_cb(_, query):
    payment_methods_text = "**" + to_bold_sans("Available Payment Methods") + "**\n\n"
    payment_methods_text += to_bold_sans("Choose Your Preferred Method To Proceed With Payment.")
    await safe_edit_message(query.message, payment_methods_text, reply_markup=get_payment_methods_markup(), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^show_payment_qr_google_play$"))
async def show_payment_qr_google_play_cb(_, query):
    qr_file_id = global_settings.get("payment_settings", {}).get("google_play_qr_file_id")
    if not qr_file_id:
        await query.answer("QR code is not set by the admin yet.", show_alert=True)
        return
    
    caption_text = "**" + to_bold_sans("Scan & Pay") + "**\n\n" + \
                   "Send a screenshot to **[Admin Tom](https://t.me/CjjTom)** for activation."
    
    await query.message.reply_photo(
        photo=qr_file_id,
        caption=caption_text,
        parse_mode=enums.ParseMode.MARKDOWN
    )
    await query.answer()

@app.on_callback_query(filters.regex("^show_payment_details_"))
async def show_payment_details_cb(_, query):
    method = query.data.split("show_payment_details_")[1]
    payment_details = global_settings.get("payment_settings", {}).get(method, "No details available.")
    text = (
        f"**{to_bold_sans(f'{method.upper()} Payment Details')}**\n\n"
        f"`{payment_details}`\n\n"
        f"Contact **[Admin Tom](https://t.me/CjjTom)** with a screenshot for activation."
    )
    await safe_edit_message(query.message, text, reply_markup=get_payment_methods_markup(), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^show_custom_payment_"))
async def show_custom_payment_cb(_, query):
    button_name = query.data.split("show_custom_payment_")[1]
    payment_details = global_settings.get("payment_settings", {}).get("custom_buttons", {}).get(button_name, "No details available.")
    text = (
        f"**{to_bold_sans(f'{button_name.upper()} Payment Details')}**\n\n"
        f"`{payment_details}`\n\n"
        f"Contact **[Admin Tom](https://t.me/CjjTom)** with a screenshot for activation."
    )
    await safe_edit_message(query.message, text, reply_markup=get_payment_methods_markup(), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^buy_now$"))
async def buy_now_cb(_, query):
    text = (
        f"**{to_bold_sans('Purchase Confirmation')}**\n\n"
        f"Please contact **[Admin Tom](https://t.me/CjjTom)** to complete the payment."
    )
    await safe_edit_message(query.message, text, parse_mode=enums.ParseMode.MARKDOWN)

# --- Admin Panel Callbacks ---
@app.on_callback_query(filters.regex("^admin_panel$"))
async def admin_panel_cb(_, query):
    if not is_admin(query.from_user.id):
        return await query.answer("❌ Admin access required", show_alert=True)
    await safe_edit_message(
        query.message,
        "🛠 " + to_bold_sans("Welcome To The Admin Panel!"),
        reply_markup=admin_markup,
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^global_settings_panel$"))
async def global_settings_panel_cb(_, query):
    if not is_admin(query.from_user.id):
        return await query.answer("❌ Admin access required", show_alert=True)
    
    settings_text = (
        "⚙️ **" + to_bold_sans("Global Bot Settings") + "**\n\n"
        f"**📢 Special Event:** `{global_settings.get('special_event_toggle', False)}`\n"
        f"**Max concurrent uploads:** `{global_settings.get('max_concurrent_uploads')}`\n"
    )
    await safe_edit_message(query.message, settings_text, reply_markup=get_admin_global_settings_markup(), parse_mode=enums.ParseMode.MARKDOWN)

# --- Back Callbacks ---
@app.on_callback_query(filters.regex("^back_to_"))
async def back_to_cb(_, query):
    data = query.data
    user_id = query.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    
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
        await safe_edit_message(query.message, "⚙️ " + to_bold_sans("Settings Panel"), reply_markup=get_main_settings_markup())
    elif data == "back_to_admin":
        await admin_panel_cb(app, query)
    elif data == "back_to_premium_plans":
        await buypypremium_cb(app, query)
    elif data == "back_to_global":
        await global_settings_panel_cb(app, query)

# --- Trial Activation ---
@app.on_callback_query(filters.regex("^activate_trial_"))
async def activate_trial_cb(_, query):
    user_id = query.from_user.id
    platform = query.data.split("_")[-1]
    
    if await is_premium_for_platform(user_id, platform):
        return await query.answer(f"Your {platform.capitalize()} trial is already active!", show_alert=True)

    premium_until = datetime.utcnow() + timedelta(hours=6)
    user_data = await _get_user_data(user_id) or {}
    user_premium_data = user_data.get("premium", {})
    user_premium_data[platform] = {
        "type": "6_hour_trial", "added_by": "callback_trial",
        "added_at": datetime.utcnow(), "until": premium_until,
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

# ... (Other admin callbacks are preserved below) ...
@app.on_callback_query(filters.regex("^toggle_special_event$"))
async def toggle_special_event_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    new_status = not global_settings.get("special_event_toggle", False)
    await _update_global_setting("special_event_toggle", new_status)
    await query.answer(f"Special Event toggled {'ON' if new_status else 'OFF'}.", show_alert=True)
    await global_settings_panel_cb(app, query)

@app.on_callback_query(filters.regex("^set_event_title$"))
async def set_event_title_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required.", show_alert=True)
    user_states[query.from_user.id] = {"action": "waiting_for_event_title"}
    await safe_edit_message(query.message, "✏️ " + to_bold_sans("Please Send The New Title For The Special Event."))

@app.on_callback_query(filters.regex("^set_event_message$"))
async def set_event_message_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required.", show_alert=True)
    user_states[query.from_user.id] = {"action": "waiting_for_event_message"}
    await safe_edit_message(query.message, "💬 " + to_bold_sans("Please Send The New Message For The Special Event."))

@app.on_callback_query(filters.regex("^set_max_uploads$"))
@with_user_lock
async def set_max_uploads_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    user_states[query.from_user.id] = {"action": "waiting_for_max_uploads"}
    current_limit = global_settings.get("max_concurrent_uploads")
    await safe_edit_message(
        query.message,
        to_bold_sans(f"Please Send The New Max Number Of Concurrent Uploads.\ncurrent Limit: `{current_limit}`"),
        parse_mode=enums.ParseMode.MARKDOWN
    )

@app.on_callback_query(filters.regex("^reset_stats$"))
@with_user_lock
async def reset_stats_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    await safe_edit_message(query.message, "⚠️ **WARNING!** " + to_bold_sans("Are You Sure You Want To Reset All Upload Stats?"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes, reset stats", callback_data="confirm_reset_stats")],
            [InlineKeyboardButton("❌ No, cancel", callback_data="admin_panel")]
        ]), parse_mode=enums.ParseMode.MARKDOWN)

@app.on_callback_query(filters.regex("^confirm_reset_stats$"))
@with_user_lock
async def confirm_reset_stats_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    if db is None: return await query.answer("⚠️ Database unavailable.", show_alert=True)
    
    result = await asyncio.to_thread(db.uploads.delete_many, {})
    await query.answer(f"✅ All stats reset! Deleted {result.deleted_count} uploads.", show_alert=True)
    await admin_panel_cb(app, query)
    await send_log_to_channel(app, LOG_CHANNEL, f"📊 Admin `{query.from_user.id}` has reset all bot upload stats.")

@app.on_callback_query(filters.regex("^show_system_stats$"))
async def show_system_stats_cb(_, query):
    if not is_admin(query.from_user.id): return await query.answer("❌ Admin access required", show_alert=True)
    try:
        cpu_usage = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        system_stats_text = (
            f"💻 **{to_bold_sans('System Stats')}**\n\n"
            f"**CPU:** `{cpu_usage}%`\n"
            f"**RAM:** `{ram.percent}%` (Used: `{ram.used / (1024**3):.2f}` GB / Total: `{ram.total / (1024**3):.2f}` GB)\n"
            f"**Disk:** `{disk.percent}%` (Used: `{disk.used / (1024**3):.2f}` GB / Total: `{disk.total / (1024**3):.2f}` GB)\n\n"
        )
        gpu_info = ""
        try:
            gpus = GPUtil.getGPUs()
            if gpus:
                gpu_info = "**GPU Info:**\n"
                for i, gpu in enumerate(gpus):
                    gpu_info += (
                        f"  - **GPU {i}:** `{gpu.name}`\n"
                        f"  - Load: `{gpu.load*100:.1f}%`, Temp: `{gpu.temperature}°C`\n"
                    )
        except Exception:
            gpu_info = "Could not retrieve GPU info."
            
        system_stats_text += gpu_info
        await safe_edit_message(
            query.message, system_stats_text,
            reply_markup=get_admin_global_settings_markup(),
            parse_mode=enums.ParseMode.MARKDOWN
        )
    except Exception as e:
        await query.answer("❌ Failed to retrieve system stats.", show_alert=True)
        logger.error(f"Error retrieving system stats: {e}")

@app.on_callback_query(filters.regex("^select_fb_page_"))
async def select_fb_page_cb(_, query):
    user_id = query.from_user.id
    state_data = user_states.get(user_id, {})
    if not state_data or state_data.get('action') != 'selecting_fb_page':
        return await query.answer("❌ Error: State lost. Please /fblogin again.", show_alert=True)

    page_id = query.data.split("_")[-1]
    page = next((p for p in state_data['pages'] if p['id'] == page_id), None)
    
    if not page:
        return await query.answer("❌ Error: Invalid page selected.", show_alert=True)

    session_data = {
        'id': page['id'],
        'name': page['name'],
        'access_token': page['access_token'],
        'app_id': state_data['app_id'] # Store for potential future use
    }
    await save_platform_session(user_id, "facebook", session_data)

    user_settings = await get_user_settings(user_id)
    user_settings["active_facebook_id"] = page['id']
    await save_user_settings(user_id, user_settings)
    
    await safe_edit_message(query.message, f"✅ " + to_bold_sans(f"Facebook Login Successful For Page: {page['name']}!"))
    await send_log_to_channel(app, LOG_CHANNEL, f"📝 New Facebook Login: User `{user_id}`, Page: `{page['name']}`")
    if user_id in user_states: del user_states[user_id]


@app.on_callback_query(filters.regex("^set_visibility_"))
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

async def _deferred_download_and_show_options(msg, file_info):
    """Downloads the media and then shows the final upload options."""
    user_id = msg.from_user.id
    
    processing_msg = await msg.reply("⏳ " + to_bold_sans("Starting Download..."))
    file_info["processing_msg"] = processing_msg
    
    try:
        start_time = time.time()
        last_update_time = [0]
        task_tracker.create_task(monitor_progress_task(msg.chat.id, processing_msg.id, processing_msg), user_id=user_id, task_name="progress_monitor")
        
        file_info["downloaded_path"] = await app.download_media(
            file_info["original_media_msg"],
            progress=progress_callback_threaded,
            progress_args=("Download", processing_msg.id, msg.chat.id, start_time, last_update_time)
        )
        
        task_tracker.cancel_user_task(user_id, "progress_monitor")

        title_preview = file_info.get('custom_title') or '*(Using Default Title)*'
        if len(title_preview) > 100:
            title_preview = title_preview[:100] + "..."
            
        await safe_edit_message(
            processing_msg,
            "📝 " + to_bold_sans("Title Ready. Choose Options Or Upload:") + f"\n\n**Preview:** `{title_preview}`",
            reply_markup=get_upload_options_markup(file_info['platform']),
            parse_mode=enums.ParseMode.MARKDOWN
        )
        user_states[user_id] = {"action": "waiting_for_upload_options", "file_info": file_info, "status_msg": processing_msg}
        task_tracker.create_task(safe_task_wrapper(timeout_task(user_id, processing_msg.id)), user_id=user_id, task_name="timeout")

    except asyncio.CancelledError:
        logger.info(f"Deferred download cancelled by user {user_id}.")
        cleanup_temp_files([file_info.get("downloaded_path")])
    except Exception as e:
        logger.error(f"Error during deferred file download for user {user_id}: {e}", exc_info=True)
        await safe_edit_message(processing_msg, f"❌ " + to_bold_sans(f"Download Failed: {e}"))
        cleanup_temp_files([file_info.get("downloaded_path")])
        if user_id in user_states: del user_states[user_id]

@app.on_message(filters.media & filters.private)
@with_user_lock
async def handle_media_upload(_, msg):
    user_id = msg.from_user.id
    await _save_user_data(user_id, {"last_active": datetime.utcnow()})
    state_data = user_states.get(user_id, {})

    if is_admin(user_id) and state_data and state_data.get("action") == "waiting_for_google_play_qr" and msg.photo:
        # This is for the admin payment settings panel
        new_payment_settings = global_settings.get("payment_settings", {})
        new_payment_settings["google_play_qr_file_id"] = msg.photo.file_id
        await _update_global_setting("payment_settings", new_payment_settings)
        if user_id in user_states: del user_states[user_id]
        return await msg.reply("✅ " + to_bold_sans("Google Pay QR Code Image Saved!"), reply_markup=payment_settings_markup)

    # Check for thumbnail upload during YouTube flow
    if state_data and state_data.get("action") == 'waiting_for_yt_thumbnail':
        if not msg.photo:
            return await msg.reply("❌ " + to_bold_sans("Please send an image file for the thumbnail."))
        
        status_msg = await msg.reply("🖼️ " + to_bold_sans("Downloading thumbnail..."))
        thumb_path = await app.download_media(msg.photo)
        state_data['file_info']['thumbnail_path'] = thumb_path
        user_states[user_id]['action'] = "waiting_for_upload_options"
        await status_msg.edit_text("✅ " + to_bold_sans("Thumbnail Set! Choose Next Option Or Upload."))
        return

    action = state_data.get("action")
    valid_actions = [
        "waiting_for_facebook_post", "waiting_for_facebook_video", "waiting_for_facebook_reel",
        "waiting_for_youtube_video", "waiting_for_youtube_short"
    ]
    if not action or action not in valid_actions:
        return await msg.reply("❌ " + to_bold_sans("Please Use One Of The Upload Buttons First."))

    media = msg.video or msg.photo or msg.document
    if not media: return await msg.reply("❌ " + to_bold_sans("Unsupported Media Type."))

    if media.file_size > MAX_FILE_SIZE_BYTES:
        if user_id in user_states: del user_states[user_id]
        return await msg.reply(f"❌ " + to_bold_sans(f"File Size Exceeds The Limit Of `{MAX_FILE_SIZE_BYTES / (1024 * 1024):.0f}` Mb."))

    file_info = {
        "platform": state_data["platform"],
        "upload_type": state_data["upload_type"],
        "original_media_msg": msg
    }
    
    user_states[user_id] = {"action": "waiting_for_title", "file_info": file_info}
    await msg.reply(
        to_bold_sans("Media Received. First, Send Your Title.") + "\n\n" +
        "• " + to_bold_sans("Send Text Now") + "\n" +
        "• Or use `/skip` to use your default title."
    )


# ===================================================================
# ==================== UPLOAD PROCESSING ==========================
# ===================================================================

async def start_upload_task(msg, file_info, user_id):
    task_tracker.create_task(
        safe_task_wrapper(process_and_upload(msg, file_info, user_id)),
        user_id=user_id,
        task_name="upload"
    )

async def process_and_upload(msg, file_info, user_id):
    platform = file_info["platform"]
    upload_type = file_info["upload_type"]
    processing_msg = file_info.get("processing_msg") or msg
    
    task_tracker.cancel_user_task(user_id, "timeout")

    async with upload_semaphore:
        logger.info(f"Semaphore acquired for user {user_id}. Starting upload to {platform}.")
        files_to_clean = [file_info.get("downloaded_path"), file_info.get("thumbnail_path")]
        try:
            user_settings = await get_user_settings(user_id)
            
            path = file_info.get("downloaded_path")
            if not path or not os.path.exists(path):
                raise FileNotFoundError("Downloaded file path is missing or invalid.")
            
            # --- Video conversion check ---
            is_video = file_info['original_media_msg'].video or (file_info['original_media_msg'].document and 'video' in file_info['original_media_msg'].document.mime_type)
            upload_path = path
            if is_video and await asyncio.to_thread(needs_conversion, path):
                await safe_edit_message(processing_msg, "⚙️ " + to_bold_sans("Processing Video... This May Take A Moment."))
                fixed_path = path.rsplit(".", 1)[0] + "_fixed.mp4"
                converted_path = await asyncio.to_thread(fix_video_format, path, fixed_path)
                files_to_clean.append(converted_path)
                upload_path = converted_path

            await safe_edit_message(processing_msg, "⬆️ " + to_bold_sans(f"Uploading To {platform.capitalize()}... Please Wait."))

            # --- Platform-specific upload logic ---
            url, media_id = "N/A", "N/A"
            final_title = file_info.get("custom_title") if file_info.get("custom_title") is not None else user_settings.get(f"title_{platform}", user_settings.get(f"caption_{platform}", ""))
            
            if platform == "facebook":
                session = await get_active_session(user_id, 'facebook')
                if not session: raise ConnectionError("Facebook session not found. Please /fblogin.")
                page_id = session['id']
                token = session['access_token']
                description = file_info.get("description", "")
                
                if upload_type == 'post':
                    # TODO: Implement Facebook photo post
                    with open(upload_path, 'rb') as f:
                        post_url = f"https://graph.facebook.com/{page_id}/photos"
                        payload = {'access_token': token, 'caption': final_title + "\n\n" + description}
                        files = {'source': f}
                        r = requests.post(post_url, data=payload, files=files)
                        r.raise_for_status()
                        post_id = r.json()['post_id']
                        url = f"https://facebook.com/{post_id}"
                        media_id = post_id
                else: # video or reel
                    # TODO: Implement resumable video upload for robustness
                    endpoint = 'videos' if upload_type == 'video' else 'video_reels'
                    upload_url = f"https://graph-video.facebook.com/{page_id}/{endpoint}"
                    with open(upload_path, 'rb') as f:
                        params = {'access_token': token, 'description': final_title + "\n\n" + description}
                        files = {'source': f}
                        r = requests.post(upload_url, data=params, files=files, timeout=600)
                        r.raise_for_status()
                        video_id = r.json()['id']
                        url = f"https://facebook.com/{video_id}"
                        media_id = video_id
            
            elif platform == "youtube":
                session = await get_active_session(user_id, 'youtube')
                if not session: raise ConnectionError("YouTube session not found. Please /ytlogin.")
                
                creds = Credentials.from_authorized_user_info(json.loads(session['credentials_json']))
                if creds.expired and creds.refresh_token:
                    try:
                        creds.refresh(requests.Request())
                        session['credentials_json'] = creds.to_json()
                        await save_platform_session(user_id, 'youtube', {'id': session['id'], 'name': session['name'], 'credentials_json': session['credentials_json']})
                    except RefreshError as e:
                        raise ConnectionError(f"YouTube token expired and failed to refresh. Please /ytlogin again. Error: {e}")

                youtube = build('youtube', 'v3', credentials=creds)
                
                description = file_info.get("description", user_settings.get("description_youtube", ""))
                tags = file_info.get("tags", user_settings.get("tags_youtube", "")).split(',')
                visibility = user_settings.get("visibility_youtube", "private")
                thumbnail = file_info.get("thumbnail_path")

                body = {
                    "snippet": {
                        "title": final_title,
                        "description": description,
                        "tags": [tag.strip() for tag in tags if tag.strip()]
                    },
                    "status": {
                        "privacyStatus": visibility
                    }
                }
                
                media_file = MediaFileUpload(upload_path, chunksize=-1, resumable=True)
                request = youtube.videos().insert(part=",".join(body.keys()), body=body, media_body=media_file)
                
                # TODO: Implement progress monitoring for resumable uploads
                response = None
                while response is None:
                    status, response = await asyncio.to_thread(request.next_chunk)
                    if status:
                        logger.info(f"Uploaded {int(status.progress() * 100)}%")
                
                media_id = response['id']
                url = f"https://youtu.be/{media_id}"

                if thumbnail:
                    await asyncio.to_thread(youtube.thumbnails().set(videoId=media_id, media_body=MediaFileUpload(thumbnail)).execute)


            if db is not None:
                await asyncio.to_thread(db.uploads.insert_one, {
                    "user_id": user_id, "media_id": str(media_id),
                    "platform": platform, "upload_type": upload_type, "timestamp": datetime.utcnow(),
                    "url": url, "title": final_title
                })

            log_msg = f"📤 New {platform.capitalize()} {upload_type.capitalize()} Upload\n" \
                      f"👤 User: `{user_id}`\n🔗 URL: {url}\n📅 {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}"
            await safe_edit_message(processing_msg, f"✅ " + to_bold_sans("Uploaded Successfully!") + f"\n\n{url}", parse_mode=None)
            await send_log_to_channel(app, LOG_CHANNEL, log_msg)

        except (ConnectionError, RefreshError, requests.RequestException, FileNotFoundError) as e:
            error_msg = f"❌ " + to_bold_sans(f"Upload Failed: {e}")
            await safe_edit_message(processing_msg, error_msg)
            logger.error(f"Upload error for {user_id}: {e}")
        except Exception as e:
            error_msg = f"❌ " + to_bold_sans(f"An Unexpected Error Occurred: {str(e)}")
            await safe_edit_message(processing_msg, error_msg)
            logger.error(f"General upload failed for {user_id} on {platform}: {e}", exc_info=True)
        finally:
            cleanup_temp_files(files_to_clean)
            if user_id in user_states: del user_states[user_id]
            logger.info(f"Semaphore released for user {user_id}.")

async def timeout_task(user_id, message_id):
    await asyncio.sleep(900) # 15 minutes
    if user_id in user_states:
        state = user_states.pop(user_id)
        logger.info(f"Task for user {user_id} timed out and was canceled. State was: {state.get('action')}")
        try:
            await app.edit_message_text(
                chat_id=user_id, message_id=message_id,
                text="⚠️ " + to_bold_sans("Timeout! The Operation Was Canceled Due To Inactivity.")
            )
            # Cleanup any downloaded files if timeout happens
            file_info = state.get('file_info', {})
            cleanup_temp_files([file_info.get("downloaded_path"), file_info.get("thumbnail_path")])
        except Exception as e:
            logger.warning(f"Could not send timeout message to user {user_id}: {e}")

# === HTTP Server for Health Checks ===
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot is running")
    def do_HEAD(self):
        self.send_response(200)

def run_server():
    try:
        server = HTTPServer(('0.0.0.0', 8080), HealthHandler)
        logger.info("HTTP health check server started on port 8080.")
        server.serve_forever()
    except Exception as e:
        logger.error(f"HTTP server failed: {e}")

async def send_log_to_channel(client, channel_id, text):
    global valid_log_channel
    if not valid_log_channel:
        return
    try:
        await client.send_message(channel_id, text, disable_web_page_preview=True, parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Failed to log to channel {channel_id}: {e}")
        valid_log_channel = False

# ===================================================================
# ======================== BOT STARTUP ============================
# ===================================================================
async def start_bot():
    global mongo, db, global_settings, upload_semaphore, MAX_CONCURRENT_UPLOADS, MAX_FILE_SIZE_BYTES, task_tracker, valid_log_channel

    try:
        mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        mongo.admin.command('ping')
        db = mongo.NowTok # Use a relevant DB name
        logger.info("✅ Connected to MongoDB successfully.")
        
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
    
    task_tracker.loop = asyncio.get_running_loop()

    if LOG_CHANNEL:
        try:
            await app.send_message(LOG_CHANNEL, "✅ **" + to_bold_sans("Bot Is Now Online And Running!") + "**", parse_mode=enums.ParseMode.MARKDOWN)
            valid_log_channel = True
        except Exception as e:
            logger.error(f"Could not log to channel {LOG_CHANNEL}. Invalid or bot isn't admin. Error: {e}")
            valid_log_channel = False

    logger.info("Bot is now online! Waiting for tasks...")
    await idle()

    logger.info("Shutting down...")
    await task_tracker.cancel_and_wait_all()
    await app.stop()
    if mongo:
        mongo.close()
    logger.info("Bot has been shut down gracefully.")

if __name__ == "__main__":
    task_tracker = TaskTracker()
    try:
        app.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Bot crashed during startup: {e}", exc_info=True)
