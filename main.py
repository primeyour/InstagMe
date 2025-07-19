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
import urllib.parse
import uuid # For unique filenames

from pyrogram import Client, filters, enums
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING
import requests

# Google API Client Imports (for YouTube)
# You need to install these: pip install google-api-python-client google-auth-oauthlib google-auth-httplib2
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

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

# === Facebook API Configuration ===
FACEBOOK_APP_ID = os.getenv("FACEBOOK_APP_ID", "")
FACEBOOK_APP_SECRET = os.getenv("FACEBOOK_APP_SECRET", "")
FACEBOOK_REDIRECT_URI = os.getenv("FACEBOOK_REDIRECT_URI", "http://localhost:8080/facebook_oauth_callback")

# === YouTube API Configuration ===
YOUTUBE_CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID", "")
YOUTUBE_CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET", "")
YOUTUBE_REDIRECT_URI = os.getenv("YOUTUBE_REDIRECT_URI", "http://localhost:8080/youtube_oauth_callback")
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.upload", "https://www.googleapis.com/auth/youtube", "https://www.googleapis.com/auth/userinfo.profile"]

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
# MODIFIED: Added YouTube Upload button to main menus
main_menu_user = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📤 Upload Video (Facebook)"), KeyboardButton("📤 Upload Video (YouTube)")],
        [KeyboardButton("⚙️ Settings")]
    ],
    resize_keyboard=True
)

main_menu_admin = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📤 Upload Video (Facebook)"), KeyboardButton("📤 Upload Video (YouTube)")],
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
        [InlineKeyboardButton("⬅️ Back to General Settings", callback_data='settings_user_menu_inline')]
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
    """Generates inline keyboard for selecting Facebook pages, dynamically including linked pages."""
    user_doc = get_user_data(user_id)
    linked_pages = user_doc.get("facebook_linked_pages", [])

    keyboard = []
    if linked_pages:
        for page in linked_pages:
            page_name = page.get("name", "Unnamed Page")
            callback_data = f'select_fb_page_{page["id"]}'
            if for_upload:
                callback_data += f'_upload' # Differentiate callback for upload flow
            keyboard.append([InlineKeyboardButton(page_name, callback_data=callback_data)])
    else:
        # Added message if no pages are linked
        keyboard.append([InlineKeyboardButton("No Pages Linked Yet", callback_data='no_fb_pages')])

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

def upload_facebook_video(file_path, title, description, page_access_token, page_id, visibility="PUBLISHED", schedule_time=None):
    """Uploads a video to a specific Facebook Page using Graph API and its Page Access Token."""
    if not all([file_path, title, description, page_access_token, page_id]):
        raise ValueError("Missing required parameters for Facebook video upload.")

    post_url = f"https://graph-video.facebook.com/v19.0/{page_id}/videos"

    params = {
        'access_token': page_access_token,
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

async def upload_youtube_video(file_path, title, description, user_id):
    """
    Uploads a video to YouTube using the user's stored credentials.
    Requires google-api-python-client, google-auth-oauthlib, google-auth-httplib2.
    """
    user_doc = get_user_data(user_id)
    creds_data = user_doc.get("youtube_credentials")

    if not creds_data:
        raise ValueError("YouTube credentials not found for this user. Please link your YouTube account.")

    # Convert stored JSON credentials back to Credentials object
    creds = Credentials.from_authorized_user_info(json.loads(creds_data), YOUTUBE_SCOPES)


    # If the token is expired and there's a refresh token, refresh it.
    if not creds.valid and creds.refresh_token:
        try:
            creds.refresh(Request())
            update_user_data(user_id, {"youtube_credentials": creds.to_json()})
            logger.info(f"YouTube credentials refreshed for user {user_id}.")
        except Exception as e:
            logger.error(f"Error refreshing YouTube token for user {user_id}: {e}")
            raise ValueError("Failed to refresh YouTube access token. Please re-link your account.")
    elif not creds.valid and not creds.refresh_token:
        raise ValueError("YouTube access token expired and no refresh token available. Please re-link your account.")
    elif not creds.valid:
        raise ValueError("YouTube access token is invalid. Please re-link your account.")


    try:
        youtube = build('youtube', 'v3', credentials=creds)

        body = {
            'snippet': {
                'title': title,
                'description': description,
                'tags': user_doc.get('youtube_settings', {}).get('tag', '').split(','),
                'categoryId': '22' # Example category: People & Blogs
            },
            'status': {
                'privacyStatus': user_doc.get('youtube_settings', {}).get('privacy', 'public').lower(),
                'selfDeclaredMadeForKids': False, # You might want to make this configurable
            }
        }

        # If it's a Short, mark it as such. YouTube detects shorts based on aspect ratio usually.
        # This metadata helps, but the video dimensions are key.
        if user_doc.get('youtube_settings', {}).get('video_type') == 'Shorts (Short Vertical Video)':
            body['snippet']['description'] = f"{description}\n#shorts" # Add shorts hashtag to description

        media_body = MediaFileUpload(file_path, chunksize=-1, resumable=True)

        logger.info(f"Initiating YouTube upload for user {user_id} with title: {title}")
        request = youtube.videos().insert(
            part='snippet,status',
            body=body,
            media_body=media_body
        )

        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                logger.info(f"Uploaded {int(status.resumable_progress * 100)}% of YouTube video for user {user_id}")

        logger.info(f"YouTube video upload complete for user {user_id}. Video ID: {response.get('id')}")
        return response

    except HttpError as e:
        logger.error(f"An HTTP error occurred during YouTube upload for user {user_id}: {e.resp.status} - {e.content}")
        raise RuntimeError(f"YouTube API Error: {e.content.decode()}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during YouTube upload for user {user_id}: {e}")
        raise RuntimeError(f"YouTube upload failed: {e}")


def convert_video_to_mp4(input_path, output_path):
    """
    Converts video to MP4 format, copying video and audio streams. Ensures output is MP4.
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


# === KEEP ALIVE SERVER (for OAuth Callbacks) ===
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_path.query)

        if parsed_path.path == "/facebook_oauth_callback":
            self.handle_facebook_oauth_callback(query_params)
        elif parsed_path.path == "/youtube_oauth_callback":
            self.handle_youtube_oauth_callback(query_params)
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"<h1>Bot Server is Running!</h1><p>This port is used for OAuth callbacks.</p>")

    def handle_facebook_oauth_callback(self, query_params):
        code = query_params.get('code', [None])[0]
        state = query_params.get('state', [None])[0] # This 'state' should be the user_id

        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        if not code or not state:
            self.wfile.write(b"<h1>Facebook OAuth Failed</h1><p>Missing code or state parameter.</p><p>Please try again from the Telegram bot.</p>")
            logger.error(f"Facebook OAuth callback missing code or state. Query: {query_params}")
            return

        user_id = int(state) # Convert state back to user_id

        try:
            # Exchange code for access token
            token_url = "https://graph.facebook.com/v19.0/oauth/access_token"
            token_params = {
                'client_id': FACEBOOK_APP_ID,
                'client_secret': FACEBOOK_APP_SECRET,
                'redirect_uri': FACEBOOK_REDIRECT_URI,
                'code': code
            }
            response = requests.get(token_url, params=token_params)
            response.raise_for_status()
            token_data = response.json()
            user_access_token = token_data.get('access_token')

            if not user_access_token:
                raise ValueError("No user access token received from Facebook.")

            # Get user's linked pages and their access tokens
            pages_url = f"https://graph.facebook.com/v19.0/me/accounts?access_token={user_access_token}"
            pages_response = requests.get(pages_url)
            pages_response.raise_for_status()
            pages_data = pages_response.json()
            linked_pages = []
            for page in pages_data.get('data', []):
                linked_pages.append({
                    "id": page.get("id"),
                    "name": page.get("name"),
                    "access_token": page.get("access_token"), # This is the page access token
                    "category": page.get("category"),
                    "perms": page.get("perms", []),
                    "linked_at": datetime.now().isoformat()
                })

            # Update user data in MongoDB
            update_user_data(user_id, {
                "facebook_user_access_token": user_access_token, # Store user access token for future use if needed
                "facebook_linked_pages": linked_pages
            })

            # IMPROVED: Feedback if no pages were linked
            feedback_message = ""
            if not linked_pages:
                feedback_message = "<p><b>Important:</b> No Facebook Pages were found linked to your account with the granted permissions. Please ensure you granted 'Manage your Pages' and 'Show a list of the Pages you manage' permissions during the Facebook login process.</p>"
            else:
                feedback_message = f"<p>Successfully linked {len(linked_pages)} Facebook Page(s).</p>"

            self.wfile.write(f"<h1>Facebook Account Linked Successfully!</h1>{feedback_message}<p>You can now close this window and return to the Telegram bot.</p>".encode())
            # Send confirmation message to Telegram bot
            threading.Thread(target=self.send_telegram_confirmation, args=(user_id, "Facebook", True, len(linked_pages))).start()
            logger.info(f"Facebook OAuth successful for user {user_id}. Linked {len(linked_pages)} pages.")

        except requests.exceptions.RequestException as req_e:
            self.wfile.write(b"<h1>Facebook OAuth Failed</h1><p>Network or API error during token exchange/page fetch.</p><p>Please try again from the Telegram bot. Check your Redirect URI setup.</p>")
            threading.Thread(target=self.send_telegram_confirmation, args=(user_id, "Facebook", False, 0, f"Network/API Error: {req_e}")).start()
            logger.error(f"Facebook OAuth Request Error for user {user_id}: {req_e}", exc_info=True)
        except Exception as e:
            self.wfile.write(b"<h1>Facebook OAuth Failed</h1><p>An unexpected error occurred: " + str(e).encode() + b"</p><p>Please try again from the Telegram bot.</p>")
            threading.Thread(target=self.send_telegram_confirmation, args=(user_id, "Facebook", False, 0, f"Unexpected Error: {e}")).start()
            logger.error(f"Facebook OAuth General Error for user {user_id}: {e}", exc_info=True)

    def handle_youtube_oauth_callback(self, query_params):
        code = query_params.get('code', [None])[0]
        state = query_params.get('state', [None])[0] # This 'state' should be the user_id

        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        if not code or not state:
            self.wfile.write(b"<h1>YouTube OAuth Failed</h1><p>Missing code or state parameter.</p><p>Please try again from the Telegram bot.</p>")
            logger.error(f"YouTube OAuth callback missing code or state. Query: {query_params}")
            return

        user_id = int(state) # Convert state back to user_id

        try:
            # The 'token_uri' and 'scopes' are needed for Credentials.from_authorized_user_info
            credentials_data = {
                "client_id": YOUTUBE_CLIENT_ID,
                "client_secret": YOUTUBE_CLIENT_SECRET,
                "redirect_uri": YOUTUBE_REDIRECT_URI,
                "code": code,
                "grant_type": "authorization_code",
                "token_uri": "https://oauth2.googleapis.com/token", # Essential for Credential object
                "scopes": YOUTUBE_SCOPES # Essential for Credential object
            }
            
            token_response = requests.post("https://oauth2.googleapis.com/token", data=credentials_data)
            token_response.raise_for_status()
            token_json = token_response.json()

            # Add missing fields to token_json for Credentials.from_authorized_user_info
            token_json['client_id'] = YOUTUBE_CLIENT_ID
            token_json['client_secret'] = YOUTUBE_CLIENT_SECRET
            token_json['token_uri'] = "https://oauth2.googleapis.com/token"
            token_json['scopes'] = YOUTUBE_SCOPES


            creds = Credentials.from_authorized_user_info(token_json, YOUTUBE_SCOPES)

            # Store the credentials in MongoDB
            update_user_data(user_id, {"youtube_credentials": creds.to_json()})

            self.wfile.write(b"<h1>YouTube Account Linked Successfully!</h1><p>You can now close this window and return to the Telegram bot.</p>")
            # Send confirmation message to Telegram bot
            threading.Thread(target=self.send_telegram_confirmation, args=(user_id, "YouTube", True, 0)).start()
            logger.info(f"YouTube OAuth successful for user {user_id}.")

        except requests.exceptions.RequestException as req_e:
            self.wfile.write(b"<h1>YouTube OAuth Failed</h1><p>Network or API error during token exchange.</p><p>Please try again from the Telegram bot. Check your Redirect URI setup.</p>")
            threading.Thread(target=self.send_telegram_confirmation, args=(user_id, "YouTube", False, 0, f"Network/API Error: {req_e}")).start()
            logger.error(f"YouTube OAuth Request Error for user {user_id}: {req_e}", exc_info=True)
        except Exception as e:
            self.wfile.write(b"<h1>YouTube OAuth Failed</h1><p>An unexpected error occurred: " + str(e).encode() + b"</p><p>Please try again from the Telegram bot.</p>")
            threading.Thread(target=self.send_telegram_confirmation, args=(user_id, "YouTube", False, 0, f"Unexpected Error: {e}")).start()
            logger.error(f"YouTube OAuth General Error for user {user_id}: {e}", exc_info=True)

    # MODIFIED: Added num_linked_pages for Facebook feedback, and made error_message optional
    def send_telegram_confirmation(self, user_id, platform, success, num_linked_pages=0, error_message=""):
        try:
            if success:
                if platform == "Facebook":
                    if num_linked_pages > 0:
                        app.send_message(user_id, f"✅ **Facebook Account Linked!**\n\nYour Facebook account has been successfully connected, and **{num_linked_pages} page(s)** detected. You can now upload videos to your Facebook Pages.")
                    else:
                        app.send_message(user_id, f"✅ **Facebook Account Linked! (No Pages Found)**\n\nYour Facebook account has been connected. However, no pages were detected. Please ensure you granted 'Manage your Pages' and 'Show a list of the Pages you manage' permissions during the Facebook login process if you want to upload to pages.")
                else: # YouTube
                    app.send_message(user_id, f"✅ **{platform} Account Linked!**\n\nYour {platform} account has been successfully connected. You can now use the bot's upload features for {platform}.")
            else:
                app.send_message(user_id, f"❌ **{platform} Account Linking Failed!**\n\nThere was an issue connecting your {platform} account. Please try again. Error: `{error_message}`")
            logger.info(f"Sent {platform} OAuth confirmation to user {user_id}: Success={success}")
        except Exception as e:
            logger.error(f"Failed to send Telegram confirmation for user {user_id}, platform {platform}: {e}", exc_info=True)


def run_server():
    httpd = HTTPServer(('0.0.0.0', 8080), Handler)
    logger.info("Keep-alive HTTP server started on port 8080 for OAuth callbacks.")
    httpd.serve_forever()

threading.Thread(target=run_server, daemon=True).start()


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
        },
        "youtube_credentials": None # NEW: To store YouTube OAuth credentials
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
    """Displays a list of all registered users for admin, or sends as file if too long."""
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

    if len(user_list_text) > 4000: # Telegram message character limit
        temp_file_path = f"user_list_{uuid.uuid4().hex}.txt"
        try:
            with open(temp_file_path, "w", encoding="utf-8") as f:
                f.write(user_list_text)
            await client.send_document(user_id, document=temp_file_path, caption="**👥 Registered System Users List (Too long for direct message):**")
            await callback_query.message.edit_text("✅ **User list sent as a file.**", reply_markup=Admin_markup)
            logger.info(f"Admin {user_id} received user list as file due to length.")
        except Exception as e:
            logger.error(f"Failed to send user list as file to admin {user_id}: {e}", exc_info=True)
            await callback_query.edit_message_text("❌ **Error generating user list file.** Try again later.", reply_markup=Admin_markup)
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
    else:
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
            await client.send_message(target_user_id, "🎉 **System Notification!** You have been upgraded to **PREMIUM** status! Use `/start` to access your enhanced features.")
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
            if target_user_id == user_id: # Don't send broadcast to self
                continue
            await client.send_message(target_user_id, f"📢 **ADMIN BROADCAST MESSAGE:**\n\n{text_to_broadcast}")
            success_count += 1
            time.sleep(0.05) # Small delay to avoid rate limits
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
    """Restarts the bot (exits the current process)."""
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return

    await callback_query.answer("Initiating system reboot sequence...")
    await callback_query.edit_message_text("🔄 **System Rebooting...** Please wait a moment while the bot restarts.", reply_markup=Admin_markup)
    await log_to_channel(client, f"Admin `{user_id}` (`{callback_query.from_user.username}`) initiated a bot restart.")

    # Properly close Pyrogram client before exiting
    await app.stop()
    logger.info("Pyrogram client stopped. Initiating sys.exit(0).")
    sys.exit(0) # Exit the process, relying on external process manager to restart

# --- Settings Navigation Callbacks ---
@app.on_callback_query(filters.regex("^settings_tiktok$"))
async def settings_tiktok_callback(client, callback_query):
    """Displays TikTok settings menu."""
    user_id = callback_query.from_user.id
    user_doc = get_user_data(user_id)
    if not user_doc or not is_premium_user(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing TikTok configurations...")
    await callback_query.edit_message_text(
        "🎵 **TikTok Configuration Interface:**\n\nAdjust your TikTok preferences:",
        reply_markup=tiktok_settings_inline_menu
    )

@app.on_callback_query(filters.regex("^settings_facebook$"))
async def settings_facebook_callback(client, callback_query):
    """Displays Facebook settings menu (now leads to page management)."""
    user_id = callback_query.from_user.id
    user_doc = get_user_data(user_id)
    if not user_doc or not is_premium_user(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing Facebook configurations...")
    await callback_query.edit_message_text(
        "📘 **Facebook Configuration Interface:**\n\nManage your linked pages or set global preferences:",
        reply_markup=facebook_settings_inline_menu_main # Directs to the main Facebook settings with "Manage Linked Pages"
    )

@app.on_callback_query(filters.regex("^fb_manage_pages$"))
async def fb_manage_pages_callback(client, callback_query):
    """Displays the linked Facebook pages and login/refresh option."""
    user_id = callback_query.from_user.id
    user_doc = get_user_data(user_id)
    if not user_doc or not is_premium_user(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Retrieving Facebook pages...")
    
    # Get the dynamic markup for page selection/linking
    markup = get_facebook_page_selection_markup(user_id, for_upload=False)
    
    # MODIFIED: Improved message when no pages are linked
    user_pages = user_doc.get("facebook_linked_pages", [])
    if not user_pages:
        message_text = (
            "🗂️ **Your Linked Facebook Pages:**\n\n"
            "Currently, no Facebook Pages are linked to your account. To upload to Facebook, "
            "you need to link your account and grant permissions to your pages.\n\n"
            "➡️ Click '🔑 Link/Refresh Facebook Pages' to start. Ensure you accept all permissions, especially for managing pages."
        )
    else:
        message_text = (
            "🗂️ **Your Linked Facebook Pages:**\n\n"
            "Select a page to view/edit its specific settings, or link a new one:"
        )

    await callback_query.edit_message_text(
        message_text,
        reply_markup=markup
    )

@app.on_callback_query(filters.regex("^settings_youtube$"))
async def settings_youtube_callback(client, callback_query):
    """Displays YouTube settings menu."""
    user_id = callback_query.from_user.id
    user_doc = get_user_data(user_id)
    if not user_doc or not is_premium_user(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer("Accessing YouTube configurations...")
    await callback_query.edit_message_text(
        "▶️ **YouTube Configuration Interface:**\n\nAdjust your YouTube preferences:",
        reply_markup=youtube_settings_inline_menu
    )

# --- Platform-specific Login Callbacks ---
@app.on_callback_query(filters.regex("^fb_login_prompt$"))
async def fb_login_prompt_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return

    # Generate Facebook OAuth URL
    oauth_url = (
        f"https://www.facebook.com/v19.0/dialog/oauth?"
        f"client_id={FACEBOOK_APP_ID}&"
        f"redirect_uri={urllib.parse.quote(FACEBOOK_REDIRECT_URI)}&"
        f"scope=public_profile,pages_show_list,pages_read_engagement,pages_manage_posts,pages_manage_metadata,business_management,read_insights&" # Request necessary permissions
        f"response_type=code&"
        f"state={user_id}" # Pass user ID as state
    )
    
    inline_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Link Facebook Account", url=oauth_url)],
        [InlineKeyboardButton("⬅️ Back to FB Settings", callback_data='settings_facebook')]
    ])
    
    await callback_query.edit_message_text(
        "Click the button below to link your Facebook account and grant the necessary permissions. "
        "This will allow the bot to manage your pages and upload videos on your behalf.\n\n"
        "**Important:** During the Facebook login process, you **must** grant permissions for 'Manage your Pages' and 'Show a list of the Pages you manage' if you want to use the page upload features.",
        reply_markup=inline_keyboard,
        disable_web_page_preview=True
    )
    await callback_query.answer("Redirecting to Facebook for login...")
    logger.info(f"User {user_id} initiated Facebook OAuth.")

@app.on_callback_query(filters.regex("^yt_login_prompt$"))
async def yt_login_prompt_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return

    # Generate Google OAuth URL for YouTube
    oauth_url = (
        f"https://accounts.google.com/o/oauth2/auth?"
        f"client_id={YOUTUBE_CLIENT_ID}&"
        f"redirect_uri={urllib.parse.quote(YOUTUBE_REDIRECT_URI)}&"
        f"scope={urllib.parse.quote(' '.join(YOUTUBE_SCOPES))}&"
        f"response_type=code&"
        f"access_type=offline&" # Request a refresh token
        f"prompt=consent&" # Ensure consent screen is shown for refresh token
        f"state={user_id}" # Pass user ID as state
    )

    inline_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Link YouTube Account", url=oauth_url)],
        [InlineKeyboardButton("⬅️ Back to YT Settings", callback_data='settings_youtube')]
    ])

    await callback_query.edit_message_text(
        "Click the button below to link your YouTube account and grant the necessary permissions. "
        "This will allow the bot to upload videos on your behalf.",
        reply_markup=inline_keyboard,
        disable_web_page_preview=True
    )
    await callback_query.answer("Redirecting to Google for YouTube login...")
    logger.info(f"User {user_id} initiated YouTube OAuth.")

# --- Upload Video (Facebook) Main Entry ---
@app.on_message(filters.text & filters.regex("^📤 Upload Video (Facebook)$") & filters.create(lambda _, __, m: is_premium_user(m.from_user.id)))
async def upload_video_facebook_prompt(client, message):
    user_id = message.from_user.id
    user_doc = get_user_data(user_id)
    linked_pages = user_doc.get("facebook_linked_pages", [])

    if not linked_pages:
        # IMPROVED: Clearer message when no pages are linked
        await message.reply(
            "❌ **No Facebook Pages Linked!**\n\n"
            "To upload to Facebook, you first need to link your Facebook account and grant access to your pages.\n"
            "Go to `⚙️ Settings` -> `📘 Facebook Settings` -> `🗂️ Manage Linked Pages` and use '🔑 Link/Refresh Facebook Pages'.\n\n"
            "**Important:** During the Facebook login process, make sure to grant 'Manage your Pages' and 'Show a list of the Pages you manage' permissions.",
            reply_markup=main_menu_user
        )
        return

    # Set user state to await page selection for upload
    user_states[user_id] = {"step": AWAITING_FB_PAGE_SELECTION_UPLOAD, "platform": "facebook"}
    
    # Get the dynamic markup for page selection for upload
    markup = get_facebook_page_selection_markup(user_id, for_upload=True)

    await message.reply(
        "📤 **Facebook Video Upload Initiated!**\n\n"
        "Please select the Facebook Page you wish to upload the video to:",
        reply_markup=markup
    )
    logger.info(f"User {user_id} started Facebook video upload flow, awaiting page selection.")

# NEW: Upload Video (YouTube) Main Entry
@app.on_message(filters.text & filters.regex("^📤 Upload Video (YouTube)$") & filters.create(lambda _, __, m: is_premium_user(m.from_user.id)))
async def upload_video_youtube_prompt(client, message):
    user_id = message.from_user.id
    user_doc = get_user_data(user_id)
    youtube_creds = user_doc.get("youtube_credentials")

    if not youtube_creds:
        await message.reply(
            "❌ **YouTube Account Not Linked!**\n\n"
            "To upload to YouTube, you first need to link your YouTube account.\n"
            "Go to `⚙️ Settings` -> `▶️ YouTube Settings` -> `🔑 YouTube Login`.",
            reply_markup=main_menu_user
        )
        return

    # Set user state to await video
    user_states[user_id] = {"step": AWAITING_UPLOAD_VIDEO, "platform": "youtube"}
    
    await message.reply(
        "📤 **YouTube Video Upload Initiated!**\n\n"
        "Please send the video file you wish to upload to YouTube. "
        "Supported formats will be converted to MP4 automatically.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Cancel Upload", callback_data="cancel_upload")]])
    )
    logger.info(f"User {user_id} started YouTube video upload flow, awaiting video.")


@app.on_callback_query(filters.regex("^select_fb_page_(.*)_upload$") & filters.create(lambda _, __, m: user_states.get(m.message.chat.id, {}).get("step") == AWAITING_FB_PAGE_SELECTION_UPLOAD))
async def select_fb_page_for_upload(client, callback_query):
    user_id = callback_query.from_user.id
    page_id = callback_query.matches[0].group(1) # Extract page_id from regex match

    user_doc = get_user_data(user_id)
    linked_pages = user_doc.get("facebook_linked_pages", [])
    
    selected_page = next((p for p in linked_pages if p["id"] == page_id), None)

    if not selected_page or not selected_page.get("access_token"):
        await callback_query.answer("❌ Selected page not found or its access token is invalid. Please re-link your Facebook account.", show_alert=True)
        logger.error(f"User {user_id} selected non-existent Facebook page {page_id} or missing token for upload.")
        user_states.pop(user_id, None)
        await callback_query.edit_message_text(
            "❌ **Error!** Selected Facebook Page not found or its access token is invalid. Please start the upload process again and ensure your pages are properly linked.",
            reply_markup=main_menu_user
        )
        return
    
    page_name = selected_page.get("name", "Unnamed Page")
    user_states[user_id]["selected_facebook_page_id"] = page_id
    user_states[user_id]["selected_facebook_page_access_token"] = selected_page["access_token"]
    user_states[user_id]["step"] = AWAITING_UPLOAD_VIDEO # Next step is to await video
    user_states[user_id]["platform"] = "facebook" # Ensure platform is set

    await callback_query.answer(f"Selected page: {page_name}", show_alert=False)
    await callback_query.edit_message_text(
        f"✅ **Page Selected:** `{page_name}`\n\n"
        "Please send the video file you wish to upload to this Facebook Page. "
        "Supported formats will be converted to MP4 automatically.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Cancel Upload", callback_data="cancel_upload")]])
    )
    logger.info(f"User {user_id} selected Facebook page {page_id} ({page_name}) for upload. Awaiting video.")


# --- Unified Video Upload Handler ---
@app.on_message(filters.video & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step") == AWAITING_UPLOAD_VIDEO))
async def handle_video_upload(client, message):
    user_id = message.from_user.id
    current_state = user_states.get(user_id, {})
    platform = current_state.get("platform")

    if not platform:
        await message.reply("❌ **Error!** No platform selected for upload. Please start the upload process again.", reply_markup=main_menu_user)
        user_states.pop(user_id, None)
        return

    await client.send_chat_action(user_id, enums.ChatAction.UPLOAD_VIDEO)
    status_message = await message.reply("⏳ **Initiating video processing...** Please wait, this might take a moment.")
    logger.info(f"User {user_id} sent video for {platform} upload.")

    file_path = None
    processed_file_path = None

    try:
        # Download the video
        await status_message.edit_text("⬇️ **Downloading video...**")
        file_path = await message.download(file_name=f"downloads/{user_id}_{uuid.uuid4().hex}")
        logger.info(f"Downloaded video to: {file_path}")

        # Convert to MP4 if necessary
        if not file_path.lower().endswith(".mp4"):
            await status_message.edit_text("🔄 **Converting video to MP4...**")
            processed_file_path = f"{file_path}.mp4"
            processed_file_path = convert_video_to_mp4(file_path, processed_file_path)
            logger.info(f"Video converted to MP4: {processed_file_path}")
        else:
            processed_file_path = file_path

        user_doc = get_user_data(user_id)
        settings = user_doc.get(f"{platform}_settings", {})
        title = settings.get("title", "Default Title")
        description = settings.get("description", "Default Description")
        visibility = settings.get("privacy", "public")
        schedule_time = settings.get("schedule_time") # This needs parsing if it's a string

        # Placeholder for dynamic title/description input during upload (future enhancement)
        # For now, it uses defaults from settings

        if platform == "facebook":
            page_id = current_state.get("selected_facebook_page_id")
            page_access_token = current_state.get("selected_facebook_page_access_token")

            if not page_id or not page_access_token:
                await status_message.edit_text("❌ **Upload Failed!** No Facebook Page selected or invalid token. Please start the upload process again.", reply_markup=main_menu_user)
                return

            await status_message.edit_text("⬆️ **Uploading video to Facebook...**")
            result = upload_facebook_video(
                processed_file_path,
                title,
                description,
                page_access_token,
                page_id,
                visibility=visibility,
                schedule_time=schedule_time
            )
            video_id = result.get('id')
            post_id = result.get('post_id')
            await status_message.edit_text(f"✅ **Video Uploaded to Facebook!**\n\n"
                                           f"Video ID: `{video_id}`\n"
                                           f"Post ID: `{post_id}`\n"
                                           f"Check your Facebook Page for the uploaded video.",
                                           reply_markup=main_menu_user)
            await log_to_channel(client, f"Video uploaded to Facebook by `{user_id}`. Video ID: `{video_id}`.")

        elif platform == "youtube":
            await status_message.edit_text("⬆️ **Uploading video to YouTube...**")
            result = await upload_youtube_video(
                processed_file_path,
                title,
                description,
                user_id
            )
            video_id = result.get('id')
            # CORRECTED: YouTube video URL format
            await status_message.edit_text(f"✅ **Video Uploaded to YouTube!**\n\n"
                                           f"Video ID: `{video_id}`\n"
                                           f"Link: `https://youtu.be/{video_id}`\n"
                                           f"Check your YouTube channel for the uploaded video.",
                                           reply_markup=main_menu_user)
            await log_to_channel(client, f"Video uploaded to YouTube by `{user_id}`. Video ID: `{video_id}`.")

        await client.send_chat_action(user_id, enums.ChatAction.CANCEL) # End chat action
        logger.info(f"Video upload completed for user {user_id} to {platform}.")

    except ValueError as ve:
        await status_message.edit_text(f"❌ **Upload Failed!** Configuration error: `{ve}`", reply_markup=main_menu_user)
        logger.error(f"Upload failed for user {user_id} due to value error: {ve}", exc_info=True)
    except RuntimeError as re:
        await status_message.edit_text(f"❌ **Upload Failed!** Processing error: `{re}`", reply_markup=main_menu_user)
        logger.error(f"Upload failed for user {user_id} due to runtime error: {re}", exc_info=True)
    except requests.exceptions.RequestException as req_e:
        await status_message.edit_text(f"❌ **Upload Failed!** Network or API error. Ensure internet connection or inspect the configured API parameters. `{req_e}`", reply_markup=main_menu_user)
        logger.error(f"Network/API Error for user {user_id}: {req_e}", exc_info=True)
    except Exception as e:
        await status_message.edit_text(f"❌ **Critical Transmission Failure.** An unexpected system error occurred: `{e}`", reply_markup=main_menu_user)
        logger.error(f"Upload failed for user {user_id}: {e}", exc_info=True)
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up original file: {file_path}")
        if processed_file_path and processed_file_path != file_path and os.path.exists(processed_file_path):
            os.remove(processed_file_path)
            logger.info(f"Cleaned up processed file: {processed_file_path}")
        user_states.pop(user_id, None) # Clear user state after upload attempt
        await client.send_chat_action(user_id, enums.ChatAction.CANCEL) # Ensure action is cancelled


@app.on_callback_query(filters.regex("^cancel_upload$"))
async def cancel_upload_callback(client, callback_query):
    """Cancels an ongoing upload process."""
    user_id = callback_query.from_user.id
    if user_states.get(user_id, {}).get("step") == AWAITING_UPLOAD_VIDEO:
        user_states.pop(user_id, None)
        await callback_query.answer("Upload sequence terminated.")
        await callback_query.edit_message_text("🛑 **Video Upload Cancelled.**", reply_markup=main_menu_user)
        await client.send_chat_action(user_id, enums.ChatAction.CANCEL)
        logger.info(f"User {user_id} cancelled video upload.")
    else:
        await callback_query.answer("No active upload process to terminate.", show_alert=True)


# --- Remaining (Placeholder) Settings Handlers ---
# These are placeholders; full implementation for setting values (title, description, etc.)
# would follow a similar pattern to admin_add_user_id_input, using user_states for text input.

@app.on_callback_query(filters.regex("^(fb|yt|tiktok)_set_title$"))
async def set_title_prompt(client, callback_query):
    platform = callback_query.matches[0].group(1)
    user_id = callback_query.from_user.id
    if not is_premium_user(user_id):
        await callback_query.answer("⚠️ **Access Restricted.** This feature requires premium access.", show_alert=True)
        return
    await callback_query.answer(f"Setting {platform} title...")
    user_states[user_id] = {"step": f"awaiting_{platform}_title", "platform": platform}
    await callback_query.edit_message_text(f"Please send the new default title for your {platform.upper()} uploads:",
                                           reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f'settings_{platform}')]]))

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step", "").endswith("_title")))
async def handle_title_input(client, message):
    user_id = message.from_user.id
    current_state = user_states.pop(user_id, {})
    platform = current_state.get("platform")
    if not platform:
        return # Should not happen if state is correctly managed
    
    new_title = message.text.strip()
    update_user_data(user_id, {f"{platform}_settings.title": new_title})
    await message.reply(f"✅ **{platform.upper()} Title Updated!**\n\nNew default title: `{new_title}`",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Settings", callback_data=f'settings_{platform}')]]))
    logger.info(f"User {user_id} updated {platform} title to: {new_title}")


# --- Similarly implement handlers for: ---
# - `(fb|yt|tiktok)_set_tag`
# - `(fb|yt|tiktok)_set_description`
# - `(fb|yt|tiktok)_set_schedule_time` (requires datetime parsing)
# - `(fb|yt|tiktok)_set_privacy` (using `get_privacy_inline_menu` and handlers for public/private/unlisted)
# - `(fb|yt|tiktok)_video_type` (using `_video_type_inline_menu` and handlers)
# - `(fb|yt|tiktok)_check_account_info` / `_check_expiry_date` (fetch and display info from stored tokens)


@app.on_callback_query(filters.regex("^(fb|yt|tiktok)_video_type_reels|video|shorts$"))
async def handle_video_type_selection(client, callback_query):
    user_id = callback_query.from_user.id
    parts = callback_query.data.split('_')
    platform = parts[0]
    # Correctly parse video_type from callback data
    if platform == "fb":
        video_type_raw = parts[3] # e.g., 'fb_video_type_reels' -> 'reels'
    elif platform == "yt" or platform == "tiktok":
        video_type_raw = parts[2] # e.g., 'yt_video_type_shorts' -> 'shorts'
    else:
        await callback_query.answer("Invalid video type selection.", show_alert=True)
        return

    video_type_map = {
        "reels": "Reels (Short Vertical Video)",
        "video": "Video (Standard Horizontal/Square)",
        "shorts": "Shorts (Short Vertical Video)",
        "1_1": "1:1 Aspect Ratio (Square)", # For TikTok
        "9_16": "9:16 Aspect Ratio (Vertical)" # For TikTok
    }
    selected_video_type = video_type_map.get(video_type_raw, "Video (Standard Horizontal/Square)")

    update_user_data(user_id, {f"{platform}_settings.video_type": selected_video_type})
    await callback_query.answer(f"{platform.upper()} Video Type set to {selected_video_type}", show_alert=False)
    
    # Use the correct inline menu for each platform
    if platform == "fb":
        reply_markup = facebook_video_type_inline_menu
    elif platform == "yt":
        reply_markup = youtube_video_type_inline_menu
    elif platform == "tiktok":
        reply_markup = tiktok_video_type_inline_menu
    else:
        reply_markup = None # Fallback if platform is unexpected


    await callback_query.edit_message_text(f"✅ **{platform.upper()} Video Type Updated!**\n\nSet to: `{selected_video_type}`",
                                           reply_markup=reply_markup)
    logger.info(f"User {user_id} updated {platform} video type to: {selected_video_type}")


@app.on_callback_query(filters.regex("^(fb|yt|tiktok)_privacy_(public|private|unlisted)$"))
async def handle_privacy_selection(client, callback_query):
    user_id = callback_query.from_user.id
    parts = callback_query.data.split('_')
    platform = parts[0]
    privacy_status = parts[2] # public, private, unlisted

    update_user_data(user_id, {f"{platform}_settings.privacy": privacy_status.capitalize()})
    await callback_query.answer(f"{platform.upper()} Privacy set to {privacy_status}", show_alert=False)
    await callback_query.edit_message_text(f"✅ **{platform.upper()} Privacy Updated!**\n\nSet to: `{privacy_status.capitalize()}`",
                                           reply_markup=get_privacy_inline_menu(platform))
    logger.info(f"User {user_id} updated {platform} privacy to: {privacy_status}")


# --- Admin Upload Facebook (direct upload for admin, if needed) ---
@app.on_callback_query(filters.regex("^admin_upload_fb$"))
async def admin_upload_fb_prompt(client, callback_query):
    user_id = callback_query.from_user.id
    if not is_admin(user_id):
        await callback_query.answer("🚫 **Unauthorized Access.**", show_alert=True)
        return
    
    user_doc = get_user_data(user_id)
    linked_pages = user_doc.get("facebook_linked_pages", [])

    if not linked_pages:
        await callback_query.answer("❌ Admin has no Facebook Pages linked! Please link pages via settings.", show_alert=True)
        await callback_query.edit_message_text(
            "❌ **No Facebook Pages Linked for Admin!**\n\n"
            "To use admin upload, link your Facebook account and pages via `⚙️ Settings` -> `📘 Facebook Settings` -> `🗂️ Manage Linked Pages`.",
            reply_markup=Admin_markup
        )
        return

    user_states[user_id] = {"step": AWAITING_FB_PAGE_SELECTION_UPLOAD, "platform": "facebook", "admin_upload": True}
    markup = get_facebook_page_selection_markup(user_id, for_upload=True) # Use the same markup
    await callback_query.edit_message_text(
        "📤 **Admin Facebook Video Upload Initiated!**\n\n"
        "Please select the Facebook Page to upload the video to:",
        reply_markup=markup
    )
    logger.info(f"Admin {user_id} started Facebook direct upload flow, awaiting page selection.")


# === START THE BOT ===
if __name__ == "__main__":
    logger.info("Bot starting...")
    app.run()
    logger.info("Bot stopped.")
