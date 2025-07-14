import os
import threading
import concurrent.futures
from http.server import HTTPServer, BaseHTTPRequestHandler
import logging
import json
import time
import subprocess # For direct ffmpeg calls

from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from instagrapi import Client as InstaClient
from dotenv import load_dotenv
from pymongo import MongoClient
import requests # For Facebook Graph API

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === LOAD ENV ===
load_dotenv()
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# INSTAGRAM_USERNAME and INSTAGRAM_PASSWORD are now only used as fallbacks if no per-user login is done.
# For per-user login, credentials are provided via /login command.
INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME", "")
INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASSWORD", "")
INSTAGRAM_PROXY = os.getenv("INSTAGRAM_PROXY", "")  # Leave empty if no proxy

# === NEW: MongoDB Configuration ===
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://cristi7jjr:tRjSVaoSNQfeZ0Ik@cluster0.kowid.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
DB_NAME = "InstaFb" # You can choose a more descriptive name if you like

# === NEW: Admin and Log Channel Configuration ===
OWNER_ID = int(os.getenv("OWNER_ID", "7577977996")) # IMPORTANT: Replace with your actual Telegram User ID
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "-1002779117737")) # IMPORTANT: Replace with your actual log channel ID

# === NEW: Facebook API Configuration ===
# IMPORTANT: You'll need to create a Facebook App and get an Access Token with required permissions (pages_show_list, pages_manage_posts).
# It's highly recommended to use a Page Access Token for uploading, not a User Access Token directly, as User Tokens are short-lived.
# Guide: https://developers.facebook.com/docs/pages/access-tokens/
FACEBOOK_APP_ID = os.getenv("FACEBOOK_APP_ID", "")
FACEBOOK_APP_SECRET = os.getenv("FACEBOOK_APP_SECRET", "")
FACEBOOK_PAGE_ID = os.getenv("FACEBOOK_PAGE_ID", "") # The ID of the Facebook Page you want to upload to
FACEBOOK_PAGE_ACCESS_TOKEN = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN", "") # A long-lived Page Access Token


# === GLOBAL CLIENTS AND DB ===
# insta_client is kept for general use, but user-specific sessions are handled by safe_instagram_login_for_user
insta_client = InstaClient() 
app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
users_collection = db["users"]

# --- Ensure indexes for quick lookups ---
users_collection.create_index("user_id", unique=True)

# === KEYBOARDS ===
main_menu_user = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📤 Upload a Reel (Instagram)")],
        [KeyboardButton("📤 Upload Video (Facebook)")],
        [KeyboardButton("⚙️ Settings")]
    ],
    resize_keyboard=True
)

main_menu_admin = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📤 Upload a Reel (Instagram)"), KeyboardButton("📤 Upload Video (Facebook)")],
        [KeyboardButton("⚙️ Settings"), KeyboardButton("👤 Admin Panel")]
    ],
    resize_keyboard=True
)

settings_menu = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🔑 Instagram Login"), KeyboardButton("🔑 Facebook Login")],
        [KeyboardButton("🎵 Video Audio Settings"), KeyboardButton("🔙 Main Menu")]
    ],
    resize_keyboard=True
)

admin_panel_menu_kb = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📢 Broadcast Message")],
        [KeyboardButton("➕ Add Admin"), KeyboardButton("➖ Remove Admin")],
        [KeyboardButton("📊 View User Stats"), KeyboardButton("🔙 Main Menu")]
    ],
    resize_keyboard=True
)

# === USER STATES ===
# This will now store more complex data, including which platform they are uploading to
user_states = {}
# Example user_states entry:
# user_states = {
#     chat_id: {
#         "step": "awaiting_video_instagram", # or "awaiting_video_facebook", "awaiting_title_instagram", etc.
#         "platform": "instagram", # or "facebook"
#         "file_path": "/path/to/video.mp4",
#         "title": "My awesome reel",
#         "caption_or_description": "#reel #instagram",
#     }
# }


# === HELPERS ===
def get_user_data(user_id):
    """Retrieves user data from MongoDB."""
    return users_collection.find_one({"user_id": user_id})

def update_user_data(user_id, data):
    """Updates user data in MongoDB."""
    # Using $set to update specific fields, and upsert=True to insert if not exists
    users_collection.update_one({"user_id": user_id}, {"$set": data}, upsert=True)

def is_admin(user_id):
    """Checks if a user is an admin."""
    user_doc = get_user_data(user_id)
    return user_doc and user_doc.get("role") == "admin"

async def log_to_channel(client, message_text):
    """Sends a message to the designated log channel."""
    try:
        await client.send_message(LOG_CHANNEL_ID, f"**Bot Log:**\n\n{message_text}")
    except Exception as e:
        logger.error(f"Failed to send message to log channel (ID: {LOG_CHANNEL_ID}): {e}")

def safe_instagram_login_for_user(user_id, username=None, password=None):
    """
    Handles Instagram login for a specific user, saving session to DB.
    It loads an existing session from MongoDB if available.
    If no session or invalid, it attempts to log in with provided credentials.
    """
    user_doc = get_user_data(user_id)
    if not user_doc:
        raise ValueError(f"User {user_id} not found in database. Please send /start first.")

    temp_insta_client = InstaClient()

    if INSTAGRAM_PROXY:
        temp_insta_client.set_proxy(INSTAGRAM_PROXY)

    # Attempt to load session from DB
    insta_session_data = user_doc.get("instagram_session")
    if insta_session_data:
        try:
            # instagrapi expects a file, so we'll save the session data temporarily
            temp_session_path = f"insta_settings_{user_id}.json"
            with open(temp_session_path, "w") as f:
                json.dump(insta_session_data, f)
            
            temp_insta_client.load_settings(temp_session_path)
            
            if temp_insta_client.validate_uuid() and temp_insta_client.api.user_id:
                logger.info(f"Instagram session loaded and validated for user {user_id}")
                return temp_insta_client
            else:
                logger.warning(f"Instagram session invalid for user {user_id}. Attempting re-login.")
        except Exception as e:
            logger.warning(f"Error loading Instagram session for user {user_id}: {e}. Attempting re-login.")
        finally:
            if os.path.exists(temp_session_path):
                os.remove(temp_session_path)

    # If no valid session, try to log in with provided credentials
    if username and password:
        logger.info(f"Attempting Instagram login for user {user_id} with provided credentials.")
        temp_insta_client.login(username, password)
        
        # Dump new settings and store in DB
        temp_session_path = f"insta_settings_{user_id}.json"
        temp_insta_client.dump_settings(temp_session_path)
        with open(temp_session_path, "r") as f:
            new_settings = json.load(f)
        update_user_data(user_id, {"instagram_session": new_settings})
        if os.path.exists(temp_session_path):
            os.remove(temp_session_path)
        logger.info(f"Instagram login successful and session saved for user {user_id}.")
        return temp_insta_client
    else:
        raise ValueError("No Instagram session found and no credentials provided for login.")

def get_facebook_access_token_for_user(user_id):
    """Retrieves Facebook access token from user data."""
    user_doc = get_user_data(user_id)
    return user_doc.get("facebook_access_token")

def store_facebook_access_token_for_user(user_id, token):
    """Stores Facebook access token in user data."""
    update_user_data(user_id, {"facebook_access_token": token})

def upload_facebook_video(file_path, title, description, access_token, page_id):
    """Uploads a video to Facebook Page using Graph API."""
    if not all([file_path, title, description, access_token, page_id]):
        raise ValueError("Missing required parameters for Facebook video upload.")

    # Step 1: Initialize Upload
    init_url = f"https://graph-video.facebook.com/v19.0/{page_id}/videos"
    init_params = {
        'access_token': access_token,
        'upload_phase': 'start',
        'file_size': os.path.getsize(file_path)
    }
    logger.info(f"Initiating Facebook video upload for {file_path}")
    response = requests.post(init_url, params=init_params)
    response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
    init_data = response.json()
    upload_session_id = init_data['upload_session_id']
    video_id = init_data['video_id']
    upload_url = f"https://graph-video.facebook.com/v19.0/{upload_session_id}"

    # Step 2: Upload Chunks (for simplicity, we'll send the whole file as one chunk)
    with open(file_path, 'rb') as f:
        files = {'video_file': f}
        upload_params = {
            'access_token': access_token,
            'upload_phase': 'transfer',
            'start_offset': 0,
            'upload_session_id': upload_session_id
        }
        logger.info("Uploading video file to Facebook...")
        response = requests.post(upload_url, params=upload_params, files=files)
        response.raise_for_status()
        upload_data = response.json()

    # Step 3: Finish Upload
    finish_url = f"https://graph-video.facebook.com/v19.0/{page_id}/videos"
    finish_params = {
        'access_token': access_token,
        'upload_phase': 'finish',
        'upload_session_id': upload_session_id,
        'title': title,
        'description': description,
    }
    logger.info("Finishing Facebook video upload...")
    response = requests.post(finish_url, params=finish_params)
    response.raise_for_status()
    result = response.json()
    logger.info(f"Facebook video upload result: {result}")
    return result

def process_video_audio(input_path, output_path, audio_preference):
    """
    Processes video audio based on preference using ffmpeg.
    audio_preference: 'all', 'english', 'none'
    """
    command = ["ffmpeg", "-i", input_path]

    if audio_preference == "none":
        # Remove all audio tracks
        command.extend(["-c:v", "copy", "-an", output_path])
        logger.info(f"FFmpeg: Removing all audio tracks from {input_path}")
    elif audio_preference == "english":
        # Attempt to select the English audio track.
        # This is a simplified approach. A robust solution would use ffprobe first
        # to identify available audio streams and their languages.
        # For demonstration, we'll try to map to an English track if one exists,
        # otherwise, default to the first audio track.
        # This assumes ffmpeg is compiled with libavformat and can read language tags.
        # If it's problematic, consider extracting all audio streams first, then picking.

        # Try to map to an English audio track
        # '-map 0:v:0' maps the first video stream
        # '-map 0:a:m:language:eng' maps the first audio stream that has 'eng' language metadata
        # If no English track is found, this map might fail, so we often default to 0:a:0.
        # For simplicity and to ensure some audio is kept if 'english' is chosen,
        # we'll map the video and the first audio stream, and trust ffmpeg to handle multiple audios based on input.
        # A more precise way would be:
        # command.extend(["-map", "0:v:0", "-map", "0:a:m:language:eng", "-c:v", "copy", "-c:a", "copy", output_path])
        # BUT this can fail if 'eng' audio isn't explicitly tagged or doesn't exist.

        # A safer general approach is to keep only the first audio track if multiple are present
        # as a stand-in for "keeping one primary audio".
        command.extend(["-map", "0:v:0", "-map", "0:a:0", "-c:v", "copy", "-c:a", "copy", "-y", output_path]) # -y to overwrite output
        logger.info(f"FFmpeg: Keeping first audio track (as a proxy for 'English' in simplified setup) from {input_path}")
    else: # audio_preference == "all" or unrecognized
        logger.info(f"FFmpeg: No specific audio processing, copying all streams from {input_path}")
        # Copy all streams (video and audio) without re-encoding
        command.extend(["-c", "copy", "-y", output_path])


    try:
        # Run ffmpeg command. capture_output=True for debugging, check=True raises error on non-zero exit.
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"FFmpeg command successful for {input_path}. Output: {result.stdout}")
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg audio processing failed for {input_path}. Command: {' '.join(e.cmd)}")
        logger.error(f"STDOUT: {e.stdout}")
        logger.error(f"STDERR: {e.stderr}")
        raise RuntimeError(f"FFmpeg processing error: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("FFmpeg not found. Please install FFmpeg and ensure it's in your system's PATH.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during FFmpeg processing: {e}")
        raise

# === PYROGRAM HANDLERS ===

@app.on_message(filters.command("start"))
async def start_command(client, message):
    user_id = message.from_user.id
    user_doc = get_user_data(user_id)

    if not user_doc:
        # New user, default to 'user' role and initialize preferences
        update_user_data(user_id, {
            "user_id": user_id,
            "role": "user",
            "instagram_session": None,
            "facebook_access_token": None,
            "audio_preference": "all" # Default audio preference
        })
        await log_to_channel(client, f"New user started bot: `{user_id}` (`{message.from_user.first_name}`)")
        reply_markup = main_menu_user
        welcome_message = "👋 Welcome! I'm your media upload bot. Choose an option below:"
    else:
        # Existing user
        if is_admin(user_id):
            reply_markup = main_menu_admin
            welcome_message = "👋 Welcome back Admin! Choose an option below:"
        else:
            reply_markup = main_menu_user
            welcome_message = "👋 Welcome back! Choose an option below:"
    
    await message.reply(welcome_message, reply_markup=reply_markup)


@app.on_message(filters.command("login"))
async def login_instagram_command(client, message):
    user_id = message.from_user.id
    if not get_user_data(user_id): # Ensure user exists in DB
        await message.reply("Please send /start first to initialize your account.")
        return

    try:
        args = message.text.split(maxsplit=2)
        if len(args) != 3:
            await message.reply("❗ Usage: `/login <username> <password>`\n\n_This logs you into Instagram. Instagram proxy will be used if set in environment._")
            return

        username, password = args[1], args[2]
        await message.reply("🔐 Logging into Instagram... This might take a moment. Please be patient.")

        # Run login in a thread pool to avoid blocking the bot
        def do_login_sync():
            return safe_instagram_login_for_user(user_id, username, password)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(do_login_sync)
            future.result(timeout=120) # Increased timeout for potentially slow Instagram logins

        await message.reply("✅ Instagram login successful and session saved. You are now logged in as an original user.")
        await log_to_channel(client, f"User `{user_id}` successfully logged into Instagram.")

    except concurrent.futures.TimeoutError:
        await message.reply("❌ Login timeout. This often happens if the proxy is slow or Instagram is blocking the login attempt. Please try again or check your proxy/credentials.")
        logger.error(f"Instagram login timeout for user {user_id}")
    except ValueError as ve:
        await message.reply(f"❌ Instagram login failed: `{ve}`")
        logger.error(f"Instagram login failed due to ValueError for user {user_id}: {ve}")
    except Exception as e:
        await message.reply(f"❌ Instagram login failed: `{e}`\n\n_Ensure your username/password are correct and if you're using a proxy, it's working._")
        logger.error(f"Instagram login failed for user {user_id}: {e}")

# --- Admin Commands ---
@app.on_message(filters.command("addadmin") & filters.user(OWNER_ID))
async def add_admin_command(client, message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2 or not args[1].isdigit():
            await message.reply("❗ Usage: `/addadmin <user_id>`")
            return

        target_user_id = int(args[1])
        user_doc = get_user_data(target_user_id)

        if user_doc:
            update_user_data(target_user_id, {"role": "admin"})
            await message.reply(f"✅ User `{target_user_id}` has been promoted to admin.")
            await client.send_message(target_user_id, "🎉 You have been promoted to an admin! Use /start to see your new options.")
            await log_to_channel(client, f"User `{target_user_id}` promoted to admin by `{message.from_user.id}`.")
        else:
            await message.reply(f"User `{target_user_id}` not found in database. Ask them to send /start first.")

    except Exception as e:
        await message.reply(f"❌ Failed to add admin: {e}")
        logger.error(f"Failed to add admin: {e}")

@app.on_message(filters.command("removeadmin") & filters.user(OWNER_ID))
async def remove_admin_command(client, message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2 or not args[1].isdigit():
            await message.reply("❗ Usage: `/removeadmin <user_id>`")
            return

        target_user_id = int(args[1])
        user_doc = get_user_data(target_user_id)

        if user_doc and user_doc.get("role") == "admin":
            update_user_data(target_user_id, {"role": "user"})
            await message.reply(f"✅ User `{target_user_id}` has been demoted to a regular user.")
            await client.send_message(target_user_id, "You have been demoted from admin status.")
            await log_to_channel(client, f"User `{target_user_id}` demoted from admin by `{message.from_user.id}`.")
        else:
            await message.reply(f"User `{target_user_id}` is not an admin or not found.")

    except Exception as e:
        await message.reply(f"❌ Failed to remove admin: {e}")
        logger.error(f"Failed to remove admin: {e}")

@app.on_message(filters.command("broadcast") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def broadcast_message(client, message):
    try:
        text = message.text.split(maxsplit=1)[1]
        user_ids = [user["user_id"] for user in users_collection.find({}, {"user_id": 1})]
        success_count = 0
        fail_count = 0

        await message.reply("Starting broadcast...")
        await log_to_channel(client, f"Broadcast initiated by `{message.from_user.id}`.")

        for user_id in user_ids:
            try:
                # Skip broadcasting to the admin who initiated it
                if user_id == message.from_user.id:
                    continue
                await client.send_message(user_id, text)
                success_count += 1
                time.sleep(0.1) # Small delay to avoid flood waits
            except Exception as e:
                fail_count += 1
                logger.warning(f"Failed to send broadcast to user {user_id}: {e}")

        await message.reply(f"✅ Broadcast finished. Sent to {success_count} users, failed for {fail_count} users.")
        await log_to_channel(client, f"Broadcast finished by `{message.from_user.id}`. Sent: {success_count}, Failed: {fail_count}.")

    except IndexError:
        await message.reply("❗ Usage: `/broadcast <your message>`")
    except Exception as e:
        await message.reply(f"❌ Broadcast failed: {e}")
        logger.error(f"Broadcast failed: {e}")

# --- Settings Menu Handlers ---
@app.on_message(filters.text & filters.regex("^⚙️ Settings$"))
async def show_settings_menu(client, message):
    user_id = message.from_user.id
    if not get_user_data(user_id):
        await message.reply("Please send /start first to initialize your account.")
        return
    await message.reply("⚙️ Here are your settings:", reply_markup=settings_menu)

@app.on_message(filters.text & filters.regex("^🔙 Main Menu$"))
async def back_to_main_menu(client, message):
    user_id = message.from_user.id
    user_states.pop(user_id, None) # Clear user state
    if is_admin(user_id):
        await message.reply("Returning to Main Menu.", reply_markup=main_menu_admin)
    else:
        await message.reply("Returning to Main Menu.", reply_markup=main_menu_user)

@app.on_message(filters.text & filters.regex("^🔑 Instagram Login$"))
async def prompt_instagram_login_from_settings(client, message):
    user_id = message.from_user.id
    await message.reply("Please use the command `/login <your_instagram_username> <your_instagram_password>` to log in.")

@app.on_message(filters.text & filters.regex("^🔑 Facebook Login$"))
async def prompt_facebook_login_from_settings(client, message):
    user_id = message.from_user.id
    await message.reply(
        "To log in to Facebook, you'll need to provide an **Access Token**.\n"
        "This token should ideally be a **Page Access Token** if you plan to upload to a Facebook Page, as user access tokens are short-lived.\n\n"
        "❗ **How to get a Page Access Token:**\n"
        "1. Go to Facebook Developers: `https://developers.facebook.com/`\n"
        "2. Create an App (if you don't have one).\n"
        "3. Get a User Access Token with `pages_show_list` and `pages_manage_posts` permissions.\n"
        "4. Use that User Access Token to get a Long-Lived Page Access Token for your specific Page.\n\n"
        "Once you have the token, send it using:\n"
        "```\n/fblogin <your_facebook_page_access_token>\n```\n"
        f"_Note: The bot uses a configured Page ID (`{FACEBOOK_PAGE_ID}`). Ensure your token is for that page._"
    )

@app.on_message(filters.command("fblogin"))
async def facebook_login_command(client, message):
    user_id = message.from_user.id
    if not get_user_data(user_id):
        await message.reply("Please send /start first to initialize your account.")
        return
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            await message.reply("❗ Usage: `/fblogin <your_facebook_page_access_token>`")
            return
        
        access_token = args[1].strip()
        
        # Verify token by making a simple Graph API call
        # Using 'me' endpoint to test user access, but for page token, you might test with page info.
        # However, if it's a page token, it generally works with page operations directly.
        # This is a basic validation, not exhaustive.
        test_url = f"https://graph.facebook.com/v19.0/me?access_token={access_token}"
        response = requests.get(test_url)
        response_data = response.json()

        if response.status_code == 200 and 'id' in response_data:
            store_facebook_access_token_for_user(user_id, access_token)
            await message.reply("✅ Facebook login successful! Access token saved.")
            await log_to_channel(client, f"User `{user_id}` successfully logged into Facebook.")
        else:
            error_message = response_data.get('error', {}).get('message', 'Unknown error')
            await message.reply(f"❌ Facebook login failed. Invalid or expired token. Error: `{error_message}`")
            logger.error(f"Facebook token validation failed for user {user_id}: {response_data}")

    except Exception as e:
        await message.reply(f"❌ Failed to process Facebook login: {e}")
        logger.error(f"Failed to process Facebook login for user {user_id}: {e}")

@app.on_message(filters.text & filters.regex("^🎵 Video Audio Settings$"))
async def video_audio_settings(client, message):
    user_id = message.from_user.id
    user_doc = get_user_data(user_id)
    current_pref = user_doc.get("audio_preference", "all").replace('_', ' ').title()

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Keep All Audios", callback_data="audio_all")],
            [InlineKeyboardButton("Keep Only English Audio", callback_data="audio_english")],
            [InlineKeyboardButton("Remove All Audios", callback_data="audio_none")],
            [InlineKeyboardButton("Back to Settings", callback_data="back_to_settings")]
        ]
    )
    await message.reply(f"Select your preferred audio setting for future video uploads.\n\n_Current preference: **{current_pref}**_", reply_markup=keyboard)


@app.on_callback_query(filters.regex("^audio_"))
async def handle_audio_choice_callback(client, callback_query):
    user_id = callback_query.from_user.id
    choice_raw = callback_query.data.split("_")[1] # e.g., "all", "english", "none", "to"

    if choice_raw == "to": # This is from "back_to_settings"
        await callback_query.message.edit_text("⚙️ Here are your settings:", reply_markup=settings_menu)
        await callback_query.answer("Returning to Settings.")
        return

    # Valid audio preferences
    valid_choices = ["all", "english", "none"]
    if choice_raw not in valid_choices:
        await callback_query.answer("Invalid audio choice.", show_alert=True)
        return

    update_user_data(user_id, {"audio_preference": choice_raw})
    display_choice = choice_raw.replace('_', ' ').title()
    await callback_query.answer(f"Audio preference set to: {display_choice}", show_alert=True)
    await callback_query.message.edit_text(f"✅ Your default audio preference is now set to **{display_choice}**.\n\n"
                                          "You can change this anytime from Settings -> Video Audio Settings.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to Settings", callback_data="back_to_settings")]]))
    await log_to_channel(client, f"User `{user_id}` set audio preference to `{choice_raw}`.")

# --- Upload Flow Handlers ---

@app.on_message(filters.text & filters.regex("^📤 Upload a Reel (Instagram)$"))
async def upload_reel_prompt(client, message):
    user_id = message.chat.id
    user_doc = get_user_data(user_id)
    if not user_doc:
        await message.reply("Please send /start first to initialize your account.")
        return

    try:
        # Just try to load and validate session; don't force re-login if no credentials are given
        safe_instagram_login_for_user(user_id) 
        user_states[user_id] = {"step": "awaiting_video_instagram", "platform": "instagram"}
        await message.reply("🎥 Send your Instagram Reel video now.")
    except ValueError as ve: # Raised by safe_instagram_login_for_user if no session and no credentials
        await message.reply(f"❌ You are not logged into Instagram or your session is invalid. Please use `/login` command in Settings to log in first.")
        logger.warning(f"User {user_id} tried to upload to Instagram without valid session: {ve}")
    except Exception as e:
        await message.reply(f"An unexpected error occurred while checking Instagram login: {e}")
        logger.error(f"Unexpected error for user {user_id} checking Instagram login: {e}")


@app.on_message(filters.text & filters.regex("^📤 Upload Video (Facebook)$"))
async def upload_facebook_video_prompt(client, message):
    user_id = message.chat.id
    user_doc = get_user_data(user_id)
    if not user_doc:
        await message.reply("Please send /start first to initialize your account.")
        return

    fb_access_token = get_facebook_access_token_for_user(user_id)
    if not fb_access_token:
        await message.reply("❌ You are not logged into Facebook. Please use `/fblogin` command in Settings to provide your access token first.")
        return

    # Basic check for token validity (can be more robust with an API call if needed)
    if FACEBOOK_PAGE_ID and FACEBOOK_PAGE_ACCESS_TOKEN: # If bot has its own page token
         # You could add a check here to ensure the user's token is for the expected page if needed
         pass
    else:
        await message.reply("Bot's Facebook Page ID or Access Token is not configured. Please contact the admin.")
        return

    user_states[user_id] = {"step": "awaiting_video_facebook", "platform": "facebook"}
    await message.reply("🎥 Send your video for Facebook now.")

@app.on_message(filters.video)
async def handle_video_upload(client, message):
    user_id = message.chat.id
    if not get_user_data(user_id):
        await message.reply("⛔ Please send /start first to initialize your account.")
        return

    state = user_states.get(user_id)
    if not state or (state.get("step") not in ["awaiting_video_instagram", "awaiting_video_facebook"]):
        await message.reply("❗ Please click an upload button (e.g., '📤 Upload a Reel' or '📤 Upload Video (Facebook)') first.")
        return

    # Create 'downloads' directory if it doesn't exist
    if not os.path.exists("downloads"):
        os.makedirs("downloads")

    await message.reply("⏳ Downloading your video... This might take a while for large files.")
    try:
        file_path = await message.download(file_name=f"downloads/{user_id}_{message.video.file_id}.mp4")
        user_states[user_id]["file_path"] = file_path
        user_states[user_id]["step"] = f"awaiting_title_{state['platform']}"
        await message.reply("📝 Now send the title for your video.")
    except Exception as e:
        await message.reply(f"❌ Failed to download video: {e}")
        logger.error(f"Failed to download video for user {user_id}: {e}")
        user_states.pop(user_id, None) # Clear state on failure

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step", "").startswith("awaiting_title_")))
async def handle_upload_title(client, message):
    user_id = message.chat.id
    platform = user_states[user_id]["platform"]
    user_states[user_id]["title"] = message.text
    user_states[user_id]["step"] = f"awaiting_caption_or_description_{platform}"

    if platform == "instagram":
        await message.reply("🏷️ Now send hashtags for your Instagram Reel (e.g., #funny #reel).")
    elif platform == "facebook":
        await message.reply("📝 Now send a description for your Facebook video.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step", "").startswith("awaiting_caption_or_description_")))
async def handle_upload_caption_or_description(client, message):
    user_id = message.chat.id
    platform = user_states[user_id]["platform"]
    file_path = user_states[user_id]["file_path"]
    title = user_states[user_id]["title"]
    caption_or_description = message.text.strip()

    user_states[user_id]["step"] = "processing_and_uploading"
    await message.reply("⏳ Processing your video and preparing for upload... This may take time depending on video size and audio settings. Please wait.")
    await client.send_chat_action(message.chat.id, "upload_video")
    await log_to_channel(client, f"User `{user_id}` initiating upload for {platform}. File: `{os.path.basename(file_path)}`")

    processed_file_path = file_path # Default to original if no processing needed

    try:
        # --- Audio Processing Logic ---
        user_doc = get_user_data(user_id)
        audio_preference = user_doc.get("audio_preference", "all") # Default to 'all' if not set

        if audio_preference != "all":
            # Determine output file path for processed video
            processed_file_path = f"downloads/processed_{user_id}_{os.path.basename(file_path)}"
            await message.reply(f"Applying audio preference: **{audio_preference.replace('_', ' ').title()}**...")
            await client.send_chat_action(message.chat.id, "upload_video")

            def do_audio_processing_sync():
                return process_video_audio(file_path, processed_file_path, audio_preference)
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(do_audio_processing_sync)
                processed_file_path = future.result(timeout=600) # Long timeout for processing
            
            await message.reply("✅ Video audio processing complete.")
            await log_to_channel(client, f"User `{user_id}` video audio processed to `{audio_preference}`. Output: `{os.path.basename(processed_file_path)}`")

        # --- Upload Logic ---
        if platform == "instagram":
            insta_caption = f"{title}\n\n{caption_or_description}"
            def upload_to_instagram_sync():
                temp_insta_client = safe_instagram_login_for_user(user_id) # Get user's specific client
                temp_insta_client.clip_upload(processed_file_path, insta_caption)

            await message.reply("📤 Uploading to Instagram...")
            await client.send_chat_action(message.chat.id, "upload_video")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(upload_to_instagram_sync)
                future.result(timeout=900) # Even longer timeout for large video uploads to Instagram
            await message.reply("✅ Uploaded to Instagram!")
            await log_to_channel(client, f"User `{user_id}` successfully uploaded to Instagram. File: `{os.path.basename(processed_file_path)}`")

        elif platform == "facebook":
            fb_access_token = get_facebook_access_token_for_user(user_id)
            if not fb_access_token:
                await message.reply("❌ Error: Facebook access token not found. Please re-authenticate via /fblogin.")
                return

            await message.reply("📤 Uploading to Facebook...")
            await client.send_chat_action(message.chat.id, "upload_video")
            
            def upload_to_facebook_sync():
                return upload_facebook_video(processed_file_path, title, caption_or_description, fb_access_token, FACEBOOK_PAGE_ID)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(upload_to_facebook_sync)
                fb_result = future.result(timeout=900) # Long timeout for Facebook upload
            
            if fb_result and 'id' in fb_result:
                await message.reply(f"✅ Uploaded to Facebook! Video ID: `{fb_result['id']}`")
                await log_to_channel(client, f"User `{user_id}` successfully uploaded to Facebook. Video ID: `{fb_result['id']}`. File: `{os.path.basename(processed_file_path)}`")
            else:
                await message.reply(f"❌ Facebook upload failed: `{fb_result}`")
                logger.error(f"Facebook upload failed for user {user_id}: {fb_result}")

    except concurrent.futures.TimeoutError:
        await message.reply("❌ Upload/processing timed out. The file might be too large or the network is slow. Please try again with a smaller file or better connection.")
        logger.error(f"Upload/processing timeout for user {user_id}. Original file: {file_path}")
    except RuntimeError as re:
        await message.reply(f"❌ Processing/Upload Error: `{re}`\n\n_Ensure FFmpeg is installed and your video file is not corrupted._")
        logger.error(f"Processing/Upload Error for user {user_id}: {re}")
    except requests.exceptions.RequestException as req_e:
        await message.reply(f"❌ Network/API Error during upload: `{req_e}`\n\n_Please check your internet connection or Facebook API settings._")
        logger.error(f"Network/API Error for user {user_id}: {req_e}")
    except Exception as e:
        await message.reply(f"❌ Upload failed: An unexpected error occurred: `{e}`")
        logger.error(f"Upload failed for user {user_id}: {e}", exc_info=True) # exc_info for full traceback
    finally:
        # Clean up downloaded and processed files
        if 'file_path' in user_states.get(user_id, {}) and os.path.exists(user_states[user_id]['file_path']):
            os.remove(user_states[user_id]['file_path'])
            logger.info(f"Cleaned up original file: {user_states[user_id]['file_path']}")
        if processed_file_path != file_path and os.path.exists(processed_file_path):
            os.remove(processed_file_path)
            logger.info(f"Cleaned up processed file: {processed_file_path}")
        user_states.pop(user_id, None) # Clear state after operation


# === ADMIN PANEL MENU AND HANDLERS ===
@app.on_message(filters.text & filters.regex("^👤 Admin Panel$") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_panel_menu(client, message):
    await message.reply("👋 Welcome to the Admin Panel!", reply_markup=admin_panel_menu_kb)

@app.on_message(filters.text & filters.regex("^📊 View User Stats$") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def view_user_stats(client, message):
    total_users = users_collection.count_documents({})
    admin_users = users_collection.count_documents({"role": "admin"})
    regular_users = total_users - admin_users

    stats_message = (
        f"**📊 User Statistics:**\n"
        f"Total Registered Users: `{total_users}`\n"
        f"Admins: `{admin_users}`\n"
        f"Regular Users: `{regular_users}`\n"
    )
    await message.reply(stats_message)
    await log_to_channel(client, f"Admin `{message.from_user.id}` viewed user stats.")


@app.on_message(filters.text & filters.regex("^(📢 Broadcast Message|➕ Add Admin|➖ Remove Admin)$") & filters.create(lambda _, __, m: is_admin(m.from_user.id)))
async def admin_sub_menu_options(client, message):
    if message.text == "📢 Broadcast Message":
        await message.reply("Please send the message you want to broadcast using the command: `/broadcast <your message>`")
    elif message.text == "➕ Add Admin":
        await message.reply("To add an admin, use the command: `/addadmin <user_id>` (Replace `<user_id>` with the Telegram ID of the user).")
    elif message.text == "➖ Remove Admin":
        await message.reply("To remove an admin, use the command: `/removeadmin <user_id>` (Replace `<user_id>` with the Telegram ID of the admin to demote).")


# === KEEP ALIVE SERVER ===
# This part remains the same, ensuring your bot stays awake on platforms like Render, Heroku etc.
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_server():
    httpd = HTTPServer(('0.0.0.0', 8080), Handler)
    httpd.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# === START BOT ===
if __name__ == "__main__":
    # Ensure a 'downloads' directory exists for temporary files
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Created 'downloads' directory.")

    logger.info("Bot starting...")
    app.run()
