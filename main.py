import os
import threading
import concurrent.futures
from http.server import HTTPServer, BaseHTTPRequestHandler
import logging
import json
import time
import subprocess 

from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv
from pymongo import MongoClient
import requests 

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === LOAD ENV ===
load_dotenv()
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# === NEW: MongoDB Configuration ===
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://cristi7jjr:tRjSVaoSNQfeZ0Ik@cluster0.kowid.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
DB_NAME = "bot_database" 

# === NEW: Admin and Log Channel Configuration ===
OWNER_ID = int(os.getenv("OWNER_ID", "7577977996")) 
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "-1002779117737")) 

# === NEW: Facebook API Configuration ===
FACEBOOK_APP_ID = os.getenv("FACEBOOK_APP_ID", "")
FACEBOOK_APP_SECRET = os.getenv("FACEBOOK_APP_SECRET", "")
FACEBOOK_PAGE_ID = os.getenv("FACEBOOK_PAGE_ID", "") 
FACEBOOK_PAGE_ACCESS_TOKEN = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN", "") 


# === GLOBAL CLIENTS AND DB ===
app = Client("upload_bot", api_id=TELEGRAM_API_ID, api_hash=TELEGRAM_API_HASH, bot_token=TELEGRAM_BOT_TOKEN)

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
users_collection = db["users"]

# --- Ensure indexes for quick lookups ---
users_collection.create_index("user_id", unique=True)

# === KEYBOARDS ===
# Main menu will now only have Facebook and later YouTube/TikTok
main_menu_user = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📤 Upload Video (Facebook)")],
        [KeyboardButton("⚙️ Settings")]
    ],
    resize_keyboard=True
)

main_menu_admin = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📤 Upload Video (Facebook)")],
        [KeyboardButton("⚙️ Settings"), KeyboardButton("👤 Admin Panel")]
    ],
    resize_keyboard=True
)

# Settings menu will be updated for Facebook, YouTube, TikTok logins
settings_menu = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🔑 Facebook Login")], # Instagram Login removed
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
user_states = {}
# Example user_states entry:
# user_states = {
#     chat_id: {
#         "step": "awaiting_video_facebook", 
#         "platform": "facebook", 
#         "file_path": "/path/to/video.mp4",
#         "title": "My awesome video",
#         "caption_or_description": "My video description",
#         "visibility": "public", # new: 'public', 'private', 'unlisted'
#         "schedule_time": None # new: datetime object for scheduling
#     }
# }


# === HELPERS ===
def get_user_data(user_id):
    """Retrieves user data from MongoDB."""
    return users_collection.find_one({"user_id": user_id})

def update_user_data(user_id, data):
    """Updates user data in MongoDB."""
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

def get_facebook_access_token_for_user(user_id):
    """Retrieves Facebook access token from user data."""
    user_doc = get_user_data(user_id)
    return user_doc.get("facebook_access_token")

def store_facebook_access_token_for_user(user_id, token):
    """Stores Facebook access token in user data."""
    update_user_data(user_id, {"facebook_access_token": token})

def upload_facebook_video(file_path, title, description, access_token, page_id, visibility="PUBLISHED"):
    """Uploads a video to Facebook Page using Graph API."""
    if not all([file_path, title, description, access_token, page_id]):
        raise ValueError("Missing required parameters for Facebook video upload.")

    # Facebook API supports different upload phases. For simplicity, we'll use resumable upload.
    # This is a simplified version. For large files, you'd use chunked uploads.
    # The 'status' parameter controls visibility: 'PUBLISHED', 'SCHEDULED_PUBLISH', 'DRAFT'
    # For 'SCHEDULED_PUBLISH', you'd also need 'scheduled_publish_time'
    
    post_url = f"https://graph-video.facebook.com/v19.0/{page_id}/videos"
    
    params = {
        'access_token': access_token,
        'title': title,
        'description': description,
        'published': 'false' if visibility == 'DRAFT' else 'true', # For draft, set published to false
        'status_type': visibility # PUBLISHED, SCHEDULED_PUBLISH, DRAFT
    }

    # If scheduling, add scheduled_publish_time (Unix timestamp)
    # This requires 'SCHEDULED_PUBLISH' as status_type
    # Example: if visibility == 'SCHEDULED_PUBLISH' and schedule_time:
    #     params['scheduled_publish_time'] = int(schedule_time.timestamp())
    
    with open(file_path, 'rb') as f:
        files = {'file': f}
        logger.info(f"Uploading video to Facebook with visibility: {visibility}")
        response = requests.post(post_url, params=params, files=files)
        response.raise_for_status() 
        result = response.json()
        logger.info(f"Facebook video upload result: {result}")
        return result

def process_video_audio(input_path, output_path, audio_preference="all"):
    """
    Processes video audio based on preference using ffmpeg.
    audio_preference: 'all' (keep all), 'none' (remove all), 'mp4' (ensure MP4 container)
    """
    command = ["ffmpeg", "-i", input_path]

    # Check if output is MP4 and input is not, or if audio preference is 'none'
    # This is a simplified approach. A robust solution would check input/output formats
    # and codecs more thoroughly.
    input_ext = os.path.splitext(input_path)[1].lower()
    output_ext = os.path.splitext(output_path)[1].lower()

    if audio_preference == "none":
        command.extend(["-c:v", "copy", "-an", "-y", output_path])
        logger.info(f"FFmpeg: Removing all audio tracks from {input_path}")
    elif input_ext == ".mkv" and output_ext == ".mp4":
        # Convert MKV to MP4, copying video and audio streams
        command.extend(["-c:v", "copy", "-c:a", "copy", "-map", "0", "-y", output_path])
        logger.info(f"FFmpeg: Converting MKV to MP4 for {input_path}")
    else: # Default: copy all streams, no re-encoding
        command.extend(["-c", "copy", "-y", output_path])
        logger.info(f"FFmpeg: Copying all streams from {input_path}")

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"FFmpeg command successful for {input_path}. Output: {result.stdout}")
        return output_path
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg audio/video processing failed for {input_path}. Command: {' '.join(e.cmd)}")
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
        update_user_data(user_id, {
            "user_id": user_id,
            "role": "user",
            "facebook_access_token": None,
            "audio_preference": "all", # Default audio preference
            "premium_platforms": [] # New: List of platforms user has premium for
        })
        await log_to_channel(client, f"New user started bot: `{user_id}` (`{message.from_user.first_name}`)")
        reply_markup = main_menu_user
        welcome_message = "👋 Welcome! I'm your media upload bot. Choose an option below:"
    else:
        if is_admin(user_id):
            reply_markup = main_menu_admin
            welcome_message = "👋 Welcome back Admin! Choose an option below:"
        else:
            reply_markup = main_menu_user
            welcome_message = "👋 Welcome back! Choose an option below:"
    
    await message.reply(welcome_message, reply_markup=reply_markup)


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
                if user_id == message.from_user.id:
                    continue
                await client.send_message(user_id, text)
                success_count += 1
                time.sleep(0.1) 
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
    user_states.pop(user_id, None) 
    if is_admin(user_id):
        await message.reply("Returning to Main Menu.", reply_markup=main_menu_admin)
    else:
        await message.reply("Returning to Main Menu.", reply_markup=main_menu_user)

# Instagram login removed
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
    current_pref_key = user_doc.get("audio_preference", "all")
    display_current_pref = {
        "all": "Keep All Audios",
        "none": "Remove All Audios"
    }.get(current_pref_key, "Unknown")


    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Keep All Audios", callback_data="audio_set_all")],
            [InlineKeyboardButton("Remove All Audios", callback_data="audio_set_none")],
            [InlineKeyboardButton("Back to Settings", callback_data="back_to_settings")]
        ]
    )
    await message.reply(f"Select your **default** audio setting for future video uploads.\n\n"
                        f"_This default will be used for all uploads._\n\n"
                        f"Current default preference: **{display_current_pref}**", reply_markup=keyboard)


@app.on_callback_query(filters.regex("^audio_set_"))
async def handle_default_audio_choice_callback(client, callback_query):
    user_id = callback_query.from_user.id
    choice_raw = callback_query.data.split("_")[2] 

    valid_choices = ["all", "none"]
    if choice_raw not in valid_choices:
        await callback_query.answer("Invalid audio choice.", show_alert=True)
        return

    update_user_data(user_id, {"audio_preference": choice_raw})
    display_choice = {
        "all": "Keep All Audios",
        "none": "Remove All Audios"
    }.get(choice_raw)

    await callback_query.answer(f"Audio preference set to: {display_choice}", show_alert=True)
    await callback_query.message.edit_text(f"✅ Your **default** audio preference is now set to **{display_choice}**.\n\n"
                                          "You can change this anytime from Settings -> Video Audio Settings.",
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back to Settings", callback_data="back_to_settings")]]))
    await log_to_channel(client, f"User `{user_id}` set default audio preference to `{choice_raw}`.")

@app.on_callback_query(filters.regex("^back_to_settings$"))
async def handle_back_to_settings_callback(client, callback_query):
    await callback_query.message.edit_text("⚙️ Here are your settings:", reply_markup=settings_menu)
    await callback_query.answer("Returning to Settings.")


# --- Upload Flow Handlers ---

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

    if not FACEBOOK_PAGE_ID or not FACEBOOK_PAGE_ACCESS_TOKEN:
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
    if not state or (state.get("step") != "awaiting_video_facebook"): # Only Facebook for now
        await message.reply("❗ Please click an upload button (e.g., '📤 Upload Video (Facebook)') first.")
        return

    if not os.path.exists("downloads"):
        os.makedirs("downloads")

    initial_status_msg = await message.reply("⏳ Downloading your video... This might take a while for large files.")
    try:
        file_path = await message.download(file_name=f"downloads/{user_id}_{message.video.file_id}.mp4")
        user_states[user_id]["file_path"] = file_path
        user_states[user_id]["step"] = f"awaiting_title_{state['platform']}"
        await initial_status_msg.edit_text("📝 Now send the title for your video.")
    except Exception as e:
        await initial_status_msg.edit_text(f"❌ Failed to download video: {e}")
        logger.error(f"Failed to download video for user {user_id}: {e}")
        user_states.pop(user_id, None) 

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step", "").startswith("awaiting_title_")))
async def handle_upload_title(client, message):
    user_id = message.chat.id
    platform = user_states[user_id]["platform"]
    user_states[user_id]["title"] = message.text
    user_states[user_id]["step"] = f"awaiting_caption_or_description_{platform}"

    if platform == "facebook":
        await message.reply("📝 Now send a description for your Facebook video.")

@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step", "").startswith("awaiting_caption_or_description_")))
async def handle_upload_caption_or_description(client, message):
    user_id = message.chat.id
    platform = user_states[user_id]["platform"]
    file_path = user_states[user_id]["file_path"]
    title = user_states[user_id]["title"]
    caption_or_description = message.text.strip()
    
    user_states[user_id]["caption_or_description"] = caption_or_description
    user_states[user_id]["step"] = f"awaiting_visibility_{platform}"

    # Offer visibility options
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Public", callback_data="visibility_public")],
            [InlineKeyboardButton("Private", callback_data="visibility_private")],
            [InlineKeyboardButton("Draft", callback_data="visibility_draft")]
        ]
    )
    await message.reply("🌐 Select video visibility:", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^visibility_"))
async def handle_visibility_selection(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or not state.get("step", "").startswith("awaiting_visibility_"):
        await callback_query.answer("Please start an upload process first.", show_alert=True)
        return

    platform = state["platform"]
    visibility_choice = callback_query.data.split("_")[1] # public, private, draft

    user_states[user_id]["visibility"] = visibility_choice
    user_states[user_id]["step"] = f"awaiting_schedule_{platform}"

    # For Facebook, 'private' is 'SELF' and 'public' is 'EVERYONE' in some contexts,
    # but for video uploads, 'PUBLISHED', 'DRAFT', 'SCHEDULED_PUBLISH' are the main types.
    # We'll map 'private' to 'DRAFT' for simplicity for now, as direct 'private' view is complex.
    # The user asked for public/private, so 'public' -> PUBLISHED, 'private' -> DRAFT.

    if platform == "facebook":
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Publish Now", callback_data="schedule_now")],
                [InlineKeyboardButton("Schedule Later", callback_data="schedule_later")]
            ]
        )
        await callback_query.message.edit_text("⏰ Do you want to publish now or schedule for later?", reply_markup=keyboard)
    else: # For other platforms if added later, default to publish now
        await callback_query.message.edit_text("⏳ Processing your video and preparing for upload... Please wait.")
        await callback_query.answer("Processing initiated.")
        await initiate_upload(client, callback_query.message, user_id)


@app.on_callback_query(filters.regex("^schedule_"))
async def handle_schedule_selection(client, callback_query):
    user_id = callback_query.from_user.id
    state = user_states.get(user_id)

    if not state or not state.get("step", "").startswith("awaiting_schedule_"):
        await callback_query.answer("Please start an upload process first.", show_alert=True)
        return

    schedule_choice = callback_query.data.split("_")[1] # now, later

    if schedule_choice == "now":
        user_states[user_id]["schedule_time"] = None # No scheduling
        await callback_query.message.edit_text("⏳ Processing your video and preparing for upload... Please wait.")
        await callback_query.answer("Processing initiated.")
        await initiate_upload(client, callback_query.message, user_id)
    elif schedule_choice == "later":
        user_states[user_id]["step"] = f"awaiting_schedule_datetime_{state['platform']}"
        await callback_query.message.edit_text(
            "📅 Please send the schedule date and time in `YYYY-MM-DD HH:MM` format (e.g., `2025-07-20 14:30`).\n"
            "_Time will be interpreted in UTC._"
        )
        await callback_query.answer("Awaiting schedule time.")

# This handler will be for parsing the schedule time
@app.on_message(filters.text & filters.create(lambda _, __, m: user_states.get(m.chat.id, {}).get("step", "").startswith("awaiting_schedule_datetime_")))
async def handle_schedule_datetime_input(client, message):
    user_id = message.chat.id
    state = user_states.get(user_id)
    
    try:
        # Simple parsing, can be enhanced with more robust date/time libraries if needed
        schedule_str = message.text.strip()
        from datetime import datetime
        schedule_dt = datetime.strptime(schedule_str, "%Y-%m-%d %H:%M")
        
        # Ensure schedule time is in the future
        if schedule_dt <= datetime.utcnow():
            await message.reply("❌ Schedule time must be in the future. Please try again.")
            return

        user_states[user_id]["schedule_time"] = schedule_dt
        await message.reply("⏳ Processing your video and preparing for upload... Please wait.")
        await initiate_upload(client, message, user_id)

    except ValueError:
        await message.reply("❌ Invalid date/time format. Please use `YYYY-MM-DD HH:MM` (e.g., `2025-07-20 14:30`).")
    except Exception as e:
        await message.reply(f"❌ An error occurred while parsing schedule time: {e}")
        logger.error(f"Error parsing schedule time for user {user_id}: {e}")

async def initiate_upload(client, message, user_id):
    state = user_states.get(user_id)
    if not state:
        await client.send_message(user_id, "❌ Upload process interrupted. Please start again.")
        return

    platform = state["platform"]
    file_path = state["file_path"]
    title = state["title"]
    caption_or_description = state["caption_or_description"]
    visibility = state.get("visibility", "public") # Default to public
    schedule_time = state.get("schedule_time") # datetime object or None

    user_states[user_id]["step"] = "processing_and_uploading"
    await client.send_chat_action(user_id, "upload_video")
    await log_to_channel(client, f"User `{user_id}` initiating upload for {platform}. File: `{os.path.basename(file_path)}`. Visibility: {visibility}. Schedule: {schedule_time}")

    processed_file_path = file_path 

    try:
        # Determine output file path based on input extension and target platform
        input_ext = os.path.splitext(file_path)[1].lower()
        output_ext = ".mp4" # Most platforms prefer MP4

        if input_ext != output_ext:
            processed_file_path = f"downloads/processed_{user_id}_{os.path.basename(file_path).replace(input_ext, output_ext)}"
            await client.send_message(user_id, f"Converting video to {output_ext.upper()} format...")
            await client.send_chat_action(user_id, "upload_video")
            def do_processing_sync():
                return process_video_audio(file_path, processed_file_path, audio_preference="all") # Process to ensure MP4, keep all audio
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(do_processing_sync)
                processed_file_path = future.result(timeout=600) 
            await client.send_message(user_id, "✅ Video format conversion complete.")
            await log_to_channel(client, f"User `{user_id}` video converted. Output: `{os.path.basename(processed_file_path)}`")
        
        # Apply audio preference (remove all audio) if selected
        user_doc = get_user_data(user_id)
        audio_preference = user_doc.get("audio_preference", "all")
        if audio_preference == "none":
            temp_processed_path = f"downloads/audio_removed_{user_id}_{os.path.basename(processed_file_path)}"
            await client.send_message(user_id, "Removing all audio tracks...")
            await client.send_chat_action(user_id, "upload_video")
            def do_audio_removal_sync():
                return process_video_audio(processed_file_path, temp_processed_path, audio_preference="none")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(do_audio_removal_sync)
                processed_file_path = future.result(timeout=600)
            await client.send_message(user_id, "✅ Audio removal complete.")
            await log_to_channel(client, f"User `{user_id}` audio removed. Output: `{os.path.basename(processed_file_path)}`")


        # --- Upload Logic ---
        if platform == "facebook":
            fb_access_token = get_facebook_access_token_for_user(user_id)
            if not fb_access_token:
                await client.send_message(user_id, "❌ Error: Facebook access token not found. Please re-authenticate via /fblogin.")
                return

            await client.send_message(user_id, "📤 Uploading to Facebook...")
            await client.send_chat_action(user_id, "upload_video")
            
            # Map visibility choices to Facebook API status_type
            fb_visibility = "PUBLISHED"
            if visibility == "private" or visibility == "draft":
                fb_visibility = "DRAFT" # Facebook treats 'private' or 'unlisted' generally as 'DRAFT' for videos
            # If scheduling, it would be 'SCHEDULED_PUBLISH'
            # For now, we'll simplify to PUBLISHED/DRAFT based on public/private/draft choice.
            # Scheduling will be added in a later iteration.

            def upload_to_facebook_sync():
                return upload_facebook_video(processed_file_path, title, caption_or_description, fb_access_token, FACEBOOK_PAGE_ID, visibility=fb_visibility)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(upload_to_facebook_sync)
                fb_result = future.result(timeout=900) 
            
            if fb_result and 'id' in fb_result:
                await client.send_message(user_id, f"✅ Uploaded to Facebook! Video ID: `{fb_result['id']}`")
                await log_to_channel(client, f"User `{user_id}` successfully uploaded to Facebook. Video ID: `{fb_result['id']}`. File: `{os.path.basename(processed_file_path)}`")
            else:
                await client.send_message(user_id, f"❌ Facebook upload failed: `{fb_result}`")
                logger.error(f"Facebook upload failed for user {user_id}: {fb_result}")

    except concurrent.futures.TimeoutError:
        await client.send_message(user_id, "❌ Upload/processing timed out. The file might be too large or the network is slow. Please try again with a smaller file or better connection.")
        logger.error(f"Upload/processing timeout for user {user_id}. Original file: {file_path}")
    except RuntimeError as re:
        await client.send_message(user_id, f"❌ Processing/Upload Error: `{re}`\n\n_Ensure FFmpeg is installed and your video file is not corrupted._")
        logger.error(f"Processing/Upload Error for user {user_id}: {re}")
    except requests.exceptions.RequestException as req_e:
        await client.send_message(user_id, f"❌ Network/API Error during upload: `{req_e}`\n\n_Please check your internet connection or Facebook API settings._")
        logger.error(f"Network/API Error for user {user_id}: {req_e}")
    except Exception as e:
        await client.send_message(user_id, f"❌ Upload failed: An unexpected error occurred: `{e}`")
        logger.error(f"Upload failed for user {user_id}: {e}", exc_info=True) 
    finally:
        if 'file_path' in user_states.get(user_id, {}) and os.path.exists(user_states[user_id]['file_path']):
            os.remove(user_states[user_id]['file_path'])
            logger.info(f"Cleaned up original file: {user_states[user_id]['file_path']}")
        if processed_file_path != file_path and os.path.exists(processed_file_path):
            os.remove(processed_file_path)
            logger.info(f"Cleaned up processed file: {processed_file_path}")
        user_states.pop(user_id, None) 


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
    # New: List of users with their premium status
    all_users_details = "All Users:\n"
    for user_doc in users_collection.find({}):
        user_id = user_doc.get("user_id")
        user_role = user_doc.get("role", "user")
        premium_platforms = user_doc.get("premium_platforms", [])
        
        premium_status = f"Premium: {', '.join(premium_platforms)}" if premium_platforms else "No Premium"
        all_users_details += f"- `{user_id}` ({user_role}, {premium_status})\n"

    await message.reply(stats_message + "\n" + all_users_details)
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
    if not os.path.exists("downloads"):
        os.makedirs("downloads")
        logger.info("Created 'downloads' directory.")

    logger.info("Bot starting...")
    app.run()
