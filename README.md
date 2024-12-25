
# Telegram Instagram Reels Uploader Bot

This bot allows users to upload Instagram Reels directly from Telegram, providing a seamless integration between the two platforms. With support for both single and multiple Reels uploads, users can effortlessly share their content on Instagram through a simple Telegram interface.

## Features

- **Upload Single Reels**: Send a single video and upload it to Instagram as a Reel.
- **Upload Multiple Reels**: Send multiple videos at once, and the bot will upload them one by one with a 30-second gap.
- **Multi-Language Support**: Supports both Persian and English, allowing users to interact with the bot in their preferred language.
- **User Authorization**: Only authorized users can interact with the bot, ensuring security.
- **Custom Captions**: Upload Reels with custom captions, stored in a text file.

## Technologies Used

- **Python 3.x**
- **Pyrogram**: A Python library for Telegram Bot API.
- **instagrapi**: A Python library for Instagram API.
- **Asyncio**: For asynchronous handling of multiple tasks, such as downloading videos and uploading them with delays.

## Prerequisites

Before running the bot, make sure you have the following installed:

- Python 3.x
- `pip` for managing Python packages

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/telegram-instagram-reels-uploader-bot.git
cd telegram-instagram-reels-uploader-bot
```

### 2. Install Dependencies

Install the required libraries using `pip`:

```bash
pip install -r requirements.txt
```

### 3. Set Up Your Telegram Bot

- Go to [BotFather](https://core.telegram.org/bots#botfather) on Telegram and create a new bot.
- Get your bot token from BotFather.
- Add the token in the `TELEGRAM_BOT_TOKEN` variable inside the script.

### 4. Set Up Instagram Account

- Log in to your Instagram account via the `instagrapi` library using your credentials.
- Store your Instagram username and password in the appropriate variables (`INSTAGRAM_USERNAME` and `INSTAGRAM_PASSWORD`).

### 5. Configure Authorized Users

- Create a text file called `authorized_users.txt` and add the Telegram user IDs of users who are allowed to use the bot.
- Each user ID should be on a new line.

### 6. Set Up Language Preferences

- You can manually set the language for each user in the `languages.txt` file, where the format is `user_id:language`.
- Available languages: `fa` (Persian), `en` (English).

### 7. Set Up Captions

- Create a file named `caption.txt` and add a default caption that will be used when uploading Reels.

## Running the Bot

Once you’ve configured the bot with your credentials and settings, you can start the bot with the following command:

```bash
python bot.py
```

The bot will start running and be ready to handle incoming messages.

## How to Use

1. **Start the Bot**: Type `/start` to begin the interaction.
2. **Choose Language**: Select your preferred language (Persian or English).
3. **Upload a Reel**: Choose either the option to upload a single Reel or multiple Reels.
4. **Send Video(s)**: After selecting the upload option, send your video or videos. The bot will handle the rest, including uploading them to Instagram.

### Example Commands

- `/start` – Start the bot and begin interaction.
- "📤 ارسال یک Reels" (in Persian) or "📤 Upload a Reels" (in English) – Upload a single Reel.
- "📤 ارسال چند Reels همزمان" (in Persian) or "📤 Upload Multiple Reels" (in English) – Upload multiple Reels.

## Notes

- The bot will upload multiple Reels with a 30-second gap between uploads.
- Users who are not authorized will be blocked from using the bot.
- The captions for the Reels are customizable through the `caption.txt` file.
- You can extend the bot to support additional features, such as hashtags or geotags for the uploaded Reels.

## License

This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.

## Contributing

We welcome contributions! Please feel free to open issues or submit pull requests.

## Contact

For any issues or suggestions, feel free to open an issue on the GitHub repository, or reach out via email at [your-email@example.com](mailto:your-email@example.com).

---

Happy Reels uploading! 🎥🚀
