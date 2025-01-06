# bot.py
import logging
import sys
import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# Load environment variables from the .env file
load_dotenv()

# Ensure the bot finds chatgpt.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from chatgpt import process_query_with_chatgpt  # ✅ Ensure chatgpt.py is accessible

# Retrieve the Telegram bot token from the environment variable
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Configure logging to both console and file
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),  # Log to bot.log
        logging.StreamHandler(sys.stdout),  # Log to console
    ]
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command."""
    logging.info(f"Received /start command from {update.effective_user.id}")
    await update.message.reply_text(
        "Welcome! Ask me about NBA players or teams, and I'll provide the stats."
    )

async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles general user messages."""
    user_query = update.message.text.strip()
    logging.info(f"Received message: {user_query}")

    # Send user query to ChatGPT API
    try:
        response = process_query_with_chatgpt(user_query)
        logging.info(f"ChatGPT Response: {response}")
        await update.message.reply_text(response)
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        await update.message.reply_text("Something went wrong. Please try again later.")

def main():
    """Main function to initialize and run the bot."""
    if not TELEGRAM_BOT_TOKEN:
        logging.error("TELEGRAM_BOT_TOKEN not found. Make sure it's set in the .env file.")
        sys.exit(1)

    logging.info("Initializing Telegram Bot...")
    
    try:
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        application.add_handler(CommandHandler("start", start))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_query))

        logging.info("Bot is now running... Press Ctrl+C to stop.")
        application.run_polling()
    
    except Exception as e:
        logging.error(f"Bot failed to start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
