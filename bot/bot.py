# bot.py
from dotenv import load_dotenv
import os
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from chatgpt import process_query_with_chatgpt

# Load environment variables from the .env file
load_dotenv()

# Retrieve the Telegram bot token from the environment variable
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Welcome! Ask me about NBA players or teams, and I'll provide the stats."
    )

async def handle_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_query = update.message.text.strip()
    
    # Send user query to ChatGPT API
    try:
        response = process_query_with_chatgpt(user_query)
        await update.message.reply_text(response)
    except Exception as e:
        await update.message.reply_text(f"Something went wrong: {e}")

def main():
    # Ensure the token is loaded
    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN not found. Make sure it's set in the .env file.")
        return

    # Set up the Telegram bot application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Add command and message handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_query))

    # Start the bot
    print("Bot is running. Ask your questions about NBA players and teams.")
    application.run_polling()

if __name__ == "__main__":
    main()
