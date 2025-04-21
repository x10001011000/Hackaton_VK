import logging
import os
from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from datetime import datetime

import html_parser
import llm_connection
import antispam

# Загрузка переменных из .env
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")

user_data = eval(os.getenv("USER_DATA", "{}"))

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

SITES = [
    "People hub инструкции",
    "People hub архитектура",
    "Информация о Хакатоне",
    "Работы Фролова",
    "Музеи",
    "Литература",
    "Таблицы"
]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if antispam.update_request_stats(update.effective_user.id):
        until = user_data[update.effective_user.id]["banned_until"].strftime("%H:%M:%S")
        await update.message.reply_text(f"Лимит запросов. Попробуйте после {until}.")
        return

    buttons = [[site] for site in SITES]
    await update.message.reply_text(
        "Выберите источник информации:",
        reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True)
    )

async def handle_site_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id

    if antispam.update_request_stats(user_id):
        until = user_data[user_id]["banned_until"].strftime("%H:%M:%S")
        await update.message.reply_text(f"Лимит запросов. Попробуйте после {until}.")
        return

    site_name = update.message.text
    if site_name not in SITES:
        await update.message.reply_text("Пожалуйста, выберите источник из списка.")
        return

    await update.message.reply_text(f"Загружаю данные из '{site_name}'...")
    try:
        text = html_parser.get_site_pages_text(site_name)
        user_data[user_id].update({"text": text, "site": site_name})
        await update.message.reply_text(
            f"Готово! Теперь задайте вопрос из области сайта'{site_name}'.",
            reply_markup=ReplyKeyboardRemove()
        )
    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}")
        await update.message.reply_text("Ошибка загрузки. Попробуйте другой источник.")

async def handle_question(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id

    if user_id not in user_data or "text" not in user_data[user_id]:
        await update.message.reply_text("Сначала выберите источник через /start")
        return

    if antispam.update_request_stats(user_id):
        until = user_data[user_id]["banned_until"].strftime("%H:%M:%S")
        await update.message.reply_text(f"Лимит запросов. Попробуйте после {until}.")
        return

    question = update.message.text
    if antispam.contains_spam(question):
        await update.message.reply_text("Запрос содержит запрещённые слова.")
        return

    await update.message.reply_chat_action(action="typing")
    try:
        response = await context.application.run_async(
            llm_connection.chain.run,
            data=user_data[user_id]["text"],
            question=question
        )
        await update.message.reply_text(response)
    except Exception as e:
        logger.error(f"Ошибка LLM: {e}")
        await update.message.reply_text("Ошибка генерации ответа.")

def main() -> None:
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Text(SITES), handle_site_selection))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_question))

    application.run_polling()

if __name__ == "__main__":
    main()
