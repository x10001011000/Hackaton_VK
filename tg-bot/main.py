from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters

async def handle_message(update: Update, context):
    question = update.message.text
    response = chain.run(data=informational_text, question=question)
    await update.message.reply_text(response)

app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
app.add_handler(MessageHandler(filters.TEXT, handle_message))
app.run_polling()
