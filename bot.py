from credentials import *
import telegram
import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
bot = telegram.Bot(token=telegram_token)

def send_telegram_msg(message, chat_id):
    bot.sendMessage(chat_id=chat_id, text=message)
