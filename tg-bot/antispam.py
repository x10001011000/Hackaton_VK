import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Загрузка .env
load_dotenv()

# Глобальная переменная пользователей
user_data = eval(os.getenv("USER_DATA", "{}"))

ANTISPAM = {
    "MAX_REQUESTS": 5,
    "BAN_TIME": 60,
    "BLACKLIST": ["Интересует?", "Хотите заработать", "http", "https", "подработка"]
}

def update_request_stats(user_id: int) -> bool:
    now = datetime.now()

    if user_id not in user_data:
        user_data[user_id] = {"requests": [], "banned_until": None}

    user_data[user_id]["requests"] = [
        t for t in user_data[user_id]["requests"]
        if now - t < timedelta(minutes=1)
    ]

    if user_data[user_id]["banned_until"] and user_data[user_id]["banned_until"] > now:
        return True

    if len(user_data[user_id]["requests"]) >= ANTISPAM["MAX_REQUESTS"]:
        user_data[user_id]["banned_until"] = now + timedelta(seconds=ANTISPAM["BAN_TIME"])
        return True

    user_data[user_id]["requests"].append(now)
    return False

def contains_spam(text: str) -> bool:
    return any(word.lower() in text.lower() for word in ANTISPAM["BLACKLIST"])
