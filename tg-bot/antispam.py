ANTISPAM = {
    "MAX_REQUESTS": 5,  # Макс. запросов в минуту
    "BAN_TIME": 60,     # Блокировка на 60 секунд
    "BLACKLIST": ["Интересует?", "Хотите заработать", "http", "https" "подработка"]
}

def update_request_stats(user_id: int) -> bool:
    """Обновляет статистику запросов и возвращает True если превышен лимит."""
    now = datetime.now()
    
    if user_id not in user_data:
        user_data[user_id] = {"requests": [], "banned_until": None}
    
    # Очистка старых запросов
    user_data[user_id]["requests"] = [
        t for t in user_data[user_id]["requests"]
        if now - t < timedelta(minutes=1)
    ]
    
    # Проверка активного бана
    if user_data[user_id]["banned_until"] and user_data[user_id]["banned_until"] > now:
        return True
    
    # Проверка лимита
    if len(user_data[user_id]["requests"]) >= ANTISPAM["MAX_REQUESTS"]:
        user_data[user_id]["banned_until"] = now + timedelta(seconds=ANTISPAM["BAN_TIME"])
        return True
    
    # Добавляем текущий запрос
    user_data[user_id]["requests"].append(now)
    return False

def contains_spam(text: str) -> bool:
    return any(word in text.lower() for word in ANTISPAM["BLACKLIST"])

