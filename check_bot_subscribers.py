#!/usr/bin/env python3
"""
Проверка подписчиков бота
"""
import json
import os

def check_bot_subscribers():
    """Проверяет подписчиков бота"""
    subscribers_file = "bot_data/subscribers.json"
    
    if os.path.exists(subscribers_file):
        with open(subscribers_file, 'r') as f:
            subscribers = json.load(f)
        print(f"✅ Подписчиков в боте: {len(subscribers)}")
        print(f"📋 Список: {subscribers}")
    else:
        print("❌ Файл подписчиков не найден")
    
    # Проверяем config
    from core.config import CONFIG
    print(f"\n📋 Конфигурация:")
    print(f"   • telegram_enabled: {CONFIG.get('telegram_enabled')}")
    print(f"   • telegram_chat_id: {CONFIG.get('telegram_chat_id')}")

if __name__ == "__main__":
    check_bot_subscribers()