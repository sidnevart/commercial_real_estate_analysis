#!/usr/bin/env python3
"""
Простой тест Telegram бота
"""
import asyncio
import logging
from aiogram import Bot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_bot():
    """Тестируем подключение к боту"""
    try:
        bot_token = "7927196434:AAFFuvxIGSI3IWnkYbyNrEUPUAhdVsvoEnQ"
        bot = Bot(token=bot_token)
        
        # Проверяем подключение
        me = await bot.get_me()
        logger.info(f"✅ Бот подключен успешно: {me.first_name} (@{me.username})")
        
        await bot.session.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к боту: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_bot())
    if result:
        print("✅ Тест прошел успешно!")
    else:
        print("❌ Тест провален!")
