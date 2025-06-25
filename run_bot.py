#!/usr/bin/env python3
"""
Точка входа для запуска Telegram бота
"""
import asyncio
import logging
import sys
import os

# Добавляем корневую директорию в path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.bot_service import bot_service
from core.config import CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger(__name__)

async def main():
    """Запуск Telegram бота"""
    # Проверяем наличие токена
    bot_token = CONFIG.get('telegram_bot_token')
    if not bot_token:
        logger.error("❌ Telegram bot token не найден в конфигурации!")
        logger.info("Добавьте токен в config.yaml:")
        logger.info("telegram_bot_token: 'YOUR_BOT_TOKEN_FROM_BOTFATHER'")
        return
    
    logger.info("✅ Токен бота найден в конфигурации")
    logger.info(f"🤖 Запускаем Telegram бота...")
    
    logger.info("🤖 Запуск Telegram бота для анализа недвижимости...")
    
    try:
        # Инициализируем бота
        bot_service.initialize(bot_token)
        
        if not bot_service.is_enabled():
            logger.error("❌ Не удалось инициализировать бота")
            return
        
        # Запускаем polling
        bot_task = await bot_service.start_bot_polling()
        
        if bot_task:
            logger.info("🟢 Бот запущен и работает. Нажмите Ctrl+C для остановки...")
            await bot_task  # Ждем завершения задачи бота
        else:
            logger.error("❌ Не удалось запустить polling")
        
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал остановки")
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске бота: {e}")
    finally:
        # Остановка бота
        await bot_service.stop()
        logger.info("✅ Бот остановлен")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✅ Бот остановлен пользователем")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
