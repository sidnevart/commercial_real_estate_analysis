"""
Скрипт для тестирования бота без планировщика
"""
# filepath: test_bot.py

import asyncio
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from bot.bot_service import bot_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_bot():
    """Тестирует бота отдельно"""
    logger.info("🤖 Тестирование Telegram бота")
    
    # Инициализируем бот
    bot_service.initialize()
    
    if not bot_service.is_enabled():
        logger.error("❌ Бот не настроен (проверьте config.yaml)")
        return
    
    try:
        # Тест ежедневной сводки
        await bot_service.send_daily_summary(15, 3)
        logger.info("✅ Ежедневная сводка отправлена")
        
        # Тест уведомления о лотах (создаем тестовый лот)
        from core.models import Lot
        test_lot = Lot(
            id="test_001",
            name="Тестовый лот",
            address="Москва, Тверская, 1",
            area=100.0,
            price=5000000,
            annual_yield_percent=25.0
        )
        await bot_service.notify_new_lots([test_lot])
        logger.info("✅ Уведомление о тестовом лоте отправлено")
        
        logger.info("🎉 Все тесты бота прошли успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования бота: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(test_bot())