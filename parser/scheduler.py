import asyncio
import logging
import schedule
import time
from datetime import datetime
import pytz
from parser.main import main as parser_main
from core.deduplication_db import dedup_db

# Московская временная зона
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/production.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def daily_production_run():
    """Ежедневный запуск в production режиме"""
    moscow_time = datetime.now(MOSCOW_TZ)
    logger.info(f"🚀 DAILY PRODUCTION RUN - {moscow_time.strftime('%Y-%m-%d %H:%M:%S')} МСК")
    
    try:
        # Статистика до запуска
        stats_before = dedup_db.get_stats()
        logger.info(f"📊 Статистика до запуска: {stats_before}")
        
        # Запуск парсера в production режиме
        await parser_main(max_pages=40, production_mode=True)
        
        # Статистика после запуска  
        stats_after = dedup_db.get_stats()
        logger.info(f"📊 Статистика после запуска: {stats_after}")
        
        logger.info("✅ PRODUCTION RUN ЗАВЕРШЕН УСПЕШНО")
        
    except Exception as e:
        logger.error(f"❌ ОШИБКА В PRODUCTION RUN: {e}")
        
        # Отправляем уведомление об ошибке в Telegram
        try:
            from bot.bot_service import bot_service
            bot_service.initialize()
            if bot_service.is_enabled():
                error_message = f"❌ Ошибка в ежедневном парсинге:\n{str(e)}"
                # Отправляем админам (можно добавить отдельный список админов)
                pass  # Реализуйте отправку админам
        except Exception as bot_error:
            logger.error(f"Не удалось отправить уведомление об ошибке: {bot_error}")

def schedule_daily_runs():
    """Настройка ежедневного расписания"""
    # Запуск каждый день в 01:00 по московскому времени
    schedule.every().day.at("01:00").do(lambda: asyncio.run(daily_production_run()))
    
    logger.info("⏰ Запланирован ежедневный запуск в 01:00 МСК")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверяем каждую минуту

if __name__ == "__main__":
    logger.info("🚀 ЗАПУСК PRODUCTION ПЛАНИРОВЩИКА")
    schedule_daily_runs()