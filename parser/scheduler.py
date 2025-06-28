"""
Планировщик для автоматического запуска парсера
Запускается каждый день в 01:00 по московскому времени
"""
# filepath: parser/scheduler.py

import asyncio
import logging
import sys
import schedule
import time
from pathlib import Path
from datetime import datetime
import pytz

# Добавляем путь к проекту
sys.path.append(str(Path(__file__).parent.parent))

from parser.main import main as parser_main
from core.deduplication_db import dedup_db

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/scheduler.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Московская временная зона
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

async def scheduled_parser_run():
    """Запуск парсера по расписанию"""
    moscow_time = datetime.now(MOSCOW_TZ)
    logger.info(f"🚀 ЗАПУСК ПЛАНОВОГО ПАРСИНГА - {moscow_time.strftime('%Y-%m-%d %H:%M:%S')} МСК")
    logger.info("=" * 80)
    
    try:
        # Получаем статистику перед запуском
        stats_before = dedup_db.get_stats()
        logger.info(f"📊 Статистика БД перед запуском:")
        logger.info(f"   • Лотов в БД: {stats_before.get('total_lots', 0)}")
        logger.info(f"   • Объявлений в БД: {stats_before.get('total_offers', 0)}")
        
        # Запускаем основной парсер на ВСЕ страницы
        logger.info("🔄 Запуск основного парсера (все 35 страниц)...")
        await parser_main()
        
        # Получаем статистику после запуска
        stats_after = dedup_db.get_stats()
        logger.info(f"📊 Статистика БД после запуска:")
        logger.info(f"   • Лотов в БД: {stats_after.get('total_lots', 0)} (+{stats_after.get('total_lots', 0) - stats_before.get('total_lots', 0)})")
        logger.info(f"   • Объявлений в БД: {stats_after.get('total_offers', 0)} (+{stats_after.get('total_offers', 0) - stats_before.get('total_offers', 0)})")
        
        logger.info("=" * 80)
        logger.info(f"✅ ПЛАНОВЫЙ ПАРСИНГ ЗАВЕРШЕН УСПЕШНО - {datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M:%S')} МСК")
        
    except Exception as e:
        logger.error(f"❌ ОШИБКА ПРИ ПЛАНОВОМ ПАРСИНГЕ: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Можно добавить отправку уведомления об ошибке
        try:
            from bot.telegram_service import TelegramService
            bot_service = TelegramService()
            if bot_service.is_enabled():
                await bot_service.send_error_notification(f"Ошибка планового парсинга: {str(e)}")
        except Exception as bot_error:
            logger.error(f"Не удалось отправить уведомление об ошибке: {bot_error}")

def run_scheduled_task():
    """Обертка для запуска асинхронной задачи"""
    try:
        # Создаем новый event loop для каждого запуска
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(scheduled_parser_run())
        loop.close()
    except Exception as e:
        logger.error(f"Критическая ошибка в планировщике: {e}")

def setup_scheduler():
    """Настройка планировщика"""
    # Запуск каждый день в 01:00 по московскому времени
    schedule.every().day.at("01:00").do(run_scheduled_task)
    
    logger.info("⏰ Планировщик настроен:")
    logger.info("   • Время запуска: 01:00 МСК ежедневно")
    logger.info("   • Режим: полный парсинг (все 35 страниц)")
    logger.info("   • Дедупликация: включена")
    
    # Показываем время следующего запуска
    next_run = schedule.next_run()
    if next_run:
        moscow_next_run = next_run.astimezone(MOSCOW_TZ)
        logger.info(f"   • Следующий запуск: {moscow_next_run.strftime('%Y-%m-%d %H:%M:%S')} МСК")

def main():
    """Основная функция планировщика"""
    logger.info("🚀 ЗАПУСК ПЛАНИРОВЩИКА ПАРСЕРА")
    logger.info("=" * 60)
    
    # Создаем директорию для логов
    Path("logs").mkdir(exist_ok=True)
    
    # Проверяем БД дедупликации
    try:
        stats = dedup_db.get_stats()
        logger.info(f"✅ База данных дедупликации готова")
        logger.info(f"   • Лотов в БД: {stats.get('total_lots', 0)}")
        logger.info(f"   • Объявлений в БД: {stats.get('total_offers', 0)}")
    except Exception as e:
        logger.error(f"❌ Ошибка БД дедупликации: {e}")
        return
    
    # Настраиваем планировщик
    setup_scheduler()
    
    # Основной цикл планировщика
    logger.info("🔄 Планировщик запущен. Ожидание расписания...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Проверяем каждую минуту
            
    except KeyboardInterrupt:
        logger.info("⚠️ Планировщик остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка планировщика: {e}")

"""if __name__ == "__main__":
    main()"""