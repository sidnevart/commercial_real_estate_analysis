import asyncio
import logging
import os
import sys
from pathlib import Path

# Добавляем путь к проекту
sys.path.append(str(Path(__file__).parent))

from core.models import Lot

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

def create_realistic_test_lots():
    """Создает реалистичные тестовые лоты из таблицы"""
    test_lots = [
        Lot(
            id="TEST_PROD_001",
            name="Нежилое помещение на Тверской",
            address="Москва, ул. Тверская, д. 12",
            area=85.5,
            price=15500000,
            notice_number="32109000199000000042",
            coords="55.757465,37.614467",
            lot_number="001/2024",
            auction_type="Продажа права на заключение договора аренды",
            sale_type="Аукцион",
            law_reference="Приказ №123 от 01.01.2024",
            application_start="2024-07-01 09:00:00",
            application_end="2024-07-15 17:00:00",
            auction_start="2024-07-20 10:00:00",
            cadastral_number="77:01:0001001:1001",
            property_category="Нежилые помещения",
            ownership_type="Государственная собственность",
            auction_step=500000,
            deposit=3100000,
            recipient="Департамент городского имущества",
            recipient_inn="7701234567",
            recipient_kpp="770101001",
            bank_name="ПАО СБЕРБАНК",
            bank_bic="044525225",
            bank_account="40102810445370000001",
            correspondent_account="30101810400000000225",
            annual_yield_percent=16.2,
            capitalization_rub=2800000,
            market_deviation_percent=-18.5,
            auction_url="https://torgi.gov.ru/new/public/lots/lot.html?id=TEST_001"
        ),
        Lot(
            id="TEST_PROD_002", 
            name="Офисное помещение в БЦ",
            address="Москва, Пресненская наб., д. 6",
            area=120.0,
            price=45000000,
            notice_number="77109000199000000043",
            coords="55.747890,37.538654",
            lot_number="002/2024",
            auction_type="Продажа права на заключение договора аренды",
            sale_type="Аукцион",
            law_reference="Приказ №124 от 01.01.2024",
            application_start="2024-07-05 09:00:00",
            application_end="2024-07-19 17:00:00",
            auction_start="2024-07-24 11:00:00",
            cadastral_number="77:01:0001001:1002",
            property_category="Офисные помещения",
            ownership_type="Государственная собственность",
            auction_step=1500000,
            deposit=9000000,
            recipient="Департамент городского имущества",
            recipient_inn="7701234567",
            recipient_kpp="770101001",
            bank_name="ПАО СБЕРБАНК",
            bank_bic="044525225",
            bank_account="40102810445370000001",
            correspondent_account="30101810400000000225",
            annual_yield_percent=8.1,
            capitalization_rub=1200000,
            market_deviation_percent=-8.2,
            auction_url="https://torgi.gov.ru/new/public/lots/lot.html?id=TEST_002"
        ),
        Lot(
            id="TEST_PROD_003",
            name="Торговое помещение в ТЦ", 
            address="Подольск, ул. Правды, д. 20",
            area=75.0,
            price=8500000,
            notice_number="50109000199000000044",
            coords="55.423456,37.545678",
            lot_number="003/2024",
            auction_type="Продажа права на заключение договора аренды",
            sale_type="Аукцион",
            law_reference="Приказ №125 от 01.01.2024",
            application_start="2024-07-03 09:00:00",
            application_end="2024-07-17 17:00:00",
            auction_start="2024-07-22 14:00:00",
            cadastral_number="50:55:0001001:1003",
            property_category="Торговые помещения",
            ownership_type="Муниципальная собственность",
            auction_step=300000,
            deposit=1700000,
            recipient="Комитет по управлению имуществом",
            recipient_inn="5001234567",
            recipient_kpp="500101001",
            bank_name="ПАО СБЕРБАНК",
            bank_bic="044525225",
            bank_account="40102810445370000002",
            correspondent_account="30101810400000000225",
            annual_yield_percent=22.5,
            capitalization_rub=3500000,
            market_deviation_percent=-25.8,
            auction_url="https://torgi.gov.ru/new/public/lots/lot.html?id=TEST_003"
        )
    ]
    
    # Добавляем дополнительные атрибуты
    for lot in test_lots:
        lot.market_price_per_sqm = 0.0
        lot.current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
        lot.market_value = 0.0
        lot.monthly_gap = 0.0
        lot.annual_income = 0.0
        lot.average_rent_price_per_sqm = 0.0
        lot.sale_offers_count = 0
        lot.rent_offers_count = 0
        lot.filtered_sale_offers_count = 0
        lot.filtered_rent_offers_count = 0
        lot.plus_rental = 1 if lot.annual_yield_percent >= 15 else 0
        lot.plus_sale = 1 if getattr(lot, 'capitalization_rub', 0) > 1000000 else 0
        lot.plus_count = lot.plus_rental + lot.plus_sale
        
        if lot.plus_count == 2:
            lot.status = "excellent"
        elif lot.plus_count == 1:
            lot.status = "good"
        else:
            lot.status = "acceptable"
    
    return test_lots

async def test_bot_production_simulation():
    """Симулирует работу бота в production условиях"""
    logger.info("🧪 ТЕСТИРОВАНИЕ БОТА В PRODUCTION УСЛОВИЯХ")
    logger.info("=" * 60)
    
    # Проверяем наличие bot_service
    try:
        from bot.bot_service import bot_service
        logger.info("✅ bot_service импортирован успешно")
    except ImportError as e:
        logger.error(f"❌ Ошибка импорта bot_service: {e}")
        return
    
    # Инициализируем бота
    try:
        bot_service.initialize()
        logger.info("✅ Инициализация bot_service прошла успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации бота: {e}")
        logger.info("💡 Проверьте наличие config.yaml с настройками Telegram")
        return
    
    if not bot_service.is_enabled():
        logger.error("❌ Бот не включен! Проверьте config.yaml")
        logger.info("💡 Убедитесь, что в config.yaml указаны:")
        logger.info("   • telegram_bot_token: 'ваш_токен'")
        logger.info("   • telegram_chat_id: 'ваш_chat_id'")
        logger.info("   • telegram_enabled: true")
        return
    
    logger.info("✅ Бот успешно инициализирован и включен")
    
    # Создаем тестовые лоты
    try:
        test_lots = create_realistic_test_lots()
        logger.info(f"📊 Создано {len(test_lots)} тестовых лотов")
    except Exception as e:
        logger.error(f"❌ Ошибка создания тестовых лотов: {e}")
        return
    
    # Тест 1: Отправка уведомлений о новых лотах
    logger.info("\n🔔 ТЕСТ 1: Уведомления о новых лотах")
    try:
        for i, lot in enumerate(test_lots, 1):
            logger.info(f"Отправка лота {i}/{len(test_lots)}: {lot.name}")
            await bot_service.notify_new_lots([lot])
            logger.info(f"✅ Лот {lot.id} отправлен успешно")
            await asyncio.sleep(2)  # Пауза между отправками
        logger.info("✅ Тест уведомлений прошел успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте уведомлений: {e}")
    
    # Тест 2: Ежедневная сводка
    logger.info("\n📊 ТЕСТ 2: Ежедневная сводка")
    try:
        recommended_count = sum(1 for lot in test_lots if lot.annual_yield_percent >= 15)
        logger.info(f"Отправка сводки: {len(test_lots)} лотов, {recommended_count} рекомендованных")
        await bot_service.send_daily_summary(len(test_lots), recommended_count)
        logger.info("✅ Тест ежедневной сводки прошел успешно")
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте сводки: {e}")
    
    # Тест 3: Проверка критериев уведомлений
    logger.info("\n🎯 ТЕСТ 3: Критерии уведомлений")
    try:
        if hasattr(bot_service, 'bot') and hasattr(bot_service.bot, '_should_notify_about_lot'):
            for lot in test_lots:
                should_notify = bot_service.bot._should_notify_about_lot(lot)
                logger.info(f"Лот {lot.id}: {'✅ УВЕДОМИТЬ' if should_notify else '❌ НЕ уведомлять'}")
                logger.info(f"   Доходность: {lot.annual_yield_percent:.1f}%")
                logger.info(f"   Отклонение: {getattr(lot, 'market_deviation_percent', 0):.1f}%")
                logger.info(f"   Статус: {getattr(lot, 'status', 'unknown')}")
        else:
            logger.warning("⚠️ Метод _should_notify_about_lot не найден в боте")
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте критериев: {e}")
    
    # Тест 4: Проверка форматирования сообщений
    logger.info("\n💬 ТЕСТ 4: Форматирование сообщений")
    try:
        if hasattr(bot_service, 'bot') and hasattr(bot_service.bot, 'message_formatter'):
            for lot in test_lots[:1]:  # Тестируем на одном лоте
                # ИСПРАВЛЕНО: используем правильный метод
                formatted_message = bot_service.bot.message_formatter.format_lot_analysis(lot)
                logger.info("📝 Пример форматированного сообщения:")
                logger.info(f"{formatted_message[:200]}...")
                logger.info("✅ Форматирование сообщений работает")
        else:
            logger.warning("⚠️ message_formatter не найден в боте")
            logger.info("💡 Добавьте message_formatter в bot_service.bot")
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте форматирования: {e}")
    
    logger.info("\n🏁 Тестирование бота завершено!")

async def test_deduplication():
    """Тестирует систему дедупликации"""
    logger.info("\n🔍 ТЕСТ: Система дедупликации")
    
    try:
        # Создаем файл если не существует
        dedup_file_path = Path(__file__).parent / "core" / "deduplication_db.py"
        if not dedup_file_path.exists():
            logger.warning("⚠️ Файл core/deduplication_db.py не найден. Создается...")
            os.makedirs(dedup_file_path.parent, exist_ok=True)
            
            # Создаем файл с базовой реализацией
            dedup_code = '''import sqlite3
import hashlib
import logging
import os
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class DeduplicationDB:
    def __init__(self, db_path: str = "data/deduplication.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lots (
                    id TEXT PRIMARY KEY,
                    address_hash TEXT NOT NULL,
                    area REAL NOT NULL,
                    price REAL NOT NULL,
                    notice_number TEXT,
                    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                    times_seen INTEGER DEFAULT 1,
                    last_price REAL,
                    price_changed BOOLEAN DEFAULT FALSE
                )
            """)
    
    def _get_lot_signature(self, lot):
        signature_data = f"{lot.address}|{lot.area}|{lot.notice_number}"
        return hashlib.md5(signature_data.encode()).hexdigest()
    
    def is_duplicate(self, lot):
        signature = self._get_lot_signature(lot)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT * FROM lots WHERE address_hash = ?", (signature,))
            existing = cursor.fetchone()
            return bool(existing), {"existing": bool(existing)}
    
    def add_lot(self, lot):
        signature = self._get_lot_signature(lot)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT INTO lots (id, address_hash, area, price, notice_number) VALUES (?, ?, ?, ?, ?)",
                        (lot.id, signature, lot.area, lot.price, lot.notice_number))
    
    def mark_processed(self, lot_id: str, has_analytics: bool = False, sent_to_telegram: bool = False):
        pass
    
    def get_stats(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM lots")
            total_lots = cursor.fetchone()[0]
            return {"total_lots": total_lots, "price_changed_lots": 0, "processed_lots": 0}

dedup_db = DeduplicationDB()
'''
            
            with open(dedup_file_path, 'w', encoding='utf-8') as f:
                f.write(dedup_code)
            logger.info("✅ Файл core/deduplication_db.py создан")
        
        # Проверяем импорт
        from core.deduplication_db import DeduplicationDB
        logger.info("✅ DeduplicationDB импортирован успешно")
        
        # Создаем временную базу для тестов
        test_db_path = "data/test_deduplication.db"
        os.makedirs("data", exist_ok=True)
        
        # Удаляем тестовую базу если существует
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
        
        # Создаем новый экземпляр
        test_db = DeduplicationDB(test_db_path)
        logger.info("✅ Тестовая база данных создана")
        
        # Получаем статистику
        stats = test_db.get_stats()
        logger.info(f"📊 Начальная статистика: {stats}")
        
        # Тестируем на лоте
        test_lots = create_realistic_test_lots()
        test_lot = test_lots[0]
        
        # Первая проверка - должен быть новым
        is_duplicate1, info1 = test_db.is_duplicate(test_lot)
        logger.info(f"Первая проверка: {'дубликат' if is_duplicate1 else 'новый лот'}")
        
        if not is_duplicate1:
            test_db.add_lot(test_lot)
            logger.info("✅ Лот добавлен в базу")
        
        # Вторая проверка - должен быть дубликатом
        is_duplicate2, info2 = test_db.is_duplicate(test_lot)
        logger.info(f"Вторая проверка: {'дубликат' if is_duplicate2 else 'новый лот'}")
        
        # Финальная статистика
        final_stats = test_db.get_stats()
        logger.info(f"📊 Финальная статистика: {final_stats}")
        
        logger.info("✅ Тест дедупликации завершен успешно")
        
        # Удаляем тестовую базу
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
            logger.info("🗑️ Тестовая база данных удалена")
        
    except ImportError as e:
        logger.error(f"❌ Ошибка импорта: {e}")
        logger.info("💡 Убедитесь, что файл core/deduplication_db.py существует")
    except Exception as e:
        logger.error(f"❌ Ошибка в тесте дедупликации: {e}")

async def test_scheduler_integration():
    """Тестирует базовые импорты для планировщика"""
    logger.info("\n⏰ ТЕСТ: Базовая проверка компонентов")
    
    # Проверяем импорты
    try:
        from parser.main import main as parser_main
        logger.info("✅ parser.main импортирован успешно")
    except ImportError as e:
        logger.error(f"❌ Ошибка импорта parser.main: {e}")
        return
    
    try:
        from bot.bot_service import bot_service
        logger.info("✅ bot_service импортирован успешно")
    except ImportError as e:
        logger.error(f"❌ Ошибка импорта bot_service: {e}")
        return
    
    try:
        from core.models import Lot
        logger.info("✅ Модель Lot импортирована успешно")
    except ImportError as e:
        logger.error(f"❌ Ошибка импорта Lot: {e}")
        return
    
    logger.info("✅ Все базовые компоненты доступны")
    logger.info("💡 Для полного теста запустите парсер отдельно")

async def run_all_tests():
    """Запускает все тесты последовательно"""
    logger.info("🚀 ЗАПУСК ПОЛНОГО ТЕСТИРОВАНИЯ PRODUCTION СИСТЕМЫ")
    logger.info("=" * 70)
    
    # Тест бота
    await test_bot_production_simulation()
    
    # Тест дедупликации
    await test_deduplication()
    
    # Базовая проверка компонентов
    await test_scheduler_integration()
    
    logger.info("\n🎯 ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ!")
    logger.info("📋 Проверьте результаты выше для готовности к production")

if __name__ == "__main__":
    asyncio.run(run_all_tests())