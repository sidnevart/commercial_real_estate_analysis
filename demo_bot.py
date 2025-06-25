#!/usr/bin/env python3
"""
Демонстрация возможностей бота с тестовыми данными
"""
import asyncio
import logging
from typing import List
from core.models import Lot, PropertyClassification
from bot.bot_service import bot_service
from datetime import datetime
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_lot() -> Lot:
    """Создает тестовый лот для демонстрации"""
    lot = Lot(
        id="TEST_LOT_001",
        name="Коммерческое помещение на Арбате",
        address="Москва, ул. Арбат, 15",
        coords=(55.752004, 37.618423),  # Координаты Арбата
        area=120.0,  # 120 м²
        price=25000000.0,  # 25 млн рублей
        notice_number="TEST-2025-001",
        lot_number=1,
        auction_type="Электронный аукцион",
        sale_type="Продажа",
        law_reference="Федеральный закон №44-ФЗ",
        application_start=datetime.now(),
        application_end=datetime.now(),
        auction_start=datetime.now(),
        cadastral_number="77:01:0001001:1234",
        property_category="Нежилые помещения",
        ownership_type="Государственная",
        auction_step=1250000.0,  # 5% от стартовой цены
        deposit=2500000.0,  # 10% от стартовой цены
        recipient="Росимущество",
        recipient_inn="7710542117",
        recipient_kpp="771001001",
        bank_name="ПАО Сбербанк",
        bank_bic="044525225",
        bank_account="40102810445370000023",
        correspondent_account="30101810400000000225",
        auction_url="https://torgi.gov.ru/new/public/lots/lot.html?id=test001",
        district="Арбатский"
    )
    
    # Добавляем расчетные метрики
    lot.market_price_per_sqm = 250000  # 250к за м²
    lot.market_value = 30000000  # 30 млн рыночная цена
    lot.capitalization_rub = 2500000  # 2.5 млн капитализации
    lot.capitalization_percent = 12.5  # 12.5% капитализации
    lot.monthly_gap = 150000  # 150к в месяц ГАП
    lot.annual_yield_percent = 15.2  # 15.2% доходность
    lot.market_deviation_percent = -16.7  # -16.7% от рынка (выгодно)
    
    # Добавляем классификацию
    lot.classification = PropertyClassification(
        category="Стрит-ритейл",
        size_category="120-250 м²",
        has_basement=False,
        is_top_floor=False
    )
    
    return lot

async def demo_bot_features():
    """Демонстрирует возможности бота"""
    try:
        # Инициализируем бота
        logger.info("🤖 Инициализация бота...")
        bot_service.initialize()
        
        if not bot_service.is_enabled():
            logger.error("❌ Бот не инициализирован")
            return
        
        logger.info("✅ Бот успешно инициализирован")
        
        # Создаем тестовый лот
        test_lot = create_test_lot()
        logger.info(f"📊 Создан тестовый лот: {test_lot.name}")
        
        # Демонстрируем форматирование сообщения
        from bot.message_formatter import MessageFormatter
        
        message = MessageFormatter.format_lot_analysis(test_lot)
        logger.info("📝 Пример сообщения о лоте:")
        print("=" * 60)
        print(message)
        print("=" * 60)
        
        # Проверяем критерии уведомления
        from bot.telegram_bot import RealEstateBot
        bot_instance = RealEstateBot("7927196434:AAFFuvxIGSI3IWnkYbyNrEUPUAhdVsvoEnQ")
        should_notify = bot_instance._should_notify_about_lot(test_lot)
        
        logger.info(f"🔔 Стоит ли отправлять уведомление: {'ДА' if should_notify else 'НЕТ'}")
        logger.info(f"   Доходность: {test_lot.annual_yield_percent:.1f}%")
        logger.info(f"   Отклонение от рынка: {test_lot.market_deviation_percent:.1f}%")
        logger.info(f"   Капитализация: {test_lot.capitalization_rub:,.0f} ₽")
        
        # Демонстрируем краткое описание
        short_summary = MessageFormatter.format_short_lot_summary(test_lot)
        logger.info("📋 Краткое описание лота:")
        print(short_summary)
        
        logger.info("✅ Демонстрация завершена успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в демонстрации: {e}")

async def main():
    """Главная функция демонстрации"""
    logger.info("🚀 Запуск демонстрации возможностей бота...")
    
    await demo_bot_features()
    
    logger.info("🏁 Демонстрация завершена")

if __name__ == "__main__":
    asyncio.run(main())
