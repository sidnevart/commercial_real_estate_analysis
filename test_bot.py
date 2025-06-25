#!/usr/bin/env python3
"""
Тест Telegram бота без реального запуска
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.config import CONFIG
from core.models import Lot, PropertyClassification
from bot.message_formatter import MessageFormatter
from uuid import uuid4
from datetime import datetime

def test_message_formatting():
    """Тест форматирования сообщений"""
    print("🧪 Тестирование форматирования сообщений...")
    
    # Создаем тестовый лот
    test_lot = Lot(
        id="123",
        name="Коммерческое помещение на Тверской",
        address="Москва, ул. Тверская, д. 1",
        coords=(55.7558, 37.6176),
        area=150.0,
        price=15000000,
        notice_number="Документ №12345",
        lot_number=1,
        auction_type="Аукцион",
        sale_type="Продажа",
        law_reference="44-ФЗ",
        application_start=datetime.now(),
        application_end=datetime.now(),
        auction_start=datetime.now(),
        cadastral_number="77:01:0001234:567",
        property_category="Нежилое помещение",
        ownership_type="Собственность",
        auction_step=500000,
        deposit=1500000,
        recipient="Росимущество",
        recipient_inn="1234567890",
        recipient_kpp="123456789",
        bank_name="Сбербанк",
        bank_bic="044525225",
        bank_account="40102810445370000022",
        correspondent_account="30101810400000000225",
        auction_url="https://torgi.gov.ru/test",
        uuid=uuid4(),
        district="Тверской район"
    )
    
    # Добавляем финансовые метрики
    test_lot.market_price_per_sqm = 120000
    test_lot.current_price_per_sqm = 100000
    test_lot.market_value = 18000000
    test_lot.capitalization_rub = 1800000
    test_lot.capitalization_percent = 12.0
    test_lot.monthly_gap = 150000
    test_lot.annual_yield_percent = 12.0
    test_lot.market_deviation_percent = -16.7
    test_lot.classification = PropertyClassification(
        category="Стрит-ритейл",
        size_category="120-250 м²",
        has_basement=False,
        is_top_floor=False
    )
    
    # Тестируем форматирование
    message = MessageFormatter.format_lot_analysis(test_lot)
    print("📄 Отформатированное сообщение о лоте:")
    print("=" * 50)
    print(message)
    print("=" * 50)
    
    return True

def test_config():
    """Тест конфигурации"""
    print("🧪 Тестирование конфигурации...")
    
    bot_token = CONFIG.get('telegram_bot_token', '')
    enabled = CONFIG.get('telegram_notifications_enabled', False)
    
    print(f"✅ Уведомления включены: {enabled}")
    print(f"{'✅' if bot_token else '❌'} Токен бота: {'установлен' if bot_token else 'не установлен'}")
    
    if not bot_token:
        print("⚠️  Для работы бота нужно добавить токен в config.yaml")
        print("   telegram_bot_token: 'ваш_токен_от_botfather'")
    
    return bool(bot_token)

def main():
    """Основная функция теста"""
    print("🤖 Тест Telegram бота для анализа недвижимости")
    print("=" * 60)
    
    # Тестируем конфигурацию
    config_ok = test_config()
    print()
    
    # Тестируем форматирование
    formatting_ok = test_message_formatting()
    print()
    
    if config_ok and formatting_ok:
        print("✅ Все тесты пройдены! Бот готов к запуску.")
        print("💡 Для запуска используйте: python3 run_bot.py")
    else:
        print("❌ Некоторые тесты не пройдены. Проверьте конфигурацию.")
    
    return config_ok and formatting_ok

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
