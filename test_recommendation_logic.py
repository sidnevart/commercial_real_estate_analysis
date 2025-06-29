#!/usr/bin/env python3
"""
Полный тест системы рекомендаций и отправки уведомлений
"""
import asyncio
import logging
from core.config import CONFIG
from bot.bot_service import bot_service
from core.models import Lot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_lots_from_table():
    """Создает тестовые лоты на основе реальных данных из таблицы"""
    
    # Данные из вашей таблицы
    test_lots_data = [
        {
            "id": "TEST_001",
            "name": "Нежилое помещение на Гагарина (25.91%)",
            "address": "Московская область, г. Клин, ул. Гагарина, д.6",
            "area": 562.8,
            "price": 16130568,
            "annual_yield_percent": 0.2591,  # 25.91% как дробь
            "market_deviation_percent": -0.185,  # -18.5%
            "capitalization_rub": 2800000,
            "auction_url": "https://torgi.gov.ru/new/public/lots/lot/22000030890000000173_1"
        },
        {
            "id": "TEST_002", 
            "name": "Нежилое помещение в Орехово-Зуево (87.77%)",
            "address": "Московская область, г. Орехово-Зуево, ул. Ленина, д. 57",
            "area": 68.9,
            "price": 1503000,
            "annual_yield_percent": 0.8777,  # 87.77%
            "market_deviation_percent": 0.0,
            "capitalization_rub": 1200000,
            "auction_url": "https://torgi.gov.ru/new/public/lots/lot/21000004710000018748_1"
        },
        {
            "id": "TEST_003",
            "name": "Нежилое помещение на Стасовой (112.89%)", 
            "address": "г. Москва, ул. Стасовой, д. 3/27",
            "area": 270.5,
            "price": 13937000,
            "annual_yield_percent": 1.1289,  # 112.89%
            "market_deviation_percent": 0.0,
            "capitalization_rub": 1500000,
            "auction_url": "https://torgi.gov.ru/new/public/lots/lot/21000005000000025768_1"
        },
        {
            "id": "TEST_004",
            "name": "Помещение в Лыткарино (32.09%)",
            "address": "Московская область, г. Лыткарино, ул. Октябрьская, д. 7/8",
            "area": 85.4,
            "price": 5270000,
            "annual_yield_percent": 0.3209,  # 32.09%
            "market_deviation_percent": 0.0,
            "capitalization_rub": 900000,
            "auction_url": "https://torgi.gov.ru/new/public/lots/lot/21000004710000018686_1"
        },
        {
            "id": "TEST_005",
            "name": "Нежилое помещение на Крылатской (40.72%)",
            "address": "г. Москва, ул. Крылатская, д. 45, корп. 1",
            "area": 615.1,
            "price": 97216000,
            "annual_yield_percent": 0.4072,  # 40.72%
            "market_deviation_percent": -0.35,  # -35%
            "capitalization_rub": 63409192,
            "auction_url": "https://torgi.gov.ru/new/public/lots/lot/21000005000000025763_1"
        },
        {
            "id": "TEST_006",
            "name": "Обычное помещение (8.16% - низкая доходность)",
            "address": "Московская область, г. Подольск, ул. Правды, д. 20",
            "area": 84.6,
            "price": 9745920,
            "annual_yield_percent": 0.0816,  # 8.16%
            "market_deviation_percent": 0.0,
            "capitalization_rub": 0,
            "auction_url": "https://torgi.gov.ru/new/public/lots/lot/21000004710000018749_1"
        }
    ]
    
    lots = []
    for data in test_lots_data:
        lot = Lot(
            id=data["id"],
            name=data["name"],
            address=data["address"],
            area=data["area"],
            price=data["price"],
            notice_number=f"TEST_{data['id']}",
            coords="55.7558,37.6176",  # Москва
            lot_number="001/2024",
            auction_type="Электронный аукцион",
            sale_type="Продажа",
            law_reference="Федеральный закон №44-ФЗ",
            application_start="2024-07-01 09:00:00",
            application_end="2024-07-15 17:00:00", 
            auction_start="2024-07-20 10:00:00",
            cadastral_number="77:01:0001001:1001",
            property_category="Нежилые помещения",
            ownership_type="Государственная собственность",
            auction_step=100000,
            deposit=500000,
            recipient="Департамент городского имущества",
            recipient_inn="7701234567",
            recipient_kpp="770101001",
            bank_name="ПАО СБЕРБАНК",
            bank_bic="044525225",
            bank_account="40102810445370000001",
            correspondent_account="30101810400000000225",
            auction_url=data["auction_url"]
        )
        
        # Добавляем расчетные метрики
        lot.annual_yield_percent = data["annual_yield_percent"]
        lot.market_deviation_percent = data["market_deviation_percent"]
        lot.capitalization_rub = data["capitalization_rub"]
        lot.market_price_per_sqm = 0.0
        lot.current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
        lot.market_value = 0.0
        lot.monthly_gap = 0.0
        lot.annual_income = lot.annual_yield_percent * lot.price
        lot.average_rent_price_per_sqm = 0.0
        lot.sale_offers_count = 5
        lot.rent_offers_count = 10
        lot.filtered_sale_offers_count = 3
        lot.filtered_rent_offers_count = 8
        
        # Определяем статус на основе метрик
        if lot.annual_yield_percent >= 0.15 and lot.capitalization_rub > 1000000:
            lot.status = "excellent"
            lot.plus_rental = 1
            lot.plus_sale = 1
            lot.plus_count = 2
        elif lot.annual_yield_percent >= 0.08 or lot.capitalization_rub > 500000:
            lot.status = "good"
            lot.plus_rental = 1 if lot.annual_yield_percent >= 0.08 else 0
            lot.plus_sale = 1 if lot.capitalization_rub > 500000 else 0
            lot.plus_count = lot.plus_rental + lot.plus_sale
        else:
            lot.status = "acceptable"
            lot.plus_rental = 0
            lot.plus_sale = 0
            lot.plus_count = 0
        
        lots.append(lot)
    
    return lots

async def test_notification_criteria():
    """Тестирует критерии уведомлений"""
    print("🧪 ТЕСТ КРИТЕРИЕВ УВЕДОМЛЕНИЙ")
    print("=" * 70)
    
    # Проверяем конфигурацию
    threshold = CONFIG.get('market_yield_threshold', 10)
    print(f"📊 Порог доходности из config: {threshold}%")
    
    # Инициализируем бота
    try:
        bot_service.initialize()
        if not bot_service.is_enabled():
            print("❌ Бот не настроен! Проверьте config.yaml")
            return False
        print("✅ Бот успешно инициализирован")
    except Exception as e:
        print(f"❌ Ошибка инициализации бота: {e}")
        return False
    
    # Создаем тестовые лоты
    test_lots = create_test_lots_from_table()
    print(f"📋 Создано {len(test_lots)} тестовых лотов")
    
    # Проверяем критерии для каждого лота
    print(f"\n📋 Анализ критериев уведомлений:")
    print("   #  Лот                           Доходность  Отклонение  Капитализация  Уведомление")
    print("   " + "="*90)
    
    lots_to_notify = []
    
    for i, lot in enumerate(test_lots, 1):
        # Получаем доступ к методу проверки критериев
        should_notify = False
        notification_reason = ""
        
        if hasattr(bot_service, 'bot') and hasattr(bot_service.bot, '_should_notify_about_lot'):
            should_notify = bot_service.bot._should_notify_about_lot(lot)
            
            # Определяем причину уведомления
            yield_threshold = CONFIG.get('market_yield_threshold', 10)
            if yield_threshold > 1:
                yield_threshold = yield_threshold / 100
            
            reasons = []
            if lot.annual_yield_percent >= yield_threshold:
                reasons.append(f"доходность {lot.annual_yield_percent*100:.1f}%")
            if hasattr(lot, 'market_deviation_percent') and lot.market_deviation_percent <= -0.20:
                reasons.append(f"скидка {abs(lot.market_deviation_percent)*100:.1f}%")
            if hasattr(lot, 'capitalization_rub') and lot.capitalization_rub > 0:
                reasons.append(f"капитализация {lot.capitalization_rub:,.0f}₽")
            
            notification_reason = ", ".join(reasons) if reasons else "не соответствует критериям"
        
        if should_notify:
            lots_to_notify.append(lot)
        
        deviation_str = f"{lot.market_deviation_percent*100:+6.1f}%" if hasattr(lot, 'market_deviation_percent') else "н/д"
        cap_str = f"{lot.capitalization_rub:10,.0f}₽" if hasattr(lot, 'capitalization_rub') else "н/д"
        notify_str = f"{'✅ ДА' if should_notify else '❌ НЕТ'} ({notification_reason})"
        
        print(f"   {i:2d}. {lot.name[:29]:29s} | {lot.annual_yield_percent*100:7.1f}% | {deviation_str:9s} | {cap_str:12s} | {notify_str}")
    
    print(f"\n✅ Результат анализа критериев:")
    print(f"   • Всего лотов: {len(test_lots)}")
    print(f"   • Подходят для уведомления: {len(lots_to_notify)}")
    print(f"   • Процент подходящих: {len(lots_to_notify)/len(test_lots)*100:.1f}%")
    
    return lots_to_notify

async def test_message_sending():
    """Тестирует отправку сообщений"""
    print(f"\n📱 ТЕСТ ОТПРАВКИ СООБЩЕНИЙ")
    print("=" * 50)
    
    # Получаем лоты для уведомления
    lots_to_notify = await test_notification_criteria()
    
    if not lots_to_notify:
        print("❌ Нет лотов для тестирования отправки")
        return False
    
    print(f"📤 Будет отправлено {len(lots_to_notify)} уведомлений")
    
    # Проверяем подписчиков
    if hasattr(bot_service, 'bot') and hasattr(bot_service.bot, '_load_subscribers'):
        print("📥 Загружаем подписчиков...")
        await bot_service.bot._load_subscribers()
    
    # Проверяем подписчиков ПОСЛЕ загрузки
    if hasattr(bot_service, 'bot') and hasattr(bot_service.bot, 'subscribers'):
        subscribers_count = len(bot_service.bot.subscribers)
        print(f"👥 Подписчиков: {subscribers_count}")
        
        if subscribers_count == 0:
            print("⚠️ Нет подписчиков! Для полного теста добавьте подписчика:")
            print("   1. Запустите бота: python run_bot.py")
            print("   2. Напишите боту /subscribe")
            print("   3. Повторите тест")
            print("\n💡 ИЛИ добавьте подписчика принудительно в тест:")
            
            # ДОБАВЛЯЕМ ПОДПИСЧИКА ПРИНУДИТЕЛЬНО ДЛЯ ТЕСТА
            bot_service.bot.subscribers.add(764315256)  # Ваш chat_id
            await bot_service.bot._save_subscribers()
            print(f"✅ Добавлен тестовый подписчик: 764315256")
            subscribers_count = 1
        
        print(f"👥 Финальное количество подписчиков: {subscribers_count}")
    
    # Тестируем отправку каждого лота
    success_count = 0
    
    for i, lot in enumerate(lots_to_notify, 1):
        try:
            print(f"\n📩 Отправка {i}/{len(lots_to_notify)}: {lot.name[:50]}...")
            
            # Отправляем уведомление
            await bot_service.notify_new_lots([lot])
            
            print(f"✅ Лот {lot.id} отправлен успешно")
            success_count += 1
            
            # Пауза между отправками
            await asyncio.sleep(2)
            
        except Exception as e:
            print(f"❌ Ошибка отправки лота {lot.id}: {e}")
    
    print(f"\n📊 Результат отправки:")
    print(f"   • Успешно отправлено: {success_count}/{len(lots_to_notify)}")
    print(f"   • Процент успеха: {success_count/len(lots_to_notify)*100:.1f}%")
    
    return success_count > 0

async def test_daily_summary():
    """Тестирует отправку ежедневной сводки"""
    print(f"\n📊 ТЕСТ ЕЖЕДНЕВНОЙ СВОДКИ")
    print("=" * 40)
    
    try:
        # Считаем статистику из тестовых данных
        test_lots = create_test_lots_from_table()
        
        threshold = CONFIG.get('market_yield_threshold', 10)
        if threshold > 1:
            threshold = threshold / 100
            
        recommended_count = sum(1 for lot in test_lots if lot.annual_yield_percent >= threshold)
        
        print(f"📈 Статистика для сводки:")
        print(f"   • Всего лотов: {len(test_lots)}")
        print(f"   • Рекомендованных: {recommended_count}")
        print(f"   • Порог доходности: {threshold*100:.1f}%")
        
        # Отправляем сводку
        await bot_service.send_daily_summary(len(test_lots), recommended_count)
        
        print(f"✅ Ежедневная сводка отправлена успешно")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка отправки сводки: {e}")
        return False

async def full_notification_test():
    """Полный тест системы уведомлений"""
    print("🚀 ПОЛНЫЙ ТЕСТ СИСТЕМЫ УВЕДОМЛЕНИЙ")
    print("=" * 80)
    
    results = {
        "criteria_test": False,
        "message_sending": False, 
        "daily_summary": False
    }
    
    try:
        # 1. Тест критериев
        print("🔍 ШАГ 1: Тестирование критериев...")
        lots_to_notify = await test_notification_criteria()
        results["criteria_test"] = len(lots_to_notify) > 0
        
        # 2. Тест отправки сообщений
        print("\n📱 ШАГ 2: Тестирование отправки сообщений...")
        results["message_sending"] = await test_message_sending()
        
        # 3. Тест ежедневной сводки
        print("\n📊 ШАГ 3: Тестирование ежедневной сводки...")
        results["daily_summary"] = await test_daily_summary()
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
    
    # Итоговый отчет
    print(f"\n🎯 ИТОГОВЫЙ ОТЧЕТ:")
    print("=" * 50)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, passed in results.items():
        status = "✅ ПРОШЕЛ" if passed else "❌ ПРОВАЛЕН"
        print(f"   • {test_name}: {status}")
    
    print(f"\n📈 Общий результат: {passed}/{total} тестов прошли")
    
    if passed == total:
        print("🎉 ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО! Система готова к работе!")
    else:
        print("⚠️ Некоторые тесты провалены. Проверьте настройки.")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(full_notification_test())