#!/usr/bin/env python3
"""
Тестирование поиска аналогов по UUID лота
"""
import asyncio
import logging
from parser.google_sheets import find_lot_by_uuid, find_analogs_in_sheets
from bot.analog_search import AnalogSearchService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_analog_search_by_uuid():
    """Тестирование поиска аналогов по UUID"""
    
    # Сначала посмотрим, есть ли лоты в таблице
    print("🔍 Поиск первого доступного лота для тестирования...")
    
    try:
        from parser.google_sheets import _svc, GSHEET_ID
        
        # Получаем первые несколько лотов из таблицы
        result = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range="lots_all!A2:R10"  # Берем первые 9 лотов (строки 2-10)
        ).execute()
        
        values = result.get('values', [])
        if not values:
            print("❌ В таблице lots_all нет данных для тестирования")
            return
        
        print(f"📊 Найдено {len(values)} лотов в таблице")
        
        # Берем несколько лотов с UUID для тестирования
        test_lots = []
        
        for i, row in enumerate(values):
            if len(row) > 17 and row[17]:  # Колонка R - UUID
                lot_info = {
                    'uuid': row[17],
                    'name': row[1] if len(row) > 1 else '',
                    'address': row[2] if len(row) > 2 else '',
                    'area': row[5] if len(row) > 5 else '',
                }
                test_lots.append(lot_info)
                print(f"📝 Лот #{i+1}: {lot_info['name'][:50]} (UUID: {lot_info['uuid'][:8]}...)")
        
        if not test_lots:
            print("❌ Не найдено лотов с UUID для тестирования")
            return
        
        print(f"\n🔄 Будем тестировать {len(test_lots)} лотов...")
        
        # Тестируем каждый лот, пока не найдем аналоги
        for test_num, lot_info in enumerate(test_lots, 1):
            test_lot_uuid = lot_info['uuid']
            
            print(f"\n{'='*60}")
            print(f"🧪 ТЕСТИРОВАНИЕ ЛОТА #{test_num}")
            print(f"UUID: {test_lot_uuid}")
            print(f"Название: {lot_info['name'][:50]}...")
            print(f"Адрес: {lot_info['address']}")
            print(f"{'='*60}")
            
            # Тест 1: Поиск лота по UUID
            print(f"\n1️⃣ Поиск лота по UUID...")
            lot = find_lot_by_uuid(test_lot_uuid)
            if lot:
                print(f"✅ Лот найден:")
                print(f"   Название: {lot.name}")
                print(f"   Адрес: {lot.address}")
                print(f"   Площадь: {lot.area} м²")
            else:
                print(f"❌ Лот с UUID {test_lot_uuid} не найден")
                continue  # Переходим к следующему лоту
            
            # Тест 2: Поиск аналогов в Google Sheets
            print(f"\n2️⃣ Поиск аналогов в Google Sheets...")
            analogs = find_analogs_in_sheets(test_lot_uuid)
            if analogs:
                print(f"✅ Найдено {len(analogs)} аналогов в Google Sheets:")
                for i, analog in enumerate(analogs[:3], 1):  # Показываем первые 3
                    print(f"   {i}. {analog.address} - {analog.price:,.0f} ₽ ({analog.area} м²)")
                if len(analogs) > 3:
                    print(f"   ... и еще {len(analogs) - 3} аналогов")
            else:
                print(f"⚠️  Аналоги в Google Sheets не найдены")
            
            # Тест 3: Полный поиск аналогов через сервис
            print(f"\n3️⃣ Полный поиск аналогов (с fallback)...")
            all_analogs = await AnalogSearchService.find_analogs_for_lot_uuid(test_lot_uuid)
            if all_analogs:
                print(f"✅ Найдено {len(all_analogs)} аналогов через сервис:")
                for i, analog in enumerate(all_analogs[:3], 1):  # Показываем первые 3
                    distance_info = f" ({analog.distance_to_lot:.1f} км)" if analog.distance_to_lot > 0 else ""
                    print(f"   {i}. {analog.address} - {analog.price:,.0f} ₽ ({analog.area} м²){distance_info}")
                if len(all_analogs) > 3:
                    print(f"   ... и еще {len(all_analogs) - 3} аналогов")
                
                # Если нашли аналоги, останавливаемся
                print(f"\n🎉 Успешно найдены аналоги для лота #{test_num}!")
                break
            else:
                print(f"⚠️  Аналоги через сервис не найдены")
                
            # Если это не последний лот, продолжаем
            if test_num < len(test_lots):
                print(f"\n➡️  Переходим к следующему лоту...")
        
        print(f"\n✅ Тестирование завершено!")
        
    except Exception as e:
        logger.error(f"Ошибка при тестировании: {e}")
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(test_analog_search_by_uuid())
