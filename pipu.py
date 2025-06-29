#!/usr/bin/env python3
"""
Диагностика парсинга площади из Google Sheets
"""
from parser.google_sheets import _svc, GSHEET_ID

def diagnose_area_parsing():
    """Диагностирует парсинг площади"""
    print("🔍 ДИАГНОСТИКА ПАРСИНГА ПЛОЩАДИ")
    print("=" * 50)
    
    # Ищем тестовый UUID
    test_uuid = "fc6c1435-53b1-489e-8437-abf4838f8b8a"
    
    try:
        # Читаем данные из таблицы
        result = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range="lots_all!A1:AC10"  # Первые 10 строк включая заголовки
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print("❌ Нет данных в таблице")
            return
        
        # Показываем заголовки
        headers = values[0] if len(values) > 0 else []
        print(f"📋 Заголовки колонок:")
        for i, header in enumerate(headers):
            print(f"   {i:2d}. {header}")
        
        # Ищем нашу строку
        print(f"\n🔍 Ищем строку с UUID: {test_uuid}")
        
        for row_idx, row in enumerate(values[1:], 1):  # Пропускаем заголовки
            if len(row) > 17 and row[17] == test_uuid:
                print(f"✅ Найдена строка {row_idx}:")
                
                # Показываем интересные нам колонки
                important_columns = [0, 1, 2, 5, 8, 17]  # id, name, address, area, price, uuid
                
                for col_idx in important_columns:
                    if col_idx < len(row):
                        col_name = headers[col_idx] if col_idx < len(headers) else f"Column_{col_idx}"
                        value = row[col_idx]
                        print(f"   {col_idx:2d}. {col_name}: '{value}'")
                
                # Специально проверяем площадь
                print(f"\n🔍 АНАЛИЗ ПЛОЩАДИ:")
                if len(row) > 5:
                    area_raw = row[5]
                    print(f"   • Сырое значение: '{area_raw}'")
                    print(f"   • Тип: {type(area_raw)}")
                    print(f"   • Длина: {len(area_raw)}")
                    
                    # Пробуем разные способы парсинга
                    print(f"   • Попытки парсинга:")
                    
                    try:
                        # Способ 1: прямое преобразование
                        area1 = float(area_raw)
                        print(f"     1. float(area_raw): {area1}")
                    except:
                        print(f"     1. float(area_raw): ОШИБКА")
                    
                    try:
                        # Способ 2: убираем ' м²'
                        area2 = float(area_raw.replace(' м²', ''))
                        print(f"     2. После удаления ' м²': {area2}")
                    except:
                        print(f"     2. После удаления ' м²': ОШИБКА")
                    
                    try:
                        # Способ 3: убираем все нечисловые символы кроме точки и запятой
                        import re
                        area_clean = re.sub(r'[^0-9.,]', '', area_raw)
                        area_clean = area_clean.replace(',', '.')
                        area3 = float(area_clean) if area_clean else 0
                        print(f"     3. Очищенное значение '{area_clean}': {area3}")
                    except:
                        print(f"     3. Очищенное значение: ОШИБКА")
                
                return
        
        print(f"❌ Строка с UUID {test_uuid} не найдена")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    diagnose_area_parsing()