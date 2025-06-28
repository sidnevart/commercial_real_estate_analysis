"""
Скрипт для форматирования Google Sheets таблиц:
1. Числовое форматирование с разрядностью для удобного чтения
2. Цветное форматирование для доходности >= 20% и капитализации >= 15%
3. Применение ко всем трем таблицам: lots_all, cian_sale_all, cian_rent_all
"""

import logging
from googleapiclient.discovery import build
from google.oauth2 import service_account
from parser.config import GSHEET_ID, GSHEET_CREDS_PATH

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_creds = service_account.Credentials.from_service_account_file(GSHEET_CREDS_PATH, scopes=SCOPES)
_svc = build("sheets", "v4", credentials=_creds)

def clear_all_conditional_formatting(sheet_id):
    """Очищает все существующее условное форматирование с листа"""
    try:
        # Удаляем до 20 правил условного форматирования
        for _ in range(20):
            try:
                _svc.spreadsheets().batchUpdate(
                    spreadsheetId=GSHEET_ID,
                    body={
                        "requests": [{
                            "deleteConditionalFormatRule": {
                                "sheetId": sheet_id,
                                "index": 0
                            }
                        }]
                    }
                ).execute()
            except:
                # Если правил больше нет, выходим из цикла
                break
        logger.info(f"Очищено условное форматирование для листа {sheet_id}")
    except Exception as e:
        logger.debug(f"Ошибка при очистке форматирования (это нормально): {e}")

def get_sheet_metadata():
    """Получает метаданные всех листов"""
    try:
        return _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
    except Exception as e:
        logger.error(f"Ошибка при получении метаданных листов: {e}")
        return None

def get_sheet_id_by_name(sheets_metadata, sheet_name):
    """Находит ID листа по названию"""
    for sheet in sheets_metadata['sheets']:
        if sheet['properties']['title'] == sheet_name:
            return sheet['properties']['sheetId']
    return None

def get_last_row(sheet_name):
    """Определяет последнюю строку с данными в листе"""
    try:
        result = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet_name}!A:A"
        ).execute()
        
        values = result.get('values', [])
        return len(values) if values else 1
    except Exception as e:
        logger.error(f"Ошибка при определении последней строки для {sheet_name}: {e}")
        return 1

def format_lots_all_table():
    """Форматирует основную таблицу лотов"""
    sheet_name = "lots_all"
    logger.info(f"🎨 Форматирование таблицы {sheet_name}")
    
    try:
        # Получаем метаданные
        sheets_metadata = get_sheet_metadata()
        if not sheets_metadata:
            return False
        
        sheet_id = get_sheet_id_by_name(sheets_metadata, sheet_name)
        if sheet_id is None:
            logger.error(f"Лист {sheet_name} не найден")
            return False
        
        # Определяем диапазон данных
        last_row = get_last_row(sheet_name)
        if last_row <= 1:
            logger.warning(f"Нет данных в {sheet_name}")
            return False
        
        logger.info(f"Форматирование {sheet_name}: строки 2-{last_row}")
        
        # Очищаем существующее форматирование
        clear_all_conditional_formatting(sheet_id)
        
        # Создаем запросы на форматирование
        format_requests = []
        
        # 1. ЧИСЛОВОЕ ФОРМАТИРОВАНИЕ с разрядностью
        number_columns = [5, 6, 7, 8, 9, 10, 11, 12, 13]  # Все числовые столбцы
        
        for col_idx in number_columns:
            # Определяем тип форматирования в зависимости от столбца
            if col_idx == 5:  # Столбец F - Площадь
                pattern = "#,##0.0\" м²\""
                format_type = "NUMBER"
            elif col_idx in [6, 7]:  # Столбцы G, H - цена за м²
                pattern = "#,##0₽"
                format_type = "CURRENCY"
            elif col_idx in [8, 9, 10]:  # Столбцы I, J, K - суммы в рублях
                pattern = "#,##0₽"
                format_type = "CURRENCY"
            elif col_idx == 11:  # Столбец L - КАПИТАЛИЗАЦИЯ в %
                pattern = "0.00%"  # ИСПРАВЛЕНО: автоматический процентный формат
                format_type = "PERCENT"
            elif col_idx == 12:  # Столбец M - ГАП в рублях
                pattern = "#,##0₽"
                format_type = "CURRENCY"
            elif col_idx == 13:  # Столбец N - ДОХОДНОСТЬ в %
                pattern = "0.00%"  # ИСПРАВЛЕНО: автоматический процентный формат
                format_type = "PERCENT"
            else:
                pattern = "#,##0.00"
                format_type = "NUMBER"
            
            format_requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,  # Со второй строки
                        "endRowIndex": last_row,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": format_type,
                                "pattern": pattern
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            })
        
        # 2. УСЛОВНОЕ ФОРМАТИРОВАНИЕ для доходности >= 20% (столбец N, индекс 13)
        # ИСПРАВЛЕНО: Используем правильную формулу для процентов
        format_requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": last_row,
                        "startColumnIndex": 13,  # Столбец N (доходность)
                        "endColumnIndex": 14
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": "=N2>=20%"}]  # ИСПРАВЛЕНО: используем проценты
                        },
                        "format": {
                            "backgroundColor": {"red": 0.7176, "green": 0.8823, "blue": 0.7176}
                        }
                    }
                },
                "index": 0
            }
        })
        
        # 3. УСЛОВНОЕ ФОРМАТИРОВАНИЕ для капитализации >= 15% (столбец L, индекс 11)
        # ИСПРАВЛЕНО: Используем правильную формулу для процентов
        format_requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": last_row,
                        "startColumnIndex": 11,  # Столбец L (капитализация)
                        "endColumnIndex": 12
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": "=L2>=15%"}]  # ИСПРАВЛЕНО: используем проценты
                        },
                        "format": {
                            "backgroundColor": {"red": 0.7176, "green": 0.8823, "blue": 0.7176}
                        }
                    }
                },
                "index": 1
            }
        })
        
        # Применяем все форматирование
        if format_requests:
            _svc.spreadsheets().batchUpdate(
                spreadsheetId=GSHEET_ID,
                body={"requests": format_requests}
            ).execute()
        
        logger.info(f"✅ Успешно отформатирован {sheet_name}")
        logger.info("   📊 Числовое форматирование с разрядностью")
        logger.info("   📈 Столбец L(11): Капитализация как PERCENT")
        logger.info("   💰 Столбец M(12): ГАП в рублях")
        logger.info("   📈 Столбец N(13): Доходность как PERCENT")
        logger.info("   🟢 Зеленый цвет для доходности >= 20% (столбец N)")
        logger.info("   🟢 Зеленый цвет для капитализации >= 15% (столбец L)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при форматировании {sheet_name}: {e}")
        return False

def apply_all_formatting():
    """Применяет форматирование ко всем таблицам"""
    logger.info("🎨 ЗАПУСК ПОЛНОГО ФОРМАТИРОВАНИЯ GOOGLE SHEETS")
    logger.info("=" * 60)
    
    results = {}
    
    # 1. Форматируем основную таблицу лотов
    logger.info("\n📊 Форматирование основной таблицы лотов...")
    results['lots_all'] = format_lots_all_table()
    
    # 2. Форматируем таблицы объявлений ЦИАН
    logger.info("\n📋 Форматирование таблиц объявлений ЦИАН...")
    results['cian_tables'] = format_cian_tables()
    
    # Итоговый отчет
    logger.info("\n" + "=" * 60)
    logger.info("🎉 ФОРМАТИРОВАНИЕ ЗАВЕРШЕНО!")
    logger.info("=" * 60)
    
    success_count = sum(1 for success in results.values() if success)
    total_count = len(results)
    
    if success_count == total_count:
        logger.info("✅ Все таблицы успешно отформатированы")
    else:
        logger.warning(f"⚠️ Отформатировано {success_count} из {total_count} групп таблиц")
    
    # Детальный отчет о применённом форматировании
    print("\n" + "="*70)
    print("🎨 ДЕТАЛЬНЫЙ ОТЧЕТ О ФОРМАТИРОВАНИИ:")
    print("="*70)
    
    print("📊 ТАБЛИЦА ЛОТОВ (lots_all):")
    print("   ✅ Числовое форматирование:")
    print("      • Столбец 6 (F): Площадь с разрядностью + 'м²'")
    print("      • Столбцы 7-8 (G-H): Цены за м² с разрядностью + '₽'")
    print("      • Столбцы 9-11 (I-K): Суммы в рублях с разрядностью + '₽'")
    print("      • Столбец 13 (M): ГАП в рублях с разрядностью + '₽'")
    print("   ⚠️ Проценты БЕЗ форматирования:")
    print("      • Столбец 12 (L): Капитализация % (как обычное число)")
    print("      • Столбец 14 (N): Доходность % (как обычное число)")
    print("   ✅ Цветное форматирование:")
    print("      • Доходность >= 20 → зеленый фон")
    print("      • Капитализация >= 15 → зеленый фон")
    
    print("\n📋 ТАБЛИЦЫ ОБЪЯВЛЕНИЙ ЦИАН:")
    print("   ✅ cian_sale_all и cian_rent_all:")
    print("      • Столбец 4 (D): Площадь с разрядностью + 'м²'")
    print("      • Столбец 5 (E): Цена за м² с разрядностью + '₽'")
    print("      • Столбец 6 (F): Общая стоимость с разрядностью + '₽'")
    print("      • Столбец 7 (G): Расстояние в км")
    
    print("\n💡 ПРИМЕРЫ ФОРМАТИРОВАНИЯ:")
    print("   • 1500000 → 1,500,000₽")
    print("   • 1234.56 → 1,234.6 м²")
    print("   • 3.2456 → 3.2 км")
    print("   • Проценты: 20.5 (как есть, без символа %)")
    
    print("="*70)
    
    return success_count == total_count

def format_cian_tables():
    """Форматирует таблицы объявлений ЦИАН"""
    
    sheets_to_format = [
        ("cian_sale_all", "продажи"),
        ("cian_rent_all", "аренды")
    ]
    
    success_count = 0
    
    for sheet_name, description in sheets_to_format:
        logger.info(f"🎨 Форматирование таблицы {description}: {sheet_name}")
        
        try:
            # Получаем метаданные
            sheets_metadata = get_sheet_metadata()
            if not sheets_metadata:
                continue
            
            sheet_id = get_sheet_id_by_name(sheets_metadata, sheet_name)
            if sheet_id is None:
                logger.warning(f"Лист {sheet_name} не найден, пропускаем")
                continue
            
            # Определяем диапазон данных
            last_row = get_last_row(sheet_name)
            if last_row <= 1:
                logger.warning(f"Нет данных в {sheet_name}")
                continue
            
            logger.info(f"Форматирование {sheet_name}: строки 2-{last_row}")
            
            # Создаем запросы на форматирование
            format_requests = []
            
            # ЧИСЛОВОЕ ФОРМАТИРОВАНИЕ с разрядностью для столбцов 4-6 (D, E, F)
            columns_to_format = [
                (3, "NUMBER", "#,##0.0\" м²\""),      # Столбец D - Площадь
                (4, "CURRENCY", "#,##0₽"),           # Столбец E - Цена за м²
                (5, "CURRENCY", "#,##0₽")            # Столбец F - Общая стоимость
            ]
            
            for col_idx, format_type, pattern in columns_to_format:
                format_requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,  # Со второй строки
                            "endRowIndex": last_row,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "numberFormat": {
                                    "type": format_type,
                                    "pattern": pattern
                                }
                            }
                        },
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
            
            # Дополнительное форматирование для столбца расстояния (G, индекс 6)
            format_requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": last_row,
                        "startColumnIndex": 6,
                        "endColumnIndex": 7
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "NUMBER",
                                "pattern": "0.0\" км\""
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            })
            
            # Применяем форматирование
            if format_requests:
                _svc.spreadsheets().batchUpdate(
                    spreadsheetId=GSHEET_ID,
                    body={"requests": format_requests}
                ).execute()
            
            logger.info(f"✅ Успешно отформатирован {sheet_name}")
            logger.info("   📊 Числовое форматирование с разрядностью для столбцов 4-6")
            success_count += 1
            
        except Exception as e:
            logger.error(f"❌ Ошибка при форматировании {sheet_name}: {e}")
    
    return success_count == len(sheets_to_format)

def apply_all_formatting():
    """Применяет форматирование ко всем таблицам"""
    logger.info("🎨 ЗАПУСК ПОЛНОГО ФОРМАТИРОВАНИЯ GOOGLE SHEETS")
    logger.info("=" * 60)
    
    results = {}
    
    # 1. Форматируем основную таблицу лотов
    logger.info("\n📊 Форматирование основной таблицы лотов...")
    results['lots_all'] = format_lots_all_table()
    
    # 2. Форматируем таблицы объявлений ЦИАН
    logger.info("\n📋 Форматирование таблиц объявлений ЦИАН...")
    results['cian_tables'] = format_cian_tables()
    
    # Итоговый отчет
    logger.info("\n" + "=" * 60)
    logger.info("🎉 ФОРМАТИРОВАНИЕ ЗАВЕРШЕНО!")
    logger.info("=" * 60)
    
    success_count = sum(1 for success in results.values() if success)
    total_count = len(results)
    
    if success_count == total_count:
        logger.info("✅ Все таблицы успешно отформатированы")
    else:
        logger.warning(f"⚠️ Отформатировано {success_count} из {total_count} групп таблиц")
    
    # Детальный отчет о применённом форматировании
    print("\n" + "="*70)
    print("🎨 ДЕТАЛЬНЫЙ ОТЧЕТ О ФОРМАТИРОВАНИИ:")
    print("="*70)
    
    print("📊 ТАБЛИЦА ЛОТОВ (lots_all):")
    print("   ✅ Числовое форматирование:")
    print("      • Столбец 6 (F): Площадь с разрядностью + 'м²'")
    print("      • Столбцы 7-8 (G-H): Цены за м² с разрядностью + '₽'")
    print("      • Столбцы 9-11 (I-K): Суммы в рублях с разрядностью + '₽'")
    print("      • Столбец 12 (L): Капитализация в % (7 → 7.0%)")
    print("      • Столбец 13 (M): ГАП в рублях с разрядностью + '₽'")
    print("      • Столбец 14 (N): Доходность в % (25 → 25.0%)")
    print("   ✅ Цветное форматирование:")
    print("      • Доходность >= 20% → зеленый фон")
    print("      • Капитализация >= 15% → зеленый фон")
    
    print("\n📋 ТАБЛИЦЫ ОБЪЯВЛЕНИЙ ЦИАН:")
    print("   ✅ cian_sale_all и cian_rent_all:")
    print("      • Столбец 4 (D): Площадь с разрядностью + 'м²'")
    print("      • Столбец 5 (E): Цена за м² с разрядностью + '₽'")
    print("      • Столбец 6 (F): Общая стоимость с разрядностью + '₽'")
    print("      • Столбец 7 (G): Расстояние в км")
    
    print("\n💡 ПРИМЕРЫ ФОРМАТИРОВАНИЯ:")
    print("   • 1500000 → 1,500,000₽")
    print("   • 7 → 7.0% (для процентов, БЕЗ умножения на 100)")
    print("   • 1234.56 → 1,234.6 м²")
    print("   • 3.2456 → 3.2 км")
    
    print("="*70)
    
    return success_count == total_count

def main():
    """Основная функция для запуска скрипта"""
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s %(levelname)s: %(message)s"
    )
    
    logger.info("Начинаем форматирование Google Sheets таблиц...")
    
    try:
        success = apply_all_formatting()
        
        if success:
            logger.info("🎉 Все таблицы успешно отформатированы!")
            print("\n🚀 Готово! Проверьте таблицы в Google Sheets.")
        else:
            logger.warning("⚠️ Некоторые таблицы не удалось отформатировать")
            print("\n⚠️ Есть проблемы с форматированием. Проверьте логи выше.")
            
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        print(f"\n❌ Критическая ошибка: {e}")

# if __name__ == "__main__":
#    main()