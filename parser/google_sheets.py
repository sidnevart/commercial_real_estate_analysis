from __future__ import annotations
import logging
from typing import List, Dict
from googleapiclient.discovery import build
from google.oauth2 import service_account
from core.models import Lot, Offer
from core.config import CONFIG
from .config import GSHEET_ID, GSHEET_CREDS_PATH

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

_creds = service_account.Credentials.from_service_account_file(GSHEET_CREDS_PATH, scopes=SCOPES)
_svc = build("sheets", "v4", credentials=_creds)


def _append(range_: str, values: List[List]):
    """Добавить данные в таблицу с логированием."""
    if not values:
        logger.warning(f"Пытаемся добавить пустой список в {range_}")
        return
    
    try:
        logger.info(f"Добавление {len(values)} строк в диапазон {range_}")
        response = _svc.spreadsheets().values().append(
            spreadsheetId=GSHEET_ID,
            range=range_, 
            valueInputOption="USER_ENTERED", 
            body={"values": values}
        ).execute()
        logger.info(f"Результат добавления: {response.get('updates').get('updatedCells')} ячеек обновлено")
        return response
    except Exception as e:
        logger.error(f"Ошибка при добавлении данных в {range_}: {e}", exc_info=True)
        raise

def _format_cells(sheet_id, start_row, end_row, column, condition, color):
    """Apply conditional formatting to a range of cells."""
    # Соответствие пользовательских условий и API-констант
    condition_mapping = {
        "NUMBER_LESS_THAN_OR_EQUAL": "NUMBER_LESS_THAN_EQ",
        "NUMBER_GREATER_THAN_OR_EQUAL": "NUMBER_GREATER_THAN_EQ",
        "NUMBER_LESS": "NUMBER_LESS",
        "NUMBER_GREATER": "NUMBER_GREATER",
        "NUMBER_EQUAL": "NUMBER_EQ"
    }
    
    # Парсим условие и значение из строки
    parts = condition.split(' ', 1)
    condition_type = parts[0]
    condition_value = parts[1] if len(parts) > 1 else None
    
    # Проверяем, нужно ли преобразовать условие в API-совместимый формат
    if condition_type in condition_mapping:
        api_condition_type = condition_mapping[condition_type]
    else:
        api_condition_type = condition_type
    
    logger.info(f"Применяем форматирование: {condition_type} -> {api_condition_type} со значением {condition_value}")
    
    rule = {
        "ranges": [{
            "sheetId": sheet_id,
            "startRowIndex": start_row,
            "endRowIndex": end_row,
            "startColumnIndex": column,
            "endColumnIndex": column + 1
        }],
        "booleanRule": {
            "condition": {"type": api_condition_type},
            "format": {"backgroundColor": color}
        }
    }
    
    # Добавляем значение только если оно есть
    if condition_value:
        rule["booleanRule"]["condition"]["values"] = [{"userEnteredValue": condition_value}]
    
    request = {"addConditionalFormatRule": {"rule": rule, "index": 0}}
    body = {"requests": [request]}
    
    try:
        _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        logger.info(f"Условное форматирование успешно применено: {api_condition_type}")
    except Exception as e:
        logger.error(f"Ошибка при применении условного форматирования: {e}")
        
        # Последовательно пробуем альтернативные варианты форматирования
        alternative_conditions = ["NUMBER_LESS_THAN_EQ", "NUMBER_LESS", "NUMBER_GREATER_THAN_EQ", "NUMBER_GREATER", "NUMBER_EQ"]
        
        for alt_condition in alternative_conditions:
            if alt_condition == api_condition_type:
                continue  # Пропускаем уже опробованный вариант
                
            try:
                rule["booleanRule"]["condition"]["type"] = alt_condition
                _svc.spreadsheets().batchUpdate(
                    spreadsheetId=GSHEET_ID, 
                    body={"requests": [{"addConditionalFormatRule": {"rule": rule, "index": 0}}]}
                ).execute()
                logger.info(f"Успешно применено альтернативное форматирование: {alt_condition}")
                break
            except Exception as e2:
                logger.debug(f"Не удалось применить форматирование {alt_condition}: {e2}")

# public API ------------------------------------------------------
def push_lots(lots: List[Lot], sheet_name: str = "lots_all"):
    """Push full lot data without overwriting existing lots."""
    logger.info(f"Начинаем выгрузку {len(lots)} лотов в Google Sheets на лист {sheet_name}")
    
    if not lots:
        logger.warning("Пустой список лотов, нечего выгружать")
        return
    
    try:
        # Подготавливаем заголовки таблицы
        headers = [
            "№", "Название", "Адрес", "Категория", "Площадь, м²", 
            "Текущая ставка, ₽/м²", "Рыночная ставка, ₽/м²", 
            # ... остальные заголовки ...
            "URL аукциона", "UUID (technical)", "Категория размера", "Наличие подвала", "Верхний этаж"
        ]
        
        # Проверка существования листа
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheet_exists = any(sheet['properties']['title'] == sheet_name for sheet in sheets_metadata['sheets'])
        
        if not sheet_exists:
            # Если лист не существует, создаем его с заголовками
            logger.info(f"Лист '{sheet_name}' не найден. Создаем новый лист.")
            body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_name,
                            'gridProperties': {
                                'frozenRowCount': 1
                            }
                        }
                    }
                }]
            }
            _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
            
            # Вставляем заголовки в новый лист
            _append(sheet_name, [headers])
        
        # Получаем существующие данные для проверки дубликатов
        existing_data = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet_name}!AG2:AG1000"  # UUID техническое поле
        ).execute()
        
        existing_uuids = set()
        if 'values' in existing_data:
            # Извлекаем существующие UUID
            for row in existing_data.get('values', []):
                if row and row[0]:  # Если есть UUID
                    existing_uuids.add(row[0])
        
        # Фильтруем лоты, оставляя только новые
        new_lots = [lot for lot in lots if str(lot.uuid) not in existing_uuids]
        
        if not new_lots:
            logger.info("Все лоты уже существуют в таблице, нечего добавлять")
            return
            
        logger.info(f"Добавление {len(new_lots)} новых лотов из {len(lots)}")
        
        # Получаем текущее количество строк для нумерации
        count_response = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet_name}!A:A"
        ).execute()
        
        start_row = len(count_response.get('values', [])) + 1 if 'values' in count_response else 2
        
        # Подготавливаем строки данных для новых лотов
        rows = []
        for i, lot in enumerate(new_lots):
            try:
                # Та же логика формирования строки как и раньше...
                category = ""
                size_category = ""
                has_basement = "Нет"
                is_top_floor = "Нет"
                
                if hasattr(lot, 'classification') and lot.classification is not None:
                    category = lot.classification.category
                    size_category = lot.classification.size_category
                    has_basement = "Да" if lot.classification.has_basement else "Нет"
                    is_top_floor = "Да" if lot.classification.is_top_floor else "Нет"
                
                row = [
                    start_row + i - 1,  # № строки с учетом смещения
                    lot.name,  # Название
                    lot.address,  # Адрес
                    # ... остальные поля ...
                    str(lot.uuid),  # UUID (technical)
                    size_category,  # Категория размера
                    has_basement,  # Наличие подвала
                    is_top_floor  # Верхний этаж
                ]
                rows.append(row)
            except Exception as e:
                logger.error(f"Ошибка при подготовке данных лота {lot.id}: {e}")
        
        if rows:
            # Добавляем новые строки (без перезаписи существующих)
            _append(sheet_name, rows)
            logger.info(f"Успешно добавлено {len(rows)} новых лотов в таблицу")
        
        # Применяем форматирование
        try:
            # Получаем ID листа
            sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
            sheet_id = next((sheet['properties']['sheetId'] 
                            for sheet in sheets_metadata['sheets'] 
                            if sheet['properties']['title'] == sheet_name),
                            None)
            
            if not sheet_id:
                logger.warning(f"Не удалось найти ID листа '{sheet_name}' для форматирования")
                return
                
            # Удаляем предыдущее условное форматирование
            try:
                # Сначала получаем информацию о существующих правилах форматирования
                formatting_response = _svc.spreadsheets().get(
                    spreadsheetId=GSHEET_ID,
                    ranges=[sheet_name],
                    fields="sheets(sheetId,conditionalFormats)"
                ).execute()
                
                # Проверяем, есть ли правила форматирования
                sheet_data = formatting_response.get('sheets', [])
                if sheet_data and 'conditionalFormats' in sheet_data[0]:
                    existing_rules = len(sheet_data[0]['conditionalFormats'])
                    logger.info(f"Найдено {existing_rules} правил форматирования для удаления")
                    
                    # Создаем запросы на удаление всех правил, начиная с последнего
                    delete_requests = []
                    for i in range(existing_rules - 1, -1, -1):  # В обратном порядке
                        delete_requests.append({
                            "deleteConditionalFormatRule": {
                                "sheetId": sheet_id,
                                "index": i
                            }
                        })
                    
                    if delete_requests:
                        # Выполняем удаление всех правил одновременно
                        _svc.spreadsheets().batchUpdate(
                            spreadsheetId=GSHEET_ID, 
                            body={"requests": delete_requests}
                        ).execute()
                        logger.info(f"Успешно удалено {len(delete_requests)} правил форматирования")
                else:
                    logger.info(f"Не найдено правил форматирования для листа '{sheet_name}'")
                    
            except Exception as e:
                logger.info(f"Пропускаем удаление форматирования: {str(e)}")
                
            # Форматирование только для доходности
            yield_threshold = CONFIG.get("market_yield_threshold", 10)
            
            _format_cells(
                sheet_id=sheet_id,
                start_row=1,
                end_row=len(rows),
                column=13,  # Колонка "Доходность (рыночная), %"
                condition=f"NUMBER_GREATER {yield_threshold}",
                color={"red": 0.7176, "green": 0.8823, "blue": 0.7176}  # Светло-зеленый
            )
            
            logger.info("Условное форматирование успешно применено")
            
            # Автоматическая настройка ширины колонок
            auto_resize_request = {
                "requests": [
                    {
                        "autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": len(headers)
                            }
                        }
                    }
                ]
            }
            _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=auto_resize_request).execute()
            logger.info("Ширина колонок автоматически настроена")
            
        except Exception as e:
            logger.error(f"Ошибка при форматировании таблицы: {e}")
    
    except Exception as e:
        logger.error(f"Ошибка при выгрузке лотов в Google Sheets: {e}", exc_info=True)
        raise


def push_offers(sheet: str, offers: List[Offer]):
    """Push offer data without overwriting existing offers."""
    if not offers:
        logger.warning(f"Попытка отправить пустой список объявлений в лист {sheet}")
        return
    
    logger.info(f"Начинаем выгрузку {len(offers)} объявлений в лист {sheet}")
    
    try:
        # Фильтруем объявления с некорректными значениями
        valid_offers = [o for o in offers if o.price > 0 and o.area > 0]
        
        if len(valid_offers) != len(offers):
            logger.warning(f"Отфильтровано {len(offers) - len(valid_offers)} объявлений с нулевыми значениями")
        
        if not valid_offers:
            logger.warning(f"После фильтрации не осталось корректных объявлений для записи в {sheet}")
            return
            
        # Заголовки таблицы
        headers = [
            "№", "Адрес", "Район", "Площадь, м²", "Цена за м²", 
            "Общая стоимость, ₽", "Расстояние, км", "Ссылка", "UUID лота", "ID объявления"
        ]
        
        # Проверяем существование листа
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheet_exists = any(s['properties']['title'] == sheet for s in sheets_metadata['sheets'])
        
        if not sheet_exists:
            # Если лист не существует, создаем его
            logger.info(f"Лист '{sheet}' не найден. Создаем новый лист.")
            body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet,
                            'gridProperties': {
                                'frozenRowCount': 1
                            }
                        }
                    }
                }]
            }
            _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
            
            # Вставляем заголовки
            _append(sheet, [headers])
        
        # Получаем существующие ID объявлений
        existing_data = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet}!J2:J10000"  # ID объявления
        ).execute()
        
        existing_ids = set()
        if 'values' in existing_data:
            for row in existing_data.get('values', []):
                if row and row[0]:
                    existing_ids.add(row[0])
        
        # Фильтруем только новые объявления
        new_offers = [o for o in valid_offers if o.id not in existing_ids]
        
        if not new_offers:
            logger.info(f"Все объявления уже существуют в таблице {sheet}, нечего добавлять")
            return
            
        logger.info(f"Добавление {len(new_offers)} новых объявлений из {len(valid_offers)}")
        
        # Получаем текущее количество строк
        count_response = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet}!A:A"
        ).execute()
        
        start_row = len(count_response.get('values', [])) + 1 if 'values' in count_response else 2
        
        # Подготавливаем строки данных
        rows = []
        for i, offer in enumerate(new_offers):
            # Рассчитываем цену за квадратный метр
            price_per_sqm = offer.price / offer.area if offer.area > 0 else 0
            
            # Получаем расстояние до лота, если есть
            distance_to_lot = getattr(offer, 'distance_to_lot', '')
            
            # Формируем строку данных
            row = [
                start_row + i - 1,  # № с учетом смещения
                offer.address,  # Адрес
                offer.district if hasattr(offer, 'district') and offer.district else "",  # Район
                offer.area,  # Площадь, м²
                price_per_sqm,  # Цена за м²
                offer.price,  # Общая стоимость, ₽
                distance_to_lot,  # Расстояние, км
                offer.url,  # Ссылка
                str(offer.lot_uuid),  # UUID лота
                offer.id  # ID объявления
            ]
            rows.append(row)
        
        # Добавляем новые данные (без перезаписи)
        if rows:
            _append(sheet, rows)
            logger.info(f"Успешно добавлено {len(rows)} новых объявлений в лист {sheet}")
        
        # Применяем форматирование
        try:
            # Получаем ID листа для форматирования
            sheet_id = next((s['properties']['sheetId'] for s in sheets_metadata['sheets'] 
                          if s['properties']['title'] == sheet), None)
                          
            if sheet_id:
                # Автоматическая настройка ширины колонок
                auto_resize_request = {
                    "requests": [
                        {
                            "autoResizeDimensions": {
                                "dimensions": {
                                    "sheetId": sheet_id,
                                    "dimension": "COLUMNS",
                                    "startIndex": 0,
                                    "endIndex": len(headers)
                                }
                            }
                        }
                    ]
                }
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=auto_resize_request).execute()
                logger.info(f"Ширина колонок для листа {sheet} автоматически настроена")
        except Exception as e:
            logger.error(f"Ошибка при форматировании листа {sheet}: {e}")
    
    except Exception as e:
        logger.error(f"Ошибка при выгрузке объявлений в Google Sheets: {e}", exc_info=True)

   
def push_district_stats(district_stats: Dict[str, int]):
    """Push district offer count statistics to a separate sheet."""
    if not district_stats:
        logger.warning("Попытка отправить пустую статистику по районам")
        district_stats = {"Москва": 0}  # Заглушка, чтобы не было пустого списка
    
    rows = []
    for district, count in district_stats.items():
        rows.append([district, count])
    
    if not rows:
        logger.warning("После обработки получен пустой список районов")
        rows = [["Москва", 0]]  # Вторая защита от пустого списка
        
    _append("district_stats", rows)

# Вспомогательная функция для форматирования дат
def format_date(dt):
    """Форматирует дату в строковый формат для Excel"""
    return dt.strftime('%Y-%m-%d %H:%M:%S') if dt else ""