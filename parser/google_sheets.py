from __future__ import annotations
import logging
from typing import Any, List, Dict, Optional
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
    """Добавляет лоты в таблицу без перезаписи существующих данных."""
    logger.info(f"Начинаем выгрузку {len(lots)} лотов в Google Sheets на лист {sheet_name}")
    
    if not lots:
        logger.warning("Пустой список лотов, нечего выгружать")
        return
    
    try:
        # Проверка существования листа
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheet_exists = any(sheet['properties']['title'] == sheet_name for sheet in sheets_metadata['sheets'])
        
        # Заголовки таблицы
        headers = [
            "№", "Название", "Адрес", "Район", "Категория", "Площадь, м²", 
            "Текущая ставка, ₽/м²", "Рыночная ставка, ₽/м²", "Общая стоимость (торги), ₽", 
            "Общая стоимость (рыночная), ₽", "Капитализация, ₽", "Капитализация, %",
            "ГАП (рыночный), ₽/мес", "Доходность (рыночная), %", "Аукцион", "Документ", 
            "URL аукциона", "UUID (technical)", "Категория размера", "Наличие подвала", "Верхний этаж"
        ]
        
        if not sheet_exists:
            # Если лист не существует, создаем его и добавляем заголовки
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
            _append(sheet_name, [headers])
        
        # Запрашиваем существующие данные для проверки на дубликаты
        existing_data = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet_name}!R2:R10000"  # Колонка с UUID 
        ).execute()
        
        existing_uuids = set()
        if 'values' in existing_data:
            for row in existing_data.get('values', []):
                if row and row[0]:
                    existing_uuids.add(str(row[0]))
                    
        # Фильтруем лоты, оставляя только новые
        new_lots = [lot for lot in lots if str(lot.uuid) not in existing_uuids]
        
        if not new_lots:
            logger.info("Все лоты уже существуют в таблице, добавление не требуется")
            return
        
        logger.info(f"Добавление {len(new_lots)} новых лотов из {len(lots)} предоставленных")
        
        # Получаем текущий размер таблицы для определения номеров строк
        range_data = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet_name}!A:A"
        ).execute()
        
        next_row_number = len(range_data.get('values', [])) + 1 if 'values' in range_data else 2
        
        # Подготавливаем данные лотов
        rows = []
        for i, lot in enumerate(new_lots, start=1):
            try:
                # Извлекаем классификационные данные
                category = ""
                size_category = ""
                has_basement = "Нет"
                is_top_floor = "Нет"
                
                if hasattr(lot, 'classification') and lot.classification is not None:
                    category = lot.classification.category
                    size_category = lot.classification.size_category
                    has_basement = "Да" if lot.classification.has_basement else "Нет"
                    is_top_floor = "Да" if lot.classification.is_top_floor else "Нет"
                
                # Подготовка данных для текущего лота
                row = [
                    next_row_number + i - 1,  # № с учетом смещения
                    lot.name,  # Название
                    lot.address,  # Адрес
                    lot.district,  # Район
                    category,  # Категория
                    lot.area,  # Площадь, м²
                    round(lot.price / lot.area if lot.area > 0 else 0),  # Текущая ставка, ₽/м²
                    round(getattr(lot, 'market_price_per_sqm', 0)),  # Рыночная ставка, ₽/м²
                    lot.price,  # Общая стоимость (торги), ₽
                    round(getattr(lot, 'market_value', 0)),  # Общая стоимость (рыночная), ₽
                    round(getattr(lot, 'capitalization_rub', 0)),  # Капитализация, ₽
                    round(getattr(lot, 'capitalization_percent', 0), 1),  # Капитализация, %
                    round(getattr(lot, 'monthly_gap', 0)),  # ГАП (рыночный), ₽/мес
                    round(getattr(lot, 'annual_yield_percent', 0), 1),  # Доходность (рыночная), %
                    lot.auction_type,  # Аукцион
                    lot.notice_number,  # Документ
                    lot.auction_url,  # URL аукциона
                    str(lot.uuid),  # UUID (technical)
                    size_category,  # Категория размера
                    has_basement,  # Наличие подвала
                    is_top_floor,  # Верхний этаж
                    getattr(lot, 'sale_offers_count', 0),  # Найдено предложений о продаже
                    getattr(lot, 'rent_offers_count', 0),
                    getattr(lot, 'filtered_sale_offers_count', 0),
                    getattr(lot, 'filtered_rent_offers_count', 0),
                    getattr(lot, 'plus_rental', 0),
                    getattr(lot, 'plus_sale', 0),
                    getattr(lot, 'plus_count', 0),
                    getattr(lot, 'status', 'unknown'), # Найдено предложений об аренде
                ]
                rows.append(row)
            except Exception as e:
                logger.error(f"Ошибка при подготовке данных лота {getattr(lot, 'id', 'unknown')}: {e}")
        
        # Добавляем новые данные в конец таблицы
        if rows:
            _append(sheet_name, rows)
            logger.info(f"Добавлено {len(rows)} лотов в лист {sheet_name}")
        
        # Форматирование таблицы (необязательно, но полезно)
        try:
            # Получаем ID листа
            sheet_id = next((sheet['properties']['sheetId'] for sheet in sheets_metadata['sheets'] 
                          if sheet['properties']['title'] == sheet_name), None)
            
            if sheet_id:
                # Автоподбор ширины колонок
                auto_resize_request = {
                    "requests": [{
                        "autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": len(headers)
                            }
                        }
                    }]
                }
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=auto_resize_request).execute()
                logger.info("Применен автоподбор ширины колонок")
                
                # Добавляем форматирование для доходности
                yield_threshold = CONFIG.get("market_yield_threshold", 10)
                last_row = next_row_number + len(rows) - 1
                _format_cells(
                    sheet_id=sheet_id,
                    start_row=1,  # С первой строки данных (после заголовков)
                    end_row=last_row + 1,  # До последней добавленной строки
                    column=13,  # Колонка "Доходность (рыночная), %"
                    condition=f"NUMBER_GREATER {yield_threshold}",
                    color={"red": 0.7176, "green": 0.8823, "blue": 0.7176}  # Светло-зеленый
                )
                logger.info("Добавлено форматирование доходности")
                
        
                
                # Форматирование для капитализации (колонка L)
                _format_cells(
                    sheet_id=sheet_id,
                    start_row=1,
                    end_row=last_row + 1,
                    column=11,  # Колонка L (капитализация, %)
                    condition="NUMBER_GREATER_THAN_OR_EQUAL 1",
                    color={"red": 0.7176, "green": 0.8823, "blue": 0.7176}
                )


                _format_cells(
                    sheet_id=sheet_id,
                    start_row=1,
                    end_row=last_row + 1,
                    column=11,  # Column L (index 11)
                    condition="NUMBER_GREATER 0",  # Changed from NUMBER_GREATER_THAN_OR_EQUAL 1 to NUMBER_GREATER 0
                    color={"red": 0.7176, "green": 0.8823, "blue": 0.7176}  # Green color
                )
                
                logging.info("Добавлено форматирование для доходности и капитализации")
        except Exception as e:
            logger.error(f"Ошибка при применении форматирования таблицы: {e}")
    
    except Exception as e:
        logger.error(f"Общая ошибка при добавлении лотов в Google Sheets: {e}", exc_info=True)


def push_offers(sheet_name: str, offers: List[Offer]):
    """Добавляет объявления в таблицу без перезаписи существующих данных."""
    logger.info(f"Начинаем выгрузку {len(offers)} объявлений в Google Sheets на лист {sheet_name}")
    
    if not offers:
        logger.warning(f"Пустой список объявлений для листа {sheet_name}")
        return
    
    try:
        # Фильтрация объявлений с некорректными данными
        valid_offers = [offer for offer in offers if offer.price > 0 and offer.area > 0]
        
        if len(valid_offers) < len(offers):
            logger.warning(f"Отфильтровано {len(offers) - len(valid_offers)} объявлений с некорректными данными")
        
        if not valid_offers:
            logger.warning(f"Нет корректных объявлений для добавления на лист {sheet_name}")
            return
        
        # Проверка существования листа
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheet_exists = any(sheet['properties']['title'] == sheet_name for sheet in sheets_metadata['sheets'])
        
        # Заголовки таблицы
        headers = [
            "№", "Адрес", "Район", "Площадь, м²", "Цена за м²", 
            "Общая стоимость, ₽", "Расстояние, км", "Ссылка", "UUID лота", "ID объявления"
        ]
        
        if not sheet_exists:
            # Если лист не существует, создаем его и добавляем заголовки
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
            _append(sheet_name, [headers])
        
        # Запрашиваем существующие данные для проверки на дубликаты
        existing_data = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet_name}!J2:J100000"  # Колонка с ID объявления
        ).execute()
        
        existing_ids = set()
        if 'values' in existing_data:
            for row in existing_data.get('values', []):
                if row and row[0]:
                    existing_ids.add(str(row[0]))
        
        # Фильтруем объявления, оставляя только новые
        new_offers = [offer for offer in valid_offers if str(offer.id) not in existing_ids]
        
        if not new_offers:
            logger.info(f"Все объявления уже существуют на листе {sheet_name}, добавление не требуется")
            return
            
        logger.info(f"Добавление {len(new_offers)} новых объявлений из {len(valid_offers)} предоставленных")
        
        # Получаем текущий размер таблицы для определения номеров строк
        range_data = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet_name}!A:A"
        ).execute()
        
        next_row_number = len(range_data.get('values', [])) + 1 if 'values' in range_data else 2
        
        # Подготавливаем данные объявлений
        rows = []
        for i, offer in enumerate(new_offers, start=1):
            # Вычисление цены за квадратный метр
            price_per_sqm = offer.price / offer.area if offer.area > 0 else 0
            logger.info(f"📍 Сохраняем адрес объявления {offer.id} [{i}/{len(new_offers)}]: '{offer.address}'")
            
            # Убедимся, что у объявления есть атрибут district
            if not hasattr(offer, 'district') or not offer.district:
                # Импортируем функцию calculate_district из parser.main
                from parser.main import calculate_district
                offer.district = calculate_district(offer.address)
                logger.info(f"Вычислен район для объявления {offer.id}: {offer.district}")
            
            row = [
                next_row_number + i - 1,  # № с учетом смещения
                offer.address,  # Адрес
                getattr(offer, 'district', ''),  # Район
                offer.area,  # Площадь, м²
                round(price_per_sqm),  # Цена за м²
                offer.price,  # Общая стоимость, ₽
                round(getattr(offer, 'distance_to_lot', 0), 1),  # Расстояние, км
                offer.url,  # Ссылка
                str(offer.lot_uuid),  # UUID лота
                str(offer.id)  # ID объявления
            ]
            rows.append(row)
        
        # Добавляем данные в конец таблицы
        _append(sheet_name, rows)
        logger.info(f"Добавлено {len(rows)} объявлений на лист {sheet_name}")
        
        # Форматирование таблицы
        try:
            # Получаем ID листа
            sheet_id = next((sheet['properties']['sheetId'] for sheet in sheets_metadata['sheets'] 
                          if sheet['properties']['title'] == sheet_name), None)
            
            if sheet_id:
                # Автоподбор ширины колонок
                auto_resize_request = {
                    "requests": [{
                        "autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": len(headers)
                            }
                        }
                    }]
                }
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=auto_resize_request).execute()
                logger.info("Применен автоподбор ширины колонок")
        except Exception as e:
            logger.error(f"Ошибка при применении форматирования таблицы: {e}")
    
    except Exception as e:
        logger.error(f"Общая ошибка при добавлении объявлений в Google Sheets: {e}", exc_info=True)


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


def push_custom_data(sheet_name: str, rows: List[List[Any]]):
    """Вспомогательная функция для выгрузки произвольных данных в указанный лист Google Sheets."""
    try:
        # Проверяем существование листа
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheet_exists = any(sheet['properties']['title'] == sheet_name for sheet in sheets_metadata['sheets'])
        
        if not sheet_exists:
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
        
        # Очищаем лист и вставляем данные
        _svc.spreadsheets().values().clear(
            spreadsheetId=GSHEET_ID,
            range=sheet_name
        ).execute()
        
        _append(sheet_name, rows)
        logger.info(f"Данные успешно выгружены в лист '{sheet_name}'")
        
        # Автоподбор ширины колонок
        sheet_id = next((s['properties']['sheetId'] for s in sheets_metadata['sheets'] 
                      if s['properties']['title'] == sheet_name), None)
        
        if sheet_id:
            auto_resize_request = {
                "requests": [
                    {
                        "autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": len(rows[0]) if rows else 10
                            }
                        }
                    }
                ]
            }
            _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=auto_resize_request).execute()
    
    except Exception as e:
        logger.error(f"Ошибка при выгрузке данных в лист '{sheet_name}': {e}")


def setup_lots_all_header():
    """Создает или обновляет заголовки в таблице лотов."""
    sheet_name = "lots_all"
    logger.info(f"Настройка заголовков для таблицы {sheet_name}")
    
    headers = [
        "№", "Название", "Адрес", "Район", "Категория", "Площадь, м²", 
        "Текущая ставка, ₽/м²", "Рыночная ставка, ₽/м²", "Общая стоимость (торги), ₽", 
        "Общая стоимость (рыночная), ₽", "Капитализация, ₽", "Капитализация, %",
        "ГАП (рыночный), ₽/мес", "Доходность (рыночная), %", "Аукцион", "Документ", 
        "URL аукциона", "UUID (technical)", "Категория размера", "Наличие подвала", "Верхний этаж",
        "Найдено предл. продажи", "Найдено предл. аренды",
        "Отфильтровано предл. продажи", "Отфильтровано предл. аренды",
        "Плюсик за аренду", "Плюсик за продажу", "Всего плюсиков", "Статус"   # Новые колонки
    ]
    
    try:
        # Проверяем существование листа
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheet_exists = any(sheet['properties']['title'] == sheet_name for sheet in sheets_metadata['sheets'])
        
        if sheet_exists:
            # Лист существует - обновляем только заголовки
            logger.info(f"Лист {sheet_name} существует, обновляем заголовки")
            
            # Получаем текущие данные первой строки
            result = _svc.spreadsheets().values().get(
                spreadsheetId=GSHEET_ID,
                range=f"{sheet_name}!A1:Z1"
            ).execute()
            
            # Очищаем первую строку
            _svc.spreadsheets().values().clear(
                spreadsheetId=GSHEET_ID,
                range=f"{sheet_name}!A1:Z1"
            ).execute()
            
            # Добавляем заголовки в первую строку
            _svc.spreadsheets().values().update(
                spreadsheetId=GSHEET_ID,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [headers]}
            ).execute()
            
            # Делаем первую строку жирной и замороженной
            sheet_id = next((sheet['properties']['sheetId'] for sheet in sheets_metadata['sheets'] 
                          if sheet['properties']['title'] == sheet_name), None)
            
            if sheet_id:
                body = {
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "textFormat": {"bold": True},
                                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                                    }
                                },
                                "fields": "userEnteredFormat(textFormat,backgroundColor)"
                            }
                        },
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": sheet_id,
                                    "gridProperties": {
                                        "frozenRowCount": 1
                                    }
                                },
                                "fields": "gridProperties.frozenRowCount"
                            }
                        }
                    ]
                }
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        else:
            # Лист не существует - создаем его с заголовками
            logger.info(f"Лист {sheet_name} не существует, создаем новый")
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
            
            # Добавляем заголовки в первую строку
            _svc.spreadsheets().values().update(
                spreadsheetId=GSHEET_ID,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [headers]}
            ).execute()
            
            # Форматируем заголовки
            sheet_id = next((sheet['properties']['sheetId'] for sheet in sheets_metadata['sheets'] 
                          if sheet['properties']['title'] == sheet_name), None)
            
            if sheet_id:
                body = {
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "textFormat": {"bold": True},
                                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                                    }
                                },
                                "fields": "userEnteredFormat(textFormat,backgroundColor)"
                            }
                        }
                    ]
                }
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        
        logger.info(f"Заголовки для таблицы {sheet_name} успешно настроены")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при настройке заголовков для {sheet_name}: {e}")
        return False


def setup_cian_sale_all_header():
    """Создает или обновляет заголовки в таблице объявлений о продаже."""
    sheet_name = "cian_sale_all"
    logger.info(f"Настройка заголовков для таблицы {sheet_name}")
    
    headers = [
        "№", "Адрес", "Район", "Площадь, м²", "Цена за м²", 
        "Общая стоимость, ₽", "Расстояние, км", "Ссылка", "UUID лота", "ID объявления"
    ]
    
    try:
        # Проверяем существование листа
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheet_exists = any(sheet['properties']['title'] == sheet_name for sheet in sheets_metadata['sheets'])
        
        if sheet_exists:
            # Лист существует - обновляем только заголовки
            logger.info(f"Лист {sheet_name} существует, обновляем заголовки")
            
            # Очищаем первую строку
            _svc.spreadsheets().values().clear(
                spreadsheetId=GSHEET_ID,
                range=f"{sheet_name}!A1:J1"
            ).execute()
            
            # Добавляем заголовки в первую строку
            _svc.spreadsheets().values().update(
                spreadsheetId=GSHEET_ID,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [headers]}
            ).execute()
            
            # Делаем первую строку жирной и замороженной
            sheet_id = next((sheet['properties']['sheetId'] for sheet in sheets_metadata['sheets'] 
                          if sheet['properties']['title'] == sheet_name), None)
            
            if sheet_id:
                body = {
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "textFormat": {"bold": True},
                                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                                    }
                                },
                                "fields": "userEnteredFormat(textFormat,backgroundColor)"
                            }
                        },
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": sheet_id,
                                    "gridProperties": {
                                        "frozenRowCount": 1
                                    }
                                },
                                "fields": "gridProperties.frozenRowCount"
                            }
                        }
                    ]
                }
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        else:
            # Лист не существует - создаем его с заголовками
            logger.info(f"Лист {sheet_name} не существует, создаем новый")
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
            
            # Добавляем заголовки в первую строку
            _svc.spreadsheets().values().update(
                spreadsheetId=GSHEET_ID,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [headers]}
            ).execute()
            
            # Форматируем заголовки
            sheet_id = next((sheet['properties']['sheetId'] for sheet in sheets_metadata['sheets'] 
                          if sheet['properties']['title'] == sheet_name), None)
            
            if sheet_id:
                body = {
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "textFormat": {"bold": True},
                                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                                    }
                                },
                                "fields": "userEnteredFormat(textFormat,backgroundColor)"
                            }
                        }
                    ]
                }
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        
        logger.info(f"Заголовки для таблицы {sheet_name} успешно настроены")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при настройке заголовков для {sheet_name}: {e}")
        return False


def setup_cian_rent_all_header():
    """Создает или обновляет заголовки в таблице объявлений об аренде."""
    sheet_name = "cian_rent_all"
    logger.info(f"Настройка заголовков для таблицы {sheet_name}")
    
    headers = [
        "№", "Адрес", "Район", "Площадь, м²", "Цена за м²", 
        "Общая стоимость, ₽", "Расстояние, км", "Ссылка", "UUID лота", "ID объявления"
    ]
    
    try:
        # Проверяем существование листа
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheet_exists = any(sheet['properties']['title'] == sheet_name for sheet in sheets_metadata['sheets'])
        
        if sheet_exists:
            # Лист существует - обновляем только заголовки
            logger.info(f"Лист {sheet_name} существует, обновляем заголовки")
            
            # Очищаем первую строку
            _svc.spreadsheets().values().clear(
                spreadsheetId=GSHEET_ID,
                range=f"{sheet_name}!A1:J1"
            ).execute()
            
            # Добавляем заголовки в первую строку
            _svc.spreadsheets().values().update(
                spreadsheetId=GSHEET_ID,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [headers]}
            ).execute()
            
            # Делаем первую строку жирной и замороженной
            sheet_id = next((sheet['properties']['sheetId'] for sheet in sheets_metadata['sheets'] 
                          if sheet['properties']['title'] == sheet_name), None)
            
            if sheet_id:
                body = {
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "textFormat": {"bold": True},
                                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                                    }
                                },
                                "fields": "userEnteredFormat(textFormat,backgroundColor)"
                            }
                        },
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": sheet_id,
                                    "gridProperties": {
                                        "frozenRowCount": 1
                                    }
                                },
                                "fields": "gridProperties.frozenRowCount"
                            }
                        }
                    ]
                }
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        else:
            # Лист не существует - создаем его с заголовками
            logger.info(f"Лист {sheet_name} не существует, создаем новый")
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
            
            # Добавляем заголовки в первую строку
            _svc.spreadsheets().values().update(
                spreadsheetId=GSHEET_ID,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [headers]}
            ).execute()
            
            # Форматируем заголовки
            sheet_id = next((sheet['properties']['sheetId'] for sheet in sheets_metadata['sheets'] 
                          if sheet['properties']['title'] == sheet_name), None)
            
            if sheet_id:
                body = {
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "textFormat": {"bold": True},
                                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                                    }
                                },
                                "fields": "userEnteredFormat(textFormat,backgroundColor)"
                            }
                        }
                    ]
                }
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        
        logger.info(f"Заголовки для таблицы {sheet_name} успешно настроены")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при настройке заголовков для {sheet_name}: {e}")
        return False


def setup_all_headers():
    """Настраивает заголовки во всех основных таблицах."""
    logger.info("Настраиваем заголовки во всех таблицах...")
    setup_lots_all_header()
    setup_cian_sale_all_header()
    setup_cian_rent_all_header()
    logger.info("Настройка заголовков завершена")

setup_all_headers()

def find_lot_by_uuid(lot_uuid: str, sheet_name: str = "lots_all") -> Optional[Lot]:
    """
    Поиск лота по UUID в Google Sheets
    
    Args:
        lot_uuid: UUID лота для поиска
        sheet_name: Название листа для поиска (по умолчанию "lots_all")
    
    Returns:
        Объект Lot если найден, иначе None
    """
    try:
        logger.info(f"Поиск лота с UUID {lot_uuid} в листе {sheet_name}")
        
        # Получаем все данные из листа
        result = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet_name}!A:R"  # Берем все колонки до UUID (колонка R)
        ).execute()
        
        values = result.get('values', [])
        if not values:
            logger.warning(f"Лист {sheet_name} пуст")
            return None
        
        # Первая строка - заголовки
        headers = values[0] if values else []
        
        # Находим индекс колонки с UUID
        uuid_column_index = None
        for i, header in enumerate(headers):
            if "UUID" in header:
                uuid_column_index = i
                break
        
        if uuid_column_index is None:
            logger.error(f"Колонка UUID не найдена в листе {sheet_name}")
            return None
        
        # Ищем строку с нужным UUID
        for row_index, row in enumerate(values[1:], start=2):  # Пропускаем заголовки
            if len(row) > uuid_column_index and row[uuid_column_index] == lot_uuid:
                logger.info(f"Найден лот с UUID {lot_uuid} в строке {row_index}")
                
                # Создаем объект Lot из найденной строки
                try:
                    # Функция для безопасного парсинга чисел с запятыми
                    def safe_float(value):
                        if not value:
                            return 0.0
                        try:
                            # Заменяем запятые на точки для парсинга float
                            return float(str(value).replace(',', '.'))
                        except:
                            return 0.0
                    
                    from datetime import datetime
                    from uuid import UUID
                    
                    lot = Lot(
                        id=row[0] if len(row) > 0 else "",
                        name=row[1] if len(row) > 1 else "",
                        address=row[2] if len(row) > 2 else "",
                        coords=None,  # Координаты не сохраняем в таблице
                        area=safe_float(row[5]) if len(row) > 5 else 0.0,
                        price=safe_float(row[8]) if len(row) > 8 else 0.0,
                        notice_number=row[15] if len(row) > 15 else "",
                        lot_number=0,  # Не сохраняем в таблице
                        auction_type=row[14] if len(row) > 14 else "",
                        sale_type="",  # Не сохраняем в таблице
                        law_reference="",  # Не сохраняем в таблице
                        application_start=datetime.now(),  # Значения по умолчанию
                        application_end=datetime.now(),
                        auction_start=datetime.now(),
                        cadastral_number="",  # Не сохраняем в таблице
                        property_category="",  # Не сохраняем в таблице
                        ownership_type="",  # Не сохраняем в таблице
                        auction_step=0.0,  # Не сохраняем в таблице
                        deposit=0.0,  # Не сохраняем в таблице
                        recipient="",  # Не сохраняем в таблице
                        recipient_inn="",  # Не сохраняем в таблице
                        recipient_kpp="",  # Не сохраняем в таблице
                        bank_name="",  # Не сохраняем в таблице
                        bank_bic="",  # Не сохраняем в таблице
                        bank_account="",  # Не сохраняем в таблице
                        correspondent_account="",  # Не сохраняем в таблице
                        auction_url=row[16] if len(row) > 16 else "",
                        uuid=UUID(lot_uuid),  # Конвертируем в UUID
                        district=row[3] if len(row) > 3 else ""
                    )
                    return lot
                except Exception as e:
                    logger.error(f"Ошибка при создании объекта Lot из строки: {e}")
                    return None
        
        logger.info(f"Лот с UUID {lot_uuid} не найден в листе {sheet_name}")
        return None
        
    except Exception as e:
        logger.error(f"Ошибка при поиске лота по UUID {lot_uuid}: {e}")
        return None


def find_analogs_in_sheets(lot_uuid: str, radius_km: float = 3.0) -> List[Offer]:
    """
    Поиск аналогов для лота по UUID в листах cian_sale_all и cian_rent_all
    
    Args:
        lot_uuid: UUID лота для поиска аналогов
        radius_km: Радиус поиска (пока не используется, но может быть полезен в будущем)
    
    Returns:
        Список найденных аналогов
    """
    try:
        logger.info(f"Поиск аналогов для лота {lot_uuid} в Google Sheets")
        analogs = []
        
        # Ищем в листах продаж и аренды
        for sheet_name in ["cian_sale_all", "cian_rent_all"]:
            try:
                # Получаем данные из листа
                result = _svc.spreadsheets().values().get(
                    spreadsheetId=GSHEET_ID,
                    range=f"{sheet_name}!A:J"  # Берем все основные колонки
                ).execute()
                
                values = result.get('values', [])
                if not values:
                    logger.info(f"Лист {sheet_name} пуст")
                    continue
                
                headers = values[0] if values else []
                logger.info(f"Поиск в листе {sheet_name}, найдено {len(values)-1} строк")
                
                # Находим индекс колонки с UUID лота
                lot_uuid_column_index = None
                for i, header in enumerate(headers):
                    if "UUID лота" in header or "lot_uuid" in header.lower():
                        lot_uuid_column_index = i
                        break
                
                if lot_uuid_column_index is None:
                    logger.warning(f"Колонка UUID лота не найдена в листе {sheet_name}")
                    continue
                
                # Ищем строки с нужным UUID лота
                found_count = 0
                for row in values[1:]:  # Пропускаем заголовки
                    if len(row) > lot_uuid_column_index and row[lot_uuid_column_index] == lot_uuid:
                        try:
                            # Функция для безопасного парсинга чисел с запятыми
                            def safe_float(value):
                                if not value:
                                    return 0.0
                                try:
                                    # Заменяем запятые на точки для парсинга float
                                    return float(str(value).replace(',', '.'))
                                except:
                                    return 0.0
                            
                            # Создаем объект Offer из найденной строки
                            offer = Offer(
                                id=row[9] if len(row) > 9 else "",  # ID объявления
                                lot_uuid=lot_uuid,
                                price=safe_float(row[5]) if len(row) > 5 else 0.0,
                                area=safe_float(row[3]) if len(row) > 3 else 0.0,
                                url=row[7] if len(row) > 7 else "",
                                type="sale" if "sale" in sheet_name else "rent",
                                address=row[1] if len(row) > 1 else "",
                                district=row[2] if len(row) > 2 else "",
                                distance_to_lot=safe_float(row[6]) if len(row) > 6 else 0.0
                            )
                            analogs.append(offer)
                            found_count += 1
                        except Exception as e:
                            logger.error(f"Ошибка при создании объекта Offer: {e}")
                            continue
                
                logger.info(f"Найдено {found_count} аналогов в листе {sheet_name}")
                
            except Exception as e:
                logger.error(f"Ошибка при поиске в листе {sheet_name}: {e}")
                continue
        
        logger.info(f"Всего найдено {len(analogs)} аналогов для лота {lot_uuid}")
        return analogs
        
    except Exception as e:
        logger.error(f"Ошибка при поиске аналогов для лота {lot_uuid}: {e}")
        return []

def _get_offers_from_sheet(sheet_name: str, lot_uuid: str, offer_type: str) -> List[Offer]:
    """
    Получение объявлений для указанного лота из конкретной таблицы
    
    Args:
        sheet_name: Имя листа (cian_sale_all или cian_rent_all)
        lot_uuid: UUID лота
        offer_type: Тип объявления (sale или rent)
        
    Returns:
        Список объявлений
    """
    try:
        # Получаем данные из таблицы
        result = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"{sheet_name}!A:Z"
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) < 2:
            return []
        
        offers = []
        
        # Ищем объявления с соответствующим UUID лота (колонка I, индекс 8)
        for i, row in enumerate(values[1:], 1):  # Пропускаем заголовок
            if len(row) > 8 and str(row[8]) == str(lot_uuid):
                try:
                    from core.models import Offer
                    from uuid import UUID
                    
                    # Функция для безопасного парсинга чисел с запятыми
                    def safe_float(value):
                        if not value:
                            return 0.0
                        try:
                            # Заменяем запятые на точки для парсинга float
                            return float(str(value).replace(',', '.'))
                        except:
                            return 0.0
                    
                    offer = Offer(
                        id=f"{offer_type}_{sheet_name}_{i}",
                        lot_uuid=UUID(lot_uuid),
                        address=row[1] if len(row) > 1 else "",
                        area=safe_float(row[3]) if len(row) > 3 else 0.0,
                        price=safe_float(row[5]) if len(row) > 5 else 0.0,
                        url=row[7] if len(row) > 7 else "",
                        type=offer_type,
                        district=row[2] if len(row) > 2 else "",
                        distance_to_lot=safe_float(row[6]) if len(row) > 6 else 0.0
                    )
                    
                    offers.append(offer)
                    
                except Exception as e:
                    logger.warning(f"Error parsing offer from {sheet_name} row {i+1}: {e}")
                    continue
        
        return offers
        
    except Exception as e:
        logger.error(f"Error getting offers from {sheet_name} for lot {lot_uuid}: {e}")
        return []