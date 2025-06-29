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
    """Apply conditional formatting with better zero handling."""
    parts = condition.split(' ', 1)
    condition_type = parts[0]
    condition_value = parts[1] if len(parts) > 1 else None
    
    # Специальная обработка для капитализации - исключаем нулевые значения
    if column == 11 and condition_type == "NUMBER_GREATER_THAN_EQ":
        # Используем CUSTOM_FORMULA для исключения нулей
        column_letter = chr(65 + column)  # L для колонки 11
        formula = f"=AND({column_letter}{start_row + 1}>={condition_value},{column_letter}{start_row + 1}>0)"
        
        rule = {
            "ranges": [{
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": column,
                "endColumnIndex": column + 1
            }],
            "booleanRule": {
                "condition": {
                    "type": "CUSTOM_FORMULA",
                    "values": [{"userEnteredValue": formula}]
                },
                "format": {"backgroundColor": color}
            }
        }
        
        logger.info(f"Applying special formatting for capitalization column with formula: {formula}")
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
        try:
            # Пытаемся преобразовать в число
            num_value = float(condition_value)
            
            # Для целых чисел убираем десятичную часть
            if num_value.is_integer():
                formatted_value = str(int(num_value))
            else:
                formatted_value = str(num_value)
                
            rule["booleanRule"]["condition"]["values"] = [{"userEnteredValue": formatted_value}]
            logger.debug(f"Использовано числовое значение: {formatted_value}")
            
        except ValueError:
            rule["booleanRule"]["condition"]["values"] = [{"userEnteredValue": condition_value}]
            logger.debug(f"Использовано строковое значение: {condition_value}")
    
    request = {"addConditionalFormatRule": {"rule": rule, "index": 0}}
    body = {"requests": [request]}
    
    try:
        _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        logger.debug(f"Успешно применено форматирование для колонки {column}")
    except Exception as e:
        logger.error(f"Ошибка при применении форматирования для колонки {column}: {e}")
        
        # Попробуем альтернативный подход с CUSTOM_FORMULA
        if condition_value and api_condition_type in ["NUMBER_GREATER_THAN_EQ", "NUMBER_EQ"]:
            try:
                logger.info(f"Пробуем альтернативный подход для {api_condition_type}")
                
                # Создаем новое правило с CUSTOM_FORMULA  
                column_letter = chr(65 + column)
                
                if api_condition_type == "NUMBER_GREATER_THAN_EQ":
                    formula = f"=AND({column_letter}{start_row + 1}>={condition_value},{column_letter}{start_row + 1}<>0)"
                elif api_condition_type == "NUMBER_EQ":
                    formula = f"={column_letter}{start_row + 1}={condition_value.split('.')[0]}"
                else:
                    formula = f"={column_letter}{start_row + 1}>{condition_value}"
                
                alternative_rule = {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": end_row,
                        "startColumnIndex": column,
                        "endColumnIndex": column + 1
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": formula}]
                        },
                        "format": {"backgroundColor": color}
                    }
                }
                
                alternative_request = {"addConditionalFormatRule": {"rule": alternative_rule, "index": 0}}
                alternative_body = {"requests": [alternative_request]}
                
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=alternative_body).execute()
                logger.info(f"Успешно применено альтернативное форматирование для колонки {column} с формулой: {formula}")
                
            except Exception as e2:
                logger.error(f"Альтернативное форматирование также не удалось для колонки {column}: {e2}")

# public API ------------------------------------------------------
def push_lots(lots: List[Lot], sheet_name: str = "lots_all"):
    """Добавляет лоты в таблицу без перезаписи существующих данных."""
    logger.info(f"Начинаем выгрузку {len(lots)} лотов в Google Sheets на лист {sheet_name}")
    
    if not lots:
        logger.warning("Список лотов пуст, выгрузка не выполнена")
        return
    
    try:
        # Конвертируем лоты в строки для таблицы
        rows = []
        seen_lots = set()
        for i, lot in enumerate(lots, 1):
            classification = getattr(lot, 'classification', None)
            lot_signature = (lot.address.strip().lower(), round(lot.area, 2))
            if lot_signature in seen_lots:
                logging.info(f"Пропуск дубликата лота: {lot.name} ({lot.address}, {lot.area} м²)")
                continue
            seen_lots.add(lot_signature)
            row = [
                i,  # Порядковый номер
                lot.name,
                lot.address,
                getattr(lot, 'district', 'Неизвестно'),
                lot.property_category,
                lot.area,
                getattr(lot, 'current_price_per_sqm', 0),
                getattr(lot, 'market_price_per_sqm', 0),
                lot.price,
                getattr(lot, 'market_value', 0),
                getattr(lot, 'capitalization_rub', 0),
                getattr(lot, 'capitalization_percent', 0),
                getattr(lot, 'monthly_gap', 0),
                getattr(lot, 'annual_yield_percent', 0),
                lot.auction_type,
                lot.notice_number,
                lot.auction_url,
                str(lot.uuid),
                getattr(classification, 'size_category', '') if classification else '',
                'Да' if (classification and classification.has_basement) else 'Нет',
                'Да' if (classification and classification.is_top_floor) else 'Нет',
                getattr(lot, 'sale_offers_count', 0),
                getattr(lot, 'rent_offers_count', 0),
                getattr(lot, 'filtered_sale_offers_count', 0),
                getattr(lot, 'filtered_rent_offers_count', 0),
                getattr(lot, 'plus_rental', 0),  # Плюсик за аренду (1 или 0)
                getattr(lot, 'plus_sale', 0),    # Плюсик за продажу (1 или 0)
                getattr(lot, 'plus_count', 0),   # Общее количество плюсиков
                getattr(lot, 'status', 'unknown')  # Статус лота
            ]
            rows.append(row)
        
        # Отправляем данные в Google Sheets
        _append(sheet_name, rows)
        logger.info(f"Успешно добавлено {len(rows)} лотов в таблицу {sheet_name}")
        
        # Применяем условное форматирование после добавления данных
        try:
            # Получаем информацию о листе для форматирования
            sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
            sheet_id = None
            
            for sheet in sheets_metadata['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
            
            if sheet_id is not None:
                # Определяем диапазон строк для форматирования
                start_row = 1  # Начинаем со второй строки (индекс 1)
                last_row = start_row + len(rows)
                
                # Форматирование для капитализации в % (колонка L, индекс 11)
                # ТОЛЬКО для значений >= 15%
                _format_cells(
                    sheet_id=sheet_id,
                    start_row=start_row,
                    end_row=last_row,
                    column=11,  # Колонка L (капитализация, %)
                    condition="NUMBER_GREATER_THAN_EQ 10",  # Изменено с 15 на 10
                    color={"red": 0.7176, "green": 0.8823, "blue": 0.7176}  # Светло-зеленый
                )
                
                # Форматирование для доходности (колонка N, индекс 13)
                # Окрашиваем зеленым доходность >= 8%
                _format_cells(
                    sheet_id=sheet_id,
                    start_row=start_row,
                    end_row=last_row,
                    column=13,  # Колонка N (доходность, %)
                    condition="NUMBER_GREATER_THAN_EQ 8",  # >= 8%
                    color={"red": 0.7176, "green": 0.8823, "blue": 0.7176}  # Светло-зеленый
                )
                logger.info("Добавлено форматирование доходности (>= 8%)")
                
                # Форматирование для плюсиков за аренду (колонка Z, индекс 25)
                _format_cells(
                    sheet_id=sheet_id,
                    start_row=start_row,
                    end_row=last_row,
                    column=25,  # Колонка Z (плюсик за аренду)
                    condition="NUMBER_EQ 1",  # = 1
                    color={"red": 0.5647, "green": 0.9333, "blue": 0.5647}  # Зеленый
                )
                
                # Форматирование для плюсиков за продажу (колонка AA, индекс 26)
                _format_cells(
                    sheet_id=sheet_id,
                    start_row=start_row,
                    end_row=last_row,
                    column=26,  # Колонка AA (плюсик за продажу)
                    condition="NUMBER_EQ 1",  # = 1
                    color={"red": 0.5647, "green": 0.9333, "blue": 0.5647}  # Зеленый
                )
                
                # Форматирование для общего количества плюсиков (колонка AB, индекс 27)
                # Градиент: 3 плюса - темно-зеленый, 2 - зеленый, 1 - светло-зеленый
                _format_cells(
                    sheet_id=sheet_id,
                    start_row=start_row,
                    end_row=last_row,
                    column=27,  # Колонка AB (общее количество плюсиков)
                    condition="NUMBER_EQ 3",  # = 3
                    color={"red": 0.2745, "green": 0.7412, "blue": 0.2745}  # Темно-зеленый
                )
                
                _format_cells(
                    sheet_id=sheet_id,
                    start_row=start_row,
                    end_row=last_row,
                    column=27,  # Колонка AB
                    condition="NUMBER_EQ 2",  # = 2
                    color={"red": 0.5647, "green": 0.9333, "blue": 0.5647}  # Зеленый
                )
                
                _format_cells(
                    sheet_id=sheet_id,
                    start_row=start_row,
                    end_row=last_row,
                    column=27,  # Колонка AB
                    condition="NUMBER_EQ 1",  # = 1
                    color={"red": 0.7176, "green": 0.8823, "blue": 0.7176}  # Светло-зеленый
                )
                
                logger.info("Добавлено форматирование плюсиков")
                
            else:
                logger.warning(f"Не удалось найти лист {sheet_name} для форматирования")
                
        except Exception as e:
            logger.error(f"Ошибка при применении форматирования: {e}")
            # Не прерываем выполнение из-за ошибок форматирования
        
    except Exception as e:
        logger.error(f"Ошибка при выгрузке лотов в Google Sheets: {e}")
        raise

def _format_cells(sheet_id, start_row, end_row, column, condition, color):
    """Apply conditional formatting to a range of cells with improved handling."""
    # Соответствие пользовательских условий и API-констант
    condition_mapping = {
        "NUMBER_LESS_THAN_OR_EQUAL": "NUMBER_LESS_THAN_EQ",
        "NUMBER_GREATER_THAN_OR_EQUAL": "NUMBER_GREATER_THAN_EQ",
        "NUMBER_LESS": "NUMBER_LESS",
        "NUMBER_GREATER": "NUMBER_GREATER",
        "NUMBER_EQUAL": "NUMBER_EQ",
        "NUMBER_EQ": "NUMBER_EQ",
        "NUMBER_GREATER_THAN_EQ": "NUMBER_GREATER_THAN_EQ"
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
        try:
            # Пытаемся преобразовать в число
            num_value = float(condition_value)
            
            # Для целых чисел убираем десятичную часть
            if num_value.is_integer():
                formatted_value = str(int(num_value))  # "15" вместо "15.0"
            else:
                formatted_value = str(num_value)  # "15.5" остается как есть
                
            rule["booleanRule"]["condition"]["values"] = [{"userEnteredValue": formatted_value}]
            logger.debug(f"Использовано числовое значение: {formatted_value}")
            
        except ValueError:
            # Если не число, сохраняем как строку
            rule["booleanRule"]["condition"]["values"] = [{"userEnteredValue": condition_value}]
            logger.debug(f"Использовано строковое значение: {condition_value}")
    
    request = {"addConditionalFormatRule": {"rule": rule, "index": 0}}
    body = {"requests": [request]}
    
    try:
        _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        logger.debug(f"Успешно применено форматирование для колонки {column}")
    except Exception as e:
        logger.error(f"Ошибка при применении форматирования для колонки {column}: {e}")
        
        # Попробуем альтернативный подход с CUSTOM_FORMULA
        if condition_value and api_condition_type in ["NUMBER_GREATER_THAN_EQ", "NUMBER_EQ"]:
            try:
                logger.info(f"Пробуем альтернативный подход для {api_condition_type}")
                
                # Создаем новое правило с CUSTOM_FORMULA
                column_letter = chr(65 + column)  # A=0, B=1, etc.
                
                if api_condition_type == "NUMBER_GREATER_THAN_EQ":
                    formula = f"={column_letter}{start_row + 1}>={condition_value}"
                elif api_condition_type == "NUMBER_EQ":
                    formula = f"={column_letter}{start_row + 1}={condition_value.split('.')[0]}"
                else:
                    formula = f"={column_letter}{start_row + 1}>{condition_value}"
                
                alternative_rule = {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": end_row,
                        "startColumnIndex": column,
                        "endColumnIndex": column + 1
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": formula}]
                        },
                        "format": {"backgroundColor": color}
                    }
                }
                
                alternative_request = {"addConditionalFormatRule": {"rule": alternative_rule, "index": 0}}
                alternative_body = {"requests": [alternative_request]}
                
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=alternative_body).execute()
                logger.info(f"Успешно применено альтернативное форматирование для колонки {column} с формулой: {formula}")
                
            except Exception as e2:
                logger.error(f"Альтернативное форматирование также не удалось для колонки {column}: {e2}")



def _format_cells(sheet_id, start_row, end_row, column, condition, color):
    """Apply conditional formatting to a range of cells with improved handling."""
    # Соответствие пользовательских условий и API-констант
    condition_mapping = {
        "NUMBER_LESS_THAN_OR_EQUAL": "NUMBER_LESS_THAN_EQ",
        "NUMBER_GREATER_THAN_OR_EQUAL": "NUMBER_GREATER_THAN_EQ",
        "NUMBER_LESS": "NUMBER_LESS",
        "NUMBER_GREATER": "NUMBER_GREATER",
        "NUMBER_EQUAL": "NUMBER_EQ",
        "NUMBER_EQ": "NUMBER_EQ",
        "NUMBER_GREATER_THAN_EQ": "NUMBER_GREATER_THAN_EQ"
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
        try:
            # Пытаемся преобразовать в число
            num_value = float(condition_value)
            
            # Для целых чисел убираем десятичную часть
            if num_value.is_integer():
                formatted_value = str(int(num_value))  # "15" вместо "15.0"
            else:
                formatted_value = str(num_value)  # "15.5" остается как есть
                
            rule["booleanRule"]["condition"]["values"] = [{"userEnteredValue": formatted_value}]
            logger.debug(f"Использовано числовое значение: {formatted_value}")
            
        except ValueError:
            # Если не число, сохраняем как строку
            rule["booleanRule"]["condition"]["values"] = [{"userEnteredValue": condition_value}]
            logger.debug(f"Использовано строковое значение: {condition_value}")
    
    request = {"addConditionalFormatRule": {"rule": rule, "index": 0}}
    body = {"requests": [request]}
    
    try:
        _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        logger.debug(f"Успешно применено форматирование для колонки {column}")
    except Exception as e:
        logger.error(f"Ошибка при применении форматирования для колонки {column}: {e}")
        
        # Попробуем альтернативный подход с CUSTOM_FORMULA
        if condition_value and api_condition_type in ["NUMBER_GREATER_THAN_EQ", "NUMBER_EQ"]:
            try:
                logger.info(f"Пробуем альтернативный подход для {api_condition_type}")
                
                # Создаем новое правило с CUSTOM_FORMULA
                column_letter = chr(65 + column)  # A=0, B=1, etc.
                
                if api_condition_type == "NUMBER_GREATER_THAN_EQ":
                    formula = f"={column_letter}{start_row + 1}>={condition_value}"
                elif api_condition_type == "NUMBER_EQ":
                    formula = f"={column_letter}{start_row + 1}={condition_value.split('.')[0]}"
                else:
                    formula = f"={column_letter}{start_row + 1}>{condition_value}"
                
                alternative_rule = {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": end_row,
                        "startColumnIndex": column,
                        "endColumnIndex": column + 1
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": formula}]
                        },
                        "format": {"backgroundColor": color}
                    }
                }
                
                alternative_request = {"addConditionalFormatRule": {"rule": alternative_rule, "index": 0}}
                alternative_body = {"requests": [alternative_request]}
                
                _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=alternative_body).execute()
                logger.info(f"Успешно применено альтернативное форматирование для колонки {column} с формулой: {formula}")
                
            except Exception as e2:
                logger.error(f"Альтернативное форматирование также не удалось для колонки {column}: {e2}")

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

def find_lot_by_uuid(lot_uuid: str) -> Optional[Lot]:
    """Находит лот по UUID в таблице Google Sheets"""
    try:
        # Читаем данные из таблицы lots_all
        result = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range="lots_all!A2:AC1000"
        ).execute()
        
        values = result.get('values', [])
        
        for row in values:
            if len(row) < 18:  # Недостаточно колонок
                continue
                
            try:
                # UUID находится в колонке R (индекс 17)
                if len(row) > 17 and row[17] == lot_uuid:
                    
                    # ИСПРАВЛЕНО: улучшенный парсинг площади
                    def parse_area(area_str: str) -> float:
                        """Парсит площадь из строки"""
                        if not area_str:
                            return 0.0
                        
                        try:
                            # Убираем все символы кроме цифр, точек и запятых
                            import re
                            area_clean = re.sub(r'[^0-9.,]', '', area_str)
                            area_clean = area_clean.replace(',', '.')
                            
                            # Если несколько точек, берем последнюю как десятичную
                            if area_clean.count('.') > 1:
                                parts = area_clean.split('.')
                                area_clean = ''.join(parts[:-1]) + '.' + parts[-1]
                            
                            return float(area_clean) if area_clean else 0.0
                        except:
                            return 0.0
                    
                    # ИСПРАВЛЕНО: улучшенный парсинг цены
                    def parse_price(price_str: str) -> float:
                        """Парсит цену из строки"""
                        if not price_str:
                            return 0.0
                        
                        try:
                            # Убираем все символы кроме цифр
                            import re
                            price_clean = re.sub(r'[^0-9]', '', price_str)
                            return float(price_clean) if price_clean else 0.0
                        except:
                            return 0.0
                    
                    # Парсим данные из строки
                    area = parse_area(row[5]) if len(row) > 5 else 0.0
                    price = parse_price(row[8]) if len(row) > 8 else 0.0
                    
                    # Создаем объект лота
                    lot = Lot(
                        id=row[0] if len(row) > 0 else "",
                        name=row[1] if len(row) > 1 else "",
                        address=row[2] if len(row) > 2 else "",
                        area=area,
                        price=price,
                        coords="55.7558,37.6176",  # Значения по умолчанию
                        notice_number=row[15] if len(row) > 15 else "",
                        lot_number=1,
                        auction_type=row[14] if len(row) > 14 else "",
                        sale_type="Продажа",
                        law_reference="Федеральный закон №44-ФЗ",
                        application_start=datetime.now(),
                        application_end=datetime.now(),
                        auction_start=datetime.now(),
                        cadastral_number="",
                        property_category=row[4] if len(row) > 4 else "",
                        ownership_type="Государственная собственность",
                        auction_step=0,
                        deposit=0,
                        recipient="",
                        recipient_inn="",
                        recipient_kpp="",
                        bank_name="",
                        bank_bic="",
                        bank_account="",
                        correspondent_account="",
                        auction_url=row[16] if len(row) > 16 else "",
                    )
                    
                    # Добавляем дополнительные метрики
                    if len(row) > 25:
                        lot.plus_rental = int(row[25]) if row[25].isdigit() else 0
                        lot.plus_sale = int(row[26]) if len(row) > 26 and row[26].isdigit() else 0
                        lot.plus_count = int(row[27]) if len(row) > 27 and row[27].isdigit() else 0
                        lot.status = row[28] if len(row) > 28 else "acceptable"
                    
                    logger.info(f"✅ Найден лот по UUID {lot_uuid}: {lot.area} м², {lot.price:,.0f} ₽")
                    return lot
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Ошибка парсинга строки: {e}")
                continue
        
        logger.warning(f"❌ Лот с UUID {lot_uuid} не найден")
        return None
        
    except Exception as e:
        logger.error(f"❌ Ошибка поиска лота: {e}")
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