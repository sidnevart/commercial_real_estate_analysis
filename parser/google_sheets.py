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
def push_lots(lots: List[Lot]):
    """Push full lot data to the 'torgi' sheet."""
    logger.info(f"Начинаем выгрузку {len(lots)} лотов в Google Sheets")
    
    if not lots:
        logger.warning("Пустой список лотов, нечего выгружать")
        return
    
    try:
        rows = []
        for lot in lots:
            try:
                current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
                market_price_per_sqm = lot.median_market_price / lot.area if lot.area > 0 and lot.median_market_price > 0 else 0
                total_history_cost = lot.price  
                total_current_cost = lot.price
                total_market_cost = lot.median_market_price
                
                cap_amount = lot.median_market_price - lot.price
                cap_percent = (cap_amount / lot.median_market_price * 100) if lot.median_market_price > 0 else 0
                
                monthly_gap = lot.median_market_price * 0.007
                
                annual_yield = 8.4  
                
                row = [
                    lot.id,
                    lot.lot_number,
                    lot.name,
                    lot.address,
                    lot.district,
                    lot.classification.category if hasattr(lot, 'classification') else "",
                    lot.area,
                    current_price_per_sqm,
                    market_price_per_sqm,
                    total_history_cost,
                    total_current_cost,
                    total_market_cost,
                    cap_amount,
                    cap_percent,
                    monthly_gap,
                    annual_yield,
                    lot.sale_type if hasattr(lot, 'sale_type') else '',
                    lot.auction_url,
                    lot.notice_number,
                    lot.lot_number,
                    lot.auction_type,
                    lot.sale_type,
                    lot.law_reference,
                    lot.application_start.strftime('%Y-%m-%d %H:%M:%S'),
                    lot.application_end.strftime('%Y-%m-%d %H:%M:%S'),
                    lot.auction_start.strftime('%Y-%m-%d %H:%M:%S'),
                    str(lot.cadastral_number),  # Преобразуем к строке, чтобы избежать проблем с JSON
                    lot.property_category,
                    lot.ownership_type,
                    lot.auction_step,
                    lot.deposit,
                    lot.recipient,
                    lot.recipient_inn,
                    lot.recipient_kpp,
                    lot.bank_name,
                    lot.bank_bic,
                    lot.bank_account,
                    lot.correspondent_account,
                    str(lot.uuid),
                    lot.classification.size_category if hasattr(lot, 'classification') else "",
                    "Да" if hasattr(lot, 'classification') and lot.classification.has_basement else "Нет",
                    "Да" if hasattr(lot, 'classification') and lot.classification.is_top_floor else "Нет"
                ]
                rows.append(row)
            except Exception as e:
                logger.error(f"Ошибка при подготовке данных лота {lot.id}: {e}")
        
        logger.info(f"Подготовлено {len(rows)} строк данных для выгрузки")
        
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheet_exists = any(sheet['properties']['title'] == 'torgi' for sheet in sheets_metadata['sheets'])
        
        if not sheet_exists:
            logger.error("Лист 'torgi' не найден в таблице Google Sheets. Создаем новый лист.")
            body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': 'torgi'
                        }
                    }
                }]
            }
            _svc.spreadsheets().batchUpdate(spreadsheetId=GSHEET_ID, body=body).execute()
        
        result = _svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range="torgi!A:A"
        ).execute()
        start_row = len(result.get('values', [])) + 1
        logger.info(f"Начинаем запись с {start_row} строки")
        
        _append("torgi", rows)
        logger.info(f"Данные успешно отправлены в Google Sheets")
        
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        
        try:
            sheet_id = next(sheet['properties']['sheetId'] 
                        for sheet in sheets_metadata['sheets'] 
                        if sheet['properties']['title'] == 'torgi')
            
            threshold = CONFIG["advantage_price_threshold"]
            
            _format_cells(
                sheet_id=sheet_id,
                start_row=start_row - 1,
                end_row=start_row + len(rows) - 1,
                column=13,  # Колонка капитализации в % (индекс 13)
                condition=f"NUMBER_LESS {threshold}",  # Исправлено с NUMBER_LESS_THAN_OR_EQUAL
                color={"red": 0.7176, "green": 0.8823, "blue": 0.7176}  # Светло-зеленый
            )

            yield_threshold = CONFIG.get("market_yield_threshold", 10)  # Используем настройку или 10% по умолчанию

            _format_cells(
                sheet_id=sheet_id,
                start_row=start_row - 1,
                end_row=start_row + len(rows) - 1,
                column=15,  # Колонка годовой доходности (индекс 15)
                condition=f"NUMBER_GREATER {yield_threshold}",  # Исправлено с NUMBER_GREATER_THAN_OR_EQUAL
                color={"red": 0.7176, "green": 0.8823, "blue": 0.7176}  # Светло-зеленый
            )
            logger.info(f"Форматирование для доходности выше {yield_threshold}% успешно применено")
            logger.info("Форматирование успешно применено")
        except StopIteration:
            logger.error("Не удалось найти ID листа 'torgi' для применения форматирования")
        except Exception as e:
            logger.error(f"Ошибка при применении форматирования: {e}")
    
    except Exception as e:
        logger.error(f"Ошибка при выгрузке лотов в Google Sheets: {e}", exc_info=True)
        raise


# В функции push_offers в файле google_sheets.py добавить вывод расстояния
def push_offers(sheet: str, offers: List[Offer]):
    """Push offer data including address and calculated price per sq.m."""
    rows = []
    for off in offers:
        # Calculate price per square meter
        price_per_sqm = off.price / off.area if off.area > 0 else 0
        
        # Добавляем расстояние до лота, если оно определено
        distance_to_lot = getattr(off, 'distance_to_lot', '')
        
        # Format row with all required fields
        rows.append([
            off.address,       # Address column
            off.district if hasattr(off, 'district') else "",  # District
            off.area,          # Area in square meters
            price_per_sqm,     # Price per square meter (calculated)
            off.price,         # Total price
            distance_to_lot,   # Расстояние до лота
            off.url,           # URL to the offer
            str(off.lot_uuid)  # UUID of the associated lot
        ])
    _append(sheet, rows)

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