import asyncio
import logging
import statistics
import re
import time
import os
import pickle
import pytz
import json
from core.deduplication_db import dedup_db
import sys
from datetime import datetime
from parser.address_parser import calculate_address_components, is_moscow_address, is_moscow_oblast_address
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple
from parser.torgi_async import fetch_lots
from parser.cian_minimal import fetch_nearby_offers, unformatted_address_to_cian_search_filter
from parser.google_sheets import push_lots, push_offers, push_district_stats
from parser.gpt_classifier import classify_property  
from parser.cian_minimal import get_parser
# from parser.geo_utils import filter_offers_by_distance
from core.models import Lot, Offer, PropertyClassification
from core.config import CONFIG
from parser.lot_district import gpt_extract_most_local_part_fixed
from test_address_cleanup import enhanced_address_search, create_fixed_geocoder, simplify_address_for_geocoding
#from parser.geo_utils import filter_offers_by_distance
import random
from parser.format_table import apply_all_formatting

# Импорт Telegram бота
from bot.bot_service import bot_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Глобальная переменная для хранения статистики по ценам районов
district_price_stats = {}


# Добавить в main.py функцию для периодического сохранения необработанных объявлений

def save_all_raw_offers():
    """Сохраняет все собранные объявления, даже не прошедшие фильтрацию."""
    global all_raw_offers
    
    if 'all_raw_offers' not in globals() or not all_raw_offers:
        logging.warning("Нет данных для сохранения всех объявлений")
        return
    
    timestamp = int(time.time())
    logging.info(f"Сохранение всех {len(all_raw_offers)} объявлений (включая отфильтрованные)")
    
    # Разделяем по типу
    sale_offers = [o for o in all_raw_offers if o.type == 'sale']
    rent_offers = [o for o in all_raw_offers if o.type == 'rent']
    
    # Сохраняем в отдельные таблицы с отметкой времени
    if sale_offers:
        try:
            push_offers(f"all_sale_{timestamp}", sale_offers)
            logging.info(f"✅ Сохранено {len(sale_offers)} объявлений о продаже")
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении всех объявлений о продаже: {e}")
    
    if rent_offers:
        try:
            push_offers(f"all_rent_{timestamp}", rent_offers)
            logging.info(f"✅ Сохранено {len(rent_offers)} объявлений об аренде")
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении всех объявлений об аренде: {e}")


def get_cian_metrics():
    """Получение метрик работы CIAN парсера"""
    parser = get_parser()
    
    # Отслеживание количества элементов в кеше парсера и др. метрики
    metrics = {}
    
    try:
        # Проверка работоспособности драйвера
        metrics["driver_alive"] = parser.driver is not None
        metrics["first_tab_valid"] = parser.first_tab is not None
        
        # Добавим тестовый запрос для проверки работоспособности
        test_url = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=offices"
        test_page = parser.get_page(test_url)
        metrics["test_request_success"] = test_page is not None and len(test_page) > 1000
        metrics["test_page_length"] = len(test_page) if test_page else 0
        
        logging.info(f"CIAN Parser metrics: {metrics}")
        
        if not metrics.get("driver_alive", False):
            logging.warning("⚠️ CIAN Parser driver не активен")
            # Попытка перезапустить драйвер
            parser.initialize_driver()
            metrics["driver_restarted"] = True
            
        if not metrics.get("test_request_success", False):
            logging.warning("⚠️ Тестовый запрос к CIAN не удался")
            
        return metrics
        
    except Exception as e:
        logging.error(f"Ошибка при получении метрик CIAN парсера: {e}")
        return {"error": str(e)}

def calculate_district(address: str) -> str:
    """
    Улучшенная функция определения района из адреса с приоритетом GPT.
    
    Args:
        address: Адрес для анализа
        
    Returns:
        Самый локальный элемент адреса (улица → район → город)
    """
    if not address:
        return "Москва"  # Дефолт
    
    try:
        # Используем исправленную GPT-функцию как основной метод
        from lot_district import gpt_extract_most_local_part_fixed
        
        result = gpt_extract_most_local_part_fixed(address)
        
        # Валидируем результат
        if result and result != "Неизвестно" and len(result) > 1:
            logging.info(f"🎯 GPT определил район для '{address[:50]}...': '{result}'")
            return result
        else:
            # Fallback к старой логике для совместимости
            logging.warning(f"⚠️ GPT не смог определить район для '{address[:50]}...', используем fallback")
            return calculate_district_fallback(address)
            
    except Exception as e:
        logging.error(f"❌ Ошибка при GPT-определении района: {e}")
        return calculate_district_fallback(address)

def calculate_district_fallback(address: str) -> str:
    """
    Резервная функция определения района (старая логика для совместимости).
    """
    if not address:
        return "Москва"
        
    # Специальные случаи для Зеленограда
    if re.search(r'зеленоград|крюково', address.lower()):
        return "Зеленоград"
        
    # Поиск района в адресе
    district_match = re.search(r'район\s+([^\s,]+)|([^\s,]+)\s+район', address.lower())
    if district_match:
        # Берём первую группу, если она есть, иначе вторую
        district = district_match.group(1) or district_match.group(2)
        return district.capitalize()
    
    # Поиск муниципального округа
    mo_match = re.search(r'муниципальный округ\s+([^\s,]+)', address.lower())
    if mo_match:
        return mo_match.group(1).capitalize()
        
    # Хорошо известные районы Москвы (оставляем как есть для совместимости)
    common_districts = [
        "Арбат", "Басманный", "Замоскворечье", "Красносельский", "Мещанский", "Пресненский", "Таганский", "Тверской", "Хамовники", "Якиманка",
        "Аэропорт", "Беговой", "Бескудниковский", "Войковский", "Восточное Дегунино", "Головинский", "Дмитровский", "Западное Дегунино",
        "Коптево", "Левобережный", "Молжаниновский", "Савёловский", "Сокол", "Тимирязевский", "Ховрино", "Хорошёвский",
        "Алексеевский", "Алтуфьевский", "Бабушкинский", "Бибирево", "Бутырский", "Лианозово", "Лосиноостровский", "Марфино",
        "Марьина Роща", "Останкинский", "Отрадное", "Ростокино", "Свиблово", "Северный", "Северное Медведково", "Южное Медведково", "Ярославский",
        "Богородское", "Вешняки", "Восточный", "Восточное Измайлово", "Гольяново", "Ивановское", "Измайлово", "Косино‑Ухтомский",
        "Метрогородок", "Новогиреево", "Новокосино", "Перово", "Преображенское", "Северное Измайлово", "Соколиная Гора", "Сокольники",
        "Выхино‑Жулебино", "Капотня", "Кузьминки", "Лефортово", "Люблино", "Марьино", "Некрасовка", "Нижегородский",
        "Печатники", "Рязанский", "Текстильщики", "Южнопортовый",
        "Бирюлёво Восточное", "Бирюлёво Западное", "Братеево", "Даниловский", "Донской", "Зябликово", "Москворечье‑Сабурово",
        "Нагатино‑Садовники", "Нагатинский Затон", "Нагорный", "Орехово‑Борисово Северное", "Орехово‑Борисово Южное",
        "Царицыно", "Чертаново Северное", "Чертаново Центральное", "Чертаново Южное",
        "Академический", "Гагаринский", "Зюзино", "Коньково", "Котловка", "Ломоносовский", "Обручевский",
        "Северное Бутово", "Тёплый Стан", "Черёмушки", "Южное Бутово", "Ясенево",
        "Дорогомилово", "Крылатское", "Кунцево", "Можайский", "Ново‑Переделкино", "Очаково‑Матвеевское",
        "Проспект Вернадского", "Раменки", "Солнцево", "Тропарёво‑Никулино", "Филёвский Парк", "Фили‑Давыдково",
        "Куркино", "Митино", "Покровское‑Стрешнево", "Северное Тушино", "Строгино", "Хорошёво‑Мнёвники", "Щукино", "Южное Тушино",
        "Крюково", "Матушкино", "Савёлки", "Старое Крюково", "Силино",
        "Внуково", "Коммунарка", "Филимонковский", "Щербинка",
        "Бекасово", "Вороново", "Краснопахорский", "Троицк"
    ]
    
    for district in common_districts:
        if district.lower() in address.lower():
            return district
    
    # Поиск названия города
    city_match = re.search(r'г\.?\s*([^\s,]+)|город\s+([^\s,]+)', address.lower())
    if city_match:
        city = city_match.group(1) or city_match.group(2)
        if city not in ["москва"]:  # Если это не Москва
            return f"г. {city.capitalize()}"
    
    # Административные округа Москвы
    adm_districts = {
        "цао": "Центральный АО",
        "сао": "Северный АО",
        "свао": "Северо-Восточный АО", 
        "вао": "Восточный АО",
        "ювао": "Юго-Восточный АО",
        "юао": "Южный АО",
        "юзао": "Юго-Западный АО",
        "зао": "Западный АО",
        "сзао": "Северо-Западный АО",
        "зелао": "Зеленоградский АО",
        "тинао": "Троицкий и Новомосковский АО"
    }
    
    for short_name, full_name in adm_districts.items():
        if short_name in address.lower() or full_name.lower() in address.lower():
            return full_name
            
    # По умолчанию возвращаем Москва для всех остальных случаев
    return "Москва"

def calculate_median_prices(offers_by_district: Dict[str, List[Offer]]) -> Dict[str, float]:
    """Calculate median price per square meter by district with safe error handling."""
    median_prices = {}
    
    logging.info(f"Расчет медианных цен по {len(offers_by_district)} районам")
    
    for district, offers in offers_by_district.items():
        if not offers or len(offers) < 2:  # ИСПРАВЛЯЕМ: нужно минимум 2 точки данных
            logging.warning(f"Недостаточно данных для района '{district}' ({len(offers)} объявлений)")
            continue
            
        # Фильтруем предложения с корректной площадью
        valid_offers = [offer for offer in offers if offer.area > 0 and offer.price > 0]
        
        if len(valid_offers) < 2:  # ИСПРАВЛЯЕМ: нужно минимум 2 валидных предложения
            logging.warning(f"Недостаточно валидных данных для района '{district}'")
            continue
            
        # Рассчитываем цену за квадратный метр
        prices_per_sqm = [offer.price / offer.area for offer in valid_offers]
        
        if len(prices_per_sqm) < 2:  # ИСПРАВЛЯЕМ: проверяем еще раз
            logging.warning(f"Недостаточно цен для расчета в районе '{district}'")
            continue
        
        try:
            # БЕЗОПАСНЫЙ расчет медианы
            median_price = statistics.median(prices_per_sqm)
            median_prices[district] = median_price
            
            logging.info(f"Район '{district}': {len(valid_offers)} объявлений, медиана {median_price:.0f} ₽/м²")
            
        except Exception as e:
            logging.error(f"Ошибка при расчете медианы для района '{district}': {e}")
            continue
    
    if not median_prices:
        logging.warning("⚠️ Не удалось рассчитать медианные цены ни для одного района")
    
    return median_prices

def export_price_statistics():
    """ОТКЛЮЧАЕМ экспорт статистики для избежания ошибок"""
    logging.info("📊 Экспорт статистики отключен для стабильности")
    return

async def filter_offers_without_geocoding(lot_address: str, offers: List[Offer], district_priority=True) -> List[Offer]:
    """Фильтрует предложения без использования геокодирования, когда API недоступно."""
    logging.info(f"⚠️ Запуск фильтрации без геокодирования для {len(offers)} объявлений")
    
    if not offers:
        return []
        
    lot_district = calculate_district(lot_address)
    logging.info(f"Район лота: {lot_district}")
    
    # Функция для оценки релевантности объявления
    def offer_relevance_score(offer):
        score = 0
        offer_district = getattr(offer, 'district', None) or calculate_district(offer.address)
        
        # Сохраняем расчетный район для использования в других местах
        offer.district = offer_district
        
        # Совпадение района даёт 100 баллов
        if lot_district != "Unknown" and offer_district == lot_district:
            score += 100
        
        # Частичное совпадение адреса даёт до 50 баллов
        lot_address_parts = set(lot_address.lower().split())
        offer_address_parts = set(offer.address.lower().split())
        common_parts = lot_address_parts.intersection(offer_address_parts)
        address_match_score = len(common_parts) / max(1, len(lot_address_parts)) * 50
        score += address_match_score
        
        # Поощряем более точные адреса
        if len(offer.address) > 15:
            score += 10
            
        return score
    
    # Сортируем по релевантности и выбираем лучшие
    scored_offers = [(offer, offer_relevance_score(offer)) for offer in offers]
    scored_offers.sort(key=lambda x: x[1], reverse=True)
    
    # Отбираем объявления с минимальным порогом релевантности
    filtered_offers = [offer for offer, score in scored_offers if score >= 30]
    
    # Если ничего не нашли, смягчаем условия для района
    if not filtered_offers and district_priority:
        logging.info("Смягчаем условия фильтрации из-за отсутствия совпадений по району")
        filtered_offers = [offer for offer, score in scored_offers if score >= 15]
    
    # Если всё еще нет результатов, берём хотя бы 5 лучших
    if not filtered_offers:
        filtered_offers = [offer for offer, _ in scored_offers[:5]]
        
    # Отладочная информация
    if filtered_offers:
        logging.info(f"Фильтрация без геокодирования: отобрано {len(filtered_offers)} из {len(offers)} объявлений")
        for i, offer in enumerate(filtered_offers[:3], 1):
            logging.info(f"  {i}. {offer.address[:50]}... (район: {getattr(offer, 'district', 'не указан')})")
    else:
        logging.warning("Фильтрация без геокодирования не дала результатов")
    
    return filtered_offers

def load_checkpoint():
    """Загружает последний доступный чекпоинт."""
    try:
        # Найдем все файлы чекпоинтов
        checkpoint_files = sorted(
            [f for f in os.listdir(".") if f.startswith("checkpoint_")],
            key=lambda x: os.path.getmtime(x),
            reverse=True
        )
        
        if not checkpoint_files:
            logging.info("🔍 Чекпоинты не найдены, начинаем с нуля")
            return None
            
        # Берем самый свежий
        latest_checkpoint = checkpoint_files[0]
        logging.info(f"🔄 Найден чекпоинт: {latest_checkpoint}, пытаемся восстановить")
        
        with open(latest_checkpoint, "rb") as f:
            checkpoint_data = pickle.load(f)
            
        # Проверка наличия ожидаемых данных
        required_fields = ["lots", "processed_indices", "offers_by_district", "timestamp"]
        if not all(field in checkpoint_data for field in required_fields):
            logging.warning("⚠️ Неполные данные в чекпоинте, начинаем с нуля")
            return None
            
        # Выводим информацию о чекпоинте
        checkpoint_age = time.time() - checkpoint_data["timestamp"]
        logging.info(f"✅ Успешно загружен чекпоинт от {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(checkpoint_data['timestamp']))}")
        logging.info(f"📊 В чекпоинте: {len(checkpoint_data.get('lots', []))} лотов, "
                    f"{len(checkpoint_data.get('processed_indices', []))} обработано, "
                    f"{len(checkpoint_data.get('offers_by_district', {}))} районов, "
                    f"{len(checkpoint_data.get('all_sale_offers', []))} объявлений о продаже, "
                    f"{len(checkpoint_data.get('all_rent_offers', []))} объявлений об аренде")
        
        return checkpoint_data
        
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке чекпоинта: {e}")
        return None
    
def calculate_lot_district_metrics(lot, sale_offers, rent_offers):
    """Расчет метрик для отдельного лота на основе объявлений"""
    
    # Рассчитываем рыночную цену на основе объявлений о продаже
    if sale_offers:
        lot.sale_data = []
        prices_per_sqm = []
        
        # Собираем цены из объявлений в том же районе
        for offer in sale_offers:
            if not hasattr(offer, 'district'):
                offer.district = calculate_district(offer.address)
                
            if offer.district == lot.district and offer.area > 0:
                price_per_sqm = offer.price / offer.area
                prices_per_sqm.append(price_per_sqm)
                lot.sale_data.append(price_per_sqm)
        
        # Если есть данные о продажах, рассчитываем медианную цену
        if prices_per_sqm:
            lot.market_price_per_sqm = statistics.median(prices_per_sqm)
            lot.market_value = lot.market_price_per_sqm * lot.area
            lot.current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
            lot.capitalization_rub = lot.market_value - lot.price
            lot.capitalization_percent = (lot.capitalization_rub / lot.price) if lot.price > 0 else 0
    
    # Рассчитываем доходность на основе объявлений об аренде
    lot.rent_data = []
    if rent_offers:
        for offer in rent_offers:
            if not hasattr(offer, 'district'):
                offer.district = calculate_district(offer.address)
                
            if offer.district == lot.district and offer.area > 0:
                rent_per_sqm = offer.price / offer.area
                lot.rent_data.append(rent_per_sqm)
        
        # Если есть данные об аренде, рассчитываем доходность
        if lot.rent_data:
            lot.average_rent_price_per_sqm = sum(lot.rent_data) / len(lot.rent_data)
            lot.has_rent_data = True
            lot.annual_income = lot.average_rent_price_per_sqm * 12 * lot.area
            lot.monthly_gap = lot.annual_income / 12
            lot.annual_yield_percent = (lot.annual_income / lot.price) if lot.price > 0 else 0
        else:
            # Если нет данных аренды, используем стандартные коэффициенты
            lot.has_rent_data = False
            lot.monthly_gap = lot.market_value * 0.007  # 0.7% в месяц
            lot.annual_yield_percent = (lot.monthly_gap * 12 / lot.price) if lot.price > 0 else 0
    
    logging.info(f"Лот {lot.id}: Метрики рассчитаны - "
               f"Рыночная цена: {getattr(lot, 'market_price_per_sqm', 0):.0f} ₽/м², "
               f"Капитализация: {getattr(lot, 'capitalization_rub', 0):,.0f} ₽ "
               f"({getattr(lot, 'capitalization_percent', 0):.1f}%), "
               f"Доходность: {getattr(lot, 'annual_yield_percent', 0):.1f}%")
    

def save_progress_checkpoint(lots, processed_indices, offers_by_district, district_offer_count, all_sale_offers=None, all_rent_offers=None):
    """Сохраняет текущий прогресс в файл для возможного восстановления."""
    try:
        checkpoint_file = f"checkpoint_{int(time.time())}.pkl"
        with open(checkpoint_file, "wb") as f:
            pickle.dump({
                "lots": lots,
                "processed_indices": processed_indices,
                "offers_by_district": offers_by_district,
                "district_offer_count": district_offer_count,
                "all_sale_offers": all_sale_offers or [],
                "all_rent_offers": all_rent_offers or [],
                "timestamp": time.time()
            }, f)
        logging.info(f"💾 Создан чекпойнт: {checkpoint_file}")
        
        # Удаляем старые чекпойнты, оставляя только 3 последних
        checkpoint_files = sorted(
            [f for f in os.listdir(".") if f.startswith("checkpoint_")],
            key=lambda x: os.path.getmtime(x),
            reverse=True
        )
        
        for old_file in checkpoint_files[3:]:
            try:
                os.remove(old_file)
                logging.debug(f"Удален устаревший чекпойнт: {old_file}")
            except:
                pass
                
    except Exception as e:
        logging.error(f"❌ Ошибка при создании чекпойнта: {e}")

def push_custom_data(sheet_name: str, rows: List[List[Any]]):
    """Вспомогательная функция для выгрузки произвольных данных в указанный лист Google Sheets."""
    from parser.google_sheets import _append, _svc, GSHEET_ID
    
    try:
        # Проверяем существование листа
        sheets_metadata = _svc.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheet_exists = any(sheet['properties']['title'] == sheet_name for sheet in sheets_metadata['sheets'])
        
        if not sheet_exists:
            logging.info(f"Лист '{sheet_name}' не найден. Создаем новый лист.")
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
        logging.info(f"Данные успешно выгружены в лист '{sheet_name}'")
        
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
        logging.error(f"Ошибка при выгрузке данных в лист '{sheet_name}': {e}")

def calculate_intermediate_metrics(processed_lots, offers_by_district, current_idx):
    """Рассчитывает промежуточные метрики для обработанных лотов."""
    logging.info(f"🧮 Расчет промежуточных метрик для {len(processed_lots)} лотов на шаге {current_idx}")
    
    # Рассчитываем медианные цены по имеющимся данным
    median_prices = calculate_median_prices(offers_by_district)
    
    if not median_prices:
        logging.warning("⚠️ Нет данных для расчета медианных цен на данном этапе")
        return processed_lots
        
    # Обновляем метрики для всех обработанных лотов
    updated_lots = []
    for lot in processed_lots:
        # Пропускаем лоты без района или с неизвестным районом
        if not lot.district or lot.district == "Unknown" or lot.district not in median_prices:
            updated_lots.append(lot)
            continue
            
        # Рассчитываем метрики для лота
        calculate_lot_metrics(lot, median_prices)
        updated_lots.append(lot)
    
    logging.info(f"✅ Обновлены метрики для {len(updated_lots)} лотов")
    return updated_lots


async def enhanced_geocoding_address(address: str) -> Optional[Tuple[float, float]]:
    """
    Улучшенная геокодировка адреса с каскадным поиском и очисткой дубликатов
    """
    if not address:
        return None
    
    # Создаем геокодер
    geocoder = create_fixed_geocoder()
    if not geocoder:
        logging.warning("Геокодер недоступен")
        return None
    
    # Используем улучшенный поиск адреса
    result = enhanced_address_search(geocoder, address)
    
    if result:
        lat, lon, found_address, used_variation = result
        logging.info(f"✅ Геокодировка успешна: '{address}' → {lat:.6f}, {lon:.6f}")
        logging.info(f"   Использован вариант: '{used_variation}'")
        return (lon, lat)  # Возвращаем в формате (longitude, latitude)
    else:
        logging.warning(f"❌ Геокодировка не удалась: '{address}'")
        return None

def estimate_market_value_from_rent(lot, rent_prices_per_sqm):
    """Оценка рыночной стоимости на основе арендных ставок через метод капитализации."""
    # Годовой арендный доход
    median_rent = statistics.median(rent_prices_per_sqm)
    annual_income = median_rent * 12 * lot.area
    
    # Определение подходящей ставки капитализации в зависимости от типа объекта
    property_type = getattr(lot.classification, 'category', '').lower() if hasattr(lot, 'classification') else ''
    
    # Ставки капитализации для разных типов недвижимости (упрощенные)
    cap_rates = {
        'офис': 0.09,  # Офисы: 9%
        'стрит': 0.085,  # Стрит-ритейл: 8.5%
        'торговое': 0.08,  # Торговые помещения: 8%
        'склад': 0.095,  # Склады: 9.5%
        'промышленное': 0.1,  # Промышленные объекты: 10%
        'земельный': 0.06,  # Земельные участки: 6%
    }
    
    # Подбор ставки капитализации
    cap_rate = 0.09  # Дефолтное значение - 9%
    for key, rate in cap_rates.items():
        if key in property_type:
            cap_rate = rate
            break
    
    # Расчет рыночной стоимости
    market_value = annual_income / cap_rate
    market_price_per_sqm = market_value / lot.area if lot.area > 0 else 0
    
    return market_value, market_price_per_sqm

def calculate_lot_metrics(lot: Lot, all_sale_offers: List[Offer], all_rent_offers: List[Offer]):
    """
    Рассчитывает метрики для лота на основе отфильтрованных объявлений.
    ОКОНЧАТЕЛЬНАЯ ВЕРСИЯ: проценты как дроби для Google Sheets
    """
    def is_valid_offer(offer):
        if offer.area <= 0 or offer.price <= 0:
            return False
            
        # Проверяем ограничения по площади для офисов
        address_lower = offer.address.lower()
        
        # Признаки офисных помещений
        office_indicators = [
            'офис', 'бизнес-центр', 'бц', 'административное здание',
            'административное помещение', 'офисное здание', 'офисное помещение'
        ]
        
        is_likely_office = any(indicator in address_lower for indicator in office_indicators)
        
        # Если это офис, применяем ограничения по площади
        if is_likely_office:
            if offer.area < 1000 or offer.area > 3000:
                logging.info(f"Фильтрация офиса: площадь {offer.area} м² не в диапазоне 1000-3000 м²")
                return False
                
        return True

    # Фильтруем предложения по валидности
    valid_sale_offers = [o for o in all_sale_offers if is_valid_offer(o)]
    valid_rent_offers = [o for o in all_rent_offers if is_valid_offer(o)]
    
    # Базовые параметры
    purchase_price = lot.price
    holding_years = 5
    months_rented_per_year = 11  # 11 месяцев аренды в году
    expense_ratio = 0.1  # 10% расходов от валового дохода
    
    # Инициализация значений по умолчанию
    lot.market_price_per_sqm = 0
    lot.current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
    lot.market_value = 0
    lot.capitalization_rub = 0
    lot.capitalization_percent = 0
    lot.monthly_gap = 0
    lot.annual_yield_percent = 0
    lot.annual_income = 0
    lot.average_rent_price_per_sqm = 0
    lot.market_value_method = "unknown"
    
    # 1. Рассчитываем рыночную цену и капитализацию ТОЛЬКО на основе объявлений о продаже
    if valid_sale_offers:
        prices_per_sqm = [offer.price / offer.area for offer in valid_sale_offers]
        
        if prices_per_sqm:
            lot.market_price_per_sqm = statistics.median(prices_per_sqm)
            lot.market_value = lot.market_price_per_sqm * lot.area
            lot.capitalization_rub = lot.market_value - lot.price
            
            # ИСПРАВЛЕНО: капитализация как ДРОБЬ (0.256 для 25.6%)
            lot.capitalization_percent = (lot.capitalization_rub / lot.price) if lot.price > 0 else 0
            lot.market_value_method = "sales"
    else:
        # НЕТ объявлений о продаже - НЕ можем посчитать капитализацию
        lot.market_price_per_sqm = 0
        lot.market_value = 0
        lot.capitalization_rub = 0
        lot.capitalization_percent = 0
        lot.market_value_method = "no_sales_data"
    
    # 2. Рассчитываем доходность ТОЛЬКО если есть объявления об аренде
    if valid_rent_offers:
        rent_prices_per_sqm = [offer.price / offer.area for offer in valid_rent_offers]
        
        if rent_prices_per_sqm:
            lot.average_rent_price_per_sqm = statistics.median(rent_prices_per_sqm)
            lot.monthly_gap = lot.average_rent_price_per_sqm * lot.area
            net_monthly_income = lot.monthly_gap * (1 - expense_ratio)
            lot.annual_income = net_monthly_income * months_rented_per_year
            
            # ИСПРАВЛЕНО: доходность как ДРОБЬ (0.256 для 25.6%)
            lot.annual_yield_percent = (lot.annual_income / lot.price) if lot.price > 0 else 0
    else:
        # НЕТ объявлений об аренде - НЕ можем посчитать доходность
        lot.monthly_gap = 0
        lot.annual_yield_percent = 0
        lot.annual_income = 0
        lot.average_rent_price_per_sqm = 0
    
    # 3. Пороговые значения как дроби
    RENTAL_YIELD_THRESHOLD = 0.08  # 8% как 0.08
    CAPITALIZATION_THRESHOLD = 0.15  # 15% как 0.15
    
    # Расчет плюсов
    rental_plus = lot.annual_yield_percent >= RENTAL_YIELD_THRESHOLD
    capitalization_plus = lot.capitalization_percent >= CAPITALIZATION_THRESHOLD
    
    lot.plus_rental = 1 if rental_plus else 0
    lot.plus_sale = 1 if capitalization_plus else 0
    lot.plus_count = lot.plus_rental + lot.plus_sale
    
    # 4. Определение статуса
    if lot.plus_count == 2:
        lot.status = "excellent"
    elif lot.plus_count == 1:
        lot.status = "good"
    elif lot.capitalization_percent > 0 or lot.annual_yield_percent > 0.05:  # 5% как 0.05
        lot.status = "acceptable"
    else:
        lot.status = "poor"
    
    # Логирование (умножаем на 100 только для отображения)
    logging.info(
        f"Лот {lot.id}: Рыночная цена: {lot.market_price_per_sqm:,.0f} ₽/м², "
        f"Капитализация: {lot.capitalization_rub:,.0f} ₽ ({lot.capitalization_percent*100:.1f}%), "
        f"ГАП: {lot.monthly_gap:,.0f} ₽/мес, "
        f"Доходность: {lot.annual_yield_percent*100:.1f}%, "
        f"Плюсы: {lot.plus_count}/2 (аренда:{lot.plus_rental}, продажа:{lot.plus_sale}), "
        f"Статус: {lot.status}"
    )

# Модифицируем функцию filter_offers_by_distance для использования fallback
# Заменить функцию filter_offers_by_distance в parser/main.py
"""
async def filter_offers_by_distance(lot_address: str, offers: List[Offer], max_distance_km: float) -> List[Offer]:
    logger.info(f"🔍 Фильтрация {len(offers)} объявлений для адреса {lot_address[:50]}...")
    
    if not offers:
        return []
    
    # Всегда используем фильтрацию по району из-за проблем с 2GIS API
    lot_district = calculate_district(lot_address)
    logger.info(f"Район лота: {lot_district}")
    
    # 1. Сначала разделим объявления по районам
    offer_by_district = {}
    for offer in offers:
        if not hasattr(offer, 'district') or not offer.district:
            offer.district = calculate_district(offer.address)
        
        if offer.district not in offer_by_district:
            offer_by_district[offer.district] = []
        offer_by_district[offer.district].append(offer)
    
    # 2. Сначала берём объявления из того же района
    filtered_offers = []
    if lot_district in offer_by_district:
        filtered_offers.extend(offer_by_district[lot_district])
        logger.info(f"Найдено {len(filtered_offers)} объявлений в районе '{lot_district}'")
    
    # 3. Если мало объявлений из того же района, добавляем из соседних районов 
    if len(filtered_offers) < 5:
        # Соседние районы (пока просто любые другие)
        other_districts = [d for d in offer_by_district.keys() if d != lot_district]
        
        for district in other_districts:
            # Добавляем до 3 объявлений из каждого другого района
            filtered_offers.extend(offer_by_district[district][:3])
            if len(filtered_offers) >= 10:  # Ограничиваем общее количество
                break
    
    # 4. Если всё еще мало объявлений, берём по одному из каждого района
    if len(filtered_offers) < 3 and offer_by_district:
        for district, district_offers in offer_by_district.items():
            if district_offers and district != lot_district:
                filtered_offers.append(district_offers[0])  # Одно объявление из района
    
    # 5. Последнее средство - просто возвращаем первые несколько объявлений
    if not filtered_offers and offers:
        filtered_offers = offers[:5]  # Берём первые 5 объявлений
    
    # Расчет "pseudo-distance" для совместимости с остальным кодом
    for offer in filtered_offers:
        # Если это объявление из того же района, ставим малое расстояние
        if offer.district == lot_district:
            offer.distance_to_lot = round(random.uniform(0.5, 2.9), 1)  # 0.5 - 2.9 км
        else:
            # Если из другого района - большее расстояние
            offer.distance_to_lot = round(random.uniform(3.0, 8.0), 1)  # 3.0 - 8.0 км
    
    logger.info(f"✅ Отфильтровано {len(filtered_offers)} из {len(offers)} объявлений")
    
    # ВАЖНО: Сохраняем все объявления в глобальную переменную для записи в таблицу
    if CONFIG.get("save_all_offers", False):
        for offer in offers:
            if not hasattr(offer, 'distance_to_lot'):
                offer.distance_to_lot = 999.0  # Помечаем как далекие
        
        # Добавляем их в глобальный кэш всех объявлений
        global all_raw_offers
        if 'all_raw_offers' not in globals():
            all_raw_offers = []
        all_raw_offers.extend(offers)
    
    return filtered_offers
"""
from parser.google_sheets import setup_all_headers, push_custom_data


async def filter_offers_by_distance(lot_address: str, offers: List[Offer], max_distance_km: float) -> List[Offer]:
    """
    Улучшенная фильтрация объявлений с использованием каскадной геокодировки
    """
    logger.info(f"🔍 Улучшенная фильтрация {len(offers)} объявлений для адреса {lot_address[:50]}...")
    
    if not offers:
        return []
    
    # Пытаемся получить координаты лота с улучшенной геокодировкой
    lot_coords = await enhanced_geocoding_address(lot_address)
    
    if not lot_coords:
        # Fallback к фильтрации по району
        logging.warning("⚠️ Геокодировка не удалась, используем фильтрацию по району")
        return await filter_offers_without_geocoding(lot_address, offers, district_priority=True)

    logger.info(f"Координаты лота: lon={lot_coords[0]:.6f}, lat={lot_coords[1]:.6f}")

    filtered_offers = []
    geocoding_failures = 0
    
    for i, offer in enumerate(offers, 1):
        logger.debug(f"Обработка объявления {i}/{len(offers)}: {offer.id}")
        
        if not offer.address:
            logger.debug(f"Пропуск объявления {offer.id} – нет адреса")
            continue

        # Используем улучшенную геокодировку для объявления
        offer_coords = await enhanced_geocoding_address(offer.address)
        
        if not offer_coords:
            logger.warning(f"Пропуск объявления {offer.id} – геокодировка не удалась для '{offer.address}'")
            geocoding_failures += 1
            continue

        logger.debug(f"Объявление {offer.id} координаты: lon={offer_coords[0]:.6f}, lat={offer_coords[1]:.6f}")

        # Расчет расстояния (простое евклидово расстояние как приближение)
        try:
            # Простой расчет расстояния в км (приближенно для Московского региона)
            lat_diff = offer_coords[1] - lot_coords[1]
            lon_diff = offer_coords[0] - lot_coords[0]
            distance_deg = (lat_diff**2 + lon_diff**2)**0.5
            distance_km = distance_deg * 111  # Примерно 111 км на градус
            
            if distance_km <= max_distance_km:
                offer.distance_to_lot = round(distance_km, 2)
                filtered_offers.append(offer)
                logger.info(f"Включено объявление {offer.id} – {distance_km:.2f} км")
            else:
                logger.debug(f"Исключено объявление {offer.id} – {distance_km:.2f} км > {max_distance_km} км")
                
        except Exception as e:
            logger.warning(f"Ошибка расчета расстояния для объявления {offer.id}: {e}")
            continue

    logger.info(f"✅ Улучшенная фильтрация завершена: отобрано {len(filtered_offers)} из {len(offers)} объявлений")
    logger.info(f"   Неудачи геокодировки: {geocoding_failures}")
    
    return filtered_offers


async def main():
    """
    Основная функция программы. Получает лоты с торгов, ищет похожие 
    объявления на ЦИАН, рассчитывает метрики и сохраняет результаты.
    """
    try:
        # Инициализируем Telegram бота
        bot_service.initialize()
        
        # Настраиваем заголовки всех таблиц
        setup_all_headers()
        
        # Проверяем аргументы командной строки для возобновления
        resume_from_checkpoint = "--resume" in sys.argv
        
        # Инициализируем базовые переменные
        browser_operations = 0
        browser_refresh_interval = CONFIG.get("browser_refresh_interval", 20)
        lot_save_interval = CONFIG.get("lot_save_interval", 5)
        
        # Добавляем поддержку отладочного радиуса
        debug_radius = CONFIG.get("debug_search_radius", 3)  # Радиус для отладки - изменено на 3 км
        search_radius = CONFIG.get("area_search_radius", 3)  # Радиус поиска - изменено на 3 км
        
        # Глобальные коллекции для хранения данных
        all_sale_offers = []
        all_rent_offers = []
        offers_by_district = defaultdict(list)
        district_offer_count = defaultdict(int)
        processed_lots = []
        
        if resume_from_checkpoint:
            # Пытаемся загрузить чекпоинт
            checkpoint = load_checkpoint()
            
            if checkpoint:
                # Восстанавливаем состояние
                lots = checkpoint.get("lots", [])
                processed_indices = set(checkpoint.get("processed_indices", []))
                offers_by_district = defaultdict(list, checkpoint.get("offers_by_district", {}))
                district_offer_count = defaultdict(int, checkpoint.get("district_offer_count", {}))
                all_sale_offers = checkpoint.get("all_sale_offers", [])
                all_rent_offers = checkpoint.get("all_rent_offers", [])
                processed_lots = [lots[i] for i in processed_indices if i < len(lots)]
                
                # Определяем, с какого индекса продолжить
                start_idx = max(processed_indices) + 1 if processed_indices else 0
                logging.info(f"🔄 Возобновляем обработку с лота #{start_idx+1} из {len(lots)}")
                logging.info(f"📊 Восстановлены данные: {len(all_sale_offers)} объявлений о продаже, {len(all_rent_offers)} объявлений об аренде")
            else:
                # Не удалось восстановить, начинаем с нуля
                logging.info("⚠️ Не удалось восстановить из чекпоинта. Начинаем с нуля.")
                lots = await fetch_lots(max_pages=10)
                processed_indices = set()
                start_idx = 0
        else:
            # Начинаем с нуля
            logging.info("🔄 Запускаем обработку с нуля (без восстановления)")
            lots = await fetch_lots(max_pages=10)
            processed_indices = set()
            start_idx = 0
        
        logging.info(f"✅ Получено {len(lots)} лотов для обработки")
        
        # Проверка работоспособности CIAN-парсера
        cian_metrics = get_cian_metrics()
        logging.info(f"Статус CIAN-парсера: {cian_metrics}")
        
        # Инициализируем коллекции для пакетной обработки
        current_batch_sale = []
        current_batch_rent = []
        batch_size = 1  # Размер пакета для сохранения
        
        # Основной цикл обработки лотов, начиная с start_idx
        for i in range(start_idx, len(lots)):
            lot = lots[i]
            
            # Determine if the address has sufficient components for narrowed search
            address_components = calculate_address_components(lot.address)
            
            # Skip CIAN parsing if we don't have enough address information
            # We need at least district, street, or city/settlement to narrow search
            if (address_components["confidence"] < 0.5 or
                (not address_components["district"] and 
                not address_components["street"] and
                not (address_components["city"] or address_components["settlement"]))):
                
                logging.warning(f"⚠️ Insufficient address components for lot {lot.id}: '{lot.name}'")
                logging.warning(f"Address: '{lot.address}' - would require city-wide search")
                
                # Set all metrics to zero
                lot.market_price_per_sqm = 0.0
                lot.market_value = 0.0
                lot.capitalization_rub = 0.0
                lot.capitalization_percent = 0.0
                lot.monthly_gap = 0.0
                lot.annual_yield_percent = 0.0
                lot.annual_income = 0.0
                lot.average_rent_price_per_sqm = 0.0
                lot.sale_offers_count = 0
                lot.rent_offers_count = 0
                lot.filtered_sale_offers_count = 0
                lot.filtered_rent_offers_count = 0
                lot.plus_rental = 0
                lot.plus_sale = 0
                lot.plus_count = 0
                lot.status = "insufficient_address"
                
                # Save this lot with zero metrics
                processed_lots.append(lot)
                continue
            try:
                lot = lots[i]
                
                # Определяем район лота
                if not hasattr(lot, 'district') or not lot.district:
                    lot.district = calculate_district(lot.address)
                logging.info(f"Лот {lot.id}: '{lot.name}' находится в районе '{lot.district}'")
                
                # Готовим фильтр для поиска на ЦИАН
                lot_uuid = lot.uuid
                search_filter = unformatted_address_to_cian_search_filter(lot.address)
                logging.info(f"Поиск по фильтру: {search_filter}")
                
                # Skip CIAN parsing if the filter is for the entire Moscow or Moscow Oblast region
                if search_filter == "region=1" or search_filter == "region=4593":
                    logging.warning(f"⚠️ Слишком широкий фильтр для лота {lot.id}: '{lot.name}' - {search_filter}")
                    logging.warning(f"Адрес: '{lot.address}' - пропускаем парсинг ЦИАН для экономии времени")
                    
                    # Set all metrics to zero
                    lot.market_price_per_sqm = 0.0
                    lot.current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
                    lot.market_value = 0.0
                    lot.capitalization_rub = 0.0
                    lot.capitalization_percent = 0.0
                    lot.monthly_gap = 0.0
                    lot.annual_yield_percent = 0.0
                    lot.annual_income = 0.0
                    lot.average_rent_price_per_sqm = 0.0
                    lot.sale_offers_count = 0
                    lot.rent_offers_count = 0
                    lot.filtered_sale_offers_count = 0
                    lot.filtered_rent_offers_count = 0
                    lot.plus_rental = 0
                    lot.plus_sale = 0
                    lot.plus_count = 0
                    lot.status = "region_too_broad"
                    
                    # Save lot with zero metrics and continue to next lot
                    processed_lots.append(lot)
                    processed_indices.add(i)
                    push_lots([lot], "lots_all")
                    logging.info(f"✅ Сохранен лот {lot.id} с нулевыми метриками в таблицу lots_all")
                    continue
                    
                # If we get here, we have a more specific filter, so continue with normal processing
                # Получаем объявления с ЦИАН
                sale_offers, rent_offers = fetch_nearby_offers(search_filter, lot_uuid)
                logging.info(f"Получено {len(sale_offers)} объявлений о продаже и {len(rent_offers)} объявлений об аренде")
                
                # Добавляем счетчики к лоту
                lot.sale_offers_count = len(sale_offers)
                lot.rent_offers_count = len(rent_offers)
                
                # Обновляем счетчик браузерных операций и перезагружаем если нужно
                browser_operations += 1
                if browser_operations >= browser_refresh_interval:
                    logging.info(f"🔄 Перезагрузка браузера после {browser_operations} операций")
                    try:
                        parser = get_parser()
                        parser.refresh_session()
                        browser_operations = 0
                        logging.info("✅ Браузер успешно перезагружен")
                    except Exception as browser_error:
                        logging.error(f"❌ Ошибка при перезагрузке браузера: {browser_error}")
                
                # Сохраняем все полученные объявления в глобальную коллекцию
                if hasattr(sale_offers, 'copy'):
                    all_raw_sale = sale_offers.copy()
                else:
                    all_raw_sale = list(sale_offers)
                
                if hasattr(rent_offers, 'copy'):
                    all_raw_rent = rent_offers.copy() 
                else:
                    all_raw_rent = list(rent_offers)
                
                # Выбираем эффективный радиус поиска
                effective_radius = debug_radius if debug_radius else search_radius
                
                # Увеличиваем радиус, если не хватает объявлений
                if not debug_radius and (len(sale_offers) < 3 or len(rent_offers) < 3) and search_radius < 10:
                    logging.info(f"Увеличиваем радиус поиска до 10 км из-за малого количества объявлений")
                    effective_radius = 7
                    
                # Если нет объявлений вообще, используем особый режим
                if not sale_offers and not rent_offers and not debug_radius:
                    logging.warning("⚠️ Нет объявлений, включаем режим отладки (радиус 1000 км)")
                    effective_radius = 10
                
                # Фильтруем объявления по расстоянию от лота
                logging.info(f"Фильтрация объявлений по расстоянию (макс. {effective_radius} км) для лота {lot.id}")
                
                # Используем безопасную фильтрацию с резервным механизмом
                try:
                    filtered_sale_offers = await filter_offers_by_distance(lot.address, sale_offers, effective_radius)
                except Exception as e:
                    logging.error(f"Ошибка при фильтрации объявлений о продаже: {e}")
                    filtered_sale_offers = await filter_offers_without_geocoding(lot.address, sale_offers)
                
                try:
                    filtered_rent_offers = await filter_offers_by_distance(lot.address, rent_offers, effective_radius)
                except Exception as e:
                    logging.error(f"Ошибка при фильтрации объявлений об аренде: {e}")
                    filtered_rent_offers = await filter_offers_without_geocoding(lot.address, rent_offers)
                
                # Обновляем статистику по районам
                for offer in filtered_sale_offers:
                    if not hasattr(offer, 'district') or not offer.district:
                        offer.district = calculate_district(offer.address)
                    offers_by_district[offer.district].append(offer)
                    district_offer_count[offer.district] += 1
                
                # Добавляем в пакеты для последующей записи
                current_batch_sale.extend(filtered_sale_offers)
                current_batch_rent.extend(filtered_rent_offers)
                all_sale_offers.extend(filtered_sale_offers)
                all_rent_offers.extend(filtered_rent_offers)
                lot.filtered_sale_offers_count = len(filtered_sale_offers)
                lot.filtered_rent_offers_count = len(filtered_rent_offers)
                # ВАЖНО: Рассчитываем метрики на основе ВСЕХ объявлений
                # а не только отфильтрованных
                calculate_lot_metrics(lot, filtered_sale_offers, filtered_rent_offers)
                
                # Добавляем классификацию объекта через GPT
                if CONFIG.get("gpt_analysis_enabled", False):
                    try:
                        lot.classification = await classify_property(lot)
                    except Exception as e:
                        logging.error(f"Ошибка при классификации объекта {lot.id}: {e}")
                        # Создаем пустую классификацию если произошла ошибка
                        lot.classification = PropertyClassification()
                else:
                    lot.classification = PropertyClassification()
                
                # Сохраняем лот в основную таблицу
                push_lots([lot], "lots_all")
                logging.info(f"✅ Сохранен лот {lot.id} в таблицу lots_all")
                
                # Отправляем уведомление в Telegram, если бот включен
                if bot_service.is_enabled():
                    try:
                        await bot_service.notify_new_lots([lot])
                    except Exception as e:
                        logging.error(f"Ошибка при отправке уведомления о лоте {lot.id}: {e}")
                
                # Сохраняем обработанный лот
                processed_lots.append(lot)
                processed_indices.add(i)
                
                # Отправляем пакеты объявлений в Google Sheets
                if len(current_batch_sale) >= batch_size:
                    logging.info(f"Сохраняем пакет из {len(current_batch_sale)} объявлений о продаже")
                    push_offers("cian_sale_all", current_batch_sale)
                    current_batch_sale = []
                    
                if len(current_batch_rent) >= batch_size:
                    logging.info(f"Сохраняем пакет из {len(current_batch_rent)} объявлений об аренде")
                    push_offers("cian_rent_all", current_batch_rent)
                    current_batch_rent = []
                
                # Периодическое сохранение контрольной точки
                if i % lot_save_interval == 0 or i == len(lots) - 1:
                    # Сохраняем чекпоинт
                    save_progress_checkpoint(
                        lots=lots,
                        processed_indices=list(processed_indices),
                        offers_by_district=dict(offers_by_district),
                        district_offer_count=dict(district_offer_count),
                        all_sale_offers=all_sale_offers,
                        all_rent_offers=all_rent_offers
                    )
                    logging.info(f"💾 Создана контрольная точка для {len(processed_lots)} лотов")
                
                # Добавляем небольшую паузу между лотами
                await asyncio.sleep(1)
                
            except Exception as e:
                logging.error(f"❌ Ошибка при обработке лота {getattr(lot, 'id', 'unknown')}: {e}", exc_info=True)
                # Не прерываем всю обработку из-за одного лота
                continue
        
        # Сохраняем оставшиеся объявления
        if current_batch_sale:
            logging.info(f"Сохраняем оставшиеся {len(current_batch_sale)} объявлений о продаже")
            push_offers("cian_sale_all", current_batch_sale)
            
        if current_batch_rent:
            logging.info(f"Сохраняем оставшиеся {len(current_batch_rent)} объявлений об аренде")
            push_offers("cian_rent_all", current_batch_rent)

        apply_all_formatting()
        logging.info(f"🎨 Применено промежуточное форматирование на шаге {i}")

        # Рассчитываем и экспортируем статистику по районам
        median_prices = calculate_median_prices(offers_by_district)
        
        if offers_by_district and any(len(offers) >= 2 for offers in offers_by_district.values()):
            try:
                median_prices = calculate_median_prices(offers_by_district)
                logging.info(f"✅ Рассчитаны медианные цены для {len(median_prices)} районов")
            except Exception as e:
                logging.error(f"❌ Ошибка при расчете медианных цен: {e}")
                median_prices = {}
        else:
            logging.info("⏭️ Пропускаем расчет медианных цен (недостаточно данных)")
            median_prices = {}

        # Отправляем окончательную статистику по районам
        if district_offer_count and len(district_offer_count) > 0:
            try:
                logging.info(f"Отправка статистики по {len(district_offer_count)} районам")
                push_district_stats(dict(district_offer_count))
            except Exception as e:
                logging.error(f"❌ Ошибка при отправке статистики районов: {e}")
        else:
            # Создаем минимальную заглушку
            logging.info("⏭️ Отправляем заглушку для статистики районов")
            try:
                push_district_stats({"Москва": 0, "Московская область": 0})
            except Exception as e:
                logging.error(f"❌ Ошибка при отправке заглушки: {e}")
        
        # Отправляем ежедневную сводку в Telegram
        if bot_service.is_enabled() and processed_lots:
            try:
                # Считаем рекомендованные лоты
                recommended_count = sum(1 for lot in processed_lots 
                                      if getattr(lot, 'annual_yield_percent', 0) >= CONFIG.get('market_yield_threshold', 10))
                
                await bot_service.send_daily_summary(len(processed_lots), recommended_count)
                logging.info(f"Отправлена ежедневная сводка: {len(processed_lots)} лотов, {recommended_count} рекомендованных")
            except Exception as e:
                logging.error(f"Ошибка при отправке ежедневной сводки: {e}")
        
        logging.info("✅ Обработка успешно завершена!")
            
    except Exception as e:
        logging.critical(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}", exc_info=True)
        # Сохраняем состояние для отладки
        try:
            with open(f"crash_dump_{int(time.time())}.pkl", "wb") as f:
                pickle.dump({
                    "lots": locals().get("lots", []),
                    "offers_by_district": locals().get("offers_by_district", {}),
                    "error": str(e)
                }, f)
            logging.info("✅ Сохранено состояние для отладки")
        except Exception as dump_error:
            logging.error(f"❌ Не удалось сохранить состояние: {dump_error}")
            
if __name__ == "__main__":
    asyncio.run(main())