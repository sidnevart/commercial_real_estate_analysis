import asyncio
import logging
import statistics
import re
import time
import os
import pickle
import pytz
import json
import sys
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any, Optional
from parser.torgi_async import fetch_lots
from parser.cian_minimal import fetch_nearby_offers, unformatted_address_to_cian_search_filter
from parser.google_sheets import push_lots, push_offers, push_district_stats
from parser.gpt_classifier import classify_property  
from parser.cian_minimal import get_parser
from parser.geo_utils import filter_offers_by_distance
from core.models import Lot, Offer, PropertyClassification
from core.config import CONFIG
#from parser.geo_utils import filter_offers_by_distance
import random

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Глобальная переменная для хранения статистики по ценам районов
district_price_stats = {}


seen_offer_ids = set()
seen_offer_signatures = set()

# Создать функцию для проверки дубликатов
def is_duplicate_offer(offer):
    """Проверяет, является ли объявление дубликатом."""
    # Проверка по ID
    if offer.id in seen_offer_ids:
        return True
    
    # Проверка по комбинации адреса, площади и цены
    signature = f"{offer.address}|{offer.area}|{offer.price}"
    if signature in seen_offer_signatures:
        return True
        
    # Если не дубликат, добавляем в кэш
    seen_offer_ids.add(offer.id)
    seen_offer_signatures.add(signature)
    return False

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
    """Улучшенная функция определения района из адреса."""
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
        
    # Хорошо известные районы Москвы 
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
    return "Москва" # Default to Moscow instead of Unknown

def calculate_median_prices(offers_by_district: Dict[str, List[Offer]]) -> Dict[str, float]:
    """Calculate median price per square meter by district with detailed logging."""
    global district_price_stats
    
    median_prices = {}
    district_stats = {}
    
    logging.info(f"Расчет медианных цен по {len(offers_by_district)} районам")
    
    for district, offers in offers_by_district.items():
        if not offers:
            logging.warning(f"Нет объявлений для района '{district}'")
            continue
            
        # Фильтруем предложения с корректной площадью
        valid_offers = [offer for offer in offers if offer.area > 0]
        
        if len(valid_offers) < len(offers):
            logging.warning(f"В районе '{district}' найдено {len(offers) - len(valid_offers)} объявлений с нулевой площадью")
        
        if not valid_offers:
            logging.warning(f"Нет объявлений с корректной площадью для района '{district}'")
            continue
            
        # Рассчитываем цену за квадратный метр
        prices_per_sqm = [offer.price / offer.area for offer in valid_offers]
        
        if not prices_per_sqm:
            logging.warning(f"Не удалось рассчитать цены за м² для района '{district}'")
            continue
            
        # Рассчитываем статистику
        min_price = min(prices_per_sqm)
        max_price = max(prices_per_sqm)
        avg_price = sum(prices_per_sqm) / len(prices_per_sqm)
        median_price = statistics.median(prices_per_sqm)
        
        # Проверка на выбросы (опционально)
        q1 = statistics.quantiles(prices_per_sqm, n=4)[0]
        q3 = statistics.quantiles(prices_per_sqm, n=4)[2]
        iqr = q3 - q1
        
        # Фильтрация выбросов
        filtered_prices = [p for p in prices_per_sqm if q1 - 1.5 * iqr <= p <= q3 + 1.5 * iqr]
        
        outliers_count = len(prices_per_sqm) - len(filtered_prices)
        if outliers_count > 0:
            logging.info(f"В районе '{district}' обнаружено {outliers_count} выбросов цен")
            if filtered_prices:
                filtered_median = statistics.median(filtered_prices)
                logging.info(f"Медиана после фильтрации: {filtered_median:.0f} ₽/м² (было {median_price:.0f} ₽/м²)")
                median_price = filtered_median
        
        median_prices[district] = median_price
        
        # Сохраняем статистику для логирования и возможного экспорта
        district_stats[district] = {
            "count": len(valid_offers),
            "min": min_price,
            "max": max_price,
            "avg": avg_price,
            "median": median_price,
            "outliers": outliers_count
        }
        
        logging.info(
            f"Район '{district}': {len(valid_offers)} объявлений, "
            f"цены {min_price:.0f} - {max_price:.0f} ₽/м², "
            f"медиана {median_price:.0f} ₽/м²"
        )
    
    # Логируем итоговые результаты
    if median_prices:
        avg_median = sum(median_prices.values()) / len(median_prices)
        min_median = min(median_prices.values())
        max_median = max(median_prices.values())
        
        logging.info(f"Итого рассчитаны медианы для {len(median_prices)} районов")
        logging.info(f"Диапазон медиан по районам: {min_median:.0f} - {max_median:.0f} ₽/м², в среднем {avg_median:.0f} ₽/м²")
    else:
        logging.warning("Не удалось рассчитать медианные цены ни для одного района")
    
    # Сохраняем статистику для возможного использования
    district_price_stats = district_stats
    
    return median_prices

def export_price_statistics():
    """Экспортирует детальную статистику по ценам в отдельный лист Google Sheets."""
    global district_price_stats
    
    if not district_price_stats:
        logging.warning("Нет данных статистики для экспорта")
        return
        
    try:
        headers = ["Район", "Кол-во объявлений", "Мин. цена ₽/м²", "Макс. цена ₽/м²", 
                  "Средняя цена ₽/м²", "Медианная цена ₽/м²", "Кол-во выбросов"]
                  
        rows = [headers]
        
        for district, stats in district_price_stats.items():
            row = [
                district,
                stats["count"],
                round(stats["min"]),
                round(stats["max"]),
                round(stats["avg"]),
                round(stats["median"]),
                stats["outliers"]
            ]
            rows.append(row)
            
        push_custom_data("price_statistics", rows)
        logging.info(f"Экспортирована статистика цен по {len(district_price_stats)} районам")
        
    except Exception as e:
        logging.error(f"Ошибка при экспорте статистики цен: {e}")

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
            lot.capitalization_percent = (lot.capitalization_rub / lot.price) * 100 if lot.price > 0 else 0
    
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
            lot.annual_yield_percent = (lot.annual_income / lot.price) * 100 if lot.price > 0 else 0
        else:
            # Если нет данных аренды, используем стандартные коэффициенты
            lot.has_rent_data = False
            lot.monthly_gap = lot.market_value * 0.007  # 0.7% в месяц
            lot.annual_yield_percent = (lot.monthly_gap * 12 / lot.price) * 100 if lot.price > 0 else 0
    
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

DISTRICT_PRICE_FLOOR = {
    "Красносельский": 300000,  # Красносельский район: минимум 300 тыс/м²
    "Тверской": 400000,        # Тверской район: минимум 400 тыс/м²
    "Басманный": 280000,       # Басманный район: минимум 280 тыс/м²
    "Пресненский": 350000,     # Пресненский район: минимум 350 тыс/м²
}
# Добавляем в parser/main.py
def calculate_lot_metrics(lot: Lot, filtered_sale_offers: List[Offer], filtered_rent_offers: List[Offer]):
    """
    Расчет финансовых метрик объекта на основе отфильтрованных объявлений.
    Использует запрос к GPT для интеллектуальной обработки данных.
    При ошибке или отключенном GPT использует обычный расчет.
    """
    # Сохраняем старую функцию для резервного использования
    try:
        if CONFIG.get("gpt_analysis_enabled", False):
            # Для асинхронной функции используем синхронную обертку
            loop = asyncio.get_running_loop()
            # Используем run_in_executor для запуска асинхронной функции в синхронном контексте
            future = asyncio.run_coroutine_threadsafe(
                calculate_lot_metrics_with_gpt(lot, filtered_sale_offers, filtered_rent_offers),
                loop
            )
            # Ждем результата с таймаутом
            return future.result(timeout=30)
        else:
            logging.info(f"GPT анализ отключен в настройках, используется стандартный расчет метрик")
            return calculate_lot_metrics_standard(lot, filtered_sale_offers, filtered_rent_offers)
    except Exception as e:
        logging.error(f"Ошибка при расчете метрик через GPT для лота {lot.id}: {e}")
        logging.info(f"Переключаемся на стандартный метод расчета метрик")
        return calculate_lot_metrics_standard(lot, filtered_sale_offers, filtered_rent_offers)

async def calculate_lot_metrics_with_gpt(lot: Lot, filtered_sale_offers: List[Offer], filtered_rent_offers: List[Offer]):
    """
    Расчет финансовых метрик с использованием GPT.
    Получает входные данные, формирует запрос и обрабатывает ответ.
    """
    from core.gpt_tunnel_client import chat
    import json
    import re

    # Фильтруем только валидные объявления
    valid_sale_offers = [o for o in filtered_sale_offers if o.area > 0 and o.price > 0]
    valid_rent_offers = [o for o in filtered_rent_offers if o.area > 0 and o.price > 0]
    
    # Логирование найденных предложений
    logging.info(f"Лот {lot.id}: Найдено {len(valid_sale_offers)} валидных объявлений о продаже и {len(valid_rent_offers)} об аренде")
    
    # 1. Подготовка данных для передачи в GPT
    
    # Рассчитываем медианную цену продажи (₽/м²) если есть данные
    market_price_per_sqm = 0
    if valid_sale_offers:
        prices_per_sqm = [o.price / o.area for o in valid_sale_offers]
        if prices_per_sqm:
            market_price_per_sqm = statistics.median(prices_per_sqm)
    
    # Рассчитываем медианную арендную ставку (₽/м²/месяц) если есть данные
    avg_rent_price_per_sqm = 0
    if valid_rent_offers:
        rent_prices_per_sqm = [o.price / o.area for o in valid_rent_offers]
        if rent_prices_per_sqm:
            avg_rent_price_per_sqm = statistics.median(rent_prices_per_sqm)
    
    # 2. Формирование запроса к GPT
    prompt = CONFIG.get("gpt_metrics_template", "").format(
        lot_id=lot.id,
        name=lot.name,
        area=lot.area,
        price=lot.price,
        district=lot.district or "Неизвестно",
        category=lot.property_category or "Коммерческая недвижимость",
        market_price_per_sqm=int(market_price_per_sqm),
        avg_rent_price_per_sqm=int(avg_rent_price_per_sqm),
        sale_offers_count=len(valid_sale_offers),
        rent_offers_count=len(valid_rent_offers)
    )
    
    # Если шаблона нет, используем стандартный метод
    if not prompt:
        logging.error(f"Шаблон gpt_metrics_template не найден в конфигурации")
        return calculate_lot_metrics_standard(lot, filtered_sale_offers, filtered_rent_offers)
    
    # 3. Отправка запроса в GPT и получение ответа
    logging.info(f"Отправляем запрос в GPT для расчета метрик лота {lot.id}")
    
    MODEL = "gpt-3.5-turbo"  # Можно использовать и gpt-4o-mini если нужно более качественное решение
    
    try:
        raw_response = await chat(
            MODEL,
            [{"role": "user", "content": prompt}],
            max_tokens=800,
        )
        
        logging.debug(f"Получен ответ от GPT для лота {lot.id}: {raw_response[:100]}...")
        
        # 4. Извлечение JSON из ответа GPT
        json_pattern = r'({[\s\S]*?})'
        json_match = re.search(json_pattern, raw_response)
        
        if json_match:
            metrics_data = json.loads(json_match.group(1))
        else:
            raise ValueError("Не удалось извлечь JSON из ответа GPT")
            
        # 5. Применение полученных значений к лоту
        # Копируем все ключи из результата в атрибуты лота
        for key, value in metrics_data.items():
            setattr(lot, key, value)
        
        # 6. Логирование результатов
        logging.info(f"Лот {lot.id}: Метрики рассчитаны через GPT - "
                   f"Рыночная цена: {lot.market_price_per_sqm:.0f} ₽/м², "
                   f"Капитализация: {lot.capitalization_rub:,.0f} ₽ ({lot.capitalization_percent:.1f}%), "
                   f"ГАП: {lot.monthly_gap:,.0f} ₽/мес, "
                   f"Доходность: {lot.annual_yield_percent:.1f}%, "
                   f"Плюсики: {lot.plus_count}/2, "
                   f"Статус: {lot.status}")
                   
        return lot
        
    except Exception as e:
        logging.error(f"Ошибка при использовании GPT для расчета метрик лота {lot.id}: {e}")
        # Если произошла ошибка, используем стандартный метод
        return calculate_lot_metrics_standard(lot, filtered_sale_offers, filtered_rent_offers)

def calculate_lot_metrics_standard(lot: Lot, filtered_sale_offers: List[Offer], filtered_rent_offers: List[Offer]):
    """
    Стандартный метод расчета финансовых метрик объекта на основе отфильтрованных объявлений.
    Использует формулы из технического задания для определения доходности и капитализации.
    """
    # Фильтруем только валидные объявления
    valid_sale_offers = [o for o in filtered_sale_offers if o.area > 0 and o.price > 0]
    valid_rent_offers = [o for o in filtered_rent_offers if o.area > 0 and o.price > 0]
    
    # Более детальное логирование найденных предложений
    logging.info(f"Лот {lot.id}: Найдено {len(valid_sale_offers)} валидных объявлений о продаже")
    logging.info(f"Параметры лота: площадь {lot.area} м², цена {lot.price:,} ₽, ставка {lot.price/lot.area:,.0f} ₽/м²")
    
    # Логируем детали по первым объявлениям
    for i, offer in enumerate(valid_sale_offers[:5], 1):
        price_per_sqm = offer.price / offer.area
        logging.info(f"  Продажа #{i}: {offer.address}, {offer.area} м², {price_per_sqm:,.0f} ₽/м²")
    
    # 1. СЕГМЕНТАЦИЯ ПО РАЗМЕРУ:
    # Сегментируем предложения по размеру относительно лота
    if valid_sale_offers and lot.area > 0:
        # Определяем диапазоны для категоризации объектов по площади
        area_similar_range = (0.5 * lot.area, 2.0 * lot.area)  # 50% - 200% от площади лота
        
        # Разделяем на категории
        similar_size_offers = []
        other_size_offers = []
        
        for offer in valid_sale_offers:
            if area_similar_range[0] <= offer.area <= area_similar_range[1]:
                similar_size_offers.append(offer)
            else:
                other_size_offers.append(offer)
        
        # Логируем результаты сегментации
        logging.info(f"Сегментация по площади: {len(similar_size_offers)} объектов схожей площади, "
                     f"{len(other_size_offers)} других объектов")
        
        # Определяем предложения для анализа, отдавая приоритет объектам схожего размера
        offers_to_analyze = similar_size_offers if len(similar_size_offers) >= 3 else valid_sale_offers
    else:
        offers_to_analyze = valid_sale_offers
    
    # 2. УЛУЧШЕННАЯ ФИЛЬТРАЦИЯ ВЫБРОСОВ:
    if offers_to_analyze:
        # Вычисляем цены за м² для анализируемых предложений
        prices_per_sqm = [offer.price / offer.area for offer in offers_to_analyze]
        original_prices = prices_per_sqm.copy()
        
        # Логируем исходный диапазон цен
        if prices_per_sqm:
            logging.info(f"Исходный диапазон цен: {min(prices_per_sqm):,.0f} - {max(prices_per_sqm):,.0f} ₽/м², "
                         f"среднее: {sum(prices_per_sqm)/len(prices_per_sqm):,.0f} ₽/м²")
        
        # Применяем двухэтапную фильтрацию выбросов для более надежных результатов
        if len(prices_per_sqm) >= 3:
            # Шаг 1: Фильтрация по методу IQR (межквартильный размах)
            q1 = statistics.quantiles(prices_per_sqm, n=4)[0]
            q3 = statistics.quantiles(prices_per_sqm, n=4)[2]
            iqr = q3 - q1
            lower_bound = max(q1 - 1.5 * iqr, 0)  # Не допускаем отрицательных значений
            upper_bound = q3 + 1.5 * iqr
            
            # Фильтруем выбросы на основе IQR
            filtered_prices = [p for p in prices_per_sqm if lower_bound <= p <= upper_bound]
            
            # Шаг 2: Проверка на экстремальные коэффициенты различий
            if filtered_prices and len(filtered_prices) >= 2:
                # Сортируем цены
                filtered_prices.sort()
                
                # Находим максимальный коэффициент между соседними значениями
                max_ratio = 0
                max_ratio_idx = 0
                for i in range(1, len(filtered_prices)):
                    ratio = filtered_prices[i] / filtered_prices[i-1] if filtered_prices[i-1] > 0 else 1
                    if ratio > max_ratio:
                        max_ratio = ratio
                        max_ratio_idx = i
                
                # Если обнаружен разрыв больше 3x, разделяем данные
                if max_ratio > 3.0:
                    logging.warning(f"Обнаружен значительный разрыв в ценах (x{max_ratio:.1f}). "
                                   f"Разделение на группы: "
                                   f"{filtered_prices[:max_ratio_idx]} и {filtered_prices[max_ratio_idx:]}")
                    
                    # Выбираем группу, которая ближе к цене лота
                    lot_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
                    
                    group1_avg = sum(filtered_prices[:max_ratio_idx]) / max(1, len(filtered_prices[:max_ratio_idx]))
                    group2_avg = sum(filtered_prices[max_ratio_idx:]) / max(1, len(filtered_prices[max_ratio_idx:]))
                    
                    # Определяем, какая группа ближе к лоту по цене
                    group1_diff = abs(group1_avg - lot_price_per_sqm)
                    group2_diff = abs(group2_avg - lot_price_per_sqm)
                    
                    if group1_diff <= group2_diff and len(filtered_prices[:max_ratio_idx]) >= 2:
                        filtered_prices = filtered_prices[:max_ratio_idx]
                        logging.info(f"Выбрана первая группа цен как более релевантная")
                    elif len(filtered_prices[max_ratio_idx:]) >= 2:
                        filtered_prices = filtered_prices[max_ratio_idx:]
                        logging.info(f"Выбрана вторая группа цен как более релевантная")
            
            # Используем отфильтрованные данные, если их достаточно
            if filtered_prices and len(filtered_prices) >= 2:
                orig_median = statistics.median(original_prices)
                filtered_median = statistics.median(filtered_prices)
                
                # Проверка на значительное изменение медианы
                change_pct = abs(filtered_median - orig_median) / orig_median * 100 if orig_median else 0
                
                logging.info(f"Применена фильтрация выбросов: {len(filtered_prices)} из {len(prices_per_sqm)} цен")
                logging.info(f"Медиана до фильтрации: {orig_median:,.0f} ₽/м², "
                            f"после: {filtered_median:,.0f} ₽/м² (изменение: {change_pct:.1f}%)")
                
                # Если изменение слишком сильное, проверяем еще раз
                if change_pct > 40:
                    logging.warning(f"⚠️ Значительное изменение медианы после фильтрации: {change_pct:.1f}%")
                    
                    # Проверка на аномально низкие значения
                    if filtered_median < 50000 and lot.district == "Красносельский":
                        logging.warning(f"⚠️ Аномально низкая медиана для района Красносельский: {filtered_median:,.0f} ₽/м²")
                        logging.warning(f"Используем оригинальную медиану: {orig_median:,.0f} ₽/м²")
                        filtered_prices = original_prices
                
                prices_per_sqm = filtered_prices
        
        # 3. РАСЧЕТ И ПРОВЕРКА РЕЗУЛЬТАТОВ:
        if prices_per_sqm:
            # Рассчитываем медиану после всех фильтраций
            lot.market_price_per_sqm = statistics.median(prices_per_sqm)  # Рыночная ставка, ₽/м²
            lot.current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0  # Текущая ставка, ₽/м²
            lot.market_value = lot.market_price_per_sqm * lot.area  # Рыночная стоимость, ₽
            
            # Проверка на недопустимо низкую рыночную цену
            if lot.district == "Красносельский" and lot.market_price_per_sqm < 150000:
                logging.warning(f"⚠️ Рыночная цена для района Красносельский слишком низкая: "
                               f"{lot.market_price_per_sqm:,.0f} ₽/м². Применяем корректировку.")
                lot.market_price_per_sqm = 300000  # Корректировка на основе известной рыночной стоимости
                lot.market_value = lot.market_price_per_sqm * lot.area
            
            # 3. Капитализация, ₽
            lot.capitalization_rub = lot.market_value - lot.price
            
            # 4. Капитализация, %
            lot.capitalization_percent = (lot.capitalization_rub / lot.price) * 100 if lot.price > 0 else 0
            
            # 6. Плюсик за продажу
            lot.plus_sale = 1 if lot.capitalization_percent >= 0 else 0
            
            lot.market_value_method = "sales"
            
            # Подробное логирование для проверки расчетов
            logging.info(f"Итоговые расчеты для лота {lot.id}:")
            logging.info(f"  Рыночная цена: {lot.market_price_per_sqm:,.0f} ₽/м²")
            logging.info(f"  Текущая цена: {lot.current_price_per_sqm:,.0f} ₽/м²")
            logging.info(f"  Рыночная стоимость: {lot.market_value:,.0f} ₽")
            logging.info(f"  Капитализация: {lot.capitalization_rub:,.0f} ₽ ({lot.capitalization_percent:.1f}%)")
    else:
        # Значения по умолчанию при отсутствии данных о продажах
        lot.market_price_per_sqm = 0
        lot.current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
        lot.market_value = 0
        lot.capitalization_rub = 0
        lot.capitalization_percent = 0
        lot.plus_sale = 0
        lot.market_value_method = "none"
    
    # 2. Расчет доходности на основе объявлений об аренде
    if valid_rent_offers:
        rent_prices_per_sqm = [offer.price / offer.area for offer in valid_rent_offers]
        if rent_prices_per_sqm:
            lot.average_rent_price_per_sqm = statistics.median(rent_prices_per_sqm)  # Арендная ставка, ₽/м²/месяц
            
            # 1. GAP (рыночный арендный поток), ₽/мес
            lot.monthly_gap = lot.average_rent_price_per_sqm * lot.area
            
            # Годовой арендный доход
            lot.annual_income = lot.monthly_gap * 12
            
            # 2. Доходность по аренде, %
            lot.annual_yield_percent = (lot.annual_income / lot.price) * 100 if lot.price > 0 else 0
            
            # 5. Плюсик за аренду
            lot.plus_rental = 1 if lot.annual_yield_percent >= 10 else 0
            
            lot.has_rent_data = True
    else:
        # Если нет данных об аренде, используем примерную формулу
        lot.has_rent_data = False
        lot.monthly_gap = lot.market_value * 0.007  # Примерно 0.7% в месяц
        lot.annual_income = lot.monthly_gap * 12
        lot.annual_yield_percent = (lot.annual_income / lot.price) * 100 if lot.price > 0 else 0
        lot.average_rent_price_per_sqm = lot.monthly_gap / lot.area if lot.area > 0 else 0
        lot.plus_rental = 1 if lot.annual_yield_percent >= 10 else 0
    
    # 7. Общее число плюсиков
    lot.plus_count = lot.plus_sale + lot.plus_rental
    
    # 8. Статус объекта
    if lot.plus_count == 0:
        lot.status = "discard"
    elif lot.plus_count == 1:
        lot.status = "review"
    else:  # lot.plus_count == 2
        lot.status = "approved"
    
    # Логирование результатов
    logging.info(f"Лот {lot.id}: Метрики рассчитаны - "
               f"Рыночная цена: {lot.market_price_per_sqm:.0f} ₽/м², "
               f"Капитализация: {lot.capitalization_rub:,.0f} ₽ ({lot.capitalization_percent:.1f}%), "
               f"ГАП: {lot.monthly_gap:,.0f} ₽/мес, "
               f"Доходность: {lot.annual_yield_percent:.1f}%, "
               f"Плюсики: {lot.plus_count}/2, "
               f"Статус: {lot.status}")
               
    return lot