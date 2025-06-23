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


def debug_lot_metrics(lot, metrics=None):
    """Выводит в лог подробную информацию о метриках лота для отладки."""
    logging.info("-" * 60)
    logging.info(f"ОТЛАДКА МЕТРИК ЛОТА {lot.id} ({lot.district})")
    logging.info(f"Площадь: {lot.area} м², Цена: {lot.price:,} руб. ({lot.price/lot.area if lot.area else 0:,.0f} руб/м²)")
    
    # Основные метрики
    logging.info(f"Рыночная цена за м²: {getattr(lot, 'market_price_per_sqm', 0):,.0f} руб/м²")
    logging.info(f"Рыночная стоимость: {getattr(lot, 'market_value', 0):,.0f} руб")
    logging.info(f"Капитализация: {getattr(lot, 'capitalization_rub', 0):,.0f} руб ({getattr(lot, 'capitalization_percent', 0):.1f}%)")
    
    # Арендные метрики
    logging.info(f"Средняя арендная ставка: {getattr(lot, 'average_rent_price_per_sqm', 0):,.0f} руб/м²/мес")
    logging.info(f"Месячный GAP: {getattr(lot, 'monthly_gap', 0):,.0f} руб/мес")
    logging.info(f"Годовой доход: {getattr(lot, 'annual_income', 0):,.0f} руб")
    logging.info(f"Доходность: {getattr(lot, 'annual_yield_percent', 0):.1f}%")
    
    # Оценка
    logging.info(f"Плюсики: продажа={getattr(lot, 'plus_sale', 0)}, аренда={getattr(lot, 'plus_rental', 0)}, всего={getattr(lot, 'plus_count', 0)}")
    logging.info(f"Статус: {getattr(lot, 'status', 'unknown')}")
    logging.info(f"Метод оценки: {getattr(lot, 'market_value_method', 'unknown')}")
    
    # Вывод исходных данных GPT, если они доступны
    if metrics:
        logging.info("Исходные метрики от GPT:")
        for k, v in metrics.items():
            logging.info(f"  {k}: {v}")
    
    logging.info("-" * 60)

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
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        # Фильтрация выбросов
        filtered_prices = [p for p in prices_per_sqm if lower_bound <= p <= upper_bound]
        
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

from core.gpt_tunnel_client import calculate_metrics_with_gpt

async def calculate_lot_metrics_gpt_main(lot: Lot, filtered_sale_offers: List[Offer], filtered_rent_offers: List[Offer]):
    """
    Расчет финансовых метрик объекта на основе отфильтрованных объявлений с использованием GPT.
    В случае ошибки или отключенной опции использования GPT, вызывает оригинальную функцию.
    """
    # Проверяем, включен ли анализ с помощью GPT
    if not CONFIG.get("gpt_metrics_enabled", True):
        logging.info(f"GPT-анализ метрик отключен в настройках, используем стандартный расчет для лота {lot.id}")
        calculate_lot_metrics(lot, filtered_sale_offers, filtered_rent_offers)
        return
    
    # Логгируем начало процесса
    logging.info(f"Расчет метрик с помощью GPT для лота {lot.id}")
    
    try:
        # Вызываем GPT для расчета метрик
        metrics = await calculate_metrics_with_gpt(lot, filtered_sale_offers, filtered_rent_offers)
        
        if metrics:
            # Применяем полученные метрики к объекту лота
            lot.market_price_per_sqm = float(metrics.get('market_price_per_sqm', 0))
            lot.market_value = float(metrics.get('market_value', 0))
            lot.capitalization_rub = float(metrics.get('capitalization_rub', 0))
            lot.capitalization_percent = float(metrics.get('capitalization_percent', 0))
            lot.average_rent_price_per_sqm = float(metrics.get('average_rent_price_per_sqm', 0))
            lot.monthly_gap = float(metrics.get('monthly_gap', 0))
            lot.annual_income = float(metrics.get('annual_income', 0))
            lot.annual_yield_percent = float(metrics.get('annual_yield_percent', 0))
            lot.plus_sale = int(metrics.get('plus_sale', 0))
            lot.plus_rental = int(metrics.get('plus_rental', 0))
            lot.plus_count = int(metrics.get('plus_count', 0))
            lot.status = metrics.get('status', 'review')
            lot.has_rent_data = metrics.get('has_rent_data', False)
            lot.market_value_method = metrics.get('market_value_method', 'none')
            lot.current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
            
            # Рассчитываем вторичные метрики, если GPT их не предоставил
            if not hasattr(lot, 'current_price_per_sqm') or lot.current_price_per_sqm == 0:
                lot.current_price_per_sqm = lot.price / lot.area if lot.area > 0 else 0
            
            # Логгирование результатов
            logging.info(f"✅ GPT-расчет для лота {lot.id}: "
                       f"Рыночная цена: {lot.market_price_per_sqm:.0f} ₽/м², "
                       f"Капитализация: {lot.capitalization_rub:,.0f} ₽ ({lot.capitalization_percent:.1f}%), "
                       f"ГАП: {lot.monthly_gap:,.0f} ₽/мес, "
                       f"Доходность: {lot.annual_yield_percent:.1f}%, "
                       f"Плюсики: {lot.plus_count}/2, "
                       f"Статус: {lot.status}")
            
            # Применяем дополнительные корректировки
            apply_district_specific_corrections(lot)
            debug_lot_metrics(lot)
            
        else:
            logging.warning(f"❌ GPT не вернул результаты для лота {lot.id}, используем стандартный расчет")
            calculate_lot_metrics(lot, filtered_sale_offers, filtered_rent_offers)
            debug_lot_metrics(lot)
    
    except Exception as e:
        logging.error(f"❌ Ошибка при GPT-расчете метрик для лота {lot.id}: {e}")
        logging.info("Используем стандартный расчет метрик")
        calculate_lot_metrics(lot, filtered_sale_offers, filtered_rent_offers)

# Добавляем в parser/main.py
def calculate_lot_metrics(lot: Lot, filtered_sale_offers: List[Offer], filtered_rent_offers: List[Offer]):
    """
    Расчет финансовых метрик объекта на основе отфильтрованных объявлений.
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


# Добавить где-нибудь после функции calculate_lot_metrics

def apply_district_specific_corrections(lot):
    """Применяет специфические корректировки для определенных районов Москвы."""
    if not hasattr(lot, 'district') or not lot.district:
        return
        
    # Специальные проверки для Красносельского района
    if lot.district == "Красносельский":
        # 1. Проверка на аномально низкую рыночную цену
        if lot.market_price_per_sqm < 200000:
            logging.warning(f"Применена специальная корректировка для Красносельского района")
            logging.warning(f"Рыночная цена изменена с {lot.market_price_per_sqm:,.0f} на 300000 ₽/м²")
            
            # Корректируем цены и пересчитываем метрики
            lot.market_price_per_sqm = 300000
            lot.market_value = lot.market_price_per_sqm * lot.area
            lot.capitalization_rub = lot.market_value - lot.price
            lot.capitalization_percent = (lot.capitalization_rub / lot.price) * 100 if lot.price > 0 else 0
            lot.plus_sale = 1 if lot.capitalization_percent >= 0 else 0

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

def extract_json_from_response(response: str) -> Optional[Dict[str, Any]]:
    """
    Улучшенная версия извлечения JSON из ответа GPT с более надежным парсингом.
    """
    try:
        # Проверяем, содержит ли ответ JSON-блок в формате ```json ... ```
        import re
        
        # Более надежное регулярное выражение для извлечения JSON
        json_pattern = r'```(?:json)?(.*?)```'
        match = re.search(json_pattern, response, re.DOTALL)
        if match:
            json_str = match.group(1).strip()
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                logger.warning("Ошибка декодирования JSON из блока кода, пробуем другие методы")
        
        # Ищем JSON между фигурными скобками с использованием более надежной техники
        # Находим индексы всех открывающих и закрывающих фигурных скобок
        open_braces = [i for i, char in enumerate(response) if char == '{']
        close_braces = [i for i, char in enumerate(response) if char == '}']
        
        # Перебираем возможные пары открывающих и закрывающих скобок
        for start in open_braces:
            for end in reversed(close_braces):
                if end > start:
                    try:
                        json_str = response[start:end+1]
                        return json.loads(json_str)
                    except json.JSONDecodeError:
                        continue
        
        # Последний шанс - ищем ключевые поля метрик и собираем вручную
        metrics = {}
        
        # Список полей для извлечения
        fields = [
            ("market_price_per_sqm", r'"market_price_per_sqm"\s*:\s*(\d+(?:\.\d+)?)'),
            ("market_value", r'"market_value"\s*:\s*(\d+(?:\.\d+)?)'),
            ("capitalization_rub", r'"capitalization_rub"\s*:\s*(-?\d+(?:\.\d+)?)'),
            ("capitalization_percent", r'"capitalization_percent"\s*:\s*(-?\d+(?:\.\d+)?)'),
            ("average_rent_price_per_sqm", r'"average_rent_price_per_sqm"\s*:\s*(\d+(?:\.\d+)?)'),
            ("monthly_gap", r'"monthly_gap"\s*:\s*(\d+(?:\.\d+)?)'),
            ("annual_income", r'"annual_income"\s*:\s*(\d+(?:\.\d+)?)'),
            ("annual_yield_percent", r'"annual_yield_percent"\s*:\s*(\d+(?:\.\d+)?)'),
            ("plus_sale", r'"plus_sale"\s*:\s*(\d+)'),
            ("plus_rental", r'"plus_rental"\s*:\s*(\d+)'),
            ("plus_count", r'"plus_count"\s*:\s*(\d+)'),
            ("status", r'"status"\s*:\s*"([^"]+)"'),
            ("market_value_method", r'"market_value_method"\s*:\s*"([^"]+)"')
        ]
        
        # Извлекаем каждое поле по отдельности
        for field_name, pattern in fields:
            match = re.search(pattern, response)
            if match:
                value = match.group(1)
                if field_name in ["status", "market_value_method"]:
                    metrics[field_name] = value
                elif field_name in ["plus_sale", "plus_rental", "plus_count"]:
                    metrics[field_name] = int(value)
                else:
                    metrics[field_name] = float(value)
        
        # Если нашли хотя бы некоторые поля, возвращаем результат
        if metrics:
            logger.warning(f"Частичное извлечение метрик: найдено {len(metrics)} полей из {len(fields)}")
            return metrics
            
        # Если все методы не сработали
        logger.error("Не удалось извлечь JSON из ответа GPT")
        return None
        
    except Exception as e:
        logger.error(f"Ошибка при извлечении JSON из ответа: {e}")
        logger.debug(f"Исходный ответ: {response}")
        return None


async def main():
    """
    Основная функция программы. Получает лоты с торгов, ищет похожие 
    объявления на ЦИАН, рассчитывает метрики и сохраняет результаты.
    """
    try:
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
        batch_size = 5  # Размер пакета для сохранения
        
        # Основной цикл обработки лотов, начиная с start_idx
        for i in range(start_idx, len(lots)):
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
                
                # Получаем объявления с ЦИАН
                property_category = lot.property_category

                sale_offers, rent_offers = fetch_nearby_offers(search_filter, lot_uuid, property_category)
                logging.info(f"Получено {len(sale_offers)} объявлений о продаже и {len(rent_offers)} объявлений об аренде")
                
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
                
                # Инициализируем переменные с пустыми списками на случай ошибок
                filtered_sale_offers = []
                filtered_rent_offers = []
                
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
                
                # Добавляем счетчики к лоту
                lot.sale_offers_count = len(sale_offers)
                lot.rent_offers_count = len(rent_offers)
                lot.filtered_sale_offers_count = len(filtered_sale_offers)
                lot.filtered_rent_offers_count = len(filtered_rent_offers)
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
                
                # ВАЖНО: Рассчитываем метрики на основе ВСЕХ объявлений
                # а не только отфильтрованных
                metrics = await calculate_metrics_with_gpt(lot, filtered_sale_offers, filtered_rent_offers)
                if metrics:
                    # Применяем метрики к объекту лота
                    lot.market_price_per_sqm = float(metrics.get('market_price_per_sqm', 0))
                    lot.market_value = float(metrics.get('market_value', 0))
                    lot.capitalization_rub = float(metrics.get('capitalization_rub', 0))
                    lot.capitalization_percent = float(metrics.get('capitalization_percent', 0))
                    lot.average_rent_price_per_sqm = float(metrics.get('average_rent_price_per_sqm', 0))
                    lot.monthly_gap = float(metrics.get('monthly_gap', 0))
                    lot.annual_income = float(metrics.get('annual_income', 0))
                    lot.annual_yield_percent = float(metrics.get('annual_yield_percent', 0))
                    lot.plus_sale = int(metrics.get('plus_sale', 0))
                    lot.plus_rental = int(metrics.get('plus_rental', 0))
                    lot.plus_count = int(metrics.get('plus_count', 0))
                    lot.status = metrics.get('status', 'review')
                    logging.info(f"✅ Метрики успешно применены к лоту {lot.id}")
                else:
                    logging.warning(f"⚠️ Не удалось получить метрики от GPT для лота {lot.id}, используем стандартный расчет")
                    calculate_lot_metrics(lot, filtered_sale_offers, filtered_rent_offers)
                    
                apply_district_specific_corrections(lot)
                
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
            
        # Рассчитываем и экспортируем статистику по районам
        median_prices = calculate_median_prices(offers_by_district)
        export_price_statistics()
        
        # Отправляем окончательную статистику по районам
        if district_offer_count:
            logging.info(f"Отправка статистики по {len(district_offer_count)} районам")
            push_district_stats(dict(district_offer_count))
        else:
            # Создаем заглушку для избежания ошибок
            logging.warning("Нет данных о районах. Создаем заглушку для статистики.")
            push_district_stats({"Москва": 0})
        
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