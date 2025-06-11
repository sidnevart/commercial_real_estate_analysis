import asyncio
import logging
import statistics
import re
from collections import defaultdict
from typing import Dict, List

from parser.torgi_async import fetch_lots
#from parser.cian_selenium import fetch_nearby_offers, unformatted_address_to_cian_search_filter
#from parser.cian_minimal import fetch_nearby_offers, unformatted_address_to_cian_search_filter
from parser.cian_minimal import fetch_nearby_offers, unformatted_address_to_cian_search_filter
from parser.google_sheets import push_lots, push_offers, push_district_stats
from parser.gpt_classifier import classify_property  
from parser.cian_minimal import get_parser
from core.models import Lot, Offer, PropertyClassification
from core.config import CONFIG
from parser.geo_utils import filter_offers_by_distance

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


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
    """Enhanced district extraction from address string."""
    parts = address.split(',')
    for part in parts:
        part = part.strip().lower()
        if "район" in part or "округ" in part:
            return part.capitalize()
    
    common_districts = [
        "Хамовники", "Арбат", "Тверской", "Пресненский", "Замоскворечье",
        "Басманный", "Таганский", "Беговой", "Сокол", "Аэропорт", 
        "Щукино", "Хорошево-Мневники", "Строгино"
    ]
    
    for district in common_districts:
        if district.lower() in address.lower():
            return district
            
    for part in parts:
        part = part.strip().lower()
        if "г." in part or "город" in part:
            city_part = part.replace("г.", "").replace("город", "").strip()
            return f"г. {city_part.capitalize()}"
    
    return "Unknown"

def calculate_median_prices(offers_by_district: Dict[str, List[Offer]]) -> Dict[str, float]:
    """Calculate median price per square meter by district."""
    median_prices = {}
    for district, offers in offers_by_district.items():
        if not offers:
            continue
        prices_per_sqm = [offer.price / offer.area for offer in offers if offer.area > 0]
        if prices_per_sqm:
            median_prices[district] = statistics.median(prices_per_sqm)
    return median_prices

def calculate_profitability(lot: Lot, median_prices: Dict[str, float]) -> float:
    """Calculate profitability as (median market price - auction price) / median market price."""
    if not lot.district or lot.district not in median_prices or lot.area <= 0:
        logging.warning(f"Невозможно рассчитать рентабельность для лота {lot.id}: "
                      f"district={lot.district}, area={lot.area}, "
                      f"has_median_price={lot.district in median_prices}")
        return 0.0
    
    median_price_per_sqm = median_prices[lot.district]
    auction_price_per_sqm = lot.price / lot.area
    
    if median_price_per_sqm <= 0:
        logging.warning(f"Некорректная медианная цена {median_price_per_sqm} для района {lot.district}")
        return 0.0
        
    profitability = (median_price_per_sqm - auction_price_per_sqm) / median_price_per_sqm * 100
    return round(profitability, 2)

async def main():
    lots = await fetch_lots(max_pages=2)
    logging.info("Got %d lots", len(lots))

    cian_metrics = get_cian_metrics()
    logging.info(f"Статус CIAN-парсера: {cian_metrics}")
    
    offers_by_district = defaultdict(list)
    district_offer_count = defaultdict(int)
    total_sale_offers = 0
    total_rent_offers = 0
    batch_size = 3
    current_batch_sale = []
    current_batch_rent = []
    
    # Получаем радиус поиска из конфигурации
    search_radius = CONFIG.get("area_search_radius", 5)  # По умолчанию 5 км
    
    for i, lot in enumerate(lots, 1):
        try:
            lot.district = calculate_district(lot.address)
            logger.info(f"Lot {lot.id} is in district: {lot.district}")
            
            lot_uuid = lot.uuid
            search_filter = unformatted_address_to_cian_search_filter(lot.address)
            logging.info(f"Generated search filter: {search_filter}")
            
            # Получаем все объявления из района
            sale_offers, rent_offers = fetch_nearby_offers(search_filter, lot_uuid)
            logging.info(f"Получено {len(sale_offers)} объявлений о продаже и {len(rent_offers)} объявлений об аренде")
            
            # Дополнительная проверка для отладки
            if not sale_offers and not rent_offers:
                logging.warning(f"⚠️ Не получено ни одного объявления для лота {lot.id} (адрес: {lot.address})")
            
            # Фильтруем объявления по расстоянию от лота
            logging.info(f"Фильтрация объявлений по расстоянию (макс. {search_radius} км) для лота {lot.id}")
            filtered_sale_offers = await filter_offers_by_distance(lot.address, sale_offers, search_radius)
            filtered_rent_offers = await filter_offers_by_distance(lot.address, rent_offers, search_radius)
            
            logging.info(f"После фильтрации: {len(filtered_sale_offers)} из {len(sale_offers)} объявлений о продаже и "
                         f"{len(filtered_rent_offers)} из {len(rent_offers)} объявлений об аренде в радиусе {search_radius} км")
            
            # Обрабатываем только отфильтрованные объявления и добавляем их в offers_by_district
            for offer in filtered_sale_offers:
                offer.district = calculate_district(offer.address)
                # Дополнительная проверка для отладки - вывести найденный район предложения
                logging.debug(f"Offer {offer.id} district: {offer.district}")
                
                # Убедимся, что district определен и не пуст
                if offer.district and offer.district != "Unknown":
                    offers_by_district[offer.district].append(offer)
                    district_offer_count[offer.district] += 1
                else:
                    # Если район не определен, используем район лота
                    if lot.district and lot.district != "Unknown":
                        logging.info(f"Используем район лота ({lot.district}) для предложения без района")
                        offer.district = lot.district
                        offers_by_district[offer.district].append(offer)
                        district_offer_count[offer.district] += 1
            
            # Выведем промежуточную статистику для отладки
            logging.info(f"Текущее количество объявлений по районам: {dict(district_offer_count)}")
            
            current_batch_sale.extend(filtered_sale_offers)
            current_batch_rent.extend(filtered_rent_offers)
            
            total_sale_offers += len(filtered_sale_offers)
            total_rent_offers += len(filtered_rent_offers)
            
            # Добавим паузу между обработкой лотов, чтобы уменьшить нагрузку
            await asyncio.sleep(1)
            
            if i % batch_size == 0 or i == len(lots):
                if current_batch_sale:
                    logging.info(f"Pushing batch of {len(current_batch_sale)} sale offers to Google Sheets")
                    push_offers("cian_sale", current_batch_sale)
                    current_batch_sale = []
                    
                if current_batch_rent:
                    logging.info(f"Pushing batch of {len(current_batch_rent)} rent offers to Google Sheets")
                    push_offers("cian_rent", current_batch_rent)
                    current_batch_rent = []
                
        except Exception as e:
            logging.error(f"Error processing lot {lot.id}: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())