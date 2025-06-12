import asyncio
import logging
import statistics
import re
from collections import defaultdict
from typing import Dict, List
import time
from parser.torgi_async import fetch_lots
from parser.cian_minimal import fetch_nearby_offers, unformatted_address_to_cian_search_filter
from parser.google_sheets import push_lots, push_offers, push_district_stats
from parser.gpt_classifier import classify_property  
from parser.cian_minimal import get_parser
from core.models import Lot, Offer, PropertyClassification
from core.config import CONFIG
from parser.geo_utils import filter_offers_by_distance
import os
import pickle

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
    try:
       # Проверяем аргументы командной строки для возобновления
        import sys
        resume_from_checkpoint = "--resume" in sys.argv
        
        # Инициализируем базовые переменные
        browser_operations = 0
        browser_refresh_interval = CONFIG.get("browser_refresh_interval", 20)
        lot_save_interval = CONFIG.get("lot_save_interval", 5)
        
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
                
                # Информация о восстановленных данных
                logging.info(f"📊 Восстановлены данные: {len(all_sale_offers)} объявлений о продаже, {len(all_rent_offers)} объявлений об аренде")
            else:
                # Не удалось восстановить, начинаем с нуля
                logging.info("⚠️ Не удалось восстановить из чекпоинта. Начинаем с нуля.")
                lots = await fetch_lots(max_pages=2)
                processed_indices = set()
                offers_by_district = defaultdict(list)
                district_offer_count = defaultdict(int)
                all_sale_offers = []
                all_rent_offers = []
                processed_lots = []
                start_idx = 0
        else:
            # Начинаем с нуля
            logging.info("🔄 Запускаем обработку с нуля (без восстановления)")
            lots = await fetch_lots(max_pages=2)
            processed_indices = set()
            offers_by_district = defaultdict(list)
            district_offer_count = defaultdict(int)
            all_sale_offers = []
            all_rent_offers = []
            processed_lots = []
            start_idx = 0
        
        logging.info(f"✅ Получено {len(lots)} лотов для обработки")
        
        # Проверка работоспособности CIAN-парсера
        cian_metrics = get_cian_metrics()
        logging.info(f"Статус CIAN-парсера: {cian_metrics}")
        
        total_sale_offers = 0
        total_rent_offers = 0
        batch_size = 3
        current_batch_sale = []
        current_batch_rent = []
        
        # Основной цикл обработки лотов, начиная с start_idx
        for i in range(start_idx, len(lots)):
            try:
                lot = lots[i]
                lot.district = calculate_district(lot.address)
                logger.info(f"Lot {lot.id} is in district: {lot.district}")
                
                lot_uuid = lot.uuid
                search_filter = unformatted_address_to_cian_search_filter(lot.address)
                logging.info(f"Generated search filter: {search_filter}")
                
                # Получаем все объявления из района
                sale_offers, rent_offers = fetch_nearby_offers(search_filter, lot_uuid)
                logging.info(f"Получено {len(sale_offers)} объявлений о продаже и {len(rent_offers)} объявлений об аренде")

                # Инкрементируем счетчик операций браузера
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


                # Дополнительная проверка для отладки
                if not sale_offers and not rent_offers:
                    logging.warning(f"⚠️ Не получено ни одного объявления для лота {lot.id} (адрес: {lot.address})")
                
                # Используем обычный или отладочный радиус
                effective_radius = debug_radius if debug_radius else search_radius
                
                # Временно увеличиваем радиус, если нет объявлений и не задан отладочный радиус
                if not debug_radius and (not sale_offers or not rent_offers) and search_radius < 10:
                    logging.info(f"Увеличиваем радиус поиска до 10 км из-за малого количества объявлений")
                    effective_radius = 10
                    
                # Если нет объявлений вообще, используем режим отладки с большим радиусом
                if not sale_offers and not rent_offers and not debug_radius:
                    logging.warning("⚠️ Нет объявлений, включаем режим отладки (радиус 1000 км)")
                    effective_radius = 1000  # Очень большой радиус для отладки
                
                # Фильтруем объявления по расстоянию от лота
                logging.info(f"Фильтрация объявлений по расстоянию (макс. {effective_radius} км) для лота {lot.id}")
                
                filtered_sale_offers = await filter_offers_by_distance(lot.address, sale_offers, effective_radius)
                filtered_rent_offers = await filter_offers_by_distance(lot.address, rent_offers, effective_radius)
                
                # Добавим логирование расстояний для отладки
                if filtered_sale_offers:
                    logging.info("Примеры расстояний для объявлений о продаже:")
                    for i, offer in enumerate(filtered_sale_offers[:3]):
                        dist = getattr(offer, 'distance_to_lot', 'не определено')
                        logging.info(f"  {i+1}. ID: {offer.id}, Цена: {offer.price}, "
                                    f"Расстояние до лота: {dist} км, "
                                    f"Адрес: {offer.address[:50]}...")
                
                logging.info(f"После фильтрации: {len(filtered_sale_offers)} из {len(sale_offers)} объявлений о продаже и "
                            f"{len(filtered_rent_offers)} из {len(rent_offers)} объявлений об аренде в радиусе {effective_radius} км")
                
                # Если после фильтрации нет объявлений, выдаем предупреждение
                if not filtered_sale_offers and sale_offers:
                    logging.warning(f"⚠️ Все объявления о продаже отфильтрованы по расстоянию (радиус {effective_radius} км)")
                
                if not filtered_rent_offers and rent_offers:
                    logging.warning(f"⚠️ Все объявления об аренде отфильтрованы по расстоянию (радиус {effective_radius} км)")
                
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

                processed_lots.append(lot)
                all_sale_offers.extend(filtered_sale_offers)
                all_rent_offers.extend(filtered_rent_offers)
                # Добавить промежуточное сохранение лотов
                if i % lot_save_interval == 0:
                    lots_to_save = processed_lots.copy()
                    logging.info(f"💾 Промежуточное сохранение {len(lots_to_save)} лотов")

                    if all_sale_offers:
                        logging.info(f"💾 Промежуточное сохранение {len(all_sale_offers)} объявлений о продаже")
                        try:
                            push_offers(f"cian_sale_part{i//lot_save_interval}", all_sale_offers)
                            logging.info(f"✅ Промежуточно сохранено {len(all_sale_offers)} объявлений о продаже")
                        except Exception as save_error:
                            logging.error(f"❌ Ошибка при промежуточном сохранении объявлений о продаже: {save_error}")
                    
                    if all_rent_offers:
                        logging.info(f"💾 Промежуточное сохранение {len(all_rent_offers)} объявлений об аренде")
                        try:
                            push_offers(f"cian_rent_part{i//lot_save_interval}", all_rent_offers)
                            logging.info(f"✅ Промежуточно сохранено {len(all_rent_offers)} объявлений об аренде")
                        except Exception as save_error:
                            logging.error(f"❌ Ошибка при промежуточном сохранении объявлений об аренде: {save_error}")

                    try:
                        push_lots(lots_to_save, sheet_suffix=f"_part{i//lot_save_interval}")
                        logging.info(f"✅ Промежуточно сохранено {len(lots_to_save)} лотов")
                    except Exception as save_error:
                        logging.error(f"❌ Ошибка при промежуточном сохранении лотов: {save_error}")
                
                save_progress_checkpoint(
                    lots=lots,
                    processed_indices=list(range(i)),
                    offers_by_district=dict(offers_by_district),
                    district_offer_count=dict(district_offer_count),
                    all_sale_offers=all_sale_offers,  # Добавляем все объявления
                    all_rent_offers=all_rent_offers   # в чекпоинт
                )
                
                # Добавим паузу между обработкой лотов, чтобы уменьшить нагрузку
                await asyncio.sleep(1)
                
                # Отправляем пакеты объявлений в Google Sheets
                if i % batch_size == 0 or i == len(lots):
                    if current_batch_sale:
                        logging.info(f"Pushing batch of {len(current_batch_sale)} sale offers to Google Sheets")
                        push_offers("cian_sale", current_batch_sale)
                        current_batch_sale = []
                        
                    if current_batch_rent:
                        logging.info(f"Pushing batch of {len(current_batch_rent)} rent offers to Google Sheets")
                        push_offers("cian_rent", current_batch_rent)
                        current_batch_rent = []

                # Обрабатываем последний лот
                if i == len(lots):  # После обработки всех лотов
                    # Рассчитываем медианные цены по районам
                    logging.info("Calculating median prices by district")
                    median_prices = calculate_median_prices(offers_by_district)
                    logging.info(f"Median prices: {median_prices}")
                    
                    # Добавляем рыночные цены и доходность в лоты
                    for lot in lots:
                        # Добавляем информацию о рыночной цене
                        if lot.district in median_prices:
                            lot.median_market_price = median_prices[lot.district] * lot.area
                            lot.profitability = calculate_profitability(lot, median_prices)
                            logging.info(f"Lot {lot.id}: Profitability = {lot.profitability:.1f}%")
                        
                        # Классификация объектов через GPT
                        if CONFIG.get("gpt_analysis_enabled", False):
                            lot.classification = await classify_property(lot)
                    
                    # Отправляем лоты в Google Sheets независимо от наличия объявлений
                    logging.info("Pushing lots to Google Sheets")
                    push_lots(lots)
                    
                    # Отправляем статистику по районам с защитой от пустого списка
                    if district_offer_count:
                        logging.info(f"Отправка статистики по {len(district_offer_count)} районам")
                        push_district_stats(dict(district_offer_count))
                    else:
                        # Создаем заглушку, чтобы избежать ошибки с пустым списком
                        logging.warning("Нет данных о районах. Создаем заглушку для статистики.")
                        push_district_stats({"Москва": 0})
                    
            except Exception as e:
                logging.error(f"Error processing lot {lot.id}: {e}", exc_info=True)
    except Exception as e:
        logging.critical(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}", exc_info=True)
        # Сохраняем состояние для отладки
        import pickle
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