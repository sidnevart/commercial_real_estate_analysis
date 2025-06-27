
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для тестирования геокодирования адресов лотов и объявлений ЦИАН
"""
# filepath: test_geocoding.py

import asyncio
import logging
import sys
import json
from pathlib import Path

# Добавляем путь к проекту
sys.path.append(str(Path(__file__).parent))

from parser.address_parser import calculate_address_components
from parser.geo_utils import get_coords_by_address, calculate_distance
from parser.cian_minimal import fetch_nearby_offers, unformatted_address_to_cian_search_filter
from parser.torgi_async import fetch_lots

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

async def test_address_geocoding(address: str) -> dict:
    """Тестирует геокодирование одного адреса"""
    logger.info(f"🔍 Тестирование адреса: {address}")
    
    result = {
        "original_address": address,
        "address_components": None,
        "coordinates": None,
        "geocoding_success": False,
        "error": None
    }
    
    try:
        # 1. Парсинг компонентов адреса
        components = calculate_address_components(address)
        result["address_components"] = components
        logger.info(f"📍 Компоненты: район={components.get('district', 'н/д')}, "
                   f"улица={components.get('street', 'н/д')}, "
                   f"город={components.get('city', 'н/д')}, "
                   f"уверенность={components.get('confidence', 0):.2f}")
        
        # 2. Геокодирование
        coords = await get_coords_by_address(address)
        if coords:
            result["coordinates"] = {"lon": coords[0], "lat": coords[1]}
            result["geocoding_success"] = True
            logger.info(f"✅ Координаты: {coords[0]:.6f}, {coords[1]:.6f}")
        else:
            logger.warning("❌ Геокодирование не удалось")
            
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"❌ Ошибка: {e}")
    
    return result

async def test_lot_with_offers(lot_address: str, max_offers: int = 5) -> dict:
    """Тестирует лот с его объявлениями"""
    logger.info(f"🏢 Тестирование лота: {lot_address}")
    
    result = {
        "lot_address": lot_address,
        "lot_coords": None,
        "search_filter": "",
        "offers_found": 0,
        "offers_geocoded": 0,
        "distances": [],
        "errors": []
    }
    
    try:
        # 1. Геокодируем адрес лота
        lot_coords = await get_coords_by_address(lot_address)
        if not lot_coords:
            result["errors"].append("Не удалось геокодировать адрес лота")
            return result
            
        result["lot_coords"] = {"lon": lot_coords[0], "lat": lot_coords[1]}
        logger.info(f"📍 Лот: {lot_coords[0]:.6f}, {lot_coords[1]:.6f}")
        
        # 2. Получаем поисковый фильтр
        search_filter = unformatted_address_to_cian_search_filter(lot_address)
        result["search_filter"] = search_filter
        logger.info(f"🔍 Фильтр поиска: {search_filter}")
        
        # 3. Получаем объявления
        sale_offers, rent_offers = fetch_nearby_offers(search_filter, "test_uuid")
        all_offers = sale_offers + rent_offers
        result["offers_found"] = len(all_offers)
        logger.info(f"📋 Найдено объявлений: {len(all_offers)}")
        
        # 4. Обрабатываем первые несколько объявлений
        for i, offer in enumerate(all_offers[:max_offers]):
            logger.info(f"📍 Объявление {i+1}: {offer.address}")
            
            try:
                # Геокодируем адрес объявления
                offer_coords = await get_coords_by_address(offer.address)
                if offer_coords:
                    result["offers_geocoded"] += 1
                    
                    # Рассчитываем расстояние
                    distance = await calculate_distance(lot_coords, offer_coords)
                    if distance:
                        result["distances"].append({
                            "offer_id": offer.id,
                            "offer_address": offer.address,
                            "offer_coords": {"lon": offer_coords[0], "lat": offer_coords[1]},
                            "distance_km": distance
                        })
                        logger.info(f"📏 Расстояние: {distance:.2f} км")
                    else:
                        result["errors"].append(f"Не удалось рассчитать расстояние для {offer.id}")
                else:
                    result["errors"].append(f"Не удалось геокодировать объявление {offer.id}: {offer.address}")
                    
            except Exception as e:
                result["errors"].append(f"Ошибка при обработке объявления {offer.id}: {str(e)}")
                
    except Exception as e:
        result["errors"].append(f"Общая ошибка: {str(e)}")
        logger.error(f"❌ Ошибка: {e}")
    
    return result

async def main():
    """Основная функция тестирования"""
    logger.info("🚀 Запуск тестирования геокодирования")
    
    # Тестовые адреса лотов (можно взять из реальных данных)
    test_lot_addresses = [
        "г Москва, ул Тверская, дом 7",
        "Москва, Пресненская набережная, дом 12",
        "Московская область, г Подольск, ул Правды, дом 20",
        "г Москва, ВАО, Перово, ул Новогиреевская, д 42",
        "Московская область, г Химки, ул Загородная, дом 4"
    ]
    
    # Можно также получить реальные адреса из системы
    try:
        lots = await fetch_lots(max_pages=1)
        if lots:
            real_addresses = [lot.address for lot in lots[:3]]
            test_lot_addresses.extend(real_addresses)
            logger.info(f"Добавлены реальные адреса: {len(real_addresses)}")
    except Exception as e:
        logger.warning(f"Не удалось получить реальные адреса: {e}")
    
    results = []
    
    for i, address in enumerate(test_lot_addresses[:5], 1):  # Тестируем первые 5
        logger.info(f"\n{'='*60}")
        logger.info(f"🧪 ТЕСТ {i}/{min(5, len(test_lot_addresses))}")
        logger.info(f"{'='*60}")
        
        try:
            # Сначала тестируем простое геокодирование
            simple_result = await test_address_geocoding(address)
            
            # Если геокодирование успешно, тестируем с объявлениями
            if simple_result["geocoding_success"]:
                lot_result = await test_lot_with_offers(address, max_offers=3)
                results.append(lot_result)
            else:
                logger.warning("⚠️ Пропускаем тест с объявлениями из-за неудачного геокодирования")
                results.append({
                    "lot_address": address,
                    "error": "Геокодирование лота не удалось"
                })
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при тестировании {address}: {e}")
            results.append({
                "lot_address": address,
                "error": f"Критическая ошибка: {str(e)}"
            })
        
        # Пауза между тестами
        if i < len(test_lot_addresses):
            logger.info("⏱️ Пауза 3 секунды...")
            await asyncio.sleep(3)
    
    # Сохраняем результаты
    timestamp = int(asyncio.get_event_loop().time())
    results_file = f"geocoding_test_results_{timestamp}.json"
    
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    
    logger.info(f"\n📊 ИТОГИ ТЕСТИРОВАНИЯ:")
    logger.info(f"Протестировано лотов: {len(results)}")
    successful_tests = [r for r in results if not r.get("error") and r.get("offers_geocoded", 0) > 0]
    logger.info(f"Успешных тестов: {len(successful_tests)}")
    logger.info(f"Результаты сохранены в: {results_file}")
    
    # Краткая статистика
    total_offers = sum(r.get("offers_found", 0) for r in results)
    total_geocoded = sum(r.get("offers_geocoded", 0) for r in results)
    logger.info(f"Всего объявлений: {total_offers}")
    logger.info(f"Успешно геокодированных: {total_geocoded}")
    if total_offers > 0:
        logger.info(f"Процент успеха геокодирования: {total_geocoded/total_offers*100:.1f}%")

if __name__ == "__main__":
    asyncio.run(main())