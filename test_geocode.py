#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ТЕСТОВЫЙ скрипт для геокодирования с множественными геокодерами и GPT-улучшением
Здесь тестируем все новые подходы перед внедрением в основной код
"""
# filepath: test_geocode.py

import asyncio
import logging
import sys
import json
import math
import re
import warnings
from pathlib import Path
from typing import List, Optional, Tuple, Dict

# Добавляем путь к проекту
sys.path.append(str(Path(__file__).parent))

# Подавляем предупреждения геопакетов
warnings.filterwarnings("ignore", category=UserWarning, module="geopandas")
warnings.filterwarnings("ignore", category=FutureWarning, module="geopandas")

# Импорты для множественного геокодирования
try:
    from geopy.geocoders import Nominatim, Photon
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderQuotaExceeded
    GEOPY_AVAILABLE = True
except ImportError:
    print("⚠️ geopy не установлен. Установите: pip install geopy")
    GEOPY_AVAILABLE = False

from parser.address_parser import calculate_address_components
from core.gpt_tunnel_client import sync_chat

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ============================================================================
# КОНФИГУРАЦИЯ МНОЖЕСТВЕННЫХ ГЕОКОДЕРОВ
# ============================================================================

if GEOPY_AVAILABLE:
    GEOCODERS = [
        ("Nominatim", Nominatim(
            user_agent="commercial_real_estate_test/1.0", 
            timeout=20
        )),
        ("Photon", Photon(timeout=15)),
    ]
else:
    GEOCODERS = []

# Расширенные границы для Москвы и МО
MOSCOW_EXTENDED_BBOX = {
    'north': 57.5,
    'south': 54.5,
    'east': 40.5,
    'west': 35.0
}

# Кэш для тестирования
_test_geocoding_cache = {}

# ============================================================================
# GPT-УЛУЧШЕНИЕ АДРЕСОВ
# ============================================================================

def improve_address_with_gpt(address: str) -> str:
    """Улучшает адрес с помощью GPT для лучшего геокодирования"""
    if not address or len(address) < 10:
        return address
    
    # Простые случаи не отправляем в GPT
    if any(marker in address.lower() for marker in ['россия,', 'moscow,', 'russia,']):
        return address
    
    try:
        prompt = f"""
        Улучши следующий российский адрес для геокодирования:
        "{address}"
        
        Правила:
        1. Добавь "Россия," в начало если нет страны
        2. Стандартизируй: г. → город, ул. → улица, д. → дом
        3. Убери лишнее: номера квартир, этажи, офисы
        4. Для Москвы: убери административные округа, оставь только улицу
        5. Для МО: оставь область, город, улицу
        6. Исправь опечатки в названиях
        
        Верни ТОЛЬКО улучшенный адрес без объяснений.
        """
        
        response = sync_chat(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты эксперт по стандартизации российских адресов."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=100
        )
        
        improved = response.strip().strip('"').strip("'")
        if len(improved) > 10 and len(improved) < len(address) * 2:
            logger.debug(f"GPT улучшил: '{address}' → '{improved}'")
            return improved
        else:
            logger.warning(f"GPT вернул странный результат: '{improved}'")
            return address
            
    except Exception as e:
        logger.debug(f"GPT ошибка: {e}")
        return address

# ============================================================================
# УМНАЯ НОРМАЛИЗАЦИЯ АДРЕСОВ
# ============================================================================

def create_address_variants(address: str) -> List[str]:
    """Создает список вариантов адреса для геокодирования"""
    if not address:
        return []
    
    variants = []
    
    # 1. Оригинальный адрес
    variants.append(address)
    
    # 2. GPT-улучшенный адрес
    try:
        improved = improve_address_with_gpt(address)
        if improved != address:
            variants.append(improved)
    except Exception as e:
        logger.debug(f"Пропуск GPT улучшения: {e}")
    
    # 3. Базовая очистка
    cleaned = re.sub(r'\s+', ' ', address.strip())
    if cleaned != address:
        variants.append(cleaned)
    
    # 4. Удаление дублирующихся частей
    parts = [p.strip() for p in cleaned.split(',')]
    if len(parts) > 1:
        seen_parts = []
        for part in parts:
            part_lower = part.lower()
            is_duplicate = False
            
            for seen in seen_parts:
                if (part_lower in seen.lower() or seen.lower() in part_lower) and len(part_lower) > 5:
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                seen_parts.append(part)
        
        deduplicated = ', '.join(seen_parts)
        if deduplicated != cleaned:
            variants.append(deduplicated)
    
    # 5. Стандартизация сокращений
    standardized = address
    replacements = {
        r'\bг\.?\s*': 'город ',
        r'\bул\.?\s*': 'улица ',
        r'\bд\.?\s*': 'дом ',
        r'\bпр-т\.?\s*': 'проспект ',
        r'\bпер\.?\s*': 'переулок ',
        r'\bнаб\.?\s*': 'набережная ',
    }
    
    for pattern, replacement in replacements.items():
        standardized = re.sub(pattern, replacement, standardized, flags=re.IGNORECASE)
    
    if standardized != address:
        variants.append(standardized)
    
    # 6. Упрощенные варианты для Москвы
    if 'москва' in address.lower():
        street_match = re.search(r'(?:улица?|ул\.?|проспект|набережная|бульвар)\s+([^,]+?)(?:,|\s+дом|\s+\d)', address, re.IGNORECASE)
        if street_match:
            street_part = street_match.group(0).strip()
            variants.extend([
                f"Москва, {street_part}",
                f"Moscow, Russia, {street_part}",
                f"Россия, Москва, {street_part}"
            ])
    
    # 7. Упрощенные варианты для МО
    elif 'московская область' in address.lower():
        city_match = re.search(r'(?:город|г\.?)\s+([А-Яа-я\-]+)', address, re.IGNORECASE)
        if city_match:
            city = city_match.group(1)
            variants.extend([
                f"Московская область, {city}",
                f"Moscow Oblast, Russia, {city}",
                f"{city}, Moscow Oblast, Russia"
            ])
    
    # Убираем дубликаты, сохраняя порядок
    unique_variants = []
    for variant in variants:
        if variant not in unique_variants and len(variant) > 5:
            unique_variants.append(variant)
    
    logger.debug(f"Создано {len(unique_variants)} вариантов для: {address}")
    return unique_variants

# ============================================================================
# МНОЖЕСТВЕННОЕ ГЕОКОДИРОВАНИЕ
# ============================================================================

def is_reasonable_coordinates(lat: float, lon: float) -> bool:
    """Проверяет, что координаты в разумных пределах"""
    return (MOSCOW_EXTENDED_BBOX['south'] <= lat <= MOSCOW_EXTENDED_BBOX['north'] and 
            MOSCOW_EXTENDED_BBOX['west'] <= lon <= MOSCOW_EXTENDED_BBOX['east'])

def try_geocode_with_geocoder(geocoder_name: str, geocoder, text: str) -> Optional[Tuple[float, float]]:
    """Пробует геокодировать адрес одним геокодером"""
    try:
        logger.debug(f"🔍 {geocoder_name}: '{text}'")
        
        location = geocoder.geocode(
            text,
            exactly_one=True,
            timeout=15,
            language='ru' if hasattr(geocoder, 'language') else None
        )
        
        if location:
            coords = (location.longitude, location.latitude)
            logger.debug(f"✅ {geocoder_name}: {coords}")
            
            if is_reasonable_coordinates(location.latitude, location.longitude):
                return coords
            else:
                logger.debug(f"❌ {geocoder_name}: координаты вне области")
                
    except (GeocoderTimedOut, GeocoderServiceError, GeocoderQuotaExceeded) as e:
        logger.debug(f"⚠️ {geocoder_name} недоступен: {e}")
    except Exception as e:
        logger.debug(f"❌ {geocoder_name} ошибка: {e}")
        
    return None

async def test_geocode_address(address: str) -> Optional[Tuple[float, float]]:
    """Тестовая функция множественного геокодирования"""
    if not address:
        return None

    # Проверяем кэш
    if address in _test_geocoding_cache:
        logger.debug(f"💾 Кэш: {address}")
        return _test_geocoding_cache[address]

    logger.info(f"🌍 Мульти-геокодирование: '{address}'")

    if not GEOPY_AVAILABLE:
        logger.warning("❌ geopy недоступен - используем заглушку")
        return None

    # Получаем варианты адреса
    address_variants = create_address_variants(address)
    logger.info(f"📝 Создано вариантов: {len(address_variants)}")

    # Пробуем все комбинации
    for i, variant in enumerate(address_variants, 1):
        logger.debug(f"🧪 Вариант {i}/{len(address_variants)}: '{variant}'")
        
        for geocoder_name, geocoder in GEOCODERS:
            coords = try_geocode_with_geocoder(geocoder_name, geocoder, variant)
            if coords:
                logger.info(f"✅ {geocoder_name} успех: '{variant}' → {coords}")
                _test_geocoding_cache[address] = coords
                return coords

    # Кэшируем неудачу
    _test_geocoding_cache[address] = None
    logger.warning(f"❌ Все попытки неудачны: '{address}'")
    return None

# ============================================================================
# РАСЧЕТ РАССТОЯНИЙ (простой)
# ============================================================================

def haversine_distance(p1: Tuple[float, float], p2: Tuple[float, float]) -> float:
    """Расстояние по прямой (формула гаверсинуса)"""
    lon1, lat1 = p1
    lon2, lat2 = p2
    
    R = 6371.0  # Радиус Земли в км
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    
    a = (math.sin(dlat / 2) ** 2 + 
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c

# ============================================================================
# ОСНОВНЫЕ ТЕСТОВЫЕ ФУНКЦИИ
# ============================================================================

async def test_address_geocoding(address: str) -> dict:
    """Тестирует геокодирование одного адреса"""
    logger.info(f"🔍 ТЕСТ АДРЕСА: {address}")
    
    result = {
        "original_address": address,
        "address_components": None,
        "coordinates": None,
        "geocoding_success": False,
        "variants_tried": 0,
        "successful_variant": None,
        "geocoder_used": None,
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
        
        # 2. Создаем варианты для геокодирования
        variants = create_address_variants(address)
        result["variants_tried"] = len(variants)
        logger.info(f"🔄 Вариантов для тестирования: {len(variants)}")
        
        # 3. Тестируем геокодирование
        coords = await test_geocode_address(address)
        if coords:
            result["coordinates"] = {"lon": coords[0], "lat": coords[1]}
            result["geocoding_success"] = True
            logger.info(f"✅ УСПЕХ: {coords[0]:.6f}, {coords[1]:.6f}")
        else:
            logger.warning("❌ НЕУДАЧА: геокодирование не удалось")
            
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"❌ ОШИБКА: {e}")
    
    return result

async def test_mock_lot_with_offers(lot_address: str) -> dict:
    """Тестирует лот с мок-объявлениями (без реального парсинга ЦИАН)"""
    logger.info(f"🏢 ТЕСТ ЛОТА: {lot_address}")
    
    result = {
        "lot_address": lot_address,
        "lot_coords": None,
        "mock_offers_tested": 0,
        "offers_geocoded": 0,
        "distances": [],
        "errors": []
    }
    
    try:
        # 1. Геокодируем адрес лота
        lot_coords = await test_geocode_address(lot_address)
        if not lot_coords:
            result["errors"].append("Не удалось геокодировать адрес лота")
            return result
            
        result["lot_coords"] = {"lon": lot_coords[0], "lat": lot_coords[1]}
        logger.info(f"📍 Лот: {lot_coords[0]:.6f}, {lot_coords[1]:.6f}")
        
        # 2. Создаем мок-объявления рядом с лотом
        mock_offers = generate_mock_offers_near_lot(lot_address, lot_coords)
        result["mock_offers_tested"] = len(mock_offers)
        logger.info(f"📋 Создано мок-объявлений: {len(mock_offers)}")
        
        # 3. Тестируем геокодирование объявлений
        for i, (offer_id, offer_address) in enumerate(mock_offers, 1):
            logger.info(f"📍 Объявление {i}: {offer_address}")
            
            try:
                offer_coords = await test_geocode_address(offer_address)
                if offer_coords:
                    result["offers_geocoded"] += 1
                    
                    # Рассчитываем расстояние
                    distance = haversine_distance(lot_coords, offer_coords)
                    result["distances"].append({
                        "offer_id": offer_id,
                        "offer_address": offer_address,
                        "offer_coords": {"lon": offer_coords[0], "lat": offer_coords[1]},
                        "distance_km": distance
                    })
                    logger.info(f"📏 Расстояние: {distance:.2f} км")
                else:
                    result["errors"].append(f"Не удалось геокодировать: {offer_address}")
                    
            except Exception as e:
                result["errors"].append(f"Ошибка {offer_id}: {str(e)}")
                
    except Exception as e:
        result["errors"].append(f"Общая ошибка: {str(e)}")
        logger.error(f"❌ Ошибка: {e}")
    
    return result

def generate_mock_offers_near_lot(lot_address: str, lot_coords: Tuple[float, float]) -> List[Tuple[str, str]]:
    """Генерирует мок-объявления рядом с лотом"""
    mock_offers = []
    
    if 'москва' in lot_address.lower():
        # Мок-объявления для Москвы
        mock_offers = [
            ("mock_001", "Москва, улица Тверская, дом 10"),
            ("mock_002", "г Москва, Красная площадь, дом 1"),
            ("mock_003", "Москва, Пресненская набережная, дом 8"),
            ("mock_004", "г Москва, ул Арбат, дом 15"),
            ("mock_005", "Москва, Ленинский проспект, дом 25")
        ]
    elif 'подольск' in lot_address.lower():
        # Мок-объявления для Подольска
        mock_offers = [
            ("mock_101", "Московская область, г Подольск, ул Правды, дом 25"),
            ("mock_102", "Подольск, улица Ленина, дом 5"),
            ("mock_103", "г Подольск, Комсомольская улица, дом 12"),
            ("mock_104", "Московская область, Подольск, ул Большая Серпуховская, дом 30")
        ]
    else:
        # Универсальные мок-объявления
        mock_offers = [
            ("mock_201", "Москва, улица Мясницкая, дом 20"),
            ("mock_202", "Московская область, г Химки, ул Ленина, дом 10"),
            ("mock_203", "г Москва, Садовое кольцо, дом 5")
        ]
    
    return mock_offers

# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ ТЕСТИРОВАНИЯ
# ============================================================================

async def main():
    """Главная функция тестирования улучшенного геокодирования"""
    logger.info("🚀 ЗАПУСК РАСШИРЕННОГО ТЕСТИРОВАНИЯ ГЕОКОДИРОВАНИЯ")
    logger.info("=" * 80)
    
    # Проверяем доступность геокодеров
    if not GEOPY_AVAILABLE:
        logger.error("❌ geopy не установлен!")
        logger.info("💡 Установите: pip install geopy")
        return
    
    logger.info(f"✅ Доступно геокодеров: {len(GEOCODERS)}")
    for name, _ in GEOCODERS:
        logger.info(f"   - {name}")
    
    # Тестовые адреса - более разнообразные
    test_addresses = [
        # Простые московские адреса
        "г Москва, ул Тверская, дом 7",
        "Москва, Пресненская набережная, дом 12",
        
        # Проблемные адреса из логов
        "обл Московская, г.о. Подольск, г Подольск, ул Правды, дом 20 Московская область, г. Подольск, ул. Правды, д. 20, пом. 1",
        "Московская область, г Подольск, ул Правды, дом 20",
        
        # Адреса с административными округами
        "г Москва, ВАО, Перово, ул Новогиреевская, д 42",
        "Москва, СВАО, район Останкинский, проспект Мира, 119с536",
        
        # Адреса МО
        "Московская область, г Химки, ул Загородная, дом 4",
        "обл Московская, г Домодедово, мкр Северный, ул Советская, дом 50",
        
        # Сложные адреса
        "г Москва муниципальный округ Басманный ул Покровка дом 42",
        "Зеленоград, корпус 847"
    ]
    
    logger.info(f"📝 Будет протестировано адресов: {len(test_addresses)}")
    
    results = []
    
    # Тестируем каждый адрес
    for i, address in enumerate(test_addresses, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"🧪 ТЕСТ {i}/{len(test_addresses)}")
        logger.info(f"{'='*60}")
        
        try:
            # Тестируем простое геокодирование
            address_result = await test_address_geocoding(address)
            
            # Если геокодирование успешно, тестируем с мок-объявлениями
            if address_result["geocoding_success"]:
                lot_result = await test_mock_lot_with_offers(address)
                lot_result["address_test"] = address_result
                results.append(lot_result)
            else:
                logger.warning("⚠️ Пропускаем тест объявлений (адрес не геокодируется)")
                results.append({
                    "lot_address": address,
                    "error": "Геокодирование не удалось",
                    "address_test": address_result
                })
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            results.append({
                "lot_address": address,
                "error": f"Критическая ошибка: {str(e)}"
            })
        
        # Пауза между тестами
        if i < len(test_addresses):
            logger.info("⏱️ Пауза 2 секунды...")
            await asyncio.sleep(2)
    
    # Сохраняем результаты
    timestamp = int(asyncio.get_event_loop().time())
    results_file = f"enhanced_geocoding_test_results_{timestamp}.json"
    
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    
    # Итоговая статистика
    logger.info(f"\n{'='*60}")
    logger.info(f"📊 ИТОГИ ТЕСТИРОВАНИЯ")
    logger.info(f"{'='*60}")
    
    total_tests = len(results)
    successful_geocoding = len([r for r in results if not r.get("error")])
    successful_with_offers = len([r for r in results if r.get("offers_geocoded", 0) > 0])
    
    logger.info(f"Всего тестов: {total_tests}")
    logger.info(f"Успешно геокодированных лотов: {successful_geocoding}")
    logger.info(f"Лотов с геокодированными объявлениями: {successful_with_offers}")
    
    if total_tests > 0:
        success_rate = successful_geocoding / total_tests * 100
        logger.info(f"Процент успеха геокодирования: {success_rate:.1f}%")
    
    total_offers = sum(r.get("mock_offers_tested", 0) for r in results)
    total_geocoded_offers = sum(r.get("offers_geocoded", 0) for r in results)
    
    if total_offers > 0:
        offers_success_rate = total_geocoded_offers / total_offers * 100
        logger.info(f"Успешность геокодирования объявлений: {offers_success_rate:.1f}%")
    
    logger.info(f"Результаты сохранены в: {results_file}")
    
    # Показываем примеры успешных и неуспешных случаев
    successful_examples = [r for r in results if not r.get("error")][:3]
    if successful_examples:
        logger.info(f"\n✅ Примеры успешных:")
        for example in successful_examples:
            logger.info(f"   {example['lot_address'][:50]}...")
    
    failed_examples = [r for r in results if r.get("error")][:3]
    if failed_examples:
        logger.info(f"\n❌ Примеры неудачных:")
        for example in failed_examples:
            logger.info(f"   {example['lot_address'][:50]}... - {example['error']}")

if __name__ == "__main__":
    asyncio.run(main())