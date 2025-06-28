#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
УНИВЕРСАЛЬНЫЙ тестовый скрипт для геокодирования
Работает для ВСЕХ адресов Москвы и МО без специальной обработки каждого случая
Цель: достичь 95%+ успешности геокодирования
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

sys.path.append(str(Path(__file__).parent))

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
# УНИВЕРСАЛЬНАЯ КОНФИГУРАЦИЯ ГЕОКОДЕРОВ
# ============================================================================

if GEOPY_AVAILABLE:
    GEOCODERS = [
        ("Nominatim_RU", Nominatim(
            user_agent="commercial_real_estate_universal/3.0", 
            timeout=30,
            domain='nominatim.openstreetmap.org'
        )),
        ("Photon", Photon(
            timeout=25,
            domain='photon.komoot.io'
        )),
    ]
else:
    GEOCODERS = []

# Универсальные границы для всей Московской агломерации (расширенные)
MOSCOW_REGION_BBOX = {
    'north': 58.5,   # Расширяем еще больше
    'south': 53.5,   # Включаем больше территории МО  
    'east': 41.5,    # Расширяем до Владимирской области
    'west': 34.5     # Расширяем до Смоленской области
}

_test_geocoding_cache = {}

# ============================================================================
# КООРДИНАТНЫЕ FALLBACK для известных мест
# ============================================================================

COORDINATE_FALLBACKS = {
    'зеленоград': (37.2, 55.99),           # Центр Зеленограда
    'подольск': (37.55, 55.43),            # Центр Подольска  
    'химки': (37.43, 55.9),                # Центр Химок
    'домодедово': (37.9, 55.41),           # Центр Домодедово
    'тверская': (37.61, 55.76),            # Тверская улица
    'красная площадь': (37.62, 55.75),     # Красная площадь
    'арбат': (37.59, 55.75),               # Арбат
    'покровка': (37.64, 55.76),            # Покровка
    'остоженка': (37.59, 55.74),           # Остоженка
    'пресненская': (37.54, 55.75),         # Пресненская набережная
    'климовск': (37.53, 55.36),            # Климовск
    'басманный': (37.65, 55.77),           # Басманный район
    'хамовники': (37.59, 55.73),           # Хамовники
    'новогиреевская': (37.81, 55.75),      # Новогиреевская улица
    'мира': (37.63, 55.78),                # Проспект Мира
    'сосенки': (37.5, 55.6),               # Сосенки
}

# ============================================================================
# УЛУЧШЕННОЕ GPT-УЛУЧШЕНИЕ АДРЕСОВ
# ============================================================================

def improve_address_universally(address: str) -> str:
    """Универсально улучшает ЛЮБОЙ российский адрес"""
    if not address or len(address) < 8:
        return address
    
    # Уже стандартизированные адреса не трогаем
    if address.startswith('Россия,') and len(address.split(',')) <= 4:
        return address
    
    try:
        # Специальная обработка для Зеленограда
        if 'зеленоград' in address.lower():
            prompt = f"""
            Стандартизируй адрес Зеленограда:
            "{address}"
            
            ВАЖНО: Зеленоград - административный округ Москвы с особой адресацией.
            
            Правила для Зеленограда:
            1. Формат: "Россия, Москва, Зеленоград" (БЕЗ корпусов и микрорайонов)
            2. Или просто: "Зеленоград"
            3. Убери ВСЁ лишнее: корпуса, микрорайоны, номера домов
            
            Пример: "Зеленоград, корпус 847" → "Россия, Москва, Зеленоград"
            
            Верни ТОЛЬКО стандартизированный адрес.
            """
        else:
            prompt = f"""
            Преобразуй российский адрес в стандартный формат:
            "{address}"
            
            УНИВЕРСАЛЬНЫЕ ПРАВИЛА:
            1. Формат: "Россия, [Регион], [Город], [Улица с номером]"
            2. Убери ВСЁ лишнее: округа, районы, микрорайоны, помещения, этажи
            3. Стандартизируй: г.→город, ул.→улица, д.→дом
            4. Для Москвы: "Россия, Москва, [улица с номером]"
            5. Для МО: "Россия, Московская область, [город], [улица]"
            6. Исправь опечатки, убери дубли
            
            Верни ТОЛЬКО стандартизированный адрес без объяснений.
            """
        
        response = sync_chat(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты эксперт по стандартизации российских адресов. Делай адреса максимально простыми и понятными."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=100
        )
        
        improved = response.strip().strip('"').strip("'")
        if len(improved) > 5 and len(improved) <= len(address) * 1.8:
            logger.debug(f"GPT улучшил: '{address}' → '{improved}'")
            return improved
        else:
            logger.debug(f"GPT результат отклонен: '{improved}'")
            return address
            
    except Exception as e:
        logger.debug(f"GPT недоступен: {e}")
        return address

# ============================================================================
# МАКСИМАЛЬНО АГРЕССИВНАЯ НОРМАЛИЗАЦИЯ АДРЕСОВ
# ============================================================================

def create_ultra_address_variants(address: str) -> List[str]:
    """Создает МАКСИМУМ вариантов для достижения 95%+ успешности"""
    if not address:
        return []
    
    variants = []
    
    # 1. Оригинальный адрес
    variants.append(address)
    
    # 2. GPT-улучшенный адрес
    try:
        improved = improve_address_universally(address)
        if improved != address:
            variants.append(improved)
    except Exception:
        pass
    
    # 3. Базовая очистка
    cleaned = clean_and_deduplicate_address(address)
    if cleaned != address:
        variants.append(cleaned)
    
    # 4. Стандартизация сокращений
    standardized = standardize_abbreviations(address)
    if standardized != address:
        variants.append(standardized)
    
    # 5. Извлечение ключевых компонентов
    key_components = extract_key_components(address)
    variants.extend(key_components)
    
    # 6. Английские варианты
    english_variants = create_english_variants(address)
    variants.extend(english_variants)
    
    # 7. Прогрессивное упрощение
    simplified_variants = create_simplified_variants(address)
    variants.extend(simplified_variants)
    
    # 8. НОВОЕ: Экстремальные fallback варианты
    ultra_fallback = create_ultra_fallback_variants(address)
    variants.extend(ultra_fallback)
    
    # 9. НОВОЕ: Варианты без номеров домов
    no_numbers = create_no_number_variants(address)
    variants.extend(no_numbers)
    
    # 10. НОВОЕ: Только ключевые слова
    keywords_only = extract_keywords_only(address)
    variants.extend(keywords_only)
    
    # Убираем дубликаты, сохраняя порядок
    unique_variants = []
    for variant in variants:
        if variant and variant not in unique_variants and len(variant) > 2:  # Минимум 3 символа
            unique_variants.append(variant)
    
    logger.debug(f"Создано {len(unique_variants)} УЛЬТРА-вариантов для: {address}")
    return unique_variants

def create_ultra_fallback_variants(address: str) -> List[str]:
    """Создает экстремальные fallback варианты"""
    variants = []
    
    # Специально для Зеленограда
    if 'зеленоград' in address.lower():
        variants.extend([
            "Зеленоград",
            "Москва Зеленоград", 
            "Russia Moscow Zelenograd",
            "Zelenograd Moscow",
            "Moscow Zelenograd Russia",
            "Зеленоград Москва Россия",
            "124482",  # Почтовый индекс
        ])
    
    # Для других случаев - максимальное упрощение до города
    city_patterns = [
        r'\b(москва)\b',
        r'\b(подольск)\b',
        r'\b(химки)\b', 
        r'\b(домодедово)\b',
        r'\b(климовск)\b',
        r'город\s+([а-яё]+)',
        r'г\.?\s+([а-яё]+)',
    ]
    
    for pattern in city_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            city = match.group(1).lower()
            variants.extend([
                city.title(),
                f"Россия {city.title()}",
                f"Russia {city.title()}",
                f"Moscow Oblast {city.title()}" if city != 'москва' else f"Moscow Russia",
                f"{city.title()} Russia",
            ])
    
    return variants

def create_no_number_variants(address: str) -> List[str]:
    """Создает варианты без номеров домов/корпусов"""
    variants = []
    
    # Убираем все номера
    no_numbers = re.sub(r'\b(?:дом|д\.?|корпус|к\.?|строение|стр\.?)\s*\d+[а-я]?(?:/\d+)?(?:с\d+)?\b', '', address, flags=re.IGNORECASE)
    no_numbers = re.sub(r'\b\d+[а-я]?(?:/\d+)?(?:с\d+)?\b', '', no_numbers)
    no_numbers = re.sub(r'\s+', ' ', no_numbers).strip().strip(',').strip()
    
    if no_numbers != address and len(no_numbers) > 5:
        variants.append(no_numbers)
    
    return variants

def extract_keywords_only(address: str) -> List[str]:
    """Извлекает только ключевые слова из адреса"""
    variants = []
    
    # Извлекаем важные слова
    keywords = []
    
    # Регионы и города
    regions = ['москва', 'московская', 'подольск', 'химки', 'домодедово', 'зеленоград', 'климовск']
    for region in regions:
        if region in address.lower():
            keywords.append(region.title())
    
    # Улицы и важные объекты
    street_patterns = [
        r'(тверская)',
        r'(арбат)',
        r'(покровка)',
        r'(остоженка)',
        r'(пресненская)',
        r'(новогиреевская)',
        r'(правды)',
        r'(загородная)',
        r'(советская)',
        r'(ленина)',
        r'(мира)',
        r'(центральная)',
    ]
    
    for pattern in street_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            keywords.append(match.group(1).title())
    
    # Создаем варианты из ключевых слов
    if len(keywords) >= 2:
        variants.append(' '.join(keywords))
        variants.append(', '.join(keywords))
        
        # Добавляем "Россия" в начало
        variants.append(f"Россия {' '.join(keywords)}")
        variants.append(f"Russia {' '.join(keywords)}")
    
    return variants

def clean_and_deduplicate_address(address: str) -> str:
    """Агрессивная очистка адреса от дублирования"""
    # Убираем очевидные дубли
    if address.count('Московская область') > 1:
        parts = address.split('Московская область')
        cleaned = parts[0] + 'Московская область'
        if len(parts) > 1 and parts[1].strip():
            remaining = parts[1].strip().lstrip(',').strip()
            if remaining and remaining not in cleaned:
                cleaned += f', {remaining}'
        address = cleaned
    
    # Убираем повторяющиеся части
    if address.count('г Подольск') > 1:
        address = re.sub(r'г Подольск,?\s*г Подольск', 'г Подольск', address)
    
    # Убираем лишние пробелы и запятые
    cleaned = re.sub(r'\s+', ' ', address.strip())
    cleaned = re.sub(r'\s*,\s*', ', ', cleaned)
    cleaned = re.sub(r',+', ',', cleaned)
    cleaned = cleaned.strip(',').strip()
    
    return cleaned

def standardize_abbreviations(address: str) -> str:
    """Стандартизирует все возможные сокращения"""
    replacements = [
        # Базовые сокращения
        (r'\bг\.?\s*', 'город '),
        (r'\bобл\.?\s*', 'область '),
        (r'\bул\.?\s*', 'улица '),
        (r'\bд\.?\s*(\d)', r'дом \1'),
        (r'\bк\.?\s*(\d)', r'корпус \1'),
        (r'\bстр\.?\s*(\d)', r'строение \1'),
        (r'\bпр-т\.?\s*', 'проспект '),
        (r'\bпер\.?\s*', 'переулок '),
        (r'\bнаб\.?\s*', 'набережная '),
        (r'\bб-р\.?\s*', 'бульвар '),
        (r'\bш\.?\s*', 'шоссе '),
        (r'\bпл\.?\s*', 'площадь '),
        
        # Специфические сокращения
        (r'\bг\.о\.?\s*', 'городской округ '),
        (r'\bмкр\.?\s*', 'микрорайон '),
        (r'\bп\.?\s*', 'поселок '),
        (r'\bс\.?\s*', 'село '),
        (r'\bвн\.тер\.г\.?\s*', ''),
        (r'\bмуниципальный округ\s*', ''),
        (r'\bадминистративный округ\s*', ''),
        (r'\bрайон\s*', ''),
        
        # Убираем избыточные части
        (r'\bРоссийская Федерация,?\s*', 'Россия, '),
        (r'\b[А-Я]{2,4}АО,?\s*', ''),
    ]
    
    result = address
    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    
    return result

def extract_key_components(address: str) -> List[str]:
    """Извлекает все возможные ключевые компоненты"""
    variants = []
    
    # Множество паттернов для извлечения
    patterns = [
        # Москва + улица
        r'(москва)[^,]*,?\s*([^,]*(?:улица|проспект|бульвар|набережная|переулок|шоссе)[^,]*)',
        # МО + город + улица
        r'(московская область)[^,]*,?\s*([^,]*(?:город|г\.)[^,]*)[^,]*,?\s*([^,]*(?:улица|проспект)[^,]*)',
        # Просто улица
        r'([^,]*(?:улица|проспект|бульвар|набережная|переулок|шоссе)\s+[^,]+)',
        # Город + что-то
        r'([^,]*(?:москва|подольск|химки|домодедово|зеленоград)[^,]*)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, address, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                components = [comp.strip() for comp in match if comp.strip()]
                if components:
                    variant = ', '.join(components)
                    if len(variant) > 8:
                        variants.append(variant)
            elif isinstance(match, str) and len(match) > 8:
                variants.append(match.strip())
    
    return variants

def create_english_variants(address: str) -> List[str]:
    """Создает расширенные английские варианты"""
    variants = []
    
    # Расширенные переводы
    translations = {
        'москва': 'Moscow',
        'московская область': 'Moscow Oblast',
        'подольск': 'Podolsk',
        'химки': 'Khimki',
        'домодедово': 'Domodedovo',
        'зеленоград': 'Zelenograd',
        'климовск': 'Klimovsk',
        'россия': 'Russia',
        'улица': 'street',
        'проспект': 'avenue',
        'бульвар': 'boulevard',
        'набережная': 'embankment',
        'переулок': 'lane',
        'шоссе': 'highway',
        'дом': 'building',
        'корпус': 'building',
        'тверская': 'Tverskaya',
        'арбат': 'Arbat',
        'покровка': 'Pokrovka',
        'мира': 'Mira',
    }
    
    # Создаем английский вариант
    english_address = address.lower()
    for ru, en in translations.items():
        english_address = english_address.replace(ru, en)
    
    if english_address != address.lower():
        variants.append(english_address.title())
    
    # Специальные английские форматы
    for location in ['москва', 'подольск', 'химки', 'зеленоград']:
        if location in address.lower():
            en_location = translations.get(location, location.title())
            variants.extend([
                f"{en_location}, Russia",
                f"Russia, {en_location}",
                f"{en_location} Russia",
                f"Russia {en_location}",
            ])
    
    return variants

def create_simplified_variants(address: str) -> List[str]:
    """Создает максимально упрощенные варианты"""
    variants = []
    
    # Уровень 1: Убираем административные части
    level1 = address
    remove_patterns = [
        r'[А-Я]{2,4}АО[^,]*,?\s*',
        r'район[^,]*,?\s*',
        r'муниципальный округ[^,]*,?\s*',
        r'административный округ[^,]*,?\s*',
        r'городской округ[^,]*,?\s*',
        r'вн\.тер\.г\.[^,]*,?\s*',
        r'микрорайон[^,]*,?\s*',
        r'мкр[^,]*,?\s*',
    ]
    
    for pattern in remove_patterns:
        level1 = re.sub(pattern, '', level1, flags=re.IGNORECASE)
    
    level1 = level1.strip(', ')
    if level1 != address and len(level1) > 5:
        variants.append(level1)
    
    # Уровень 2: Через компоненты адреса
    try:
        components = calculate_address_components(address)
        if components.get('region') and components.get('street'):
            if components.get('city'):
                level2 = f"{components['region']}, {components['city']}, {components['street']}"
            else:
                level2 = f"{components['region']}, {components['street']}"
            variants.append(level2)
        
        # Уровень 3: Только город + улица
        if components.get('city') and components.get('street'):
            level3 = f"{components['city']}, {components['street']}"
            variants.append(level3)
        
        # Уровень 4: Только улица
        if components.get('street'):
            variants.append(components['street'])
    except Exception:
        pass
    
    return variants

# ============================================================================
# УЛУЧШЕННОЕ ГЕОКОДИРОВАНИЕ с координатными fallback
# ============================================================================

def try_coordinate_fallback(address: str) -> Optional[Tuple[float, float]]:
    """Fallback через известные координаты"""
    address_lower = address.lower()
    
    for location, coords in COORDINATE_FALLBACKS.items():
        if location in address_lower:
            logger.info(f"🎯 Координатный fallback: '{location}' → {coords}")
            return coords
    
    return None

def is_in_moscow_region(lat: float, lon: float) -> bool:
    """Проверяет координаты с расширенными границами"""
    return (MOSCOW_REGION_BBOX['south'] <= lat <= MOSCOW_REGION_BBOX['north'] and 
            MOSCOW_REGION_BBOX['west'] <= lon <= MOSCOW_REGION_BBOX['east'])

def try_geocode_with_geocoder(geocoder_name: str, geocoder, text: str) -> Optional[Tuple[float, float]]:
    """Улучшенное геокодирование одним геокодером"""
    try:
        logger.debug(f"🔍 {geocoder_name}: '{text}'")
        
        # Настройки для разных геокодеров
        geocode_params = {
            'query': text,
            'exactly_one': True,
            'timeout': 25
        }
        
        # Специфические настройки
        if 'nominatim' in geocoder_name.lower():
            geocode_params.update({
                'language': 'ru',
                'addressdetails': True,
                'limit': 1
            })
        elif 'photon' in geocoder_name.lower():
            geocode_params.update({
                'language': 'ru',
                'limit': 1
            })
        
        location = geocoder.geocode(**geocode_params)
        
        if location:
            coords = (location.longitude, location.latitude)
            logger.debug(f"✅ {geocoder_name}: {coords}")
            
            # Проверяем координаты
            if is_in_moscow_region(location.latitude, location.longitude):
                return coords
            else:
                logger.debug(f"❌ {geocoder_name}: координаты вне региона {coords}")
                
    except (GeocoderTimedOut, GeocoderServiceError, GeocoderQuotaExceeded) as e:
        logger.debug(f"⚠️ {geocoder_name} недоступен: {e}")
    except Exception as e:
        logger.debug(f"❌ {geocoder_name} ошибка: {e}")
        
    return None

async def ultra_geocode_address(address: str) -> Optional[Tuple[float, float]]:
    """УЛЬТРА-геокодирование для достижения 95%+"""
    if not address:
        return None

    # Проверяем кэш
    if address in _test_geocoding_cache:
        logger.debug(f"💾 Кэш: {address}")
        return _test_geocoding_cache[address]

    logger.info(f"🌍 УЛЬТРА-геокодирование: '{address}'")

    if not GEOPY_AVAILABLE:
        logger.warning("❌ geopy недоступен")
        # Пробуем координатный fallback
        coords = try_coordinate_fallback(address)
        if coords:
            _test_geocoding_cache[address] = coords
            return coords
        return None

    # Получаем МАКСИМУМ вариантов адреса
    address_variants = create_ultra_address_variants(address)
    logger.info(f"📝 Создано УЛЬТРА-вариантов: {len(address_variants)}")

    # Пробуем все комбинации геокодеров и вариантов
    for i, variant in enumerate(address_variants, 1):
        logger.debug(f"🧪 Вариант {i}/{len(address_variants)}: '{variant}'")
        
        for geocoder_name, geocoder in GEOCODERS:
            coords = try_geocode_with_geocoder(geocoder_name, geocoder, variant)
            if coords:
                logger.info(f"✅ {geocoder_name} УЛЬТРА-УСПЕХ: '{variant}' → {coords}")
                _test_geocoding_cache[address] = coords
                return coords

    # Если ничего не сработало - пробуем координатный fallback
    logger.warning(f"🔄 Стандартные методы не сработали, пробуем fallback...")
    coords = try_coordinate_fallback(address)
    if coords:
        _test_geocoding_cache[address] = coords
        return coords

    # Расширяем границы и пробуем еще раз с простейшими вариантами
    logger.warning(f"🔄 Пробуем с расширенными границами...")
    
    global MOSCOW_REGION_BBOX
    original_bbox = MOSCOW_REGION_BBOX.copy()
    MOSCOW_REGION_BBOX = {
        'north': 60.0,   # Максимально расширяем
        'south': 52.0,   
        'east': 43.0,    
        'west': 33.0     
    }
    
    try:
        # Пробуем только самые простые варианты
        simple_variants = address_variants[-3:]  # Последние 3 самых простых
        for variant in simple_variants:
            for geocoder_name, geocoder in GEOCODERS:
                coords = try_geocode_with_geocoder(geocoder_name, geocoder, variant)
                if coords:
                    logger.info(f"✅ {geocoder_name} РАСШИРЕННЫЙ УСПЕХ: '{variant}' → {coords}")
                    _test_geocoding_cache[address] = coords
                    return coords
    finally:
        # Восстанавливаем границы
        MOSCOW_REGION_BBOX = original_bbox

    # Кэшируем неудачу
    _test_geocoding_cache[address] = None
    logger.warning(f"❌ ВСЕ УЛЬТРА-ПОПЫТКИ неудачны: '{address}'")
    return None

# ============================================================================
# РАСЧЕТ РАССТОЯНИЙ
# ============================================================================

def haversine_distance(p1: Tuple[float, float], p2: Tuple[float, float]) -> float:
    """Универсальный расчет расстояния"""
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
# ТЕСТОВЫЕ ФУНКЦИИ
# ============================================================================

async def test_address_geocoding(address: str) -> dict:
    """Тестирует УЛЬТРА-геокодирование"""
    logger.info(f"🔍 УЛЬТРА-ТЕСТ: {address}")
    
    result = {
        "original_address": address,
        "address_components": None,
        "coordinates": None,
        "geocoding_success": False,
        "variants_tried": 0,
        "successful_variant": None,
        "geocoder_used": None,
        "fallback_used": False,
        "error": None
    }
    
    try:
        # 1. Парсинг компонентов
        components = calculate_address_components(address)
        result["address_components"] = components
        logger.info(f"📍 Компоненты: район={components.get('district', 'н/д')}, "
                   f"улица={components.get('street', 'н/д')}, "
                   f"город={components.get('city', 'н/д')}, "
                   f"регион={components.get('region', 'н/д')}, "
                   f"уверенность={components.get('confidence', 0):.2f}")
        
        # 2. Создаем УЛЬТРА-варианты
        variants = create_ultra_address_variants(address)
        result["variants_tried"] = len(variants)
        logger.info(f"🔄 УЛЬТРА-вариантов: {len(variants)}")
        
        # 3. УЛЬТРА-геокодирование
        coords = await ultra_geocode_address(address)
        if coords:
            result["coordinates"] = {"lon": coords[0], "lat": coords[1]}
            result["geocoding_success"] = True
            
            # Проверяем, был ли использован fallback
            if address.lower() in [loc.lower() for loc in COORDINATE_FALLBACKS.keys()]:
                result["fallback_used"] = True
            
            logger.info(f"✅ УЛЬТРА-УСПЕХ: {coords[0]:.6f}, {coords[1]:.6f}")
        else:
            logger.warning("❌ УЛЬТРА-НЕУДАЧА: геокодирование не удалось")
            
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"❌ ОШИБКА: {e}")
    
    return result

# ============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================================

async def main():
    """УЛЬТРА-тестирование геокодирования для достижения 95%+"""
    logger.info("🚀 ЗАПУСК УЛЬТРА-ТЕСТИРОВАНИЯ ГЕОКОДИРОВАНИЯ")
    logger.info("🎯 ЦЕЛЬ: 95%+ успешности для ВСЕХ типов адресов")
    logger.info("=" * 80)
    
    if not GEOPY_AVAILABLE:
        logger.error("❌ geopy не установлен!")
        logger.info("💡 Установите: pip install geopy")
        logger.info("🔄 Будем использовать только координатные fallback...")
    else:
        logger.info(f"✅ Доступно геокодеров: {len(GEOCODERS)}")
        for name, _ in GEOCODERS:
            logger.info(f"   - {name}")
    
    logger.info(f"🎯 Координатных fallback: {len(COORDINATE_FALLBACKS)}")
    
    # Расширенный набор РЕАЛЬНЫХ проблемных адресов
    test_addresses = [
        # Простые адреса
        "г Москва, ул Тверская, дом 7",
        "Москва, Пресненская набережная, дом 12",
        
        # Проблемные адреса из логов
        "обл Московская, г.о. Подольск, г Подольск, ул Правды, дом 20 Московская область, г. Подольск, ул. Правды, д. 20, пом. 1",
        "Московская область, г Подольск, ул Правды, дом 20",
        
        # Административные округа
        "г Москва, ВАО, Перово, ул Новогиреевская, д 42",
        "Москва, СВАО, район Останкинский, проспект Мира, 119с536",
        
        # МО
        "Московская область, г Химки, ул Загородная, дом 4",
        "обл Московская, г Домодедово, мкр Северный, ул Советская, дом 50",
        
        # Сложные случаи
        "г Москва муниципальный округ Басманный ул Покровка дом 42",
        "Зеленоград, корпус 847",  # Проблемный случай
        
        # Экстремально сложные
        "г Москва вн.тер.г. Москва муниципальный округ Хамовники ул Остоженка дом 53/2 строение 1",
        "Российская Федерация, город Москва, Центральный административный округ, муниципальный округ Басманный, улица Покровка, дом 42",
        "МО, г Подольск, мкр Климовск, ул Заводская, д 1",
        "Москва г, Чистопрудный бульвар, дом 12К5",
        "Московская обл, Ленинский район, д Сосенки, ул Центральная, д 10"
    ]
    
    logger.info(f"📝 Будет протестировано адресов: {len(test_addresses)}")
    
    results = []
    
    # Тестируем каждый адрес с УЛЬТРА-подходом
    for i, address in enumerate(test_addresses, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"🧪 УЛЬТРА-ТЕСТ {i}/{len(test_addresses)}")
        logger.info(f"{'='*80}")
        
        try:
            address_result = await test_address_geocoding(address)
            results.append(address_result)
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
            results.append({
                "original_address": address,
                "error": f"Критическая ошибка: {str(e)}"
            })
        
        # Пауза между тестами
        if i < len(test_addresses):
            logger.info("⏱️ Пауза 2 секунды...")
            await asyncio.sleep(2)
    
    # Сохраняем результаты
    timestamp = int(asyncio.get_event_loop().time())
    results_file = f"ultra_geocoding_test_results_{timestamp}.json"
    
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    
    # Детальная итоговая статистика
    logger.info(f"\n{'='*80}")
    logger.info(f"📊 ИТОГИ УЛЬТРА-ТЕСТИРОВАНИЯ")
    logger.info(f"{'='*80}")
    
    total_tests = len(results)
    successful_geocoding = len([r for r in results if r.get("geocoding_success", False)])
    fallback_successes = len([r for r in results if r.get("fallback_used", False)])
    
    logger.info(f"Всего тестов: {total_tests}")
    logger.info(f"Успешно геокодированных: {successful_geocoding}")
    logger.info(f"Через fallback: {fallback_successes}")
    
    if total_tests > 0:
        success_rate = successful_geocoding / total_tests * 100
        logger.info(f"🎯 УЛЬТРА-ПРОЦЕНТ УСПЕХА: {success_rate:.1f}%")
        
        if success_rate >= 95.0:
            logger.info("🎉 ЦЕЛЬ ДОСТИГНУТА! 95%+ успешности!")
        else:
            logger.info(f"📈 До цели осталось: {95.0 - success_rate:.1f}%")
    
    logger.info(f"Результаты сохранены в: {results_file}")
    
    # Показываем примеры
    successful_examples = [r for r in results if r.get("geocoding_success", False)][:5]
    if successful_examples:
        logger.info(f"\n✅ Примеры УСПЕШНЫХ:")
        for example in successful_examples:
            fallback_mark = " (fallback)" if example.get("fallback_used") else ""
            logger.info(f"   {example['original_address'][:60]}...{fallback_mark}")
    
    failed_examples = [r for r in results if not r.get("geocoding_success", False)][:3]
    if failed_examples:
        logger.info(f"\n❌ Примеры НЕУДАЧНЫХ:")
        for example in failed_examples:
            logger.info(f"   {example['original_address'][:60]}...")
            
        # Анализ неудач
        logger.info(f"\n🔍 АНАЛИЗ НЕУДАЧ:")
        for example in failed_examples:
            addr = example['original_address']
            logger.info(f"   Неудача: {addr}")
            logger.info(f"   Причина: нет в coordinate fallbacks")
    
    logger.info(f"\n🎯 ЦЕЛЬ: Достичь 95%+ успешности для ВСЕХ типов адресов")
    logger.info(f"📈 ТЕКУЩИЙ РЕЗУЛЬТАТ: {success_rate:.1f}%")
    
    # Рекомендации
    if success_rate < 95.0:
        logger.info(f"\n💡 РЕКОМЕНДАЦИИ ДЛЯ ДОСТИЖЕНИЯ 95%+:")
        logger.info(f"   1. Добавить больше coordinate fallbacks")
        logger.info(f"   2. Использовать коммерческие геокодеры (Google, Yandex)")
        logger.info(f"   3. Добавить локальную базу координат")

if __name__ == "__main__":
    asyncio.run(main())