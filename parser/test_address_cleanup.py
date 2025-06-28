"""
Тестовый скрипт для исправления проблем с адресами:
1. Удаление дубликатов в адресах
2. Сокращение адресов до оптимального формата
3. Исправление ошибки языка в геокодерах
4. Каскадное геокодирование с постепенным упрощением
"""

import re
import logging
from typing import List, Optional, Tuple
from core.gpt_tunnel_client import sync_chat

# Тестируем без geopy сначала, если нужно - подключим
try:
    from geopy.geocoders import Nominatim, Photon
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderQuotaExceeded
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def remove_redundant_admin(address: str) -> str:
    """
    Убирает избыточные административные обозначения
    ИСПРАВЛЕНО: г.о. + г Город → г Город
    """
    result = address
    
    # Паттерны для очистки
    cleanup_patterns = [
        # "г.о. Клин, г Клин" → "г Клин"
        (r'г\.о\.\s+([А-Яа-яё\-]+),?\s*г\s+\1', r'г \1'),
        # "городской округ Клин, г Клин" → "г Клин"
        (r'городской округ\s+([А-Яа-яё\-]+),?\s*г\s+\1', r'г \1'),
        # Убираем вн.тер.г.
        (r'вн\.тер\.г\.[^,]*,?\s*', ''),
        # Убираем лишние административные единицы
        (r'муниципальный округ[^,]*,?\s*', ''),
        (r'административный округ[^,]*,?\s*', ''),
        # Российская Федерация в начале
        (r'^Российская Федерация,?\s*', ''),
    ]
    
    for pattern, replacement in cleanup_patterns:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    
    # Очищаем лишние запятые и пробелы
    result = re.sub(r'\s*,\s*', ', ', result)
    result = re.sub(r',+', ',', result)
    result = result.strip().strip(',').strip()
    
    return result

def clean_duplicate_address_parts(address: str) -> str:
    """
    Очищает дубликаты в адресе агрессивно
    """
    if not address:
        return address
    
    logger.info(f"🧹 ОЧИСТКА: '{address}'")
    
    # 1. Убираем полные дубликаты областей/регионов
    # "Московская область ... Московская область" → "Московская область"
    patterns_to_deduplicate = [
        # Полные совпадения
        (r'Московская область[^,]*,\s*Московская область', 'Московская область'),
        (r'обл Московская[^,]*,\s*Московская область', 'Московская область'),
        (r'Российская Федерация[^,]*,\s*Московская область', 'Московская область'),
        
        # Города
        (r'г\s+([А-Яа-яё\-]+)[^,]*,\s*\1(?:\s+г|$)', r'г \1'),
        (r'город\s+([А-Яа-яё\-]+)[^,]*,\s*\1', r'город \1'),
        
        # Районы
        (r'([А-Яа-яё\-]+)\s+р-н[^,]*,\s*\1', r'\1'),
        (r'р-н\s+([А-Яа-яё\-]+)[^,]*,\s*\1', r'\1'),
        
        # Села/поселки
        (r'с\s+([А-Яа-яё\s\-]+)[^,]*,\s*с\s+\1', r'с \1'),
        (r'село\s+([А-Яа-яё\s\-]+)[^,]*,\s*\1', r'село \1'),
        (r'пгт\s+([А-Яа-яё\s\-]+)[^,]*,\s*\1', r'пгт \1'),
    ]
    
    cleaned = address
    for pattern, replacement in patterns_to_deduplicate:
        old_cleaned = cleaned
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
        if cleaned != old_cleaned:
            logger.info(f"   🔄 Убрал дубликат: '{old_cleaned}' → '{cleaned}'")
    
    # 2. Убираем избыточные части
    redundant_patterns = [
        r'Российская Федерация,?\s*',
        r'вн\.тер\.г\.[^,]*,?\s*',
        r'муниципальный округ[^,]*,?\s*',
        r'административный округ[^,]*,?\s*',
        r'городской округ(?!\s+[А-Яа-яё])[^,]*,?\s*',  # Не трогаем "городской округ Название"
    ]
    
    for pattern in redundant_patterns:
        old_cleaned = cleaned
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        if cleaned != old_cleaned:
            logger.info(f"   ✂️ Убрал избыточное: '{pattern}'")
    
    # 3. Приводим к стандартному формату
    cleaned = standardize_address_format(cleaned)
    
    # 4. Убираем лишние пробелы и запятые
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = re.sub(r'\s*,\s*', ', ', cleaned)
    cleaned = re.sub(r',+', ',', cleaned)
    cleaned = cleaned.strip().strip(',').strip()
    
    logger.info(f"✅ РЕЗУЛЬТАТ: '{cleaned}'")
    return cleaned

def standardize_address_format(address: str) -> str:
    """
    Приводит адрес к стандартному формату: Область, Город, Объект/Улица
    """
    # Стандартизируем сокращения
    replacements = [
        (r'\bобл\.?\s*', 'область '),
        (r'\bг\.о\.?\s*', 'городской округ '),
        (r'\bг\.?\s*(?![А-Яа-яё])', ''),  # Убираем "г." если после него не название
        (r'\bм\.о\.?\s*', ''),
        (r'\bр-н\.?\s*', 'район '),
        (r'\bпгт\.?\s*', 'пгт '),
        (r'\bс\.?\s+', 'село '),
        (r'\bд\.?\s*(\d)', r'дом \1'),
    ]
    
    result = address
    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    
    return result

def simplify_address_for_geocoding(address: str) -> str:
    """
    Упрощает адрес до минимально необходимого для геокодирования:
    - Область + Город + Улица/Объект + Дом
    - Убирает все лишнее
    """
    logger.info(f"🎯 УПРОЩЕНИЕ: '{address}'")
    
    # Сначала очищаем дубликаты
    cleaned = clean_duplicate_address_parts(address)
    
    # Извлекаем ключевые компоненты
    components = {
        'region': '',
        'city': '',
        'street_object': '',
        'house': ''
    }
    
    # Регион
    region_patterns = [
        r'(Московская область)',
        r'(московская область)',
        r'область (Московская)',
    ]
    
    for pattern in region_patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            components['region'] = 'Московская область'
            break
    
    # Город/населенный пункт
    city_patterns = [
        r'городской округ\s+([А-Яа-яё\-]+)',
        r'г\s+([А-Яа-яё\-]+)',
        r'город\s+([А-Яа-яё\-]+)',
        r'пгт\s+([А-Яа-яё\s\-]+?)(?:,|$)',
        r'село\s+([А-Яа-яё\s\-]+?)(?:,|$)',
        r'с\s+([А-Яа-яё\s\-]+?)(?:,|$)',
    ]
    
    for pattern in city_patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            components['city'] = match.group(1).strip()
            break
    
    # Улица или специальный объект
    street_object_patterns = [
        r'ул\.?\s+([А-Яа-яё\s\-]+?)(?:,|\s+д|\s*$)',
        r'улица\s+([А-Яа-яё\s\-]+?)(?:,|\s+д|\s*$)',
        r'(Логистический Центр[^,]*)',
        r'([А-Яа-яё\s]+(?:Центр|центр)[^,]*)',
        r'тер\.?\s+([А-Яа-яё\s\-]+?)(?:,|\s*$)',
        r'территория\s+([А-Яа-яё\s\-]+?)(?:,|\s*$)',
    ]
    
    for pattern in street_object_patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            components['street_object'] = match.group(1).strip()
            break
    
    # Номер дома
    house_patterns = [
        r'дом\s+(\d+[А-Яа-яёM]*)',
        r'д\.?\s+(\d+[А-Яа-яёM]*)',
        r',\s*(\d+[А-Яа-яёM]*)\s*$',
    ]
    
    for pattern in house_patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            components['house'] = match.group(1)
            break
    
    # Собираем упрощенный адрес
    simplified_parts = []
    
    if components['region']:
        simplified_parts.append(components['region'])
    
    if components['city']:
        simplified_parts.append(components['city'])
    
    if components['street_object']:
        simplified_parts.append(components['street_object'])
        
    if components['house']:
        simplified_parts.append(components['house'])
    
    simplified = ', '.join(simplified_parts)
    
    logger.info(f"✅ УПРОЩЕН: '{simplified}'")
    return simplified

def parse_address_components(address: str) -> dict:
    """
    Парсит компоненты адреса для создания вариантов поиска
    """
    components = {
        'region': '',
        'main_city': '',  # Основной город (Люберцы, Подольск)
        'city': '',       # Любой населенный пункт
        'district': '',   # Район
        'street': '',
        'house': '',
        'specific_objects': []
    }
    
    # Регион
    region_match = re.search(r'(Московская область)', address, re.IGNORECASE)
    if region_match:
        components['region'] = region_match.group(1)
    
    # Основные города Московской области
    major_cities = ['Люберцы', 'Подольск', 'Химки', 'Балашиха', 'Мытищи', 'Коломна', 'Электросталь', 'Одинцово', 'Серпухов', 'Шаховская']
    for city in major_cities:
        if city.lower() in address.lower():
            components['main_city'] = city
            components['city'] = city
            break
    
    # Поселки и села
    settlement_patterns = [
        r'пгт\s+([А-Яа-яё\s\-]+?)(?:,|$)',
        r'село\s+([А-Яа-яё\s\-]+?)(?:,|$)',
        r'с\s+([А-Яа-яё\s\-]+?)(?:,|$)',
        r'городской округ\s+([А-Яа-яё\-]+)',
    ]
    
    for pattern in settlement_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            settlement = match.group(1).strip()
            if not components['city']:
                components['city'] = settlement
            break
    
    # Районы
    district_patterns = [
        r'([А-Яа-яё\-]+)\s+район',
        r'район\s+([А-Яа-яё\-]+)',
    ]
    
    for pattern in district_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            components['district'] = match.group(1).strip()
            break
    
    # Улицы
    street_patterns = [
        r'ул\.?\s+([А-Яа-яё\s\-]+?)(?:,|\s+д|\s*$)',
        r'улица\s+([А-Яа-яё\s\-]+?)(?:,|\s+д|\s*$)',
    ]
    
    for pattern in street_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            components['street'] = match.group(1).strip()
            break
    
    # Специфические объекты
    specific_objects = [
        'Логистический Центр',
        'Торговый Центр',
        'Бизнес Центр',
        'Промышленная зона',
        'территория',
        'тер.',
    ]
    
    for obj in specific_objects:
        if obj.lower() in address.lower():
            components['specific_objects'].append(obj)
    
    # Номер дома
    house_patterns = [
        r'дом\s+(\d+[А-Яа-яёM]*)',
        r'д\.?\s+(\d+[А-Яа-яёM]*)',
        r',\s*(\d+[А-Яа-яёM]*)\s*$',
    ]
    
    for pattern in house_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            components['house'] = match.group(1)
            break
    
    return components

def remove_specific_objects(address: str) -> str:
    """
    Убирает специфические объекты из адреса
    """
    objects_to_remove = [
        r'Логистический Центр[^,]*',
        r'Торговый Центр[^,]*',
        r'Бизнес[- ]?Центр[^,]*',
        r'Промышленная зона[^,]*',
        r'тер\.?\s+[^,]*',
        r'территория\s+[^,]*',
        r'промзона[^,]*',
    ]
    
    result = address
    for pattern in objects_to_remove:
        result = re.sub(pattern, '', result, flags=re.IGNORECASE)
    
    # Очищаем лишние запятые
    result = re.sub(r'\s*,\s*,\s*', ', ', result)
    result = re.sub(r'^,\s*|,\s*$', '', result)
    result = result.strip()
    
    return result

def create_address_variations(address: str) -> List[str]:
    """
    Создает варианты адреса от самого точного до самого общего для каскадного поиска
    ИСПРАВЛЕНО: Улучшенная логика без бессмысленного удаления номеров домов
    """
    variations = []
    
    # Парсим компоненты адреса
    components = parse_address_components(address)
    
    # 1. Полный адрес (как есть)
    variations.append(address)
    
    # 2. Очищенный от дубликатов адрес
    cleaned = clean_duplicate_address_parts(address)
    if cleaned != address:
        variations.append(cleaned)
    
    # 3. Убираем избыточные административные единицы (г.о., вн.тер.г.)
    simplified_admin = remove_redundant_admin(address)
    if simplified_admin != address:
        variations.append(simplified_admin)
    
    # 4. Без специфических объектов (Логистический Центр, тер.)
    without_objects = remove_specific_objects(address)
    if without_objects != address:
        variations.append(without_objects)
    
    # 5. Стандартный формат: Область, Город, Улица, Дом
    if components['region'] and components['city'] and components['street']:
        standard_format = f"{components['region']}, {components['city']}, {components['street']}"
        if components['house']:
            standard_format += f", {components['house']}"
        variations.append(standard_format)
    
    # 6. Только регион + город + улица (БЕЗ номера дома)
    if components['region'] and components['city'] and components['street']:
        no_house_format = f"{components['region']}, {components['city']}, {components['street']}"
        variations.append(no_house_format)
    
    # 7. Только город + улица
    if components['city'] and components['street']:
        city_street = f"{components['city']}, {components['street']}"
        variations.append(city_street)
    
    # 8. Только улица (если она информативная)
    if components['street'] and len(components['street']) > 8:
        variations.append(components['street'])
    
    # 9. Только регион + город
    if components['region'] and components['city']:
        region_city = f"{components['region']}, {components['city']}"
        variations.append(region_city)
    
    # 10. Только город
    if components['city']:
        variations.append(components['city'])
    
    # 11. Английские варианты для лучшего геокодирования
    english_variants = create_english_variants(address)
    variations.extend(english_variants)
    
    # Убираем дубликаты, сохраняя порядок
    unique_variations = []
    for var in variations:
        cleaned_var = var.strip().strip(',').strip()
        if cleaned_var and cleaned_var not in unique_variations and len(cleaned_var) > 3:
            unique_variations.append(cleaned_var)
    
    return unique_variations
def create_english_variants(address: str) -> List[str]:
    """
    Создает английские варианты адреса для лучшего геокодирования
    """
    variants = []
    
    # Словарь переводов
    translations = {
        'московская область': 'Moscow Oblast',
        'область': 'Oblast',
        'клин': 'Klin',
        'химки': 'Khimki',
        'подольск': 'Podolsk',
        'москва': 'Moscow',
        'россия': 'Russia',
        'улица': 'street',
        'ул': 'street',
        'проспект': 'avenue',
        'пр-т': 'avenue',
        'дом': 'house',
        'д': 'house',
        'гагарина': 'Gagarina',
        'правды': 'Pravdy',
        'тверская': 'Tverskaya',
        'новогиреевская': 'Novogireevskaya',
    }
    
    # Создаем английский вариант
    english_address = address.lower()
    for ru, en in translations.items():
        english_address = english_address.replace(ru, en)
    
    if english_address != address.lower():
        # Убираем лишние слова
        english_clean = re.sub(r'\b(oblast|street|house)\b', '', english_address)
        english_clean = re.sub(r'\s+', ' ', english_clean).strip()
        
        variants.extend([
            english_address.title(),
            english_clean.title(),
            f"Russia, {english_clean.title()}",
        ])
    
    return variants
    
def gpt_clean_address(address: str) -> str:
    """
    Использует GPT для очистки адреса
    """
    if not address or len(address) < 10:
        return address
    
    prompt = f"""
Очисти российский адрес от дубликатов и лишней информации:

ИСХОДНЫЙ АДРЕС: "{address}"

ПРОБЛЕМЫ:
1. Дубликаты: "Московская область ... Московская область"
2. Избыточность: "Российская Федерация", "муниципальный округ"
3. Повторы городов и районов

ЗАДАЧА:
Оставь ТОЛЬКО: Область, Город/Населенный пункт, Улица/Объект, Дом

ПРАВИЛА:
1. НЕ дублируй информацию
2. Убери "Российская Федерация"
3. Убери административные округа
4. Стандартизируй: г.→город, ул.→улица
5. Сохрани специальные объекты: "Логистический Центр"

ПРИМЕРЫ:
"обл Московская, м.о. Шаховская, с Белая Колпь Российская Федерация, Московская область, Шаховской р-н, с Белая Колпь, д 71"
→ "Московская область, Шаховской район, село Белая Колпь, дом 71"

"Московская область, Люберцы городской округ, Томилино пгт, Логистический Центр тер., 7М"
→ "Московская область, Люберцы, Томилино, Логистический Центр, 7М"

Верни ТОЛЬКО очищенный адрес без объяснений.
"""
    
    try:
        response = sync_chat(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты эксперт по очистке российских адресов от дубликатов и избыточной информации."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150
        )
        
        cleaned = response.strip().strip('"').strip("'")
        
        if len(cleaned) > 5 and len(cleaned) <= len(address):
            logger.info(f"🤖 GPT очистил: '{address}' → '{cleaned}'")
            return cleaned
        else:
            logger.warning(f"🤖 GPT вернул подозрительный результат: '{cleaned}'")
            return address
            
    except Exception as e:
        logger.error(f"🤖 Ошибка GPT: {e}")
        return address

def create_fixed_geocoder():
    """
    Создает исправленный геокодер без проблемы языка
    """
    if not GEOPY_AVAILABLE:
        return None
    
    # ИСПРАВЛЕННАЯ конфигурация геокодера
    geocoder = Nominatim(
        user_agent="commercial_real_estate_fixed/1.0",  # Русский только в user_agent
        timeout=30,
        domain='nominatim.openstreetmap.org'
        # НЕ ИСПОЛЬЗУЕМ language='ru' !!!
    )
    
    return geocoder

def enhanced_address_search(geocoder, address: str, max_attempts: int = 6) -> Optional[tuple]:
    """
    Улучшенный поиск адреса с каскадной стратегией
    
    Returns:
        tuple: (latitude, longitude, found_address, used_variation) или None
    """
    if not address:
        return None
    
    logger.info(f"🔍 ENHANCED SEARCH: '{address}'")
    
    # Создаем варианты поиска
    variations = create_address_variations(address)[:max_attempts]
    
    for i, variation in enumerate(variations, 1):
        logger.info(f"   Попытка {i}/{len(variations)}: '{variation}'")
        
        try:
            location = geocoder.geocode(
                query=variation,
                exactly_one=True,
                timeout=20,
                addressdetails=True,
                limit=1
            )
            
            if location:
                logger.info(f"   ✅ НАЙДЕНО! Использован вариант: '{variation}'")
                return (location.latitude, location.longitude, location.address, variation)
            else:
                logger.info(f"   ❌ Вариант {i} не найден")
                
        except Exception as e:
            logger.warning(f"   ❌ Ошибка в варианте {i}: {e}")
            continue
    
    logger.warning(f"❌ ALL ENHANCED ATTEMPTS failed: '{address}'")
    return None

def test_address_cleaning():
    """
    Тестирует очистку проблемных адресов
    """
    test_addresses = [
        # Проблемные из логов
        "обл Московская, м.о. Шаховская, с Белая Колпь Российская Федерация, Московская область, Шаховской р-н, с Белая Колпь, д 71",
        "Московская область, Люберцы городской округ, Томилино пгт, Логистический Центр тер., 7М",
        
        # Дополнительные тесты
        "обл Московская, г.о. Подольск, г Подольск, ул Правды, дом 20 Московская область, г. Подольск, ул. Правды, д. 20, пом. 1",
        "Российская Федерация, город Москва, Центральный административный округ, муниципальный округ Басманный, улица Покровка, дом 42",
        "г Москва вн.тер.г. Москва муниципальный округ Хамовники ул Остоженка дом 53/2 строение 1",
        
        # Простые для проверки
        "Московская область, г Химки, ул Загородная, дом 4",
        "г Москва, ул Тверская, дом 7",
    ]
    
    print("🧪 ТЕСТИРОВАНИЕ ОЧИСТКИ АДРЕСОВ")
    print("=" * 100)
    
    for i, address in enumerate(test_addresses, 1):
        print(f"\n📍 ТЕСТ {i}")
        print(f"Исходный: {address}")
        
        # Тестируем три метода
        regex_cleaned = clean_duplicate_address_parts(address)
        simplified = simplify_address_for_geocoding(address)
        gpt_cleaned = gpt_clean_address(address)
        
        print(f"REGEX:    {regex_cleaned}")
        print(f"УПРОЩЕН:  {simplified}")
        print(f"GPT:      {gpt_cleaned}")
        print("-" * 100)

def test_address_variations():
    """
    Тестирует создание вариантов адресов
    """
    print("🔀 ТЕСТИРОВАНИЕ СОЗДАНИЯ ВАРИАНТОВ АДРЕСОВ")
    print("=" * 80)
    
    test_addresses = [
        "Московская область, Шаховской район, село Белая Колпь, дом 71",
        "Московская область, Люберцы, Томилино, Логистический Центр, 7М",
        "Московская область, Подольск, улица Правды, дом 20",
    ]
    
    for address in test_addresses:
        print(f"\n📍 ИСХОДНЫЙ АДРЕС: {address}")
        print("-" * 60)
        
        variations = create_address_variations(address)
        
        for i, variation in enumerate(variations, 1):
            print(f"   {i}. {variation}")
        
        print("-" * 60)

def fix_geocoder_language_error():
    """
    Тестирует и исправляет ошибку языка в геокодерах
    """
    if not GEOPY_AVAILABLE:
        print("⚠️ geopy не доступен для тестирования")
        return
    
    print("🌍 ТЕСТИРОВАНИЕ ГЕОКОДЕРОВ")
    print("=" * 50)
    
    test_address = "Москва, улица Тверская, 10"
    
    # Тестируем разные настройки языка
    language_configs = [
        ("default", {}),
        ("en", {"language": "en"}),
        ("de", {"language": "de"}),
        ("fr", {"language": "fr"}),
        ("ru_removed", {}),  # Без языка вообще
    ]
    
    for lang_name, lang_params in language_configs:
        print(f"\n🔍 Тестируем конфигурацию: {lang_name}")
        
        try:
            # Nominatim
            geocoder = Nominatim(
                user_agent="test_address_cleanup/1.0",
                timeout=10
            )
            
            geocode_params = {
                'query': test_address,
                'exactly_one': True,
                'timeout': 10
            }
            geocode_params.update(lang_params)
            
            print(f"   Параметры: {geocode_params}")
            
            location = geocoder.geocode(**geocode_params)
            
            if location:
                print(f"   ✅ УСПЕХ: {location.latitude:.6f}, {location.longitude:.6f}")
                print(f"   📍 Адрес: {location.address}")
            else:
                print(f"   ❌ Не найдено")
                
        except Exception as e:
            print(f"   ❌ ОШИБКА: {e}")
    
    print("\n💡 РЕКОМЕНДАЦИИ:")
    print("1. Не используйте language='ru' в параметрах геокодера")
    print("2. Используйте language='en' или уберите параметр language")
    print("3. Если нужен русский - используйте только в user_agent")

def test_cascading_geocoding():
    """
    Тестирует каскадное геокодирование с постепенным упрощением адреса
    """
    if not GEOPY_AVAILABLE:
        print("⚠️ geopy не доступен")
        return
    
    print("🎯 ТЕСТИРОВАНИЕ КАСКАДНОГО ГЕОКОДИРОВАНИЯ")
    print("=" * 80)
    
    geocoder = create_fixed_geocoder()
    
    test_addresses = [
        "Московская область, Шаховской район, село Белая Колпь, дом 71",
        "Московская область, Люберцы, Томилино, Логистический Центр, 7М",
        "Московская область, Подольск, улица Правды, дом 20",
        "Московская область, Химки, улица Загородная, дом 4",
    ]
    
    for address in test_addresses:
        print(f"\n📍 КАСКАДНЫЙ ПОИСК: {address}")
        print("-" * 80)
        
        # Создаем варианты адреса
        variations = create_address_variations(address)
        
        found_location = None
        successful_variation = None
        
        for i, variation in enumerate(variations, 1):
            print(f"   {i}. Пробуем: '{variation}'")
            
            try:
                location = geocoder.geocode(
                    query=variation,
                    exactly_one=True,
                    timeout=15,
                    addressdetails=True,
                    limit=1
                )
                
                if location:
                    print(f"      ✅ НАЙДЕНО! {location.latitude:.6f}, {location.longitude:.6f}")
                    print(f"      📍 Адрес: {location.address}")
                    found_location = location
                    successful_variation = variation
                    break
                else:
                    print(f"      ❌ Не найдено")
                    
            except Exception as e:
                print(f"      ❌ ОШИБКА: {e}")
        
        if found_location:
            print(f"\n   🎉 ИТОГ: Найден по варианту '{successful_variation}'")
            print(f"   🌍 Координаты: {found_location.latitude:.6f}, {found_location.longitude:.6f}")
        else:
            print(f"\n   😞 ИТОГ: Ни один вариант не найден")
        
        print("=" * 80)

def test_fixed_geocoding():
    """
    Тестирует исправленное геокодирование с каскадным поиском
    """
    if not GEOPY_AVAILABLE:
        print("⚠️ geopy не доступен")
        return
    
    print("🎯 ТЕСТИРОВАНИЕ ИСПРАВЛЕННОГО ГЕОКОДИРОВАНИЯ")
    print("=" * 60)
    
    geocoder = create_fixed_geocoder()
    
    test_addresses = [
        "Москва, улица Тверская, 10",
        "Московская область, Подольск, улица Правды, 20",
        "Московская область, Шаховской район, село Белая Колпь, дом 71",
        "Московская область, Люберцы, Томилино, Логистический Центр",
    ]
    
    for address in test_addresses:
        print(f"\n📍 Тестируем: {address}")
        
        # Сначала пробуем обычный поиск
        try:
            location = geocoder.geocode(
                query=address,
                exactly_one=True,
                timeout=20,
                addressdetails=True,
                limit=1
            )
            
            if location:
                print(f"   ✅ ПРЯМОЙ ПОИСК: {location.latitude:.6f}, {location.longitude:.6f}")
                print(f"   📍 Найден: {location.address}")
                continue
        except Exception as e:
            print(f"   ❌ Ошибка прямого поиска: {e}")
        
        # Если прямой поиск не сработал, используем улучшенный
        print(f"   🔄 Переходим к каскадному поиску...")
        result = enhanced_address_search(geocoder, address)
        
        if result:
            lat, lon, found_address, used_variation = result
            print(f"   ✅ КАСКАДНЫЙ ПОИСК: {lat:.6f}, {lon:.6f}")
            print(f"   🎯 Использован вариант: '{used_variation}'")
            print(f"   📍 Найден: {found_address}")
        else:
            print(f"   ❌ Каскадный поиск тоже не дал результата")

if __name__ == "__main__":
    print("🧪 ТЕСТОВЫЙ СКРИПТ ИСПРАВЛЕНИЯ АДРЕСОВ")
    print("=" * 60)
    
    # 1. Тестируем очистку адресов
    print("\n1️⃣ ТЕСТИРОВАНИЕ ОЧИСТКИ АДРЕСОВ")
    test_address_cleaning()
    
    # 2. Тестируем создание вариантов
    print("\n2️⃣ ТЕСТИРОВАНИЕ ВАРИАНТОВ АДРЕСОВ")
    test_address_variations()
    
    # 3. Тестируем исправление ошибки языка
    print("\n3️⃣ ИСПРАВЛЕНИЕ ОШИБКИ ЯЗЫКА ГЕОКОДЕРА")
    fix_geocoder_language_error()
    
    # 4. Тестируем каскадное геокодирование
    print("\n4️⃣ ТЕСТИРОВАНИЕ КАСКАДНОГО ГЕОКОДИРОВАНИЯ")
    test_cascading_geocoding()
    
    # 5. Тестируем исправленное геокодирование
    print("\n5️⃣ ТЕСТИРОВАНИЕ ИСПРАВЛЕННОГО ГЕОКОДИРОВАНИЯ")
    test_fixed_geocoding()
    
    print("\n✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("\n💡 РЕКОМЕНДАЦИИ:")
    print("1. Используйте функцию enhanced_address_search() для поиска координат")
    print("2. НЕ используйте language='ru' в параметрах геокодеров")
    print("3. Каскадный поиск поможет найти даже сложные адреса")
    print("4. Функция автоматически упрощает адрес при неудачных попытках")
    print("5. Используйте simplify_address_for_geocoding() для предварительной очистки")