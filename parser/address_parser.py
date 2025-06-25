import json
import logging
import re
from typing import Dict, Optional

from core.gpt_tunnel_client import sync_chat

logger = logging.getLogger(__name__)

def extract_address_components_gpt_sync(address: str) -> Dict:
    """
    Extracts structured components from an address using GPT.
    Returns a dictionary with address components.
    """
    if not address or len(address) < 5:
        logger.warning(f"Empty or too short address: '{address}'")
        return {
            "district": "",
            "street": "",
            "settlement": "",
            "city": "",
            "region": "",
            "confidence": 0
        }
    
    prompt = f"""
    Проанализируй следующий адрес и извлеки из него все компоненты в формате JSON:
    "{address}"
    
    Верни строго JSON-объект со следующими полями (пустые, если не удалось найти):
    * district: название района
    * street: название улицы
    * settlement: населенный пункт (город, поселок, деревня и т.д.)
    * city: город
    * region: регион/область
    * confidence: уровень уверенности от 0 до 1
    
    Формат ответа:
    ```json
    {{
      "district": "название района",
      "street": "название улицы",
      "settlement": "населенный пункт",
      "city": "город",
      "region": "регион/область",
      "confidence": 0.95
    }}
    ```
    """
    
    try:
        response = sync_chat(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты - эксперт по анализу и структурированию адресов в России. Извлекай компоненты точно и возвращай только JSON."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500
        )
        
        # Extract JSON from response
        json_pattern = r'```(?:json)?(.*?)```'
        match = re.search(json_pattern, response, re.DOTALL)
        if match:
            json_str = match.group(1).strip()
        else:
            # If no code block, try parsing the whole response
            json_str = response
            
        result = json.loads(json_str)
        logger.info(f"Successfully parsed address components for: '{address}'")
        return result
        
    except Exception as e:
        logger.error(f"Error extracting address components with GPT: {e}")
        return {
            "district": "",
            "street": "",
            "settlement": "",
            "city": "",
            "region": "",
            "confidence": 0
        }


def extract_address_components_regex(address: str) -> Dict:
    """
    Fallback function to extract address components using regex patterns.
    """
    address_lower = address.lower()
    
    components = {
        "district": "",
        "street": "",
        "settlement": "",
        "city": "",
        "region": "",
        "confidence": 0.0
    }
    
    # Region extraction
    if "москва" in address_lower:
        components["region"] = "Москва"
        components["city"] = "Москва"
    elif "московская область" in address_lower or "мо," in address_lower or "мо " in address_lower:
        components["region"] = "Московская область"
    
    # City extraction (for Moscow Oblast)
    city_patterns = [
        r'г\.?\s*([А-Яа-я\-]+)',
        r'город\s+([А-Яа-я\-]+)',
        r'г\.о\.\s+([А-Яа-я\-]+)',
        r'городской округ\s+([А-Яа-я\-]+)'
    ]
    
    for pattern in city_patterns:
        match = re.search(pattern, address)
        if match:
            city = match.group(1).capitalize()
            if city.lower() != 'москва':
                components["city"] = city
                components["settlement"] = city
                break
    
    # Street extraction with improved pattern for набережная
    street_patterns = [
        r'ул\.?\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'улица\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'шоссе\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'проспект\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'пр-т\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'пр\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'бульвар\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'б-р\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'переулок\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'пер\.\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'набережная\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',  # Add pattern for "набережная"
        r'наб\.\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',       # Add pattern for abbreviated "наб."
        r'([А-Яа-я\-\s]+?(?:улица|набережная|проспект|бульвар|переулок))(?:,|\s+дом|\s+д\.|\s+\d|$)'  # Pattern for street name + type
    ]
    
    for pattern in street_patterns:
        match = re.search(pattern, address)
        if match:
            components["street"] = match.group(1).strip()
            break
    
    # If standard patterns didn't work, try a more general approach
    if not components["street"]:
        # Look for known street types anywhere in the address
        street_types = ["улица", "проспект", "бульвар", "набережная", "шоссе", "переулок", "проезд", "площадь"]
        for street_type in street_types:
            if street_type in address_lower:
                # Find words around the street type
                parts = address.split(",")
                for part in parts:
                    if street_type in part.lower():
                        # Clean up and store as street
                        clean_street = part.strip()
                        if len(clean_street) > 4:  # Minimum sensible length
                            components["street"] = clean_street
                            break
                if components["street"]:
                    break
    
    # District extraction (for Moscow)
    # Administrative districts
    admin_districts = {
        'цао': 'Центральный',
        'сао': 'Северный', 
        'свао': 'Северо-Восточный',
        'вао': 'Восточный',
        'ювао': 'Юго-Восточный',
        'юао': 'Южный',
        'юзао': 'Юго-Западный', 
        'зао': 'Западный',
        'сзао': 'Северо-Западный',
        'зелао': 'Зеленоградский',
        'тинао': 'Троицкий и Новомосковский'
    }
    
    for short, full in admin_districts.items():
        if short in address_lower or full.lower() in address_lower:
            components["district"] = f"{full} АО"
            break
    
    # Municipal districts
    district_patterns = [
        r'район\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
        r'р-н\s+([А-Яа-я\-\s]+?)(?:,|\d|$)'
    ]
    
    for pattern in district_patterns:
        match = re.search(pattern, address)
        if match:
            components["district"] = match.group(1).strip()
            break
    
    # Calculate confidence based on number of fields filled
    filled_count = sum(1 for v in components.values() if v)
    components["confidence"] = min(0.8, filled_count / 5.0)  # Max 0.8 since it's regex-based
    
    return components


def calculate_address_components(address: str) -> Dict:
    """
    Determines address components using multiple methods.
    Falls back to regex-based extraction if GPT fails.
    """
    # Try GPT-based extraction first
    components = extract_address_components_gpt_sync(address)
    
    # If GPT extraction worked with good confidence, return results
    if components.get("confidence", 0) >= 0.7:
        return components
        
    # Otherwise, try regex-based extraction for known patterns
    regex_components = extract_address_components_regex(address)
    
    # Merge results, preferring GPT for components where it had some confidence
    merged = {}
    for key in ["district", "street", "settlement", "city", "region"]:
        gpt_value = components.get(key, "")
        regex_value = regex_components.get(key, "")
        
        # Use GPT value if it exists and is not empty
        if gpt_value and len(gpt_value) > 1:
            merged[key] = gpt_value
        # Otherwise use regex value if it exists
        elif regex_value:
            merged[key] = regex_value
        # Default to empty string
        else:
            merged[key] = ""
    
    # Calculate confidence based on how many fields were populated
    filled_count = sum(1 for v in merged.values() if v)
    merged["confidence"] = min(1.0, filled_count / 4.0)  # Max 1.0
    
    # If we have a street but no district, increase confidence slightly
    if merged["street"] and not merged["district"] and merged["confidence"] < 0.7:
        merged["confidence"] = min(0.7, merged["confidence"] + 0.2)
    
    return merged


def is_moscow_address(address: str) -> bool:
    """
    Determine if an address is in Moscow.
    """
    components = calculate_address_components(address)
    
    is_moscow = (
        components["city"].lower() == "москва" or
        components["region"].lower() == "москва" or 
        "москва" in address.lower() or
        components["district"].endswith("АО")  # Administrative districts of Moscow
    )
    
    return is_moscow


def is_moscow_oblast_address(address: str) -> bool:
    """
    Determine if an address is in Moscow Oblast.
    """
    components = calculate_address_components(address)
    
    is_mo = (
        "московская область" in components["region"].lower() or
        "мо" in components["region"].lower() or
        "московская область" in address.lower() or
        "мо," in address.lower()
    )
    
    return is_mo


# Test function
if __name__ == "__main__":
    test_addresses = [
        "г Москва, Пресненская набережная, дом 12",
        "Московская область, городской округ Химки, город Химки, квартал Международный, ул. Загородная, дом 4",
        "Москва, СВАО, район Останкинский, проспект Мира, 119с536",
        "г Москва, ул Тверская, дом 7",
        "обл Московская, г Домодедово, мкр Северный, ул Советская, дом 50",
        "Зеленоград, корпус 847"
    ]
    
    print("Testing address parsing:")
    for address in test_addresses:
        components = calculate_address_components(address)
        print(f"\n{address}")
        print(f"  Region: {components['region']}")
        print(f"  City: {components['city']}")
        print(f"  District: {components['district']}")
        print(f"  Settlement: {components['settlement']}")
        print(f"  Street: {components['street']}")
        print(f"  Confidence: {components['confidence']:.2f}")
        print(f"  Is Moscow: {is_moscow_address(address)}")
        print(f"  Is MO: {is_moscow_oblast_address(address)}")