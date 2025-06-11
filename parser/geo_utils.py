import os
import math
import logging
import httpx
import asyncio
from typing import Tuple, Optional, List, Dict

# Configure logging
logger = logging.getLogger(__name__)

# Получаем API ключ из переменных окружения или используем тестовый
DGIS_API_KEY = os.getenv("DGIS_API_KEY", "rutnpt3272")

# API эндпоинты
DGIS_GEOCODE_API = "https://catalog.api.2gis.com/3.0/items/geocode"
DGIS_DISTANCE_API = "https://routing.api.2gis.com/get_dist_matrix"

async def get_coords_by_address(address: str) -> Optional[Tuple[float, float]]:
    """
    Получение координат по адресу через API 2GIS.
    Возвращает кортеж (долгота, широта) или None в случае ошибки.
    """
    try:
        params = {
            "q": address,
            "fields": "items.point",
            "key": DGIS_API_KEY
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(DGIS_GEOCODE_API, params=params, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            
            if data.get("meta", {}).get("code") == 200 and data.get("result", {}).get("total") > 0:
                items = data.get("result", {}).get("items", [])
                if items:
                    point = items[0].get("point")
                    if point:
                        lon, lat = point.get("lon"), point.get("lat")
                        logger.info(f"Получены координаты для адреса '{address}': {lat}, {lon}")
                        return (lon, lat)
            
            logger.warning(f"Не удалось получить координаты для адреса '{address}'")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при получении координат для адреса '{address}': {e}")
        return None

async def calculate_distance(source_coords: Tuple[float, float], 
                           target_coords: Tuple[float, float]) -> Optional[float]:
    """
    Расчет расстояния между двумя точками через API 2GIS.
    Возвращает расстояние в километрах или None в случае ошибки.
    """
    try:
        # Формат координат для API: [lon, lat]
        source_point = f"{source_coords[0]},{source_coords[1]}"
        target_point = f"{target_coords[0]},{target_coords[1]}"
        
        params = {
            "key": DGIS_API_KEY,
            "sources": source_point,
            "targets": target_point,
            "type": "distance",
            "transport_type": "car"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(DGIS_DISTANCE_API, params=params, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "OK" and data.get("rows"):
                # Расстояние в метрах, переводим в километры
                distance_meters = data["rows"][0]["elements"][0]["distance"]["value"]
                distance_km = distance_meters / 1000
                return distance_km
            
            logger.warning(f"Не удалось получить расстояние между точками")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при расчете расстояния: {e}")
        return None

def haversine_distance(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    """
    Расчет прямого расстояния между двумя точками без использования API.
    Использует формулу гаверсинусов для вычисления расстояния по дуге большого круга.
    Это быстрый резервный метод, но он не учитывает дороги.
    
    coord1, coord2: Кортежи (долгота, широта)
    Возвращает расстояние в километрах.
    """
    # Радиус Земли в км
    R = 6371.0
    
    # Переводим координаты из градусов в радианы
    lon1, lat1 = math.radians(coord1[0]), math.radians(coord1[1])
    lon2, lat2 = math.radians(coord2[0]), math.radians(coord2[1])
    
    # Разница координат
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    
    # Формула гаверсинуса
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = R * c
    
    return distance

async def filter_offers_by_distance(lot_address: str, offers: List[Dict], 
                                  max_distance_km: float) -> List[Dict]:
    """
    Фильтрует список объявлений по расстоянию от лота.
    
    Параметры:
    - lot_address: Адрес лота на торгах
    - offers: Список объявлений
    - max_distance_km: Максимальное расстояние в километрах
    
    Возвращает:
    - Отфильтрованный список объявлений в пределах заданного расстояния
    """
    if not offers:
        return []
    
    # Получаем координаты лота
    lot_coords = await get_coords_by_address(lot_address)
    if not lot_coords:
        logger.warning(f"Не удалось получить координаты для лота с адресом '{lot_address}'. Все объявления будут включены.")
        return offers
    
    filtered_offers = []
    
    # Обрабатываем каждое объявление
    for offer in offers:
        try:
            # Если в объявлении нет адреса, пропускаем его
            if not offer.address:
                continue
                
            # Получаем координаты объявления
            offer_coords = await get_coords_by_address(offer.address)
            if not offer_coords:
                continue
                
            # Сначала используем быструю приблизительную проверку через haversine
            approx_distance = haversine_distance(lot_coords, offer_coords)
            
            if approx_distance <= max_distance_km * 1.5:  # Используем запас 50%
                # Для потенциально близких объявлений делаем точную проверку через API
                exact_distance = await calculate_distance(lot_coords, offer_coords)
                
                # Если API вернул ошибку, используем приблизительное расстояние
                distance = exact_distance if exact_distance is not None else approx_distance
                
                if distance <= max_distance_km:
                    # Сохраняем расстояние в объявлении для дальнейшего использования
                    offer.distance_to_lot = distance
                    filtered_offers.append(offer)
                    logger.debug(f"Объявление с ID {offer.id} находится в {distance:.2f} км от лота")
            
        except Exception as e:
            logger.error(f"Ошибка при фильтрации объявления: {e}")
    
    logger.info(f"Отфильтровано {len(filtered_offers)} из {len(offers)} объявлений по расстоянию {max_distance_km} км")
    return filtered_offers