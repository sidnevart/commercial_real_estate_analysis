import os
import math
import logging
import httpx
import asyncio
from core.models import Offer  # Предполагается, что Offer - это модель объявления с атрибутами address и distance_to_lot
from typing import Tuple, Optional, List, Dict

# Configure logging
logger = logging.getLogger(__name__)

# Получаем API ключ из переменных окружения или используем тестовый
DGIS_API_KEY = os.getenv("DGIS_API_KEY", "rutnpt3272")

# API эндпоинты
DGIS_GEOCODE_API = "https://catalog.api.2gis.com/3.0/items/geocode"
DGIS_DISTANCE_API = "https://routing.api.2gis.com/get_dist_matrix"


# В parser/geo_utils.py добавляем надежную функцию расчета расстояний

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Рассчитывает расстояние между двумя точками на Земле (в километрах).
    Использует формулу гаверсинуса для сферической тригонометрии.
    """
    # Радиус Земли в километрах
    R = 6371.0
    
    # Конвертируем координаты из градусов в радианы
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Разницы координат
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    # Формула гаверсинуса
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = R * c
    
    return distance

async def get_coords_by_address(address: str) -> Optional[Tuple[float, float]]:
    """
    Получение координат по адресу через API 2GIS.
    Возвращает кортеж (долгота, широта) или None в случае ошибки.
    """
    if not address:
        logger.warning("get_coords_by_address: Пустой адрес")
        return None
    
    # Очищаем адрес от лишних символов и сокращений для улучшения геокодирования
    clean_address = address.strip()
    for old, new in {
        "г.": "город ", "ул.": "улица ", "пр-т": "проспект ", "пр.": "проспект ",
        "б-р": "бульвар ", "пер.": "переулок ", "пл.": "площадь ", "р-н": "район "
    }.items():
        clean_address = clean_address.replace(old, new)
    
    try:
        params = {
            "q": clean_address,
            "fields": "items.point",
            "key": DGIS_API_KEY
        }
        
        logger.info(f"Запрос координат для: '{clean_address}'")
        
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
                        logger.info(f"✅ Получены координаты для '{clean_address}': широта={lat}, долгота={lon}")
                        return (float(lon), float(lat))
                    else:
                        logger.warning(f"⚠️ В ответе нет точки для '{clean_address}': {items[0]}")
                else:
                    logger.warning(f"⚠️ Пустой список items для '{clean_address}'")
            else:
                if "meta" in data:
                    logger.warning(f"⚠️ API вернул код {data['meta'].get('code')} для '{clean_address}'")
                    if data['meta'].get('message'):
                        logger.warning(f"⚠️ Сообщение API: {data['meta'].get('message')}")
                
                # Пытаемся извлечь больше информации об ошибке
                if "message" in data:
                    logger.warning(f"⚠️ Сообщение ошибки: {data['message']}")
            
            logger.warning(f"❌ Не удалось получить координаты для '{clean_address}'")
            return None
            
    except Exception as e:
        logger.error(f"❌ Ошибка при получении координат для '{clean_address}': {str(e)}")
        return None


# Обновляем функцию calculate_distance для лучшей обработки ошибок
async def calculate_distance(source_coords: Tuple[float, float], 
                           target_coords: Tuple[float, float]) -> Optional[float]:
    """
    Расчет расстояния между двумя точками через API 2GIS.
    Возвращает расстояние в километрах или None в случае ошибки.
    """
    try:
        # Убедимся, что координаты являются числами
        # В функции calculate_distance исправьте форматирование координат:
        source_point = f"{source_coords[0]},{source_coords[1]}"  # Было
        source_point = f"{source_coords[0]:.6f},{source_coords[1]:.6f}"  # Стало (с фиксированной точностью)
        target_point = f"{target_coords[0]},{target_coords[1]}"  # Было
        target_point = f"{target_coords[0]:.6f},{target_coords[1]:.6f}"  # Стало
        
        logger.debug(f"Расчет расстояния между точками: {source_point} → {target_point}")
        
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
                logger.debug(f"API вернул расстояние: {distance_km:.2f} км")
                return distance_km
            
            # Подробный вывод ошибки, если есть
            if "message" in data:
                logger.warning(f"⚠️ API вернул ошибку: {data['message']}")
            elif "error" in data:
                logger.warning(f"⚠️ API вернул ошибку: {data['error']}")
            else:
                logger.warning(f"⚠️ Неожиданный формат ответа: {data}")
            
            # Если API не сработал, вернём расстояние по прямой
            distance_km = haversine_distance(source_coords, target_coords)
            logger.info(f"Используем резервный метод расчета расстояния: {distance_km:.2f} км")
            return distance_km
            
    except Exception as e:
        logger.error(f"❌ Ошибка при расчете расстояния: {str(e)}")
        # В случае ошибки возвращаем расстояние по прямой
        try:
            distance = haversine_distance(source_coords, target_coords)
            logger.info(f"Используем формулу гаверсинуса из-за ошибки API: {distance:.2f} км")
            return distance
        except Exception as e2:
            logger.error(f"❌ Также не удалось рассчитать расстояние по гаверсинусу: {str(e2)}")
            return None

async def filter_offers_by_distance(lot_address: str, offers: List[Offer], 
                                  max_distance_km: float) -> List[Offer]:
    """
    Фильтрует список объявлений по расстоянию от лота.
    
    Параметры:
    - lot_address: Адрес лота на торгах
    - offers: Список объявлений
    - max_distance_km: Максимальное расстояние в километрах
    
    Возвращает:
    - Отфильтрованный список объявлений в пределах заданного расстояния
    """
    logger.info(f"🔍 Фильтрация {len(offers)} объявлений в радиусе {max_distance_km} км от {lot_address}")
    
    if not offers:
        logger.warning("📭 Получен пустой список объявлений для фильтрации")
        return []
    
    # Режим отладки - пропускать ошибки геокодирования при очень большом радиусе
    debug_mode = max_distance_km > 1000
    if debug_mode:
        logger.warning(f"⚠️ Включен режим отладки с радиусом {max_distance_km} км")
    
    # Проверяем адрес лота
    if not lot_address:
        logger.error("❌ Не указан адрес лота для фильтрации")
        return offers if debug_mode else []
    
    # Получаем координаты лота
    lot_coords = await get_coords_by_address(lot_address)
    if not lot_coords:
        logger.error(f"❌ Не удалось получить координаты для лота с адресом '{lot_address}'")
        if debug_mode:
            logger.warning("⚠️ Режим отладки: возвращаем все объявления без фильтрации")
            return offers
        return []
    
    logger.info(f"✅ Координаты лота: долгота={lot_coords[0]}, широта={lot_coords[1]}")
    
    # Счетчики для отладки
    no_address_count = 0
    no_coords_count = 0
    rejected_by_distance_count = 0
    accepted_count = 0
    
    filtered_offers = []
    
    # Сохраняем информацию о расстояниях для отладки
    distance_info = []
    
    # Обрабатываем каждое объявление
    for i, offer in enumerate(offers):
        try:
            offer_id = getattr(offer, 'id', f'offer_{i}')
            
            # Если в объявлении нет адреса, пропускаем его
            if not getattr(offer, 'address', None):
                no_address_count += 1
                logger.warning(f"⚠️ Объявление {offer_id} не имеет адреса")
                continue
                
            # Получаем координаты объявления
            offer_address = getattr(offer, 'address', '')
            logger.info(f"Получение координат для объявления {offer_id}: {offer_address}")
            
            offer_coords = await get_coords_by_address(offer_address)
            if not offer_coords:
                no_coords_count += 1
                logger.warning(f"❌ Не удалось получить координаты для объявления {offer_id}")
                continue
            
            logger.info(f"✅ Координаты объявления {offer_id}: долгота={offer_coords[0]}, широта={offer_coords[1]}")
                
            # Сначала используем быструю приблизительную проверку через haversine
            approx_distance = haversine_distance(lot_coords, offer_coords)
            logger.info(f"📏 Приблизительное расстояние до объявления {offer_id}: {approx_distance:.2f} км")
            
            # Сохраняем для отладки
            distance_info.append({
                "offer_id": offer_id,
                "address": offer_address,
                "coords": offer_coords,
                "approx_distance": approx_distance
            })
            
            # Используем запас 50%, так как haversine дает расстояние по прямой, а не по дорогам
            buffer_distance = max_distance_km * 1.5
            if approx_distance <= buffer_distance:
                # Для потенциально близких объявлений делаем точную проверку через API
                exact_distance = await calculate_distance(lot_coords, offer_coords)
                
                if exact_distance is not None:
                    logger.info(f"📏 Точное расстояние до объявления {offer_id}: {exact_distance:.2f} км")
                else:
                    logger.warning(f"⚠️ Не удалось получить точное расстояние для объявления {offer_id}")
                    exact_distance = approx_distance  # Используем приблизительное расстояние как резерв
                
                # Используем точное расстояние, если оно доступно, иначе приблизительное
                distance = exact_distance if exact_distance is not None else approx_distance
                
                if distance <= max_distance_km:
                    # Записываем расстояние в атрибут объявления для отображения в Excel
                    offer.distance_to_lot = distance
                    filtered_offers.append(offer)
                    accepted_count += 1
                    logger.info(f"✅ Объявление {offer_id} (адрес: {offer_address[:50]}...) включено: "
                                f"расстояние {distance:.2f} км от лота (адрес: {lot_address[:50]}...)")
                else:
                    rejected_by_distance_count += 1
                    logger.info(f"❌ Объявление {offer_id} (адрес: {offer_address[:50]}...) отклонено: "
                                f"расстояние {distance:.2f} км > {max_distance_km} км")
            else:
                rejected_by_distance_count += 1
                logger.info(f"✗ Объявление {offer_id} слишком далеко: {approx_distance:.2f} км > {buffer_distance:.2f} км (буфер)")
        
        except Exception as e:
            logger.error(f"❌ Ошибка при фильтрации объявления {getattr(offer, 'id', 'unknown')}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
    logger.info(f"Статистика по расстояниям для лота (адрес: {lot_address}):")
    if distance_info:
        min_dist = min(info['approx_distance'] for info in distance_info)
        max_dist = max(info['approx_distance'] for info in distance_info)
        avg_dist = sum(info['approx_distance'] for info in distance_info) / len(distance_info)
        
        logger.info(f"Минимальное расстояние: {min_dist:.2f} км")
        logger.info(f"Максимальное расстояние: {max_dist:.2f} км")
        logger.info(f"Среднее расстояние: {avg_dist:.2f} км")
    
    # Сводная информация
    logger.info(f"📊 Результаты фильтрации по расстоянию {max_distance_km} км:")
    logger.info(f"  • Всего объявлений: {len(offers)}")
    logger.info(f"  • Без адреса: {no_address_count}")
    logger.info(f"  • Без координат: {no_coords_count}")
    logger.info(f"  • Отклонено по расстоянию: {rejected_by_distance_count}")
    logger.info(f"  • ПРИНЯТО: {accepted_count}")
    
    if not filtered_offers and distance_info:
        # Если ни одно объявление не прошло фильтр, показываем топ-10 ближайших
        distance_info.sort(key=lambda x: x['approx_distance'])
        logger.warning("⚠️ НИ ОДНО ОБЪЯВЛЕНИЕ НЕ ПРОШЛО ФИЛЬТР! 10 ближайших:")
        for i, info in enumerate(distance_info[:10]):
            logger.warning(f"  {i+1}. ID: {info['offer_id']}, "
                         f"Расстояние: {info['approx_distance']:.2f} км, "
                         f"Адрес: {info['address']}")
            
        # Если в режиме отладки и нет подходящих объявлений, вернем первые 5 ближайших
        if debug_mode and distance_info:
            logger.warning("⚠️ Режим отладки: возвращаем 5 ближайших объявлений")
            closest_offers = []
            for i, info in enumerate(distance_info[:5]):
                for offer in offers:
                    if getattr(offer, 'id', '') == info['offer_id']:
                        offer.distance_to_lot = info['approx_distance']
                        closest_offers.append(offer)
                        break
            if closest_offers:
                return closest_offers
    
    return filtered_offers