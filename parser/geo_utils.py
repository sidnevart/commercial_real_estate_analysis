# Файл: geo_utils.py

import asyncio
import logging
from typing import Optional, Tuple, List
from dataclasses import dataclass
from core.models import Offer

import openrouteservice
from openrouteservice import Client

# — Offer dataclass

# — Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# — OpenRouteService client
API_KEY = '5b3ce3597851110001cf6248fefa491989844016a5e595d066ef61d8'
ors_client = Client(key=API_KEY)

# — Async helper to run sync code in executor
async def run_sync(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

# — Geocoding via OpenRouteService Pelias
async def get_coords_by_address(address: str) -> Optional[Tuple[float, float]]:
    
    # Возвращает (lon, lat) для заданного адреса через ORS Pelias.
    
    if not address:
        logger.warning("get_coords_by_address: пустой адрес")
        return None

    logger.info(f"Geocoding address via ORS: '{address}'")
    try:
        def pelias_search(text):
            res = ors_client.pelias_search(text=text, size=1, layers=['address', 'street', 'locality'])
            feat = res["features"][0]
            coords = feat["geometry"]["coordinates"]
            return coords  # [lon, lat]
        coords = await run_sync(pelias_search, address)
        logger.info(f"Geocoded '{address}' → lon={coords[0]:.6f}, lat={coords[1]:.6f}")
        return tuple(coords)
    except Exception as e:
        logger.error(f"Geocoding error for '{address}': {e}")
        return None



# — Distance calculation via ORS Matrix
async def calculate_distance(origin: Tuple[float, float], dest: Tuple[float, float]) -> Optional[float]:
    
    #Возвращает расстояние в километрах между двумя точками через ORS distance_matrix.

    logger.info(f"Calculating distance via ORS matrix: {origin} → {dest}")
    try:
        def ors_matrix(o, d):
            matrix = ors_client.distance_matrix(
                locations=[o, d],
                profile='foot-walking',  # Можно изменить на 'driving-car' или другой профиль
                metrics=["distance"],
                units="m"
            )
            return matrix["distances"][0][1] / 1000
        dist = await run_sync(ors_matrix, origin, dest)
        logger.info(f"ORS matrix distance: {dist:.2f} km")
        return dist
    except Exception as e:
        logger.error(f"Distance matrix error: {e}")
        return None


# — Фильтрация объявлений по расстоянию
async def filter_offers_by_distance(
    lot_address: str,
    offers: List[Offer],
    max_distance_km: float
) -> List[Offer]:
    
    #Геокодирует lot_address и адреса offers через ORS,
    #считает расстояние matrix и оставляет только те, что <= max_distance_km.
    
    logger.info(f"filter_offers_by_distance: lot_address='{lot_address}', max_distance={max_distance_km} km")

    lot_coords = await get_coords_by_address(lot_address)
    if not lot_coords:
        logger.error(f"Cannot geocode lot address: '{lot_address}'")
        return []

    filtered: List[Offer] = []
    for o in offers:
        if not o.address:
            logger.debug(f"Skipping offer {o.id}: no address")
            continue

        offer_coords = await get_coords_by_address(o.address)
        if not offer_coords:
            logger.warning(f"Skipping offer {o.id}: failed to geocode '{o.address}'")
            continue

        dist = await calculate_distance(lot_coords, offer_coords)
        if dist is None:
            logger.warning(f"Skipping offer {o.id}: failed to calculate distance")
            continue

        if dist <= max_distance_km:
            o.distance_to_lot = dist
            filtered.append(o)
            logger.info(f"Including offer {o.id}: distance={dist:.2f} km")
        else:
            logger.debug(f"Excluding offer {o.id}: distance={dist:.2f} km > {max_distance_km} km")

    logger.info(f"filter_offers_by_distance: {len(filtered)} of {len(offers)} offers within {max_distance_km} km")
    return filtered


"""
# — Тестовый блок
if __name__ == "__main__":
    import uuid

    async def _test():
        lot = "Деловой центр, Москва-Сити «Город Столиц» 123122, Пресненская наб., 12, Москва, 101000"
        offers = [
            Offer(
                id="1",
                lot_uuid=str(uuid.uuid4()),
                price=0,
                area=0,
                url="",
                type="sale",
                address="Сосновая аллея, 1, Зеленоград, Москва, Россия"
            ),
            Offer(
                id="2",
                lot_uuid=str(uuid.uuid4()),
                price=0,
                area=0,
                url="",
                type="sale",
                address="Тверская улица, 1, Москва, Россия"
            ),
        ]
        res = await filter_offers_by_distance(lot, offers, max_distance_km=50)
        for o in res:
            print(f"{o.id}: {o.address} — {o.distance_to_lot:.2f} km")

    asyncio.run(_test())
"""