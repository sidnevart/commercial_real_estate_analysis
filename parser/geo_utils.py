"""
Utility functions for geocoding addresses and calculating distances with **2GIS**
==========================================================================

This module replaces the previous OpenRouteService implementation and works
entirely through 2GIS public REST APIs:

* **Geocoder API** – direct geocoding of free‑form addresses.
  Docs: https://docs.2gis.com/en/api/search/geocoder/overview

* **Routing API** – single‑route request that returns `total_distance` and
  `total_duration` (we use it instead of the heavier Distance‑Matrix API).
  Docs: https://docs.2gis.com/en/api/navigation/routing/overview

The public interface (function names + signatures) remains the same, so the
rest of the project does not need to change.
"""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from typing import List, Optional, Tuple

import requests

from core.models import Offer  # your original @dataclass remains untouched

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# 2GIS credentials & endpoints
# ----------------------------------------------------------------------------
DGIS_API_KEY: str = "09b2ea30-9713-4930-b7fe-2213afc9289a"  # TODO: move to .env in prod
GEOCODER_URL = "https://catalog.api.2gis.com/3.0/items/geocode"
ROUTING_URL = "https://routing.api.2gis.com/routing/7.0.0/global"

# ----------------------------------------------------------------------------
# Low‑level helpers
# ----------------------------------------------------------------------------

def _haversine_km(p1: Tuple[float, float], p2: Tuple[float, float]) -> float:
    """Fallback straight‑line distance (WGS‑84 haversine)."""
    lon1, lat1 = p1
    lon2, lat2 = p2
    r = 6371.0088  # mean Earth radius in kilometres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = phi2 - phi1
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))

async def _run_blocking(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

# ----------------------------------------------------------------------------
# Public API – Geocoding & routing via 2GIS
# ----------------------------------------------------------------------------

async def get_coords_by_address(address: str) -> Optional[Tuple[float, float]]:
    """Return ``(lon, lat)`` for a free‑form *address* using 2GIS Geocoder."""
    if not address:
        logger.warning("get_coords_by_address: empty address string")
        return None

    logger.info("2GIS geocoding → '%s'", address)

    def _geocode(text: str) -> Optional[Tuple[float, float]]:
        params = {
            "q": text,
            "fields": "items.point",
            "key": DGIS_API_KEY,
        }
        try:
            resp = requests.get(GEOCODER_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("result", {}).get("items", [])
            if not items:
                return None

            point = items[0].get("point") or items[0].get("geometry", {}).get("centroid")
            # point could be dict {lon, lat} or string "POINT(lon lat)"
            if isinstance(point, dict):
                return float(point["lon"]), float(point["lat"])
            if isinstance(point, str) and point.startswith("POINT("):
                lon_str, lat_str = point[6:-1].split()
                return float(lon_str), float(lat_str)
        except Exception as exc:  # broad, but we log exact details below
            logger.error("2GIS geocoder error – %s", exc)
            return None
        return None

    coords = await _run_blocking(_geocode, address)
    if coords:
        logger.info("Geocoded '%s' → lon=%.6f, lat=%.6f", address, coords[0], coords[1])
    else:
        logger.warning("No geocode result for '%s'", address)
    return coords

async def calculate_distance(
    origin: Tuple[float, float],
    dest: Tuple[float, float],
    mode: str = "walking",  # 'walking' | 'driving' | 'bicycle' etc.
) -> Optional[float]:
    """Return route distance (km) between *origin* and *dest* using 2GIS Routing API.

    Falls back to haversine distance if API call fails.
    """
    logger.info("2GIS routing: %s → %s (mode=%s)", origin, dest, mode)

    def _route(o: Tuple[float, float], d: Tuple[float, float]) -> Optional[float]:
        params = {"key": DGIS_API_KEY}
        body = {
            "points": [
                {"type": "stop", "lon": o[0], "lat": o[1]},
                {"type": "stop", "lon": d[0], "lat": d[1]},
            ],
            "transport": mode,
            "route_mode": "shortest",
        }
        try:
            resp = requests.post(ROUTING_URL, params=params, json=body, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("result", [])
            if not results:
                return None
            dist_m = results[0].get("total_distance")
            if dist_m is None:
                return None
            return dist_m / 1000  # → km
        except Exception as exc:
            logger.error("2GIS routing error – %s", exc)
            return None

    dist = await _run_blocking(_route, origin, dest)
    if dist is None:
        # graceful fallback to straight‑line distance
        dist = _haversine_km(origin, dest)
        logger.warning(
            "Routing failed – falling back to haversine: %.2f km (may be inaccurate)",
            dist,
        )
    else:
        logger.info("2GIS route distance: %.2f km", dist)
    return dist

# ----------------------------------------------------------------------------
# Business logic – filter offers by distance
# ----------------------------------------------------------------------------

async def filter_offers_by_distance(
    lot_address: str,
    offers: List[Offer],
    max_distance_km: float,
    *,
    mode: str = "walking",
) -> List[Offer]:
    """Return only *offers* situated within *max_distance_km* from *lot_address*."""
    logger.info(
        "filter_offers_by_distance: lot='%s', mode=%s, R<=%.1f km, offers=%d",
        lot_address,
        mode,
        max_distance_km,
        len(offers),
    )

    lot_coords = await get_coords_by_address(lot_address)
    if not lot_coords:
        logger.error("Unable to geocode lot address – aborting filter")
        return []

    filtered: List[Offer] = []
    for offer in offers:
        if not offer.address:
            logger.debug("Skip offer %s – no address", offer.id)
            continue

        offer_coords = await get_coords_by_address(offer.address)
        if not offer_coords:
            logger.warning("Skip offer %s – geocoder failed for '%s'", offer.id, offer.address)
            continue

        # quick check for identical coordinates
        if offer_coords == lot_coords:
            dist_km = 0.1
        else:
            dist_km = await calculate_distance(lot_coords, offer_coords, mode=mode)
            if dist_km is None:
                logger.warning("Skip offer %s – distance calc failed", offer.id)
                continue

        if dist_km <= max_distance_km:
            offer.distance_to_lot = dist_km  # type: ignore[attr-defined]
            filtered.append(offer)
            logger.info("Include offer %s – %.2f km", offer.id, dist_km)
        else:
            logger.debug("Exclude offer %s – %.2f km > %.1f km", offer.id, dist_km, max_distance_km)

    logger.info("Done: kept %d of %d offers", len(filtered), len(offers))
    return filtered


# ----------------------------------------------------------------------------
# Ad‑hoc manual test (run:  python geo_utils_2gis.py)
# ----------------------------------------------------------------------------

"""if __name__ == "__main__":
    import uuid

    async def _demo():
        lot = "Пресненская наб., 12, Москва"  # Moscow‑City
        offers = [
            Offer(
                id="1",
                lot_uuid=str(uuid.uuid4()),
                price=0,
                area=0,
                url="",
                type="sale",
                address="Тверская улица, 1, Москва, Россия",
            ),
            Offer(
                id="2",
                lot_uuid=str(uuid.uuid4()),
                price=0,
                area=0,
                url="",
                type="sale",
                address="Сосновая аллея, 1, Зеленоград, Москва, Россия",
            ),
        ]
        res = await filter_offers_by_distance(lot, offers, max_distance_km=10)
        for o in res:
            print(f"{o.id}: {o.address} — {o.distance_to_lot:.2f} km")

    asyncio.run(_demo())
"""