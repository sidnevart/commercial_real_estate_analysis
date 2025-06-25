"""
Utility functions for geocoding addresses and calculating distances with **OSMnx**
==================================================================================

This module provides an alternative to 2GIS using OSMnx and OpenStreetMap data.
OSMnx is free but may be slower for the first request as it downloads map data.

Requirements:
pip install osmnx geopandas folium

The public interface (function names + signatures) remains the same.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
from typing import List, Optional, Tuple
import warnings

# Suppress geopandas warnings
warnings.filterwarnings("ignore", category=UserWarning, module="geopandas")
warnings.filterwarnings("ignore", category=FutureWarning, module="geopandas")

try:
    import osmnx as ox
    import networkx as nx
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    import geopandas as gpd
    from shapely.geometry import Point
except ImportError as e:
    logging.error("Required packages not installed. Run: pip install osmnx geopandas geopy folium")
    raise e

from core.models import Offer

# ----------------------------------------------------------------------------
# Logging & Configuration
# ----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Configure OSMnx - updated for newer versions
try:
    # For OSMnx >= 1.0
    ox.settings.log_console = False
    ox.settings.use_cache = True
except AttributeError:
    try:
        # For older OSMnx versions
        ox.config(log_console=False, use_cache=True)
    except AttributeError:
        # If config doesn't exist, skip configuration
        logger.warning("Could not configure OSMnx settings")

# Configure Nominatim with better user agent and timeout
geolocator = Nominatim(
    user_agent="commercial_real_estate_parser/1.0 (contact@example.com)", 
    timeout=15
)

# ----------------------------------------------------------------------------
# Caching
# ----------------------------------------------------------------------------
_geocoding_cache = {}
_graph_cache = {}
_distance_cache = {}

# Moscow bounding box for efficient map loading
MOSCOW_BBOX = {
    'north': 56.009,
    'south': 55.142,
    'east': 37.967,
    'west': 37.084
}

# ----------------------------------------------------------------------------
# Address normalization
# ----------------------------------------------------------------------------

def _normalize_address(address: str) -> str:
    """Normalize address for better geocoding results."""
    if not address:
        return address
    
    logger.debug("Original address: '%s'", address)
    
    normalized = address
    
    # Remove complex administrative references that confuse geocoding
    patterns_to_remove = [
        r'вн\.тер\.г\.\s*',  # вн.тер.г.
        r'г\s+Москва\s+вн\.тер\.г\.\s*',  # г Москва вн.тер.г.
        r'муниципальный округ\s+',  # муниципальный округ
        r'помещение\s+\d+[/\d]*\s*,?\s*',  # помещение 9/3, помещение 3/3
        r'офис\s+\d+\s*,?\s*',  # офис 123
        r'комната\s+\d+\s*,?\s*',  # комната 45
        r'каб\.\s*\d+\s*,?\s*',  # каб. 10
        r'кабинет\s+\d+\s*,?\s*',  # кабинет 10
        r'\s+этаж\s+\d+\s*,?\s*',  # этаж 5
        r'\d+\s*этаж\s*,?\s*',  # 5 этаж
    ]
    
    for pattern in patterns_to_remove:
        old_normalized = normalized
        normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)
        if old_normalized != normalized:
            logger.debug("Applied pattern '%s': '%s' -> '%s'", pattern, old_normalized, normalized)
    
    # Clean up extra spaces and commas
    normalized = re.sub(r'\s*,\s*,\s*', ', ', normalized)  # Remove double commas
    normalized = re.sub(r'\s+', ' ', normalized)  # Remove extra spaces
    normalized = normalized.strip().strip(',').strip()  # Remove leading/trailing spaces and commas
    
    # Extract core address components
    if 'муниципальный округ' in address.lower() or 'вн.тер.г' in address.lower():
        parts = normalized.split(',')
        clean_parts = []
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
                
            # Keep city
            if 'москва' in part.lower() or 'московская' in part.lower():
                clean_parts.append(part)
            # Keep district names
            elif any(district in part.lower() for district in [
                'басманный', 'тверской', 'пресненский', 'хамовники', 'арбат',
                'замоскворечье', 'красносельский', 'мещанский', 'таганский', 'якиманка'
            ]):
                clean_parts.append(part)
            # Keep street names
            elif any(street_type in part.lower() for street_type in [
                'улица', 'ул.', 'проспект', 'пр.', 'бульвар', 'б-р', 'переулок', 'пер.',
                'площадь', 'пл.', 'набережная', 'наб.', 'шоссе', 'проезд'
            ]):
                clean_parts.append(part)
            # Keep house numbers
            elif re.search(r'дом\s+\d+', part.lower()) or re.search(r'д\.\s*\d+', part.lower()):
                clean_parts.append(part)
            # Keep standalone numbers that might be house numbers
            elif re.search(r'^\d+[а-я]?$', part.strip()):
                clean_parts.append(part)
        
        if clean_parts:
            normalized = ', '.join(clean_parts)
            logger.debug("Extracted core address parts: '%s'", normalized)
    
    logger.debug("Final normalized address: '%s'", normalized)
    return normalized

# ----------------------------------------------------------------------------
# Helper functions
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
    """Run blocking function in executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

def _is_in_moscow_region(lat: float, lon: float) -> bool:
    """Check if coordinates are within Moscow region."""
    return (MOSCOW_BBOX['south'] <= lat <= MOSCOW_BBOX['north'] and 
            MOSCOW_BBOX['west'] <= lon <= MOSCOW_BBOX['east'])

def _get_moscow_graph(network_type: str = "walk") -> nx.MultiDiGraph:
    """Get or create cached Moscow street network."""
    cache_key = f"moscow_{network_type}"
    
    if cache_key in _graph_cache:
        logger.debug("Using cached Moscow graph for %s", network_type)
        return _graph_cache[cache_key]
    
    logger.info("Downloading Moscow street network for %s (this may take a while on first run)", network_type)
    
    try:
        # Download Moscow street network using bbox
        graph = ox.graph_from_bbox(
            north=MOSCOW_BBOX['north'],
            south=MOSCOW_BBOX['south'], 
            east=MOSCOW_BBOX['east'],
            west=MOSCOW_BBOX['west'],
            network_type=network_type,
            simplify=True
        )
        
        # Cache the graph
        _graph_cache[cache_key] = graph
        logger.info("Successfully downloaded and cached Moscow %s network", network_type)
        return graph
        
    except Exception as e:
        logger.error("Failed to download Moscow network: %s", e)
        raise

# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------

async def get_coords_by_address(address: str) -> Optional[Tuple[float, float]]:
    """Return ``(lon, lat)`` for a free‑form *address* using Nominatim geocoder."""
    if not address:
        logger.warning("get_coords_by_address: empty address string")
        return None

    # Check cache first
    if address in _geocoding_cache:
        logger.debug("Cache hit for address: '%s'", address)
        return _geocoding_cache[address]

    logger.info("Nominatim geocoding → '%s'", address)

    def _geocode(text: str) -> Optional[Tuple[float, float]]:
        try:
            logger.debug("Trying to geocode: '%s'", text)
            
            # Try with Moscow context first
            queries = [
                f"{text}, Moscow, Russia",
                f"{text}, Москва, Россия",
                f"Москва, {text}",
                text
            ]
            
            for i, query in enumerate(queries):
                logger.debug("Geocoding attempt %d with query: '%s'", i+1, query)
                
                try:
                    location = geolocator.geocode(
                        query,
                        exactly_one=True,
                        limit=1,
                        addressdetails=True,
                        timeout=15
                    )
                    
                    if location:
                        coords = (location.longitude, location.latitude)
                        logger.debug("Found coordinates: %s for query: '%s'", coords, query)
                        
                        # Verify coordinates are in Moscow region
                        if _is_in_moscow_region(location.latitude, location.longitude):
                            logger.debug("Coordinates are within Moscow region")
                            return coords
                        else:
                            logger.debug("Coordinates outside Moscow region: %s, trying next query", coords)
                            continue
                    else:
                        logger.debug("No location found for query: '%s'", query)
                        
                except (GeocoderTimedOut, GeocoderServiceError) as e:
                    logger.debug("Geocoding service error for query '%s': %s", query, e)
                    continue
                except Exception as e:
                    logger.debug("Geocoding error for query '%s': %s", query, e)
                    continue
            
            logger.warning("No valid coordinates found for '%s' after all attempts", text)
            return None
            
        except Exception as e:
            logger.error("Geocoding error for '%s': %s", text, e, exc_info=True)
            return None

    # Try original address first
    coords = await _run_blocking(_geocode, address)
    
    # If failed, try normalized address
    if not coords:
        normalized = _normalize_address(address)
        if normalized != address and normalized:
            logger.info("Trying normalized address: '%s'", normalized)
            coords = await _run_blocking(_geocode, normalized)
        
        # If still failed, try progressive simplification
        if not coords:
            logger.info("Trying progressive address simplification")
            
            # Try removing everything after the street and house number
            simplified_patterns = [
                # Keep only: City, Street, House
                r'^([^,]*(?:москва|московская)[^,]*),?\s*.*?([^,]*(?:улица|ул\.|проспект|пр\.|бульвар|б-р|переулок|пер\.|площадь|пл\.|набережная|наб\.)[^,]*),?\s*([^,]*(?:дом|д\.)\s*\d+[^,]*)',
                # Keep only: Street, House
                r'([^,]*(?:улица|ул\.|проспект|пр\.|бульвар|б-р|переулок|пер\.|площадь|пл\.|набережная|наб\.)[^,]*),?\s*([^,]*(?:дом|д\.)\s*\d+[^,]*)',
                # Keep only: Street name
                r'([^,]*(?:улица|ул\.|проспект|пр\.|бульвар|б-р|переулок|пер\.|площадь|пл\.|набережная|наб\.)[^,]*)',
            ]
            
            for pattern in simplified_patterns:
                match = re.search(pattern, address, re.IGNORECASE)
                if match:
                    simplified = ', '.join(match.groups()).strip(', ')
                    if simplified and simplified != address:
                        logger.info("Trying simplified address: '%s'", simplified)
                        coords = await _run_blocking(_geocode, simplified)
                        if coords:
                            break

    # Cache the result (even if None)
    _geocoding_cache[address] = coords
    
    if coords:
        logger.info("Geocoded '%s' → lon=%.6f, lat=%.6f", address, coords[0], coords[1])
    else:
        logger.warning("No geocode result for '%s' after all attempts", address)
    
    return coords

async def calculate_distance(
    origin: Tuple[float, float],
    dest: Tuple[float, float],
    mode: str = "walking",
) -> Optional[float]:
    """Return route distance (km) between *origin* and *dest* using OSMnx routing.

    Falls back to haversine distance if routing fails.
    """
    # Check cache first
    cache_key = f"{origin}_{dest}_{mode}"
    if cache_key in _distance_cache:
        logger.debug("Distance cache hit for %s → %s", origin, dest)
        return _distance_cache[cache_key]

    logger.info("OSMnx routing: %s → %s (mode=%s)", origin, dest, mode)

    def _route(o: Tuple[float, float], d: Tuple[float, float]) -> Optional[float]:
        try:
            # Map mode to OSMnx network type
            network_type_map = {
                "walking": "walk",
                "driving": "drive",
                "bicycle": "bike",
                "bike": "bike",
                "walk": "walk",
                "drive": "drive"
            }
            network_type = network_type_map.get(mode, "walk")
            
            # Get Moscow street network
            G = _get_moscow_graph(network_type)
            
            # Find nearest nodes to origin and destination
            # Updated for newer OSMnx versions
            try:
                orig_node = ox.nearest_nodes(G, o[0], o[1])
                dest_node = ox.nearest_nodes(G, d[0], d[1])
            except AttributeError:
                # Fallback for older versions
                orig_node = ox.distance.nearest_nodes(G, o[0], o[1])
                dest_node = ox.distance.nearest_nodes(G, d[0], d[1])
            
            logger.debug("Found nearest nodes: origin=%s, dest=%s", orig_node, dest_node)
            
            # Calculate shortest path
            try:
                route = nx.shortest_path(G, orig_node, dest_node, weight='length')
                logger.debug("Found route with %d nodes", len(route))
            except nx.NetworkXNoPath:
                logger.warning("No path found between nodes %s and %s", orig_node, dest_node)
                return None
            
            # Calculate route length
            try:
                # Updated for newer OSMnx versions
                route_length = sum(ox.routing.route_to_gdf(G, route)['length'])
            except (AttributeError, KeyError):
                # Fallback for older versions
                try:
                    route_length = sum(ox.utils_graph.get_route_edge_attributes(G, route, 'length'))
                except AttributeError:
                    # Manual calculation as last resort
                    route_length = 0
                    for i in range(len(route) - 1):
                        u, v = route[i], route[i + 1]
                        edge_data = G[u][v]
                        if isinstance(edge_data, dict):
                            edge_data = list(edge_data.values())[0]
                        route_length += edge_data.get('length', 0)
            
            route_length_km = route_length / 1000
            
            logger.debug("Route length: %.0f meters (%.3f km)", route_length, route_length_km)
            return route_length_km
            
        except Exception as e:
            logger.error("OSMnx routing error: %s", e, exc_info=True)
            return None

    # Check if both points are in Moscow region
    if not (_is_in_moscow_region(origin[1], origin[0]) and _is_in_moscow_region(dest[1], dest[0])):
        logger.warning("One or both points outside Moscow region, using haversine distance")
        dist = _haversine_km(origin, dest)
    else:
        dist = await _run_blocking(_route, origin, dest)
        
        if dist is None:
            # Fallback to haversine distance
            dist = _haversine_km(origin, dest)
            logger.warning(
                "Routing failed – falling back to haversine: %.2f km (may be inaccurate)",
                dist,
            )
        else:
            logger.info("OSMnx route distance: %.2f km", dist)
    
    # Cache the result
    _distance_cache[cache_key] = dist
    return dist

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

    logger.info("Lot coordinates: lon=%.6f, lat=%.6f", lot_coords[0], lot_coords[1])

    filtered: List[Offer] = []
    geocoding_failures = 0
    routing_failures = 0
    
    for i, offer in enumerate(offers, 1):
        logger.debug("Processing offer %d/%d: %s", i, len(offers), offer.id)
        
        if not offer.address:
            logger.debug("Skip offer %s – no address", offer.id)
            continue

        offer_coords = await get_coords_by_address(offer.address)
        if not offer_coords:
            logger.warning("Skip offer %s – geocoder failed for '%s'", offer.id, offer.address)
            geocoding_failures += 1
            continue

        logger.debug("Offer %s coordinates: lon=%.6f, lat=%.6f", offer.id, offer_coords[0], offer_coords[1])

        # Quick check for identical coordinates
        if offer_coords == lot_coords:
            dist_km = 0.1
            logger.debug("Offer %s has identical coordinates, using distance: %.1f km", offer.id, dist_km)
        else:
            dist_km = await calculate_distance(lot_coords, offer_coords, mode=mode)
            if dist_km is None:
                logger.warning("Skip offer %s – distance calc failed", offer.id)
                routing_failures += 1
                continue

        if dist_km <= max_distance_km:
            offer.distance_to_lot = dist_km  # type: ignore[attr-defined]
            filtered.append(offer)
            logger.info("Include offer %s – %.2f km", offer.id, dist_km)
        else:
            logger.debug("Exclude offer %s – %.2f km > %.1f km", offer.id, dist_km, max_distance_km)

    logger.info("Filtering complete: kept %d of %d offers (geocoding failures: %d, routing failures: %d)", 
                len(filtered), len(offers), geocoding_failures, routing_failures)
    
    return filtered

# ----------------------------------------------------------------------------
# Cache management
# ----------------------------------------------------------------------------

def clear_cache():
    """Clear all caches."""
    global _geocoding_cache, _distance_cache, _graph_cache
    _geocoding_cache.clear()
    _distance_cache.clear()
    _graph_cache.clear()
    logger.info("Cleared all caches")

def get_cache_stats():
    """Get cache statistics."""
    return {
        "geocoding_cache_size": len(_geocoding_cache),
        "distance_cache_size": len(_distance_cache),
        "graph_cache_size": len(_graph_cache),
        "total_cached_items": len(_geocoding_cache) + len(_distance_cache) + len(_graph_cache)
    }

# ----------------------------------------------------------------------------
# Preload Moscow network (optional)
# ----------------------------------------------------------------------------

def preload_moscow_networks():
    """Preload Moscow street networks for faster subsequent operations."""
    logger.info("Preloading Moscow street networks...")
    try:
        _get_moscow_graph("walk")
        _get_moscow_graph("drive")
        logger.info("Successfully preloaded Moscow networks")
    except Exception as e:
        logger.error("Failed to preload networks: %s", e)

# ----------------------------------------------------------------------------
# Version compatibility check
# ----------------------------------------------------------------------------

def check_osmnx_version():
    """Check OSMnx version and log compatibility info."""
    try:
        import osmnx
        version = osmnx.__version__
        logger.info("OSMnx version: %s", version)
        
        # Check for major version differences
        major_version = int(version.split('.')[0])
        if major_version >= 1:
            logger.info("Using OSMnx >= 1.0 API")
        else:
            logger.info("Using OSMnx < 1.0 API")
            
    except Exception as e:
        logger.warning("Could not determine OSMnx version: %s", e)

# Initialize version check
check_osmnx_version()