"""
Сервис поиска аналогов недвижимости
"""
import logging
from typing import List, Optional
from core.models import Lot, Offer

logger = logging.getLogger(__name__)

class AnalogSearchService:
    @staticmethod
    async def find_analogs_for_address(address: str, radius_km: float = 3.0) -> List[Offer]:
        """
        Поиск аналогов для указанного адреса
        
        Args:
            address: Адрес для поиска
            radius_km: Радиус поиска в километрах
            
        Returns:
            Список найденных аналогов
        """
        logger.info(f"Searching analogs for address: {address}")
        
        try:
            # Импортируем функции поиска из существующей системы
            from parser.cian_minimal import fetch_nearby_offers, unformatted_address_to_cian_search_filter
            
            # Преобразуем адрес в поисковый фильтр
            search_filter = unformatted_address_to_cian_search_filter(address)
            
            # Поиск предложений через CIAN API
            sale_offers, rent_offers = fetch_nearby_offers(search_filter, "temp_uuid")
            
            # Объединяем все предложения
            offers = sale_offers + rent_offers
            
            if not offers:
                logger.info(f"No offers found for address: {address}")
                return []
            
            # Фильтрация по расстоянию (если функция доступна)
            try:
                from parser.geo_utils import filter_offers_by_distance
                filtered_offers = await filter_offers_by_distance(address, offers, radius_km)
            except ImportError:
                logger.warning("geo_utils not available, using all offers")
                filtered_offers = offers
            
            # Сортировка по релевантности
            sorted_offers = AnalogSearchService._sort_offers_by_relevance(filtered_offers)
            
            logger.info(f"Found {len(sorted_offers)} analogs for {address}")
            return sorted_offers
            
        except Exception as e:
            logger.error(f"Error searching analogs for {address}: {e}")
            return []
    
    @staticmethod
    async def find_analogs_for_lot(lot: Lot, radius_km: float = 3.0) -> List[Offer]:
        """
        Поиск аналогов для конкретного лота
        
        Args:
            lot: Лот для которого ищем аналоги
            radius_km: Радиус поиска в километрах
            
        Returns:
            Список найденных аналогов
        """
        return await AnalogSearchService.find_analogs_for_address(lot.address, radius_km)
    
    @staticmethod
    async def find_analogs_for_lot_uuid(lot_uuid: str, radius_km: float = 3.0) -> List[Offer]:
        """
        Поиск аналогов для лота по его UUID в Google Sheets
        
        Args:
            lot_uuid: UUID лота для поиска аналогов
            radius_km: Радиус поиска в километрах (для fallback)
            
        Returns:
            Список найденных аналогов
        """
        logger.info(f"Searching analogs for lot UUID: {lot_uuid}")
        
        try:
            # Сначала пытаемся найти аналоги в Google Sheets
            from parser.google_sheets import find_analogs_in_sheets, find_lot_by_uuid
            
            # Ищем аналоги в листах cian_sale_all и cian_rent_all
            analogs = find_analogs_in_sheets(lot_uuid, radius_km)
            
            if analogs:
                logger.info(f"Found {len(analogs)} analogs in Google Sheets for lot {lot_uuid}")
                return AnalogSearchService._sort_offers_by_relevance(analogs)
            
            # Fallback: если в Google Sheets ничего не найдено, ищем по адресу лота
            logger.info(f"No analogs found in Google Sheets for {lot_uuid}, trying fallback search")
            
            lot = find_lot_by_uuid(lot_uuid)
            if lot and lot.address:
                logger.info(f"Found lot with address: {lot.address}, searching online")
                return await AnalogSearchService.find_analogs_for_address(lot.address, radius_km)
            else:
                logger.warning(f"Could not find lot with UUID {lot_uuid} or lot has no address")
                return []
            
        except Exception as e:
            logger.error(f"Error searching analogs for lot UUID {lot_uuid}: {e}")
            return []
    
    @staticmethod
    def _sort_offers_by_relevance(offers: List[Offer]) -> List[Offer]:
        """
        Сортировка предложений по релевантности
        
        Критерии сортировки:
        1. Расстояние (ближе = лучше)
        2. Тип предложения (сначала продажа, потом аренда)
        3. Площадь (предпочтение средним размерам)
        """
        def relevance_score(offer: Offer) -> float:
            score = 0.0
            
            # Бонус за близость
            if hasattr(offer, 'distance_to_lot') and offer.distance_to_lot:
                # Чем ближе, тем выше балл (максимум 50 баллов)
                distance_score = max(0, 50 - (offer.distance_to_lot * 10))
                score += distance_score
            
            # Бонус за тип предложения
            if hasattr(offer, 'type'):
                if offer.type == 'sale':
                    score += 30  # Продажа важнее для анализа
                elif offer.type == 'rent':
                    score += 20
            
            # Бонус за разумную площадь (не слишком маленькую и не слишком большую)
            if offer.area:
                if 50 <= offer.area <= 1000:  # Оптимальный диапазон
                    score += 20
                elif 20 <= offer.area <= 2000:  # Приемлемый диапазон
                    score += 10
            
            return score
        
        # Сортируем по убыванию релевантности
        return sorted(offers, key=relevance_score, reverse=True)
    
    @staticmethod
    def filter_offers_by_criteria(offers: List[Offer], 
                                 min_area: Optional[float] = None,
                                 max_area: Optional[float] = None,
                                 offer_type: Optional[str] = None) -> List[Offer]:
        """
        Фильтрация предложений по дополнительным критериям
        
        Args:
            offers: Список предложений
            min_area: Минимальная площадь
            max_area: Максимальная площадь  
            offer_type: Тип предложения ('sale' или 'rent')
            
        Returns:
            Отфильтрованный список предложений
        """
        filtered = offers.copy()
        
        if min_area is not None:
            filtered = [o for o in filtered if o.area >= min_area]
        
        if max_area is not None:
            filtered = [o for o in filtered if o.area <= max_area]
        
        if offer_type is not None:
            filtered = [o for o in filtered if getattr(o, 'type', 'sale') == offer_type]
        
        return filtered
