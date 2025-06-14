from __future__ import annotations
import asyncio
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp
from aiohttp.client_exceptions import ClientHttpProxyError, ContentTypeError

from dateutil.parser import isoparse
from parser.proxy_pool import get as proxy_get, drop as proxy_drop
from parser.config import PAGELOAD_TIMEOUT
from parser.retry import retry
from core.models import Lot

logger = logging.getLogger(__name__)

# URL для поискового API с параметрами пагинации (page={})
BASE = (
    "https://torgi.gov.ru/new/api/public/lotcards/search"
    "?dynSubjRF=78,53&biddType=178FZ,1041PP,229FZ,701PP,ZKPT,KGU"
    "&catCode=7&lotStatus=PUBLISHED,APPLICATIONS_SUBMISSION&ownForm=12,13,14"
    "&withFacets=false&page={}&sort=firstVersionPublicationDate,desc"
)
# URL для получения деталей конкретного лота
LOT = "https://torgi.gov.ru/new/api/public/lotcards/{}"
# URL для публичной страницы лота
PUBLIC = "https://torgi.gov.ru/new/public/lots/lot/{}"

# Список User-Agent для ротации
UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15",
]

async def _attempt(session: aiohttp.ClientSession, url: str, proxy: str | None) -> Dict[str, Any]:
    """Одна попытка запроса. Бросает исключения, если JSON не получился."""
    async with session.get(url, proxy=proxy) as resp:
        if resp.status in (403, 407):
            raise ClientHttpProxyError(req_info=resp.request_info, history=(), code=resp.status, message="proxy block!")
        try:
            data = await resp.json(content_type=None)
        except (ContentTypeError, json.JSONDecodeError) as e:
            raise ValueError(f"Non-JSON response: {e}") from e
        if not isinstance(data, dict):
            raise ValueError("JSON root is not an object")
        return data

@retry()
async def _fetch_json(url: str) -> Dict[str, Any]:
    """Получение JSON с учетом прокси и повторных попыток."""
    headers = {"User-Agent": random.choice(UAS)}
    timeout = aiohttp.ClientTimeout(total=10, connect=5)  # 5 сек на соединение, 10 сек всего
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        # Сначала пробуем через прокси
        tried: set[str] = set()
        while True:
            proxy = proxy_get()
            if not proxy or proxy in tried:
                break
            tried.add(proxy)
            try:
                return await _attempt(session, url, proxy)
            except ClientHttpProxyError:
                logger.warning("Прокси %s заблокирован → удаляем", proxy)
                proxy_drop(proxy)
            except Exception as e:
                logger.warning("Прокси %s ошибка: %s → удаляем", proxy, e)
                proxy_drop(proxy)

        # Если прокси не сработали, пробуем напрямую
        return await _attempt(session, url, None)

def _char(chars: list[dict[str, Any]], code: str, default: Any = "") -> Any:
    """Извлекает значение характеристики по коду из списка характеристик."""
    return next((c["characteristicValue"] for c in chars if c.get("code") == code), default)

def _to_lot(d: dict[str, Any]) -> Lot:
    """Преобразует JSON-данные лота в объект Lot."""
    chars = d.get("characteristics", [])
    area = float(_char(chars, "totalAreaRealty", 0))
    dt = lambda k: isoparse(d[k]).astimezone(timezone.utc) if d.get(k) else datetime.now(timezone.utc)
    return Lot(
        id=str(d["id"]),
        name=d.get("lotName", ""),
        address=d.get("estateAddress", ""),
        coords=None,
        area=area,
        price=d.get("priceMin", 0.0),
        notice_number=d.get("noticeNumber", ""),
        lot_number=d.get("lotNumber", 0),
        auction_type=d.get("biddForm", {}).get("name", ""),
        sale_type=d.get("biddType", {}).get("name", ""),
        law_reference=d.get("npaHintCode", ""),
        application_start=dt("biddStartTime"),
        application_end=dt("biddEndTime"),
        auction_start=dt("auctionStartDate"),
        cadastral_number=_char(chars, "cadastralNumberRealty", ""),
        property_category=d.get("category", {}).get("name", ""),
        ownership_type=d.get("ownershipForm", {}).get("name", ""),
        auction_step=d.get("priceStep", 0.0),
        deposit=d.get("deposit", 0.0),
        recipient=d.get("depositRecipientName", ""),
        recipient_inn=d.get("depositRecipientINN", ""),
        recipient_kpp=d.get("depositRecipientKPP", ""),
        bank_name=d.get("depositBankName", ""),
        bank_bic=d.get("depositBIK", ""),
        bank_account=d.get("depositPayAccount", ""),
        correspondent_account=d.get("depositCorAccount", ""),
        auction_url=PUBLIC.format(d["id"]),
    )

async def fetch_lots(max_pages: int = 10) -> List[Lot]:
    """
    Получает лоты с торгов с поддержкой пагинации.
    
    Args:
        max_pages: Максимальное количество страниц для загрузки (по умолчанию 10)
        
    Returns:
        List[Lot]: Список всех найденных лотов
    """
    logger.info(f"Запуск получения лотов (максимум {max_pages} страниц)")
    
    lots: List[Lot] = []
    current_page = 0
    has_more_pages = True
    total_pages = 0
    
    # Цикл по страницам
    while has_more_pages and current_page < max_pages:
        url = BASE.format(current_page)
        logger.info(f"Запрос страницы {current_page+1}: {url}")
        
        try:
            # Получаем JSON текущей страницы
            page_json = await _fetch_json(url)
            
            # Проверка на пустую страницу
            if page_json.get("empty", True):
                logger.info(f"Страница {current_page+1} пуста, завершаем загрузку")
                break
            
            # Получение информации о пагинации
            has_more_pages = not page_json.get("last", True)
            total_pages = page_json.get("totalPages", 0)
            total_elements = page_json.get("totalElements", 0)
            
            logger.info(f"Информация о пагинации: текущая страница {current_page+1}/{total_pages}, "
                         f"всего элементов: {total_elements}, есть еще страницы: {has_more_pages}")
            
            # Получение массива лотов на текущей странице
            lot_items = page_json.get("content", [])
            logger.info(f"Получено {len(lot_items)} лотов на странице {current_page+1}")
            
            # Обработка каждого лота
            for item in lot_items:
                try:
                    lot_id = item.get("id")
                    if not lot_id:
                        continue
                    
                    # Получаем детальную информацию о лоте
                    detail_url = LOT.format(lot_id)
                    logger.info(f"Получение деталей лота {lot_id}")
                    
                    detail = await _fetch_json(detail_url)
                    if not detail:
                        logger.warning(f"Нет данных для лота {lot_id}")
                        continue
                    valid_torgi_categories = [
                        "нежилые помещения", "иной объект недвижимости", 
                        "право размещения нестационарного объекта", "имущественные комплексы",
                        "единый недвижимый комплекс", "сооружения", "здания", "земельные участки",
                        "комплексное развитие территорий", "земли сельскохозяйственного назначения",
                        "земли населенных пунктов", "земельные участки"
                    ]

                    # После получения данных о лоте, но перед добавлением в список
                    property_category = detail.get("category", {}).get("name", "").lower()
                    if not any(category.lower() in property_category for category in valid_torgi_categories):
                        logger.info(f"Пропуск лота категории '{property_category}' (не соответствует критериям)")
                        continue
                    # Преобразуем JSON в объект Lot
                    lot = _to_lot(detail)
                    lots.append(lot)
                    logger.info(f"Добавлен лот {lot.id}: {lot.name[:30]}...")
                    
                except Exception as e:
                    logger.error(f"Ошибка при обработке лота: {e}")
                    continue
            
            # Переход к следующей странице
            current_page += 1
            
            # Пауза между запросами, чтобы не нагружать сервер
            if has_more_pages and current_page < max_pages:
                pause_time = random.uniform(1.5, 3.0)
                logger.info(f"Пауза {pause_time:.2f} секунд перед следующей страницей")
                await asyncio.sleep(pause_time)
        
        except Exception as e:
            logger.error(f"Ошибка при загрузке страницы {current_page+1}: {e}")
            # Делаем паузу перед повторной попыткой или переходом к следующей странице
            await asyncio.sleep(5)
            
            # Увеличиваем счетчик, чтобы избежать зацикливания на проблемной странице
            current_page += 1
    
    logger.info(f"Всего загружено {len(lots)} лотов со {current_page} страниц (из {total_pages} доступных)")
    return lots