# parser/torgi_async.py  ───────────────────────────────────────────────
from __future__ import annotations
import asyncio, json, logging, random
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

BASE = (
    "https://torgi.gov.ru/new/api/public/lotcards/search"
    "?dynSubjRF=78,53&biddType=178FZ,1041PP,229FZ,701PP,ZKPT,KGU"
    "&catCode=7&lotStatus=PUBLISHED,APPLICATIONS_SUBMISSION&ownForm=12,13,14"
    "&withFacets=false&page={}&sort=firstVersionPublicationDate,desc"
)
LOT = "https://torgi.gov.ru/new/api/public/lotcards/{}"
PUBLIC = "https://torgi.gov.ru/new/public/lots/lot/{}"

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
    """Перебираем прокси, в конце — прямой запрос."""
    headers = {"User-Agent": random.choice(UAS)}
    async with aiohttp.ClientSession(
        headers=headers, timeout=aiohttp.ClientTimeout(total=PAGELOAD_TIMEOUT)
    ) as session:

        tried: set[str] = set()
        while True:
            proxy = proxy_get()
            if not proxy or proxy in tried:
                break
            tried.add(proxy)
            try:
                return await _attempt(session, url, proxy)
            except ClientHttpProxyError:
                logger.warning("Proxy %s blocked → drop", proxy)
                proxy_drop(proxy)
            except Exception as e:
                logger.warning("Proxy %s other error: %s → drop", proxy, e)
                proxy_drop(proxy)

        # финальный прямой запрос
        return await _attempt(session, url, None)

# ───────────────────────── core parsing ─────────────────────
def _char(chars: list[dict[str, Any]], code: str, default: Any = "") -> Any:
    return next((c["characteristicValue"] for c in chars if c.get("code") == code), default)

def _to_lot(d: dict[str, Any]) -> Lot:
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

async def _parse_page(idx: int) -> Dict[str, Any]:
    """Возвращает JSON словарь по странице."""
    url = BASE.format(idx)
    data = await _fetch_json(url)
    logger.info("Page %d got %s", idx, type(data))
    return data

async def fetch_lots(max_pages: int = 3) -> List[Lot]:
    lots: List[Lot] = []
    for page in range(max_pages):
        page_json = await _parse_page(page)
        if page_json.get("empty"):
            break
        for item in page_json.get("content", []):
            detail = await _fetch_json(LOT.format(item["id"]))
            lots.append(_to_lot(detail))
    logger.info("Fetched %d lots", len(lots))
    return lots
