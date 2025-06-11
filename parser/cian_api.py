# parser/cian_minimal.py
"""
Облегчённый CIAN-парсер на базе библиотеки «cianparser».
Задача — вернуть предложения продажи и аренды (офисы / коммерческая земля)
по «поисковому фильтру» вокруг интересующего лота без Selenium.

Автор: GPT-assistant, 2025-06-11
"""

from __future__ import annotations

import uuid
import logging
from pathlib import Path
from typing import List, Tuple

import cianparser                         # pip install cianparser>=1.0.4
from core.models import Offer             # ваша dataclass-структура
                                         #  core/models.py :contentReference[oaicite:3]{index=3}

# --------------------------------------------------------------------------------------
# 1. вспомогательные утилиты
# --------------------------------------------------------------------------------------

PROXY_FILE = 'proxies.txt'  # ожидаем parser/cian_minimal.proxies
LOGGER = logging.getLogger(__name__)


def _load_proxies() -> list[str] | None:
    """
    Прокси читаем из текстового файла формата host:port:user:pass
    (если файла нет — работаем без прокси).
    """
    if not PROXY_FILE.exists():
        return None

    proxies: list[str] = []
    for line in PROXY_FILE.read_text().splitlines():
        if not line.strip():
            continue
        host, port, user, pwd = line.strip().split(":", 3)
        proxies.append(f"http://{user}:{pwd}@{host}:{port}")
    return proxies or None


# --------------------------------------------------------------------------------------
# 2. тонкая обёртка над CianParser
# --------------------------------------------------------------------------------------


class _CianGateway:
    """
    Единый объект-обёртка над `cianparser.CianParser`,
    чтобы не создавать HTTP-сессию каждый раз заново.
    """

    def __init__(self, city: str = "Москва") -> None:
        self._parser = cianparser.CianParser(
            location=city,
            proxies=_load_proxies(),  # Cloudflare обходится за счёт CloudScraper внутри
        )
        self.city = city

    # ---------- публичное API (использует main.py) ----------------------------------

    def fetch_nearby_offers(self, _dummy_filter: str, lot_uuid) -> Tuple[List[Offer], List[Offer]]:
        """
        Возвращает кортеж (sale_offers, rent_offers).

        *Аргумент `search_filter` оставлен ради совместимости со старым кодом,
        но внутри `cianparser` он не нужен — библиотека сама формирует URL.*
        """
        sale = self._query_cian(deal_type="sale", lot_uuid=lot_uuid)
        rent = self._query_cian(deal_type="rent_long", lot_uuid=lot_uuid)
        return sale, rent

    def unformatted_address_to_cian_search_filter(self, address: str) -> str:  # noqa: D401
        """
        Раньше функция возвращала строку «district[0]=…» для прямой подстановки в URL.
        Теперь это не требуется: `cianparser` работает по названию локации.
        Возвращаем адрес «как есть», чтобы сигнатура сохранилась.
        """
        return address

    # ---------- внутренняя реализация ----------------------------------------------

    def _query_cian(self, *, deal_type: str, lot_uuid) -> List[Offer]:
        """
        Делает вызов к `cianparser` и конвертирует результаты в dataclass Offer.
        Для коммерческих объектов в `cianparser` нужен `offer_type="offices"`,
        поэтому задаём его через `additional_settings`.
        """
        raw: list[dict] = self._parser.get_flats(
            deal_type=deal_type,
            rooms="all",
            with_saving_csv=False,
            with_extra_data=False,
            additional_settings={
                "offer_type": "offices",    # ключевое отличие от квартир
                "start_page": 1,
                "end_page": 3,              # можно регулировать из config.yaml
            },
        )  # пример вызова см. официальное README :contentReference[oaicite:1]{index=1}

        def convert(item: dict) -> Offer:
            # Библиотека возвращает словарь с унифицированными ключами
            return Offer(
                id=str(uuid.uuid4()),
                lot_uuid=lot_uuid,
                price=float(item.get("price", 0)),
                area=float(item.get("total_meters") or item.get("area") or 0),
                url=item.get("url", ""),
                type=deal_type,
                address=", ".join(
                    filter(None, (item.get("street"), item.get("house_number"), self.city))
                ),
            )

        offers = [convert(obj) for obj in raw if obj.get("price") and (obj.get("area") or obj.get("total_meters"))]
        LOGGER.info("Parsed %d %s offers via cianparser", len(offers), deal_type)
        return offers


# --------------------------------------------------------------------------------------
# 3. «Singleton» интерфейс для кода проекта
# --------------------------------------------------------------------------------------

_gateway: _CianGateway | None = None


def _get_gateway() -> _CianGateway:
    global _gateway
    if _gateway is None:
        _gateway = _CianGateway()
    return _gateway


# функции, которые уже импортируются в main.py
fetch_nearby_offers = lambda search_filter, lot_uuid: _get_gateway().fetch_nearby_offers(
    search_filter, lot_uuid
)

unformatted_address_to_cian_search_filter = (
    lambda address: _get_gateway().unformatted_address_to_cian_search_filter(address)
)
