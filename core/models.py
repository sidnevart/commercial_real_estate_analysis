from __future__ import annotations
from dataclasses import dataclass, field
from uuid import UUID, uuid4
from datetime import datetime
from typing import Optional, Dict, List

@dataclass(slots=True)
class PropertyClassification:
    category: str = ""  # Street retail, Office, Standalone, Industrial, Commercial land
    size_category: str = ""  # The size bucket like "70-120 m²"
    has_basement: bool = False
    is_top_floor: bool = False

@dataclass(slots=True)
class Lot:
    id: str
    name: str
    address: str
    coords: Optional[tuple[float, float]]
    area: float
    price: float
    notice_number: str
    lot_number: int
    auction_type: str
    sale_type: str
    law_reference: str
    application_start: datetime
    application_end: datetime
    auction_start: datetime
    cadastral_number: dict[str, str] | str
    property_category: str
    ownership_type: str
    auction_step: float
    deposit: float
    recipient: str
    recipient_inn: str
    recipient_kpp: str
    bank_name: str
    bank_bic: str
    bank_account: str
    correspondent_account: str
    auction_url: str
    uuid: UUID = field(default_factory=uuid4)
    district: str = ""  
    median_market_price: float = 0.0  
    profitability: float = 0.0  
    classification: PropertyClassification = field(default_factory=PropertyClassification)
    sale_offers_count: int = 0
    rent_offers_count: int = 0
    
    # Добавляем новые поля для финансовых метрик
    market_price_per_sqm: float = 0.0
    current_price_per_sqm: float = 0.0
    market_value: float = 0.0
    capitalization_rub: float = 0.0
    capitalization_percent: float = 0.0
    monthly_gap: float = 0.0
    annual_yield_percent: float = 0.0
    annual_income: float = 0.0
    average_rent_price_per_sqm: float = 0.0
    has_rent_data: bool = False
    market_value_method: str = "unknown"

    sale_data: List[float] = field(default_factory=list)
    rent_data: List[float] = field(default_factory=list)

@dataclass(slots=True)
class Offer:
    id: str
    lot_uuid: UUID
    price: float
    area: float
    url: str
    type: str
    address: str = ""
    district: str = ""
    distance_to_lot: float = 0.0  