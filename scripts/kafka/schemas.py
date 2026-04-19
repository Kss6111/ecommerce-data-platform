"""
schemas.py
Single source of truth for all Kafka event schemas.
Imported by all three producer scripts.
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional
import uuid


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id() -> str:
    return str(uuid.uuid4())


# ── Orders Stream Schema ──────────────────────────────────────────────────────
@dataclass
class OrderEvent:
    event_id:         str
    event_type:       str        # order_created, order_updated, order_cancelled
    event_timestamp:  str
    order_id:         str
    customer_id:      str
    order_status:     str
    payment_method:   str
    total_amount:     float
    item_count:       int
    shipping_city:    str
    shipping_country: str
    source:           str = "postgres_cdc"

    def to_dict(self) -> dict:
        return asdict(self)


# ── Clickstream Schema ────────────────────────────────────────────────────────
@dataclass
class ClickEvent:
    event_id:        str
    event_type:      str        # page_view, product_view, add_to_cart, checkout, purchase
    event_timestamp: str
    session_id:      str
    user_id:         Optional[str]   # None if anonymous
    page_url:        str
    product_id:      Optional[str]
    category:        Optional[str]
    device_type:     str             # desktop, mobile, tablet
    browser:         str
    referrer:        Optional[str]
    time_on_page_ms: int
    source:          str = "web_frontend"

    def to_dict(self) -> dict:
        return asdict(self)


# ── Inventory Updates Schema ──────────────────────────────────────────────────
@dataclass
class InventoryEvent:
    event_id:          str
    event_type:        str      # restock, sale_deduction, adjustment, low_stock_alert
    event_timestamp:   str
    product_id:        str
    sku:               str
    warehouse_id:      str
    quantity_before:   int
    quantity_change:   int      # positive = restock, negative = deduction
    quantity_after:    int
    reason:            str
    triggered_by:      str      # system, manual, order_fulfillment
    source:            str = "inventory_service"

    def to_dict(self) -> dict:
        return asdict(self)