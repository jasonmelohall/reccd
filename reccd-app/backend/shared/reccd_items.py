#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilities shared with the backend that originated from the standalone
`items/reccd_items.py` module. Keeping a local copy ensures the backend
deploy contains the helpers it needs without modifying the original files.
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import sqlalchemy

logger = logging.getLogger(__name__)

# Keepa Time: minutes since this instant. listedSince 0 / -1 => unknown, not a real listing date.
KEEPA_TIME_BASE = datetime(2011, 1, 1)
KEEPA_SENTINEL_END = datetime(2011, 1, 2)


def _to_naive_utc(dt: datetime) -> datetime:
    """Normalize aware datetimes to naive UTC for consistent comparisons/storage."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def keepa_minutes_to_datetime(minutes: Any) -> Optional[datetime]:
    """Convert Keepa minute offset to datetime; NULL for unknown (<= 0)."""
    if minutes is None:
        return None
    try:
        m = int(minutes)
    except (TypeError, ValueError):
        return None
    if m <= 0:
        return None
    return KEEPA_TIME_BASE + timedelta(minutes=m)


def sanitize_product_datetime(dt: Any) -> Optional[datetime]:
    """Drop missing values and Keepa epoch placeholder dates (2011-01-01)."""
    if dt is None:
        return None
    try:
        if isinstance(dt, datetime):
            candidate = dt
        elif hasattr(dt, "to_pydatetime"):
            candidate = dt.to_pydatetime()
        else:
            candidate = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    candidate = _to_naive_utc(candidate)
    if candidate < KEEPA_SENTINEL_END:
        return None
    return candidate


def earliest_valid_release_date(*dates: Any) -> Optional[datetime]:
    valid: List[datetime] = []
    for d in dates:
        cleaned = sanitize_product_datetime(d)
        if cleaned is not None:
            valid.append(cleaned)
    if not valid:
        return None
    return min(valid)


def apply_valid_release_dates(df):
    """
    For ranking: null out Keepa sentinel dates, then release_date = min of valid columns.
    Requires pandas (used by 9_reccd_items / recommendation_service).
    """
    import pandas as pd

    date_cols = [c for c in ("release_date", "listed_date", "oldest_review") if c in df.columns]
    sentinel = pd.Timestamp(KEEPA_SENTINEL_END)
    for col in date_cols:
        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        if getattr(parsed.dtype, "tz", None) is not None:
            parsed = parsed.dt.tz_convert("UTC").dt.tz_localize(None)
        df[col] = parsed
        df.loc[df[col].notna() & (df[col] < sentinel), col] = pd.NaT
    if date_cols:
        df["release_date"] = df[date_cols].min(axis=1)
    return df


def get_search_term() -> List[str]:
    """Return the default Amazon search terms."""
    list_amazon_search_terms = [
       'white bathroom trash can with lid modern',
       'matte white step trash can small with lid',
       'champagne gold small bathroom trash can with lid',
       'brushed gold small trash can with lid bathroom',
       'cream bathroom trash can with lid minimal',
    ]
    return list_amazon_search_terms


def mysqlengine():
    """Return the shared SQLAlchemy engine using the RECCD_DB_URL env var."""
    db_url = os.getenv("RECCD_DB_URL")
    if not db_url:
        raise RuntimeError(
            "Missing environment variable RECCD_DB_URL. "
            "Set it to your MySQL connection URL."
        )
    engine = sqlalchemy.create_engine(db_url)
    return engine


# ===== Parent ASIN Utility Functions =====

def get_parent_asin_from_keepa(product_data: Dict) -> Optional[str]:
    """
    Extract parent ASIN from Keepa API product response.

    Args:
        product_data: Product dict from Keepa API response

    Returns:
        Parent ASIN if available, otherwise None.
    """
    return product_data.get("parentAsin")


def get_parent_asin_from_rainforest(item_data: Dict) -> Optional[str]:
    """
    Extract parent ASIN from Rainforest API search result or product response.

    Rainforest returns parent_asin in the search results when a product has variations.

    Args:
        item_data: Item dict from Rainforest API search results or product data

    Returns:
        Parent ASIN if available, otherwise None.
    """
    parent = item_data.get("parent_asin")

    if not parent:
        parent = item_data.get("product", {}).get("parent_asin")

    return parent


def fetch_parent_product_rainforest(parent_asin: str, api_key: str) -> Optional[Dict]:
    """
    Fetch full parent product details from Rainforest API.

    Args:
        parent_asin: The parent ASIN to fetch
        api_key: Rainforest API key

    Returns:
        Product data for parent ASIN, or None if error.
    """
    url = "https://api.rainforestapi.com/request"
    params = {
        "api_key": api_key,
        "type": "product",
        "amazon_domain": "amazon.com",
        "asin": parent_asin,
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            return response.json().get("product", {})
        logger.error("Error fetching parent %s: %s", parent_asin, response.status_code)
        return None
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Exception fetching parent %s: %s", parent_asin, exc)
        return None


# ===== Item count / price per item =====
# count_type: each | pound | ounce | kilogram | gram
# item_count: quantity in that unit (e.g. 7 + pound -> 7 lb bag, price_per_item = $/lb)

_COUNT_PATTERNS: Sequence[Tuple[str, re.Pattern]] = (
    ("pack_of_n", re.compile(r"\bpack\s+of\s+(\d{1,4})\b", re.I)),
    ("n_pack", re.compile(r"\b(\d{1,3})\s*[-]?\s*pack\b", re.I)),
    ("n_pcs", re.compile(r"\b(\d{1,4})\s*pcs\b", re.I)),
    ("n_pk", re.compile(r"\b(\d{1,3})\s*pk\b", re.I)),
    ("n_count", re.compile(r"\b(\d{1,4})\s*[-]?\s*count\b", re.I)),
    ("n_ct_word", re.compile(r"\b(\d{1,4})\s*ct\b", re.I)),
    ("n_pieces", re.compile(r"\b(\d{1,4})\s*[-]?\s*pieces?\b", re.I)),
    ("n_units", re.compile(r"\b(\d{1,4})\s*units?\b", re.I)),
    ("paren_n_pack", re.compile(r"\(\s*(\d{1,3})\s*[-]?\s*pack\s*\)", re.I)),
)

_WEIGHT_PATTERNS: Sequence[Tuple[str, re.Pattern, str]] = (
    (
        "frac_lb",
        re.compile(
            r"\b(\d+)\s*/\s*(\d+)\s*(?:lb|lbs|pound|pounds)\b",
            re.I,
        ),
        "pound",
    ),
    (
        "n_lb",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:lb|lbs|pound|pounds)\b",
            re.I,
        ),
        "pound",
    ),
    (
        "n_oz",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:oz|ounce|ounces)\b",
            re.I,
        ),
        "ounce",
    ),
    (
        "n_kg",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:kg|kilogram|kilograms|kilo)\b",
            re.I,
        ),
        "kilogram",
    ),
    (
        "n_g",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:g|gram|grams)\b",
            re.I,
        ),
        "gram",
    ),
)

_MIN_EACH_COUNT = 2
_MAX_EACH_COUNT = 2000
_MIN_WEIGHT_QTY = 0.01
_MAX_WEIGHT_QTY = 500.0


def _parse_weight_quantity(match: re.Match, pattern_name: str) -> Optional[float]:
    if pattern_name == "frac_lb":
        num, den = int(match.group(1)), int(match.group(2))
        if den <= 0:
            return None
        return round(num / den, 4)
    try:
        return round(float(match.group(1)), 4)
    except (TypeError, ValueError):
        return None


def _is_seed_bulk_each_count(title: str, n: int, pattern_name: str) -> bool:
    """Skip multipack each-count when the number is almost certainly seeds/pcs in title."""
    t = title.lower()
    if n >= 100 and re.search(r"\b(seeds?|seed\s+count)\b", t):
        return True
    if n >= 50 and pattern_name in ("n_pcs", "n_count", "n_pieces", "n_units", "n_ct_word"):
        return True
    if n >= 500:
        return True
    return False


def infer_quantity_from_title(
    title: Optional[str],
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    """
    Returns (quantity, count_type, pattern_name).
    Weight units are checked first (lb, oz, kg, g), then multipack each-count.
    """
    if not title or not isinstance(title, str):
        return None, None, None
    t = title.lower()
    if re.search(r"\b\d{1,2}\s*x\s*\d{1,2}\b", t):
        return None, None, None

    for name, pat, count_type in _WEIGHT_PATTERNS:
        m = pat.search(title)
        if not m:
            continue
        qty = _parse_weight_quantity(m, name)
        if qty is None or qty < _MIN_WEIGHT_QTY or qty > _MAX_WEIGHT_QTY:
            continue
        return qty, count_type, name

    for name, pat in _COUNT_PATTERNS:
        if name == "n_pieces" and re.search(r"\b(puzzle|jigsaw)\b", t):
            continue
        m = pat.search(title)
        if not m:
            continue
        n = int(m.group(1))
        if _is_seed_bulk_each_count(title, n, name):
            continue
        if _MIN_EACH_COUNT <= n <= _MAX_EACH_COUNT:
            return float(n), "each", name

    return None, None, None


def infer_item_count_from_title(title: Optional[str]) -> Tuple[Optional[int], Optional[str]]:
    """Backward-compatible: each-count multipacks only (int)."""
    qty, count_type, pattern = infer_quantity_from_title(title)
    if qty is None or count_type != "each":
        return None, None
    return int(qty), pattern


def title_inference_fields(title: Optional[str]) -> Dict[str, Any]:
    qty, count_type, pattern = infer_quantity_from_title(title)
    return {
        "title_inferred_item_count": qty,
        "title_inferred_count_type": count_type,
        "title_inferred_pattern": pattern,
    }


def normalize_keepa_count_for_storage(value: Any) -> Optional[int]:
    """Keepa uses -1 / 0 for unknown; store NULL. Keeps 1+ for audit only."""
    if value is None:
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    return n


def _keepa_count_for_merge(value: Any) -> Optional[int]:
    """
    Keepa count usable for item_count merge only when >= 2 (multipack each).
    -1, 0, 1, and NULL are equivalent: no Keepa signal; use title or default 1.
    """
    n = normalize_keepa_count_for_storage(value)
    if n is None or n < _MIN_EACH_COUNT:
        return None
    return n


def compute_price_per_item(price: Any, item_count: Any) -> Optional[float]:
    try:
        units = float(item_count)
    except (TypeError, ValueError):
        return None
    if units <= 0:
        return None
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if p <= 0:
        return None
    return round(p / units, 4)


def parse_rainforest_unit_price_json(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    return None


def merge_item_count_signals(
    *,
    title: Optional[str],
    price: Any,
    keepa_number_of_items: Any = None,
    keepa_package_quantity: Any = None,
    rainforest_unit_price_json: Any = None,
) -> Dict[str, Any]:
    """
    Priority:
    (1) Title weight (lb, oz, kg, g) — any positive quantity
    (2) Keepa numberOfItems >= 2 (each)
    (3) Keepa packageQuantity >= 2 (each)
    (4) Title multipack each-count >= 2
    (5) default item_count=1, count_type=each

    price_per_item is price divided by item_count in the stated count_type
    (e.g. $/lb when count_type is pound).
    """
    _ = parse_rainforest_unit_price_json(rainforest_unit_price_json)
    title_fields = title_inference_fields(title)
    noi = _keepa_count_for_merge(keepa_number_of_items)
    pq = _keepa_count_for_merge(keepa_package_quantity)
    title_qty = title_fields["title_inferred_item_count"]
    title_type = title_fields["title_inferred_count_type"]

    item_count: float = 1.0
    count_type = "each"
    item_count_source = "default"

    if title_qty is not None and title_type and title_type != "each":
        item_count = float(title_qty)
        count_type = title_type
        item_count_source = "title"
    elif noi is not None and noi <= _MAX_EACH_COUNT:
        item_count = float(noi)
        count_type = "each"
        item_count_source = "keepa"
    elif pq is not None and pq <= _MAX_EACH_COUNT:
        item_count = float(pq)
        count_type = "each"
        item_count_source = "keepa"
    elif (
        title_qty is not None
        and title_type == "each"
        and title_qty >= _MIN_EACH_COUNT
    ):
        item_count = float(title_qty)
        count_type = "each"
        item_count_source = "title"

    return {
        **title_fields,
        "item_count": item_count,
        "count_type": count_type,
        "item_count_source": item_count_source,
        "price_per_item": compute_price_per_item(price, item_count),
    }


def get_keepa_unit_fields(product_data: Dict) -> Dict[str, Any]:
    """Extract numberOfItems and packageQuantity from Keepa; -1/0 stored as NULL."""
    return {
        "keepa_number_of_items": normalize_keepa_count_for_storage(
            product_data.get("numberOfItems")
        ),
        "keepa_package_quantity": normalize_keepa_count_for_storage(
            product_data.get("packageQuantity")
        ),
    }


def get_variation_asins_from_keepa(product_data: Dict) -> List[str]:
    """
    Extract all variation ASINs from Keepa product response.

    Args:
        product_data: Product dict from Keepa API response

    Returns:
        List of variation ASINs, empty list if none.
    """
    variations = product_data.get("variationASINs", [])
    return variations if variations else []


def consolidate_parent_items(items_dict: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Consolidate multiple child variations into parent entries with best search rank.
    Prioritizes items with complete data (ratings_total not None).

    Args:
        items_dict: Dict mapping ASIN -> item data (must include 'parent_asin' and 'search_rank')

    Returns:
        Dict with parent ASINs only, using best search_rank
    """
    parent_groups: Dict[str, List[Dict]] = {}

    for asin, item in items_dict.items():
        parent_asin = item.get("parent_asin") or asin
        parent_groups.setdefault(parent_asin, []).append(item)

    parent_map: Dict[str, Dict] = {}

    for parent_asin, variations in parent_groups.items():
        def _sort_price(x):
            ppi = x.get("price_per_item")
            if ppi is not None:
                return ppi
            p = x.get("price")
            return p if p is not None else float("inf")

        sorted_variations = sorted(
            variations,
            key=lambda x: (
                0 if x.get("ratings_total") is not None else 1,
                x.get("search_rank", float("inf")),
                _sort_price(x),
            ),
        )

        best_variation = sorted_variations[0].copy()
        best_variation["asin"] = parent_asin

        all_ranks = [v.get("search_rank", float("inf")) for v in variations]
        all_prices = [v.get("price") for v in variations if v.get("price") is not None]
        all_ppi = [v.get("price_per_item") for v in variations if v.get("price_per_item") is not None]

        best_variation["search_rank"] = min(all_ranks) if all_ranks else best_variation.get(
            "search_rank"
        )
        if all_prices:
            best_variation["price"] = min(all_prices)
        if all_ppi:
            best_variation["price_per_item"] = min(all_ppi)

        parent_map[parent_asin] = best_variation

    return parent_map

