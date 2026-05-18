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
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import sqlalchemy

logger = logging.getLogger(__name__)


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

_MIN_ITEM_COUNT = 2
_MAX_ITEM_COUNT = 2000


def infer_item_count_from_title(title: Optional[str]) -> Tuple[Optional[int], Optional[str]]:
    if not title or not isinstance(title, str):
        return None, None
    t = title.lower()
    if re.search(r"\b\d{1,2}\s*x\s*\d{1,2}\b", t):
        return None, None
    for name, pat in _COUNT_PATTERNS:
        if name == "n_pieces" and re.search(r"\b(puzzle|jigsaw)\b", t):
            continue
        m = pat.search(title)
        if not m:
            continue
        n = int(m.group(1))
        if _MIN_ITEM_COUNT <= n <= _MAX_ITEM_COUNT:
            return n, name
    return None, None


def title_inference_fields(title: Optional[str]) -> Dict[str, Any]:
    count, pattern = infer_item_count_from_title(title)
    return {
        "title_inferred_item_count": count,
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
    Keepa count usable for item_count merge only when >= 2 (multipack).
    -1, 0, 1, and NULL are equivalent: no Keepa signal; use title or default 1.
    """
    n = normalize_keepa_count_for_storage(value)
    if n is None or n < _MIN_ITEM_COUNT:
        return None
    return n


def compute_price_per_item(price: Any, item_count: int) -> Optional[float]:
    if item_count < 1:
        return None
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if p <= 0:
        return None
    return round(p / item_count, 4)


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
    Priority: (1) Keepa numberOfItems >= 2, (2) Keepa packageQuantity >= 2,
    (3) title heuristic, (4) default item_count=1.

    Keepa -1/0/1 are not used for merge (same as NULL); default 1 applies only
    when title inference does not find a multipack count.
    """
    _ = parse_rainforest_unit_price_json(rainforest_unit_price_json)
    title_fields = title_inference_fields(title)
    noi = _keepa_count_for_merge(keepa_number_of_items)
    pq = _keepa_count_for_merge(keepa_package_quantity)
    title_count = title_fields["title_inferred_item_count"]

    item_count = 1
    item_count_source = "default"

    if noi is not None and noi <= _MAX_ITEM_COUNT:
        item_count = noi
        item_count_source = "keepa"
    elif pq is not None and pq <= _MAX_ITEM_COUNT:
        item_count = pq
        item_count_source = "keepa"
    elif title_count is not None:
        item_count = title_count
        item_count_source = "title"

    return {
        **title_fields,
        "item_count": item_count,
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

