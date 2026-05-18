#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilities shared with the backend that originated from the standalone
`items/reccd_items.py` module. Keeping a local copy ensures the backend
deploy contains the helpers it needs without modifying the original files.
"""

import logging
import os
from typing import Dict, List, Optional

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
        sorted_variations = sorted(
            variations,
            key=lambda x: (
                0 if x.get("ratings_total") is not None else 1,
                x.get("search_rank", float("inf")),
                x.get("price") if x.get("price") is not None else float("inf"),
            ),
        )

        best_variation = sorted_variations[0].copy()
        best_variation["asin"] = parent_asin

        all_ranks = [v.get("search_rank", float("inf")) for v in variations]
        all_prices = [v.get("price") for v in variations if v.get("price") is not None]

        best_variation["search_rank"] = min(all_ranks) if all_ranks else best_variation.get(
            "search_rank"
        )
        if all_prices:
            best_variation["price"] = min(all_prices)

        parent_map[parent_asin] = best_variation

    return parent_map

