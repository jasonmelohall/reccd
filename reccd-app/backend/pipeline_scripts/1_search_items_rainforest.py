#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rainforest Amazon search → items table.
Mirrors items/11_search_items_rainforest.py (shared upsert in reccd_items).
"""

import json
import logging
import os
import sys
import time

import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DIR = os.path.join(BASE_DIR, "shared")
sys.path.insert(0, SHARED_DIR)

import reccd_items
from reccd_items import (
    consolidate_parent_items,
    get_parent_asin_from_rainforest,
    save_rainforest_search_batch,
)

RAIN_API_KEY = os.getenv("RAINFOREST_API_KEY")
if not RAIN_API_KEY:
    raise RuntimeError("Missing environment variable RAINFOREST_API_KEY. Please export it before running.")
ASSOCIATE_TAG = "reccd-20"
MAX_PAGES = 1
SEARCH_TERMS = reccd_items.get_search_term()
print()

engine = reccd_items.mysqlengine()
conn = engine.connect()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def search_amazon(query, page=1, max_retries=5):
    """Search Amazon via Rainforest API with retry logic for transient errors."""
    url = "https://api.rainforestapi.com/request"
    params = {
        "api_key": RAIN_API_KEY,
        "type": "search",
        "amazon_domain": "amazon.com",
        "search_term": query,
        "page": page,
    }
    headers = {"User-Agent": "Reccd/1.0 (API Client)"}

    for attempt in range(1, max_retries + 1):
        logging.info("Searching Amazon for: %s (page %s, attempt %s/%s)", query, page, attempt, max_retries)
        try:
            response = requests.get(url, params=params, headers=headers, timeout=60)

            if response.status_code == 200:
                return response.json()
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                logging.warning("Rate limited (429). Waiting %s seconds...", retry_after)
                time.sleep(retry_after)
                continue
            if response.status_code in [500, 502, 503, 504] and attempt < max_retries:
                wait_time = min(2 ** attempt, 60)
                try:
                    error_data = response.json()
                    if isinstance(error_data, dict) and "request_info" in error_data:
                        retry_after = error_data["request_info"].get("retry_after")
                        if retry_after is not None:
                            wait_time = int(retry_after)
                except (json.JSONDecodeError, ValueError, KeyError):
                    pass
                logging.warning("Server error %s; retrying in %ss", response.status_code, wait_time)
                time.sleep(wait_time)
                continue

            logging.error("Error: %s - %s", response.status_code, response.text[:500])
            print("❌ Critical error with API. Stopping script.")
            sys.exit(1)
        except requests.RequestException as exc:
            if attempt < max_retries:
                wait_time = min(2 ** attempt, 60)
                logging.warning("Request exception (attempt %s/%s): %s", attempt, max_retries, exc)
                time.sleep(wait_time)
                continue
            logging.error("Request failed after %s attempts: %s", max_retries, exc)
            print(f"❌ Critical error with API after {max_retries} retries. Stopping script.")
            sys.exit(1)

    logging.error("Failed to get response after %s attempts", max_retries)
    sys.exit(1)


def extract_item_data(item, search_term, search_rank):
    child_asin = item.get("asin")
    title = item.get("title")
    price = item.get("price", {}).get("value") if item.get("price") else None
    rating = item.get("rating")
    ratings_total = item.get("ratings_total")
    image_url = item.get("image")
    parent_asin = get_parent_asin_from_rainforest(item) or child_asin

    if not child_asin or not title:
        return None

    return {
        "asin": child_asin,
        "parent_asin": parent_asin,
        "title": title,
        "price": price,
        "rating": rating,
        "ratings_total": ratings_total,
        "search_term": search_term,
        "search_rank": search_rank,
        "image_url": image_url,
    }


if __name__ == "__main__":
    if isinstance(SEARCH_TERMS, str):
        SEARCH_TERMS = [SEARCH_TERMS]

    all_items_global = {}

    for term in SEARCH_TERMS:
        logging.info("=== Processing search term: %s ===", term)
        for page in range(1, MAX_PAGES + 1):
            data = search_amazon(term, page=page)
            if not data:
                continue
            results = data.get("search_results", [])
            for idx, item in enumerate(results):
                search_rank = (page - 1) * len(results) + idx + 1
                item_data = extract_item_data(item, term, search_rank)
                if not item_data:
                    continue
                child_asin = item_data["asin"]
                if child_asin in all_items_global:
                    existing = all_items_global[child_asin]
                    if search_rank < existing["search_rank"]:
                        all_items_global[child_asin] = item_data
                else:
                    all_items_global[child_asin] = item_data

    logging.info(
        "Collected %s unique items across all searches, consolidating by parent ASIN...",
        len(all_items_global),
    )
    consolidated = consolidate_parent_items(all_items_global)
    logging.info("Consolidated to %s parent items", len(consolidated))

    save_rainforest_search_batch(conn, consolidated, associate_tag=ASSOCIATE_TAG)
    logging.info("✅ Saved %s parent items to database", len(consolidated))
    logging.info("✅ Done inserting items for all search terms.")
print()
