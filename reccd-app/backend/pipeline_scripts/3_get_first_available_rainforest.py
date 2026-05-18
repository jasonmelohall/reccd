#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
import requests
import pandas as pd
from sqlalchemy import text
import logging

# Add shared directory to path for imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DIR = os.path.join(BASE_DIR, "shared")
sys.path.insert(0, SHARED_DIR)

import reccd_items
from reccd_items import (
    earliest_valid_release_date,
    get_parent_asin_from_rainforest,
    sanitize_product_datetime,
)

# Config
RAIN_API_KEY = os.getenv("RAINFOREST_API_KEY")
if not RAIN_API_KEY:
    raise RuntimeError("Missing environment variable RAINFOREST_API_KEY. Please export it before running.")
SEARCH_TERMS = reccd_items.get_search_term()
if isinstance(SEARCH_TERMS, str):
    SEARCH_TERMS = [SEARCH_TERMS]  # ✅ Fallback for single term

print()

# DB Setup
engine = reccd_items.mysqlengine()
conn = engine.connect()

# Logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def fetch_product_dates(asin):
    """Fetch product details from Rainforest API including dates, ratings, price, and title."""
    url = "https://api.rainforestapi.com/request"
    params = {
        "api_key": RAIN_API_KEY,
        "type": "product",
        "amazon_domain": "amazon.com",
        "asin": asin
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            product_data = response.json().get('product', {})
            parent_asin = get_parent_asin_from_rainforest(product_data)
            first_available = product_data.get('first_available', {}).get('utc')
            reviews = product_data.get('reviews', [])
            oldest_review = min([r.get('date', {}).get('utc') for r in reviews if r.get('date')], default=None)
            ratings_total = product_data.get('ratings_total')
            title = product_data.get('title')
            price = product_data.get('buybox_winner', {}).get('price', {}).get('value') if product_data.get('buybox_winner') else None
            rating = product_data.get('rating')
            buybox = product_data.get('buybox_winner') or {}
            unit_price = buybox.get('unit_price')
            unit_json = json.dumps(unit_price) if unit_price is not None else None
            return parent_asin, first_available, oldest_review, ratings_total, title, price, rating, unit_json
        else:
            logging.error(f"Error fetching {asin}: {response.status_code} - {response.text}")
            return None, None, None, None, None, None, None, None
    except Exception as e:
        logging.error(f"Exception fetching {asin}: {e}")
        return None, None, None, None, None, None, None, None

if __name__ == "__main__":
    total_success = 0
    total_fail = 0

    for term in SEARCH_TERMS:
        logging.info(f"=== Processing search term: {term} ===")

        # Match items by exact search_term (one term per row)
        result = conn.execute(text("""
            SELECT asin, ratings_total, listed_date, oldest_review, release_date
            FROM items
            WHERE (
                release_date IS NULL
                OR release_date < '2011-01-02'
                OR listed_date < '2011-01-02'
            )
            AND search_term = :term
        """), {"term": term}).fetchall()

        asin_data = [tuple(row) for row in result]
        logging.info(f"Found {len(asin_data)} items to update for search term '{term}'.")

        success_count = 0
        fail_count = 0

        for idx, row in enumerate(asin_data, start=1):
            asin = row[0]
            current_ratings_total = row[1]
            existing_listed = row[2] if len(row) > 2 else None
            existing_oldest_review = row[3] if len(row) > 3 else None
            parent_asin, first_available_str, oldest_review_str, ratings_total, title, price, rating, unit_json = fetch_product_dates(asin)
            update_fields = {}
            
            # Store parent_asin if found
            if parent_asin:
                update_fields['parent_asin'] = parent_asin

            first_available = None
            if first_available_str:
                fa_raw = pd.to_datetime(first_available_str, errors='coerce')
                if fa_raw is not pd.NaT:
                    first_available = sanitize_product_datetime(fa_raw)
                    if first_available is not None:
                        update_fields['first_available'] = first_available

            if oldest_review_str:
                or_raw = pd.to_datetime(oldest_review_str, errors='coerce')
                if or_raw is not pd.NaT:
                    rf_review = sanitize_product_datetime(or_raw)
                    if rf_review is not None:
                        update_fields['oldest_review'] = rf_review

            eff_release = earliest_valid_release_date(
                first_available,
                update_fields.get('oldest_review'),
                existing_listed,
                existing_oldest_review,
            )
            if eff_release is not None:
                update_fields['release_date'] = eff_release

            # Only update ratings_total (and associated metadata) if new value is greater than existing, or if existing is NULL
            if ratings_total is not None:
                if current_ratings_total is None or ratings_total > current_ratings_total:
                    update_fields['ratings_total'] = ratings_total
                    # Update all metadata together when we have higher ratings_total (more recent data)
                    if title:
                        update_fields['title'] = title
                    if price is not None:
                        update_fields['price'] = price
                    if rating is not None:
                        update_fields['rating'] = rating

            if unit_json is not None:
                update_fields['rainforest_unit_price_json'] = unit_json

            if update_fields:
                update_fields['asin'] = asin
                set_parts = [f"{col} = :{col}" for col in update_fields if col != 'asin']
                if unit_json is not None:
                    set_parts.append("rainforest_updated_at = UTC_TIMESTAMP()")
                    set_parts.append("item_count_updated_at = NULL")

                query = text(f"""
                    UPDATE items
                    SET {', '.join(set_parts)}
                    WHERE asin = :asin
                """)
                conn.execute(query, update_fields)
                conn.commit()
                success_count += 1
            else:
                fail_count += 1

            if idx % 10 == 0 or idx == len(asin_data):
                logging.info(f"Progress: {idx}/{len(asin_data)} processed ({success_count} updated, {fail_count} failed)")

        logging.info(f"✅ Finished term '{term}': {success_count} items updated, {fail_count} items failed.")
        total_success += success_count
        total_fail += fail_count

    logging.info(f"🎉 All Done! {total_success} total updated, {total_fail} total failed.")
print()
