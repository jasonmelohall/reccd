#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
from reccd_items import get_parent_asin_from_rainforest

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
            return parent_asin, first_available, oldest_review, ratings_total, title, price, rating
        else:
            logging.error(f"Error fetching {asin}: {response.status_code} - {response.text}")
            return None, None, None, None, None, None, None
    except Exception as e:
        logging.error(f"Exception fetching {asin}: {e}")
        return None, None, None, None, None, None, None

if __name__ == "__main__":
    total_success = 0
    total_fail = 0

    for term in SEARCH_TERMS:
        logging.info(f"=== Processing search term: {term} ===")

        # Match items by exact search_term (one term per row)
        result = conn.execute(text("""
            SELECT asin, ratings_total FROM items
            WHERE release_date IS NULL
            AND search_term = :term
        """), {"term": term}).fetchall()

        asin_data = [(row[0], row[1]) for row in result]
        logging.info(f"Found {len(asin_data)} items to update for search term '{term}'.")

        success_count = 0
        fail_count = 0

        for idx, (asin, current_ratings_total) in enumerate(asin_data, start=1):
            parent_asin, first_available_str, oldest_review_str, ratings_total, title, price, rating = fetch_product_dates(asin)
            update_fields = {}
            
            # Store parent_asin if found
            if parent_asin:
                update_fields['parent_asin'] = parent_asin

            if first_available_str:
                first_available = pd.to_datetime(first_available_str, errors='coerce')
                if first_available is not pd.NaT:
                    update_fields['first_available'] = first_available
                    update_fields['release_date'] = first_available  # 🔥 also sets release_date

            if oldest_review_str:
                oldest_review = pd.to_datetime(oldest_review_str, errors='coerce')
                if oldest_review is not pd.NaT:
                    update_fields['oldest_review'] = oldest_review

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

            if update_fields:
                update_fields['asin'] = asin

                query = text(f"""
                    UPDATE items
                    SET {', '.join([f"{col} = :{col}" for col in update_fields if col != 'asin'])}
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
