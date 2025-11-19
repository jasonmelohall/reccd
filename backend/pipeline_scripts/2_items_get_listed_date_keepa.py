#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import time
import sys
from datetime import datetime, timedelta
import os
import requests
from sqlalchemy import text

# Add shared directory to path for imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DIR = os.path.join(BASE_DIR, "shared")
sys.path.insert(0, SHARED_DIR)

from reccd_items import mysqlengine, get_search_term, get_parent_asin_from_keepa, get_variation_asins_from_keepa

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
KEEPA_API_KEY = os.getenv("KEEPA_API_KEY")
if not KEEPA_API_KEY:
    raise RuntimeError("Missing environment variable KEEPA_API_KEY. Please export it before running.")
KEEPA_API_URL = 'https://api.keepa.com/product'
MAX_RECORDS = 10000
BATCH_SIZE = 200
TIMEOUT = 10

# Initialize DB connection
SEARCH_TERMS = get_search_term()  # ✅ Now expected to return a list
if isinstance(SEARCH_TERMS, str):
    SEARCH_TERMS = [SEARCH_TERMS]  # ✅ Fallback for single term

print(f'Using search terms: {SEARCH_TERMS}')
engine = mysqlengine()
conn = engine.connect()
print('Connected to MySQL')


def get_listed_date(asin):
    """
    Get product dates from Keepa API.
    For parent ASINs with variations, aggregates earliest dates across all variations.
    """
    base_date = datetime(2011, 1, 1)
    max_retries = 5
    delay = 60

    for attempt in range(max_retries):
        try:
            hit_429 = False
            params = {
                'key': KEEPA_API_KEY,
                'domain': 1,
                'asin': asin,
                'history': 1,
                'rating': 1
            }
            response = requests.get(KEEPA_API_URL, params=params, timeout=TIMEOUT)
            if response.status_code == 429:
                hit_429 = True
                logger.warning(f"429 Too Many Requests for ASIN {asin}. Sleeping {delay}s...")
                time.sleep(delay)
                return asin, None, None, None, None, None, hit_429, None

            response.raise_for_status()
            data = response.json()
            if 'products' not in data or not data['products']:
                return asin, None, None, None, None, None, False, data.get('tokensLeft')

            product = data['products'][0]
            
            # Check if this is a parent ASIN
            parent_asin = get_parent_asin_from_keepa(product)
            variation_asins = get_variation_asins_from_keepa(product)
            
            # Get listed date
            listed_min = product.get('listedSince')
            listed_date = base_date + timedelta(minutes=listed_min) if listed_min is not None else None

            # Get oldest review date from reviewCountHistory (CSV style)
            csv = product.get("csv", [])
            review_csv = csv[17] if len(csv) > 17 and csv[17] is not None else []

            oldest_review_date = None
            for i in range(0, len(review_csv), 2):
                timestamp = review_csv[i]
                count = review_csv[i + 1]
                if count > 0:
                    oldest_review_date = base_date + timedelta(minutes=timestamp)
                    break

            # For parent ASINs with variations, we could fetch variation dates to get earliest
            # But this would add significant API calls. For now, parent's dates should be representative.
            # TODO v2: If needed, fetch variation dates and aggregate
            
            stats = product.get("stats")
            rating_raw = stats.get("rating") if stats else None
            rating = rating_raw / 10.0 if isinstance(rating_raw, (int, float)) else None

            # Get ratings_total from reviews data
            reviews = product.get("reviews", {})
            rating_count_data = reviews.get("ratingCount", [])
            ratings_total = None
            if rating_count_data and len(rating_count_data) >= 2:
                # The last entry in the ratingCount array is the most recent total
                ratings_total = rating_count_data[-1]

            logger.debug(f"{asin} → parent={parent_asin}, listed_date={listed_date}, oldest_review={oldest_review_date}, ratings_total={ratings_total}")
            return asin, parent_asin, listed_date, oldest_review_date, rating, ratings_total, hit_429, data.get('tokensLeft')

        except requests.RequestException as e:
            logger.warning(f"Request error for ASIN {asin}: {e}")
            time.sleep(delay)

    logger.error(f"Failed to retrieve Keepa data for ASIN {asin} after {max_retries} retries.")
    return asin, None, None, None, None, None, False, None


# === Main Processing ===
total_updated = 0

for term in SEARCH_TERMS:
    logger.info(f"=== Processing search term: {term} ===")

    select_query = text("""
        SELECT asin, release_date, ratings_total
        FROM items
        WHERE (
            (listed_date IS NULL OR oldest_review IS NULL OR ratings_total IS NULL)
            AND (
                listed_last_update IS NULL
                OR listed_last_update < NOW() - INTERVAL 7 DAY
            )
        )
        AND search_term LIKE :search_term
        ORDER BY rating DESC
        LIMIT :limit
    """)

    rows = conn.execute(select_query, {
        'limit': MAX_RECORDS,
        'search_term': f"%{term}%"
    }).mappings()

    asin_rows = list(rows)
    logger.info(f"Loaded {len(asin_rows)} ASINs to process for '{term}'.")

    if not asin_rows:
        continue

    sleep_time = 1  # Start with 1 second

    for index, row in enumerate(asin_rows, start=1):
        asin = row['asin']
        current_ratings_total = row['ratings_total']
        logger.info(f"Processing ASIN: {asin} - {index} of {len(asin_rows)}")
        asin_val, parent_asin, listed_date, oldest_review, rating, ratings_total, hit_429, tokens_left = get_listed_date(asin)

        print(f"   • Parent ASIN       : {parent_asin if parent_asin else 'None (standalone product)'}")
        print(f"   • Listed since date : {listed_date.date() if listed_date else 'None'}")
        print(f"   • Oldest review date: {oldest_review.date() if oldest_review else 'None'}")
        print(f"   • Ratings total     : {ratings_total if ratings_total else 'None'}")
        print()

        update_fields = {
            'listed_date': listed_date,
            'oldest_review': oldest_review,
            'asin': asin
        }
        
        # Update parent_asin if found
        parent_asin_clause = ""
        if parent_asin:
            update_fields['parent_asin'] = parent_asin
            parent_asin_clause = "parent_asin = :parent_asin,\n"

        sql = """
            UPDATE items
            SET
                {parent_asin_clause}
                listed_date = :listed_date,
                oldest_review = :oldest_review,
                {rating_clause}
                {ratings_total_clause}
                release_date = CASE
                    WHEN release_date IS NULL OR (
                        COALESCE(:listed_date, '9999-12-31') < release_date
                        AND COALESCE(:listed_date, '9999-12-31') IS NOT NULL
                    )
                    THEN :listed_date
                    ELSE release_date
                END,
                listed_last_update = NOW()
            WHERE asin = :asin
        """

        # Only update rating and ratings_total if new ratings_total is higher (more recent data)
        rating_clause = ""
        ratings_total_clause = ""
        
        if ratings_total is not None:
            if current_ratings_total is None or ratings_total > current_ratings_total:
                update_fields['ratings_total'] = ratings_total
                ratings_total_clause = "ratings_total = :ratings_total,\n"
                # Also update rating when ratings_total is higher (keep metadata in sync)
                if rating is not None:
                    update_fields['rating'] = rating
                    rating_clause = "rating = :rating,\n"

        update_stmt = text(sql.format(
            parent_asin_clause=parent_asin_clause,
            rating_clause=rating_clause, 
            ratings_total_clause=ratings_total_clause
        ))
        conn.execute(update_stmt, update_fields)
        conn.commit()
        total_updated += 1

        # Adjust sleep time based on whether we hit 429
        if hit_429:
            sleep_time += 30
            logger.info(f"Increasing sleep time to {sleep_time} seconds due to 429.")
        else:
            sleep_time = max(0, sleep_time - 1)  # Optional: reduce back toward baseline over time

        logger.info(f"Sleeping for {sleep_time} seconds... (tokens left: {tokens_left})")
        time.sleep(sleep_time)

logger.info(f'✅ Total updated items across all search terms: {total_updated}')
print()
