#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import requests
from sqlalchemy import text
import datetime
import logging

# Add shared directory to path for imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DIR = os.path.join(BASE_DIR, "shared")
sys.path.insert(0, SHARED_DIR)

import reccd_items
from reccd_items import get_parent_asin_from_rainforest, consolidate_parent_items

# Config
RAIN_API_KEY = os.getenv("RAINFOREST_API_KEY")
if not RAIN_API_KEY:
    raise RuntimeError("Missing environment variable RAINFOREST_API_KEY. Please export it before running.")
ASSOCIATE_TAG = 'reccd-20'
MAX_PAGES = 1  # Number of pages to fetch per search term (increase for more results, but slower)
SEARCH_TERMS = reccd_items.get_search_term()  # ✅ Now expected to return a list, e.g., ["term1", "term2"]
print()

# DB Setup
engine = reccd_items.mysqlengine()
conn = engine.connect()

# Logging Setup
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# === API Call ===
def search_amazon(query, page=1):
    logging.info(f"Searching Amazon for: {query} (page {page})")
    url = "https://api.rainforestapi.com/request"
    params = {
        "api_key": RAIN_API_KEY,
        "type": "search",
        "amazon_domain": "amazon.com",
        "search_term": query,
        "page": page
    }
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        logging.info("Successfully fetched search results.")
        return response.json()
    else:
        logging.error(f"Error: {response.status_code} - {response.text}")
        print(f"❌ Critical error with API. Stopping script.")
        sys.exit(1)

# Extract item data and prepare for parent consolidation
def extract_item_data(item, search_term, search_rank):
    """Extract item data including parent ASIN and image URL if available."""
    child_asin = item.get('asin')
    title = item.get('title')
    price = item.get('price', {}).get('value') if item.get('price') else None
    rating = item.get('rating')
    ratings_total = item.get('ratings_total')
    image_url = item.get('image')  # Extract image URL from API response
    
    # Extract parent ASIN - if no parent exists, use child ASIN as parent
    parent_asin = get_parent_asin_from_rainforest(item) or child_asin
    
    if not child_asin or not title:
        return None
    
    return {
        'asin': child_asin,
        'parent_asin': parent_asin,
        'title': title,
        'price': price,
        'rating': rating,
        'ratings_total': ratings_total,
        'search_term': search_term,
        'search_rank': search_rank,
        'image_url': image_url
    }


def save_items_batch(consolidated_items):
    """Save a batch of consolidated parent items to database."""
    for parent_asin, item_data in consolidated_items.items():
        asin = item_data['asin']  # This is now the parent ASIN
        title = item_data['title']
        price = item_data['price']
        rating = item_data['rating']
        ratings_total = item_data['ratings_total']
        search_term = item_data['search_term']
        search_rank = item_data['search_rank']
        parent_asin_field = item_data['parent_asin']
        image_url = item_data.get('image_url')  # Extract image URL
        
        # For standalone products (parent_asin == asin), store NULL in parent_asin column
        # This helps distinguish standalone products from parent products with variations
        if parent_asin_field == asin:
            parent_asin_field = None
        
        # Generate link using parent ASIN
        link = f"https://www.amazon.com/dp/{asin}?tag={ASSOCIATE_TAG}"
        
        # Check for duplicate (same price, rating, ratings_total, and search_term)
        duplicate_query = text("""
            SELECT asin FROM items
            WHERE price = :price
              AND rating = :rating
              AND ratings_total = :ratings_total
              AND search_term = :search_term
            LIMIT 1
        """)
        result = conn.execute(duplicate_query, {
            "price": price,
            "rating": rating,
            "ratings_total": ratings_total,
            "search_term": search_term
        }).fetchone()
        
        if result:
            logging.info(f"Skipping duplicate item (already exists with same price/rating/ratings_total): {title}")
            continue
        
        # Insert with parent_asin and image_url fields
        query = text("""
            INSERT INTO items (asin, parent_asin, title, link, price, rating, ratings_total, search_term, search_rank, image_url, last_update)
            VALUES (:asin, :parent_asin, :title, :link, :price, :rating, :ratings_total, :search_term, :search_rank, :image_url, :last_update)
            ON DUPLICATE KEY UPDATE
                title = IF(COALESCE(VALUES(ratings_total), 0) >= COALESCE(ratings_total, 0), VALUES(title), title),
                link = VALUES(link),
                price = IF(COALESCE(VALUES(ratings_total), 0) >= COALESCE(ratings_total, 0), VALUES(price), price),
                rating = IF(COALESCE(VALUES(ratings_total), 0) >= COALESCE(ratings_total, 0), VALUES(rating), rating),
                ratings_total = CASE 
                    WHEN ratings_total IS NULL THEN VALUES(ratings_total)
                    WHEN VALUES(ratings_total) IS NULL THEN ratings_total
                    ELSE GREATEST(ratings_total, VALUES(ratings_total))
                END,
                search_term = VALUES(search_term),
                search_rank = CASE 
                    WHEN search_rank IS NULL THEN VALUES(search_rank)
                    WHEN VALUES(search_rank) IS NULL THEN search_rank
                    ELSE LEAST(search_rank, VALUES(search_rank))
                END,
                image_url = COALESCE(VALUES(image_url), image_url),
                last_update = VALUES(last_update)
        """)
        conn.execute(query, {
            "asin": asin,
            "parent_asin": parent_asin_field,
            "title": title,
            "link": link,
            "price": price,
            "rating": rating,
            "ratings_total": ratings_total,
            "search_term": search_term,
            "search_rank": search_rank,
            "image_url": image_url,
            "last_update": datetime.datetime.utcnow()
        })
    
    conn.commit()
    logging.info(f"✅ Saved {len(consolidated_items)} parent items to database")

# === Main ===
if __name__ == "__main__":
    if isinstance(SEARCH_TERMS, str):
        SEARCH_TERMS = [SEARCH_TERMS]  # Fallback in case get_search_term() still returns a single term

    # Collect ALL items across ALL search terms before consolidating (same as items folder)
    # When the same product appears from multiple terms, keep the one with better search rank; store that term as search_term
    all_items_global = {}  # Map child_asin -> item_data (global across all searches)

    for term in SEARCH_TERMS:
        logging.info(f"=== Processing search term: {term} ===")
        
        for page in range(1, MAX_PAGES + 1):  # Fetch MAX_PAGES per search term
            data = search_amazon(term, page=page)
            if data:
                results = data.get('search_results', [])
                for idx, item in enumerate(results):
                    # Calculate absolute search rank across pages
                    search_rank = (page - 1) * len(results) + idx + 1
                    
                    item_data = extract_item_data(item, term, search_rank)
                    if item_data:
                        child_asin = item_data['asin']
                        # If this ASIN already exists (from another search), keep the one with better rank
                        if child_asin in all_items_global:
                            existing = all_items_global[child_asin]
                            if search_rank < existing['search_rank']:
                                all_items_global[child_asin] = item_data
                        else:
                            all_items_global[child_asin] = item_data

    # Now consolidate by parent ASIN across ALL search terms
    logging.info(f"Collected {len(all_items_global)} unique items across all searches, consolidating by parent ASIN...")
    consolidated = consolidate_parent_items(all_items_global)
    logging.info(f"Consolidated to {len(consolidated)} parent items")

    # Save all consolidated items (one search_term per row = the term that returned this result)
    save_items_batch(consolidated)
        
    logging.info("✅ Done inserting items for all search terms.")
print()