import os
import sys
import pandas as pd
from sqlalchemy import text
import datetime
import signal
import numpy as np

# Add shared directory to path for imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DIR = os.path.join(BASE_DIR, "shared")
sys.path.insert(0, SHARED_DIR)

import reccd_items
from reccd_items import (
    apply_exclude_wildcards,
    apply_wildcards,
    dedupe_items_by_parent_asin,
    load_user_coefficients,
    score_items_dataframe,
)

# === Configuration ===
PRINT_ROWS = 21

USER_ID = 1  # 🔥 Your user ID
EMAIL = "jasonmelohall@gmail.com"  # Your email for user table lookup

# === Wildcard Configuration ===
# Options: 'both_ends', 'start_only', 'end_only', 'none'
WILDCARD_MODE = 'both_ends'

# === Exclude Terms Configuration ===
# Items with these terms in their title will be excluded from results
EXCLUDE_TERMS = [

]

# Constant is now loaded from database

# Get original search terms and apply wildcards
ORIGINAL_SEARCH_TERMS = reccd_items.get_search_term()

# Apply wildcards to search terms and exclude terms
SEARCH_TERMS_WITH_WILDCARDS = apply_wildcards(ORIGINAL_SEARCH_TERMS, WILDCARD_MODE)
EXCLUDE_TERMS_WITH_WILDCARDS = apply_exclude_wildcards(EXCLUDE_TERMS)

# === Database Setup ===
engine = reccd_items.mysqlengine()
conn = engine.connect()

with engine.connect() as coeff_conn:
    coefficients, CONSTANT = load_user_coefficients(coeff_conn, EMAIL)

# Extract individual weights
MONETARY_WEIGHT = coefficients['price_percentile']
RATING_WEIGHT = coefficients['rating_percentile']
ITEM_RECENCY_WEIGHT = coefficients['release_date_percentile']
ITEM_FREQUENCY_WEIGHT = coefficients['frequency_percentile']
SEARCH_RANK_WEIGHT = coefficients['search_rank_percentile']

print(f"\n=== Loaded User Coefficients ===")
print(f"Price (Monetary):     {MONETARY_WEIGHT:>10.6f}")
print(f"Rating:               {RATING_WEIGHT:>10.6f}")
print(f"Release Date (Recency): {ITEM_RECENCY_WEIGHT:>10.6f}")
print(f"Frequency:            {ITEM_FREQUENCY_WEIGHT:>10.6f}")
print(f"Search Rank:          {SEARCH_RANK_WEIGHT:>10.6f}")
print(f"Constant:             {CONSTANT:>10.6f}")
print()

# Pandas display options for single-line printing
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 60)
pd.set_option('display.max_rows', PRINT_ROWS)

def clean(val):
    return None if pd.isna(val) else val

try:
    # ✅ === Fetch Items for All Search Terms with Wildcards and Exclude Terms ===
    # Build LIKE conditions for each search term
    like_conditions = []
    params = {"user_id": USER_ID}
    
    for i, term in enumerate(SEARCH_TERMS_WITH_WILDCARDS):
        param_name = f"term{i}"
        like_conditions.append(f"i.search_term LIKE :{param_name}")
        params[param_name] = term
    
    # Build NOT LIKE conditions for exclude terms
    exclude_conditions = []
    for i, term in enumerate(EXCLUDE_TERMS_WITH_WILDCARDS):
        param_name = f"exclude{i}"
        exclude_conditions.append(f"i.title NOT LIKE :{param_name}")
        params[param_name] = term
    
    # Join all LIKE conditions with OR
    like_clause = " OR ".join(like_conditions)
    
    # Join all exclude conditions with AND
    exclude_clause = " AND ".join(exclude_conditions) if exclude_conditions else "1=1"
    
    query_str = f"""
        SELECT *
        FROM items i
        WHERE ({like_clause})
        AND ({exclude_clause})
        AND NOT EXISTS (
            SELECT 1
            FROM items_user u
            WHERE u.user_id = :user_id
            AND u.asin = i.asin
            AND u.is_relevant = 0
            AND u.search_term = i.search_term
        )
    """
    result = conn.execute(text(query_str), params)
    rows = result.fetchall()
    df = pd.DataFrame(rows, columns=result.keys()) if rows else pd.DataFrame()

    df = score_items_dataframe(df, coefficients, CONSTANT, score_column="reccd")
    df = dedupe_items_by_parent_asin(df, score_column="reccd")

    df['clean_link'] = df['link'].str.replace(r'\?tag=.*$', '', regex=True)

    if df['ratings_total'].notna().all():
        df['ratings_total'] = df['ratings_total'].astype(int)

    df['release_date'] = df['release_date'].dt.strftime('%Y-%m-%d')
    df['listed_date'] = df['listed_date'].dt.strftime('%Y-%m-%d')
    df['oldest_review'] = df['oldest_review'].dt.strftime('%Y-%m-%d')
    if 'last_update' in df.columns:
        df['last_update'] = (
            pd.to_datetime(df['last_update'], errors='coerce')
            .dt.strftime('%Y-%m-%d %H:%M')
        )

    # === Display ===
    print("\n=== Search Terms ===\n")
    print(ORIGINAL_SEARCH_TERMS)
    print(df[['title', 'link', 'search_term']].head(PRINT_ROWS))

    print("\n=== Percentile Contributions ===")
    print(df[['title', 'price_percentile', 'rating_percentile', 'release_date_percentile',
              'frequency_percentile', 'search_rank_percentile', 'link']].head(PRINT_ROWS))
    print()

    print(f"\n=== Total Rows: {len(df):,} ===")

    print(f"\n=== Final Recommendations ===")
    display_cols = [
        'title', 'price', 'item_count', 'price_per_item',
        'rating', 'ratings_total', 'frequency', 'search_rank',
        'listed_date', 'oldest_review', 'release_date', 'reccd', 'asin',
        'clean_link', 'last_update',
    ]
    display_cols = [c for c in display_cols if c in df.columns]
    print(df[display_cols].head(PRINT_ROWS))
    print()

    coefficients_df = pd.DataFrame([{
        'Price (Monetary)': MONETARY_WEIGHT,
        'Rating': RATING_WEIGHT,
        'Release Date (Recency)': ITEM_RECENCY_WEIGHT,
        'Ratings Frequency': ITEM_FREQUENCY_WEIGHT,
        'Search Rank': SEARCH_RANK_WEIGHT,
        'Constant': CONSTANT
    }])
    print(coefficients_df)
    print()

    # === Save Purchased ASINs (unchanged from your version) ===
    # Skip interactive input when running in non-interactive environment (e.g., Render subprocess)
    purchased_asins = ""
    try:
        if sys.stdin.isatty():
            # Only prompt if we have an interactive terminal
            def timeout_handler(signum, frame):
                raise TimeoutError

            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(300)

            try:
                purchased_asins = input("\nEnter comma-separated ASINs you purchased (or press Enter to skip): ").strip()
                signal.alarm(0)
            except TimeoutError:
                print("\n⏰ No input received. Skipping.")
                purchased_asins = ""
        else:
            print("\n⏰ Running in non-interactive mode. Skipping purchase input.")
    except (EOFError, OSError):
        # EOFError when stdin is closed (subprocess), OSError if signal.SIGALRM not available
        print("\n⏰ No interactive input available. Skipping purchase recording.")
        purchased_asins = ""

    if purchased_asins:
        purchased_asins = [asin.strip() for asin in purchased_asins.split(",")]
        now = datetime.datetime.utcnow()

        inserted = 0
        for raw_asin in purchased_asins:
            is_negative = raw_asin.startswith('-')
            asin = raw_asin.lstrip('-')

            match = df[df['asin'] == asin]
            if not match.empty:
                row = match.iloc[0]

                # Get parent_asin from row, or use asin if no parent
                parent_asin = row.get('parent_asin') or asin
                
                conn.execute(text("""
                    INSERT INTO items_user (
                        user_id, asin, parent_asin, title, price, rating, ratings_total, frequency, search_rank, release_date,
                        reccd_score, price_percentile, rating_percentile, release_date_percentile,
                        frequency_percentile, search_rank_percentile,
                        item_count, price_per_item, item_count_percentile,
                        purchase_datetime, search_term, is_relevant, event_type
                    )
                    VALUES (
                        :user_id, :asin, :parent_asin, :title, :price, :rating, :ratings_total, :frequency, :search_rank, :release_date,
                        :reccd_score, :price_percentile, :rating_percentile, :release_date_percentile,
                        :frequency_percentile, :search_rank_percentile,
                        :item_count, :price_per_item, :item_count_percentile,
                        :purchase_datetime, :search_term, :is_relevant, :event_type
                    )
                """), {
                    "user_id": USER_ID,
                    "asin": asin,
                    "parent_asin": parent_asin,
                    "title": clean(row['title']),
                    "price": clean(row['price']),
                    "rating": clean(row['rating']),
                    "ratings_total": clean(row['ratings_total']),
                    "frequency": clean(row['frequency']),
                    "search_rank": clean(row['search_rank']),
                    "release_date": clean(row['release_date']),
                    "reccd_score": clean(row['reccd']),
                    "price_percentile": clean(row['price_percentile']),
                    "rating_percentile": clean(row['rating_percentile']),
                    "release_date_percentile": clean(row['release_date_percentile']),
                    "frequency_percentile": clean(row['frequency_percentile']),
                    "search_rank_percentile": clean(row['search_rank_percentile']),
                    "item_count": clean(row.get('item_count')),
                    "price_per_item": clean(row.get('price_per_item')),
                    "item_count_percentile": clean(row.get('item_count_percentile')),
                    "purchase_datetime": now,
                    "search_term": row['search_term'],
                    "is_relevant": not is_negative,
                    "event_type": "purchase"
                })
                inserted += 1
            else:
                print(f"⚠️ ASIN not found: {asin}")

        conn.commit()
        print(f"\n✅ Saved {inserted} purchases to items_user!")

finally:
    conn.close()
    print()