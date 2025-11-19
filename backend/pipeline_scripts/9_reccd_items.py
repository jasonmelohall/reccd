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

def apply_wildcards(search_terms, mode):
    """Apply wildcards to search terms based on mode"""
    if mode == 'both_ends':
        return [f"%{term}%" for term in search_terms]
    elif mode == 'start_only':
        return [f"%{term}" for term in search_terms]
    elif mode == 'end_only':
        return [f"{term}%" for term in search_terms]
    elif mode == 'none':
        return search_terms
    else:
        # Default to both ends if invalid mode
        return [f"%{term}%" for term in search_terms]

def apply_exclude_wildcards(exclude_terms):
    """Apply wildcards to exclude terms (always both ends for title matching)"""
    return [f"%{term}%" for term in exclude_terms]

def load_user_coefficients(engine):
    """Load user coefficients from database and handle NULL values with fallback logic"""
    query = text("""
        SELECT 
            item_monetary,
            item_rating,
            item_recency,
            item_frequency,
            item_search
        FROM user
        WHERE email = :email
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"email": EMAIL}).fetchone()
        
        if not result:
            raise ValueError(f"No user found with email {EMAIL}")
        
        coefficients = {
            'price_percentile': result.item_monetary,
            'rating_percentile': result.item_rating,
            'release_date_percentile': result.item_recency,
            'frequency_percentile': result.item_frequency,
            'search_rank_percentile': result.item_search
        }
        
        # Get non-NULL coefficients for fallback calculations
        non_null_coeffs = {k: v for k, v in coefficients.items() if v is not None}
        
        if not non_null_coeffs:
            raise ValueError("All coefficients are NULL - cannot calculate fallbacks")
        
        # Calculate fallback values
        min_abs_value = min(abs(v) for v in non_null_coeffs.values())
        
        # Handle NULL values with fallback logic
        if coefficients['price_percentile'] is None:
            # Use negative absolute value minimum of existing features / 2
            coefficients['price_percentile'] = -(min_abs_value / 2)
            print(f"⚠️  price_percentile was NULL, using fallback: {coefficients['price_percentile']:.6f}")
        
        if coefficients['search_rank_percentile'] is None:
            # Use negative absolute value minimum of existing features / 2
            coefficients['search_rank_percentile'] = -(min_abs_value / 2)
            print(f"⚠️  search_rank_percentile was NULL, using fallback: {coefficients['search_rank_percentile']:.6f}")
        
        if coefficients['rating_percentile'] is None:
            # Use positive absolute value minimum of existing features / 2
            coefficients['rating_percentile'] = min_abs_value / 2
            print(f"⚠️  rating_percentile was NULL, using fallback: {coefficients['rating_percentile']:.6f}")
        
        if coefficients['frequency_percentile'] is None:
            # Use positive absolute value minimum of existing features / 2
            coefficients['frequency_percentile'] = min_abs_value / 2
            print(f"⚠️  frequency_percentile was NULL, using fallback: {coefficients['frequency_percentile']:.6f}")
        
        # Ensure frequency_percentile is always positive
        if coefficients['frequency_percentile'] < 0:
            coefficients['frequency_percentile'] = min_abs_value / 2
            print(f"⚠️  frequency_percentile was negative, corrected to: {coefficients['frequency_percentile']:.6f}")
        
        if coefficients['release_date_percentile'] is None:
            # Use positive absolute value minimum of existing features / 2
            coefficients['release_date_percentile'] = abs(min_abs_value) / 2
            print(f"⚠️  release_date_percentile was NULL, using fallback: {coefficients['release_date_percentile']:.6f}")
        
        # Ensure release_date_percentile is always positive
        if coefficients['release_date_percentile'] < 0:
            coefficients['release_date_percentile'] = abs(min_abs_value) / 2
            print(f"⚠️  release_date_percentile was negative, corrected to: {coefficients['release_date_percentile']:.6f}")
        
        # Calculate constant to balance to 1
        constant = 1 - sum(coefficients.values())
        
        print(f"\n=== Loaded User Coefficients ===")
        print(f"Price (Monetary):     {coefficients['price_percentile']:>10.6f}")
        print(f"Rating:               {coefficients['rating_percentile']:>10.6f}")
        print(f"Release Date (Recency): {coefficients['release_date_percentile']:>10.6f}")
        print(f"Frequency:            {coefficients['frequency_percentile']:>10.6f}")
        print(f"Search Rank:          {coefficients['search_rank_percentile']:>10.6f}")
        print(f"Constant:             {constant:>10.6f}")
        print()
        
        return coefficients, constant

# Apply wildcards to search terms and exclude terms
SEARCH_TERMS_WITH_WILDCARDS = apply_wildcards(ORIGINAL_SEARCH_TERMS, WILDCARD_MODE)
EXCLUDE_TERMS_WITH_WILDCARDS = apply_exclude_wildcards(EXCLUDE_TERMS)

# === Database Setup ===
engine = reccd_items.mysqlengine()
conn = engine.connect()

# Load user coefficients from database
coefficients, CONSTANT = load_user_coefficients(engine)

# Extract individual weights
MONETARY_WEIGHT = coefficients['price_percentile']
RATING_WEIGHT = coefficients['rating_percentile']
ITEM_RECENCY_WEIGHT = coefficients['release_date_percentile']
ITEM_FREQUENCY_WEIGHT = coefficients['frequency_percentile']
SEARCH_RANK_WEIGHT = coefficients['search_rank_percentile']

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
    
    query = text(f"""
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
    """)

    df = pd.read_sql(query, conn, params=params)

    # ✅ Ensure date columns are properly converted and release_date is min of all 3
    df['listed_date'] = pd.to_datetime(df['listed_date'], errors='coerce')
    df['oldest_review'] = pd.to_datetime(df['oldest_review'], errors='coerce')
    df['release_date'] = pd.to_datetime(
        df[['release_date', 'listed_date', 'oldest_review']].min(axis=1),
        errors='coerce'
    )

    # === Calculate Features ===
    today = datetime.datetime.now()

    df['recency_days'] = np.nan
    df.loc[df['release_date'].notna(), 'recency_days'] = (
        (today - df.loc[df['release_date'].notna(), 'release_date']).dt.days
    )

    not_null_mask = df['release_date'].notna()
    df.loc[not_null_mask, 'release_date_percentile'] = (
        1 - df.loc[not_null_mask, 'recency_days'].rank(pct=True)
    )
    
    # Calculate frequency only for rows with valid recency_days
    df['frequency'] = np.nan
    df.loc[not_null_mask, 'frequency'] = df.loc[not_null_mask, 'ratings_total'] / (df.loc[not_null_mask, 'recency_days'] + 1)
    
    # Calculate frequency_percentile only for rows with valid frequency
    valid_frequency_mask = df['frequency'].notna()
    df.loc[valid_frequency_mask, 'frequency_percentile'] = (
        df.loc[valid_frequency_mask, 'frequency'].rank(pct=True)
    )

    # Set default values for rows without valid dates
    df.loc[~not_null_mask, ['release_date_percentile', 'frequency_percentile']] = 1

    df['price_percentile'] = df['price'].rank(pct=True)
    df['rating_percentile'] = df['rating'].rank(pct=True)
    df['search_rank_percentile'] = df['search_rank'].rank(pct=True)

    df['reccd'] = (
        df['price_percentile'] * MONETARY_WEIGHT +
        df['rating_percentile'] * RATING_WEIGHT +
        df['release_date_percentile'] * ITEM_RECENCY_WEIGHT +
        df['frequency_percentile'] * ITEM_FREQUENCY_WEIGHT +
        df['search_rank_percentile'] * SEARCH_RANK_WEIGHT +
        CONSTANT
    )

    # === Deduplicate by parent_asin ===
    # If multiple variations of the same parent appear (from different searches),
    # keep only the one with the best reccd score, best search_rank, and lowest price
    # BUT: Only deduplicate items that have an actual parent_asin (not NULL/None)
    # Standalone products (parent_asin is NULL) are kept as-is
    
    # Separate items with parent_asin from standalone items
    # Items with parent_asin are variations that should be deduplicated
    # Items without parent_asin are standalone products (keep all)
    has_parent = df['parent_asin'].notna() & (df['parent_asin'] != '')
    items_with_parent = df[has_parent].copy()
    standalone_items = df[~has_parent].copy()
    
    if len(items_with_parent) > 0:
        # Prioritize items with complete data (ratings_total not NaN)
        # Sort by: parent_asin, has_ratings (desc to put True first), reccd (desc), search_rank (asc), price (asc)
        items_with_parent['has_ratings'] = items_with_parent['ratings_total'].notna()
        items_with_parent = items_with_parent.sort_values(
            ['parent_asin', 'has_ratings', 'reccd', 'search_rank', 'price'], 
            ascending=[True, False, False, True, True]
        )
        
        # Keep first occurrence of each parent (has ratings, best reccd, best rank, best price)
        items_with_parent = items_with_parent.drop_duplicates(
            subset=['parent_asin'], 
            keep='first'
        )
        
        # Drop the temporary column
        items_with_parent = items_with_parent.drop(columns=['has_ratings'])
    
    # Combine deduplicated parent items with standalone items
    df = pd.concat([items_with_parent, standalone_items], ignore_index=True)
    
    # Now sort by reccd for final recommendations
    df = df.sort_values('reccd', ascending=False).reset_index(drop=True)

    df['clean_link'] = df['link'].str.replace(r'\?tag=.*$', '', regex=True)

    if df['ratings_total'].notna().all():
        df['ratings_total'] = df['ratings_total'].astype(int)

    df['release_date'] = df['release_date'].dt.strftime('%Y-%m-%d')
    df['listed_date'] = df['listed_date'].dt.strftime('%Y-%m-%d')
    df['oldest_review'] = df['oldest_review'].dt.strftime('%Y-%m-%d')

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
    print(df[['title', 'price', 'rating', 'ratings_total', 'frequency', 'search_rank',
              'listed_date', 'oldest_review', 'release_date', 'reccd', 'asin',
              'clean_link', 'last_update']].head(PRINT_ROWS))
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
                        frequency_percentile, search_rank_percentile, purchase_datetime, search_term, is_relevant, event_type
                    )
                    VALUES (
                        :user_id, :asin, :parent_asin, :title, :price, :rating, :ratings_total, :frequency, :search_rank, :release_date,
                        :reccd_score, :price_percentile, :rating_percentile, :release_date_percentile,
                        :frequency_percentile, :search_rank_percentile, :purchase_datetime, :search_term, :is_relevant, :event_type
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