#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from sqlalchemy import text
import datetime
import logging
from typing import List, Optional

from database import get_db_connection
from config import get_settings

logger = logging.getLogger(__name__)


def _search_terms_list(stored: Optional[str]) -> List[str]:
    """Split stored search_term (pipe-separated or single) into list for API."""
    if not stored or not isinstance(stored, str):
        return []
    parts = [p.strip() for p in stored.split("|") if p.strip()]
    return parts if parts else [stored]
settings = get_settings()


class RecommendationService:
    def __init__(self):
        self.user_email = settings.user_email
        self.user_id = settings.user_id
    
    def load_user_coefficients(self):
        """Load user coefficients from database and handle NULL values"""
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
        
        with get_db_connection() as conn:
            result = conn.execute(query, {"email": self.user_email}).fetchone()
            
            if not result:
                # Return default coefficients if user not found
                logger.warning(f"No user found with email {self.user_email}, using defaults")
                return {
                    'price_percentile': -0.2,
                    'rating_percentile': 0.3,
                    'release_date_percentile': 0.2,
                    'frequency_percentile': 0.2,
                    'search_rank_percentile': -0.1
                }, 0.4
            
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
                logger.warning("All coefficients are NULL, using defaults")
                return {
                    'price_percentile': -0.2,
                    'rating_percentile': 0.3,
                    'release_date_percentile': 0.2,
                    'frequency_percentile': 0.2,
                    'search_rank_percentile': -0.1
                }, 0.4
            
            # Calculate fallback values
            min_abs_value = min(abs(v) for v in non_null_coeffs.values())
            
            # Handle NULL values with fallback logic
            if coefficients['price_percentile'] is None:
                coefficients['price_percentile'] = -(min_abs_value / 2)
            
            if coefficients['search_rank_percentile'] is None:
                coefficients['search_rank_percentile'] = -(min_abs_value / 2)
            
            if coefficients['rating_percentile'] is None:
                coefficients['rating_percentile'] = min_abs_value / 2
            
            if coefficients['frequency_percentile'] is None:
                coefficients['frequency_percentile'] = min_abs_value / 2
            elif coefficients['frequency_percentile'] < 0:
                coefficients['frequency_percentile'] = min_abs_value / 2
            
            if coefficients['release_date_percentile'] is None:
                coefficients['release_date_percentile'] = abs(min_abs_value) / 2
            elif coefficients['release_date_percentile'] < 0:
                coefficients['release_date_percentile'] = abs(min_abs_value) / 2
            
            # Calculate constant to balance to 1
            constant = 1 - sum(coefficients.values())
            
            return coefficients, constant
    
    def get_recommendations(
        self,
        search_term: Optional[str] = None,
        search_terms: Optional[List[str]] = None,
        user_id: int = None,
        wildcard_mode: str = 'both_ends',
    ):
        """
        Get personalized recommendations for a search term or multiple terms (GenAI).
        
        Args:
            search_term: Single search term (regular mode).
            search_terms: List of terms (GenAI mode); matches items whose stored
                search_term contains any of these (pipe-delimited or single).
            user_id: User ID (defaults to settings.user_id)
            wildcard_mode: For single search_term only ('both_ends', etc.)
            
        Returns:
            Tuple of (list of items with scores, coefficients dict, constant).
            Each item includes 'search_terms' (list) derived by splitting stored search_term on '|'.
        """
        if user_id is None:
            user_id = self.user_id

        use_multi = search_terms and len(search_terms) > 0
        if not use_multi and not search_term:
            coeffs, const = self.load_user_coefficients()
            return [], coeffs, const

        coefficients, constant = self.load_user_coefficients()

        if use_multi:
            # Match rows where stored search_term (pipe-separated or single) contains any term
            # Use CONCAT('|', search_term, '|') LIKE '%|term|%' to avoid substring false positives
            conditions = []
            params = {"user_id": user_id}
            for i, term in enumerate(search_terms):
                key = f"term_{i}"
                params[key] = term
                conditions.append(
                    "CONCAT('|', COALESCE(i.search_term, ''), '|') LIKE CONCAT('%|', :" + key + ", '|%')"
                )
            where_clause = " OR ".join(conditions)
            query_str = """
                SELECT *
                FROM items i
                WHERE (""" + where_clause + """)
                AND NOT EXISTS (
                    SELECT 1
                    FROM items_user u
                    WHERE u.user_id = :user_id
                    AND u.asin = i.asin
                    AND u.is_relevant = 0
                    AND u.search_term = i.search_term
                )
            """
            with get_db_connection() as conn:
                result = conn.execute(text(query_str), params)
                rows = result.fetchall()
                if not rows:
                    df = pd.DataFrame()
                else:
                    df = pd.DataFrame(rows, columns=result.keys())
        else:
            if wildcard_mode == 'both_ends':
                search_pattern = f"%{search_term}%"
            elif wildcard_mode == 'start_only':
                search_pattern = f"%{search_term}"
            elif wildcard_mode == 'end_only':
                search_pattern = f"{search_term}%"
            else:
                search_pattern = search_term

            query = text("""
                SELECT *
                FROM items i
                WHERE i.search_term LIKE :search_term
                AND NOT EXISTS (
                    SELECT 1
                    FROM items_user u
                    WHERE u.user_id = :user_id
                    AND u.asin = i.asin
                    AND u.is_relevant = 0
                    AND u.search_term = i.search_term
                )
            """)
            with get_db_connection() as conn:
                df = pd.read_sql(query, conn, params={
                    "search_term": search_pattern,
                    "user_id": user_id
                })

        if len(df) == 0:
            logger.info("No items found for search (term=%s, terms=%s)", search_term, search_terms)
            return [], coefficients, constant
        logger.info("Found %s items for search", len(df))

        # Convert date columns
        df['listed_date'] = pd.to_datetime(df['listed_date'], errors='coerce')
        df['oldest_review'] = pd.to_datetime(df['oldest_review'], errors='coerce')
        df['release_date'] = pd.to_datetime(
            df[['release_date', 'listed_date', 'oldest_review']].min(axis=1),
            errors='coerce'
        )
        
        # Calculate features (always calculate in memory - scores are relative to current result set)
        today = datetime.datetime.now()
        
        df['recency_days'] = np.nan
        df.loc[df['release_date'].notna(), 'recency_days'] = (
            (today - df.loc[df['release_date'].notna(), 'release_date']).dt.days
        )
        
        not_null_mask = df['release_date'].notna()
        df.loc[not_null_mask, 'release_date_percentile'] = (
            1 - df.loc[not_null_mask, 'recency_days'].rank(pct=True)
        )
        
        # Calculate frequency
        df['frequency'] = np.nan
        df.loc[not_null_mask, 'frequency'] = (
            df.loc[not_null_mask, 'ratings_total'] / (df.loc[not_null_mask, 'recency_days'] + 1)
        )
        
        # Calculate frequency_percentile
        valid_frequency_mask = df['frequency'].notna()
        df.loc[valid_frequency_mask, 'frequency_percentile'] = (
            df.loc[valid_frequency_mask, 'frequency'].rank(pct=True)
        )
        
        # Set default values for rows without valid dates
        df.loc[~not_null_mask, ['release_date_percentile', 'frequency_percentile']] = 1
        
        # Calculate other percentiles
        df['price_percentile'] = df['price'].rank(pct=True)
        df['rating_percentile'] = df['rating'].rank(pct=True)
        df['search_rank_percentile'] = df['search_rank'].rank(pct=True)
        
        # Calculate reccd score (always calculated in memory, relative to current result set)
        df['reccd_score'] = (
            df['price_percentile'] * coefficients['price_percentile'] +
            df['rating_percentile'] * coefficients['rating_percentile'] +
            df['release_date_percentile'] * coefficients['release_date_percentile'] +
            df['frequency_percentile'] * coefficients['frequency_percentile'] +
            df['search_rank_percentile'] * coefficients['search_rank_percentile'] +
            constant
        )
        
        # Deduplicate by parent_asin
        has_parent = df['parent_asin'].notna() & (df['parent_asin'] != '')
        items_with_parent = df[has_parent].copy()
        standalone_items = df[~has_parent].copy()
        
        if len(items_with_parent) > 0:
            items_with_parent['has_ratings'] = items_with_parent['ratings_total'].notna()
            items_with_parent = items_with_parent.sort_values(
                ['parent_asin', 'has_ratings', 'reccd_score', 'search_rank', 'price'],
                ascending=[True, False, False, True, True]
            )
            items_with_parent = items_with_parent.drop_duplicates(
                subset=['parent_asin'],
                keep='first'
            )
            items_with_parent = items_with_parent.drop(columns=['has_ratings'])
        
        # Combine and sort by reccd
        df = pd.concat([items_with_parent, standalone_items], ignore_index=True)
        df = df.sort_values('reccd_score', ascending=False).reset_index(drop=True)
        
        # Convert to dict format for API response
        df['release_date'] = df['release_date'].dt.strftime('%Y-%m-%d')
        
        # Replace NaN and inf values with None for JSON serialization
        df = df.replace([np.nan, np.inf, -np.inf], None)
        
        # Convert to list of dicts and add search_terms (split pipe-separated) for frontend
        items = df.to_dict('records')
        for item in items:
            item['search_terms'] = _search_terms_list(item.get('search_term'))

        return items, coefficients, constant


# Global instance
recommendation_service = RecommendationService()

