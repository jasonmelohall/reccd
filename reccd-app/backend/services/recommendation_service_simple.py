#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple recommendation service that just queries pre-computed results.
Run your existing Python scripts (1_search, 9_reccd) to populate data,
then this API just returns what's already in the database.
"""

import pandas as pd
import numpy as np
from sqlalchemy import text
from database import get_db_connection
from config import get_settings
import logging

logger = logging.getLogger(__name__)
settings = get_settings()


class SimpleRecommendationService:
    def __init__(self):
        self.user_id = settings.user_id
    
    def get_recent_results(self, search_term: str, limit: int = 100):
        """
        Get items for a search term - no computation, just query database
        Returns items sorted by rating * ratings_total (popularity)
        """
        search_pattern = f"%{search_term}%"
        
        query_str = """
            SELECT 
                asin,
                parent_asin,
                title,
                link,
                image_url,
                price,
                rating,
                ratings_total,
                search_rank,
                release_date,
                last_update
            FROM items
            WHERE search_term LIKE :search_term
            AND title IS NOT NULL
            ORDER BY (rating * COALESCE(ratings_total, 0)) DESC
            LIMIT :limit
        """
        stmt = text(query_str).bindparams(search_term=search_pattern, limit=limit)
        with get_db_connection() as conn:
            result = conn.execute(stmt)
            rows = result.fetchall()
            columns = result.keys()
        df = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame()
        
        if len(df) == 0:
            logger.info(f"No items found for search term: {search_term}")
            return []
        
        # Replace NaN with None for JSON
        df = df.replace([np.nan, np.inf, -np.inf], None)
        
        # Format dates
        if 'release_date' in df.columns:
            df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
            df['release_date'] = df['release_date'].dt.strftime('%Y-%m-%d')
            df['release_date'] = df['release_date'].replace('NaT', None)
        
        items = df.to_dict('records')
        return items


# Global instance
simple_recommendation_service = SimpleRecommendationService()

