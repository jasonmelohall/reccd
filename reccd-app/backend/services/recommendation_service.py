#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI item recommendations: loads scored rows for the mobile/web app.

Query + scoring mirrors reccd-app/backend/pipeline_scripts/9_reccd_items.py
and items/reccd_items.py (wildcard LIKE on search_term, OR across terms).
"""

import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from config import get_settings
from database import get_db_connection
from shared.reccd_items import (
    ITEMS_IRRELEVANT_EXCLUSION_SQL,
    apply_wildcards,
    dedupe_items_by_parent_asin,
    load_user_coefficients,
    read_items_dataframe,
    score_items_dataframe,
)

logger = logging.getLogger(__name__)
settings = get_settings()


def _fetch_items_for_terms(
    conn,
    search_terms: List[str],
    user_id: int,
    wildcard_mode: str = "both_ends",
) -> pd.DataFrame:
    """Same WHERE clause as 9_reccd_items.py: OR of search_term LIKE wildcards."""
    patterns = apply_wildcards(search_terms, wildcard_mode)
    like_conditions = []
    params = {"user_id": user_id}
    for i, pattern in enumerate(patterns):
        param_name = f"term_{i}"
        like_conditions.append(f"i.search_term LIKE :{param_name}")
        params[param_name] = pattern
    query_str = f"""
        SELECT *
        FROM items i
        WHERE ({' OR '.join(like_conditions)})
        {ITEMS_IRRELEVANT_EXCLUSION_SQL}
    """
    return read_items_dataframe(conn, query_str, params)


def _items_to_api_records(df: pd.DataFrame) -> List[dict]:
    if df is None or df.empty:
        return []
    df = df.copy()
    df["release_date"] = df["release_date"].dt.strftime("%Y-%m-%d")
    df = df.replace([np.nan, np.inf, -np.inf], None)
    items = df.to_dict("records")
    for item in items:
        st = item.get("search_term")
        item["search_terms"] = [st] if st else []
    return items


def get_recommendations(
    search_term: Optional[str] = None,
    search_terms: Optional[List[str]] = None,
    user_id: Optional[int] = None,
    wildcard_mode: str = "both_ends",
) -> Tuple[List[dict], dict, float]:
    """
    Score items in memory for a single search term or GenAI term list.

    Returns (items, coefficients, constant).
    """
    if user_id is None:
        user_id = settings.user_id

    terms = [t for t in (search_terms or []) if t]
    if not terms and search_term:
        terms = [search_term]

    if not terms:
        with get_db_connection() as conn:
            coefficients, constant = load_user_coefficients(
                conn, settings.user_email, defaults_if_missing=True
            )
        return [], coefficients, constant

    with get_db_connection() as conn:
        coefficients, constant = load_user_coefficients(
            conn, settings.user_email, defaults_if_missing=True
        )
        df = _fetch_items_for_terms(conn, terms, user_id, wildcard_mode)

    if df.empty:
        logger.info("No items found for search (term=%s, terms=%s)", search_term, search_terms)
        return [], coefficients, constant

    logger.info("Found %s items for search terms %s", len(df), terms)
    df = score_items_dataframe(df, coefficients, constant, score_column="reccd_score")
    df = dedupe_items_by_parent_asin(df, score_column="reccd_score")
    return _items_to_api_records(df), coefficients, constant


def get_recommendations_with_fallback(
    search_term: Optional[str] = None,
    search_terms: Optional[List[str]] = None,
    user_id: Optional[int] = None,
) -> Tuple[List[dict], dict, float]:
    """Alias for get_recommendations (no suffix fallback; matches items pipeline)."""
    return get_recommendations(
        search_term=search_term,
        search_terms=search_terms,
        user_id=user_id,
    )


class RecommendationService:
    """Thin wrapper so routers can use a module-level singleton."""

    def get_recommendations(self, *args, **kwargs):
        return get_recommendations(*args, **kwargs)

    def get_recommendations_with_fallback(self, *args, **kwargs):
        return get_recommendations_with_fallback(*args, **kwargs)


recommendation_service = RecommendationService()
