#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI item recommendations: loads scored rows for the mobile/web app.

Ranking helpers live in shared/reccd_items.py (mirrors items/reccd_items.py).
This module only wires DB access, API-specific queries, and response shaping.
"""

import logging
import json
import os
import time
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from config import get_settings
from database import get_db_connection
from shared.reccd_items import (
    ITEMS_IRRELEVANT_EXCLUSION_SQL,
    dedupe_items_by_parent_asin,
    load_user_coefficients,
    read_items_dataframe,
    score_items_dataframe,
    search_pattern_for_term,
)

logger = logging.getLogger(__name__)
settings = get_settings()

# #region agent log
_DEBUG_LOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    ".cursor",
    "debug-f9f258.log",
)


def _agent_log(hypothesis_id: str, location: str, message: str, data: dict | None = None):
    try:
        payload = {
            "sessionId": "f9f258",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload) + "\n")
    except OSError:
        pass


# #endregion


def _search_term_suffix_fallbacks(term: str, min_words: int = 1) -> List[str]:
    """Drop leading words to find related cached results (e.g. 'shower exfoliator' -> 'exfoliator')."""
    words = (term or "").split()
    if len(words) <= min_words:
        return []
    return [" ".join(words[i:]) for i in range(1, len(words) - min_words + 1)]


def _fetch_items_genai_wildcard(
    conn,
    search_terms: List[str],
    user_id: int,
    wildcard_mode: str = "both_ends",
) -> pd.DataFrame:
    like_conditions = [
        f"i.search_term LIKE :term_{i}" for i in range(len(search_terms))
    ]
    params = {"user_id": user_id}
    for i, term in enumerate(search_terms):
        params[f"term_{i}"] = search_pattern_for_term(term, wildcard_mode)
    query_str = f"""
        SELECT *
        FROM items i
        WHERE ({' OR '.join(like_conditions)})
        {ITEMS_IRRELEVANT_EXCLUSION_SQL}
    """
    return read_items_dataframe(conn, query_str, params)


def _fetch_items_like(conn, search_pattern: str, user_id: int) -> pd.DataFrame:
    query_str = f"""
        SELECT *
        FROM items i
        WHERE i.search_term LIKE :search_term
        {ITEMS_IRRELEVANT_EXCLUSION_SQL}
    """
    return read_items_dataframe(
        conn,
        query_str,
        {"search_term": search_pattern, "user_id": user_id},
    )


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

    use_multi = bool(search_terms)
    if not use_multi and not search_term:
        with get_db_connection() as conn:
            coefficients, constant = load_user_coefficients(
                conn, settings.user_email, defaults_if_missing=True
            )
        return [], coefficients, constant

    with get_db_connection() as conn:
        coefficients, constant = load_user_coefficients(
            conn, settings.user_email, defaults_if_missing=True
        )
        if use_multi:
            df = _fetch_items_genai_wildcard(conn, search_terms, user_id, wildcard_mode)
        else:
            pattern = search_pattern_for_term(search_term, wildcard_mode)
            df = _fetch_items_like(conn, pattern, user_id)

    if df.empty:
        logger.info("No items found for search (term=%s, terms=%s)", search_term, search_terms)
        return [], coefficients, constant

    logger.info("Found %s items for search", len(df))
    df = score_items_dataframe(df, coefficients, constant, score_column="reccd_score")
    df = dedupe_items_by_parent_asin(df, score_column="reccd_score")
    return _items_to_api_records(df), coefficients, constant


def get_recommendations_with_fallback(
    search_term: Optional[str] = None,
    search_terms: Optional[List[str]] = None,
    user_id: Optional[int] = None,
) -> Tuple[List[dict], dict, float]:
    """Try exact match first, then progressively shorter suffixes (regular search only)."""
    items, coefficients, constant = get_recommendations(
        search_term=search_term,
        search_terms=search_terms,
        user_id=user_id,
    )
    if items:
        return items, coefficients, constant

    # Suffix fallbacks are for single-term search only; GenAI lists must match those terms.
    if search_terms:
        return [], coefficients, constant

    terms_to_try = list(search_terms) if search_terms else []
    if search_term and search_term not in terms_to_try:
        terms_to_try.append(search_term)

    # #region agent log
    fallbacks_by_term = {t: _search_term_suffix_fallbacks(t) for t in terms_to_try}
    _agent_log(
        "H4",
        "recommendation_service.get_recommendations_with_fallback",
        "primary_miss_trying_fallbacks",
        {"terms_to_try": terms_to_try, "fallbacks_by_term": fallbacks_by_term},
    )
    # #endregion

    for term in terms_to_try:
        for fallback in _search_term_suffix_fallbacks(term):
            items, coefficients, constant = get_recommendations(
                search_term=fallback,
                user_id=user_id,
            )
            if items:
                logger.info(
                    "Fallback %r -> %r returned %s items",
                    term,
                    fallback,
                    len(items),
                )
                # #region agent log
                _agent_log(
                    "H4",
                    "recommendation_service.get_recommendations_with_fallback",
                    "fallback_hit",
                    {"term": term, "fallback": fallback, "count": len(items)},
                )
                # #endregion
                return items, coefficients, constant

    # #region agent log
    _agent_log(
        "H4",
        "recommendation_service.get_recommendations_with_fallback",
        "no_results_after_fallbacks",
        {"terms_to_try": terms_to_try},
    )
    # #endregion
    return [], coefficients, constant


class RecommendationService:
    """Thin wrapper so routers can use a module-level singleton."""

    def get_recommendations(self, *args, **kwargs):
        return get_recommendations(*args, **kwargs)

    def get_recommendations_with_fallback(self, *args, **kwargs):
        return get_recommendations_with_fallback(*args, **kwargs)


recommendation_service = RecommendationService()
