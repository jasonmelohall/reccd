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
    "debug-84fc74.log",
)


def _agent_log(hypothesis_id: str, location: str, message: str, data: dict | None = None):
    try:
        payload = {
            "sessionId": "84fc74",
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


def _term_matches_pill(stored: str, pill: str) -> bool:
    """Bidirectional substring match (mirrors ResultsScreen termMatchesPill)."""
    if not stored or not pill:
        return False
    s, p = str(stored).strip().lower(), str(pill).strip().lower()
    return s == p or s in p or p in s


GENAI_MODIFIER_STOP = frozenset(
    {
        "sustainable",
        "durable",
        "eco-friendly",
        "eco",
        "friendly",
        "natural",
        "organic",
        "long-lasting",
        "long",
        "lasting",
        "the",
        "a",
        "an",
        "and",
    }
)
GENAI_GENERIC_PRODUCT_WORDS = frozenset(
    {"exfoliator", "exfoliating", "sponge", "scrubber", "scrub", "body"}
)
LIP_CATEGORY_MARKERS = frozenset({"lip", "lips", "lipstick"})


def _lip_category_mismatch(combined_lower: str, pill_lower: str) -> bool:
    """Exclude lip/cosmetic items when GenAI pills target shower/body."""
    if not any(marker in combined_lower for marker in LIP_CATEGORY_MARKERS):
        return False
    if "shower" in pill_lower or "body" in pill_lower:
        return "shower" not in combined_lower and "body" not in combined_lower
    return False


def _pill_content_tokens(pill: str) -> List[str]:
    return [
        w
        for w in pill.strip().lower().split()
        if w not in GENAI_MODIFIER_STOP and len(w) > 2
    ]


def _item_matches_genai_pill(
    item: dict,
    pill: str,
    *,
    query_term: Optional[str] = None,
) -> bool:
    stored = (item.get("search_term") or "").strip()
    title = (item.get("title") or "").strip()
    combined = f"{stored} {title}".lower()
    pill_lower = pill.strip().lower()

    if stored and _term_matches_pill(stored, pill_lower):
        return not _lip_category_mismatch(combined, pill_lower)

    if not query_term or not _term_matches_pill(query_term, pill_lower):
        return False

    tokens = _pill_content_tokens(pill)
    if not tokens:
        return False
    distinctive = [t for t in tokens if t not in GENAI_GENERIC_PRODUCT_WORDS]
    check_tokens = distinctive if distinctive else tokens
    if not any(token in combined for token in check_tokens):
        return False
    return not _lip_category_mismatch(combined, pill_lower)


def _filter_items_matching_genai_terms(
    items: List[dict],
    search_terms: List[str],
    *,
    query_term: Optional[str] = None,
) -> List[dict]:
    if not items or not search_terms:
        return []
    matched: List[dict] = []
    for item in items:
        for pill in search_terms:
            if _item_matches_genai_pill(item, pill, query_term=query_term):
                out = dict(item)
                label = out.get("search_term") or pill
                out["search_term"] = label
                out["search_terms"] = [label]
                matched.append(out)
                break
    return matched


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
    like_conditions = []
    params = {"user_id": user_id}
    for i, term in enumerate(search_terms):
        like_conditions.append(
            f"(i.search_term LIKE :term_{i} OR :raw_term_{i} LIKE CONCAT('%', i.search_term, '%'))"
        )
        params[f"term_{i}"] = search_pattern_for_term(term, wildcard_mode)
        params[f"raw_term_{i}"] = term
    query_str = f"""
        SELECT *
        FROM items i
        WHERE ({' OR '.join(like_conditions)})
        {ITEMS_IRRELEVANT_EXCLUSION_SQL}
    """
    # #region agent log
    _agent_log(
        "H1",
        "recommendation_service._fetch_items_genai_wildcard",
        "genai_query",
        {"terms": search_terms, "patterns": {f"term_{i}": params[f"term_{i}"] for i in range(len(search_terms))}},
    )
    # #endregion
    df = read_items_dataframe(conn, query_str, params)
    # #region agent log
    _agent_log(
        "H1",
        "recommendation_service._fetch_items_genai_wildcard",
        "genai_query_result",
        {"row_count": len(df), "sample_terms": df["search_term"].head(5).tolist() if not df.empty and "search_term" in df.columns else []},
    )
    # #endregion
    return df


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
        # #region agent log
        _agent_log(
            "H1",
            "recommendation_service.get_recommendations",
            "empty_result",
            {"search_term": search_term, "search_terms": search_terms, "use_multi": use_multi},
        )
        # #endregion
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

    if search_terms:
        # Suffix fallbacks for GenAI, but keep only items whose stored term matches a pill.
        for term in search_terms:
            for fallback in _search_term_suffix_fallbacks(term):
                fallback_items, coefficients, constant = get_recommendations(
                    search_term=fallback,
                    user_id=user_id,
                )
                filtered = _filter_items_matching_genai_terms(
                    fallback_items, search_terms, query_term=fallback
                )
                sample_stored = [
                    i.get("search_term") for i in fallback_items[:5]
                ]
                # #region agent log
                _agent_log(
                    "H4",
                    "recommendation_service.get_recommendations_with_fallback",
                    "genai_fallback_filter",
                    {
                        "term": term,
                        "fallback": fallback,
                        "raw_count": len(fallback_items),
                        "filtered_count": len(filtered),
                        "sample_stored_terms": sample_stored,
                        "sample_titles": [
                            (i.get("title") or "")[:60] for i in fallback_items[:3]
                        ],
                    },
                )
                # #endregion
                if filtered:
                    logger.info(
                        "GenAI fallback %r -> %r returned %s items after pill filter",
                        term,
                        fallback,
                        len(filtered),
                    )
                    return filtered, coefficients, constant
        # #region agent log
        _agent_log(
            "H1",
            "recommendation_service.get_recommendations_with_fallback",
            "genai_no_results",
            {"search_terms": search_terms},
        )
        # #endregion
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
