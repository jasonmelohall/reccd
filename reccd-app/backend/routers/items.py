#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Optional, Union

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from models.schemas import (
    SearchRequest,
    SearchResponse,
    ResultsResponse,
    ProductItem,
    ClickEventRequest,
)
from services.search_service import search_service
from services.recommendation_service import recommendation_service
from services.pipeline_service import pipeline_service
from services.openai_service import generate_search_terms
from database import get_db_connection
from sqlalchemy import text
import datetime
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["items"])


def _sanitize_search_input(s: str) -> str:
    """Remove/escape chars that can break requests (e.g. double-quote)."""
    if not s or not isinstance(s, str):
        return (s or "").strip()
    return s.replace("\\", "").replace('"', "'").strip()


def run_full_pipeline_background(search_term: Union[str, List[str]]):
    """
    Background task to run the complete items pipeline.
    Accepts a single term (str) or list of terms (GenAI).
    Takes approximately 5 minutes to complete.
    """
    try:
        logger.info("Starting full pipeline for %s", search_term)
        result = pipeline_service.run_full_pipeline(search_term)
        if result['status'] == 'completed':
            logger.info("Pipeline completed successfully")
        else:
            logger.error("Pipeline failed: %s", result.get('message'))
    except Exception as e:
        logger.error("Pipeline background task failed: %s", e, exc_info=True)


@router.post("/search", response_model=SearchResponse)
async def search_items(request: SearchRequest, background_tasks: BackgroundTasks):
    """
    Initiate a search for items. Regular mode: search_term. GenAI mode: genai=True, user_input, num_terms.
    Returns search_terms in response for GenAI so client can show pills on Results.
    """
    try:
        if request.genai and request.user_input:
            # GenAI: generate terms, run pipeline with list, return terms for Results pills
            raw = _sanitize_search_input(request.user_input or "")
            if not raw:
                raise HTTPException(status_code=400, detail="user_input required for GenAI search")
            num_terms = max(1, min(10, request.num_terms))
            search_terms = generate_search_terms(raw, num_terms)
            if not search_terms:
                search_terms = [raw]
            primary = search_terms[0]
            items, _, _ = recommendation_service.get_recommendations(
                search_terms=search_terms,
                user_id=request.user_id
            )
            items_count = len(items)
            background_tasks.add_task(run_full_pipeline_background, search_terms)
            logger.info("GenAI search: running pipeline for %s terms", len(search_terms))
            if items_count > 0:
                return SearchResponse(
                    search_term=primary,
                    status="refreshing",
                    message=f"Showing {items_count} existing results while we fetch the latest. Pull down to refresh in 2-3 minutes.",
                    items_found=items_count,
                    search_terms=search_terms,
                )
            return SearchResponse(
                search_term=primary,
                status="processing",
                message="Analyzing products... This takes 2-3 minutes. Pull down to check for results.",
                items_found=0,
                search_terms=search_terms,
            )
        # Regular search
        st = _sanitize_search_input(request.search_term or "")
        if not st:
            raise HTTPException(status_code=400, detail="search_term required for regular search")
        items, _, _ = recommendation_service.get_recommendations(
            search_term=st,
            user_id=request.user_id
        )
        items_count = len(items)
        background_tasks.add_task(run_full_pipeline_background, st)
        logger.info("Running full pipeline for '%s'", st)
        if items_count > 0:
            return SearchResponse(
                search_term=st,
                status="refreshing",
                message=f"Showing {items_count} existing results while we fetch the latest rankings. Pull down to refresh in 2-3 minutes.",
                items_found=items_count
            )
        return SearchResponse(
            search_term=st,
            status="processing",
            message=f"Analyzing products for '{st}'... This takes 2-3 minutes. Pull down to check for results.",
            items_found=0
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Search endpoint error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/results", response_model=ResultsResponse)
async def get_results(
    search_term: Optional[str] = Query(None),
    search_terms: Optional[List[str]] = Query(None, alias="search_terms"),
    user_id: int = 1,
):
    """
    Get personalized recommendations. Pass search_term (single) or search_terms (multi-term GenAI).
    """
    try:
        if search_terms and len(search_terms) > 0:
            search_terms = [_sanitize_search_input(t) for t in search_terms if t]
            search_terms = [t for t in search_terms if t]
            if not search_terms:
                raise HTTPException(status_code=400, detail="search_terms required")
            items, coefficients, constant = recommendation_service.get_recommendations(
                search_terms=search_terms,
                user_id=user_id
            )
            if len(items) == 0 and len(search_terms) > 0:
                items, coefficients, constant = recommendation_service.get_recommendations(
                    search_term=search_terms[0],
                    user_id=user_id
                )
                logger.info("GET /results search_terms=%s -> 0 items; fallback search_term=%s -> %s items",
                            search_terms, search_terms[0], len(items))
            else:
                logger.info("GET /results search_terms=%s -> %s items", search_terms, len(items))
            primary = search_terms[0]
        else:
            if not search_term:
                raise HTTPException(status_code=400, detail="search_term or search_terms required")
            search_term = _sanitize_search_input(search_term)
            if not search_term:
                raise HTTPException(status_code=400, detail="search_term required")
            items, coefficients, constant = recommendation_service.get_recommendations(
                search_term=search_term,
                user_id=user_id
            )
            logger.info("GET /results search_term=%s -> %s items", search_term, len(items))
            primary = search_term

        product_items = [ProductItem(**item) for item in items]
        coeffs_dict = {**coefficients, 'constant': constant}

        return ResultsResponse(
            search_term=primary,
            user_id=user_id,
            total_results=len(product_items),
            items=product_items,
            coefficients=coeffs_dict,
            search_terms=search_terms if search_terms else None,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Results endpoint error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/click")
async def log_click(event: ClickEventRequest):
    """
    Record a click event from the frontend when a user taps on a product.
    Stores the same fields as purchases with event_type='click'.
    """
    try:
        release_date_dt = None
        if event.release_date:
            try:
                sanitized_date = event.release_date.replace("Z", "+00:00")
                release_date_dt = datetime.datetime.fromisoformat(sanitized_date)
            except ValueError:
                try:
                    release_date_dt = datetime.datetime.strptime(event.release_date, "%Y-%m-%d")
                except ValueError:
                    release_date_dt = None

        now = datetime.datetime.utcnow()

        with get_db_connection() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO items_user (
                        user_id,
                        asin,
                        parent_asin,
                        title,
                        price,
                        rating,
                        ratings_total,
                        frequency,
                        search_rank,
                        release_date,
                        reccd_score,
                        price_percentile,
                        rating_percentile,
                        release_date_percentile,
                        frequency_percentile,
                        search_rank_percentile,
                        purchase_datetime,
                        search_term,
                        is_relevant,
                        event_type
                    )
                    VALUES (
                        :user_id,
                        :asin,
                        :parent_asin,
                        :title,
                        :price,
                        :rating,
                        :ratings_total,
                        :frequency,
                        :search_rank,
                        :release_date,
                        :reccd_score,
                        :price_percentile,
                        :rating_percentile,
                        :release_date_percentile,
                        :frequency_percentile,
                        :search_rank_percentile,
                        :purchase_datetime,
                        :search_term,
                        :is_relevant,
                        'click'
                    )
                    """
                ),
                {
                    "user_id": event.user_id,
                    "asin": event.asin,
                    "parent_asin": event.parent_asin,
                    "title": event.title,
                    "price": event.price,
                    "rating": event.rating,
                    "ratings_total": event.ratings_total,
                    "frequency": event.frequency,
                    "search_rank": event.search_rank,
                    "release_date": release_date_dt,
                    "reccd_score": event.reccd_score,
                    "price_percentile": event.price_percentile,
                    "rating_percentile": event.rating_percentile,
                    "release_date_percentile": event.release_date_percentile,
                    "frequency_percentile": event.frequency_percentile,
                    "search_rank_percentile": event.search_rank_percentile,
                    "purchase_datetime": now,
                    "search_term": event.search_term,
                    "is_relevant": event.is_relevant,
                },
            )
            conn.commit()

        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Click logging error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to record click event")

