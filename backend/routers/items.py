#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from fastapi import APIRouter, HTTPException, BackgroundTasks
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
from database import get_db_connection
from sqlalchemy import text
import datetime
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["items"])


def run_full_pipeline_background(search_term: str):
    """
    Background task to run the complete items pipeline.
    This includes:
    1. Search items (Rainforest API)
    2. Get listed dates (Keepa API)  
    3. Get first available dates (Rainforest API)
    4. Run regression to update user coefficients
    5. Calculate recommendations
    
    Takes approximately 5 minutes to complete.
    """
    try:
        logger.info(f"Starting full pipeline for '{search_term}'")
        result = pipeline_service.run_full_pipeline(search_term)
        
        if result['status'] == 'completed':
            logger.info(f"✅ Pipeline completed successfully for '{search_term}'")
        else:
            logger.error(f"❌ Pipeline failed for '{search_term}': {result.get('message')}")
            
    except Exception as e:
        logger.error(f"Pipeline background task failed for '{search_term}': {e}", exc_info=True)


@router.post("/search", response_model=SearchResponse)
async def search_items(request: SearchRequest, background_tasks: BackgroundTasks):
    """
    Initiate a search for items
    
    Always runs the full pipeline to get fresh Amazon rankings:
    - Search Amazon via Rainforest API
    - Get product dates via Keepa API
    - Get additional details via Rainforest API
    - Update ML regression coefficients
    
    Takes approximately 2-3 minutes to complete.
    """
    try:
        # Check if we have existing results to show while pipeline runs
        items, _, _ = recommendation_service.get_recommendations(
            search_term=request.search_term,
            user_id=request.user_id
        )
        
        items_count = len(items)
        
        # Always run pipeline in background to get fresh rankings
        background_tasks.add_task(run_full_pipeline_background, request.search_term)
        logger.info(f"Running full pipeline for '{request.search_term}'")
        
        if items_count > 0:
            # We have old results - show them while refreshing
            return SearchResponse(
                search_term=request.search_term,
                status="refreshing",
                message=f"Showing {items_count} existing results while we fetch the latest rankings. Pull down to refresh in 2-3 minutes.",
                items_found=items_count
            )
        else:
            # No results - need to wait for pipeline
            return SearchResponse(
                search_term=request.search_term,
                status="processing",
                message=f"Analyzing products for '{request.search_term}'... This takes 2-3 minutes. Pull down to check for results.",
                items_found=0
            )
    except Exception as e:
        logger.error(f"Search endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/results", response_model=ResultsResponse)
async def get_results(search_term: str, user_id: int = 1):
    """
    Get personalized recommendations for a search term
    
    Returns items ranked by the Reccd ML algorithm based on user preferences.
    """
    try:
        items, coefficients, constant = recommendation_service.get_recommendations(
            search_term=search_term,
            user_id=user_id
        )
        
        # Convert to Pydantic models
        product_items = [ProductItem(**item) for item in items]
        
        # Format coefficients for response
        coeffs_dict = {
            **coefficients,
            'constant': constant
        }
        
        return ResultsResponse(
            search_term=search_term,
            user_id=user_id,
            total_results=len(product_items),
            items=product_items,
            coefficients=coeffs_dict
        )
    except Exception as e:
        logger.error(f"Results endpoint error: {e}")
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

