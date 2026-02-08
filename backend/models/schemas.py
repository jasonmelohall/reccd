#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class SearchRequest(BaseModel):
    """Regular search: search_term. GenAI search: genai=True, user_input, num_terms."""
    search_term: Optional[str] = Field(None, min_length=1, max_length=200)
    user_id: int = Field(default=1, ge=1)
    genai: bool = False
    user_input: Optional[str] = Field(None, max_length=2000)
    num_terms: int = Field(default=3, ge=1, le=10)


class SearchResponse(BaseModel):
    search_term: str
    status: str
    message: str
    items_found: Optional[int] = None
    search_terms: Optional[List[str]] = None


class ProductItem(BaseModel):
    asin: str
    parent_asin: Optional[str] = None
    title: Optional[str] = None  # Allow NULL titles
    link: Optional[str] = None  # Allow NULL links
    image_url: Optional[str] = None
    price: Optional[float] = None
    rating: Optional[float] = None
    ratings_total: Optional[int] = None
    search_rank: Optional[int] = None
    release_date: Optional[str] = None
    reccd_score: Optional[float] = None
    price_percentile: Optional[float] = None
    rating_percentile: Optional[float] = None
    release_date_percentile: Optional[float] = None
    frequency_percentile: Optional[float] = None
    search_rank_percentile: Optional[float] = None
    frequency: Optional[float] = None
    last_update: Optional[datetime] = None
    search_terms: Optional[List[str]] = None  # For GenAI: terms that found this item (split from pipe)


class ResultsResponse(BaseModel):
    search_term: str
    user_id: int
    total_results: int
    items: List[ProductItem]
    coefficients: Optional[dict] = None
    search_terms: Optional[List[str]] = None  # When multi-term request


class HealthResponse(BaseModel):
    status: str
    database: str
    timestamp: datetime


class ClickEventRequest(BaseModel):
    user_id: int
    asin: str
    parent_asin: Optional[str] = None
    title: Optional[str] = None
    price: Optional[float] = None
    rating: Optional[float] = None
    ratings_total: Optional[int] = None
    frequency: Optional[float] = None
    search_rank: Optional[int] = None
    release_date: Optional[str] = None  # ISO format string
    reccd_score: Optional[float] = None
    price_percentile: Optional[float] = None
    rating_percentile: Optional[float] = None
    release_date_percentile: Optional[float] = None
    frequency_percentile: Optional[float] = None
    search_rank_percentile: Optional[float] = None
    search_term: Optional[str] = None
    is_relevant: Optional[bool] = True

