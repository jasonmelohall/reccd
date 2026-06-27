#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import logging
from database import get_db_connection
from config import get_settings
from shared.reccd_items import (
    get_parent_asin_from_rainforest,
    consolidate_parent_items,
    save_rainforest_search_batch,
)

logger = logging.getLogger(__name__)
settings = get_settings()


class SearchService:
    def __init__(self):
        self.api_key = settings.rainforest_api_key
        self.associate_tag = settings.amazon_associate_tag
    
    def search_amazon(self, query: str, page: int = 1):
        """Search Amazon via Rainforest API"""
        logger.info(f"Searching Amazon for: {query} (page {page})")
        url = "https://api.rainforestapi.com/request"
        params = {
            "api_key": self.api_key,
            "type": "search",
            "amazon_domain": "amazon.com",
            "search_term": query,
            "page": page
        }
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            logger.info("Successfully fetched search results.")
            return response.json()
        else:
            logger.error(f"Error: {response.status_code} - {response.text}")
            return None
    
    def extract_item_data(self, item, search_term: str, search_rank: int):
        """Extract item data including parent ASIN and image URL"""
        child_asin = item.get('asin')
        title = item.get('title')
        price = item.get('price', {}).get('value') if item.get('price') else None
        rating = item.get('rating')
        ratings_total = item.get('ratings_total')
        image_url = item.get('image')  # Extract image URL from Rainforest response
        
        # Extract parent ASIN - if no parent exists, use child ASIN as parent
        parent_asin = get_parent_asin_from_rainforest(item) or child_asin
        
        if not child_asin or not title:
            return None
        
        return {
            'asin': child_asin,
            'parent_asin': parent_asin,
            'title': title,
            'price': price,
            'rating': rating,
            'ratings_total': ratings_total,
            'search_term': search_term,
            'search_rank': search_rank,
            'image_url': image_url
        }
    
    def save_items_batch(self, consolidated_items):
        """Save consolidated parent items via shared items/11 upsert."""
        with get_db_connection() as conn:
            save_rainforest_search_batch(
                conn,
                consolidated_items,
                associate_tag=self.associate_tag,
            )
            saved_count = len(consolidated_items)
            logger.info("Saved %s parent items to database", saved_count)
            return saved_count
    
    def perform_search(self, search_term: str, max_pages: int = 2):
        """
        Perform full search workflow: API call -> consolidate -> save to DB
        
        Args:
            search_term: Search query
            max_pages: Number of pages to fetch (default: 2)
            
        Returns:
            Number of items saved
        """
        all_items_global = {}
        
        for page in range(1, max_pages + 1):
            data = self.search_amazon(search_term, page=page)
            if data:
                results = data.get('search_results', [])
                for idx, item in enumerate(results):
                    search_rank = (page - 1) * len(results) + idx + 1
                    
                    item_data = self.extract_item_data(item, search_term, search_rank)
                    if item_data:
                        child_asin = item_data['asin']
                        # If this ASIN already exists, keep the better one
                        if child_asin in all_items_global:
                            existing = all_items_global[child_asin]
                            if search_rank < existing['search_rank']:
                                all_items_global[child_asin] = item_data
                        else:
                            all_items_global[child_asin] = item_data
        
        # Consolidate by parent ASIN
        logger.info(f"Collected {len(all_items_global)} unique items, consolidating by parent ASIN...")
        consolidated = consolidate_parent_items(all_items_global)
        logger.info(f"Consolidated to {len(consolidated)} parent items")
        
        # Save to database
        items_saved = self.save_items_batch(consolidated)
        
        return items_saved


# Global instance
search_service = SearchService()



