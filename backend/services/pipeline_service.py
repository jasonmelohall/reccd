#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline service that orchestrates running the full items workflow:
1. Search items (Rainforest API)
2. Get listed dates (Keepa API)
3. Get first available dates (Rainforest API)
4. Run regression to update coefficients
5. Generate recommendations

This runs the equivalent of 0_items_full.py
"""

import subprocess
import logging
import os
import sys

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_scripts")
SHARED_DIR = os.path.join(BASE_DIR, "shared")
RECCD_ITEMS_PATH = os.path.join(SHARED_DIR, "reccd_items.py")

logger.info("Pipeline base directory: %s", PIPELINE_DIR)


class PipelineService:
    def __init__(self):
        self.scripts = [
            "1_search_items_rainforest.py",
            "2_items_get_listed_date_keepa.py",
            "3_get_first_available_rainforest.py",
            "8_regression_items.py",
            "9_reccd_items.py"
        ]
    
    def run_full_pipeline(self, search_term: str):
        """
        Run the full items pipeline for a search term.
        This will take approximately 5 minutes.
        
        Args:
            search_term: The search term to process
            
        Returns:
            dict with status and any error messages
        """
        logger.info(f"Starting full pipeline for search term: '{search_term}'")
        
        # Update the search term in reccd_items.py temporarily
        # (This is a simple approach - in production you might want to pass it as env var)
        original_search_terms = self._get_current_search_terms()
        self._update_search_terms([search_term])
        
        try:
            for script in self.scripts:
                script_path = os.path.join(PIPELINE_DIR, script)
                logger.info(f"Running {script}...")
                
                try:
                    result = subprocess.run(
                        ["python", script_path],
                        cwd=PIPELINE_DIR,
                        capture_output=True,
                        text=True,
                        timeout=600  # 10 minute timeout per script
                    )
                    
                    if result.returncode == 0:
                        logger.info(f"✅ {script} completed successfully")
                    else:
                        logger.error(f"❌ {script} failed with return code {result.returncode}")
                        logger.error(f"stderr: {result.stderr}")
                        return {
                            "status": "error",
                            "message": f"Pipeline failed at {script}",
                            "error": result.stderr
                        }
                
                except subprocess.TimeoutExpired:
                    logger.error(f"❌ {script} timed out")
                    return {
                        "status": "error",
                        "message": f"Pipeline timed out at {script}"
                    }
                except Exception as e:
                    logger.error(f"❌ {script} raised exception: {e}")
                    return {
                        "status": "error",
                        "message": f"Pipeline error at {script}",
                        "error": str(e)
                    }
            
            logger.info(f"✅ Full pipeline completed successfully for '{search_term}'")
            return {
                "status": "completed",
                "message": f"Pipeline completed successfully for '{search_term}'"
            }
        
        finally:
            # Restore original search terms
            self._update_search_terms(original_search_terms)
    
    def _get_current_search_terms(self):
        """Read current search terms from shared reccd_items.py"""
        if not os.path.exists(RECCD_ITEMS_PATH):
            logger.warning("reccd_items file not found at %s", RECCD_ITEMS_PATH)
            return []
        
        sys.path.insert(0, SHARED_DIR)
        try:
            import importlib
            import reccd_items
            importlib.reload(reccd_items)
            return reccd_items.get_search_term()
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Could not read current search terms: %s", exc)
            return []
        finally:
            if SHARED_DIR in sys.path:
                sys.path.remove(SHARED_DIR)
    
    def _update_search_terms(self, search_terms):
        """Update search terms in reccd_items.py"""
        if not os.path.exists(RECCD_ITEMS_PATH):
            raise FileNotFoundError(f"reccd_items.py not found at {RECCD_ITEMS_PATH}")
        
        try:
            with open(RECCD_ITEMS_PATH, 'r', encoding="utf-8") as file_handle:
                content = file_handle.read()
            
            # Find and replace the list_amazon_search_terms
            import re
            pattern = r"list_amazon_search_terms = \[([^\]]*)\]"
            
            # Format the new search terms as a Python list string
            if isinstance(search_terms, str):
                search_terms = [search_terms]
            
            terms_str = ",\n        ".join([f"'{term}'" for term in search_terms])
            replacement = f"list_amazon_search_terms = [\n        {terms_str},\n    ]"
            
            new_content = re.sub(pattern, replacement, content)
            
            with open(RECCD_ITEMS_PATH, 'w', encoding="utf-8") as file_handle:
                file_handle.write(new_content)
            
            logger.info(f"Updated search terms in reccd_items.py to: {search_terms}")
            
        except Exception as e:
            logger.error(f"Failed to update search terms: {e}")
            raise


# Global instance
pipeline_service = PipelineService()



