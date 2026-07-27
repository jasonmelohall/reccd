#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline service that orchestrates running the full items workflow:
1. Search items (Rainforest API)
2. Get listed dates (Keepa API)
3. Get first available dates (Rainforest API)
4. Resolve item counts
5. Run regression to update coefficients
6. Generate recommendations
"""

import json
import logging
import os
import subprocess
import sys
import threading
from typing import List, Union

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_scripts")
SHARED_DIR = os.path.join(BASE_DIR, "shared")

# Scripts that may fail without blocking ingest (search rows already in DB).
_NON_FATAL_SCRIPTS = frozenset({"32_item_count_resolution.py"})

logger.info("Pipeline base directory: %s", PIPELINE_DIR)


class PipelineService:
    def __init__(self):
        self._lock = threading.Lock()
        self.scripts = [
            "1_search_items_rainforest.py",
            "2_items_get_listed_date_keepa.py",
            "3_get_first_available_rainforest.py",
            "32_item_count_resolution.py",
            "8_regression_items.py",
            "9_reccd_items.py",
        ]
        self.script_timeouts = {
            "2_items_get_listed_date_keepa.py": 1200,
            "32_item_count_resolution.py": 600,
        }

    def run_full_pipeline(self, search_term: Union[str, List[str]]):
        search_terms = [search_term] if isinstance(search_term, str) else list(search_term)
        logger.info("Starting full pipeline for search term(s): %s", search_terms)

        with self._lock:
            child_env = os.environ.copy()
            child_env["RECCD_PIPELINE_SEARCH_TERMS"] = json.dumps(search_terms)

            for script in self.scripts:
                script_path = os.path.join(PIPELINE_DIR, script)
                logger.info("Running %s...", script)

                try:
                    timeout = self.script_timeouts.get(script, 600)
                    result = subprocess.run(
                        [sys.executable, script_path],
                        cwd=PIPELINE_DIR,
                        capture_output=True,
                        text=True,
                        timeout=timeout,
                        env=child_env,
                    )

                    if result.returncode == 0:
                        logger.info("%s completed successfully", script)
                        continue

                    err_tail = (result.stderr or result.stdout or "")[-2000:]
                    logger.error(
                        "%s failed (code %s): %s",
                        script,
                        result.returncode,
                        err_tail,
                    )

                    if script in _NON_FATAL_SCRIPTS:
                        logger.warning(
                            "Continuing pipeline after non-fatal failure in %s",
                            script,
                        )
                        continue

                    return {
                        "status": "error",
                        "message": f"Pipeline failed at {script}",
                        "error": err_tail,
                    }

                except subprocess.TimeoutExpired:
                    logger.error("%s timed out after %ss", script, timeout)
                    if script in _NON_FATAL_SCRIPTS:
                        continue
                    return {
                        "status": "error",
                        "message": f"Pipeline timed out at {script}",
                    }
                except Exception as exc:
                    logger.error("%s raised exception: %s", script, exc, exc_info=True)
                    if script in _NON_FATAL_SCRIPTS:
                        continue
                    return {
                        "status": "error",
                        "message": f"Pipeline error at {script}",
                        "error": str(exc),
                    }

        logger.info("Full pipeline completed for %s", search_terms)
        return {
            "status": "completed",
            "message": f"Pipeline completed for {len(search_terms)} term(s)",
        }


pipeline_service = PipelineService()
