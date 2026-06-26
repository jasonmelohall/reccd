#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Resolve item_count and price_per_item for items where item_count_updated_at IS NULL.
Run after Keepa (2) and Rainforest product (3) so raw signals are populated.
"""

import logging
import os
import sys
import json

from sqlalchemy import text

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DIR = os.path.join(BASE_DIR, "shared")
sys.path.insert(0, SHARED_DIR)

import reccd_items
from reccd_items import merge_item_count_signals, mysqlengine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 500

print()
engine = mysqlengine()
conn = engine.connect()

pending_query = text("""
    SELECT COUNT(*) AS n
    FROM items
    WHERE item_count_updated_at IS NULL
""")
pending_params = {}
terms_raw = os.environ.get("RECCD_PIPELINE_SEARCH_TERMS")
pipeline_terms = None
if terms_raw:
    try:
        pipeline_terms = [
            str(t).strip() for t in json.loads(terms_raw) if str(t).strip()
        ]
    except json.JSONDecodeError:
        pipeline_terms = None

if pipeline_terms:
    term_clauses = " OR ".join(
        f"search_term = :term_{i}" for i in range(len(pipeline_terms))
    )
    pending_query = text(f"""
        SELECT COUNT(*) AS n
        FROM items
        WHERE item_count_updated_at IS NULL
        AND ({term_clauses})
    """)
    for i, term in enumerate(pipeline_terms):
        pending_params[f"term_{i}"] = term

pending = conn.execute(pending_query, pending_params).scalar() or 0
logger.info("Items pending item_count resolution: %s", pending)

if pending == 0:
    logger.info("Nothing to resolve.")
    conn.close()
    print()
    raise SystemExit(0)

select_query = text("""
    SELECT
        asin,
        title,
        price,
        keepa_number_of_items,
        keepa_package_quantity,
        rainforest_unit_price_json
    FROM items
    WHERE item_count_updated_at IS NULL
    LIMIT :limit
""")
select_params = {"limit": BATCH_SIZE}

if pipeline_terms:
    term_clauses = " OR ".join(
        f"search_term = :sel_term_{i}" for i in range(len(pipeline_terms))
    )
    select_query = text(f"""
        SELECT
            asin,
            title,
            price,
            keepa_number_of_items,
            keepa_package_quantity,
            rainforest_unit_price_json
        FROM items
        WHERE item_count_updated_at IS NULL
        AND ({term_clauses})
        LIMIT :limit
    """)
    for i, term in enumerate(pipeline_terms):
        select_params[f"sel_term_{i}"] = term

update_query = text("""
    UPDATE items
    SET
        item_count = :item_count,
        count_type = :count_type,
        item_count_source = :item_count_source,
        price_per_item = :price_per_item,
        title_inferred_item_count = :title_inferred_item_count,
        title_inferred_count_type = :title_inferred_count_type,
        title_inferred_pattern = :title_inferred_pattern,
        item_count_updated_at = UTC_TIMESTAMP()
    WHERE asin = :asin
""")

total_updated = 0

while True:
    rows = conn.execute(select_query, select_params).mappings().all()
    if not rows:
        break

    for row in rows:
        merged = merge_item_count_signals(
            title=row["title"],
            price=row["price"],
            keepa_number_of_items=row["keepa_number_of_items"],
            keepa_package_quantity=row["keepa_package_quantity"],
            rainforest_unit_price_json=row["rainforest_unit_price_json"],
        )
        conn.execute(
            update_query,
            {
                "asin": row["asin"],
                **merged,
            },
        )
        total_updated += 1

    conn.commit()
    logger.info("Resolved batch size=%s (total %s)", len(rows), total_updated)

conn.close()
logger.info("Done. Resolved %s items.", total_updated)
print()
