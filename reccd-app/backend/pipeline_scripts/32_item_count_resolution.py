#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Resolve item_count and price_per_item for items where item_count_updated_at IS NULL.
Run after Keepa (2) and Rainforest product (3) so raw signals are populated.
"""

import logging
import os
import sys

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
pending = conn.execute(pending_query).scalar() or 0
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

update_query = text("""
    UPDATE items
    SET
        item_count = :item_count,
        item_count_source = :item_count_source,
        price_per_item = :price_per_item,
        title_inferred_item_count = :title_inferred_item_count,
        title_inferred_pattern = :title_inferred_pattern,
        item_count_updated_at = UTC_TIMESTAMP()
    WHERE asin = :asin
""")

total_updated = 0

while True:
    rows = conn.execute(select_query, {"limit": BATCH_SIZE}).mappings().all()
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
