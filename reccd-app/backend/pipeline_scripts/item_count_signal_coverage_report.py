#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Print coverage stats for item_count signals (not part of the pipeline).
Run manually when you want to see data availability across Keepa, Rainforest, and title.
"""

import argparse
import os
import sys

from sqlalchemy import text

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DIR = os.path.join(BASE_DIR, "shared")
sys.path.insert(0, SHARED_DIR)

from reccd_items import mysqlengine

print()


def main():
    ap = argparse.ArgumentParser(description="Item count signal coverage report")
    ap.add_argument(
        "--since-days",
        type=int,
        default=None,
        help="Only count items touched in the last N days (any of keepa/rainforest/item_count timestamps)",
    )
    args = ap.parse_args()

    where = "1=1"
    params = {}
    if args.since_days is not None:
        where = """
            (
                keepa_updated_at >= UTC_TIMESTAMP() - INTERVAL :days DAY
                OR rainforest_updated_at >= UTC_TIMESTAMP() - INTERVAL :days DAY
                OR item_count_updated_at >= UTC_TIMESTAMP() - INTERVAL :days DAY
                OR last_update >= UTC_TIMESTAMP() - INTERVAL :days DAY
            )
        """
        params["days"] = args.since_days

    q = text(f"""
        SELECT
            COUNT(*) AS n_items,
            SUM(price IS NOT NULL AND price > 0) AS n_price_positive,
            SUM(keepa_number_of_items IS NOT NULL AND keepa_number_of_items > 0) AS n_keepa_noi,
            SUM(keepa_package_quantity IS NOT NULL AND keepa_package_quantity > 0) AS n_keepa_pq,
            SUM(keepa_updated_at IS NOT NULL) AS n_keepa_updated,
            SUM(rainforest_unit_price_json IS NOT NULL) AS n_rainforest_unit,
            SUM(rainforest_updated_at IS NOT NULL) AS n_rainforest_updated,
            SUM(title_inferred_item_count IS NOT NULL AND title_inferred_item_count >= 2) AS n_title_ge2,
            SUM(item_count_updated_at IS NOT NULL) AS n_resolved,
            SUM(price_per_item IS NOT NULL) AS n_price_per_item,
            SUM(item_count_source = 'keepa') AS n_source_keepa,
            SUM(item_count_source = 'title') AS n_source_title,
            SUM(item_count_source = 'default') AS n_source_default,
            SUM(item_count_updated_at IS NULL) AS n_pending_resolution
        FROM items
        WHERE {where}
    """)

    engine = mysqlengine()
    with engine.connect() as conn:
        row = conn.execute(q, params).mappings().one()

    n = row["n_items"] or 0

    def pct(x):
        if not n:
            return 0.0
        return 100.0 * (x or 0) / n

    scope = f"last {args.since_days} days" if args.since_days else "all items"
    print(f"=== Item count signal coverage ({scope}) ===\n")
    print(f"  rows:                         {n}")
    print(f"  price > 0:                    {row['n_price_positive']} ({pct(row['n_price_positive']):.1f}%)")
    print(f"  keepa numberOfItems > 0:      {row['n_keepa_noi']} ({pct(row['n_keepa_noi']):.1f}%)")
    print(f"  keepa packageQuantity > 0:    {row['n_keepa_pq']} ({pct(row['n_keepa_pq']):.1f}%)")
    print(f"  keepa_updated_at set:         {row['n_keepa_updated']} ({pct(row['n_keepa_updated']):.1f}%)")
    print(f"  rainforest unit JSON:         {row['n_rainforest_unit']} ({pct(row['n_rainforest_unit']):.1f}%)")
    print(f"  rainforest_updated_at set:    {row['n_rainforest_updated']} ({pct(row['n_rainforest_updated']):.1f}%)")
    print(f"  title inferred count >= 2:    {row['n_title_ge2']} ({pct(row['n_title_ge2']):.1f}%)")
    print(f"  resolved (item_count_updated): {row['n_resolved']} ({pct(row['n_resolved']):.1f}%)")
    print(f"  price_per_item computed:      {row['n_price_per_item']} ({pct(row['n_price_per_item']):.1f}%)")
    print(f"  pending resolution:           {row['n_pending_resolution']} ({pct(row['n_pending_resolution']):.1f}%)")
    print()
    print("  item_count_source breakdown (resolved rows):")
    print(f"    keepa:   {row['n_source_keepa']}")
    print(f"    title:   {row['n_source_title']}")
    print(f"    default: {row['n_source_default']}")
    print()


if __name__ == "__main__":
    main()
