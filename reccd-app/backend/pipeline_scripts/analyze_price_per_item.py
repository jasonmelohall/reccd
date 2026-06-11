#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-off analysis: feasibility of price_per_item = price / item_count.

  --demo             Run title heuristics on built-in sample rows (no DB, stdlib only).
  --db-summary       Load titles/prices from MySQL (RECCD_DB_URL) via SQLAlchemy + reccd_items.
  --sample-keepa N   After DB load, call Keepa for N ASINs (mix of heuristic hit/miss).
  --sample-rainforest N
                     Rainforest product API for N ASINs; prints buybox price + unit_price.

HTTP uses urllib (stdlib). DB needs: sqlalchemy, pymysql (see backend/requirements.txt).

Examples:
  python3 analyze_price_per_item.py --demo
  python3 analyze_price_per_item.py --db-summary --limit 5000
  python3 analyze_price_per_item.py --db-summary --limit 2000 --sample-keepa 12 --sample-rainforest 8
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DIR = os.path.join(BASE_DIR, "shared")

# Title-based inference (conservative). Returns (count >= 2, matched_pattern_name).
_COUNT_PATTERNS: Sequence[Tuple[str, re.Pattern]] = (
    ("pack_of_n", re.compile(r"\bpack\s+of\s+(\d{1,4})\b", re.I)),
    ("n_pack", re.compile(r"\b(\d{1,3})\s*[-]?\s*pack\b", re.I)),
    ("n_pk", re.compile(r"\b(\d{1,3})\s*pk\b", re.I)),
    ("n_count", re.compile(r"\b(\d{1,4})\s*[-]?\s*count\b", re.I)),
    ("n_ct_word", re.compile(r"\b(\d{1,4})\s*ct\b", re.I)),
    ("n_pieces", re.compile(r"\b(\d{1,4})\s*[-]?\s*pieces?\b", re.I)),
    ("n_units", re.compile(r"\b(\d{1,4})\s*units?\b", re.I)),
    ("paren_n_pack", re.compile(r"\(\s*(\d{1,3})\s*[-]?\s*pack\s*\)", re.I)),
)


def infer_item_count_from_title(title: str) -> Tuple[Optional[int], Optional[str]]:
    if not title or not isinstance(title, str):
        return None, None
    t = title.lower()
    if re.search(r"\b\d{1,2}\s*x\s*\d{1,2}\b", t):
        return None, None
    for name, pat in _COUNT_PATTERNS:
        m = pat.search(title)
        if not m:
            continue
        n = int(m.group(1))
        if 2 <= n <= 2000:
            return n, name
    return None, None


@dataclass
class RowMini:
    asin: str
    title: str
    price: Optional[float]


DEMO_ROWS: List[RowMini] = [
    RowMini("DEMO1", "SimpleHuman 45L Rectangular Step Trash Can, White", 89.99),
    RowMini("DEMO2", "Glad ForceFlex Tall Kitchen Drawstring Trash Bags (110 Count)", 18.49),
    RowMini("DEMO3", "Hefty Ultra Strong Large Trash Bags, Citrus Twist, 74 Count", 12.99),
    RowMini("DEMO4", "Amazon Basics 2-Pack Small Bathroom Trash Cans, Black", 34.00),
    RowMini("DEMO5", "Sterilite 106 Qt Latch Storage Box — 4 pack", 48.00),
    RowMini("DEMO6", "iDesign Mono Wastebasket Plastic Small Trash Can 9 inches x 10 (no pack keyword)", 14.00),
    RowMini("DEMO7", "Bounty Quick Size Paper Towels, 16 Family Rolls = 40 Regular Rolls", 38.00),
    RowMini("DEMO8", "Ziploc Sandwich Bags, 280 Count", 9.99),
    RowMini("DEMO9", "Clorox Disinfecting Wipes Value Pack, 75 Ct Each, Pack of 3", 14.50),
    RowMini("DEMO10", "Photo frame 8 x 10 wood grain (dimension pattern)", 22.00),
]


def _ensure_shared_path() -> None:
    if SHARED_DIR not in sys.path:
        sys.path.insert(0, SHARED_DIR)


def load_db_rows(limit: int) -> List[RowMini]:
    _ensure_shared_path()
    try:
        from sqlalchemy import text
    except ImportError as exc:
        raise SystemExit(
            "Missing sqlalchemy. From repo backend: pip install -r requirements.txt"
        ) from exc
    from reccd_items import mysqlengine

    engine = mysqlengine()
    q = text(
        """
        SELECT asin, title, price
        FROM items
        WHERE title IS NOT NULL AND title != ''
        ORDER BY rainforest_last_update DESC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q, {"lim": limit}).fetchall()
    out: List[RowMini] = []
    for r in rows:
        p = r[2]
        try:
            pf = float(p) if p is not None else None
        except (TypeError, ValueError):
            pf = None
        out.append(RowMini(asin=str(r[0]), title=str(r[1] or ""), price=pf))
    return out


def summarize_db(rows: List[RowMini]) -> None:
    n = len(rows)
    with_price = sum(1 for r in rows if r.price is not None and r.price > 0)
    inferred = 0
    by_pat: Dict[str, int] = {}
    examples: List[Tuple[float, float, str, str]] = []

    for r in rows:
        cnt, pat = infer_item_count_from_title(r.title)
        if pat:
            by_pat[pat] = by_pat.get(pat, 0) + 1
        if cnt and r.price and r.price > 0:
            inferred += 1
            ppi = r.price / cnt
            examples.append((r.price / max(ppi, 1e-9), r.price, r.title[:120], pat or ""))

    inferred_pct = 100.0 * inferred / n if n else 0.0
    print("\n=== DB title heuristic summary ===")
    print(f"rows: {n}, with positive price: {with_price}")
    print(f"rows with inferrable item_count (>=2) and price: {inferred} ({inferred_pct:.1f}%)")
    print("matches by pattern:", json.dumps(dict(sorted(by_pat.items(), key=lambda x: -x[1])), indent=2))

    examples.sort(key=lambda x: -x[0])
    print("\nLargest price / (price/count) ratio (first 12) — multi-packs where heuristic matters most:")
    for ratio, price, title, pat in examples[:12]:
        print(f"  ratio={ratio:.2f}  price={price:.2f}  pat={pat}  title={title!r}")


def http_get_json(url: str, params: Dict[str, Any], timeout: int = 20) -> Any:
    q = urllib.parse.urlencode(params)
    full = f"{url}?{q}"
    req = urllib.request.Request(full, headers={"User-Agent": "reccd-analyze/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def keepa_product(asin: str, api_key: str) -> Optional[Dict[str, Any]]:
    try:
        data = http_get_json(
            "https://api.keepa.com/product",
            {"key": api_key, "domain": 1, "asin": asin, "history": 0, "rating": 0},
            timeout=15,
        )
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        print(f"  Keepa error for {asin}: {e}")
        return None
    prods = data.get("products") or []
    if not prods:
        print(f"  Keepa no products for {asin}")
        return None
    return prods[0]


def rainforest_product(asin: str, api_key: str) -> Optional[Dict[str, Any]]:
    try:
        data = http_get_json(
            "https://api.rainforestapi.com/request",
            {
                "api_key": api_key,
                "type": "product",
                "amazon_domain": "amazon.com",
                "asin": asin,
            },
            timeout=25,
        )
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        print(f"  Rainforest error for {asin}: {e}")
        return None
    return data.get("product") or {}


def pick_samples(rows: List[RowMini], n: int) -> List[RowMini]:
    hits = [r for r in rows if infer_item_count_from_title(r.title)[0]]
    misses = [r for r in rows if not infer_item_count_from_title(r.title)[0]]
    random.shuffle(hits)
    random.shuffle(misses)
    half = max(1, n // 2)
    out = hits[:half] + misses[: n - half]
    return out[:n]


def run_keepa_samples(rows: List[RowMini], n: int, api_key: str) -> None:
    print("\n=== Keepa product sample (numberOfItems, packageQuantity) ===")
    for r in pick_samples(rows, n):
        p = keepa_product(r.asin, api_key)
        if not p:
            continue
        noi = p.get("numberOfItems")
        pq = p.get("packageQuantity")
        title_db = r.title[:80]
        hc, hp = infer_item_count_from_title(r.title)
        print(
            f"  ASIN={r.asin}  numberOfItems={noi!r}  packageQuantity={pq!r}  "
            f"heuristic_count={hc!r} heuristic_pat={hp!r}\n    title={title_db!r}..."
        )


def run_rainforest_samples(rows: List[RowMini], n: int, api_key: str) -> None:
    print("\n=== Rainforest product sample (buybox_winner price vs unit_price) ===")
    for r in pick_samples(rows, n):
        prod = rainforest_product(r.asin, api_key)
        bb = prod.get("buybox_winner") or {}
        price = (bb.get("price") or {}).get("value")
        unit_price = bb.get("unit_price")
        hc, hp = infer_item_count_from_title(r.title)
        print(
            f"  ASIN={r.asin}  buybox price={price!r}  unit_price={unit_price!r}  "
            f"heuristic_count={hc!r} pat={hp!r}\n    title={r.title[:90]!r}..."
        )


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze price-per-item feasibility.")
    ap.add_argument("--demo", action="store_true", help="Run heuristics on built-in sample titles (no DB).")
    ap.add_argument("--db-summary", action="store_true", help="Query items table and summarize heuristics.")
    ap.add_argument("--limit", type=int, default=3000, help="Max rows from DB (default 3000).")
    ap.add_argument("--sample-keepa", type=int, default=0, metavar="N", help="Keepa samples after DB load.")
    ap.add_argument("--sample-rainforest", type=int, default=0, metavar="N", help="Rainforest samples after DB load.")
    ap.add_argument("--seed", type=int, default=42, help="RNG seed for sampling.")
    args = ap.parse_args()
    random.seed(args.seed)

    if not args.demo and not args.db_summary and not args.sample_keepa and not args.sample_rainforest:
        ap.print_help()
        print("\nProvide --demo and/or --db-summary and/or sampling flags.")
        sys.exit(0)

    if args.demo:
        print("=== Demo rows (synthetic titles, no database) ===")
        summarize_db(DEMO_ROWS)

    rows: List[RowMini] = []
    need_db = bool(args.db_summary or args.sample_keepa or args.sample_rainforest)
    if need_db:
        rows = load_db_rows(args.limit)
        if args.db_summary:
            if args.demo:
                print("\n=== Live database (same heuristics on items table) ===")
            summarize_db(rows)

    if args.sample_keepa:
        key = os.getenv("KEEPA_API_KEY")
        if not key:
            print("KEEPA_API_KEY not set; skipping Keepa samples.")
        elif not rows:
            print("No DB rows; cannot sample Keepa.")
        else:
            run_keepa_samples(rows, args.sample_keepa, key)

    if args.sample_rainforest:
        key = os.getenv("RAINFOREST_API_KEY")
        if not key:
            print("RAINFOREST_API_KEY not set; skipping Rainforest samples.")
        elif not rows:
            print("No DB rows; cannot sample Rainforest.")
        else:
            run_rainforest_samples(rows, args.sample_rainforest, key)

    print(
        "\n=== API notes (from Rainforest docs + Keepa Product struct + this codebase) ===\n"
        "- Rainforest search (1_search_items): only item['price']['value'] is used; no unit fields in code.\n"
        "- Rainforest product `buybox_winner` may include `unit_price` (e.g. per kg/l); not always per discrete item.\n"
        "- Keepa product JSON includes `numberOfItems` and `packageQuantity` (Amazon catalog fields).\n"
        "- Pipeline scripts do not persist those fields today; Keepa calls use history=1 only in listed-date script.\n"
    )


if __name__ == "__main__":
    main()
