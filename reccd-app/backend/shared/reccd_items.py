#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilities shared with the backend that originated from the standalone
`items/reccd_items.py` module. Keeping a local copy ensures the backend
deploy contains the helpers it needs without modifying the original files.
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import sqlalchemy

logger = logging.getLogger(__name__)

# Keepa Time: minutes since this instant. listedSince 0 / -1 => unknown, not a real listing date.
KEEPA_TIME_BASE = datetime(2011, 1, 1)
KEEPA_SENTINEL_END = datetime(2011, 1, 2)


def _to_naive_utc(dt: datetime) -> datetime:
    """Normalize aware datetimes to naive UTC for consistent comparisons/storage."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def keepa_minutes_to_datetime(minutes: Any) -> Optional[datetime]:
    """Convert Keepa minute offset to datetime; NULL for unknown (<= 0)."""
    if minutes is None:
        return None
    try:
        m = int(minutes)
    except (TypeError, ValueError):
        return None
    if m <= 0:
        return None
    return KEEPA_TIME_BASE + timedelta(minutes=m)


def sanitize_product_datetime(dt: Any) -> Optional[datetime]:
    """Drop missing values and Keepa epoch placeholder dates (2011-01-01)."""
    if dt is None:
        return None
    try:
        if isinstance(dt, datetime):
            candidate = dt
        elif hasattr(dt, "to_pydatetime"):
            candidate = dt.to_pydatetime()
        else:
            candidate = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    candidate = _to_naive_utc(candidate)
    if candidate < KEEPA_SENTINEL_END:
        return None
    return candidate


def earliest_valid_release_date(*dates: Any) -> Optional[datetime]:
    valid: List[datetime] = []
    for d in dates:
        cleaned = sanitize_product_datetime(d)
        if cleaned is not None:
            valid.append(cleaned)
    if not valid:
        return None
    return min(valid)


def apply_valid_release_dates(df):
    """
    For ranking: null out Keepa sentinel dates, then release_date = min of valid columns.
    Requires pandas (used by 9_reccd_items / recommendation_service).
    """
    import pandas as pd

    date_cols = [c for c in ("release_date", "listed_date", "oldest_review") if c in df.columns]
    sentinel = pd.Timestamp(KEEPA_SENTINEL_END)
    for col in date_cols:
        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        if getattr(parsed.dtype, "tz", None) is not None:
            parsed = parsed.dt.tz_convert("UTC").dt.tz_localize(None)
        df[col] = parsed
        df.loc[df[col].notna() & (df[col] < sentinel), col] = pd.NaT
    if date_cols:
        df["release_date"] = df[date_cols].min(axis=1)
    return df


def get_search_term() -> List[str]:
    """Return the default Amazon search terms."""
    list_amazon_search_terms = [
       'white bathroom trash can with lid modern',
       'matte white step trash can small with lid',
       'champagne gold small bathroom trash can with lid',
       'brushed gold small trash can with lid bathroom',
       'cream bathroom trash can with lid minimal',
    ]
    return list_amazon_search_terms


def mysqlengine():
    """Return the shared SQLAlchemy engine using the RECCD_DB_URL env var."""
    db_url = os.getenv("RECCD_DB_URL")
    if not db_url:
        raise RuntimeError(
            "Missing environment variable RECCD_DB_URL. "
            "Set it to your MySQL connection URL."
        )
    engine = sqlalchemy.create_engine(db_url)
    return engine


# ===== Parent ASIN Utility Functions =====

def get_parent_asin_from_keepa(product_data: Dict) -> Optional[str]:
    """
    Extract parent ASIN from Keepa API product response.

    Args:
        product_data: Product dict from Keepa API response

    Returns:
        Parent ASIN if available, otherwise None.
    """
    return product_data.get("parentAsin")


def get_parent_asin_from_rainforest(item_data: Dict) -> Optional[str]:
    """
    Extract parent ASIN from Rainforest API search result or product response.

    Rainforest returns parent_asin in the search results when a product has variations.

    Args:
        item_data: Item dict from Rainforest API search results or product data

    Returns:
        Parent ASIN if available, otherwise None.
    """
    parent = item_data.get("parent_asin")

    if not parent:
        parent = item_data.get("product", {}).get("parent_asin")

    return parent


def fetch_parent_product_rainforest(parent_asin: str, api_key: str) -> Optional[Dict]:
    """
    Fetch full parent product details from Rainforest API.

    Args:
        parent_asin: The parent ASIN to fetch
        api_key: Rainforest API key

    Returns:
        Product data for parent ASIN, or None if error.
    """
    url = "https://api.rainforestapi.com/request"
    params = {
        "api_key": api_key,
        "type": "product",
        "amazon_domain": "amazon.com",
        "asin": parent_asin,
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            return response.json().get("product", {})
        logger.error("Error fetching parent %s: %s", parent_asin, response.status_code)
        return None
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Exception fetching parent %s: %s", parent_asin, exc)
        return None


# ===== Item count / price per item =====
# count_type: each | ounce (weights normalized to total ounces for $/oz comparison)
# item_count: each-count units, or total ounces when title has weight (lb/oz/kg/g/L/ml)

_MULTIPLY_COUNT_PATTERNS: Sequence[Tuple[str, re.Pattern]] = (
    (
        "n_boxes_of_m",
        re.compile(r"\b(\d{1,3})\s*box(?:e?s)?\s+of\s+(\d{1,4})\b", re.I),
    ),
    (
        "bracket_boxes_of_m",
        re.compile(r"\[\s*(\d{1,3})\s*box(?:e?s)?\s+of\s+(\d{1,4})\s*\]", re.I),
    ),
)

_COUNT_PATTERNS: Sequence[Tuple[str, re.Pattern]] = (
    ("pack_of_n", re.compile(r"\bpack\s+of\s+(\d{1,4})\b", re.I)),
    ("n_pack", re.compile(r"\b(\d{1,3})\s*[-]?\s*pack\b", re.I)),
    ("n_pcs", re.compile(r"\b(\d{1,4})\s*pcs\b", re.I)),
    ("n_pk", re.compile(r"\b(\d{1,3})\s*pk\b", re.I)),
    ("n_count", re.compile(r"\b(\d{1,4})\s*[-]?\s*counts?\b", re.I)),
    ("n_loads", re.compile(r"\b(\d{1,4})\s*[-]?\s*loads?\b", re.I)),
    ("paren_n_loads", re.compile(r"\(\s*(\d{1,4})\s*[-]?\s*loads?\b", re.I)),
    (
        "n_tabs",
        re.compile(r"\b(\d{1,4})\s+(?:[-\w]+\s+){0,3}tabs?\b", re.I),
    ),
    (
        "n_tablets",
        re.compile(r"\b(\d{1,4})\s+(?:[-\w]+\s+){0,3}tablets?\b", re.I),
    ),
    (
        "n_pods",
        re.compile(r"\b(\d{1,4})\s+(?:[-\w]+\s+){0,3}pods?\b", re.I),
    ),
    (
        "n_pacs",
        re.compile(r"\b(\d{1,4})\s+(?:[-\w]+\s+){0,3}pacs?\b", re.I),
    ),
    ("n_ct_word", re.compile(r"\b(\d{1,4})\s*ct\b", re.I)),
    ("n_pieces", re.compile(r"\b(\d{1,4})\s*[-]?\s*pieces?\b", re.I)),
    ("n_units", re.compile(r"\b(\d{1,4})\s*units?\b", re.I)),
    ("paren_n_pack", re.compile(r"\(\s*(\d{1,3})\s*[-]?\s*pack\s*\)", re.I)),
)

_WEIGHT_PATTERNS: Sequence[Tuple[str, re.Pattern, str]] = (
    (
        "frac_lb",
        re.compile(
            r"\b(\d+)\s*/\s*(\d+)\s*(?:lb|lbs|pound|pounds)\b",
            re.I,
        ),
        "pound",
    ),
    (
        "n_lb",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:lb|lbs|pound|pounds)\b",
            re.I,
        ),
        "pound",
    ),
    (
        "n_fl_oz",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:fl\.?\s*oz|fluid\s+ounces?)\b",
            re.I,
        ),
        "ounce",
    ),
    (
        "n_oz",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:oz|ounce|ounces)\b",
            re.I,
        ),
        "ounce",
    ),
    (
        "n_kg",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:kg|kilogram|kilograms|kilo)\b",
            re.I,
        ),
        "kilogram",
    ),
    (
        "n_g",
        re.compile(
            r"\b(?<![%])(\d+(?:\.\d+)?)\s*(?:g|gram|grams)\b",
            re.I,
        ),
        "gram",
    ),
    (
        "n_ml",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:ml|milliliters?|millilitres?)\b",
            re.I,
        ),
        "milliliter",
    ),
    (
        "n_liter",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:l|liter|litre|liters|litres)\b",
            re.I,
        ),
        "liter",
    ),
)

_LB_TO_OZ = 16.0
_KG_TO_OZ = 35.274
_G_TO_OZ = 1.0 / 28.3495
_L_TO_FL_OZ = 33.814
_ML_TO_FL_OZ = 1.0 / 29.5735

_MIN_EACH_COUNT = 2
_MAX_EACH_COUNT = 2000
_MIN_WEIGHT_OZ = 0.01
_MAX_WEIGHT_OZ = 2000.0

_CONTAINER_VOLUME_PATTERN = re.compile(
    r"\b(?:"
    r"thermos|thermo\b|funtainer|food\s+jar|vacuum\s+insulated|insulated\s+(?:food\s+)?jar|"
    r"insulated\s+food|soup\s+thermo|flask|tumbler|carafe|airpot|coffee\s+urn|"
    r"drinkware|water\s+bottle|bottle\s+with|travel\s+mug|mug\b|"
    r"food\s+storage\s+container|storage\s+container|food\s+container|"
    r"lunch\s+box|lunch\s+container|lunch\s+jar|lunch\s+tin|"
    r"meal\s+prep\s+tin|meal\s+prep\s+container|bento\b|"
    r"hydrapeak|stanley\b|energify"
    r")\b",
    re.I,
)


def _is_container_volume_title(title: str) -> bool:
    """True when oz/L likely describes vessel capacity (rank as 1 each, not $/oz)."""
    return bool(_CONTAINER_VOLUME_PATTERN.search(title))


def _multipack_count_for_weight_multiply(
    title: str,
    *,
    per_unit_is_each: bool,
) -> Optional[int]:
    """
    Multipack multiplier for weight (e.g. '3.81oz 3 pack' -> 3).
    'N count' only multiplies when weight is per-unit ('12 oz each'), not piece
    counts inside one bag ('16 oz ... 40 count' treats).
    """
    for pat in (
        re.compile(r"\bpack\s+of\s+(\d{1,3})\b", re.I),
        re.compile(r"\b(\d{1,3})\s*[-]?\s*pack\b", re.I),
        re.compile(r"\(\s*(\d{1,3})\s*[-]?\s*pack\s*\)", re.I),
    ):
        m = pat.search(title)
        if not m:
            continue
        n = int(m.group(1))
        if _MIN_EACH_COUNT <= n <= _MAX_EACH_COUNT:
            return n

    if per_unit_is_each:
        m = re.search(r"\b(\d{1,3})\s*[-]?\s*counts?\b", title, re.I)
        if m:
            n = int(m.group(1))
            if _MIN_EACH_COUNT <= n <= _MAX_EACH_COUNT:
                return n
    return None


_N_X_WEIGHT_PATTERN = re.compile(
    r"\b(\d{1,3})\s*x\s*(\d+(?:\.\d+)?)\s*"
    r"(fl\.?\s*oz|fluid\s+ounces?|oz|ounce|ounces|"
    r"lb|lbs|pound|pounds|g|gram|grams|"
    r"ml|milliliters?|millilitres?|l|liter|litre|liters|litres)\b",
    re.I,
)

_OZ_PACK_DESCRIPTOR = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(?:fl\.?\s*oz|oz|ounce|ounces)\s*\.?\s*pack\b(?!\s+of\b)",
    re.I,
)

_WEIGHT_EACH_PATTERNS: Sequence[Tuple[str, re.Pattern, str]] = (
    (
        "n_fl_oz_each",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:fl\.?\s*oz|fluid\s+ounces?)\s*each\b",
            re.I,
        ),
        "ounce",
    ),
    (
        "n_oz_each",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:oz|ounce|ounces)\s*each\b",
            re.I,
        ),
        "ounce",
    ),
    (
        "n_lb_each",
        re.compile(
            r"\b(\d+(?:\.\d+)?)\s*(?:lb|lbs|pound|pounds)\s*each\b",
            re.I,
        ),
        "pound",
    ),
)


def _unit_text_to_raw_unit(unit_text: str) -> str:
    u = unit_text.lower().replace(".", "")
    if u.startswith("fl") or u.startswith("fluid") or u in ("oz", "ounce", "ounces"):
        return "ounce"
    if u.startswith("lb") or u.startswith("pound"):
        return "pound"
    if u.startswith("g") and "gal" not in u:
        return "gram"
    if u.startswith("ml") or u.startswith("millil"):
        return "milliliter"
    return "liter"


def _infer_weight_ounces_from_title(
    title: str,
) -> Tuple[Optional[float], Optional[str]]:
    """
    Net product weight in ounces from title.
    When weight and pack/count both appear (e.g. '12 oz Each (2-Pack)'), returns total oz.
    """
    if _is_container_volume_title(title):
        return None, None

    oz_pack = _OZ_PACK_DESCRIPTOR.search(title)
    if oz_pack:
        try:
            qty = round(float(oz_pack.group(1)), 4)
        except (TypeError, ValueError):
            qty = None
        if qty is not None:
            oz = _weight_to_ounces(qty, "ounce")
            if _MIN_WEIGHT_OZ <= oz <= _MAX_WEIGHT_OZ:
                return oz, "n_oz_pack_descriptor"

    n_x_m = _N_X_WEIGHT_PATTERN.search(title)
    if n_x_m:
        try:
            n = int(n_x_m.group(1))
            qty = float(n_x_m.group(2))
        except (TypeError, ValueError):
            n = None
        if n is not None and _MIN_EACH_COUNT <= n <= _MAX_EACH_COUNT:
            raw_unit = _unit_text_to_raw_unit(n_x_m.group(3))
            per_unit_oz = _weight_to_ounces(qty, raw_unit)
            total_oz = round(per_unit_oz * n, 4)
            if _MIN_WEIGHT_OZ <= total_oz <= _MAX_WEIGHT_OZ:
                return total_oz, "n_x_weight"

    per_unit_oz: Optional[float] = None
    pattern_name: Optional[str] = None
    for name, pat, raw_unit in _WEIGHT_EACH_PATTERNS:
        m = pat.search(title)
        if not m:
            continue
        try:
            qty = round(float(m.group(1)), 4)
        except (TypeError, ValueError):
            continue
        oz = _weight_to_ounces(qty, raw_unit)
        if oz < _MIN_WEIGHT_OZ or oz > _MAX_WEIGHT_OZ:
            continue
        per_unit_oz = oz
        pattern_name = name
        break

    if per_unit_oz is None:
        for name, pat, raw_unit in _WEIGHT_PATTERNS:
            m = pat.search(title)
            if not m:
                continue
            qty = _parse_weight_quantity(m, name)
            if qty is None:
                continue
            oz = _weight_to_ounces(qty, raw_unit)
            if oz < _MIN_WEIGHT_OZ or oz > _MAX_WEIGHT_OZ:
                continue
            per_unit_oz = oz
            pattern_name = name
            break

    if per_unit_oz is None or pattern_name is None:
        return None, None

    per_unit_is_each = pattern_name in (
        "n_fl_oz_each",
        "n_oz_each",
        "n_lb_each",
    )
    pack_n = _multipack_count_for_weight_multiply(
        title,
        per_unit_is_each=per_unit_is_each,
    )
    if pack_n is not None:
        total_oz = round(per_unit_oz * pack_n, 4)
        if total_oz <= _MAX_WEIGHT_OZ:
            suffix = "n_pack" if "pack" in title.lower() else "n_count"
            return total_oz, f"{pattern_name}_x_{suffix}"

    return per_unit_oz, pattern_name


def _parse_weight_quantity(match: re.Match, pattern_name: str) -> Optional[float]:
    if pattern_name == "frac_lb":
        num, den = int(match.group(1)), int(match.group(2))
        if den <= 0:
            return None
        return round(num / den, 4)
    try:
        return round(float(match.group(1)), 4)
    except (TypeError, ValueError):
        return None


def _weight_to_ounces(qty: float, raw_unit: str) -> float:
    """Normalize lb/kg/g/L/ml to ounces for comparable $/oz ranking."""
    u = raw_unit.lower()
    if u == "ounce":
        return round(qty, 4)
    if u == "pound":
        return round(qty * _LB_TO_OZ, 4)
    if u == "kilogram":
        return round(qty * _KG_TO_OZ, 4)
    if u == "gram":
        return round(qty * _G_TO_OZ, 4)
    if u in ("liter", "litre"):
        return round(qty * _L_TO_FL_OZ, 4)
    if u == "milliliter":
        return round(qty * _ML_TO_FL_OZ, 4)
    return round(qty, 4)


_SHEET_PRODUCT_HINT = re.compile(
    r"\b(?:toilet\s+paper|bath\s+tissue|facial\s+tissue|paper\s+towels?|tp\b)\b",
    re.I,
)

_MIN_SHEET_COUNT = 50
_MAX_SHEET_COUNT = 50000


def _has_sheet_product_context(title: str) -> bool:
    return bool(_SHEET_PRODUCT_HINT.search(title))


def _rolls_in_title(title: str) -> Optional[re.Match]:
    return re.search(
        r"\b(\d{1,3})\s+"
        r"(?:(?:long[- ]lasting|mega|triple|double|regular|jumbo)\s+)?rolls?\b",
        title,
        re.I,
    )


def _sheets_per_roll_in_title(title: str) -> Optional[int]:
    for pat in (
        r"\b(\d{2,4})\s*sheets?\s*(?:per\s*roll|/\s*roll|each\s*roll)\b",
        r"\b(\d{2,4})\s*sheets?\s*/\s*roll\b",
    ):
        m = re.search(pat, title, re.I)
        if m:
            return int(m.group(1))
    return None


def _infer_sheet_count_from_title(title: str) -> Tuple[Optional[float], Optional[str]]:
    """Toilet paper / tissue: total sheets for $/sheet ranking."""
    if not _has_sheet_product_context(title):
        return None, None

    for pat_name, pat in (
        (
            "total_sheets",
            re.compile(r"\btotal\s+(\d{1,5})\s+sheets?\b", re.I),
        ),
        (
            "n_total_sheets",
            re.compile(
                r"\b(\d{1,5})\s+total\s+(?:bath\s+tissue\s+|tissue\s+)?sheets?\b",
                re.I,
            ),
        ),
    ):
        m = pat.search(title)
        if m:
            n = int(m.group(1))
            if _MIN_SHEET_COUNT <= n <= _MAX_SHEET_COUNT:
                return float(n), pat_name

    rolls_m = _rolls_in_title(title)
    spr = _sheets_per_roll_in_title(title)
    pack_m = re.search(
        r"\b(\d{1,3})\s+pack\b(?!\s+of\b)",
        title,
        re.I,
    )
    if rolls_m and spr:
        n = int(rolls_m.group(1)) * spr
        if _MIN_SHEET_COUNT <= n <= _MAX_SHEET_COUNT:
            return float(n), "rolls_x_sheets_per_roll"

    if pack_m and spr:
        n = int(pack_m.group(1)) * spr
        if _MIN_SHEET_COUNT <= n <= _MAX_SHEET_COUNT:
            return float(n), "pack_x_sheets_per_roll"

    if rolls_m and re.search(r"\bsheets?\b", title, re.I):
        for m in re.finditer(r"\b(\d{2,4})\s+sheets?\b", title, re.I):
            per_roll = int(m.group(1))
            if 100 <= per_roll <= 1200:
                n = int(rolls_m.group(1)) * per_roll
                if _MIN_SHEET_COUNT <= n <= _MAX_SHEET_COUNT:
                    return float(n), "rolls_x_bare_sheets"

    if re.search(r"\bsheets?\b", title, re.I):
        m = re.search(r"\b(\d{3,5})\s+counts?\b", title, re.I)
        if m:
            n = int(m.group(1))
            if _MIN_SHEET_COUNT <= n <= _MAX_SHEET_COUNT:
                return float(n), "n_sheet_count"

    if rolls_m:
        n = int(rolls_m.group(1))
        if _MIN_EACH_COUNT <= n <= _MAX_EACH_COUNT:
            return float(n), "n_rolls_no_sheet_count"

    if pack_m and not spr:
        n = int(pack_m.group(1))
        if _MIN_EACH_COUNT <= n <= _MAX_EACH_COUNT:
            return float(n), "n_rolls_no_sheet_count"

    return None, None


def _is_seed_bulk_each_count(title: str, n: int, pattern_name: str) -> bool:
    """Skip multipack each-count only when title context looks like bulk seeds, not detergent/count."""
    t = title.lower()
    seed_context = bool(
        re.search(
            r"\b(seeds?|seed\s+count|grass\s+seed|lawn\s+seed|wildflower)\b",
            t,
        )
    )
    if seed_context and n >= 100:
        return True
    if seed_context and n >= 50 and pattern_name in (
        "n_pcs",
        "n_count",
        "n_loads",
        "paren_n_loads",
        "n_tabs",
        "n_tablets",
        "n_pods",
        "n_pacs",
        "n_pieces",
        "n_units",
        "n_ct_word",
    ):
        return True
    if n >= 500:
        return True
    return False


def infer_quantity_from_title(
    title: Optional[str],
) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    """
    Returns (quantity, count_type, pattern_name).
    Consumable weights normalize to ounces for $/oz.
    Container capacity (thermos, food jar) is not used as item_count.
    Multipack each-count uses count_type='each'.
    Toilet paper / tissue uses count_type='sheet' (or 'roll' if only roll count known).
    """
    if not title or not isinstance(title, str):
        return None, None, None
    t = title.lower()
    if re.search(r"\b\d{1,2}\s*x\s*\d{1,2}\b", t) and not _N_X_WEIGHT_PATTERN.search(title):
        return None, None, None

    sheet_qty, sheet_pat = _infer_sheet_count_from_title(title)
    if sheet_qty is not None:
        if sheet_pat == "n_rolls_no_sheet_count":
            return sheet_qty, "roll", sheet_pat
        return sheet_qty, "sheet", sheet_pat

    weight_oz, weight_pat = _infer_weight_ounces_from_title(title)
    if weight_oz is not None and weight_pat is not None:
        return weight_oz, "ounce", weight_pat

    for name, pat in _MULTIPLY_COUNT_PATTERNS:
        m = pat.search(title)
        if not m:
            continue
        try:
            n = int(m.group(1)) * int(m.group(2))
        except (TypeError, ValueError):
            continue
        if _is_seed_bulk_each_count(title, n, name):
            continue
        if _MIN_EACH_COUNT <= n <= _MAX_EACH_COUNT:
            return float(n), "each", name

    if not _has_sheet_product_context(title):
        for name, pat in _COUNT_PATTERNS:
            if name == "n_pieces" and re.search(r"\b(puzzle|jigsaw)\b", t):
                continue
            m = pat.search(title)
            if not m:
                continue
            n = int(m.group(1))
            if _is_seed_bulk_each_count(title, n, name):
                continue
            if _MIN_EACH_COUNT <= n <= _MAX_EACH_COUNT:
                return float(n), "each", name

    return None, None, None


def infer_item_count_from_title(title: Optional[str]) -> Tuple[Optional[int], Optional[str]]:
    """Backward-compatible: each-count multipacks only (int)."""
    qty, count_type, pattern = infer_quantity_from_title(title)
    if qty is None or count_type != "each":
        return None, None
    return int(qty), pattern


def title_inference_fields(title: Optional[str]) -> Dict[str, Any]:
    qty, count_type, pattern = infer_quantity_from_title(title)
    return {
        "title_inferred_item_count": qty,
        "title_inferred_count_type": count_type,
        "title_inferred_pattern": pattern,
    }


def apply_item_count_fields_to_dataframe(df):
    """
    Recompute item_count, count_type, and price_per_item from title + price
    for in-memory ranking (e.g. 9_reccd_items). Requires pandas.
    """
    import pandas as pd

    if df is None or df.empty or "title" not in df.columns:
        return df

    keepa_noi = "keepa_number_of_items" if "keepa_number_of_items" in df.columns else None
    keepa_pq = "keepa_package_quantity" if "keepa_package_quantity" in df.columns else None
    rf_unit = "rainforest_unit_price_json" if "rainforest_unit_price_json" in df.columns else None

    for col in (
        "item_count",
        "count_type",
        "item_count_source",
        "price_per_item",
        "title_inferred_item_count",
        "title_inferred_count_type",
        "title_inferred_pattern",
    ):
        if col not in df.columns:
            df[col] = None

    for idx in df.index:
        title = df.at[idx, "title"]
        if title is None or (isinstance(title, float) and pd.isna(title)):
            continue
        merged = merge_item_count_signals(
            title=title,
            price=df.at[idx, "price"] if "price" in df.columns else None,
            keepa_number_of_items=df.at[idx, keepa_noi] if keepa_noi else None,
            keepa_package_quantity=df.at[idx, keepa_pq] if keepa_pq else None,
            rainforest_unit_price_json=df.at[idx, rf_unit] if rf_unit else None,
        )
        for key, val in merged.items():
            df.at[idx, key] = val

    return df


def normalize_keepa_count_for_storage(value: Any) -> Optional[int]:
    """Keepa uses -1 / 0 for unknown; store NULL. Keeps 1+ for audit only."""
    if value is None:
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    return n


def _keepa_count_for_merge(value: Any) -> Optional[int]:
    """
    Keepa count usable for item_count merge only when >= 2 (multipack each).
    -1, 0, 1, and NULL are equivalent: no Keepa signal; use title or default 1.
    """
    n = normalize_keepa_count_for_storage(value)
    if n is None or n < _MIN_EACH_COUNT:
        return None
    return n


def compute_price_per_item(price: Any, item_count: Any) -> Optional[float]:
    try:
        units = float(item_count)
    except (TypeError, ValueError):
        return None
    if units <= 0:
        return None
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if p <= 0:
        return None
    return round(p / units, 4)


def parse_rainforest_unit_price_json(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    return None


def merge_item_count_signals(
    *,
    title: Optional[str],
    price: Any,
    keepa_number_of_items: Any = None,
    keepa_package_quantity: Any = None,
    rainforest_unit_price_json: Any = None,
) -> Dict[str, Any]:
    """
    Priority:
    (1) Title sheet count (toilet paper / tissue)
    (2) Title weight normalized to ounces
    (3) Keepa multipack each-count
    (4) Title multipack each-count
    (5) default item_count=1, count_type=each

    price_per_item is price / item_count ($/sheet, $/oz, $/roll, or $/each).
    """
    _ = parse_rainforest_unit_price_json(rainforest_unit_price_json)
    title_fields = title_inference_fields(title)
    noi = _keepa_count_for_merge(keepa_number_of_items)
    pq = _keepa_count_for_merge(keepa_package_quantity)
    title_qty = title_fields["title_inferred_item_count"]
    title_type = title_fields["title_inferred_count_type"]

    item_count: float = 1.0
    count_type = "each"
    item_count_source = "default"

    if title_qty is not None and title_type == "sheet":
        item_count = float(title_qty)
        count_type = "sheet"
        item_count_source = "title"
    elif title_qty is not None and title_type == "roll":
        item_count = float(title_qty)
        count_type = "roll"
        item_count_source = "title"
    elif title_qty is not None and title_type == "ounce":
        item_count = float(title_qty)
        count_type = "ounce"
        item_count_source = "title"
    elif title_qty is not None and title_type and title_type not in ("each", "sheet", "roll"):
        oz = _weight_to_ounces(float(title_qty), title_type)
        item_count = oz
        count_type = "ounce"
        item_count_source = "title"
    elif noi is not None and noi <= _MAX_EACH_COUNT:
        item_count = float(noi)
        count_type = "each"
        item_count_source = "keepa"
    elif pq is not None and pq <= _MAX_EACH_COUNT:
        item_count = float(pq)
        count_type = "each"
        item_count_source = "keepa"
    elif (
        title_qty is not None
        and title_type == "each"
        and title_qty >= _MIN_EACH_COUNT
    ):
        item_count = float(title_qty)
        count_type = "each"
        item_count_source = "title"

    return {
        **title_fields,
        "item_count": item_count,
        "count_type": count_type,
        "item_count_source": item_count_source,
        "price_per_item": compute_price_per_item(price, item_count),
    }


def get_keepa_unit_fields(product_data: Dict) -> Dict[str, Any]:
    """Extract numberOfItems and packageQuantity from Keepa; -1/0 stored as NULL."""
    return {
        "keepa_number_of_items": normalize_keepa_count_for_storage(
            product_data.get("numberOfItems")
        ),
        "keepa_package_quantity": normalize_keepa_count_for_storage(
            product_data.get("packageQuantity")
        ),
    }


def get_variation_asins_from_keepa(product_data: Dict) -> List[str]:
    """
    Extract all variation ASINs from Keepa product response.

    Args:
        product_data: Product dict from Keepa API response

    Returns:
        List of variation ASINs, empty list if none.
    """
    variations = product_data.get("variationASINs", [])
    return variations if variations else []


def consolidate_parent_items(items_dict: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Consolidate multiple child variations into parent entries with best search rank.
    Prioritizes items with complete data (ratings_total not None).

    Args:
        items_dict: Dict mapping ASIN -> item data (must include 'parent_asin' and 'search_rank')

    Returns:
        Dict with parent ASINs only, using best search_rank
    """
    parent_groups: Dict[str, List[Dict]] = {}

    for asin, item in items_dict.items():
        parent_asin = item.get("parent_asin") or asin
        parent_groups.setdefault(parent_asin, []).append(item)

    parent_map: Dict[str, Dict] = {}

    for parent_asin, variations in parent_groups.items():
        def _sort_price(x):
            ppi = x.get("price_per_item")
            if ppi is not None:
                return ppi
            p = x.get("price")
            return p if p is not None else float("inf")

        sorted_variations = sorted(
            variations,
            key=lambda x: (
                0 if x.get("ratings_total") is not None else 1,
                x.get("search_rank", float("inf")),
                _sort_price(x),
            ),
        )

        best_variation = sorted_variations[0].copy()
        best_variation["asin"] = parent_asin

        all_ranks = [v.get("search_rank", float("inf")) for v in variations]
        all_prices = [v.get("price") for v in variations if v.get("price") is not None]
        all_ppi = [v.get("price_per_item") for v in variations if v.get("price_per_item") is not None]

        best_variation["search_rank"] = min(all_ranks) if all_ranks else best_variation.get(
            "search_rank"
        )
        if all_prices:
            best_variation["price"] = min(all_prices)
        if all_ppi:
            best_variation["price_per_item"] = min(all_ppi)

        parent_map[parent_asin] = best_variation

    return parent_map

