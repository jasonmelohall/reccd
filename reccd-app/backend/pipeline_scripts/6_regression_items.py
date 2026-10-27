#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
OLS for item ranking coefficients.

Same estimator, target, sign rules, and model selection as items/81_items_regression.py.
Training rows are the live equivalent of items_user_parent (one row per user × parent
product, target user_frequency_percentile) so a search does not depend on that table
having been refreshed by items/59_items_user_parent.py.
"""

import os
import sys
import logging
from itertools import combinations

import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import text

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DIR = os.path.join(BASE_DIR, "shared")
sys.path.insert(0, SHARED_DIR)

import reccd_items

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
print()

EMAIL = "jasonmelohall@gmail.com"

# Same target as items/81_items_regression.py and testing/root_field_mapping.json
TARGET_COLUMN = "user_frequency_percentile"

FEATURE_VALIDATION_RULES = [
    {
        "feature": "price_percentile",
        "condition": "positive",
        "reason": "Price coefficient positive - you buy expensive items more frequently",
    },
    {
        "feature": "search_rank_percentile",
        "condition": "positive",
        "reason": "Search rank coefficient positive - you buy items with worse search positions more frequently",
    },
    {
        "feature": "frequency_percentile",
        "condition": "negative",
        "reason": "Frequency coefficient negative - you buy less trendy items more frequently",
    },
    {
        "feature": "release_date_percentile",
        "condition": "negative",
        "reason": "Release date coefficient negative - you buy older items more frequently",
    },
    {
        "feature": "rating_percentile",
        "condition": "negative",
        "reason": "Rating coefficient negative - you buy lower-rated items more frequently",
    },
]

# Exact feature combinations to skip (not fitted, not eligible for selection).
EXCLUDED_FEATURE_COMBINATIONS = [
    {"search_rank_percentile"},
]


def _combo_key(features):
    return frozenset(features)


_EXCLUDED_COMBO_KEYS = {_combo_key(c) for c in EXCLUDED_FEATURE_COMBINATIONS}
_FIELD_RULE = {r["feature"]: r for r in FEATURE_VALIDATION_RULES}

_FEATURE_ABBREV = {
    "price_percentile": "price",
    "rating_percentile": "rating",
    "release_date_percentile": "recency",
    "frequency_percentile": "frequency",
    "search_rank_percentile": "search",
}

# Same aggregation as items/59_items_user_parent.py REFRESH_SQL.
TRAINING_SQL = text(
    """
    WITH agg AS (
      SELECT
        iu.user_id AS user_id,
        COALESCE(iu.parent_asin, iu.asin) AS parent_asin,
        AVG(iu.price_percentile) AS price_percentile,
        AVG(iu.rating_percentile) AS rating_percentile,
        AVG(iu.release_date_percentile) AS release_date_percentile,
        AVG(iu.frequency_percentile) AS frequency_percentile,
        AVG(iu.search_rank_percentile) AS search_rank_percentile,
        COUNT(*) AS purchase_count,
        MIN(iu.purchase_datetime) AS first_purchase,
        MAX(iu.purchase_datetime) AS last_purchase,
        COUNT(*) / GREATEST(DATEDIFF(NOW(), MIN(iu.purchase_datetime)), 1) AS user_frequency
      FROM items_user iu
      WHERE iu.is_relevant = 1
      GROUP BY iu.user_id, COALESCE(iu.parent_asin, iu.asin)
      HAVING COUNT(*) > 0
    )
    SELECT
      agg.price_percentile,
      agg.rating_percentile,
      agg.release_date_percentile,
      agg.frequency_percentile,
      agg.search_rank_percentile,
      agg.parent_asin,
      agg.user_id,
      agg.purchase_count,
      agg.first_purchase,
      agg.last_purchase,
      agg.user_frequency,
      PERCENT_RANK() OVER (
        PARTITION BY agg.user_id ORDER BY agg.user_frequency
      ) AS user_frequency_percentile
    FROM agg
    """
)


def items_signs_valid(feature_columns, coefficients):
    """Same direction logic as items/81_items_regression.py."""
    for feat in feature_columns:
        rule = _FIELD_RULE.get(feat)
        if rule is None:
            continue
        c = coefficients.get(feat)
        if c is None or (isinstance(c, float) and np.isnan(c)):
            return False
        cond = rule["condition"]
        if cond == "positive" and c > 0:
            return False
        if cond == "negative" and c < 0:
            return False
    return True


def _regression_row_dict(run_label, feature_columns, coefficients, constant, r2, rmse, mae):
    return {
        "run_label": run_label,
        "features": ", ".join(feature_columns),
        "n_features": len(feature_columns),
        "signs_valid": items_signs_valid(feature_columns, coefficients),
        "r2": r2,
        "rmse": rmse,
        "mae": mae,
        "constant": constant,
        "coef_price": coefficients.get("price_percentile"),
        "coef_rating": coefficients.get("rating_percentile"),
        "coef_recency": coefficients.get("release_date_percentile"),
        "coef_frequency": coefficients.get("frequency_percentile"),
        "coef_search": coefficients.get("search_rank_percentile"),
    }


def load_training_data():
    """
    One row per user × parent product, matching items_user_parent after
    items/59_items_user_parent.py. Target is user_frequency_percentile.
    """
    logger.info("Loading training data (items_user_parent equivalent)...")
    engine = reccd_items.mysqlengine()
    df = pd.read_sql(TRAINING_SQL, engine)
    logger.info("Loaded %s training examples", len(df))
    if len(df) and TARGET_COLUMN in df.columns and df[TARGET_COLUMN].notna().any():
        logger.info(
            "%s range: %.4f to %.4f",
            TARGET_COLUMN,
            df[TARGET_COLUMN].min(),
            df[TARGET_COLUMN].max(),
        )
    return df, engine


def run_regression_with_features(df, feature_columns):
    """OLS with intercept (same estimator as items/81_items_regression.py)."""
    cols = list(dict.fromkeys(feature_columns + [TARGET_COLUMN]))
    work = df[cols].copy()
    if "diversity_percentile" in work.columns:
        work["diversity_percentile"] = (
            pd.to_numeric(work["diversity_percentile"], errors="coerce").fillna(0.0)
        )
    work = work.dropna(subset=feature_columns + [TARGET_COLUMN])
    y = work[TARGET_COLUMN].astype(float)
    X_with_const = sm.add_constant(work[feature_columns].astype(float))
    fit = sm.OLS(y, X_with_const).fit()

    y_pred = fit.fittedvalues
    mse = mean_squared_error(y, y_pred)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y, y_pred)
    r2 = float(fit.rsquared)

    params = fit.params
    constant = float(params.iloc[0])
    coefficients = {
        feat: float(params.iloc[i])
        for i, feat in enumerate(feature_columns, start=1)
    }
    return coefficients, constant, fit, None, r2, rmse, mae


def build_all_combination_runs(df, all_features):
    """OLS on every nonempty subset, skipping EXCLUDED_FEATURE_COMBINATIONS."""
    rows = []
    n = len(all_features)
    for k in range(n, 0, -1):
        for combo in combinations(all_features, k):
            feat_list = list(combo)
            if _combo_key(feat_list) in _EXCLUDED_COMBO_KEYS:
                logger.info("Skipping excluded feature combination: %s", ", ".join(feat_list))
                continue
            coefficients, constant, _, _, r2, rmse, mae = run_regression_with_features(
                df, feat_list
            )
            abbrev = "+".join(_FEATURE_ABBREV[f] for f in feat_list)
            rows.append(
                _regression_row_dict(
                    f"combo_{abbrev}", feat_list, coefficients, constant, r2, rmse, mae
                )
            )
    return rows


def perform_regression_analysis(df):
    """
    Fit every nonempty feature subset and select the best signs_valid model.
    Same selection as items/81_items_regression.py.
    """
    logger.info("Performing OLS regression analysis across feature combinations...")

    all_features = [
        "price_percentile",
        "rating_percentile",
        "release_date_percentile",
        "frequency_percentile",
        "search_rank_percentile",
    ]

    current_features = all_features.copy()
    regression_runs = []
    coefficients, constant, model, _, r2, rmse, mae = run_regression_with_features(
        df, current_features
    )
    regression_runs.append(
        _regression_row_dict("initial", current_features, coefficients, constant, r2, rmse, mae)
    )
    for rule in FEATURE_VALIDATION_RULES:
        feature = rule["feature"]
        condition = rule["condition"]
        if feature not in current_features:
            continue
        coef_value = coefficients[feature]
        should_remove = (condition == "positive" and coef_value > 0) or (
            condition == "negative" and coef_value < 0
        )
        if not should_remove:
            logger.info("Keeping %s (coefficient: %.6f) - condition not met", feature, coef_value)
            continue
        logger.info(rule["reason"])
        logger.info(
            "Removing %s (coefficient: %.6f) and rerunning regression...",
            feature,
            coef_value,
        )
        current_features.remove(feature)
        coefficients, constant, model, _, r2, rmse, mae = run_regression_with_features(
            df, current_features
        )
        regression_runs.append(
            _regression_row_dict(
                f"drop_{feature}",
                current_features,
                coefficients,
                constant,
                r2,
                rmse,
                mae,
            )
        )

    if regression_runs:
        runs_df = pd.DataFrame(regression_runs).sort_values(
            by=["signs_valid", "r2", "rmse", "mae", "n_features"],
            ascending=[False, False, True, True, True],
        ).reset_index(drop=True)
        print("\n=== VALIDATION PATH (diagnostic only; final weights from best signs_valid combo) ===")
        print(runs_df.drop(columns=["run_label"], errors="ignore").to_string(index=False))
        print("")

    combo_runs = build_all_combination_runs(df, all_features)
    if not combo_runs:
        raise RuntimeError("No feature combination regressions succeeded")

    combo_df = pd.DataFrame(combo_runs).sort_values(
        by=["signs_valid", "r2", "rmse", "mae", "n_features"],
        ascending=[False, False, True, True, False],
    ).reset_index(drop=True)
    print(
        "\n=== ALL FEATURE COMBINATIONS "
        "(signs_valid required for acceptance; price must not be positive) ==="
    )
    print(combo_df.drop(columns=["run_label"], errors="ignore").to_string(index=False))
    print("")

    valid_runs = [r for r in combo_runs if r["signs_valid"]]
    if valid_runs:
        best = max(valid_runs, key=lambda r: (r["r2"], -r["rmse"], -r["mae"], r["n_features"]))
        logger.info("Selected best model with valid coefficient signs")
    else:
        best = max(combo_runs, key=lambda r: (r["r2"], -r["rmse"], -r["mae"], r["n_features"]))
        logger.warning(
            "No models with valid signs found! Using best R² model anyway "
            "(price may need NULL fallback at score time)"
        )

    best_features = [f.strip() for f in best["features"].split(",") if f.strip()]
    coefficients, constant, model, _, r2, rmse, mae = run_regression_with_features(
        df, best_features
    )

    final_coefficients = {feat: None for feat in all_features}
    final_coefficients.update(coefficients)

    price_coef = final_coefficients.get("price_percentile")
    if price_coef is not None and price_coef > 0:
        logger.warning(
            "Rejecting positive price coefficient %.6f — saving NULL "
            "(score time applies negative fallback)",
            price_coef,
        )
        final_coefficients["price_percentile"] = None

    print(f"\nBest model features: {', '.join(best_features)}")
    print(f"  R²: {r2:.6f}")
    print(f"  RMSE: {rmse:.6f}")
    print(f"  MAE: {mae:.6f}")
    print(f"  Signs valid: {best['signs_valid']}")
    print("")

    logger.info("Final coefficients:")
    logger.info("  Constant: %.6f", constant)
    for feature, coef in final_coefficients.items():
        if coef is None:
            logger.info("  %s: NULL", feature)
        else:
            logger.info("  %s: %.6f", feature, coef)

    return final_coefficients, constant, model, None


def update_user_table(engine, coefficients, constant):
    """Update user table with learned coefficients."""
    logger.info("Updating user table with learned coefficients...")
    query = """
        UPDATE user
        SET
            item_monetary = :item_monetary,
            item_rating = :item_rating,
            item_recency = :item_recency,
            item_frequency = :item_frequency,
            item_search = :item_search
        WHERE email = :email
    """
    params = {
        "item_monetary": coefficients["price_percentile"],
        "item_rating": coefficients["rating_percentile"],
        "item_recency": coefficients["release_date_percentile"],
        "item_frequency": coefficients["frequency_percentile"],
        "item_search": coefficients["search_rank_percentile"],
        "email": EMAIL,
    }
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        conn.commit()
        if result.rowcount > 0:
            logger.debug("Successfully updated user preferences for %s", EMAIL)
        else:
            logger.warning("No user found with email %s", EMAIL)
    logger.info("User table update completed")


def print_learned_weights(coefficients, constant):
    logger.info("Learned weights from OLS regression (predicting %s):", TARGET_COLUMN)
    print("\n" + "=" * 60)
    print(f"LEARNED WEIGHTS (for {TARGET_COLUMN} prediction)")
    print("=" * 60)
    print(f"{'Feature':<25} | {'Weight':<10}")
    print("-" * 60)
    for feature, weight in coefficients.items():
        if weight is None:
            print(f"{feature:<25} | {'NULL':>9}")
        else:
            print(f"{feature:<25} | {weight:>9.6f}")
    print(f"{'Constant':<25} | {constant:>9.6f}")
    print("=" * 60)


def main():
    try:
        logger.info("Starting items regression analysis...")
        df, engine = load_training_data()
        if len(df) == 0:
            logger.error("No training data found!")
            return
        coefficients, constant, model, _ = perform_regression_analysis(df)
        print_learned_weights(coefficients, constant)
        update_user_table(engine, coefficients, constant)
        logger.info("Items regression analysis completed successfully!")
    except Exception as e:
        logger.error("Error in regression analysis: %s", e)
        raise
    finally:
        if "engine" in locals():
            engine.dispose()


if __name__ == "__main__":
    main()
    print()
