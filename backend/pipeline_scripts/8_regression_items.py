#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import text
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import logging
from datetime import datetime

# Add shared directory to path for imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED_DIR = os.path.join(BASE_DIR, "shared")
sys.path.insert(0, SHARED_DIR)

import reccd_items

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
print()

# Configuration
EMAIL = "jasonmelohall@gmail.com"  # Your email for user table lookup

# Feature validation rules - check features in order and rerun regression after each removal
FEATURE_VALIDATION_RULES = [
    {
        'feature': 'price_percentile',
        'condition': 'positive',  # Remove if coefficient > 0
        'reason': 'Price coefficient positive - you buy expensive items more frequently'
    },
    {
        'feature': 'search_rank_percentile',
        'condition': 'positive',  # Remove if coefficient > 0
        'reason': 'Search rank coefficient positive - you buy items with worse search positions more frequently'
    },
    {
        'feature': 'frequency_percentile',
        'condition': 'negative',  # Remove if coefficient > 0
        'reason': 'Frequency coefficient negative - you buy less trendy items more frequently'
    },
    {
        'feature': 'release_date_percentile',
        'condition': 'negative',  # Remove if coefficient < 0
        'reason': 'Release date coefficient negative - you buy older items more frequently'
    },
    {
        'feature': 'rating_percentile', 
        'condition': 'negative',  # Remove if coefficient < 0
        'reason': 'Rating coefficient negative - you buy lower-rated items more frequently'
    }
]

def load_training_data():
    """
    Load training data from items_user table with user_frequency as target.
    Aggregates purchases by parent ASIN to treat variations as the same product.
    """
    logger.info("Loading training data from items_user table (aggregating by parent ASIN)...")
    
    engine = reccd_items.mysqlengine()
    
    query = text("""
        SELECT 
            iu.price_percentile,
            iu.rating_percentile,
            iu.release_date_percentile,
            iu.frequency_percentile,
            iu.search_rank_percentile,
            COALESCE(iu.parent_asin, iu.asin) as parent_asin,
            iu.user_id,
            COUNT(*) as purchase_count,
            MIN(iu.purchase_datetime) as first_purchase,
            MAX(iu.purchase_datetime) as last_purchase,
            COUNT(*) / GREATEST(DATEDIFF(NOW(), MIN(iu.purchase_datetime)), 1) as user_frequency
        FROM items_user iu
        WHERE iu.is_relevant = 1
        AND iu.price_percentile IS NOT NULL
        AND iu.rating_percentile IS NOT NULL
        AND iu.release_date_percentile IS NOT NULL
        AND iu.frequency_percentile IS NOT NULL
        AND iu.search_rank_percentile IS NOT NULL
        GROUP BY COALESCE(iu.parent_asin, iu.asin), iu.user_id
        HAVING purchase_count > 0
    """)
    
    df = pd.read_sql(query, engine)
    logger.info(f"Loaded {len(df)} training examples")
    logger.info(f"User frequency range: {df['user_frequency'].min():.4f} to {df['user_frequency'].max():.4f}")
    
    return df, engine

def run_regression_with_features(df, feature_columns):
    """Run Ridge regression with specified features"""
    X = df[feature_columns].fillna(0)
    y = df['user_frequency']
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Train Ridge Regression
    model = Ridge(alpha=1.0)
    model.fit(X_scaled, y)
    
    # Calculate performance metrics
    y_pred = model.predict(X_scaled)
    mse = mean_squared_error(y, y_pred)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y, y_pred)
    r2 = r2_score(y, y_pred)
    
    # Extract coefficients
    coefficients = dict(zip(feature_columns, model.coef_))
    constant = model.intercept_
    
    return coefficients, constant, model, scaler, r2, rmse, mae

def perform_regression_analysis(df):
    """Perform Ridge regression analysis with iterative feature validation"""
    logger.info("Performing Ridge regression analysis with feature validation...")
    
    # Initial feature set
    all_features = [
        'price_percentile',
        'rating_percentile', 
        'release_date_percentile',
        'frequency_percentile',
        'search_rank_percentile'
    ]
    
    current_features = all_features.copy()
    removed_features = {}
    
    # Run initial regression
    logger.info(f"Initial regression with features: {current_features}")
    coefficients, constant, model, scaler, r2, rmse, mae = run_regression_with_features(df, current_features)
    
    logger.info(f"Initial Ridge Regression Performance:")
    logger.info(f"  R² Score: {r2:.4f}")
    logger.info(f"  RMSE: {rmse:.4f}")
    logger.info(f"  MAE: {mae:.4f}")
    
    # Apply feature validation rules in sequence
    for rule in FEATURE_VALIDATION_RULES:
        feature = rule['feature']
        condition = rule['condition']
        reason = rule['reason']
        
        if feature not in current_features:
            logger.info(f"Skipping {feature} - already removed")
            continue
            
        coef_value = coefficients[feature]
        should_remove = False
        
        if condition == 'positive' and coef_value > 0:
            should_remove = True
        elif condition == 'negative' and coef_value < 0:
            should_remove = True
        
        if should_remove:
            logger.info(f"{reason}")
            logger.info(f"Removing {feature} (coefficient: {coef_value:.6f}) and rerunning regression...")
            
            # Remove feature and rerun
            current_features.remove(feature)
            removed_features[feature] = None
            
            # Rerun regression
            coefficients, constant, model, scaler, r2, rmse, mae = run_regression_with_features(df, current_features)
            
            logger.info(f"Regression Performance (without {feature}):")
            logger.info(f"  R² Score: {r2:.4f}")
            logger.info(f"  RMSE: {rmse:.4f}")
            logger.info(f"  MAE: {mae:.4f}")
        else:
            logger.info(f"Keeping {feature} (coefficient: {coef_value:.6f}) - condition not met")
    
    # Create final coefficients dict with removed features set to None
    final_coefficients = coefficients.copy()
    for feature in removed_features:
        final_coefficients[feature] = None
    
    logger.info(f"Final coefficients:")
    logger.info(f"  Constant: {constant:.6f}")
    for feature, coef in final_coefficients.items():
        if coef is None:
            logger.info(f"  {feature}: NULL")
        else:
            logger.info(f"  {feature}: {coef:.6f}")
    
    return final_coefficients, constant, model, scaler

def update_user_table(engine, coefficients, constant):
    """Update user table with learned coefficients"""
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
        'item_monetary': coefficients['price_percentile'],  # Will be NULL if price_percentile was removed
        'item_rating': coefficients['rating_percentile'],
        'item_recency': coefficients['release_date_percentile'],
        'item_frequency': coefficients['frequency_percentile'],
        'item_search': coefficients['search_rank_percentile'],  # Using item_search for search rank
        'email': EMAIL
    }
    
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        conn.commit()
        
        if result.rowcount > 0:
            logger.debug(f"Successfully updated user preferences for {EMAIL}")
        else:
            logger.warning(f"No user found with email {EMAIL}")
    
    logger.info("User table update completed")

def print_learned_weights(coefficients, constant):
    """Print the learned weights in a table format"""
    logger.info("Learned weights from Ridge regression (predicting user_frequency):")
    
    print("\n" + "="*60)
    print("LEARNED WEIGHTS (for user_frequency prediction)")
    print("="*60)
    print(f"{'Feature':<25} | {'Weight':<10}")
    print("-" * 60)
    
    for feature, weight in coefficients.items():
        if weight is None:
            print(f"{feature:<25} | {'NULL':>9}")
        else:
            print(f"{feature:<25} | {weight:>9.6f}")
    
    print(f"{'Constant':<25} | {constant:>9.6f}")
    print("="*60)

def main():
    """Main regression analysis function"""
    try:
        logger.info("Starting items regression analysis...")
        
        # Load training data
        df, engine = load_training_data()
        
        if len(df) == 0:
            logger.error("No training data found!")
            return
        
        # Perform regression analysis
        coefficients, constant, model, scaler = perform_regression_analysis(df)
        
        # Print learned weights
        print_learned_weights(coefficients, constant)
        
        # Update user table
        update_user_table(engine, coefficients, constant)
        
        logger.info("Items regression analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in regression analysis: {e}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    main()
    print()