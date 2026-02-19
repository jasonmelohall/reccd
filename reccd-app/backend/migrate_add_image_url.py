#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Database migration script to add image_url column to items table
and create index on search_term for better query performance.
"""

from sqlalchemy import text
from database import get_db_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def migrate():
    """Run database migration"""
    with get_db_connection() as conn:
        try:
            # Add image_url column if it doesn't exist
            logger.info("Adding image_url column to items table...")
            conn.execute(text("""
                ALTER TABLE items 
                ADD COLUMN IF NOT EXISTS image_url VARCHAR(500)
            """))
            conn.commit()
            logger.info("✅ image_url column added successfully")
            
        except Exception as e:
            if "Duplicate column name" in str(e):
                logger.info("ℹ️  image_url column already exists, skipping")
            else:
                logger.error(f"❌ Error adding image_url column: {e}")
                raise
        
        try:
            # Create index on search_term if it doesn't exist
            logger.info("Creating index on search_term...")
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_search_term ON items(search_term)
            """))
            conn.commit()
            logger.info("✅ Index on search_term created successfully")
            
        except Exception as e:
            if "Duplicate key name" in str(e):
                logger.info("ℹ️  Index on search_term already exists, skipping")
            else:
                logger.error(f"❌ Error creating index: {e}")
                raise
        
        # Verify changes
        logger.info("Verifying changes...")
        result = conn.execute(text("DESCRIBE items")).fetchall()
        columns = [row[0] for row in result]
        
        if 'image_url' in columns:
            logger.info("✅ Migration completed successfully!")
        else:
            logger.error("❌ Migration verification failed - image_url column not found")


if __name__ == "__main__":
    migrate()



