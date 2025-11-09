#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlalchemy
from sqlalchemy import create_engine, text
from contextlib import contextmanager
from config import get_settings
import logging

logger = logging.getLogger(__name__)
settings = get_settings()


def get_engine():
    """Create and return SQLAlchemy engine"""
    connection_string = (
        f"mysql+pymysql://{settings.db_user}:{settings.db_password}"
        f"@{settings.db_host}:3306/{settings.db_name}?charset=utf8mb4"
    )
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=5,
        max_overflow=10
    )
    return engine


# Global engine instance
engine = get_engine()


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


def test_db_connection():
    """Test database connection"""
    try:
        with get_db_connection() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            return result is not None
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False



