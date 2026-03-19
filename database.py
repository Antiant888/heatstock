# -*- coding: utf-8 -*-
"""
Database Configuration Module

Handles MySQL database connection and table creation for Gelonghui HK Stock data.
Uses environment variables for database credentials (Railway compatible).

Author: jasperchan
"""

import os
import logging
from sqlalchemy import create_engine, Column, String, BigInteger, Text, Boolean, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

logger = logging.getLogger(__name__)

# SQLAlchemy Base
Base = declarative_base()

# ────────────────────────────────────────────────
# Database Table Schema
# ────────────────────────────────────────────────

class HKStockLive(Base):
    """SQLAlchemy model for HK Stock Live data from Gelonghui"""
    __tablename__ = 'hk_stock_lives'
    
    id = Column(String(50), primary_key=True)
    title = Column(Text, nullable=True)
    create_timestamp = Column(BigInteger, nullable=True, index=True)
    update_timestamp = Column(BigInteger, nullable=True)
    count = Column(Text, nullable=True)  # JSON string
    statistic = Column(Text, nullable=True)  # JSON string
    content = Column(Text, nullable=True)
    content_prefix = Column(Text, nullable=True)
    related_stocks = Column(Text, nullable=True)  # JSON string
    related_infos = Column(Text, nullable=True)  # JSON string
    pictures = Column(Text, nullable=True)  # JSON string
    related_articles = Column(Text, nullable=True)  # JSON string
    source = Column(Text, nullable=True)  # JSON string
    interpretation = Column(Text, nullable=True)
    level = Column(BigInteger, nullable=True)
    route = Column(Text, nullable=True)
    close_comment = Column(Boolean, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

# ────────────────────────────────────────────────
# Database Connection
# ────────────────────────────────────────────────

def get_database_url():
    """
    Build database URL from environment variables.
    Supports both Railway's DATABASE_URL and individual MySQL credentials.
    """
    # Try Railway's DATABASE_URL first
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        # Fix for SQLAlchemy 2.0+ (Railway uses mysql://, need mysql+pymysql://)
        if database_url.startswith("mysql://"):
            database_url = database_url.replace("mysql://", "mysql+pymysql://", 1)
        logger.info("Using DATABASE_URL from environment")
        return database_url
    
    # Fall back to individual credentials
    host = os.getenv("MYSQL_HOST", "localhost")
    port = os.getenv("MYSQL_PORT", "3306")
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DATABASE", "gelonghui")
    
    database_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    logger.info(f"Using MySQL connection: {host}:{port}/{database}")
    return database_url

def create_database_engine():
    """Create SQLAlchemy engine with connection pooling"""
    database_url = get_database_url()
    
    engine = create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,  # Recycle connections after 30 minutes
        echo=False
    )
    
    return engine

def init_database(engine):
    """Initialize database tables"""
    try:
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Failed to create database tables: {e}")
        raise

def get_session(engine):
    """Create a new database session"""
    Session = sessionmaker(bind=engine)
    return Session()

# ────────────────────────────────────────────────
# Checkpoint Management (Database-based)
# ────────────────────────────────────────────────

def load_last_timestamp_db(session):
    """Load the last timestamp from database"""
    try:
        # Get the record with the smallest create_timestamp (oldest fetched)
        result = session.query(HKStockLive.create_timestamp)\
            .order_by(HKStockLive.create_timestamp.asc())\
            .first()
        
        if result and result[0]:
            timestamp = result[0]
            logger.info(f"Loaded checkpoint timestamp from DB: {timestamp}")
            return int(timestamp)
    except Exception as e:
        logger.warning(f"Failed to load checkpoint from database: {e}")
    
    return None

# ────────────────────────────────────────────────
# Main entry point for testing
# ────────────────────────────────────────────────

if __name__ == "__main__":
    # Test database connection
    logging.basicConfig(level=logging.INFO)
    
    try:
        engine = create_database_engine()
        init_database(engine)
        session = get_session(engine)
        
        # Test query
        count = session.query(HKStockLive).count()
        logger.info(f"Current records in database: {count}")
        
        session.close()
        logger.info("Database connection test successful!")
        
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")