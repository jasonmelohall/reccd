#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import items
from models.schemas import HealthResponse
from database import test_db_connection
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Reccd Items API",
    description="Backend API for personalized Amazon product recommendations",
    version="1.0.0"
)

# CORS configuration for mobile/web clients
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(items.router)


@app.get("/", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    db_status = "connected" if test_db_connection() else "disconnected"
    
    return HealthResponse(
        status="healthy",
        database=db_status,
        timestamp=datetime.utcnow()
    )


@app.on_event("startup")
async def startup_event():
    """Run on application startup"""
    logger.info("Starting Reccd Items API...")
    db_connected = test_db_connection()
    if db_connected:
        logger.info("Database connection successful")
    else:
        logger.error("Database connection failed!")


@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown"""
    logger.info("Shutting down Reccd Items API...")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)



