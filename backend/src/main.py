"""
Trading Simulator FastAPI Application.

This is the main entry point for the Trading Simulator API.
"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from simutrador_core.utils import configure_third_party_loggers, setup_logger

from api import data_analysis_router, simulation_router, trading_data_router
from api.nightly_update import router as nightly_update_router

# Configure standardized logging for the application
logger = setup_logger(
    name="data_manager",
    log_dir=Path.cwd() / "logs",
    console_level=logging.INFO,
    file_level=logging.ERROR,
)

# Configure third-party loggers to reduce noise
configure_third_party_loggers()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application lifespan events."""
    # Startup
    logger.info("Trading Simulator API starting up...")
    logger.info(
        "Available endpoints: /docs, /trading-data, /nightly-update, /data-analysis"
    )
    yield
    # Shutdown (if needed)


# Create FastAPI application with metadata
app = FastAPI(
    lifespan=lifespan,
    title="Trading Simulator API",
    description="""
    A stateless execution engine designed to simulate the execution of trading orders
    against historical market data. The simulator focuses on deterministic execution
    and evaluation of submitted orders without performing signal generation or strategy logic.

    ## Features

    * Historical OHLCV-based simulation
    * Support for daily and intraday intervals (1m to 4h)
    * Market and limit order types
    * Bracket orders with stop loss and take profit
    * Comprehensive performance metrics
    * Multiple orders per simulation request

    ## Usage

    Submit trading orders via the `/api/v1/simulate` endpoint and receive detailed
    execution results including individual trade outcomes and performance metrics.
    """,
    version="1.0.0",
    contact={
        "name": "Trading Simulator API",
        "url": "https://github.com/your-repo/trading-simulator",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",  # Allow any localhost port for development
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(simulation_router)
app.include_router(trading_data_router)
app.include_router(nightly_update_router)
app.include_router(data_analysis_router)


@app.get("/", tags=["root"])
async def read_root() -> dict[str, str]:
    """
    Root endpoint providing basic API information.

    Returns:
        dict: Basic API information
    """
    return {
        "message": "Trading Simulator API",
        "version": "1.0.0",
        "docs": "/docs",
        "openapi": "/openapi.json",
    }
