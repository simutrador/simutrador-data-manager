"""
Trading Simulator API Models.

This module contains all Pydantic models used for request/response validation
and serialization in the Trading Simulator API.
"""

from .enums import OrderSide, OrderType, TradeResult
from .orders import Order, SimulationRequest
from .price_data import (
    DataUpdateStatus,
    PriceCandle,
    PriceDataSeries,
    PriceQuote,
    Timeframe,
)
from .responses import ErrorResponse, SimulationMetrics, SimulationResponse, Trade

__all__ = [
    # Enums
    "OrderType",
    "OrderSide",
    "TradeResult",
    "Timeframe",
    # Request models
    "Order",
    "SimulationRequest",
    # Response models
    "Trade",
    "SimulationMetrics",
    "SimulationResponse",
    "ErrorResponse",
    # Price data models
    "PriceCandle",
    "PriceDataSeries",
    "PriceQuote",
    "DataUpdateStatus",
]
