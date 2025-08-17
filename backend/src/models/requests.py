"""
Request models for the Trading Simulator API.
"""

from datetime import datetime
from typing import List

from pydantic import BaseModel, Field

from simutrador_core.models.orders import Order
from simutrador_core.models.price_data import Timeframe


class SimulationRequest(BaseModel):
    """
    Request model for the trading simulation endpoint.
    """

    symbol: str = Field(..., description="Trading symbol (e.g., AAPL, MSFT)")
    timeframe: Timeframe = Field(..., description="Candle timeframe for simulation")
    start: datetime = Field(..., description="Start time for the simulation window (ISO 8601)")
    end: datetime = Field(..., description="End time for the simulation window (ISO 8601)")
    orders: List[Order] = Field(..., description="List of orders to simulate")
