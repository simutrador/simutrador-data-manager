"""
Order-related Pydantic models for the Trading Simulator API.
"""

from datetime import datetime
from typing import Any, Optional, override

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from .enums import OrderSide, OrderType
from .price_data import Timeframe


class Order(BaseModel):
    """
    Individual order model representing a trading instruction.
    """

    entry_time: datetime = Field(
        ..., description="ISO 8601 timestamp when the order should be entered"
    )
    entry_type: OrderType = Field(
        ..., description="Order type: MKT (market) or LMT (limit)"
    )
    entry_price: Optional[float] = Field(
        None,
        description="Entry price for limit orders (ignored for market orders)",
        gt=0,
    )
    side: OrderSide = Field(..., description="Order side: buy or sell")
    stop_loss: Optional[float] = Field(None, description="Stop loss price level", gt=0)
    take_profit: Optional[float] = Field(
        None, description="Take profit price level", gt=0
    )

    @field_validator("entry_price")
    @classmethod
    def validate_entry_price(
        cls, v: Optional[float], info: ValidationInfo
    ) -> Optional[float]:
        """Validate entry price is required for limit orders."""
        if info.data.get("entry_type") == OrderType.LIMIT and v is None:
            raise ValueError("entry_price is required for limit orders")
        return v

    @field_validator("stop_loss", "take_profit")
    @classmethod
    def validate_price_levels(cls, v: Optional[float]) -> Optional[float]:
        """Validate price levels are positive if provided."""
        if v is not None and v <= 0:
            raise ValueError("Price levels must be positive")
        return v

    @override
    def model_post_init(self, __context: Any) -> None:
        """Additional validation after model initialization."""
        if self.entry_type == OrderType.LIMIT and self.entry_price is None:
            raise ValueError("entry_price is required for limit orders")

        # Validate stop loss and take profit levels make sense relative to entry price
        if self.entry_price is not None:
            if self.side == OrderSide.BUY:
                if self.stop_loss is not None and self.stop_loss >= self.entry_price:
                    raise ValueError(
                        "Stop loss must be below entry price for buy orders"
                    )
                if (
                    self.take_profit is not None
                    and self.take_profit <= self.entry_price
                ):
                    raise ValueError(
                        "Take profit must be above entry price for buy orders"
                    )
            else:  # SELL
                if self.stop_loss is not None and self.stop_loss <= self.entry_price:
                    raise ValueError(
                        "Stop loss must be above entry price for sell orders"
                    )
                if (
                    self.take_profit is not None
                    and self.take_profit >= self.entry_price
                ):
                    raise ValueError(
                        "Take profit must be below entry price for sell orders"
                    )


class SimulationRequest(BaseModel):
    """
    Request model for the trading simulation endpoint.
    """

    symbol: str = Field(
        ...,
        description="Trading symbol (e.g., AAPL, MSFT)",
        min_length=1,
        max_length=20,
    )
    timeframe: Timeframe = Field(..., description="Candle timeframe for simulation")
    start: datetime = Field(
        ..., description="Start time for the simulation window (ISO 8601)"
    )
    end: datetime = Field(
        ..., description="End time for the simulation window (ISO 8601)"
    )
    orders: list[Order] = Field(
        ..., description="List of orders to simulate", min_length=1
    )

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Validate symbol format."""
        if not v.isalnum():
            raise ValueError("Symbol must contain only alphanumeric characters")
        return v.upper()

    @field_validator("end")
    @classmethod
    def validate_end_after_start(cls, v: datetime, info: ValidationInfo) -> datetime:
        """Validate end time is after start time."""
        if "start" in info.data and v <= info.data["start"]:
            raise ValueError("End time must be after start time")
        return v

    @field_validator("orders")
    @classmethod
    def validate_orders_within_timeframe(
        cls, v: list[Order], info: ValidationInfo
    ) -> list[Order]:
        """Validate all orders are within the simulation timeframe."""
        if "start" not in info.data or "end" not in info.data:
            return v

        start_time = info.data["start"]
        end_time = info.data["end"]

        for order in v:
            if order.entry_time < start_time or order.entry_time > end_time:
                raise ValueError(
                    f"Order entry time {order.entry_time} is outside simulation window "
                    f"({start_time} to {end_time})"
                )

        return v
