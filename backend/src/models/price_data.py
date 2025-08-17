"""
Price data models for the Trading Simulator API.

These models represent price candle data from external APIs and internal storage.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional, override

from pydantic import BaseModel, Field, field_serializer


class Timeframe(str, Enum):
    """Supported timeframes for price data and simulation."""

    ONE_MIN = "1min"
    FIVE_MIN = "5min"
    FIFTEEN_MIN = "15min"
    THIRTY_MIN = "30min"
    ONE_HOUR = "1h"
    TWO_HOUR = "2h"
    FOUR_HOUR = "4h"
    DAILY = "daily"


class PriceCandle(BaseModel):
    """
    Individual price candle model representing OHLCV data.

    This matches the format returned by Financial Modeling Prep API:
    {
        "date": "2025-07-03 12:59:00",
        "open": 213.57,
        "low": 213.32,
        "high": 213.69,
        "close": 213.58,
        "volume": 670856
    }
    """

    date: datetime = Field(..., description="Timestamp of the candle")
    open: Decimal = Field(..., description="Opening price", gt=0)
    low: Decimal = Field(..., description="Lowest price", gt=0)
    high: Decimal = Field(..., description="Highest price", gt=0)
    close: Decimal = Field(..., description="Closing price", gt=0)
    volume: Decimal = Field(..., description="Trading volume", ge=0)

    @field_serializer("open", "high", "low", "close")
    def serialize_price(self, value: Decimal) -> str:
        """Serialize price fields to 2 decimal places."""
        return f"{value:.2f}"

    @field_serializer("volume")
    def serialize_volume(self, value: Decimal) -> str:
        """Serialize volume field to 8 decimal places for crypto precision."""
        return f"{value:.8f}"

    @override
    def model_post_init(self, __context: Any) -> None:
        """Validate that high >= low and open/close are within range."""
        if self.high < self.low:
            raise ValueError("High price must be greater than or equal to low price")
        if not (self.low <= self.open <= self.high):
            raise ValueError("Open price must be between low and high prices")
        if not (self.low <= self.close <= self.high):
            raise ValueError("Close price must be between low and high prices")


class PaginationInfo(BaseModel):
    """
    Pagination information for data series.
    """

    page: int = Field(..., description="Current page number (1-based)", ge=1)
    page_size: int = Field(..., description="Number of items per page", ge=1, le=10000)
    total_items: int = Field(..., description="Total number of items available", ge=0)
    total_pages: int = Field(..., description="Total number of pages", ge=0)
    has_next: bool = Field(..., description="Whether there is a next page")
    has_previous: bool = Field(..., description="Whether there is a previous page")


class PriceDataSeries(BaseModel):
    """
    Collection of price candles for a specific symbol and timeframe.
    """

    symbol: str = Field(
        ..., description="Trading symbol (e.g., AAPL, MSFT)", min_length=1
    )
    timeframe: Timeframe = Field(..., description="Timeframe (e.g., 1min, 5min, daily)")
    candles: list[PriceCandle] = Field(..., description="List of price candles")
    start_date: Optional[datetime] = Field(
        default=None, description="Start date of the series"
    )
    end_date: Optional[datetime] = Field(
        default=None, description="End date of the series"
    )
    pagination: Optional[PaginationInfo] = Field(
        default=None, description="Pagination information (when paginated)"
    )

    @override
    def model_post_init(self, __context: Any) -> None:
        """Set start and end dates based on candles if not provided."""
        if self.candles:
            if self.start_date is None:
                self.start_date = min(candle.date for candle in self.candles)
            if self.end_date is None:
                self.end_date = max(candle.date for candle in self.candles)


class PriceQuote(BaseModel):
    """
    Real-time price quote model for current market data.
    """

    symbol: str = Field(..., description="Trading symbol", min_length=1)
    price: Decimal = Field(..., description="Current price", gt=0)
    timestamp: datetime = Field(..., description="Quote timestamp")
    bid: Optional[Decimal] = Field(None, description="Bid price", gt=0)
    ask: Optional[Decimal] = Field(None, description="Ask price", gt=0)
    volume: Optional[Decimal] = Field(None, description="Current volume", ge=0)

    @field_serializer("price", "bid", "ask")
    def serialize_price(self, value: Optional[Decimal]) -> Optional[str]:
        """Serialize price fields to 2 decimal places."""
        if value is None:
            return None
        return f"{value:.2f}"


class DataUpdateStatus(BaseModel):
    """
    Status information for data update operations.
    """

    symbol: str = Field(..., description="Trading symbol")
    timeframe: str = Field(..., description="Timeframe")
    last_update: Optional[datetime] = Field(
        ..., description="Last successful update timestamp"
    )
    records_updated: int = Field(..., description="Number of records updated", ge=0)
    success: bool = Field(..., description="Whether the update was successful")
    error_message: Optional[str] = Field(
        None, description="Error message if update failed"
    )
