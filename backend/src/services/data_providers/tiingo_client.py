"""
Tiingo API client for fetching trading data.

This service handles API calls to Tiingo, including:
- Authentication with API key
- Rate limiting
- Error handling and retries
- Data parsing and validation
"""

import logging
from datetime import date
from types import TracebackType
from typing import override

import httpx
from simutrador_core.models.price_data import PriceCandle, PriceDataSeries, Timeframe

from core.settings import get_settings

from .data_provider_interface import (
    DataProviderError,
    DataProviderInterface,
)

logger = logging.getLogger(__name__)


class TiingoError(DataProviderError):
    """Base exception for Tiingo API errors."""

    pass


class TiingoClient(DataProviderInterface):
    """
    Client for Tiingo API.

    Handles fetching historical price data with proper rate limiting,
    error handling, and data validation.

    Note: This is a stub implementation. Full implementation would require
    understanding Tiingo's API structure and rate limits.
    """

    def __init__(self):
        """Initialize the client with settings."""
        self.settings = get_settings()
        self.tiingo_settings = self.settings.tiingo
        self.client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )
        self._last_request_time = 0.0
        self._request_count = 0

    @override
    async def __aenter__(self) -> "TiingoClient":
        """Async context manager entry."""
        return self

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.client.aclose()

    @override
    async def fetch_historical_data(
        self,
        symbol: str,
        timeframe: str = "1min",
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> PriceDataSeries:
        """
        Fetch historical price data for a symbol.

        Args:
            symbol: Trading symbol (e.g., "AAPL")
            timeframe: Timeframe for data (e.g., "1min", "5min", "1day", "daily")
            from_date: Start date for data
            to_date: End date for data

        Returns:
            PriceDataSeries containing the fetched data

        Raises:
            TiingoError: If API request fails
            ValidationError: If response data is invalid
        """
        # Stub implementation - would need to implement actual Tiingo API calls
        logger.warning("TiingoClient is a stub implementation - returning empty data")

        return PriceDataSeries(
            symbol=symbol, timeframe=Timeframe(timeframe), candles=[]
        )

    @override
    async def fetch_latest_data(
        self, symbol: str, timeframe: str = "1min"
    ) -> PriceCandle | None:
        """
        Fetch the latest price data for a symbol.

        Args:
            symbol: Trading symbol
            timeframe: Timeframe for data

        Returns:
            Latest PriceCandle or None if no data available
        """
        # Stub implementation - would need to implement actual Tiingo API calls
        logger.warning("TiingoClient is a stub implementation - returning None")
        return None

    @override
    def get_resampling_metadata(self) -> dict[str, str]:
        """
        Get Tiingo specific resampling metadata.

        Tiingo alignment would need to be determined based on their API documentation.
        For now, using market session alignment as default.
        """
        return {
            "alignment_strategy": "market_session",
            "daily_boundary": "market_close",
            "intraday_alignment": "session_aligned",
        }
