"""
Abstract interface for trading data providers.

This module defines the common interface that all trading data providers must implement,
enabling vendor-agnostic data fetching with consistent error handling and data formats.
"""

from abc import ABC, abstractmethod
from datetime import date
from types import TracebackType
from typing import Dict, Optional

from simutrador_core.models.price_data import PriceCandle, PriceDataSeries


class DataProviderError(Exception):
    """Base exception for data provider errors."""

    pass


class AuthenticationError(DataProviderError):
    """Raised when API authentication fails."""

    pass


class RateLimitError(DataProviderError):
    """Raised when API rate limit is exceeded."""

    pass


class DataProviderInterface(ABC):
    """
    Abstract interface for trading data providers.

    All data provider implementations must inherit from this class and implement
    the required methods. This ensures consistent behavior across different vendors.
    """

    @abstractmethod
    async def fetch_historical_data(
        self,
        symbol: str,
        timeframe: str = "1min",
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
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
            DataProviderError: If API request fails
            AuthenticationError: If authentication fails
            RateLimitError: If rate limit is exceeded
        """
        pass

    @abstractmethod
    async def fetch_latest_data(
        self, symbol: str, timeframe: str = "1min"
    ) -> Optional[PriceCandle]:
        """
        Fetch the latest price data for a symbol.

        Args:
            symbol: Trading symbol
            timeframe: Timeframe for data

        Returns:
            Latest PriceCandle or None if no data available

        Raises:
            DataProviderError: If API request fails
            AuthenticationError: If authentication fails
            RateLimitError: If rate limit is exceeded
        """
        pass

    @abstractmethod
    async def __aenter__(self) -> "DataProviderInterface":
        """Async context manager entry."""
        pass

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Async context manager exit."""
        pass

    def get_resampling_metadata(self) -> Dict[str, str]:
        """
        Get provider-specific resampling metadata.

        This method returns information about how this provider aligns its data,
        which can be used by the resampling service to match the provider's
        native aggregation behavior.

        Returns:
            Dictionary with resampling metadata:
            - 'alignment_strategy': 'market_session', 'utc_midnight', 'market_close'
            - 'daily_boundary': 'market_close', 'utc_midnight'
            - 'intraday_alignment': 'session_aligned', 'utc_aligned'
        """
        # Default implementation - providers can override
        return {
            "alignment_strategy": "market_session",
            "daily_boundary": "market_close",
            "intraday_alignment": "session_aligned",
        }
