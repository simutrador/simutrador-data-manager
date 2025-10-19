"""
Polygon URL generator utility for creating API URLs for missing data periods.

This service generates Polygon API URLs for any given time periods,
allowing manual verification of data availability.
"""

import logging
from datetime import datetime, timedelta

from core.settings import get_settings

logger = logging.getLogger(__name__)


class PolygonUrlGenerator:
    """Utility service for generating Polygon API URLs."""

    def __init__(self):
        """Initialize the Polygon URL generator."""
        self.settings = get_settings()

    def generate_urls_for_missing_periods(
        self, symbol: str, missing_periods: list[tuple[datetime, datetime]]
    ) -> list[str]:
        """
        Generate Polygon API URLs for a list of missing time periods.

        Args:
            symbol: Trading symbol
            missing_periods: List of (start_time, end_time) tuples for missing periods

        Returns:
            List of Polygon API URLs, one for each missing period
        """
        urls: list[str] = []

        for start_time, end_time in missing_periods:
            url = self.generate_url_for_period(symbol, start_time, end_time)
            urls.append(url)

        return urls

    def generate_url_for_period(
        self, symbol: str, start_time: datetime, end_time: datetime
    ) -> str:
        """
        Generate a Polygon API URL for a specific time period.

        Uses either v3/trades or v2/aggs endpoint based on the plan configuration.

        Args:
            symbol: Trading symbol
            start_time: Start of the time period
            end_time: End of the time period

        Returns:
            Polygon API URL for this specific time range
        """
        try:
            if self.settings.polygon.use_trades_endpoint_for_gaps:
                # Use trades endpoint (requires higher-tier plan)
                return self.generate_trades_url_for_period(symbol, start_time, end_time)
            else:
                # Use aggregates endpoint (available on all plans)
                return self._generate_aggregates_url_for_period(
                    symbol, start_time, end_time
                )

        except Exception as e:
            logger.error(f"Error generating Polygon URL for {symbol}: {e}")
            return f"Error generating URL: {str(e)}"

    def _generate_aggregates_url_for_period(
        self, symbol: str, start_time: datetime, end_time: datetime
    ) -> str:
        """
        Generate a Polygon Aggregates API URL for a specific time period.

        Args:
            symbol: Trading symbol
            start_time: Start of the time period
            end_time: End of the time period

        Returns:
            Polygon Aggregates API URL for this specific time range
        """
        # Format timestamps for Polygon API (they use milliseconds since epoch)
        start_timestamp = int(start_time.timestamp() * 1000)
        # Polygon's 'to' parameter is INCLUSIVE, so we need to subtract 1 minute
        # to avoid requesting candles that exist beyond our missing period
        end_timestamp = int((end_time - timedelta(minutes=1)).timestamp() * 1000)

        # Polygon aggregates endpoint format
        # https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from}/{to}
        base_url = "https://api.polygon.io/v2/aggs/ticker"
        multiplier = 1
        timespan = "minute"

        url = (
            f"{base_url}/{symbol}/range/{multiplier}/{timespan}/"
            f"{start_timestamp}/{end_timestamp}"
            f"?adjusted=true&sort=asc&limit=50000"
            f"&apikey={self.settings.polygon.api_key}"
        )

        logger.debug(
            f"Generated Polygon Aggregates URL for {symbol} {start_time}-{end_time}: {url}"
        )
        return url

    def generate_url_for_date_range(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> str:
        """
        Generate a Polygon API URL for a full date range (for reference).

        Args:
            symbol: Trading symbol
            start_date: Start date
            end_date: End date

        Returns:
            Polygon API URL for the full date range
        """
        return self.generate_url_for_period(symbol, start_date, end_date)

    def generate_trades_url_for_period(
        self, symbol: str, start_time: datetime, end_time: datetime, limit: int = 50000
    ) -> str:
        """
        Generate a Polygon Trades API URL for a specific time period.

        This generates URLs for the v3/trades endpoint which uses nanosecond
        timestamps and different parameter names than the aggregates endpoint.

        Args:
            symbol: Trading symbol
            start_time: Start of the time period
            end_time: End of the time period
            limit: Maximum number of trades to return (max 50,000)

        Returns:
            Polygon Trades API URL for this specific time range
        """
        try:
            # Format timestamps for Polygon Trades API (nanoseconds since epoch)
            start_timestamp_ns = int(start_time.timestamp() * 1_000_000_000)
            end_timestamp_ns = int(end_time.timestamp() * 1_000_000_000)

            # Polygon trades endpoint format
            # https://api.polygon.io/v3/trades/{symbol}?timestamp.gte={start}&timestamp.lte={end}&limit={limit}
            base_url = "https://api.polygon.io/v3/trades"

            url = (
                f"{base_url}/{symbol}"
                f"?timestamp.gte={start_timestamp_ns}"
                f"&timestamp.lte={end_timestamp_ns}"
                f"&limit={min(limit, 50000)}"
                f"&apikey={self.settings.polygon.api_key}"
            )

            logger.debug(
                f"Generated Polygon Trades URL for {symbol} {start_time}-{end_time}: {url}"
            )
            return url

        except Exception as e:
            logger.error(f"Error generating Polygon Trades URL for {symbol}: {e}")
            return f"Error generating Trades URL: {str(e)}"

    def generate_trades_urls_for_missing_periods(
        self, symbol: str, missing_periods: list[tuple[datetime, datetime]]
    ) -> list[str]:
        """
        Generate Polygon Trades API URLs for a list of missing time periods.

        Args:
            symbol: Trading symbol
            missing_periods: List of (start_time, end_time) tuples for missing periods

        Returns:
            List of Polygon Trades API URLs, one for each missing period
        """
        urls: list[str] = []

        for start_time, end_time in missing_periods:
            url = self.generate_trades_url_for_period(symbol, start_time, end_time)
            urls.append(url)

        return urls
