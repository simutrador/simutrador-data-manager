"""
Financial Modeling Prep API client for fetching trading data.

This service handles API calls to Financial Modeling Prep, including:
- Authentication with API key
- Rate limiting
- Error handling and retries
- Data parsing and validation
"""

import asyncio
import logging
from datetime import date, datetime
from decimal import Decimal
from types import TracebackType
from typing import Any, Dict, List, Optional, TypedDict, cast, override

import httpx
from pydantic import ValidationError

from core.settings import get_settings
from models.price_data import PriceCandle, PriceDataSeries, Timeframe

from .data_provider_interface import (
    AuthenticationError,
    DataProviderError,
    DataProviderInterface,
    RateLimitError,
)

logger = logging.getLogger(__name__)


# API Response Type Definitions
class IntradayCandle(TypedDict):
    """Type definition for intraday API response candle."""

    date: str
    open: str
    high: str
    low: str
    close: str
    volume: int


class DailyCandle(TypedDict):
    """Type definition for daily/EOD API response candle."""

    date: str
    open: str
    high: str
    low: str
    close: str
    volume: int


class FinancialModelingPrepError(DataProviderError):
    """Base exception for Financial Modeling Prep API errors."""

    pass


class FinancialModelingPrepClient(DataProviderInterface):
    """
    Client for Financial Modeling Prep API.

    Handles fetching historical price data with proper rate limiting,
    error handling, and data validation.
    """

    def __init__(self):
        """Initialize the client with settings."""
        self.settings = get_settings()
        self.fmp_settings = self.settings.financial_modeling_prep
        self.client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )
        self._last_request_time = 0.0
        self._request_count = 0
        self._rate_limit_window_start = 0.0

    @override
    async def __aenter__(self) -> "FinancialModelingPrepClient":
        """Async context manager entry."""
        return self

    @override
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Async context manager exit."""
        await self.client.aclose()

    async def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting based on API limits."""
        current_time = asyncio.get_event_loop().time()

        # Reset counter if we're in a new minute window
        if current_time - self._rate_limit_window_start >= 60:
            self._request_count = 0
            self._rate_limit_window_start = current_time

        # Check if we've exceeded the rate limit
        if self._request_count >= self.fmp_settings.rate_limit_per_minute:
            sleep_time = 60 - (current_time - self._rate_limit_window_start)
            if sleep_time > 0:
                logger.warning(
                    f"Rate limit reached, sleeping for {sleep_time:.2f} seconds"
                )
                await asyncio.sleep(sleep_time)
                self._request_count = 0
                self._rate_limit_window_start = asyncio.get_event_loop().time()

        # Ensure minimum time between requests (to be respectful)
        time_since_last = current_time - self._last_request_time
        if time_since_last < 0.1:  # 100ms minimum between requests
            await asyncio.sleep(0.1 - time_since_last)

        self._last_request_time = asyncio.get_event_loop().time()
        self._request_count += 1

    async def _make_request(
        self, endpoint: str, params: dict[str, Any]
    ) -> List[Dict[str, Any]] | Dict[str, Any]:
        """Make an authenticated request to the API."""
        await self._enforce_rate_limit()

        # Add API key to parameters
        params["apikey"] = self.fmp_settings.api_key

        # Ensure proper URL construction
        base_url = self.fmp_settings.base_url
        if base_url.endswith("/"):
            url = base_url + endpoint
        else:
            url = base_url + "/" + endpoint

        try:
            response = await self.client.get(url, params=params, follow_redirects=True)
            response.raise_for_status()

            data = response.json()

            # Check for API-specific errors
            if isinstance(data, dict) and "Error Message" in data:
                error_msg = data["Error Message"]  # type: ignore[reportUnknownVariableType]
                if "api key" in error_msg.lower():  # type: ignore[reportUnknownMemberType]
                    raise AuthenticationError(f"API authentication failed: {error_msg}")
                elif "rate limit" in error_msg.lower():  # type: ignore[reportUnknownMemberType]
                    raise RateLimitError(f"Rate limit exceeded: {error_msg}")
                else:
                    raise FinancialModelingPrepError(f"API error: {error_msg}")

            return data  # type: ignore[reportUnknownVariableType]

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise AuthenticationError("Invalid API key")
            elif e.response.status_code == 429:
                raise RateLimitError("Rate limit exceeded")
            else:
                raise FinancialModelingPrepError(
                    f"HTTP error {e.response.status_code}: {e.response.text}"
                )
        except httpx.RequestError as e:
            raise FinancialModelingPrepError(f"Request failed: {str(e)}")

    @override
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
            FinancialModelingPrepError: If API request fails
            ValidationError: If response data is invalid
        """
        # Use different endpoints for daily vs intraday data
        if timeframe in ["1day", "daily"]:
            return await self._fetch_daily_data(symbol, from_date, to_date)
        else:
            return await self._fetch_intraday_data(
                symbol, timeframe, from_date, to_date
            )

    async def _fetch_intraday_data(
        self,
        symbol: str,
        timeframe: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ) -> PriceDataSeries:
        """Fetch intraday data using historical-chart endpoint."""
        # Map our timeframe format to FMP API format
        timeframe_map = {
            "1min": "1min",
            "5min": "5min",
            "15min": "15min",
            "30min": "30min",
            "1h": "1hour",
            "4h": "4hour",
        }

        api_timeframe = timeframe_map.get(timeframe, timeframe)
        endpoint = f"historical-chart/{api_timeframe}"

        params = {"symbol": symbol.upper()}

        if from_date:
            params["from"] = from_date.strftime("%Y-%m-%d")
        if to_date:
            params["to"] = to_date.strftime("%Y-%m-%d")

        logger.info(
            f"Fetching {timeframe} intraday data for {symbol} from {from_date} to {to_date}"
        )

        try:
            data = await self._make_request(endpoint, params)

            if not isinstance(data, list):
                raise FinancialModelingPrepError(
                    f"Unexpected response format: expected list, got {type(data)}"
                )

            if not data:
                logger.warning(f"No intraday data returned for {symbol} {timeframe}")
                return PriceDataSeries(
                    symbol=symbol, timeframe=Timeframe(timeframe), candles=[]
                )

            # Parse and validate candles
            candles: list[PriceCandle] = []
            for item in data:
                # Type the item as IntradayCandle for better type checking
                candle_data = cast(IntradayCandle, item)
                try:
                    # Convert the API response format to our model format
                    candle = PriceCandle(
                        date=datetime.fromisoformat(
                            candle_data["date"].replace(" ", "T")
                        ),
                        open=Decimal(str(candle_data["open"])),
                        high=Decimal(str(candle_data["high"])),
                        low=Decimal(str(candle_data["low"])),
                        close=Decimal(str(candle_data["close"])),
                        volume=Decimal(str(candle_data["volume"])),
                    )
                    candles.append(candle)
                except (KeyError, ValueError, ValidationError) as e:
                    logger.warning(
                        f"Skipping invalid intraday candle data: {item}, error: {e}"
                    )
                    continue

            logger.info(
                f"Successfully fetched {len(candles)} intraday candles for {symbol}"
            )

            return PriceDataSeries(
                symbol=symbol, timeframe=Timeframe(timeframe), candles=candles
            )

        except ValidationError as e:
            raise FinancialModelingPrepError(f"Intraday data validation failed: {e}")

    async def _fetch_daily_data(
        self,
        symbol: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ) -> PriceDataSeries:
        """Fetch daily end-of-day data using historical-price-eod endpoint."""
        endpoint = "historical-price-eod/full"

        params = {"symbol": symbol.upper()}

        if from_date:
            params["from"] = from_date.strftime("%Y-%m-%d")
        if to_date:
            params["to"] = to_date.strftime("%Y-%m-%d")

        logger.info(
            f"Fetching daily EOD data for {symbol} from {from_date} to {to_date}"
        )

        try:
            data = await self._make_request(endpoint, params)

            if not isinstance(data, list):
                raise FinancialModelingPrepError(
                    f"Unexpected response format: expected list, got {type(data)}"
                )

            if not data:
                logger.warning(f"No daily data returned for {symbol}")
                return PriceDataSeries(
                    symbol=symbol, timeframe=Timeframe.DAILY, candles=[]
                )

            # Parse and validate candles
            candles: list[PriceCandle] = []
            for item in data:
                # Type the item as DailyCandle for better type checking
                candle_data = cast(DailyCandle, item)
                try:
                    # EOD API response format might be different, handle both formats
                    if "date" in candle_data:
                        # Standard format with date string
                        date_str = candle_data["date"]
                        if " " in date_str:
                            # Has time component
                            candle_date = datetime.fromisoformat(
                                date_str.replace(" ", "T")
                            )
                        else:
                            # Date only, set to market close time (4 PM ET)
                            candle_date = datetime.fromisoformat(f"{date_str}T16:00:00")
                    else:
                        raise KeyError("No date field found in EOD data")

                    candle = PriceCandle(
                        date=candle_date,
                        open=Decimal(str(candle_data["open"])),
                        high=Decimal(str(candle_data["high"])),
                        low=Decimal(str(candle_data["low"])),
                        close=Decimal(str(candle_data["close"])),
                        volume=Decimal(str(candle_data["volume"])),
                    )
                    candles.append(candle)
                except (KeyError, ValueError, ValidationError) as e:
                    logger.warning(
                        f"Skipping invalid daily candle data: {item}, error: {e}"
                    )
                    continue

            logger.info(
                f"Successfully fetched {len(candles)} daily candles for {symbol}"
            )

            return PriceDataSeries(
                symbol=symbol, timeframe=Timeframe.DAILY, candles=candles
            )

        except ValidationError as e:
            raise FinancialModelingPrepError(f"Daily data validation failed: {e}")

    @override
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
        """
        # For latest data, we fetch the last few days and take the most recent
        from datetime import timedelta

        to_date = date.today()
        from_date = to_date - timedelta(days=2)

        series = await self.fetch_historical_data(symbol, timeframe, from_date, to_date)

        if not series.candles:
            return None

        # Return the most recent candle
        return max(series.candles, key=lambda c: c.date)

    @override
    def get_resampling_metadata(self) -> Dict[str, str]:
        """
        Get Financial Modeling Prep specific resampling metadata.

        FMP uses market session alignment for US stocks.
        """
        return {
            "alignment_strategy": "market_session",
            "daily_boundary": "market_close",
            "intraday_alignment": "session_aligned",
        }
