"""
Polygon.io API client for fetching trading data.

This service handles API calls to Polygon.io, including:
- Authentication with API key
- Rate limiting
- Error handling and retries
- Data parsing and validation
"""

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import TracebackType
from typing import Any, Dict, List, Optional, TypedDict, cast, override

import httpx
from simutrador_core.models.price_data import PriceCandle, PriceDataSeries, Timeframe

from ...core.settings import get_settings
from .data_provider_interface import (
    AuthenticationError,
    DataProviderError,
    DataProviderInterface,
    RateLimitError,
)

logger = logging.getLogger(__name__)


# API Response Type Definitions
class PolygonCandle(TypedDict):
    """Type definition for Polygon API response candle."""

    t: int  # timestamp in milliseconds
    o: float  # open price
    h: float  # high price
    l: float  # low price (note: 'l' not 'low')  # noqa: E741
    c: float  # close price
    v: int  # volume


class PolygonResponse(TypedDict):
    """Type definition for Polygon API response."""

    results: List[PolygonCandle]
    status: str


class PolygonTrade(TypedDict):
    """Type definition for Polygon API trade response."""

    t: int  # timestamp in nanoseconds
    y: int  # timestamp in milliseconds (for compatibility)
    f: int  # TRF timestamp in nanoseconds
    q: int  # sequence number
    i: str  # trade ID
    x: int  # exchange ID
    s: int  # size (shares traded)
    c: List[int]  # conditions
    p: float  # price
    z: int  # tape (1=A, 2=B, 3=C)


class PolygonTradesResponse(TypedDict):
    """Type definition for Polygon Trades API response."""

    results: List[PolygonTrade]
    status: str
    request_id: str
    next_url: Optional[str]


class FormattedTrade(TypedDict):
    """Type definition for formatted trade data returned by fetch_trades_data."""

    timestamp: datetime
    timestamp_ns: int
    price: float
    size: int
    exchange_id: int
    conditions: List[int]
    trade_id: str


class PolygonError(DataProviderError):
    """Base exception for Polygon API errors."""

    pass


class BatchInfo:
    """Information about a data batch request."""

    def __init__(
        self,
        start_date: date,
        end_date: date,
        success: bool = False,
        candles_count: int = 0,
        error_message: Optional[str] = None,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.success = success
        self.candles_count = candles_count
        self.error_message = error_message
        self.attempted_at = datetime.now()


class FetchResult:
    """Result of fetching historical data with batch tracking."""

    def __init__(self, symbol: str, timeframe: str, candles: List[PriceCandle]):
        self.symbol = symbol
        self.timeframe = timeframe
        self.candles = candles
        self.batches: List[BatchInfo] = []
        self.total_batches = 0
        self.successful_batches = 0
        self.failed_batches = 0

    def add_batch(self, batch_info: BatchInfo) -> None:
        """Add batch information to the result."""
        self.batches.append(batch_info)
        self.total_batches += 1
        if batch_info.success:
            self.successful_batches += 1
        else:
            self.failed_batches += 1

    def get_failed_batches(self) -> List[BatchInfo]:
        """Get list of failed batches."""
        return [batch for batch in self.batches if not batch.success]

    def get_missing_date_ranges(self) -> List[tuple[date, date]]:
        """Get list of date ranges that failed to download."""
        return [
            (batch.start_date, batch.end_date) for batch in self.get_failed_batches()
        ]

    def has_failures(self) -> bool:
        """Check if any batches failed."""
        return self.failed_batches > 0


class PolygonClient(DataProviderInterface):
    """
    Client for Polygon.io API.

    Handles fetching historical price data with proper rate limiting,
    error handling, and data validation.
    """

    def __init__(self):
        """Initialize the client with settings."""
        self.settings = get_settings()
        self.polygon_settings = self.settings.polygon
        self.client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )
        self._last_request_time = 0.0
        self._request_count = 0

    @override
    async def __aenter__(self) -> "PolygonClient":
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
        """Enforce rate limiting based on API limits with conservative approach."""
        current_time = asyncio.get_event_loop().time()

        # Use a more reasonable rate limit based on plan
        # Most paid plans can handle 50+ requests/second
        conservative_rate = min(
            self.polygon_settings.rate_limit_requests_per_second, 50
        )
        min_interval = 1.0 / conservative_rate
        time_since_last = current_time - self._last_request_time

        if time_since_last < min_interval:
            await asyncio.sleep(min_interval - time_since_last)

        self._last_request_time = asyncio.get_event_loop().time()
        self._request_count += 1

    async def _make_request(
        self, endpoint: str, params: dict[str, Any]
    ) -> PolygonResponse:
        """Make an authenticated request to the API."""
        await self._enforce_rate_limit()

        # Add API key to parameters
        params["apikey"] = self.polygon_settings.api_key

        # Construct URL
        url = f"{self.polygon_settings.base_url}/{endpoint}"

        try:
            response = await self.client.get(url, params=params, follow_redirects=True)
            response.raise_for_status()

            data = response.json()

            # Check for API-specific errors
            if isinstance(data, dict):
                data = cast(Dict[str, Any], data)
                status: str = data.get("status", "")
                if status == "ERROR":
                    error_msg: str = data.get("error", "Unknown error")
                    if "unauthorized" in error_msg.lower():
                        raise AuthenticationError(
                            f"API authentication failed: {error_msg}"
                        )
                    elif "rate limit" in error_msg.lower():
                        raise RateLimitError(f"Rate limit exceeded: {error_msg}")
                    else:
                        raise PolygonError(f"API error: {error_msg}")

            return cast(PolygonResponse, data)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise AuthenticationError("Invalid API key")
            elif e.response.status_code == 429:
                raise RateLimitError("Rate limit exceeded")
            else:
                raise PolygonError(
                    f"HTTP error {e.response.status_code}: {e.response.text}"
                )
        except httpx.RequestError as e:
            # Provide more detailed error information
            error_details = f"Request failed: {type(e).__name__}: {str(e)}"
            if hasattr(e, "request") and e.request:
                error_details += f" (URL: {e.request.url})"
            raise PolygonError(error_details)

    def _get_polygon_timeframe(self, timeframe: str) -> tuple[int, str]:
        """
        Convert our timeframe format to Polygon API format.

        Args:
            timeframe: Our timeframe format (e.g., "1min", "5min", "1h", "daily")

        Returns:
            Tuple of (multiplier, timespan) for Polygon API
        """
        timeframe_map = {
            "1min": (1, "minute"),
            "5min": (5, "minute"),
            "15min": (15, "minute"),
            "30min": (30, "minute"),
            "1h": (1, "hour"),
            "2h": (2, "hour"),
            "4h": (4, "hour"),
            "daily": (1, "day"),
            "1day": (1, "day"),
        }

        if timeframe not in timeframe_map:
            raise PolygonError(f"Unsupported timeframe: {timeframe}")

        return timeframe_map[timeframe]

    def _calculate_batch_size(self, timeframe: str) -> int:
        """
        Calculate optimal batch size in days based on timeframe and Polygon's 50k limit.

        Args:
            timeframe: The timeframe being requested

        Returns:
            Optimal batch size in days
        """
        # Estimate candles per day based on timeframe
        candles_per_day = {
            "1min": 390,  # ~6.5 hours * 60 minutes (market hours)
            "5min": 78,  # 390 / 5
            "15min": 26,  # 390 / 15
            "30min": 13,  # 390 / 30
            "1h": 7,  # 390 / 60
            "2h": 4,  # 390 / 120
            "4h": 2,  # 390 / 240
            "daily": 1,  # 1 candle per day
            "1day": 1,  # 1 candle per day
        }

        estimated_candles_per_day = candles_per_day.get(
            timeframe, 390
        )  # Default to 1min

        # Calculate max days per batch to stay under 50k limit with safety margin
        max_days_per_batch = int(45000 / estimated_candles_per_day)  # 45k for safety

        # For minute data, optimize batch sizes for better efficiency
        # Based on analysis showing only 55.4% completeness due to this limit
        if timeframe == "1min":
            # For 1-minute data: use 60 days for better efficiency
            # 60 days * 390 candles/day = 23,400 candles (well under 50k)
            return min(max_days_per_batch, 60)
        elif timeframe in ["5min", "15min"]:
            # For 5/15-minute data: can handle larger batches
            return min(max_days_per_batch, 90)  # 3 months max
        else:
            # For hourly and daily data: can handle very large batches
            return min(max_days_per_batch, 365)  # 1 year max

    async def _fetch_batch_with_retry(
        self,
        symbol: str,
        timeframe: str,
        batch_start: date,
        batch_end: date,
        max_retries: int = 3,
    ) -> List[PriceCandle]:
        """
        Fetch a single batch of data with retry logic.

        Args:
            symbol: Trading symbol
            timeframe: Timeframe for data
            batch_start: Start date for this batch
            batch_end: End date for this batch
            max_retries: Maximum number of retry attempts

        Returns:
            List of PriceCandle objects for this batch
        """
        for attempt in range(max_retries + 1):
            try:
                return await self._fetch_batch(
                    symbol, timeframe, batch_start, batch_end
                )
            except (RateLimitError, PolygonError) as e:
                if attempt == max_retries:
                    logger.error(
                        f"Final attempt failed for {symbol} {batch_start}-{batch_end}: {e}"
                    )
                    raise

                # Exponential backoff: 1s, 2s, 4s
                delay = 2**attempt
                logger.warning(
                    f"Batch attempt {attempt + 1} failed for {symbol} "
                    f"{batch_start}-{batch_end}: {e}. Retrying in {delay}s..."
                )
                await asyncio.sleep(delay)

        return []  # Should never reach here

    async def _fetch_batch(
        self,
        symbol: str,
        timeframe: str,
        batch_start: date,
        batch_end: date,
    ) -> List[PriceCandle]:
        """
        Fetch a single batch of data for the given date range.

        Args:
            symbol: Trading symbol
            timeframe: Timeframe for data
            batch_start: Start date for this batch
            batch_end: End date for this batch

        Returns:
            List of PriceCandle objects for this batch
        """
        multiplier, timespan = self._get_polygon_timeframe(timeframe)

        # Format dates for Polygon API
        from_str = batch_start.strftime("%Y-%m-%d")
        to_str = batch_end.strftime("%Y-%m-%d")

        endpoint = f"{symbol}/range/{multiplier}/{timespan}/{from_str}/{to_str}"
        params: dict[str, Any] = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,  # Maximum allowed by Polygon
        }

        logger.debug(
            f"Fetching batch: {symbol} {timeframe} from {batch_start} to {batch_end}"
        )

        data = await self._make_request(endpoint, params)
        results = data.get("results", [])

        if not results:
            logger.debug(
                f"No data in batch for {symbol} {timeframe} {batch_start}-{batch_end}"
            )
            return []

        # Parse and validate candles
        candles: List[PriceCandle] = []
        for item in results:
            candle_data: PolygonCandle = item
            try:
                # Convert timestamp from milliseconds to datetime
                timestamp = datetime.fromtimestamp(
                    candle_data["t"] / 1000, tz=timezone.utc
                )

                candle = PriceCandle(
                    date=timestamp,
                    open=Decimal(str(candle_data["o"])),
                    high=Decimal(str(candle_data["h"])),
                    low=Decimal(str(candle_data["l"])),  # Fixed: use 'l' not 'low'
                    close=Decimal(str(candle_data["c"])),
                    volume=Decimal(str(candle_data["v"])),
                )
                candles.append(candle)
            except (KeyError, ValueError) as e:
                logger.warning(f"Skipping invalid candle data: {item}, error: {e}")
                continue

        logger.debug(f"Batch fetched: {len(candles)} candles for {symbol}")
        return candles

    async def fetch_historical_data_with_tracking(
        self,
        symbol: str,
        timeframe: str = "1min",
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ) -> FetchResult:
        """
        Fetch historical price data with detailed batch tracking.

        This method provides detailed information about which batches succeeded
        or failed, allowing for targeted retry of missing data.

        Args:
            symbol: Trading symbol (e.g., "AAPL")
            timeframe: Timeframe for data (e.g., "1min", "5min", "1day", "daily")
            from_date: Start date for data
            to_date: End date for data

        Returns:
            FetchResult containing candles and detailed batch information

        Raises:
            PolygonError: If API request fails
            ValidationError: If response data is invalid
        """
        # Default date range if not provided
        if to_date is None:
            to_date = date.today()
        if from_date is None:
            from_date = to_date - timedelta(days=1)

        date_range_days = (to_date - from_date).days + 1

        logger.info(
            f"Fetching {timeframe} data for {symbol} from {from_date} to {to_date} "
            f"({date_range_days} days) with tracking"
        )

        # Determine if we need batching
        batch_size_days = self._calculate_batch_size(timeframe)
        all_candles: List[PriceCandle] = []
        result = FetchResult(symbol, timeframe, all_candles)

        if date_range_days <= batch_size_days:
            # Small range - single request
            logger.debug(
                f"Using single request for {symbol} (range: {date_range_days} days)"
            )
            try:
                candles = await self._fetch_batch_with_retry(
                    symbol, timeframe, from_date, to_date
                )
                all_candles.extend(candles)
                result.add_batch(BatchInfo(from_date, to_date, True, len(candles)))
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Failed to fetch data for {symbol}: {error_msg}")
                result.add_batch(BatchInfo(from_date, to_date, False, 0, error_msg))
        else:
            # Large range - batch requests
            num_batches = (date_range_days + batch_size_days - 1) // batch_size_days
            logger.info(
                f"Using {num_batches} batches for {symbol} "
                f"(batch size: {batch_size_days} days)"
            )

            current_date = from_date

            for batch_num in range(num_batches):
                batch_end = min(
                    current_date + timedelta(days=batch_size_days - 1), to_date
                )

                logger.debug(
                    f"Processing batch {batch_num + 1}/{num_batches} for {symbol}: "
                    f"{current_date} to {batch_end}"
                )

                try:
                    batch_candles = await self._fetch_batch_with_retry(
                        symbol, timeframe, current_date, batch_end
                    )
                    all_candles.extend(batch_candles)
                    result.add_batch(
                        BatchInfo(current_date, batch_end, True, len(batch_candles))
                    )

                except Exception as e:
                    error_msg = str(e)
                    logger.error(
                        f"Failed to fetch batch {batch_num + 1} for {symbol}: {error_msg}"
                    )
                    result.add_batch(
                        BatchInfo(current_date, batch_end, False, 0, error_msg)
                    )

                current_date = batch_end + timedelta(days=1)

                # Small delay between batches to be respectful to the API
                if batch_num < num_batches - 1:  # Don't delay after last batch
                    await asyncio.sleep(0.1)

            logger.info(
                f"Completed batching for {symbol}: {result.successful_batches}/\
                    {result.total_batches} "
                f"batches successful"
            )

        # Sort candles by date to ensure proper ordering
        all_candles.sort(key=lambda c: c.date)
        result.candles = all_candles

        logger.info(f"Successfully fetched {len(all_candles)} candles for {symbol}")
        if result.has_failures():
            logger.warning(
                f"{symbol} has {result.failed_batches} failed batches out of {result.total_batches}"
            )

        return result

    @override
    async def fetch_historical_data(
        self,
        symbol: str,
        timeframe: str = "1min",
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ) -> PriceDataSeries:
        """
        Fetch historical price data for a symbol with intelligent batching.

        For large date ranges, this method automatically splits the request into
        smaller batches to respect Polygon's 50k candle limit and improve reliability.

        Args:
            symbol: Trading symbol (e.g., "AAPL")
            timeframe: Timeframe for data (e.g., "1min", "5min", "1day", "daily")
            from_date: Start date for data
            to_date: End date for data

        Returns:
            PriceDataSeries containing the fetched data

        Raises:
            PolygonError: If API request fails
            ValidationError: If response data is invalid
        """
        # Use the tracking method internally but return only the PriceDataSeries
        result = await self.fetch_historical_data_with_tracking(
            symbol, timeframe, from_date, to_date
        )

        return PriceDataSeries(
            symbol=symbol, timeframe=Timeframe(timeframe), candles=result.candles
        )

    async def retry_failed_batches(self, fetch_result: FetchResult) -> FetchResult:
        """
        Retry only the failed batches from a previous fetch attempt.

        Args:
            fetch_result: Previous fetch result with failed batches

        Returns:
            Updated FetchResult with retry attempts
        """
        failed_batches = fetch_result.get_failed_batches()
        if not failed_batches:
            logger.info(f"No failed batches to retry for {fetch_result.symbol}")
            return fetch_result

        logger.info(
            f"Retrying {len(failed_batches)} failed batches for {fetch_result.symbol}"
        )

        # Create a new result to track retry attempts
        retry_result = FetchResult(
            fetch_result.symbol, fetch_result.timeframe, list(fetch_result.candles)
        )

        # Copy successful batches from original result
        for batch in fetch_result.batches:
            if batch.success:
                retry_result.add_batch(batch)

        # Retry failed batches
        for batch_info in failed_batches:
            logger.debug(
                f"Retrying batch for {fetch_result.symbol}: "
                f"{batch_info.start_date} to {batch_info.end_date}"
            )

            try:
                batch_candles = await self._fetch_batch_with_retry(
                    fetch_result.symbol,
                    fetch_result.timeframe,
                    batch_info.start_date,
                    batch_info.end_date,
                )
                retry_result.candles.extend(batch_candles)
                retry_result.add_batch(
                    BatchInfo(
                        batch_info.start_date,
                        batch_info.end_date,
                        True,
                        len(batch_candles),
                    )
                )
                logger.info(
                    f"Successfully retried batch for {fetch_result.symbol}: "
                    f"{batch_info.start_date} to {batch_info.end_date} "
                    f"({len(batch_candles)} candles)"
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(
                    f"Retry failed for {fetch_result.symbol} "
                    f"{batch_info.start_date} to {batch_info.end_date}: {error_msg}"
                )
                retry_result.add_batch(
                    BatchInfo(
                        batch_info.start_date,
                        batch_info.end_date,
                        False,
                        0,
                        error_msg,
                    )
                )

            # Small delay between retry attempts
            await asyncio.sleep(0.2)

        # Sort candles by date
        retry_result.candles.sort(key=lambda c: c.date)

        logger.info(
            f"Retry completed for {fetch_result.symbol}: "
            f"{retry_result.successful_batches}/{retry_result.total_batches} "
            f"batches successful, {len(retry_result.candles)} total candles"
        )

        return retry_result

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
        to_date = date.today()
        from_date = to_date - timedelta(days=2)

        series = await self.fetch_historical_data(symbol, timeframe, from_date, to_date)

        if not series.candles:
            return None

        # Return the most recent candle
        return max(series.candles, key=lambda c: c.date)

    async def fetch_trades_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 50000,
    ) -> List[FormattedTrade]:
        """
        Fetch trades data for a symbol within a specific time window.

        This method uses the Polygon v3/trades endpoint to get all trades
        that occurred within the specified time window. This is useful for
        gap filling analysis to verify if trading activity happened during
        periods where aggregate data is missing.

        Args:
            symbol: Trading symbol (e.g., "AAPL")
            start_time: Start time for trades (inclusive)
            end_time: End time for trades (inclusive)
            limit: Maximum number of trades to return (max 50,000)

        Returns:
            List of trade dictionaries with timestamp, price, size, etc.

        Raises:
            PolygonError: If API request fails
            ValidationError: If response data is invalid
        """
        # Convert datetime to nanosecond timestamps
        start_ns = int(start_time.timestamp() * 1_000_000_000)
        end_ns = int(end_time.timestamp() * 1_000_000_000)

        logger.info(
            f"Fetching trades for {symbol} from {start_time} to {end_time} "
            f"(ns: {start_ns} to {end_ns})"
        )

        endpoint = f"v3/trades/{symbol}"
        params: Dict[str, Any] = {
            "timestamp.gte": start_ns,
            "timestamp.lte": end_ns,
            "limit": min(limit, 50000),  # Respect API limit
        }

        try:
            response = await self._make_trades_request(endpoint, params)

            if not response.get("results"):
                logger.info(f"No trades found for {symbol} in specified time window")
                return []

            trades = response["results"]
            logger.info(f"Retrieved {len(trades)} trades for {symbol}")

            # Convert trades to a more usable format
            formatted_trades: List[FormattedTrade] = []
            for trade in trades:
                # Convert nanosecond timestamp to datetime
                trade_time = datetime.fromtimestamp(
                    trade["t"] / 1_000_000_000, tz=timezone.utc
                )

                formatted_trade: FormattedTrade = {
                    "timestamp": trade_time,
                    "timestamp_ns": trade["t"],
                    "price": trade["p"],
                    "size": trade["s"],
                    "exchange_id": trade["x"],
                    "conditions": trade.get("c", []),
                    "trade_id": trade.get("i", ""),
                }
                formatted_trades.append(formatted_trade)

            return formatted_trades

        except Exception as e:
            logger.error(f"Error fetching trades for {symbol}: {e}")
            raise PolygonError(f"Failed to fetch trades data: {str(e)}")

    async def _make_trades_request(
        self, endpoint: str, params: Dict[str, Any]
    ) -> PolygonTradesResponse:
        """Make an authenticated request to the trades API."""
        await self._enforce_rate_limit()

        # Add API key to parameters
        params["apikey"] = self.polygon_settings.api_key

        # Construct URL
        url = f"{self.polygon_settings.base_url}/{endpoint}"

        try:
            response = await self.client.get(url, params=params, follow_redirects=True)
            response.raise_for_status()

            data = response.json()

            # Check for API-specific errors
            if isinstance(data, dict):
                data = cast(Dict[str, Any], data)
                status: str = data.get("status", "")
                if status == "ERROR":
                    error_msg: str = data.get("error", "Unknown error")
                    if "unauthorized" in error_msg.lower():
                        raise AuthenticationError(
                            f"API authentication failed: {error_msg}"
                        )
                    elif "rate limit" in error_msg.lower():
                        raise RateLimitError(f"Rate limit exceeded: {error_msg}")
                    else:
                        raise PolygonError(f"API error: {error_msg}")

            return cast(PolygonTradesResponse, data)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise AuthenticationError("Invalid API key")
            elif e.response.status_code == 429:
                raise RateLimitError("Rate limit exceeded")
            else:
                raise PolygonError(f"HTTP error {e.response.status_code}: {e}")
        except httpx.RequestError as e:
            raise PolygonError(f"Request failed: {e}")

    @override
    def get_resampling_metadata(self) -> Dict[str, str]:
        """
        Get Polygon specific resampling metadata.

        Polygon uses UTC alignment for intraday data and different daily boundaries
        per asset type (market close for US stocks, UTC midnight for crypto/forex).
        """
        return {
            "alignment_strategy": "utc_aligned",
            "daily_boundary": "asset_specific",  # Varies by asset type
            "intraday_alignment": "utc_aligned",
        }
