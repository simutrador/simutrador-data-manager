"""
Gap filling service for recovering missing trading data.

This service attempts to recover missing candles by making targeted requests
to data providers for specific time periods where gaps are detected.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Tuple, Union

from simutrador_core.models.price_data import PriceCandle, PriceDataSeries, Timeframe

from core.settings import get_settings
from models.nightly_update_api import GapFillResult
from .data_providers.data_provider_factory import (
    DataProvider,
    DataProviderFactory,
)
from .data_providers.polygon_client import PolygonClient
from .polygon_url_generator import PolygonUrlGenerator
from .storage.data_storage_service import DataStorageService

logger = logging.getLogger(__name__)


# Type definitions for Polygon API responses
PolygonCandle = Dict[str, Union[int, float]]
PolygonApiResponse = Dict[str, Union[str, List[PolygonCandle], int]]


class GapFillingService:
    """Service for filling gaps in trading data."""

    def __init__(self):
        """Initialize the gap filling service."""
        self.storage_service = DataStorageService()
        self.settings = get_settings()
        # Use the polygon provider for gap filling
        self.data_provider = DataProviderFactory.create_provider(DataProvider.POLYGON)

    async def fill_gaps_for_periods(
        self,
        symbol: str,
        missing_periods: List[Tuple[datetime, datetime]],
        max_attempts: int = 50,
    ) -> List[GapFillResult]:
        """
        Attempt to fill gaps for specific missing periods.

        Args:
            symbol: Trading symbol
            missing_periods: List of (start_time, end_time) tuples for missing periods
            max_attempts: Maximum number of gaps to attempt filling

        Returns:
            List of GapFillResult objects with results of gap filling attempts
        """
        results: List[GapFillResult] = []

        # Limit the number of attempts to prevent excessive API calls
        periods_to_process = missing_periods[:max_attempts]

        if len(missing_periods) > max_attempts:
            logger.warning(
                f"Limiting gap filling to {max_attempts} periods out of {len(missing_periods)} "
                f"for symbol {symbol}"
            )

        for start_time, end_time in periods_to_process:
            result = await self._fill_single_gap(symbol, start_time, end_time)
            results.append(result)

        return results

    async def _check_trading_activity(
        self, symbol: str, start_time: datetime, end_time: datetime
    ) -> Tuple[bool, str]:
        """
        Check if there was any trading activity during the gap period using trades endpoint.

        Args:
            symbol: Trading symbol
            start_time: Start of the gap
            end_time: End of the gap

        Returns:
            Tuple of (has_trading_activity, status_message)
        """
        try:
            # Use Polygon client to check for trades
            async with DataProviderFactory.create_provider(
                DataProvider.POLYGON
            ) as client:
                if isinstance(client, PolygonClient):
                    trades = await client.fetch_trades_data(
                        symbol,
                        start_time,
                        end_time,
                        limit=1,  # Just need to know if any exist
                    )
                    has_activity = len(trades) > 0
                    logger.info(
                        f"Trading activity check for {symbol} {start_time}-{end_time}: "
                        f"{'Found' if has_activity else 'No'} trades"
                    )
                    return has_activity, "Trading activity check completed"
                else:
                    logger.warning(
                        "Non-Polygon provider, cannot check trading activity"
                    )
                    return False, "Non-Polygon provider, cannot check trading activity"

        except Exception as e:
            logger.error(f"Error checking trading activity for {symbol}: {e}")
            return False, f"Error checking trades: {str(e)}"

    def _generate_polygon_api_url(
        self, symbol: str, start_time: datetime, end_time: datetime
    ) -> str:
        """
        Generate the Polygon Trades API URL for this gap (consistent with validation service).

        Args:
            symbol: Trading symbol
            start_time: Start of the gap
            end_time: End of the gap

        Returns:
            Polygon Trades API URL for this specific time range
        """
        try:
            # Use the same URL generator as the validation service for consistency
            url_generator = PolygonUrlGenerator()
            return url_generator.generate_url_for_period(symbol, start_time, end_time)

        except Exception as e:
            logger.error(f"Error generating Polygon URL for {symbol}: {e}")
            return f"Error generating URL: {str(e)}"

    async def _fill_single_gap(
        self, symbol: str, start_time: datetime, end_time: datetime
    ) -> GapFillResult:
        """
        Attempt to fill a single gap.

        Args:
            symbol: Trading symbol
            start_time: Start of the gap
            end_time: End of the gap

        Returns:
            GapFillResult with the outcome of the gap filling attempt
        """
        logger.info(f"Attempting to fill gap for {symbol}: {start_time} to {end_time}")

        # Generate the Polygon API URLs for this gap
        polygon_url = self._generate_polygon_api_url(symbol, start_time, end_time)
        logger.info(f"Polygon Aggregates API URL for gap: {polygon_url}")

        # Generate Polygon Trades API URL for gap analysis (as requested)
        url_generator = PolygonUrlGenerator()
        trades_url = url_generator.generate_trades_url_for_period(
            symbol, start_time, end_time
        )
        logger.info(f"Polygon Trades API URL for gap analysis: {trades_url}")

        try:
            # Check if we should use trades endpoint based on plan configuration
            settings = get_settings()
            use_trades_endpoint = settings.polygon.use_trades_endpoint_for_gaps

            if use_trades_endpoint:
                # Use trades endpoint for gap filling (requires higher-tier plan)
                logger.info(
                    f"Making trades API call for gap filling: {symbol} from {start_time} to \
                        {end_time}"
                )
                logger.info(f"Polygon Trades URL being used: {trades_url}")

                # Use Polygon client to fetch trades data for gap filling
                async with DataProviderFactory.create_provider(
                    DataProvider.POLYGON
                ) as client:
                    if isinstance(client, PolygonClient):
                        trades_data = await client.fetch_trades_data(
                            symbol, start_time, end_time, limit=50000
                        )
                        logger.info(
                            f"Retrieved {len(trades_data)} trades for gap filling"
                        )

                        # Convert trades to OHLCV candles (this will need implementation)
                        # For now, we'll use the existing aggregates fallback
                        if len(trades_data) > 0:
                            logger.info(
                                "Trades data found, but OHLCV conversion not yet implemented"
                            )
                            # TODO: Implement trades-to-OHLCV conversion
                            # For now, fall back to aggregates endpoint
                            import httpx

                            async with httpx.AsyncClient() as client_http:
                                response = await client_http.get(polygon_url)
                                if response.status_code == 200:
                                    polygon_data: PolygonApiResponse = response.json()
                                    logger.info(
                                        f"Fallback aggregates API response: \
                                            {polygon_data.get('status')} - "
                                        f"{polygon_data.get('resultsCount', 0)} results"
                                    )
                                else:
                                    logger.error(
                                        f"Fallback aggregates API failed: {response.status_code}"
                                    )
                                    polygon_data: PolygonApiResponse = {
                                        "results": [],
                                        "status": "ERROR",
                                    }
                        else:
                            logger.info("No trades found for gap period")
                            polygon_data: PolygonApiResponse = {
                                "results": [],
                                "status": "OK",
                            }
                    else:
                        logger.error(
                            "Non-Polygon provider, cannot use trades endpoint for gap filling"
                        )
                        polygon_data: PolygonApiResponse = {
                            "results": [],
                            "status": "ERROR",
                        }
            else:
                # Use aggregates endpoint for gap filling (available on all plans)
                logger.info(
                    f"Making aggregates API call for gap filling: \
                        {symbol} from {start_time} to {end_time}"
                )
                logger.info(f"Polygon Aggregates URL being used: {polygon_url}")

                # Make direct HTTP request to Polygon Aggregates API
                import httpx

                async with httpx.AsyncClient() as client:
                    response = await client.get(polygon_url)
                    if response.status_code == 200:
                        polygon_data: PolygonApiResponse = response.json()
                        logger.info(
                            f"Aggregates API response: {polygon_data.get('status')} - "
                            f"{polygon_data.get('resultsCount', 0)} results"
                        )
                    else:
                        logger.error(f"Aggregates API failed: {response.status_code}")
                        polygon_data: PolygonApiResponse = {
                            "results": [],
                            "status": "ERROR",
                        }

            # Convert Polygon response to our PriceCandle format
            candles: List[PriceCandle] = []
            results = polygon_data.get("results")
            if results and isinstance(results, list):
                for result in results:
                    from decimal import Decimal

                    # Result is already typed as PolygonCandle (dict)

                    # Convert timestamp from milliseconds to datetime
                    timestamp = result.get("t")
                    if not isinstance(timestamp, (int, float)):
                        continue
                    candle_time = datetime.fromtimestamp(
                        timestamp / 1000, tz=timezone.utc
                    )

                    # Extract OHLCV values with type checking
                    open_val = result.get("o")
                    high_val = result.get("h")
                    low_val = result.get("l")
                    close_val = result.get("c")
                    volume_val = result.get("v")

                    if not all(
                        isinstance(val, (int, float))
                        for val in [open_val, high_val, low_val, close_val, volume_val]
                    ):
                        continue

                    candle = PriceCandle(
                        date=candle_time,
                        open=Decimal(str(open_val)),
                        high=Decimal(str(high_val)),
                        low=Decimal(str(low_val)),
                        close=Decimal(str(close_val)),
                        volume=Decimal(str(volume_val)),
                    )
                    candles.append(candle)

            logger.info(f"Converted {len(candles)} Polygon results to PriceCandles")

            # Filter candles to the exact time range we need
            relevant_candles: List[PriceCandle] = []
            if candles:
                logger.info(
                    f"Filtering {len(candles)} candles for time range {start_time} to {end_time}"
                )
                for i, candle in enumerate(candles):
                    candle_time = candle.date
                    if candle_time.tzinfo is None:
                        candle_time = candle_time.replace(tzinfo=timezone.utc)

                    # Log first few candles and any that might match our time range
                    if i < 5 or (start_time <= candle_time <= end_time):
                        logger.info(
                            f"Candle {i}: {candle_time} | Range: {start_time} to {end_time} | "
                            f"In range: {start_time <= candle_time < end_time}"
                        )

                    # Check if this candle falls within our missing period (inclusive end)
                    if start_time <= candle_time <= end_time:
                        relevant_candles.append(candle)
                        logger.info(
                            f"✅ Candle at {candle_time} matches missing period"
                        )
                    elif (
                        abs((candle_time - start_time).total_seconds()) < 300
                    ):  # Within 5 minutes
                        seconds_diff = (candle_time - start_time).total_seconds()
                        logger.info(
                            f"🔍 Near-miss candle at {candle_time} "
                            f"(outside range by {seconds_diff}s)"
                        )

            logger.info(f"Filtered to {len(relevant_candles)} relevant candles")
            success = len(relevant_candles) > 0

            if not success:
                # Check if there was any trading activity during this period
                has_activity, _ = await self._check_trading_activity(
                    symbol, start_time, end_time
                )

                return GapFillResult(
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    attempted=True,
                    success=False,
                    candles_recovered=0,
                    vendor_unavailable=True,
                    polygon_api_url=polygon_url,
                    trades_api_url=trades_url,
                    has_trading_activity=has_activity,
                    error_message=(
                        "Failed to fetch data from vendor"
                        if has_activity
                        else "No trading activity detected during this period"
                    ),
                )

            if success:
                # Store the recovered candles
                if relevant_candles:
                    logger.info(
                        f"Storing {len(relevant_candles)} recovered candles for {symbol}"
                    )

                    # Group candles by date for storage
                    candles_by_date: Dict[date, List[PriceCandle]] = {}
                    for candle in relevant_candles:
                        candle_date = candle.date.date()
                        if candle_date not in candles_by_date:
                            candles_by_date[candle_date] = []
                        candles_by_date[candle_date].append(candle)

                    # Store candles for each date
                    for candle_date, date_candles in candles_by_date.items():
                        try:
                            # Load existing data for the date
                            existing_series = self.storage_service.load_data(
                                symbol=symbol,
                                timeframe="1min",
                                start_date=candle_date,
                                end_date=candle_date,
                            )

                            # Merge with existing candles
                            all_candles: List[PriceCandle] = (
                                list(existing_series.candles) if existing_series else []
                            )

                            # Add new candles (avoid duplicates)
                            existing_times = {c.date for c in all_candles}
                            for new_candle in date_candles:
                                if new_candle.date not in existing_times:
                                    all_candles.append(new_candle)

                            # Sort by time
                            all_candles.sort(key=lambda c: c.date)

                            # Store updated data
                            updated_series = PriceDataSeries(
                                symbol=symbol,
                                timeframe=Timeframe.ONE_MIN,
                                candles=all_candles,
                            )

                            self.storage_service.store_data(updated_series)

                        except Exception as e:
                            logger.error(
                                f"Error storing recovered candles for {candle_date}: {e}"
                            )

                logger.info(
                    f"Successfully recovered {len(relevant_candles)} candles for {symbol} "
                    f"in period {start_time} to {end_time}"
                )
                # For successful cases, we don't need to check trading activity
                return GapFillResult(
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    attempted=True,
                    success=True,
                    candles_recovered=len(relevant_candles),
                    vendor_unavailable=False,
                    polygon_api_url=polygon_url,
                    trades_api_url=None,  # Not needed for successful fills
                    has_trading_activity=True,  # Assume true if we recovered candles
                )
            else:
                logger.warning(
                    f"No candles recovered for {symbol} in period {start_time} to {end_time} "
                    "- data may be unavailable from vendor"
                )
                # Check if there was any trading activity during this period
                has_activity, _ = await self._check_trading_activity(
                    symbol, start_time, end_time
                )

                return GapFillResult(
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    attempted=True,
                    success=False,
                    candles_recovered=0,
                    vendor_unavailable=True,
                    polygon_api_url=polygon_url,
                    trades_api_url=trades_url,
                    has_trading_activity=has_activity,
                    error_message=(
                        "No data available from vendor for this period"
                        if has_activity
                        else "No trading activity detected during this period"
                    ),
                )

        except Exception as e:
            logger.error(f"Error filling gap for {symbol}: {e}")
            return GapFillResult(
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                attempted=True,
                success=False,
                candles_recovered=0,
                vendor_unavailable=False,
                polygon_api_url=polygon_url,
                trades_api_url=trades_url,  # Include trades URL even in error case
                has_trading_activity=None,  # Unknown due to error
                error_message=str(e),
            )

    def _count_recovered_candles(
        self, symbol: str, start_time: datetime, end_time: datetime
    ) -> int:
        """
        Count how many candles were recovered in the specified time range.

        Args:
            symbol: Trading symbol
            start_time: Start of the time range
            end_time: End of the time range

        Returns:
            Number of candles found in the time range
        """
        try:
            # Load data for the specific date(s)
            start_date = start_time.date()
            end_date = end_time.date()

            total_candles = 0
            current_date = start_date

            while current_date <= end_date:
                try:
                    series = self.storage_service.load_data(
                        symbol=symbol,
                        timeframe="1min",
                        start_date=current_date,
                        end_date=current_date,
                    )

                    if series and series.candles:
                        # Count candles within the specific time range
                        for candle in series.candles:
                            candle_time = candle.date
                            if candle_time.tzinfo is None:
                                candle_time = candle_time.replace(tzinfo=timezone.utc)

                            if start_time <= candle_time < end_time:
                                total_candles += 1

                except Exception as e:
                    logger.debug(f"No data found for {symbol} on {current_date}: {e}")

                current_date += timedelta(days=1)

            return total_candles

        except Exception as e:
            logger.error(f"Error counting recovered candles for {symbol}: {e}")
            return 0
