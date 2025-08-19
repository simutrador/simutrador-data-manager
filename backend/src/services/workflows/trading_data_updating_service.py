"""
Trading data updating service.

This service orchestrates the fetching and storing of trading data:
- Fetches missing data since last update
- Stores 1-minute data in partitioned Parquet files
- Handles multiple symbols and timeframes
- Provides status tracking and error handling
"""

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from simutrador_core.models.price_data import DataUpdateStatus
from simutrador_core.utils import get_default_logger

from ..data_providers.data_provider_factory import (
    DataProvider,
    DataProviderFactory,
)
from ..data_providers.data_provider_interface import (
    AuthenticationError,
    DataProviderError,
    DataProviderInterface,
    RateLimitError,
)
from ..storage.data_storage_service import DataStorageService

logger = get_default_logger("trading_data_updating")


class TradingDataUpdatingService:
    """
    Service for updating trading data from external APIs.

    This service:
    1. Determines what data is missing since the last update
    2. Fetches missing data from Financial Modeling Prep API
    3. Stores the data in Parquet files
    4. Provides status tracking and error handling
    """

    def __init__(self, provider_type: DataProvider = DataProvider.POLYGON):
        """Initialize the updating service."""
        self.storage_service = DataStorageService()
        self.provider_type = provider_type

    async def update_symbol_data(
        self,
        symbol: str,
        timeframes: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        force_update: bool = False,
    ) -> List[DataUpdateStatus]:
        """
        Update data for a single symbol.

        Args:
            symbol: Trading symbol to update
            timeframes: List of timeframes to update (defaults to ["1min"])
            start_date: Optional start date (defaults to last update date)
            end_date: Optional end date (defaults to today)
            force_update: If True, re-fetch all data regardless of existing data

        Returns:
            List of DataUpdateStatus objects for each timeframe
        """
        if timeframes is None:
            timeframes = ["1min"]

        if end_date is None:
            end_date = date.today()

        results: list[DataUpdateStatus] = []

        async with DataProviderFactory.create_provider(self.provider_type) as client:
            for timeframe in timeframes:
                try:
                    result = await self._update_symbol_timeframe(
                        client, symbol, timeframe, start_date, end_date, force_update
                    )
                    results.append(result)
                except Exception as e:
                    logger.error(f"Failed to update {symbol} {timeframe}: {e}")
                    results.append(
                        DataUpdateStatus(
                            symbol=symbol,
                            timeframe=timeframe,
                            last_update=datetime.now(),
                            records_updated=0,
                            success=False,
                            error_message=str(e),
                        )
                    )

        return results

    async def _update_symbol_timeframe(
        self,
        client: DataProviderInterface,
        symbol: str,
        timeframe: str,
        start_date: Optional[date],
        end_date: date,
        force_update: bool,
    ) -> DataUpdateStatus:
        """Update data for a specific symbol and timeframe."""
        logger.info(f"Updating {symbol} {timeframe} data")

        try:
            # Determine the date range to fetch
            if not force_update and start_date is None:
                last_update = self.storage_service.get_last_update_date(
                    symbol, timeframe
                )
                if last_update:
                    # Start from the day after the last update
                    start_date = (last_update + timedelta(days=1)).date()
                else:
                    # No existing data, start from a reasonable default
                    start_date = end_date - timedelta(days=30)  # Last 30 days
            elif start_date is None:
                # Force update but no start date specified
                start_date = end_date - timedelta(days=30)

            # Skip if start_date is after end_date
            if start_date and start_date > end_date:
                logger.info(
                    f"No new data to fetch for {symbol} {timeframe} (already up to date)"
                )
                return DataUpdateStatus(
                    symbol=symbol,
                    timeframe=timeframe,
                    last_update=datetime.now(),
                    records_updated=0,
                    success=True,
                    error_message=None,
                )

            # Fetch data from API
            logger.info(
                f"Fetching {symbol} {timeframe} data from {start_date} to {end_date}"
            )
            series = await client.fetch_historical_data(
                symbol=symbol,
                timeframe=timeframe,
                from_date=start_date,
                to_date=end_date,
            )

            # Store the data
            if series.candles:
                self.storage_service.store_data(series)
                logger.info(
                    f"Successfully updated {len(series.candles)} records for {symbol} {timeframe}"
                )
            else:
                logger.warning(f"No data received for {symbol} {timeframe}")

            return DataUpdateStatus(
                symbol=symbol,
                timeframe=timeframe,
                last_update=datetime.now(),
                records_updated=len(series.candles),
                success=True,
                error_message=None,
            )

        except AuthenticationError as e:
            logger.error(f"Authentication failed for {symbol} {timeframe}: {e}")
            return DataUpdateStatus(
                symbol=symbol,
                timeframe=timeframe,
                last_update=datetime.now(),
                records_updated=0,
                success=False,
                error_message=f"Authentication error: {str(e)}",
            )
        except RateLimitError as e:
            logger.error(f"Rate limit exceeded for {symbol} {timeframe}: {e}")
            return DataUpdateStatus(
                symbol=symbol,
                timeframe=timeframe,
                last_update=datetime.now(),
                records_updated=0,
                success=False,
                error_message=f"Rate limit error: {str(e)}",
            )
        except DataProviderError as e:
            logger.error(f"Error updating {symbol} {timeframe}: {e}")
            return DataUpdateStatus(
                symbol=symbol,
                timeframe=timeframe,
                last_update=datetime.now(),
                records_updated=0,
                success=False,
                error_message=str(e),
            )

    def get_update_status(
        self, symbols: List[str], timeframes: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Optional[datetime]]]:
        """
        Get the last update status for multiple symbols and timeframes.

        Args:
            symbols: List of symbols to check
            timeframes: List of timeframes to check

        Returns:
            Nested dictionary: {symbol: {timeframe: last_update_datetime}}
        """
        if timeframes is None:
            timeframes = ["1min"]

        status = {}
        for symbol in symbols:
            status[symbol] = {}
            for timeframe in timeframes:
                last_update = self.storage_service.get_last_update_date(
                    symbol, timeframe
                )
                status[symbol][timeframe] = last_update

        return status  # type: ignore[reportUnknownVariableType]

    def get_stored_symbols(self, timeframe: str = "1min") -> List[str]:
        """
        Get list of symbols that have stored data.

        Args:
            timeframe: Timeframe to check

        Returns:
            List of symbol names
        """
        return self.storage_service.list_stored_symbols(timeframe)
