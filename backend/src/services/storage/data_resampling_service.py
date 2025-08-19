"""
Data resampling service for converting trading data between different timeframes.

This service:
- Reads data from storage at any supported timeframe
- Resamples to target timeframes using pandas
- Stores the resampled data in appropriate Parquet files
- Handles data validation and error cases
- Supports flexible timeframe conversions (1min→5min, 5min→1h, etc.)
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pandas as pd
from simutrador_core.models.asset_types import AssetType, get_resampling_offset
from simutrador_core.models.price_data import PriceCandle, PriceDataSeries, Timeframe
from simutrador_core.utils import (
    get_default_logger,
    get_pandas_frequency,
    get_resampling_rules,
    validate_timeframe_conversion,
)

from ..classification.asset_classification_service import (
    AssetClassificationService,
)
from .data_storage_service import DataStorageError, DataStorageService

logger = get_default_logger("data_resampling")


class DataResamplingError(Exception):
    """Base exception for data resampling errors."""

    pass


class DataResamplingService:
    """
    Service for resampling trading data between different timeframes.

    Supports flexible timeframe conversions:
    - 1min → 5min, 15min, 30min, 1h, 2h, 4h, daily
    - 5min → 15min, 30min, 1h, 2h, 4h, daily
    - 1h → 2h, 4h, daily
    - etc.
    """

    def __init__(self):
        """Initialize the resampling service."""
        self.storage_service = DataStorageService()
        self.asset_classifier = AssetClassificationService()

    def resample_data(
        self,
        symbol: str,
        from_timeframe: str,
        to_timeframe: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> PriceDataSeries:
        """
        Resample trading data from one timeframe to another.

        Args:
            symbol: Trading symbol
            from_timeframe: Source timeframe (e.g., "1min", "5min", "1h")
            to_timeframe: Target timeframe (e.g., "5min", "1h", "daily")
            start_date: Optional start date for resampling
            end_date: Optional end date for resampling

        Returns:
            PriceDataSeries with resampled candles

        Raises:
            DataResamplingError: If resampling fails or timeframes are invalid
        """
        try:
            # Validate timeframe conversion
            if not validate_timeframe_conversion(from_timeframe, to_timeframe):
                raise DataResamplingError(
                    f"Invalid timeframe conversion: {from_timeframe} → {to_timeframe}. "
                    f"Target timeframe must represent a longer period than source timeframe."
                )

            logger.info(f"Resampling {symbol} from {from_timeframe} to {to_timeframe}")

            # Load source data
            source_series = self.storage_service.load_data(
                symbol=symbol,
                timeframe=from_timeframe,
                start_date=start_date,
                end_date=end_date,
            )

            if not source_series.candles:
                logger.warning(f"No {from_timeframe} data found for {symbol}")
                target_timeframe_enum = self._get_timeframe_enum(to_timeframe)
                return PriceDataSeries(
                    symbol=symbol, timeframe=target_timeframe_enum, candles=[]
                )

            # Convert to DataFrame for resampling
            df = self._candles_to_dataframe(source_series.candles)

            if df.empty:
                target_timeframe_enum = self._get_timeframe_enum(to_timeframe)
                return PriceDataSeries(
                    symbol=symbol, timeframe=target_timeframe_enum, candles=[]
                )

            # Resample to target timeframe
            resampled_df = self._resample_dataframe(df, to_timeframe, symbol)

            # Convert back to candles
            resampled_candles = self._dataframe_to_candles(resampled_df, to_timeframe)

            logger.info(
                f"Resampled {len(source_series.candles)} {from_timeframe} candles to "
                f"{len(resampled_candles)} {to_timeframe} candles for {symbol}"
            )

            target_timeframe_enum = self._get_timeframe_enum(to_timeframe)
            return PriceDataSeries(
                symbol=symbol,
                timeframe=target_timeframe_enum,
                candles=resampled_candles,
            )

        except Exception as e:
            raise DataResamplingError(
                f"Failed to resample {symbol} from {from_timeframe} to {to_timeframe}: {str(e)}"
            )

    def resample_data_with_provider_alignment(
        self,
        symbol: str,
        from_timeframe: str,
        to_timeframe: str,
        provider_metadata: Dict[str, str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> PriceDataSeries:
        """
        Resample data using provider-specific alignment strategy.

        This method adjusts the resampling logic based on how the data provider
        natively aggregates data, ensuring our resampled data matches the provider's
        native aggregates.

        Args:
            symbol: Trading symbol
            from_timeframe: Source timeframe
            to_timeframe: Target timeframe
            provider_metadata: Provider-specific alignment metadata
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            Resampled PriceDataSeries
        """
        try:
            # Load source data
            source_series = self.storage_service.load_data(
                symbol=symbol,
                timeframe=from_timeframe,
                start_date=start_date,
                end_date=end_date,
            )

            if not source_series.candles:
                logger.warning(f"No {from_timeframe} data found for {symbol}")
                target_timeframe_enum = self._get_timeframe_enum(to_timeframe)
                return PriceDataSeries(
                    symbol=symbol, timeframe=target_timeframe_enum, candles=[]
                )

            # Convert to DataFrame
            df = self._candles_to_dataframe(source_series.candles)

            # Apply provider-specific resampling
            resampled_df = self._resample_dataframe_with_provider_alignment(
                df, to_timeframe, symbol, provider_metadata
            )

            # Convert back to candles
            resampled_candles = self._dataframe_to_candles(resampled_df, to_timeframe)

            logger.info(
                f"Resampled {len(source_series.candles)} {from_timeframe} candles to "
                f"{len(resampled_candles)} {to_timeframe} candles for {symbol} "
                f"using {provider_metadata.get('alignment_strategy', 'default')} alignment"
            )

            target_timeframe_enum = self._get_timeframe_enum(to_timeframe)
            return PriceDataSeries(
                symbol=symbol,
                timeframe=target_timeframe_enum,
                candles=resampled_candles,
            )

        except Exception as e:
            raise DataResamplingError(
                f"Failed to resample {symbol} from {from_timeframe} to {to_timeframe} "
                f"with provider alignment: {str(e)}"
            )

    def resample_to_daily(
        self,
        symbol: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        source_timeframe: str = "1min",
    ) -> PriceDataSeries:
        """
        Resample intraday data to daily candles.

        Args:
            symbol: Trading symbol
            start_date: Optional start date for resampling
            end_date: Optional end date for resampling
            source_timeframe: Source timeframe (default: "1min")

        Returns:
            PriceDataSeries with daily candles

        Raises:
            DataResamplingError: If resampling fails
        """
        try:
            logger.info(f"Resampling {symbol} from {source_timeframe} to daily")

            # Load source data
            source_series = self.storage_service.load_data(
                symbol=symbol,
                timeframe=source_timeframe,
                start_date=start_date,
                end_date=end_date,
            )

            if not source_series.candles:
                logger.warning(f"No {source_timeframe} data found for {symbol}")
                return PriceDataSeries(
                    symbol=symbol, timeframe=Timeframe.DAILY, candles=[]
                )

            # Convert to DataFrame for resampling
            df = self._candles_to_dataframe(source_series.candles)

            if df.empty:
                return PriceDataSeries(
                    symbol=symbol, timeframe=Timeframe.DAILY, candles=[]
                )

            # Resample to daily
            daily_df = self._resample_to_daily_df(df)

            # Convert back to candles
            daily_candles = self._dataframe_to_candles(daily_df, "daily")

            logger.info(
                f"Resampled {len(source_series.candles)} {source_timeframe} candles to "
                f"{len(daily_candles)} daily candles for {symbol}"
            )

            return PriceDataSeries(
                symbol=symbol, timeframe=Timeframe.DAILY, candles=daily_candles
            )

        except Exception as e:
            raise DataResamplingError(f"Failed to resample {symbol} to daily: {str(e)}")

    def _candles_to_dataframe(self, candles: List[PriceCandle]) -> pd.DataFrame:
        """Convert list of PriceCandle objects to pandas DataFrame."""
        if not candles:
            return pd.DataFrame()

        data: list[dict[str, Any]] = []
        for candle in candles:
            data.append(
                {
                    "date": candle.date,
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                }
            )

        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        return df

    def _resample_to_daily_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Resample DataFrame to daily frequency.

        Args:
            df: DataFrame with datetime index and OHLCV columns

        Returns:
            DataFrame resampled to daily frequency
        """
        # Define aggregation rules for OHLCV data
        agg_rules = {
            "open": "first",  # First value of the day
            "high": "max",  # Maximum value of the day
            "low": "min",  # Minimum value of the day
            "close": "last",  # Last value of the day
            "volume": "sum",  # Sum of all volumes
        }

        # Resample to daily frequency using standard UTC alignment
        # This will be adjusted per asset type in the main resample_data method
        daily_df = df.resample("D").agg(agg_rules)  # type: ignore[arg-type]

        # Remove rows where all OHLC values are NaN (no data for that day)
        daily_df = daily_df.dropna(subset=["open", "high", "low", "close"])  # type: ignore[reportUnknownMemberType]

        # Reset index to make date a column again
        daily_df = daily_df.reset_index()

        return daily_df

    def resample_and_store_daily(
        self,
        symbol: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        source_timeframe: str = "1min",
    ) -> int:
        """
        Resample intraday data to daily and store the result.

        Args:
            symbol: Trading symbol
            start_date: Optional start date
            end_date: Optional end date
            source_timeframe: Source timeframe

        Returns:
            Number of daily candles created and stored

        Raises:
            DataResamplingError: If resampling or storage fails
        """
        try:
            # Resample to daily
            daily_series = self.resample_to_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                source_timeframe=source_timeframe,
            )

            if not daily_series.candles:
                logger.info(f"No daily candles to store for {symbol}")
                return 0

            # Store the daily data
            self.storage_service.store_data(daily_series)

            logger.info(
                f"Successfully resampled and stored {len(daily_series.candles)} "
                f"daily candles for {symbol}"
            )
            return len(daily_series.candles)

        except DataStorageError as e:
            raise DataResamplingError(
                f"Failed to store daily data for {symbol}: {str(e)}"
            )

    def bulk_resample_to_daily(
        self,
        symbols: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        source_timeframe: str = "1min",
    ) -> Dict[str, int]:
        """
        Resample multiple symbols to daily candles.

        Args:
            symbols: List of trading symbols
            start_date: Optional start date
            end_date: Optional end date
            source_timeframe: Source timeframe

        Returns:
            Dictionary mapping symbol to number of daily candles created
        """
        results: Dict[str, int] = {}

        for symbol in symbols:
            try:
                count = self.resample_and_store_daily(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    source_timeframe=source_timeframe,
                )
                results[symbol] = count
                logger.info(f"Completed daily resampling for {symbol}: {count} candles")
            except DataResamplingError as e:
                logger.error(f"Failed to resample {symbol} to daily: {e}")
                results[symbol] = 0

        total_candles = sum(results.values())  # type: ignore[reportUnknownArgumentType]
        successful_symbols = sum(1 for count in results.values() if count > 0)  # type: ignore[reportUnknownVariableType]

        logger.info(
            f"Bulk daily resampling completed: {successful_symbols}/{len(symbols)} "
            f"symbols successful, {total_candles} total daily candles created"
        )

        return results  # type: ignore[reportUnknownVariableType]

    def update_daily_from_recent_data(
        self, symbol: str, days_back: int = 7, source_timeframe: str = "1min"
    ) -> int:
        """
        Update daily candles from recent intraday data.

        This is useful for keeping daily data up-to-date as new intraday data arrives.

        Args:
            symbol: Trading symbol
            days_back: Number of days back to resample (default: 7)
            source_timeframe: Source timeframe

        Returns:
            Number of daily candles updated
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)

        return self.resample_and_store_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            source_timeframe=source_timeframe,
        )

    def get_resampling_candidates(self, source_timeframe: str = "1min") -> List[str]:
        """
        Get list of symbols that have source data available for resampling.

        Args:
            source_timeframe: Source timeframe to check

        Returns:
            List of symbol names that have source data
        """
        return self.storage_service.list_stored_symbols(source_timeframe)

    def _resample_dataframe(
        self, df: pd.DataFrame, to_timeframe: str, symbol: str = ""
    ) -> pd.DataFrame:
        """
        Resample DataFrame to target timeframe using pandas with asset-type-aware alignment.

        Args:
            df: Source DataFrame with OHLCV data
            to_timeframe: Target timeframe string
            symbol: Trading symbol for asset type classification

        Returns:
            Resampled DataFrame

        Raises:
            DataResamplingError: If resampling fails
        """
        try:
            # Get pandas frequency string
            frequency = get_pandas_frequency(to_timeframe)
            if frequency is None:
                raise DataResamplingError(
                    f"Unsupported target timeframe: {to_timeframe}"
                )

            # Get aggregation rules
            agg_rules = get_resampling_rules()

            # Determine asset type and appropriate resampling strategy
            asset_type = (
                self.asset_classifier.classify_symbol(symbol)
                if symbol
                else AssetType.UNKNOWN
            )

            # Apply asset-type-aware resampling alignment
            # Note: Session alignment only applies to short intraday timeframes
            # Longer timeframes (1h+) use standard UTC alignment even for US equities to
            # match Polygon
            if to_timeframe in ["5min", "15min", "30min"]:
                # Get the appropriate offset for this asset type
                offset = get_resampling_offset(asset_type)

                if offset:
                    # Use asset-specific offset (e.g., US equity: 13h30min, Forex: 8h00min)
                    logger.debug(
                        f"Resampling {symbol} ({asset_type}) with offset={offset}"
                    )
                    resampled_df = df.resample(frequency, offset=offset).agg(agg_rules)  # type: ignore[arg-type]
                else:
                    # Use standard UTC alignment (crypto, commodities, etc.)
                    logger.debug(
                        f"Resampling {symbol} ({asset_type}) with standard UTC alignment"
                    )
                    resampled_df = df.resample(frequency).agg(agg_rules)  # type: ignore[arg-type]
            else:
                # Handle longer timeframes (1h, 2h, 4h, daily) with asset-specific alignment
                if to_timeframe == "daily":
                    # Daily boundaries vary by asset type per Polygon's specification
                    if asset_type == AssetType.US_EQUITY:
                        # US stocks: Daily boundary at market close (20:00 UTC / 16:00 ET)
                        logger.debug(
                            f"Resampling {symbol} (US equity) to daily with market close "
                            f"alignment (20:00 UTC)"
                        )
                        resampled_df = df.resample(frequency, offset="20h").agg(agg_rules)  # type: ignore[arg-type]
                    else:
                        # Crypto/Forex: Daily boundary at UTC midnight (00:00 UTC)
                        logger.debug(
                            f"Resampling {symbol} ({asset_type}) to daily with UTC midnight "
                            f"alignment"
                        )
                        resampled_df = df.resample(frequency).agg(agg_rules)  # type: ignore[arg-type]
                else:
                    # Standard UTC resampling for other longer timeframes (1h, 2h, 4h)
                    logger.debug(
                        f"Resampling {symbol} ({asset_type}) to {to_timeframe} with standard "
                        f"UTC alignment (long timeframe)"
                    )
                    resampled_df = df.resample(frequency).agg(agg_rules)  # type: ignore[arg-type]

            # Remove rows where all OHLC values are NaN (no data for that period)
            resampled_df = resampled_df.dropna(subset=["open", "high", "low", "close"])  # type: ignore[reportUnknownMemberType]

            # Reset index to make date a column again
            resampled_df = resampled_df.reset_index()

            return resampled_df

        except Exception as e:
            raise DataResamplingError(
                f"Failed to resample DataFrame to {to_timeframe}: {str(e)}"
            )

    def _resample_dataframe_with_provider_alignment(
        self,
        df: pd.DataFrame,
        to_timeframe: str,
        symbol: str,
        provider_metadata: Dict[str, str],
    ) -> pd.DataFrame:
        """
        Resample DataFrame using provider-specific alignment strategy.

        Args:
            df: Source DataFrame with OHLCV data
            to_timeframe: Target timeframe
            symbol: Trading symbol
            provider_metadata: Provider alignment metadata

        Returns:
            Resampled DataFrame
        """
        try:
            # Get pandas frequency
            frequency = get_pandas_frequency(to_timeframe)
            if not frequency:
                raise DataResamplingError(f"Unsupported timeframe: {to_timeframe}")

            # Get aggregation rules
            agg_rules = get_resampling_rules()

            # Determine alignment strategy based on provider metadata
            alignment_strategy = provider_metadata.get(
                "alignment_strategy", "market_session"
            )
            daily_boundary = provider_metadata.get("daily_boundary", "market_close")

            # Classify asset type for context
            asset_type = self.asset_classifier.classify_symbol(symbol)

            if alignment_strategy == "utc_aligned":
                # Provider uses UTC alignment (like Polygon)
                if to_timeframe == "daily":
                    if daily_boundary == "asset_specific":
                        # Use asset-specific daily boundaries
                        if asset_type == AssetType.US_EQUITY:
                            # US stocks: market close (20:00 UTC)
                            resampled_df = df.resample(frequency, offset="20h").agg(agg_rules)  # type: ignore[arg-type]
                        else:
                            # Crypto/Forex: UTC midnight
                            resampled_df = df.resample(frequency).agg(agg_rules)  # type: ignore[arg-type]
                    else:
                        # Standard UTC alignment
                        resampled_df = df.resample(frequency).agg(agg_rules)  # type: ignore[arg-type]
                else:
                    # Intraday: always UTC aligned for this provider
                    resampled_df = df.resample(frequency).agg(agg_rules)  # type: ignore[arg-type]

            else:
                # Provider uses market session alignment (like Financial Modeling Prep)
                # Fall back to existing asset-type-aware logic
                if to_timeframe in ["5min", "15min", "30min"]:
                    offset = get_resampling_offset(asset_type)
                    if offset:
                        resampled_df = df.resample(frequency, offset=offset).agg(agg_rules)  # type: ignore[arg-type]
                    else:
                        resampled_df = df.resample(frequency).agg(agg_rules)  # type: ignore[arg-type]
                elif to_timeframe == "daily":
                    if asset_type == AssetType.US_EQUITY:
                        resampled_df = df.resample(frequency, offset="20h").agg(agg_rules)  # type: ignore[arg-type]
                    else:
                        resampled_df = df.resample(frequency).agg(agg_rules)  # type: ignore[arg-type]
                else:
                    # Standard UTC for longer timeframes
                    resampled_df = df.resample(frequency).agg(agg_rules)  # type: ignore[arg-type]

            # Remove rows where all OHLC values are NaN
            resampled_df = resampled_df.dropna(subset=["open", "high", "low", "close"])  # type: ignore[reportUnknownMemberType]

            # Reset index to make date a column again
            resampled_df = resampled_df.reset_index()

            return resampled_df

        except Exception as e:
            raise DataResamplingError(
                f"Failed to resample DataFrame with provider alignment: {str(e)}"
            )

    def _get_timeframe_enum(self, timeframe: str) -> Timeframe:
        """
        Convert timeframe string to Timeframe enum.

        Args:
            timeframe: Timeframe string

        Returns:
            Timeframe enum value

        Raises:
            DataResamplingError: If timeframe is not supported
        """
        timeframe_map = {
            "1min": Timeframe.ONE_MIN,
            "5min": Timeframe.FIVE_MIN,
            "15min": Timeframe.FIFTEEN_MIN,
            "30min": Timeframe.THIRTY_MIN,
            "1h": Timeframe.ONE_HOUR,
            "2h": Timeframe.TWO_HOUR,
            "4h": Timeframe.FOUR_HOUR,
            "daily": Timeframe.DAILY,
        }

        enum_value = timeframe_map.get(timeframe)
        if enum_value is None:
            raise DataResamplingError(f"Unsupported timeframe: {timeframe}")

        return enum_value

    def _dataframe_to_candles(
        self, df: pd.DataFrame, timeframe: str
    ) -> List[PriceCandle]:
        """
        Convert pandas DataFrame to list of PriceCandle objects.

        Args:
            df: DataFrame with OHLCV data
            timeframe: Target timeframe for setting appropriate datetime

        Returns:
            List of PriceCandle objects
        """
        if df.empty:
            return []

        candles: list[PriceCandle] = []
        for _, row in df.iterrows():
            try:
                # Handle datetime based on timeframe
                candle_date = row["date"]
                if timeframe == "daily":
                    # For daily candles, use the appropriate boundary time based on asset type
                    # US stocks: 20:00 UTC (market close), Crypto/Forex: 00:00 UTC (midnight)
                    # For now, we'll use 20:00 UTC as the default since most daily data is for
                    # US stocks
                    if hasattr(candle_date, "date"):
                        # If it's already a datetime, extract the date and set time to 20:00 UTC
                        candle_datetime = datetime.combine(
                            candle_date.date(), datetime.min.time().replace(hour=20)
                        ).replace(tzinfo=timezone.utc)
                    else:
                        # If it's a date, convert to datetime at 20:00 UTC
                        candle_datetime = datetime.combine(
                            candle_date, datetime.min.time().replace(hour=20)
                        ).replace(tzinfo=timezone.utc)
                else:
                    # For intraday timeframes, use the timestamp as-is
                    candle_datetime = (
                        candle_date
                        if isinstance(candle_date, datetime)
                        else datetime.combine(candle_date, datetime.min.time())
                    )

                candle = PriceCandle(
                    date=candle_datetime,
                    open=Decimal(str(row["open"])),
                    high=Decimal(str(row["high"])),
                    low=Decimal(str(row["low"])),
                    close=Decimal(str(row["close"])),
                    volume=Decimal(str(row["volume"])),
                )
                candles.append(candle)
            except ValueError as e:
                logger.warning(
                    f"Skipping invalid {timeframe} candle: {row.to_dict()}, error: {e}"  # type: ignore[reportUnknownMemberType]
                )
                continue

        return candles

    def resample_and_store(
        self,
        symbol: str,
        from_timeframe: str,
        to_timeframe: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> int:
        """
        Resample data and store the result.

        Args:
            symbol: Trading symbol
            from_timeframe: Source timeframe
            to_timeframe: Target timeframe
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            Number of candles created and stored

        Raises:
            DataResamplingError: If resampling or storage fails
        """
        try:
            # Resample data
            resampled_series = self.resample_data(
                symbol=symbol,
                from_timeframe=from_timeframe,
                to_timeframe=to_timeframe,
                start_date=start_date,
                end_date=end_date,
            )

            if not resampled_series.candles:
                logger.info(f"No {to_timeframe} candles to store for {symbol}")
                return 0

            # Store the resampled data
            self.storage_service.store_data(resampled_series)

            logger.info(
                f"Successfully resampled and stored {len(resampled_series.candles)} "
                f"{to_timeframe} candles for {symbol}"
            )
            return len(resampled_series.candles)

        except DataStorageError as e:
            raise DataResamplingError(
                f"Failed to store {to_timeframe} data for {symbol}: {str(e)}"
            )

    def bulk_resample(
        self,
        symbols: List[str],
        from_timeframe: str,
        to_timeframe: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, int]:
        """
        Resample multiple symbols from one timeframe to another.

        Args:
            symbols: List of trading symbols
            from_timeframe: Source timeframe
            to_timeframe: Target timeframe
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            Dictionary mapping symbol to number of candles created
        """
        results: Dict[str, int] = {}

        for symbol in symbols:
            try:
                count = self.resample_and_store(
                    symbol=symbol,
                    from_timeframe=from_timeframe,
                    to_timeframe=to_timeframe,
                    start_date=start_date,
                    end_date=end_date,
                )
                results[symbol] = count
                logger.info(
                    f"Completed {to_timeframe} resampling for {symbol}: {count} candles"
                )
            except DataResamplingError as e:
                logger.error(f"Failed to resample {symbol} to {to_timeframe}: {e}")
                results[symbol] = 0

        return results
