"""
Data storage service for managing Parquet files.

This service handles:
- Storing price data in partitioned Parquet files
- Reading existing data
- Managing the folder structure: storage/candles/timeframe/symbol/date.parquet
- Data deduplication and merging
"""

import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from simutrador_core.models.price_data import PriceCandle, PriceDataSeries, Timeframe

from core.settings import get_settings

logger = logging.getLogger(__name__)

# Constants
PARQUET_FILE_PATTERN = "*.parquet"


class DataStorageError(Exception):
    """Exception raised for data storage errors."""

    pass


class DataStorageService:
    """Service for managing Parquet file storage of trading data."""

    def __init__(self):
        """Initialize the storage service."""
        settings = get_settings()
        self.base_path = Path(settings.data_storage.base_path)
        self.candles_path = self.base_path / settings.data_storage.candles_path
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """Ensure required directories exist."""
        self.candles_path.mkdir(parents=True, exist_ok=True)

    def _get_file_path(
        self, symbol: str, timeframe: str, date_obj: Optional[date] = None
    ) -> Path:
        """Get the file path for storing data."""
        if timeframe == Timeframe.DAILY.value:
            return self.candles_path / "daily" / f"{symbol}.parquet"
        else:
            if date_obj is None:
                raise DataStorageError("Date is required for intraday data storage")
            date_str = date_obj.strftime("%Y-%m-%d")
            return self.candles_path / timeframe / symbol / f"{date_str}.parquet"

    def _candles_to_dataframe(self, candles: List[PriceCandle]) -> pd.DataFrame:
        """Convert list of PriceCandle objects to DataFrame."""
        if not candles:
            return pd.DataFrame(
                columns=["date", "open", "high", "low", "close", "volume"]
            )

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
        df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        return df

    def _dataframe_to_candles(self, df: pd.DataFrame) -> List[PriceCandle]:
        """Convert DataFrame to list of PriceCandle objects."""
        if df.empty:
            return []

        candles: list[PriceCandle] = []
        for _, row in df.iterrows():
            candle = PriceCandle(
                date=row["date"],
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=row["volume"],
            )
            candles.append(candle)

        return candles

    def store_data(self, series: PriceDataSeries) -> None:
        """Store price data series to Parquet files."""
        if not series.candles:
            logger.warning(
                f"No candles to store for {series.symbol} {series.timeframe}"
            )
            return

        try:
            if series.timeframe == Timeframe.DAILY:
                # Store daily data in one file
                file_path = self._get_file_path(series.symbol, series.timeframe)
                file_path.parent.mkdir(parents=True, exist_ok=True)

                # Convert to DataFrame
                new_df = self._candles_to_dataframe(series.candles)

                # Merge with existing data if file exists
                if file_path.exists():
                    existing_df = pd.read_parquet(file_path)
                    # Ensure timezone consistency
                    if not existing_df.empty:
                        existing_df["date"] = pd.to_datetime(
                            existing_df["date"], utc=True
                        )
                    new_df["date"] = pd.to_datetime(new_df["date"], utc=True)
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    combined_df = combined_df.sort_values("date").drop_duplicates(
                        subset=["date"], keep="last"
                    )
                else:
                    new_df["date"] = pd.to_datetime(new_df["date"], utc=True)
                    combined_df = new_df

                # Save to file
                combined_df.to_parquet(file_path, index=False)
                logger.info(f"Stored {len(new_df)} daily candles for {series.symbol}")

            else:
                # Group intraday candles by date
                candles_by_date: Dict[date, List[PriceCandle]] = {}
                for candle in series.candles:
                    candle_date = candle.date.date()
                    if candle_date not in candles_by_date:
                        candles_by_date[candle_date] = []
                    candles_by_date[candle_date].append(candle)

                # Store each date separately
                for date_obj, date_candles in candles_by_date.items():
                    file_path = self._get_file_path(
                        series.symbol, series.timeframe, date_obj
                    )
                    file_path.parent.mkdir(parents=True, exist_ok=True)

                    # Convert to DataFrame
                    new_df = self._candles_to_dataframe(date_candles)

                    # Merge with existing data if file exists
                    if file_path.exists():
                        existing_df = pd.read_parquet(file_path)
                        # Ensure timezone consistency
                        if not existing_df.empty:
                            existing_df["date"] = pd.to_datetime(
                                existing_df["date"], utc=True
                            )
                        new_df["date"] = pd.to_datetime(new_df["date"], utc=True)
                        combined_df = pd.concat(
                            [existing_df, new_df], ignore_index=True
                        )
                        combined_df = combined_df.sort_values("date").drop_duplicates(
                            subset=["date"], keep="last"
                        )
                    else:
                        new_df["date"] = pd.to_datetime(new_df["date"], utc=True)
                        combined_df = new_df

                    # Save to file
                    combined_df.to_parquet(file_path, index=False)
                    logger.info(
                        f"Stored {len(new_df)} candles for {series.symbol} "
                        f"{series.timeframe} on {date_obj}"
                    )

        except Exception as e:
            raise DataStorageError(
                f"Failed to store data for {series.symbol}: {str(e)}"
            )

    def load_data(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        order_by: str = "desc",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> PriceDataSeries:
        """Load price data from Parquet files."""
        try:
            # Convert string timeframe to Timeframe enum for PriceDataSeries
            timeframe_enum = (
                Timeframe(timeframe)
                if timeframe != Timeframe.DAILY.value
                else Timeframe.DAILY
            )

            if timeframe == Timeframe.DAILY.value:
                file_path = self._get_file_path(symbol, timeframe)
                if not file_path.exists():
                    return PriceDataSeries(
                        symbol=symbol, timeframe=timeframe_enum, candles=[]
                    )

                df = pd.read_parquet(file_path)

                # Ensure timezone consistency for stored data
                if not df.empty:
                    df["date"] = pd.to_datetime(df["date"], utc=True)

                # Apply date filters if provided
                if start_date or end_date:
                    # Convert timezone-aware dates to date-only for comparison
                    df["date_only"] = df["date"].dt.tz_convert("UTC").dt.date
                    if start_date:
                        df = df[df["date_only"] >= start_date]
                    if end_date:
                        df = df[df["date_only"] <= end_date]
                    df = df.drop("date_only", axis=1)

                # Sort by date according to order_by parameter
                if not df.empty:
                    ascending = order_by == "asc"
                    df = df.sort_values("date", ascending=ascending)

                    # Apply pagination if limit/offset provided
                    if offset is not None and limit is not None:
                        df = df.iloc[offset : offset + limit]
                    elif limit is not None:
                        df = df.head(limit)

                candles = self._dataframe_to_candles(df)
                return PriceDataSeries(
                    symbol=symbol, timeframe=timeframe_enum, candles=candles
                )

            else:
                # For intraday data, use optimized pagination approach
                return self._load_intraday_data_paginated(
                    symbol,
                    timeframe,
                    timeframe_enum,
                    start_date,
                    end_date,
                    order_by,
                    limit,
                    offset,
                )

        except Exception as e:
            logger.error(f"Failed to load data for {symbol} {timeframe}: {e}")
            # Fallback timeframe conversion for error case
            fallback_timeframe = (
                Timeframe(timeframe)
                if timeframe != Timeframe.DAILY.value
                else Timeframe.DAILY
            )
            return PriceDataSeries(
                symbol=symbol, timeframe=fallback_timeframe, candles=[]
            )

    def _load_intraday_data_paginated(
        self,
        symbol: str,
        timeframe: str,
        timeframe_enum: Timeframe,
        start_date: Optional[date],
        end_date: Optional[date],
        order_by: str,
        limit: Optional[int],
        offset: Optional[int],
    ) -> PriceDataSeries:
        """
        Load intraday data with efficient pagination.

        This method optimizes loading by:
        1. Loading files in date order (newest first for desc, oldest first for asc)
        2. Stopping early when we have enough data for the requested page
        3. Only loading the minimum number of files needed
        """
        symbol_dir = self.candles_path / timeframe / symbol
        if not symbol_dir.exists():
            return PriceDataSeries(symbol=symbol, timeframe=timeframe_enum, candles=[])

        # Get all matching files and sort by date
        file_paths: list[tuple[date, Path]] = []
        for file_path in symbol_dir.glob(PARQUET_FILE_PATTERN):
            file_date_str = file_path.stem  # e.g., "2025-07-03"
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()

            # Skip files outside date range
            if start_date and file_date < start_date:
                continue
            if end_date and file_date > end_date:
                continue

            file_paths.append((file_date, file_path))

        if not file_paths:
            return PriceDataSeries(symbol=symbol, timeframe=timeframe_enum, candles=[])

        # Sort files by date (newest first for desc, oldest first for asc)
        ascending_files = order_by == "asc"
        file_paths.sort(key=lambda x: x[0], reverse=not ascending_files)

        # If no pagination requested, load all files (legacy behavior)
        if limit is None and offset is None:
            return self._load_all_intraday_files(
                file_paths, symbol, timeframe_enum, order_by
            )

        # Calculate how many records to skip and take
        skip_count = offset or 0
        take_count = limit or 1000

        all_dfs: list[pd.DataFrame] = []
        total_loaded = 0
        records_to_skip = skip_count

        for file_date, file_path in file_paths:
            df = pd.read_parquet(file_path)
            if df.empty:
                continue

            # Ensure timezone consistency
            df["date"] = pd.to_datetime(df["date"], utc=True)

            # Sort within file
            df = df.sort_values("date", ascending=ascending_files)

            file_record_count = len(df)

            # If we still need to skip records
            if records_to_skip > 0:
                if records_to_skip >= file_record_count:
                    # Skip entire file
                    records_to_skip -= file_record_count
                    continue
                else:
                    # Skip partial file
                    df = df.iloc[records_to_skip:]
                    records_to_skip = 0

            # Take only what we need
            remaining_needed = take_count - total_loaded
            if len(df) > remaining_needed:
                df = df.head(remaining_needed)

            all_dfs.append(df)
            total_loaded += len(df)

            # Stop if we have enough data
            if total_loaded >= take_count:
                break

        if not all_dfs:
            return PriceDataSeries(symbol=symbol, timeframe=timeframe_enum, candles=[])

        # Combine and final sort
        combined_df = pd.concat(all_dfs, ignore_index=True)
        combined_df = combined_df.sort_values("date", ascending=ascending_files)

        candles = self._dataframe_to_candles(combined_df)
        return PriceDataSeries(symbol=symbol, timeframe=timeframe_enum, candles=candles)

    def _load_all_intraday_files(
        self,
        file_paths: List[tuple[date, Path]],
        symbol: str,
        timeframe_enum: Timeframe,
        order_by: str,
    ) -> PriceDataSeries:
        """Load all intraday files (legacy behavior for non-paginated requests)."""
        all_dfs: list[pd.DataFrame] = []
        ascending = order_by == "asc"

        for _, file_path in file_paths:
            df = pd.read_parquet(file_path)
            if not df.empty:
                df["date"] = pd.to_datetime(df["date"], utc=True)
                all_dfs.append(df)

        if not all_dfs:
            return PriceDataSeries(symbol=symbol, timeframe=timeframe_enum, candles=[])

        combined_df = pd.concat(all_dfs, ignore_index=True)
        combined_df = combined_df.sort_values("date", ascending=ascending)

        candles = self._dataframe_to_candles(combined_df)
        return PriceDataSeries(symbol=symbol, timeframe=timeframe_enum, candles=candles)

    def get_total_count(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> int:
        """
        Get total count of records efficiently without loading all data.

        This method counts records by reading only the row count from Parquet files
        without loading the actual data into memory.
        """
        try:
            if timeframe == Timeframe.DAILY.value:
                file_path = self._get_file_path(symbol, timeframe)
                if not file_path.exists():
                    return 0

                # Read parquet metadata to get row count efficiently
                df = pd.read_parquet(file_path, columns=["date"])
                if df.empty:
                    return 0

                # Apply date filters if provided
                if start_date or end_date:
                    df["date"] = pd.to_datetime(df["date"], utc=True)
                    df["date_only"] = df["date"].dt.tz_convert("UTC").dt.date
                    if start_date:
                        df = df[df["date_only"] >= start_date]
                    if end_date:
                        df = df[df["date_only"] <= end_date]

                return len(df)

            else:
                # For intraday data, sum counts from matching files
                symbol_dir = self.candles_path / timeframe / symbol
                if not symbol_dir.exists():
                    return 0

                total_count = 0
                for file_path in symbol_dir.glob(PARQUET_FILE_PATTERN):
                    file_date_str = file_path.stem
                    file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()

                    # Skip files outside date range
                    if start_date and file_date < start_date:
                        continue
                    if end_date and file_date > end_date:
                        continue

                    # Read only metadata to get row count
                    df = pd.read_parquet(file_path, columns=["date"])
                    total_count += len(df)

                return total_count

        except Exception as e:
            logger.error(f"Failed to get total count for {symbol} {timeframe}: {e}")
            return 0

    def get_last_update_date(self, symbol: str, timeframe: str) -> Optional[datetime]:
        """
        Get the date of the last stored candle for a symbol and timeframe.

        This method is optimized to avoid loading entire datasets into memory.
        For intraday data, it finds the latest date file and reads only that file.
        For daily data, it reads the single file but only scans the date column.
        """
        try:
            if timeframe == Timeframe.DAILY.value:
                file_path = self._get_file_path(symbol, timeframe)
                if not file_path.exists():
                    return None

                # Read only the date column to find max date efficiently
                df = pd.read_parquet(file_path, columns=["date"])
                if df.empty:
                    return None
                # Ensure timezone consistency
                df["date"] = pd.to_datetime(df["date"], utc=True)
                return df["date"].max()

            else:
                # For intraday data: find the latest date file by filename
                symbol_dir = self.candles_path / timeframe / symbol
                if not symbol_dir.exists():
                    return None

                # Get all parquet files and find the one with the latest date
                date_files = list(symbol_dir.glob(PARQUET_FILE_PATTERN))
                if not date_files:
                    return None

                # Sort by filename (YYYY-MM-DD format) and get the latest
                latest_file = max(date_files, key=lambda f: f.stem)

                # Read only the date column from the latest file
                df = pd.read_parquet(latest_file, columns=["date"])
                if df.empty:
                    return None
                # Ensure timezone consistency
                df["date"] = pd.to_datetime(df["date"], utc=True)
                return df["date"].max()

        except Exception as e:
            logger.error(
                f"Failed to get last update date for {symbol} {timeframe}: {e}"
            )
            return None

    def list_stored_symbols(self, timeframe: str) -> List[str]:
        """List all symbols that have stored data for a given timeframe."""
        try:
            if timeframe == Timeframe.DAILY.value:
                daily_dir = self.candles_path / "daily"
                if not daily_dir.exists():
                    return []
                return [f.stem for f in daily_dir.glob(PARQUET_FILE_PATTERN)]
            else:
                timeframe_dir = self.candles_path / timeframe
                if not timeframe_dir.exists():
                    return []
                return [d.name for d in timeframe_dir.iterdir() if d.is_dir()]
        except Exception as e:
            logger.error(f"Failed to list symbols for {timeframe}: {e}")
            return []
