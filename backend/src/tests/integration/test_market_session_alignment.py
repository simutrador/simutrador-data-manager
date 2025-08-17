"""
Tests for market session alignment in data resampling.

This module validates that resampled candles are properly aligned to US market
session boundaries (13:30 UTC market open) for all intraday timeframes.
"""

import logging
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Generator, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from models.price_data import PriceCandle, PriceDataSeries, Timeframe
from services.storage.data_resampling_service import DataResamplingService
from services.storage.data_storage_service import DataStorageService

logger = logging.getLogger(__name__)


class TestMarketSessionAlignment:
    """Test cases for market session alignment in resampling."""

    @pytest.fixture
    def temp_storage_dir(self) -> Generator[Path, None, None]:
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def mock_settings(self, temp_storage_dir: Path) -> MagicMock:
        """Mock settings with temporary directory."""
        mock_settings = MagicMock()
        mock_settings.data_storage.base_path = str(temp_storage_dir)
        mock_settings.data_storage.candles_path = "candles"
        return mock_settings

    @pytest.fixture
    def storage_service(self, mock_settings: MagicMock) -> DataStorageService:
        """Create a data storage service with temporary directory."""
        with patch(
            "services.storage.data_storage_service.get_settings",
            return_value=mock_settings,
        ):
            service = DataStorageService()
            return service

    @pytest.fixture
    def resampling_service(self, mock_settings: MagicMock) -> DataResamplingService:
        """Create resampling service with temporary storage."""
        with patch(
            "services.storage.data_storage_service.get_settings",
            return_value=mock_settings,
        ):
            return DataResamplingService()

    @pytest.fixture
    def sample_1min_data(self) -> PriceDataSeries:
        """Create sample 1-minute data covering a full trading day."""
        # Create 1-minute candles from 13:30 to 20:00 UTC (market hours)
        candles: List[PriceCandle] = []
        base_price = Decimal("100.00")

        # Generate 390 minutes of data (6.5 hours of trading)
        for minute in range(390):
            timestamp = datetime(
                2024, 1, 15, 13, 30, tzinfo=timezone.utc
            ) + pd.Timedelta(minutes=minute)

            # Simulate some price movement
            price_offset = Decimal(str(minute * 0.01))  # Small price drift

            candle = PriceCandle(
                date=timestamp,
                open=base_price + price_offset,
                high=base_price + price_offset + Decimal("0.50"),
                low=base_price + price_offset - Decimal("0.25"),
                close=base_price + price_offset + Decimal("0.10"),
                volume=Decimal(str(1000 + minute * 10)),
            )
            candles.append(candle)

        return PriceDataSeries(
            symbol="TEST", timeframe=Timeframe.ONE_MIN, candles=candles
        )

    def test_5min_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_1min_data: PriceDataSeries,
    ) -> None:
        """Test that 5-minute candles align to market session boundaries."""
        # Store the 1-minute data
        resampling_service.storage_service.store_data(sample_1min_data)

        # Resample to 5-minute
        resampled_series = resampling_service.resample_data(
            symbol="TEST", from_timeframe="1min", to_timeframe="5min"
        )

        # Verify we have resampled data
        assert len(resampled_series.candles) > 0

        # Check that all 5-minute candles start at expected times
        # 5-minute intervals starting from 13:30: 30, 35, 40, 45, 50, 55, 00, 05, 10, 15, 20, 25
        expected_minutes = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]

        for candle in resampled_series.candles:
            minute = candle.date.minute
            assert minute in expected_minutes, (
                f"5min candle at {candle.date} has minute={minute}, "
                f"expected one of {expected_minutes}"
            )

            # Verify it's within market hours (13:30-20:00 UTC)
            hour = candle.date.hour
            if hour == 13:
                assert (
                    minute >= 30
                ), f"Candle at {candle.date} starts before market open"
            elif hour == 19:
                assert (
                    minute <= 55
                ), f"Candle at {candle.date} extends past market close"
            else:
                assert (
                    13 <= hour <= 19
                ), f"Candle at {candle.date} is outside market hours"

    def test_15min_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_1min_data: PriceDataSeries,
    ) -> None:
        """Test that 15-minute candles align to market session boundaries."""
        # Store the 1-minute data
        resampling_service.storage_service.store_data(sample_1min_data)

        # Resample to 15-minute
        resampled_series = resampling_service.resample_data(
            symbol="TEST", from_timeframe="1min", to_timeframe="15min"
        )

        # Verify we have resampled data
        assert len(resampled_series.candles) > 0

        # Check that all 15-minute candles start at expected times
        # 15-minute intervals starting from 13:30: 30, 45, 00, 15
        expected_minutes = [0, 15, 30, 45]

        for candle in resampled_series.candles:
            minute = candle.date.minute
            assert minute in expected_minutes, (
                f"15min candle at {candle.date} has minute={minute}, "
                f"expected one of {expected_minutes}"
            )

    def test_30min_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_1min_data: PriceDataSeries,
    ) -> None:
        """Test that 30-minute candles align to market session boundaries."""
        # Store the 1-minute data
        resampling_service.storage_service.store_data(sample_1min_data)

        # Resample to 30-minute
        resampled_series = resampling_service.resample_data(
            symbol="TEST", from_timeframe="1min", to_timeframe="30min"
        )

        # Verify we have resampled data
        assert len(resampled_series.candles) > 0

        # Check that all 30-minute candles start at expected times
        for candle in resampled_series.candles:
            minute = candle.date.minute
            # 30-minute intervals starting from 13:30: 30, 00
            expected_minute = minute in [30, 0]
            assert expected_minute, (
                f"30min candle at {candle.date} has minute={minute}, "
                f"expected 30 or 00"
            )

    def test_1h_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_1min_data: PriceDataSeries,
    ) -> None:
        """Test that 1-hour candles use UTC alignment to match Polygon's native aggregates."""
        # Store the 1-minute data
        resampling_service.storage_service.store_data(sample_1min_data)

        # Resample to 1-hour
        resampled_series = resampling_service.resample_data(
            symbol="TEST", from_timeframe="1min", to_timeframe="1h"
        )

        # Verify we have resampled data
        assert len(resampled_series.candles) > 0

        # Check that all 1-hour candles use UTC alignment (start at :00 minutes)
        # This matches Polygon's native 1h aggregation behavior
        for candle in resampled_series.candles:
            minute = candle.date.minute
            assert (
                minute == 0
            ), f"1h candle at {candle.date} has minute={minute}, expected 0 (UTC aligned)"

            # Verify hours are within expected range for market data
            hour = candle.date.hour
            assert 13 <= hour <= 19, (
                f"1h candle at {candle.date} has hour={hour}, "
                f"expected between 13-19 (market hours)"
            )

    def test_market_open_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_1min_data: PriceDataSeries,
    ) -> None:
        """Test that the first candle of session-aligned timeframes starts at market open."""
        # Store the 1-minute data
        resampling_service.storage_service.store_data(sample_1min_data)

        # Only test session-aligned timeframes (1h uses UTC alignment to match Polygon)
        session_aligned_timeframes = ["5min", "15min", "30min"]

        for timeframe in session_aligned_timeframes:
            resampled_series = resampling_service.resample_data(
                symbol="TEST", from_timeframe="1min", to_timeframe=timeframe
            )

            # Get the first candle
            first_candle = resampled_series.candles[0]

            # Verify it starts at market open (13:30 UTC)
            assert first_candle.date.hour == 13, (
                f"First {timeframe} candle starts at hour {first_candle.date.hour}, "
                f"expected 13 (market open)"
            )
            assert first_candle.date.minute == 30, (
                f"First {timeframe} candle starts at minute {first_candle.date.minute}, "
                f"expected 30 (market open)"
            )

    def test_1h_utc_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_1min_data: PriceDataSeries,
    ) -> None:
        """Test that 1h candles use UTC alignment and start at the appropriate hour."""
        # Store the 1-minute data
        resampling_service.storage_service.store_data(sample_1min_data)

        # Resample to 1-hour
        resampled_series = resampling_service.resample_data(
            symbol="TEST", from_timeframe="1min", to_timeframe="1h"
        )

        # Get the first candle
        first_candle = resampled_series.candles[0]

        # 1h candles use UTC alignment, so they start at the top of the hour
        # The first 1h candle should start at 13:00 (covers 13:00-14:00, includes 13:30 market open)
        assert first_candle.date.hour == 13, (
            f"First 1h candle starts at hour {first_candle.date.hour}, "
            f"expected 13 (UTC aligned)"
        )
        assert first_candle.date.minute == 0, (
            f"First 1h candle starts at minute {first_candle.date.minute}, "
            f"expected 0 (UTC aligned)"
        )

    def test_volume_aggregation_with_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_1min_data: PriceDataSeries,
    ) -> None:
        """Test that volume is correctly aggregated with market session alignment."""
        # Store the 1-minute data
        resampling_service.storage_service.store_data(sample_1min_data)

        # Resample to 5-minute
        resampled_series = resampling_service.resample_data(
            symbol="TEST", from_timeframe="1min", to_timeframe="5min"
        )

        # Get the first 5-minute candle (13:30-13:35)
        first_5min_candle = resampled_series.candles[0]

        # Calculate expected volume from first 5 1-minute candles
        first_5_1min_candles = sample_1min_data.candles[:5]
        expected_volume = sum(candle.volume for candle in first_5_1min_candles)

        assert first_5min_candle.volume == expected_volume, (
            f"5min candle volume {first_5min_candle.volume} doesn't match "
            f"sum of 1min volumes {expected_volume}"
        )

    def test_ohlc_aggregation_with_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_1min_data: PriceDataSeries,
    ) -> None:
        """Test that OHLC values are correctly aggregated with market session alignment."""
        # Store the 1-minute data
        resampling_service.storage_service.store_data(sample_1min_data)

        # Resample to 5-minute
        resampled_series = resampling_service.resample_data(
            symbol="TEST", from_timeframe="1min", to_timeframe="5min"
        )

        # Get the first 5-minute candle (13:30-13:35)
        first_5min_candle = resampled_series.candles[0]

        # Get the corresponding 1-minute candles
        first_5_1min_candles = sample_1min_data.candles[:5]

        # Verify OHLC aggregation
        expected_open = first_5_1min_candles[0].open
        expected_high = max(candle.high for candle in first_5_1min_candles)
        expected_low = min(candle.low for candle in first_5_1min_candles)
        expected_close = first_5_1min_candles[-1].close

        assert first_5min_candle.open == expected_open
        assert first_5min_candle.high == expected_high
        assert first_5min_candle.low == expected_low
        assert first_5min_candle.close == expected_close
