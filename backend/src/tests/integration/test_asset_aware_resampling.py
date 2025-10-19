"""
Tests for asset-type-aware resampling functionality.

This module validates that different asset types get appropriate resampling
alignment strategies: US equities use market session alignment, crypto uses
standard UTC alignment, etc.
"""

import logging
import sys
import tempfile
from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from simutrador_core.models.asset_types import AssetType
from simutrador_core.models.price_data import PriceCandle, PriceDataSeries, Timeframe

from services.classification.asset_classification_service import (
    AssetClassificationService,
)
from services.storage.data_resampling_service import DataResamplingService

logger = logging.getLogger(__name__)


class TestAssetAwareResampling:
    """Test cases for asset-type-aware resampling."""

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
    def resampling_service(self, mock_settings: MagicMock) -> DataResamplingService:
        """Create resampling service with temporary storage."""
        with patch(
            "services.storage.data_storage_service.get_settings",
            return_value=mock_settings,
        ):
            return DataResamplingService()

    @pytest.fixture
    def asset_classifier(self) -> AssetClassificationService:
        """Create asset classification service."""
        return AssetClassificationService()

    @pytest.fixture
    def sample_24h_data(self) -> list[PriceCandle]:
        """Create sample 1-minute data covering 24 hours for crypto testing."""
        candles: list[PriceCandle] = []
        base_price = Decimal("50000.00")  # BTC price

        # Generate 24 hours of 1-minute data (1440 minutes)
        for minute in range(1440):
            timestamp = datetime(2024, 1, 15, 0, 0, tzinfo=UTC) + pd.Timedelta(
                minutes=minute
            )

            # Simulate some price movement
            price_offset = Decimal(str(minute * 0.1))

            candle = PriceCandle(
                date=timestamp,
                open=base_price + price_offset,
                high=base_price + price_offset + Decimal("50.00"),
                low=base_price + price_offset - Decimal("25.00"),
                close=base_price + price_offset + Decimal("10.00"),
                volume=Decimal(str(100 + minute * 5)),
            )
            candles.append(candle)

        return candles

    def test_asset_classification(
        self, asset_classifier: AssetClassificationService
    ) -> None:
        """Test that symbols are correctly classified by asset type."""
        test_cases = [
            ("AAPL", AssetType.US_EQUITY),
            ("MSFT", AssetType.US_EQUITY),
            ("BTC-USD", AssetType.CRYPTO),
            ("ETH-USDT", AssetType.CRYPTO),
            ("EURUSD", AssetType.FOREX),
            ("GBP/USD", AssetType.FOREX),
        ]

        for symbol, expected_type in test_cases:
            actual_type = asset_classifier.classify_symbol(symbol)
            assert (
                actual_type == expected_type
            ), f"Symbol {symbol} classified as {actual_type}, expected {expected_type}"

    def test_us_equity_resampling_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_24h_data: list[PriceCandle],
    ) -> None:
        """Test that US equity symbols use market session alignment."""
        symbol = "AAPL"

        # Create series with US equity symbol
        series_1min = PriceDataSeries(
            symbol=symbol, timeframe=Timeframe.ONE_MIN, candles=sample_24h_data
        )

        # Store the data
        resampling_service.storage_service.store_data(series_1min)

        # Resample to 5-minute
        resampled_series = resampling_service.resample_data(
            symbol=symbol, from_timeframe="1min", to_timeframe="5min"
        )

        # Verify we have resampled data
        assert len(resampled_series.candles) > 0

        # For US equity, candles should align to market session boundaries
        # Since our test data starts at 00:00 UTC, we should see candles at:
        # 13:30, 13:35, 13:40, etc. (market session aligned)
        market_aligned_candles = [
            candle
            for candle in resampled_series.candles
            if candle.date.hour == 13 and candle.date.minute >= 30
        ]

        # Should have market-aligned candles
        assert (
            len(market_aligned_candles) > 0
        ), "US equity should have market-aligned candles"

        # Check that first market candle starts at 13:30
        first_market_candle = market_aligned_candles[0]
        assert first_market_candle.date.hour == 13
        assert first_market_candle.date.minute == 30

    def test_crypto_resampling_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_24h_data: list[PriceCandle],
    ) -> None:
        """Test that crypto symbols use standard UTC alignment."""
        symbol = "BTC-USD"

        # Create series with crypto symbol
        series_1min = PriceDataSeries(
            symbol=symbol, timeframe=Timeframe.ONE_MIN, candles=sample_24h_data
        )

        # Store the data
        resampling_service.storage_service.store_data(series_1min)

        # Resample to 5-minute
        resampled_series = resampling_service.resample_data(
            symbol=symbol, from_timeframe="1min", to_timeframe="5min"
        )

        # Verify we have resampled data
        assert len(resampled_series.candles) > 0

        # For crypto, candles should align to standard UTC boundaries
        # Starting at 00:00 UTC: 00:00, 00:05, 00:10, etc.
        utc_aligned_candles = [
            candle
            for candle in resampled_series.candles
            if candle.date.hour == 0
            and candle.date.minute in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        ]

        # Should have UTC-aligned candles
        assert len(utc_aligned_candles) > 0, "Crypto should have UTC-aligned candles"

        # Check that first candle starts at 00:00 (standard UTC alignment)
        first_candle = resampled_series.candles[0]
        assert first_candle.date.hour == 0
        assert first_candle.date.minute == 0

    def test_forex_resampling_alignment(
        self,
        resampling_service: DataResamplingService,
        sample_24h_data: list[PriceCandle],
    ) -> None:
        """Test that forex symbols use appropriate session alignment."""
        symbol = "EURUSD"

        # Create series with forex symbol
        series_1min = PriceDataSeries(
            symbol=symbol, timeframe=Timeframe.ONE_MIN, candles=sample_24h_data
        )

        # Store the data
        resampling_service.storage_service.store_data(series_1min)

        # Resample to 1-hour
        resampled_series = resampling_service.resample_data(
            symbol=symbol, from_timeframe="1min", to_timeframe="1h"
        )

        # Verify we have resampled data
        assert len(resampled_series.candles) > 0

        # For forex, should align to London session (8:00 UTC)
        london_aligned_candles = [
            candle
            for candle in resampled_series.candles
            if candle.date.hour == 8 and candle.date.minute == 0
        ]

        # Should have London session-aligned candles
        assert (
            len(london_aligned_candles) > 0
        ), "Forex should have London session-aligned candles"

    def test_different_timeframes_same_asset(
        self,
        resampling_service: DataResamplingService,
        sample_24h_data: list[PriceCandle],
    ) -> None:
        """Test that different timeframes maintain consistent alignment for same asset type."""
        symbol = "BTC-USD"  # Crypto symbol

        # Create series
        series_1min = PriceDataSeries(
            symbol=symbol, timeframe=Timeframe.ONE_MIN, candles=sample_24h_data
        )

        # Store the data
        resampling_service.storage_service.store_data(series_1min)

        # Test multiple timeframes
        timeframes = ["5min", "15min", "30min", "1h"]

        for timeframe in timeframes:
            resampled_series = resampling_service.resample_data(
                symbol=symbol, from_timeframe="1min", to_timeframe=timeframe
            )

            # All crypto timeframes should start at UTC boundaries
            first_candle = resampled_series.candles[0]

            # Should start at 00:00 for crypto (UTC alignment)
            assert (
                first_candle.date.hour == 0
            ), f"Crypto {timeframe} candles should start at hour 0, got {first_candle.date.hour}"

            # Minutes should be appropriate for the timeframe
            if timeframe == "5min":
                assert first_candle.date.minute == 0
            elif timeframe == "15min":
                assert first_candle.date.minute == 0
            elif timeframe == "30min":
                assert first_candle.date.minute == 0
            elif timeframe == "1h":
                assert first_candle.date.minute == 0

    def test_volume_aggregation_by_asset_type(
        self,
        resampling_service: DataResamplingService,
        sample_24h_data: list[PriceCandle],
    ) -> None:
        """Test that volume aggregation works correctly for different asset types."""
        test_symbols = [
            "AAPL",
            "BTC-USD",
        ]

        for symbol in test_symbols:
            # Create series
            series_1min = PriceDataSeries(
                symbol=symbol, timeframe=Timeframe.ONE_MIN, candles=sample_24h_data
            )

            # Store the data
            resampling_service.storage_service.store_data(series_1min)

            # Resample to 5-minute
            resampled_series = resampling_service.resample_data(
                symbol=symbol, from_timeframe="1min", to_timeframe="5min"
            )

            # Verify volume aggregation
            assert len(resampled_series.candles) > 0

            # Get first 5-minute candle and corresponding 1-minute candles
            first_5min_candle = resampled_series.candles[0]

            # Find the 5 1-minute candles that should be aggregated into this 5-minute candle
            start_time = first_5min_candle.date
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=UTC)
            end_time = start_time + pd.Timedelta(minutes=5)

            corresponding_1min_candles = [
                candle
                for candle in sample_24h_data
                if start_time <= candle.date < end_time
            ]

            if corresponding_1min_candles:
                expected_volume = sum(
                    candle.volume for candle in corresponding_1min_candles
                )
                assert first_5min_candle.volume == expected_volume, (
                    f"Volume mismatch for {symbol}: expected {expected_volume}, "
                    f"got {first_5min_candle.volume}"
                )
