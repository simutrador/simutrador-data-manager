"""
Tests for provider-aware resampling functionality.

This module demonstrates how to write tests that work with different data providers
by adjusting expectations based on provider alignment strategies.
"""

import sys
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from simutrador_core.models.price_data import PriceCandle, PriceDataSeries, Timeframe

from services.data_providers.data_provider_factory import (
    DataProvider,
    DataProviderFactory,
)
from services.storage.data_resampling_service import DataResamplingService
from services.workflows.trading_data_updating_service import TradingDataUpdatingService


class TestProviderAwareResampling:
    """Test provider-aware resampling with different alignment strategies."""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings for testing."""
        settings = MagicMock()
        settings.data_storage.base_path = tempfile.mkdtemp()
        settings.data_storage.candles_path = "candles"
        return settings

    @pytest.fixture
    def sample_1min_data_utc_aligned(self) -> PriceDataSeries:
        """Create sample 1-minute data with UTC alignment (like Polygon)."""
        candles = [
            PriceCandle(
                date=datetime(2025, 1, 15, 14, 0, tzinfo=UTC),  # 14:00 UTC
                open=Decimal("100.00"),
                high=Decimal("101.00"),
                low=Decimal("99.50"),
                close=Decimal("100.50"),
                volume=Decimal("1000"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 14, 1, tzinfo=UTC),  # 14:01 UTC
                open=Decimal("100.50"),
                high=Decimal("101.50"),
                low=Decimal("100.00"),
                close=Decimal("101.00"),
                volume=Decimal("1200"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 14, 2, tzinfo=UTC),  # 14:02 UTC
                open=Decimal("101.00"),
                high=Decimal("102.00"),
                low=Decimal("100.50"),
                close=Decimal("101.50"),
                volume=Decimal("800"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 14, 3, tzinfo=UTC),  # 14:03 UTC
                open=Decimal("101.50"),
                high=Decimal("102.50"),
                low=Decimal("101.00"),
                close=Decimal("102.00"),
                volume=Decimal("900"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 14, 4, tzinfo=UTC),  # 14:04 UTC
                open=Decimal("102.00"),
                high=Decimal("103.00"),
                low=Decimal("101.50"),
                close=Decimal("102.50"),
                volume=Decimal("1100"),
            ),
        ]

        return PriceDataSeries(
            symbol="TEST", timeframe=Timeframe.ONE_MIN, candles=candles
        )

    @pytest.fixture
    def sample_1min_data_session_aligned(self) -> PriceDataSeries:
        """Create sample 1-minute data with session alignment (like FMP)."""
        candles = [
            PriceCandle(
                date=datetime(2025, 1, 15, 13, 30, tzinfo=UTC),  # Market open
                open=Decimal("100.00"),
                high=Decimal("101.00"),
                low=Decimal("99.50"),
                close=Decimal("100.50"),
                volume=Decimal("1000"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 13, 31, tzinfo=UTC),
                open=Decimal("100.50"),
                high=Decimal("101.50"),
                low=Decimal("100.00"),
                close=Decimal("101.00"),
                volume=Decimal("1200"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 13, 32, tzinfo=UTC),
                open=Decimal("101.00"),
                high=Decimal("102.00"),
                low=Decimal("100.50"),
                close=Decimal("101.50"),
                volume=Decimal("800"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 13, 33, tzinfo=UTC),
                open=Decimal("101.50"),
                high=Decimal("102.50"),
                low=Decimal("101.00"),
                close=Decimal("102.00"),
                volume=Decimal("900"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 13, 34, tzinfo=UTC),
                open=Decimal("102.00"),
                high=Decimal("103.00"),
                low=Decimal("101.50"),
                close=Decimal("102.50"),
                volume=Decimal("1100"),
            ),
        ]

        return PriceDataSeries(
            symbol="TEST", timeframe=Timeframe.ONE_MIN, candles=candles
        )

    def test_financial_modeling_prep_resampling(
        self, mock_settings: Mock, sample_1min_data_session_aligned: PriceDataSeries
    ):
        """Test resampling with Financial Modeling Prep alignment (session-aligned)."""
        with patch(
            "services.storage.data_storage_service.get_settings",
            return_value=mock_settings,
        ):
            resampling_service = DataResamplingService()

            # Store the session-aligned data
            resampling_service.storage_service.store_data(
                sample_1min_data_session_aligned
            )

            # Get FMP provider metadata
            fmp_provider = DataProviderFactory.create_provider(
                DataProvider.FINANCIAL_MODELING_PREP
            )
            fmp_metadata = fmp_provider.get_resampling_metadata()

            # Resample using provider-aware method
            resampled_series = resampling_service.resample_data_with_provider_alignment(
                symbol="TEST",
                from_timeframe="1min",
                to_timeframe="5min",
                provider_metadata=fmp_metadata,
            )

            # For FMP (session-aligned), expect 5-min candles aligned to market session
            assert len(resampled_series.candles) == 1
            candle = resampled_series.candles[0]

            # Should be aligned to 13:30 (market session boundary)
            assert candle.date.minute == 30
            assert candle.date.hour == 13

    def test_polygon_resampling(
        self, mock_settings: Mock, sample_1min_data_utc_aligned: PriceDataSeries
    ):
        """Test resampling with Polygon alignment (UTC-aligned)."""
        with patch(
            "services.storage.data_storage_service.get_settings",
            return_value=mock_settings,
        ):
            resampling_service = DataResamplingService()

            # Store the UTC-aligned data
            resampling_service.storage_service.store_data(sample_1min_data_utc_aligned)

            # Get Polygon provider metadata
            polygon_provider = DataProviderFactory.create_provider(DataProvider.POLYGON)
            polygon_metadata = polygon_provider.get_resampling_metadata()

            # Resample using provider-aware method
            resampled_series = resampling_service.resample_data_with_provider_alignment(
                symbol="TEST",
                from_timeframe="1min",
                to_timeframe="5min",
                provider_metadata=polygon_metadata,
            )

            # For Polygon (UTC-aligned), expect 5-min candles aligned to UTC boundaries
            assert len(resampled_series.candles) == 1
            candle = resampled_series.candles[0]

            # Should be aligned to UTC boundary (14:00)
            assert candle.date.minute == 0
            assert candle.date.hour == 14

    def test_service_with_different_providers(self):
        """Test that TradingDataUpdatingService works with different providers."""

        # Test with Financial Modeling Prep
        fmp_service = TradingDataUpdatingService(
            provider_type=DataProvider.FINANCIAL_MODELING_PREP
        )
        assert fmp_service.provider_type == DataProvider.FINANCIAL_MODELING_PREP

        # Test with Polygon
        polygon_service = TradingDataUpdatingService(provider_type=DataProvider.POLYGON)
        assert polygon_service.provider_type == DataProvider.POLYGON

        # Verify they use different alignment strategies
        fmp_provider = DataProviderFactory.create_provider(
            DataProvider.FINANCIAL_MODELING_PREP
        )
        polygon_provider = DataProviderFactory.create_provider(DataProvider.POLYGON)

        fmp_metadata = fmp_provider.get_resampling_metadata()
        polygon_metadata = polygon_provider.get_resampling_metadata()

        assert fmp_metadata["alignment_strategy"] == "market_session"
        assert polygon_metadata["alignment_strategy"] == "utc_aligned"

    def test_provider_metadata_consistency(self):
        """Test that all providers return consistent metadata format."""

        providers = [
            DataProvider.FINANCIAL_MODELING_PREP,
            DataProvider.POLYGON,
            DataProvider.TIINGO,
        ]

        required_keys = ["alignment_strategy", "daily_boundary", "intraday_alignment"]

        for provider_type in providers:
            provider = DataProviderFactory.create_provider(provider_type)
            metadata = provider.get_resampling_metadata()

            # Check all required keys are present
            for key in required_keys:
                assert (
                    key in metadata
                ), f"Provider {provider_type.value} missing key: {key}"

            # Check values are valid
            assert metadata["alignment_strategy"] in [
                "market_session",
                "utc_aligned",
                "utc_midnight",
            ]
            assert metadata["daily_boundary"] in [
                "market_close",
                "utc_midnight",
                "asset_specific",
            ]
            assert metadata["intraday_alignment"] in ["session_aligned", "utc_aligned"]
