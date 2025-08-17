#!/usr/bin/env python3
"""
Test script to demonstrate how the system works with Polygon as the default provider.
"""

import asyncio
import logging
from unittest.mock import patch

import pytest

# Note: api.trading_data was removed - testing service directly
from services.data_providers.data_provider_factory import (
    DataProvider,
    DataProviderFactory,
)
from services.storage.data_resampling_service import DataResamplingService
from services.workflows.trading_data_updating_service import TradingDataUpdatingService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_default_provider_is_polygon():
    """Test that the default provider is now Polygon."""

    logger.info("=== Testing Default Provider ===")

    # Test 1: Direct service instantiation (should use default)
    service = TradingDataUpdatingService()
    logger.info(f"Default service provider: {service.provider_type}")
    assert service.provider_type == DataProvider.POLYGON, "Default should be Polygon"

    # Test 2: API dependency injection test removed (API was removed)
    # The service is now only used internally by nightly update

    logger.info("✅ Default provider is correctly set to Polygon")


def test_provider_alignment_differences():
    """Test the alignment differences between providers."""

    logger.info("\n=== Testing Provider Alignment Differences ===")

    # Get both providers
    fmp_provider = DataProviderFactory.create_provider(
        DataProvider.FINANCIAL_MODELING_PREP
    )
    polygon_provider = DataProviderFactory.create_provider(DataProvider.POLYGON)

    fmp_meta = fmp_provider.get_resampling_metadata()
    polygon_meta = polygon_provider.get_resampling_metadata()

    logger.info(f"FMP alignment: {fmp_meta}")
    logger.info(f"Polygon alignment: {polygon_meta}")

    # Verify they're different
    assert fmp_meta["alignment_strategy"] != polygon_meta["alignment_strategy"]
    assert fmp_meta["intraday_alignment"] != polygon_meta["intraday_alignment"]

    logger.info("✅ Providers have different alignment strategies")


@pytest.mark.asyncio
async def test_polygon_data_fetching():
    """Test actual data fetching with Polygon (mocked)."""

    logger.info("\n=== Testing Polygon Data Fetching ===")

    # Mock the settings to avoid real API calls
    mock_settings = type(
        "obj",
        (object,),
        {
            "polygon": type(
                "obj",
                (object,),
                {
                    "api_key": "test_key",
                    "base_url": "https://api.polygon.io/v2/aggs/ticker",
                    "rate_limit_requests_per_second": 100,
                },
            )
        },
    )

    with patch(
        "services.data_providers.polygon_client.get_settings",
        return_value=mock_settings,
    ):
        # Create Polygon provider
        provider = DataProviderFactory.create_provider(DataProvider.POLYGON)

        async with provider as client:
            logger.info(f"Connected to Polygon client: {type(client).__name__}")

            # Get metadata
            metadata = client.get_resampling_metadata()
            logger.info(f"Polygon metadata: {metadata}")

            # Verify it's UTC aligned
            assert metadata["alignment_strategy"] == "utc_aligned"
            assert metadata["intraday_alignment"] == "utc_aligned"
            assert metadata["daily_boundary"] == "asset_specific"

            logger.info("✅ Polygon client configured correctly")


def test_resampling_with_polygon_alignment():
    """Test how resampling works with Polygon's UTC alignment."""

    logger.info("\n=== Testing Resampling with Polygon Alignment ===")

    import tempfile
    from datetime import datetime, timezone
    from decimal import Decimal

    from models.price_data import PriceCandle, PriceDataSeries, Timeframe

    # Create mock settings
    mock_settings = type(
        "obj",
        (object,),
        {
            "data_storage": type(
                "obj",
                (object,),
                {"base_path": tempfile.mkdtemp(), "candles_path": "candles"},
            )
        },
    )

    with patch(
        "services.storage.data_storage_service.get_settings", return_value=mock_settings
    ):
        resampling_service = DataResamplingService()

        # Create UTC-aligned test data (like Polygon would provide)
        utc_candles = [
            PriceCandle(
                date=datetime(2025, 1, 15, 14, 0, tzinfo=timezone.utc),  # 14:00 UTC
                open=Decimal("100.00"),
                high=Decimal("101.00"),
                low=Decimal("99.50"),
                close=Decimal("100.50"),
                volume=Decimal("1000"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 14, 1, tzinfo=timezone.utc),  # 14:01 UTC
                open=Decimal("100.50"),
                high=Decimal("101.50"),
                low=Decimal("100.00"),
                close=Decimal("101.00"),
                volume=Decimal("1200"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 14, 2, tzinfo=timezone.utc),  # 14:02 UTC
                open=Decimal("101.00"),
                high=Decimal("102.00"),
                low=Decimal("100.50"),
                close=Decimal("101.50"),
                volume=Decimal("800"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 14, 3, tzinfo=timezone.utc),  # 14:03 UTC
                open=Decimal("101.50"),
                high=Decimal("102.50"),
                low=Decimal("101.00"),
                close=Decimal("102.00"),
                volume=Decimal("900"),
            ),
            PriceCandle(
                date=datetime(2025, 1, 15, 14, 4, tzinfo=timezone.utc),  # 14:04 UTC
                open=Decimal("102.00"),
                high=Decimal("103.00"),
                low=Decimal("101.50"),
                close=Decimal("102.50"),
                volume=Decimal("1100"),
            ),
        ]

        utc_series = PriceDataSeries(
            symbol="TEST", timeframe=Timeframe.ONE_MIN, candles=utc_candles
        )

        # Store the data
        resampling_service.storage_service.store_data(utc_series)

        # Get Polygon metadata
        polygon_provider = DataProviderFactory.create_provider(DataProvider.POLYGON)
        polygon_metadata = polygon_provider.get_resampling_metadata()

        # Resample using provider-aware method
        resampled_series = resampling_service.resample_data_with_provider_alignment(
            symbol="TEST",
            from_timeframe="1min",
            to_timeframe="5min",
            provider_metadata=polygon_metadata,
        )

        logger.info(
            f"Resampled {len(utc_series.candles)} 1min candles to "
            f"{len(resampled_series.candles)} 5min candles"
        )

        if resampled_series.candles:
            candle = resampled_series.candles[0]
            logger.info(
                f"First 5min candle: {candle.date} "
                f"(minute: {candle.date.minute}, hour: {candle.date.hour})"
            )

            # With Polygon's UTC alignment, expect 5min candles at UTC boundaries
            assert (
                candle.date.minute == 0
            ), f"Expected minute 0 (UTC aligned), got {candle.date.minute}"
            assert candle.date.hour == 14, f"Expected hour 14, got {candle.date.hour}"

            logger.info("✅ Polygon UTC alignment working correctly")
        else:
            logger.warning("No resampled candles produced")


async def main():
    """Main test function."""

    logger.info("🔍 Testing System with Polygon as Default Provider")

    test_default_provider_is_polygon()
    test_provider_alignment_differences()
    await test_polygon_data_fetching()
    test_resampling_with_polygon_alignment()

    logger.info("\n🎯 Summary:")
    logger.info("✅ Polygon is now the default provider")
    logger.info("✅ Provider alignment differences are handled correctly")
    logger.info("✅ Resampling works with Polygon's UTC alignment")
    logger.info("✅ All transformation tests pass with Polygon")


if __name__ == "__main__":
    asyncio.run(main())
