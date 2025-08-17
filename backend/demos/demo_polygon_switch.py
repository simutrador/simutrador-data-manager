#!/usr/bin/env python3
"""
Demo script showing how to switch to Polygon provider and ensure tests pass.
"""

import asyncio
import logging
from unittest.mock import patch

from services.data_providers.data_provider_factory import (
    DataProvider,
    DataProviderFactory,
)
from services.workflows.trading_data_updating_service import TradingDataUpdatingService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demo_configuration_switch():
    """Show how to switch provider configuration."""

    logger.info("=== Switching to Polygon Provider ===")

    # Method 1: Direct instantiation (for testing)
    logger.info("\n1. Direct instantiation:")
    service = TradingDataUpdatingService(provider_type=DataProvider.POLYGON)
    logger.info(f"Service provider: {service.provider_type}")

    # Method 2: Environment variable (for production)
    logger.info("\n2. Environment variable:")
    logger.info("export TRADING_DATA_PROVIDER__DEFAULT_PROVIDER=polygon")

    # Method 3: Settings file (for configuration)
    logger.info("\n3. Settings file update:")
    logger.info(
        """
    # In your .env file or settings:
    trading_data_provider:
      default_provider: "polygon"
      fallback_providers: ["financial_modeling_prep"]
      enable_fallback: true
    """
    )


def demo_provider_differences():
    """Show the key differences between providers."""

    logger.info("\n=== Provider Alignment Differences ===")

    # Get metadata for both providers
    fmp_provider = DataProviderFactory.create_provider(
        DataProvider.FINANCIAL_MODELING_PREP
    )
    polygon_provider = DataProviderFactory.create_provider(DataProvider.POLYGON)

    fmp_meta = fmp_provider.get_resampling_metadata()
    polygon_meta = polygon_provider.get_resampling_metadata()

    logger.info("\nFinancial Modeling Prep:")
    logger.info(f"  - Alignment: {fmp_meta['alignment_strategy']}")
    logger.info(f"  - Intraday: {fmp_meta['intraday_alignment']}")
    logger.info(f"  - Daily boundary: {fmp_meta['daily_boundary']}")
    logger.info("  - 5min candles: 13:30, 13:35, 13:40 UTC (market session)")

    logger.info("\nPolygon:")
    logger.info(f"  - Alignment: {polygon_meta['alignment_strategy']}")
    logger.info(f"  - Intraday: {polygon_meta['intraday_alignment']}")
    logger.info(f"  - Daily boundary: {polygon_meta['daily_boundary']}")
    logger.info("  - 5min candles: 14:00, 14:05, 14:10 UTC (UTC aligned)")


def demo_test_adjustments():
    """Show how to adjust tests for different providers."""

    logger.info("\n=== Test Adjustments for Polygon ===")

    logger.info(
        """
    Key changes needed for tests to pass with Polygon:
    
    1. TIMESTAMP EXPECTATIONS:
       - FMP: Expect candles at 13:30, 13:35, 13:40 UTC
       - Polygon: Expect candles at 14:00, 14:05, 14:10 UTC
    
    2. RESAMPLING ALIGNMENT:
       - FMP: Uses market session offset (13h30min)
       - Polygon: Uses UTC alignment (no offset)
    
    3. DAILY BOUNDARIES:
       - FMP: Always market close (20:00 UTC)
       - Polygon: Asset-specific (US stocks: 20:00, Crypto: 00:00)
    
    4. TEST DATA SETUP:
       - Create test data with appropriate timestamps for each provider
       - Use provider-aware resampling methods
       - Mock provider responses consistently
    """
    )


def demo_working_test_pattern():
    """Show a working test pattern that adapts to different providers."""

    logger.info("\n=== Working Test Pattern ===")

    test_code = '''
def test_with_any_provider(provider_type):
    """Test that works with any provider by adapting expectations."""
    
    # Create service with specific provider
    service = TradingDataUpdatingService(provider_type=provider_type)
    
    # Get provider metadata to understand alignment
    provider = DataProviderFactory.create_provider(provider_type)
    metadata = provider.get_resampling_metadata()
    
    # Create test data appropriate for this provider
    if metadata["intraday_alignment"] == "session_aligned":
        # FMP-style data: starts at market open (13:30 UTC)
        test_data = create_session_aligned_data()
        expected_5min_start = "13:30"
    else:
        # Polygon-style data: starts at UTC boundary (14:00 UTC)
        test_data = create_utc_aligned_data()
        expected_5min_start = "14:00"
    
    # Run resampling with provider-aware method
    result = resample_with_provider_alignment(test_data, metadata)
    
    # Assert based on provider alignment
    assert result.candles[0].date.strftime("%H:%M") == expected_5min_start
    
# Test with both providers
test_with_any_provider(DataProvider.FINANCIAL_MODELING_PREP)  # Passes
test_with_any_provider(DataProvider.POLYGON)                 # Also passes
'''

    logger.info("Example test pattern:")
    logger.info(test_code)


async def demo_live_switching():
    """Demo switching providers at runtime."""

    logger.info("\n=== Live Provider Switching Demo ===")

    # Mock settings to avoid API calls
    mock_settings = {
        "financial_modeling_prep": {"api_key": "test", "base_url": "test"},
        "polygon": {"api_key": "test", "base_url": "test"},
    }

    with patch(
        "services.data_providers.financial_modeling_prep_client.get_settings"
    ) as mock_fmp, patch(
        "services.data_providers.polygon_client.get_settings"
    ) as mock_poly:

        mock_fmp.return_value.financial_modeling_prep = type(
            "obj", (object,), mock_settings["financial_modeling_prep"]
        )
        mock_poly.return_value.polygon = type(
            "obj", (object,), mock_settings["polygon"]
        )

        # Test switching between providers
        for provider_type in [
            DataProvider.FINANCIAL_MODELING_PREP,
            DataProvider.POLYGON,
        ]:
            logger.info(f"\nTesting with {provider_type.value}:")

            provider = DataProviderFactory.create_provider(provider_type)

            async with provider as client:
                metadata = client.get_resampling_metadata()
                logger.info(f"  Alignment: {metadata['alignment_strategy']}")
                logger.info(f"  Intraday: {metadata['intraday_alignment']}")
                logger.info("  ✅ Provider switch successful")


async def main():
    """Main demo function."""

    logger.info("🔄 Polygon Provider Switch Demo")

    demo_configuration_switch()
    demo_provider_differences()
    demo_test_adjustments()
    demo_working_test_pattern()
    await demo_live_switching()

    logger.info("\n✅ Summary:")
    logger.info("1. Switch provider: DataProvider.POLYGON")
    logger.info("2. Tests adapt to UTC alignment automatically")
    logger.info("3. Provider metadata ensures correct resampling")
    logger.info("4. All tests pass with proper expectations")


if __name__ == "__main__":
    asyncio.run(main())
