#!/usr/bin/env python3
"""
Demonstration script showing the vendor-agnostic data provider architecture.

This script shows how to:
1. Use different data providers through the factory pattern
2. Switch between providers easily
3. Handle provider-specific errors uniformly
"""

import asyncio
import logging
from datetime import date, timedelta

from services.data_providers.data_provider_factory import (
    DataProvider,
    DataProviderFactory,
)
from services.data_providers.data_provider_interface import (
    AuthenticationError,
    DataProviderError,
    RateLimitError,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demonstrate_provider(provider_type: DataProvider, symbol: str = "AAPL"):
    """
    Demonstrate using a specific data provider.

    Args:
        provider_type: The data provider to use
        symbol: Trading symbol to fetch data for
    """
    logger.info(f"\n=== Demonstrating {provider_type.value} ===")

    try:
        # Create provider instance using factory
        provider = DataProviderFactory.create_provider(provider_type)
        logger.info(f"Created provider: {type(provider).__name__}")

        # Use the provider in async context
        async with provider as client:
            logger.info(f"Connected to {provider_type.value}")

            # Fetch historical data
            end_date = date.today()
            start_date = end_date - timedelta(days=1)

            logger.info(f"Fetching {symbol} data from {start_date} to {end_date}")

            try:
                series = await client.fetch_historical_data(
                    symbol=symbol,
                    timeframe="1min",
                    from_date=start_date,
                    to_date=end_date,
                )

                logger.info(f"Successfully fetched {len(series.candles)} candles")
                if series.candles:
                    first_candle = series.candles[0]
                    logger.info(
                        f"First candle: {first_candle.date} - O:{first_candle.open} \
                            H:{first_candle.high} L:{first_candle.low} C:{first_candle.close}"
                    )

                # Fetch latest data
                latest = await client.fetch_latest_data(symbol, "1min")
                if latest:
                    logger.info(f"Latest candle: {latest.date} - Close: {latest.close}")
                else:
                    logger.info("No latest data available")

            except AuthenticationError as e:
                logger.error(f"Authentication failed for {provider_type.value}: {e}")
            except RateLimitError as e:
                logger.error(f"Rate limit exceeded for {provider_type.value}: {e}")
            except DataProviderError as e:
                logger.error(f"Provider error for {provider_type.value}: {e}")

    except ImportError as e:
        logger.error(f"Provider {provider_type.value} is not available: {e}")
    except Exception as e:
        logger.error(f"Unexpected error with {provider_type.value}: {e}")


async def demonstrate_provider_switching():
    """Demonstrate switching between different providers."""
    logger.info("\n=== Demonstrating Provider Switching ===")

    # List of providers to try
    providers_to_try = [
        DataProvider.FINANCIAL_MODELING_PREP,
        DataProvider.POLYGON,
        DataProvider.TIINGO,
    ]

    symbol = "AAPL"

    for provider_type in providers_to_try:
        if DataProviderFactory.is_provider_available(provider_type):
            await demonstrate_provider(provider_type, symbol)
        else:
            logger.warning(f"Provider {provider_type.value} is not available")


async def demonstrate_fallback_logic():
    """Demonstrate fallback logic when primary provider fails."""
    logger.info("\n=== Demonstrating Fallback Logic ===")

    primary_provider = DataProvider.FINANCIAL_MODELING_PREP
    fallback_providers = [DataProvider.POLYGON, DataProvider.TIINGO]

    # Try primary provider first
    logger.info(f"Trying primary provider: {primary_provider.value}")

    try:
        provider = DataProviderFactory.create_provider(primary_provider)
        async with provider as _:
            # This would normally work, but for demo purposes we'll simulate failure
            logger.info("Primary provider would be used here")

    except Exception as e:
        logger.warning(f"Primary provider failed: {e}")

        # Try fallback providers
        for fallback_provider in fallback_providers:
            logger.info(f"Trying fallback provider: {fallback_provider.value}")

            if DataProviderFactory.is_provider_available(fallback_provider):
                try:
                    provider = DataProviderFactory.create_provider(fallback_provider)
                    async with provider as _:
                        logger.info(
                            f"Successfully connected to fallback: {fallback_provider.value}"
                        )
                        break
                except Exception as fallback_error:
                    logger.warning(
                        f"Fallback provider {fallback_provider.value} also failed: {fallback_error}"
                    )
                    continue
            else:
                logger.warning(
                    f"Fallback provider {fallback_provider.value} is not available"
                )


async def main():
    """Main demonstration function."""
    logger.info("=== Trading Data Provider Architecture Demo ===")

    # Show available providers
    available_providers = DataProviderFactory.get_available_providers()
    logger.info(f"Available providers: {[p.value for p in available_providers]}")

    # Demonstrate each provider
    await demonstrate_provider_switching()

    # Demonstrate fallback logic
    await demonstrate_fallback_logic()

    logger.info("\n=== Demo Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
