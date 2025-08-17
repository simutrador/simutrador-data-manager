"""
Timeframe utility functions for data resampling operations.

This module provides utilities for:
- Converting timeframe strings to pandas frequency strings
- Validating timeframe conversion rules
- Calculating timeframe hierarchies

Asset-Type-Aware Resampling Strategy:
------------------------------------
The resampling system automatically detects asset types and applies appropriate
alignment strategies to match how Polygon aggregates data for each asset class.

Asset Type Classification & Resampling:

1. US EQUITY (AAPL, MSFT, etc.):
   - Market Hours: 09:30-16:00 ET (13:30-20:00 UTC)
   - Resampling: offset='13h30min' (market session aligned)
   - Candle Times: 13:30, 13:35, 13:40, ..., 19:55 UTC
   - Rationale: Matches US market session boundaries

2. CRYPTO (BTC-USD, ETH-USDT, etc.):
   - Market Hours: 24/7 continuous trading
   - Resampling: No offset (standard UTC alignment)
   - Candle Times: 00:00, 00:05, 00:10, ..., 23:55 UTC
   - Rationale: Matches Polygon's UTC-based crypto aggregation

3. FOREX (EURUSD, GBP/USD, etc.):
   - Market Hours: 24/5 global sessions
   - Resampling: offset='8h00min' (London session aligned)
   - Candle Times: 08:00, 08:05, 08:10, ..., UTC
   - Rationale: Aligns with major forex trading session

4. COMMODITY & OTHERS:
   - Resampling: No offset (standard UTC alignment)
   - Rationale: Varies by exchange, default to UTC

This prevents misaligned candles and ensures resampled data matches
Polygon's native aggregation patterns for each asset type.
"""

from typing import Dict, Optional


def get_timeframe_minutes(timeframe: str) -> Optional[int]:
    """
    Get the duration of a timeframe in minutes.

    Args:
        timeframe: Timeframe string (e.g., '1min', '5min', '1h', 'daily')

    Returns:
        Duration in minutes, or None if timeframe is not supported
    """
    timeframe_minutes = {
        "1min": 1,
        "5min": 5,
        "15min": 15,
        "30min": 30,
        "1h": 60,
        "2h": 120,
        "4h": 240,
        "daily": 1440,  # 24 hours
    }
    return timeframe_minutes.get(timeframe)


def get_pandas_frequency(timeframe: str) -> Optional[str]:
    """
    Convert timeframe string to pandas frequency string for resampling.

    Args:
        timeframe: Timeframe string (e.g., '1min', '5min', '1h', 'daily')

    Returns:
        Pandas frequency string, or None if timeframe is not supported
    """
    frequency_map = {
        "1min": "1min",  # Updated from deprecated 'T'
        "5min": "5min",
        "15min": "15min",
        "30min": "30min",
        "1h": "1h",  # Updated from deprecated 'H'
        "2h": "2h",
        "4h": "4h",
        "daily": "1D",  # D = day in pandas
    }
    return frequency_map.get(timeframe)


def validate_timeframe_conversion(from_timeframe: str, to_timeframe: str) -> bool:
    """
    Validate that a timeframe conversion is valid (target must be longer than source).

    Args:
        from_timeframe: Source timeframe
        to_timeframe: Target timeframe

    Returns:
        True if conversion is valid, False otherwise
    """
    from_minutes = get_timeframe_minutes(from_timeframe)
    to_minutes = get_timeframe_minutes(to_timeframe)

    if from_minutes is None or to_minutes is None:
        return False

    return to_minutes > from_minutes


def get_supported_timeframes() -> Dict[str, int]:
    """
    Get all supported timeframes with their durations in minutes.

    Returns:
        Dictionary mapping timeframe strings to minutes
    """
    return {
        "1min": 1,
        "5min": 5,
        "15min": 15,
        "30min": 30,
        "1h": 60,
        "2h": 120,
        "4h": 240,
        "daily": 1440,
    }


def get_resampling_rules() -> Dict[str, str]:
    """
    Get the pandas aggregation rules for OHLCV resampling.

    Returns:
        Dictionary mapping OHLCV fields to aggregation functions
    """
    return {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
