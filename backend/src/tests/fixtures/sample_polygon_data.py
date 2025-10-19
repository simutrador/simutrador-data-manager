"""
Sample Polygon data for testing resampling logic.
This eliminates the need for manual reference data files.
"""

from typing import Any

# Sample 1-minute data for testing (represents 1 hour of trading)
SAMPLE_1MIN_DATA: list[dict[str, Any]] = [
    # 9:30 AM
    {"t": 1642086600000, "o": 100.0, "h": 101.0, "l": 99.5, "c": 100.5, "v": 1000},
    # 9:31 AM
    {"t": 1642086660000, "o": 100.5, "h": 102.0, "l": 100.0, "c": 101.5, "v": 1200},
    # 9:32 AM
    {"t": 1642086720000, "o": 101.5, "h": 103.0, "l": 101.0, "c": 102.0, "v": 800},
    # 9:33 AM
    {"t": 1642086780000, "o": 102.0, "h": 102.5, "l": 101.5, "c": 101.8, "v": 900},
    # 9:34 AM
    {"t": 1642086840000, "o": 101.8, "h": 102.2, "l": 101.0, "c": 101.2, "v": 1100},
    # 9:35 AM (completes first 5-min candle)
    {"t": 1642086900000, "o": 101.2, "h": 101.5, "l": 100.8, "c": 101.0, "v": 950},
    # Second 5-minute period (9:35-9:40)
    {"t": 1642086960000, "o": 101.0, "h": 102.5, "l": 100.5, "c": 102.2, "v": 1300},
    {"t": 1642087020000, "o": 102.2, "h": 103.0, "l": 102.0, "c": 102.8, "v": 1400},
    {"t": 1642087080000, "o": 102.8, "h": 103.5, "l": 102.5, "c": 103.0, "v": 1100},
    {"t": 1642087140000, "o": 103.0, "h": 103.2, "l": 102.7, "c": 102.9, "v": 1000},
    {"t": 1642087200000, "o": 102.9, "h": 103.1, "l": 102.6, "c": 102.7, "v": 900},
    # Third 5-minute period (9:40-9:45)
    {"t": 1642087260000, "o": 102.7, "h": 103.0, "l": 102.0, "c": 102.5, "v": 800},
    {"t": 1642087320000, "o": 102.5, "h": 102.8, "l": 102.1, "c": 102.3, "v": 750},
    {"t": 1642087380000, "o": 102.3, "h": 102.6, "l": 101.9, "c": 102.1, "v": 850},
    {"t": 1642087440000, "o": 102.1, "h": 102.4, "l": 101.8, "c": 102.0, "v": 900},
    {"t": 1642087500000, "o": 102.0, "h": 102.2, "l": 101.7, "c": 101.9, "v": 800},
]

# Expected 5-minute resampled data (calculated from above 1-min data)
EXPECTED_5MIN_DATA: list[dict[str, Any]] = [
    # 9:30-9:35 (first 6 candles)
    {
        "t": 1642086600000,  # 9:30 timestamp
        "o": 100.0,  # open from first candle
        "h": 103.0,  # highest high from 6 candles
        "l": 99.5,  # lowest low from 6 candles
        "c": 101.0,  # close from last candle
        "v": 5950,  # sum of volumes: 1000+1200+800+900+1100+950
    },
    # 9:35-9:40 (next 5 candles)
    {
        "t": 1642086900000,  # 9:35 timestamp
        "o": 101.0,  # open from first candle of period
        "h": 103.5,  # highest high from 5 candles
        "l": 100.5,  # lowest low from 5 candles
        "c": 102.7,  # close from last candle
        "v": 5700,  # sum of volumes: 1300+1400+1100+1000+900
    },
    # 9:40-9:45 (last 5 candles)
    {
        "t": 1642087200000,  # 9:40 timestamp
        "o": 102.7,  # open from first candle of period
        "h": 103.0,  # highest high from 5 candles
        "l": 101.7,  # lowest low from 5 candles
        "c": 101.9,  # close from last candle
        "v": 4100,  # sum of volumes: 800+750+850+900+800
    },
]

# Expected 15-minute resampled data (calculated from 1-min data)
EXPECTED_15MIN_DATA: list[dict[str, Any]] = [
    # 9:30-9:45 (all 16 candles combined)
    {
        "t": 1642086600000,  # 9:30 timestamp
        "o": 100.0,  # open from very first candle
        "h": 103.5,  # highest high from all candles
        "l": 99.5,  # lowest low from all candles
        "c": 101.9,  # close from very last candle
        "v": 15750,  # sum of all volumes
    },
]
