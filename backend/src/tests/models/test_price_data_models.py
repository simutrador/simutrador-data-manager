"""
Tests for price data models.
"""

import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.price_data import (
    DataUpdateStatus,
    PriceCandle,
    PriceDataSeries,
    PriceQuote,
    Timeframe,
)


class TestPriceCandle:
    """Test cases for PriceCandle model."""

    def test_valid_price_candle(self):
        """Test creating a valid price candle."""
        candle = PriceCandle(
            date=datetime(2025, 7, 3, 12, 59, 0),
            open=Decimal("213.57"),
            high=Decimal("213.69"),
            low=Decimal("213.32"),
            close=Decimal("213.58"),
            volume=Decimal("670856"),
        )

        assert candle.date == datetime(2025, 7, 3, 12, 59, 0)
        assert candle.open == Decimal("213.57")
        assert candle.high == Decimal("213.69")
        assert candle.low == Decimal("213.32")
        assert candle.close == Decimal("213.58")
        assert candle.volume == 670856

    def test_price_candle_validation_high_low(self):
        """Test that high must be >= low."""
        with pytest.raises(
            ValueError, match="High price must be greater than or equal to low price"
        ):
            PriceCandle(
                date=datetime(2025, 7, 3, 12, 59, 0),
                open=Decimal("213.57"),
                high=Decimal("213.30"),  # High < Low
                low=Decimal("213.32"),
                close=Decimal("213.58"),
                volume=Decimal("670856"),
            )

    def test_price_candle_validation_open_range(self):
        """Test that open must be between low and high."""
        with pytest.raises(
            ValueError, match="Open price must be between low and high prices"
        ):
            PriceCandle(
                date=datetime(2025, 7, 3, 12, 59, 0),
                open=Decimal("214.00"),  # Open > High
                high=Decimal("213.69"),
                low=Decimal("213.32"),
                close=Decimal("213.58"),
                volume=Decimal("670856"),
            )

    def test_price_candle_validation_close_range(self):
        """Test that close must be between low and high."""
        with pytest.raises(
            ValueError, match="Close price must be between low and high prices"
        ):
            PriceCandle(
                date=datetime(2025, 7, 3, 12, 59, 0),
                open=Decimal("213.57"),
                high=Decimal("213.69"),
                low=Decimal("213.32"),
                close=Decimal("213.20"),  # Close < Low
                volume=Decimal("670856"),
            )

    def test_price_candle_negative_prices(self):
        """Test that prices must be positive."""
        with pytest.raises(ValidationError):
            PriceCandle(
                date=datetime(2025, 7, 3, 12, 59, 0),
                open=Decimal("-213.57"),  # Negative price
                high=Decimal("213.69"),
                low=Decimal("213.32"),
                close=Decimal("213.58"),
                volume=Decimal("670856"),
            )

    def test_price_candle_negative_volume(self):
        """Test that volume must be non-negative."""
        with pytest.raises(ValidationError):
            PriceCandle(
                date=datetime(2025, 7, 3, 12, 59, 0),
                open=Decimal("213.57"),
                high=Decimal("213.69"),
                low=Decimal("213.32"),
                close=Decimal("213.58"),
                volume=Decimal("-1000"),  # Negative volume
            )


class TestPriceDataSeries:
    """Test cases for PriceDataSeries model."""

    def test_valid_price_data_series(self):
        """Test creating a valid price data series."""
        candles = [
            PriceCandle(
                date=datetime(2025, 7, 3, 12, 59, 0),
                open=Decimal("213.57"),
                high=Decimal("213.69"),
                low=Decimal("213.32"),
                close=Decimal("213.58"),
                volume=Decimal("670856"),
            ),
            PriceCandle(
                date=datetime(2025, 7, 3, 13, 0, 0),
                open=Decimal("213.58"),
                high=Decimal("213.75"),
                low=Decimal("213.45"),
                close=Decimal("213.62"),
                volume=Decimal("450123"),
            ),
        ]

        series = PriceDataSeries(
            symbol="AAPL", timeframe=Timeframe.ONE_MIN, candles=candles
        )

        assert series.symbol == "AAPL"
        assert series.timeframe == Timeframe.ONE_MIN
        assert len(series.candles) == 2
        assert series.start_date == datetime(2025, 7, 3, 12, 59, 0)
        assert series.end_date == datetime(2025, 7, 3, 13, 0, 0)

    def test_empty_symbol_validation(self):
        """Test that symbol cannot be empty."""
        with pytest.raises(ValidationError):
            PriceDataSeries(
                symbol="", timeframe=Timeframe.ONE_MIN, candles=[]
            )  # Empty symbol

    def test_empty_candles_series(self):
        """Test series with no candles."""
        series = PriceDataSeries(symbol="AAPL", timeframe=Timeframe.ONE_MIN, candles=[])

        assert series.symbol == "AAPL"
        assert series.timeframe == Timeframe.ONE_MIN
        assert len(series.candles) == 0
        assert series.start_date is None
        assert series.end_date is None

    def test_manual_date_range(self):
        """Test series with manually set date range."""
        candles = [
            PriceCandle(
                date=datetime(2025, 7, 3, 12, 59, 0),
                open=Decimal("213.57"),
                high=Decimal("213.69"),
                low=Decimal("213.32"),
                close=Decimal("213.58"),
                volume=Decimal("670856"),
            )
        ]

        series = PriceDataSeries(
            symbol="AAPL",
            timeframe=Timeframe.ONE_MIN,
            candles=candles,
            start_date=datetime(2025, 7, 1, 0, 0, 0),
            end_date=datetime(2025, 7, 5, 0, 0, 0),
        )

        # Manual dates should be preserved
        assert series.start_date == datetime(2025, 7, 1, 0, 0, 0)
        assert series.end_date == datetime(2025, 7, 5, 0, 0, 0)


class TestPriceQuote:
    """Test cases for PriceQuote model."""

    def test_valid_price_quote(self):
        """Test creating a valid price quote."""
        quote = PriceQuote(
            symbol="AAPL",
            price=Decimal("213.58"),
            timestamp=datetime(2025, 7, 3, 16, 0, 0),
            bid=Decimal("213.57"),
            ask=Decimal("213.59"),
            volume=Decimal("1000000"),
        )

        assert quote.symbol == "AAPL"
        assert quote.price == Decimal("213.58")
        assert quote.timestamp == datetime(2025, 7, 3, 16, 0, 0)
        assert quote.bid == Decimal("213.57")
        assert quote.ask == Decimal("213.59")
        assert quote.volume == 1000000

    def test_minimal_price_quote(self):
        """Test price quote with only required fields."""
        quote = PriceQuote(
            symbol="AAPL",
            price=Decimal("213.58"),
            timestamp=datetime(2025, 7, 3, 16, 0, 0),
            bid=None,
            ask=None,
            volume=None,
        )

        assert quote.symbol == "AAPL"
        assert quote.price == Decimal("213.58")
        assert quote.timestamp == datetime(2025, 7, 3, 16, 0, 0)
        assert quote.bid is None
        assert quote.ask is None
        assert quote.volume is None

    def test_price_quote_validation(self):
        """Test price quote validation."""
        with pytest.raises(ValidationError):
            PriceQuote(
                symbol="",  # Empty symbol
                price=Decimal("213.58"),
                timestamp=datetime(2025, 7, 3, 16, 0, 0),
                bid=None,
                ask=None,
                volume=None,
            )

        with pytest.raises(ValidationError):
            PriceQuote(
                symbol="AAPL",
                price=Decimal("-213.58"),  # Negative price
                timestamp=datetime(2025, 7, 3, 16, 0, 0),
                bid=None,
                ask=None,
                volume=None,
            )


class TestDataUpdateStatus:
    """Test cases for DataUpdateStatus model."""

    def test_successful_update_status(self):
        """Test creating a successful update status."""
        status = DataUpdateStatus(
            symbol="AAPL",
            timeframe="1min",
            last_update=datetime(2025, 7, 3, 16, 0, 0),
            records_updated=1000,
            success=True,
            error_message=None,
        )

        assert status.symbol == "AAPL"
        assert status.timeframe == "1min"
        assert status.last_update == datetime(2025, 7, 3, 16, 0, 0)
        assert status.records_updated == 1000
        assert status.success is True
        assert status.error_message is None

    def test_failed_update_status(self):
        """Test creating a failed update status."""
        status = DataUpdateStatus(
            symbol="AAPL",
            timeframe="1min",
            last_update=datetime(2025, 7, 3, 16, 0, 0),
            records_updated=0,
            success=False,
            error_message="API rate limit exceeded",
        )

        assert status.symbol == "AAPL"
        assert status.timeframe == "1min"
        assert status.last_update == datetime(2025, 7, 3, 16, 0, 0)
        assert status.records_updated == 0
        assert status.success is False
        assert status.error_message == "API rate limit exceeded"

    def test_negative_records_validation(self):
        """Test that records_updated cannot be negative."""
        with pytest.raises(ValidationError):
            DataUpdateStatus(
                symbol="AAPL",
                timeframe="1min",
                last_update=datetime(2025, 7, 3, 16, 0, 0),
                records_updated=-10,  # Negative records
                success=True,
                error_message=None,
            )
