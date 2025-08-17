"""
Tests for the stock market validation service.

This module tests the validation of stock market data including:
- Market hours validation
- Trading day detection
- Data completeness checks
- Volume and price data integrity
"""

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List
from unittest.mock import Mock, patch

import pytest
from simutrador_core.models.price_data import PriceCandle, PriceDataSeries, Timeframe

from services.validation.stock_market_validation_service import (
    StockMarketValidationService,
    ValidationResult,
)

# Try to import USStockMarketCalendar if available (fallback calendar)
try:
    from services.validation.stock_market_validation_service import (
        USStockMarketCalendar,
    )

    _has_custom_calendar = True
except ImportError:
    _has_custom_calendar = False
    USStockMarketCalendar = None


class TestStockMarketValidationService:
    """Test cases for StockMarketValidationService."""

    @pytest.fixture
    def validation_service(self) -> StockMarketValidationService:
        """Create a validation service instance for testing."""
        service = StockMarketValidationService()
        # Disable market hours check for testing to avoid timezone issues
        service.nightly_settings.enable_market_hours_check = False
        return service

    @pytest.fixture
    def sample_trading_day_candles(self) -> List[PriceCandle]:
        """Create sample 1-minute candles for a full trading day (390 candles)."""
        candles: List[PriceCandle] = []
        base_date = datetime(2025, 1, 15, 13, 30)  # 9:30 AM ET = 13:30 UTC

        for i in range(390):  # Full trading day
            # Add i minutes to the base time
            candle_time = base_date + timedelta(minutes=i)
            candles.append(
                PriceCandle(
                    date=candle_time,
                    open=Decimal("100.00") + Decimal(str(i * 0.01)),
                    high=Decimal("100.50") + Decimal(str(i * 0.01)),
                    low=Decimal("99.50") + Decimal(str(i * 0.01)),
                    close=Decimal("100.25") + Decimal(str(i * 0.01)),
                    volume=Decimal("1000"),
                )
            )

        return candles

    @pytest.fixture
    def incomplete_trading_day_candles(self) -> List[PriceCandle]:
        """Create sample candles for an incomplete trading day (missing some periods)."""
        candles: List[PriceCandle] = []
        base_date = datetime(2025, 1, 15, 13, 30)  # 9:30 AM ET = 13:30 UTC

        # Create only 300 candles instead of 390 (missing 90 minutes)
        for i in range(300):
            # Add i minutes to the base time
            candle_time = base_date + timedelta(minutes=i)
            candles.append(
                PriceCandle(
                    date=candle_time,
                    open=Decimal("100.00"),
                    high=Decimal("100.50"),
                    low=Decimal("99.50"),
                    close=Decimal("100.25"),
                    volume=Decimal("1000"),
                )
            )

        return candles

    def test_is_trading_day_weekday(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test that weekdays are correctly identified as trading days."""
        # Wednesday, January 15, 2025
        test_date = date(2025, 1, 15)
        assert validation_service.is_trading_day(test_date) is True

    def test_is_trading_day_weekend(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test that weekends are correctly identified as non-trading days."""
        # Saturday, January 18, 2025
        saturday = date(2025, 1, 18)
        assert validation_service.is_trading_day(saturday) is False

        # Sunday, January 19, 2025
        sunday = date(2025, 1, 19)
        assert validation_service.is_trading_day(sunday) is False

    def test_is_trading_day_holiday(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test that federal holidays are correctly identified as non-trading days."""
        # New Year's Day 2025 (Wednesday)
        new_years = date(2025, 1, 1)
        assert validation_service.is_trading_day(new_years) is False

        # Independence Day 2025 (Friday)
        july_4th = date(2025, 7, 4)
        assert validation_service.is_trading_day(july_4th) is False

    def test_get_expected_candle_count_full_day(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test expected candle count for a full trading day."""
        trading_day = date(2025, 1, 15)  # Wednesday
        expected = validation_service.get_expected_candle_count(trading_day)
        assert expected == 390  # 6.5 hours * 60 minutes

    def test_get_expected_candle_count_non_trading_day(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test expected candle count for non-trading days."""
        weekend = date(2025, 1, 18)  # Saturday
        expected = validation_service.get_expected_candle_count(weekend)
        assert expected == 0

    def test_get_expected_candle_count_half_day(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test expected candle count for half trading days."""
        # Black Friday 2025 (day after Thanksgiving - November 28th is the half day)
        half_day = date(2025, 11, 28)
        expected = validation_service.get_expected_candle_count(half_day)
        assert expected == 210  # 3.5 hours * 60 minutes

    @patch("services.validation.stock_market_validation_service.DataStorageService")
    def test_validate_trading_day_data_complete(
        self,
        mock_storage_class: Mock,
        validation_service: StockMarketValidationService,
        sample_trading_day_candles: List[PriceCandle],
    ) -> None:
        """Test validation of complete trading day data."""
        # Mock storage service
        mock_storage = Mock()
        mock_storage_class.return_value = mock_storage

        # Create mock series with complete data
        mock_series = PriceDataSeries(
            symbol="AAPL",
            timeframe=Timeframe.ONE_MIN,
            candles=sample_trading_day_candles,
        )
        mock_storage.load_data.return_value = mock_series

        # Re-initialize service to use mocked storage
        validation_service.storage_service = mock_storage

        # Test validation
        result = validation_service.validate_trading_day_data("AAPL", date(2025, 1, 15))

        # Debug: print errors if validation fails
        if not result.is_valid:
            print(f"Validation errors: {result.errors}")
            print(
                f"Expected: {result.expected_candles}, Actual: {result.actual_candles}"
            )

        assert result.is_valid is True
        assert result.symbol == "AAPL"
        assert result.expected_candles == 390
        assert result.actual_candles == 390
        assert len(result.errors) == 0

    @patch("services.validation.stock_market_validation_service.DataStorageService")
    def test_validate_trading_day_data_incomplete(
        self,
        mock_storage_class: Mock,
        validation_service: StockMarketValidationService,
        incomplete_trading_day_candles: List[PriceCandle],
    ) -> None:
        """Test validation of incomplete trading day data."""
        # Mock storage service
        mock_storage = Mock()
        mock_storage_class.return_value = mock_storage

        # Create mock series with incomplete data
        mock_series = PriceDataSeries(
            symbol="AAPL",
            timeframe=Timeframe.ONE_MIN,
            candles=incomplete_trading_day_candles,
        )
        mock_storage.load_data.return_value = mock_series

        # Re-initialize service to use mocked storage
        validation_service.storage_service = mock_storage

        # Test validation
        result = validation_service.validate_trading_day_data("AAPL", date(2025, 1, 15))

        assert result.is_valid is False
        assert result.symbol == "AAPL"
        assert result.expected_candles == 390
        assert result.actual_candles == 300
        assert len(result.errors) > 0
        assert "Missing 90 candles" in result.errors[0]

    @patch("services.validation.stock_market_validation_service.DataStorageService")
    def test_validate_trading_day_data_no_data(
        self, mock_storage_class: Mock, validation_service: StockMarketValidationService
    ) -> None:
        """Test validation when no data exists."""
        # Mock storage service
        mock_storage = Mock()
        mock_storage_class.return_value = mock_storage

        # Create mock series with no data
        mock_series = PriceDataSeries(
            symbol="AAPL", timeframe=Timeframe.ONE_MIN, candles=[]
        )
        mock_storage.load_data.return_value = mock_series

        # Re-initialize service to use mocked storage
        validation_service.storage_service = mock_storage

        # Test validation
        result = validation_service.validate_trading_day_data("AAPL", date(2025, 1, 15))

        assert result.is_valid is False
        assert result.symbol == "AAPL"
        assert result.expected_candles == 390
        assert result.actual_candles == 0
        assert len(result.errors) > 0
        assert "No data found" in result.errors[0]

    def test_validate_data_integrity_valid_candles(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test data integrity validation with valid candles."""
        valid_candles = [
            PriceCandle(
                date=datetime(2025, 1, 15, 13, 30),
                open=Decimal("100.00"),
                high=Decimal("100.50"),
                low=Decimal("99.50"),
                close=Decimal("100.25"),
                volume=Decimal("1000"),
            )
        ]

        errors, warnings = validation_service._validate_data_integrity(valid_candles)  # type: ignore

        assert len(errors) == 0
        assert len(warnings) == 0

    def test_validate_data_integrity_invalid_ohlc(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test data integrity validation with invalid OHLC relationships."""

        # Create a mock object that looks like a PriceCandle but has invalid OHLC relationships
        class MockPriceCandle:
            def __init__(self):
                self.date = datetime(2025, 1, 15, 13, 30)
                self.open = Decimal("100.00")
                self.high = Decimal("99.00")  # High < Open (invalid)
                self.low = Decimal("101.00")  # Low > Open (invalid)
                self.close = Decimal("100.25")
                self.volume = Decimal("1000")

        invalid_candle = MockPriceCandle()

        invalid_candles = [invalid_candle]

        errors, _ = validation_service._validate_data_integrity(invalid_candles)  # type: ignore

        assert len(errors) > 0
        assert any("High" in error and "< Open/Close" in error for error in errors)
        assert any("Low" in error and "> Open/Close" in error for error in errors)

    def test_validate_data_integrity_zero_volume(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test data integrity validation with zero volume."""
        zero_volume_candles = [
            PriceCandle(
                date=datetime(2025, 1, 15, 13, 30),
                open=Decimal("100.00"),
                high=Decimal("100.50"),
                low=Decimal("99.50"),
                close=Decimal("100.25"),
                volume=Decimal("0"),  # Zero volume should generate warning
            )
        ]

        errors, warnings = validation_service._validate_data_integrity(zero_volume_candles)  # type: ignore

        assert len(errors) == 0
        assert len(warnings) > 0
        assert any("Zero volume" in warning for warning in warnings)

    def test_validate_multiple_symbols(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test validation of multiple symbols."""
        with patch.object(
            validation_service, "validate_trading_day_data"
        ) as mock_validate:
            # Mock validation results
            mock_validate.side_effect = [
                ValidationResult("AAPL", date(2025, 1, 15), True, 390, 390),
                ValidationResult(
                    "MSFT", date(2025, 1, 15), False, 390, 300, errors=["Missing data"]
                ),
            ]

            results = validation_service.validate_multiple_symbols(
                ["AAPL", "MSFT"], date(2025, 1, 15)
            )

            assert len(results) == 2
            assert results["AAPL"].is_valid is True
            assert results["MSFT"].is_valid is False
            assert len(results["MSFT"].errors) > 0

    def test_find_symbols_needing_update(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test finding symbols that need updates."""
        with patch.object(
            validation_service, "validate_trading_day_data"
        ) as mock_validate:
            # Mock validation results - AAPL is valid, MSFT needs update
            mock_validate.side_effect = [
                ValidationResult("AAPL", date(2025, 1, 15), True, 390, 390),
                ValidationResult("MSFT", date(2025, 1, 15), False, 390, 0),
            ]

            symbols_needing_update = validation_service.find_symbols_needing_update(
                ["AAPL", "MSFT"], date(2025, 1, 15)
            )

            assert "AAPL" not in symbols_needing_update
            assert "MSFT" in symbols_needing_update

    @pytest.mark.skipif(
        not _has_custom_calendar,
        reason="Custom calendar not available when using pandas_market_calendars",
    )
    def test_stock_market_calendar_good_friday(self) -> None:
        """Test that Good Friday is correctly identified as a market holiday."""
        assert USStockMarketCalendar is not None
        calendar = USStockMarketCalendar()

        # Good Friday 2025 is April 18th
        good_friday_2025 = datetime(2025, 4, 18)
        holidays = calendar.holidays(start=good_friday_2025, end=good_friday_2025)

        assert len(holidays) == 1
        # The holiday timestamp should match the expected date
        assert holidays[0].date() == good_friday_2025.date()

    @pytest.mark.skipif(
        not _has_custom_calendar,
        reason="Custom calendar not available when using pandas_market_calendars",
    )
    def test_stock_market_calendar_excludes_columbus_day(self) -> None:
        """Test that Columbus Day is NOT a market holiday."""
        assert USStockMarketCalendar is not None
        calendar = USStockMarketCalendar()

        # Columbus Day 2025 is October 13th (2nd Monday in October)
        columbus_day_2025 = datetime(2025, 10, 13)
        holidays = calendar.holidays(start=columbus_day_2025, end=columbus_day_2025)

        # Should be empty - markets are open on Columbus Day
        assert len(holidays) == 0

    @pytest.mark.skipif(
        not _has_custom_calendar,
        reason="Custom calendar not available when using pandas_market_calendars",
    )
    def test_stock_market_calendar_excludes_veterans_day(self) -> None:
        """Test that Veterans Day is NOT a market holiday."""
        assert USStockMarketCalendar is not None
        calendar = USStockMarketCalendar()

        # Veterans Day 2025 is November 11th
        veterans_day_2025 = datetime(2025, 11, 11)
        holidays = calendar.holidays(start=veterans_day_2025, end=veterans_day_2025)

        # Should be empty - markets are open on Veterans Day
        assert len(holidays) == 0

    @pytest.mark.skipif(
        not _has_custom_calendar,
        reason="Custom calendar not available when using pandas_market_calendars",
    )
    def test_stock_market_calendar_juneteenth_2022_onwards(self) -> None:
        """Test that Juneteenth is observed starting 2022."""
        assert USStockMarketCalendar is not None
        calendar = USStockMarketCalendar()

        # Juneteenth 2021 - should NOT be observed by stock markets
        juneteenth_2021 = datetime(2021, 6, 19)
        holidays_2021 = calendar.holidays(start=juneteenth_2021, end=juneteenth_2021)
        assert len(holidays_2021) == 0

        # Juneteenth 2022 fell on Sunday, so it was observed on Monday June 20th
        juneteenth_observed_2022 = datetime(2022, 6, 20)  # Monday observance
        holidays_2022 = calendar.holidays(
            start=juneteenth_observed_2022, end=juneteenth_observed_2022
        )
        assert len(holidays_2022) == 1

        # Test a year when Juneteenth falls on a weekday (2023 was Monday)
        juneteenth_2023 = datetime(2023, 6, 19)
        holidays_2023 = calendar.holidays(start=juneteenth_2023, end=juneteenth_2023)
        assert len(holidays_2023) == 1

    def test_is_half_trading_day_black_friday(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test that Black Friday (day after Thanksgiving) is a half trading day."""
        # Thanksgiving 2025 is November 27th, so Black Friday is November 28th
        black_friday_2025 = date(2025, 11, 28)
        assert validation_service.is_half_trading_day(black_friday_2025) is True

    def test_is_half_trading_day_christmas_eve_weekday(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test that Christmas Eve on a weekday is a half trading day."""
        # Christmas Eve 2025 is December 24th (Wednesday)
        christmas_eve_2025 = date(2025, 12, 24)
        # This should be a half day if it's a trading day
        if validation_service.is_trading_day(christmas_eve_2025):
            assert validation_service.is_half_trading_day(christmas_eve_2025) is True

    def test_is_half_trading_day_july_3rd_when_july_4th_weekday(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test that July 3rd is a half day when July 4th falls on a weekday."""
        # July 4th 2025 is a Friday, so July 3rd (Thursday) should be a half day
        july_3rd_2025 = date(2025, 7, 3)
        if validation_service.is_trading_day(july_3rd_2025):
            assert validation_service.is_half_trading_day(july_3rd_2025) is True

    def test_is_half_trading_day_july_3rd_edge_case_sunday(
        self, validation_service: StockMarketValidationService
    ) -> None:
        """Test edge case: July 3rd is Sunday, July 4th is Monday (observed holiday)."""
        # In 2022, July 3rd was Sunday, July 4th was Monday
        # July 3rd (Sunday) should NOT be a half day because:
        # 1. It's already a non-trading day (Sunday)
        # 2. July 4th (Monday) becomes the observed holiday
        july_3rd_2022 = date(2022, 7, 3)  # Sunday
        july_4th_2022 = date(2022, 7, 4)  # Monday

        # Verify our assumptions
        assert july_3rd_2022.weekday() == 6  # Sunday
        assert july_4th_2022.weekday() == 0  # Monday

        # July 3rd should not be a half day (it's not even a trading day)
        assert validation_service.is_half_trading_day(july_3rd_2022) is False

        # July 3rd should not be a trading day (it's Sunday)
        assert validation_service.is_trading_day(july_3rd_2022) is False

        # July 4th should not be a trading day (it's the observed holiday)
        assert validation_service.is_trading_day(july_4th_2022) is False
