"""
Tests for the stock market nightly update service.

This module tests the stock market nightly update service including:
- Date range calculation logic
- Gap prevention by re-downloading last trading date
- Integration with storage and validation services
"""

from datetime import date, datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from services.workflows.stock_market_nightly_update_service import (
    StockMarketNightlyUpdateService,
)


class TestStockMarketNightlyUpdateService:
    """Test cases for stock market nightly update service."""

    @pytest.fixture
    def mock_storage_service(self) -> Mock:
        """Create a mock storage service."""
        return Mock()

    @pytest.fixture
    def mock_validation_service(self) -> Mock:
        """Create a mock validation service."""
        return Mock()

    @pytest.fixture
    def mock_trading_data_service(self) -> Mock:
        """Create a mock trading data updating service."""
        return Mock()

    @pytest.fixture
    def nightly_service(
        self,
        mock_storage_service: Mock,
        mock_validation_service: Mock,
        mock_trading_data_service: Mock,
    ) -> StockMarketNightlyUpdateService:
        """Create a nightly update service with mocked dependencies."""
        service = StockMarketNightlyUpdateService()
        # Replace the internal services with mocks
        service.storage_service = mock_storage_service
        service.validation_service = mock_validation_service
        service.updating_service = mock_trading_data_service
        return service

    def test_get_update_date_range_with_existing_data(
        self, nightly_service: StockMarketNightlyUpdateService
    ) -> None:
        """Test date range calculation when data exists - should start from last update date."""
        # Mock existing data from 5 days ago
        last_update = datetime.now() - timedelta(days=5)
        nightly_service.storage_service.get_last_update_date.return_value = last_update  # type: ignore

        start_date, end_date = nightly_service.get_update_date_range("AAPL")

        # Should start from the last update date to prevent gaps
        expected_start = last_update.date()
        assert start_date == expected_start

        # End date should be yesterday
        expected_end = date.today() - timedelta(days=1)
        assert end_date == expected_end

    def test_get_update_date_range_no_existing_data(
        self, nightly_service: StockMarketNightlyUpdateService
    ) -> None:
        """Test date range calculation when no data exists - should use default lookback."""
        # Mock no existing data
        nightly_service.storage_service.get_last_update_date.return_value = None  # type: ignore

        with patch(
            "services.workflows.stock_market_nightly_update_service.get_settings"
        ) as mock_get_settings:
            mock_settings = Mock()
            mock_settings.trading_data_update.default_lookback_days = 30
            mock_get_settings.return_value = mock_settings

            start_date, end_date = nightly_service.get_update_date_range("AAPL")

            # Should use default lookback period
            expected_start = date.today() - timedelta(days=30)
            assert start_date == expected_start

            # End date should be yesterday
            expected_end = date.today() - timedelta(days=1)
            assert end_date == expected_end

    def test_gap_prevention_redownload_last_date(
        self, nightly_service: StockMarketNightlyUpdateService
    ) -> None:
        """
        Test that the system redownloads the last trading date to prevent gaps
        from partial downloads or system crashes.
        """
        # Mock existing data with last update at 2:00 PM on 2025-01-15
        # This simulates a partial trading day (missing 2:00 PM - 4:00 PM)
        last_update = datetime(2025, 1, 15, 14, 0, 0)  # 2:00 PM
        nightly_service.storage_service.get_last_update_date.return_value = last_update  # type: ignore

        start_date, _ = nightly_service.get_update_date_range("AAPL")

        # Should start from the same date (2025-01-15) to fill gaps
        expected_start = date(2025, 1, 15)
        assert start_date == expected_start

        # This ensures we redownload the entire trading day, not skip to next day
        next_day = date(2025, 1, 16)
        assert start_date != next_day

    def test_get_update_date_range_with_trading_day_filtering(
        self, nightly_service: StockMarketNightlyUpdateService
    ) -> None:
        """Test that non-trading days are filtered out from the date range."""
        # Mock existing data from a Friday
        last_update = datetime(2025, 1, 17, 16, 0, 0)  # Friday 4:00 PM
        nightly_service.storage_service.get_last_update_date.return_value = last_update  # type: ignore

        # Mock validation service to simulate weekend filtering
        def mock_is_trading_day(check_date: date) -> bool:
            # Friday (17th) is trading day, weekend (18th, 19th) are not
            return check_date.weekday() < 5

        nightly_service.validation_service.is_trading_day.side_effect = (  # type: ignore
            mock_is_trading_day
        )

        start_date, _ = nightly_service.get_update_date_range("AAPL")

        # Should start from Friday (last trading day)
        expected_start = date(2025, 1, 17)
        assert start_date == expected_start

        # End date should be adjusted to the last trading day before today
        # This depends on what "today" is in the test, but the logic should filter non-trading days
        assert nightly_service.validation_service.is_trading_day.called  # type: ignore
