"""
Tests for the DataStorageService.
"""

import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Generator, List
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from models.price_data import PriceCandle, PriceDataSeries, Timeframe
from services.storage.data_storage_service import DataStorageService


class TestDataStorageService:
    """Test cases for DataStorageService."""

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
    def storage_service(self, mock_settings: MagicMock) -> DataStorageService:
        """Create a data storage service with temporary directory."""
        with patch(
            "services.storage.data_storage_service.get_settings",
            return_value=mock_settings,
        ):
            service = DataStorageService()
            return service

    @pytest.fixture
    def sample_candles(self) -> List[PriceCandle]:
        """Create sample price candles for testing."""
        return [
            PriceCandle(
                date=datetime(2025, 7, 1, 9, 30),
                open=Decimal("100.0"),
                high=Decimal("105.0"),
                low=Decimal("99.0"),
                close=Decimal("103.0"),
                volume=Decimal("1000"),
            ),
            PriceCandle(
                date=datetime(2025, 7, 1, 9, 31),
                open=Decimal("103.0"),
                high=Decimal("106.0"),
                low=Decimal("102.0"),
                close=Decimal("105.0"),
                volume=Decimal("1200"),
            ),
        ]

    @pytest.fixture
    def sample_series(self, sample_candles: List[PriceCandle]) -> PriceDataSeries:
        """Create a sample price data series."""
        return PriceDataSeries(
            symbol="AAPL", timeframe=Timeframe.ONE_MIN, candles=sample_candles
        )

    def test_service_initialization(self, storage_service: DataStorageService):
        """Test that the service initializes correctly."""
        assert storage_service is not None
        assert storage_service.base_path.exists()
        assert storage_service.candles_path.exists()

    def test_store_empty_data(self, storage_service: DataStorageService):
        """Test storing empty data series."""
        empty_series = PriceDataSeries(
            symbol="AAPL", timeframe=Timeframe.ONE_MIN, candles=[]
        )

        # Should not raise an exception
        storage_service.store_data(empty_series)

    def test_store_and_load_intraday_data(
        self, storage_service: DataStorageService, sample_series: PriceDataSeries
    ):
        """Test storing and loading intraday data."""
        # Store the data
        storage_service.store_data(sample_series)

        # Load the data back in ascending order (oldest first)
        loaded_series = storage_service.load_data(
            "AAPL", Timeframe.ONE_MIN.value, order_by="asc"
        )

        assert loaded_series.symbol == "AAPL"
        assert loaded_series.timeframe == Timeframe.ONE_MIN
        assert len(loaded_series.candles) == 2

        # Check first candle (oldest)
        first_candle = loaded_series.candles[0]
        assert first_candle.open == Decimal("100.0")
        assert first_candle.close == Decimal("103.0")

    def test_store_and_load_daily_data(self, storage_service: DataStorageService):
        """Test storing and loading daily data."""
        daily_candles = [
            PriceCandle(
                date=datetime(2025, 7, 1),
                open=Decimal("100.0"),
                high=Decimal("110.0"),
                low=Decimal("95.0"),
                close=Decimal("108.0"),
                volume=Decimal("50000"),
            )
        ]

        daily_series = PriceDataSeries(
            symbol="AAPL", timeframe=Timeframe.DAILY, candles=daily_candles
        )

        # Store the data
        storage_service.store_data(daily_series)

        # Load the data back
        loaded_series = storage_service.load_data("AAPL", Timeframe.DAILY.value)

        assert loaded_series.symbol == "AAPL"
        assert loaded_series.timeframe == Timeframe.DAILY
        assert len(loaded_series.candles) == 1
        assert loaded_series.candles[0].close == Decimal("108.0")

    def test_get_last_update_date(
        self, storage_service: DataStorageService, sample_series: PriceDataSeries
    ):
        """Test getting the last update date."""
        # Initially should be None
        last_date = storage_service.get_last_update_date(
            "AAPL", Timeframe.ONE_MIN.value
        )
        assert last_date is None

        # Store data and check again
        storage_service.store_data(sample_series)
        last_date = storage_service.get_last_update_date(
            "AAPL", Timeframe.ONE_MIN.value
        )

        assert last_date is not None
        # Convert to naive datetime for comparison (pandas returns timezone-aware)
        from pandas import Timestamp

        if isinstance(last_date, Timestamp):
            # Convert pandas Timestamp to naive datetime
            last_date_naive: datetime = datetime(
                last_date.year,
                last_date.month,
                last_date.day,
                last_date.hour,
                last_date.minute,
                last_date.second,
            )
        else:
            last_date_naive = (
                last_date.replace(tzinfo=None) if last_date.tzinfo else last_date
            )
        assert last_date_naive == datetime(2025, 7, 1, 9, 31)  # Latest candle

    def test_get_last_update_date_performance(
        self, storage_service: DataStorageService
    ):
        """Test that get_last_update_date is performant with larger datasets."""
        import time
        from datetime import timedelta

        # Create 10 days of 1-minute data (3,900 candles)
        base_date = datetime(2024, 1, 1, 9, 30)

        for day in range(10):
            current_date = base_date + timedelta(days=day)
            candles: List[PriceCandle] = []

            # Create 390 candles per day (6.5 hours * 60 minutes)
            for minute in range(390):
                candle_time = current_date + timedelta(minutes=minute)
                base_price = Decimal("150.0") + (Decimal(str(minute)) * Decimal("0.01"))

                candle = PriceCandle(
                    date=candle_time,
                    open=base_price,
                    high=base_price + Decimal("0.50"),
                    low=base_price - Decimal("0.50"),
                    close=base_price + Decimal("0.25"),
                    volume=Decimal(str(1000 + minute)),
                )
                candles.append(candle)  # type: ignore[reportUnknownMemberType]

            # Store each day's data
            series = PriceDataSeries(
                symbol="PERF_TEST", timeframe=Timeframe.ONE_MIN, candles=candles  # type: ignore[reportUnknownArgumentType]
            )
            storage_service.store_data(series)

        # Measure performance of get_last_update_date
        start_time = time.perf_counter()
        last_date = storage_service.get_last_update_date(
            "PERF_TEST", Timeframe.ONE_MIN.value
        )
        end_time = time.perf_counter()

        elapsed_ms = (end_time - start_time) * 1000

        # Should be very fast (under 100ms for 3,900 candles)
        assert (
            elapsed_ms < 100
        ), f"get_last_update_date took {elapsed_ms:.2f}ms, expected < 100ms"
        assert last_date is not None

        # Should return the latest date from the last day
        expected_latest = base_date + timedelta(days=9, minutes=389)
        # Convert to naive datetime for comparison (pandas returns timezone-aware)
        from pandas import Timestamp

        if isinstance(last_date, Timestamp):
            # Convert pandas Timestamp to naive datetime
            last_date_naive: datetime = datetime(
                last_date.year,
                last_date.month,
                last_date.day,
                last_date.hour,
                last_date.minute,
                last_date.second,
            )
        else:
            last_date_naive = (
                last_date.replace(tzinfo=None) if last_date.tzinfo else last_date
            )
        assert last_date_naive == expected_latest

        print(f"✅ Performance test passed: {elapsed_ms:.2f}ms for 3,900 candles")

    def test_list_stored_symbols(
        self, storage_service: DataStorageService, sample_series: PriceDataSeries
    ):
        """Test listing stored symbols."""
        # Initially should be empty
        symbols = storage_service.list_stored_symbols(Timeframe.ONE_MIN.value)
        assert symbols == []

        # Store data and check again
        storage_service.store_data(sample_series)
        symbols = storage_service.list_stored_symbols(Timeframe.ONE_MIN.value)

        assert "AAPL" in symbols

    def test_data_deduplication(
        self, storage_service: DataStorageService, sample_candles: List[PriceCandle]
    ):
        """Test that duplicate data is properly handled."""
        # Create series with duplicate candles
        duplicate_candles = sample_candles + [sample_candles[0]]  # Add duplicate

        series = PriceDataSeries(
            symbol="AAPL", timeframe=Timeframe.ONE_MIN, candles=duplicate_candles
        )

        storage_service.store_data(series)
        loaded_series = storage_service.load_data("AAPL", Timeframe.ONE_MIN.value)

        # Should have only 2 unique candles, not 3
        assert len(loaded_series.candles) == 2

    def test_load_nonexistent_data(self, storage_service: DataStorageService):
        """Test loading data that doesn't exist."""
        loaded_series = storage_service.load_data(
            "NONEXISTENT", Timeframe.ONE_MIN.value
        )

        assert loaded_series.symbol == "NONEXISTENT"
        assert loaded_series.timeframe == Timeframe.ONE_MIN
        assert len(loaded_series.candles) == 0

    def test_pagination_with_limit_and_offset(
        self, storage_service: DataStorageService
    ):
        """Test pagination functionality with limit and offset parameters."""
        # Create test data with multiple days
        base_date = datetime(2025, 7, 1, 9, 30, tzinfo=timezone.utc)
        all_candles: List[PriceCandle] = []

        # Create 100 candles across 2 days (50 each day)
        for day in range(2):
            for minute in range(50):
                candle_time = base_date + timedelta(days=day, minutes=minute)
                candle = PriceCandle(
                    date=candle_time,
                    open=Decimal(f"{100 + day + minute * 0.1:.1f}"),
                    high=Decimal(f"{101 + day + minute * 0.1:.1f}"),
                    low=Decimal(f"{99 + day + minute * 0.1:.1f}"),
                    close=Decimal(f"{100.5 + day + minute * 0.1:.1f}"),
                    volume=Decimal(str(1000 + minute)),
                )
                all_candles.append(candle)

        # Store the data
        series = PriceDataSeries(
            symbol="PAGINATE_TEST", timeframe=Timeframe.ONE_MIN, candles=all_candles
        )
        storage_service.store_data(series)

        # Test pagination: Get first 10 records
        page1 = storage_service.load_data(
            "PAGINATE_TEST", Timeframe.ONE_MIN.value, limit=10, offset=0, order_by="asc"
        )
        assert len(page1.candles) == 10
        assert page1.candles[0].open == Decimal("100.0")  # First record

        # Test pagination: Get next 10 records
        page2 = storage_service.load_data(
            "PAGINATE_TEST",
            Timeframe.ONE_MIN.value,
            limit=10,
            offset=10,
            order_by="asc",
        )
        assert len(page2.candles) == 10
        assert page2.candles[0].open == Decimal("101.0")  # 11th record

        # Test pagination: Get last few records
        last_page = storage_service.load_data(
            "PAGINATE_TEST",
            Timeframe.ONE_MIN.value,
            limit=10,
            offset=90,
            order_by="asc",
        )
        assert len(last_page.candles) == 10  # Should get exactly 10 (records 91-100)

        # Test pagination beyond available data
        beyond_page = storage_service.load_data(
            "PAGINATE_TEST",
            Timeframe.ONE_MIN.value,
            limit=10,
            offset=100,
            order_by="asc",
        )
        assert len(beyond_page.candles) == 0  # No more data

    def test_pagination_with_descending_order(
        self, storage_service: DataStorageService
    ):
        """Test pagination with descending order (newest first)."""
        # Create test data
        base_date = datetime(2025, 7, 1, 9, 30, tzinfo=timezone.utc)
        candles: List[PriceCandle] = []

        for minute in range(20):
            candle_time = base_date + timedelta(minutes=minute)
            candle = PriceCandle(
                date=candle_time,
                open=Decimal(f"{100 + minute:.0f}"),
                high=Decimal(f"{101 + minute:.0f}"),
                low=Decimal(f"{99 + minute:.0f}"),
                close=Decimal(f"{100 + minute:.0f}"),
                volume=Decimal(str(1000 + minute)),
            )
            candles.append(candle)

        series = PriceDataSeries(
            symbol="DESC_TEST", timeframe=Timeframe.ONE_MIN, candles=candles
        )
        storage_service.store_data(series)

        # Get first 5 records in descending order (newest first)
        desc_page = storage_service.load_data(
            "DESC_TEST", Timeframe.ONE_MIN.value, limit=5, offset=0, order_by="desc"
        )

        assert len(desc_page.candles) == 5
        # First record should be the newest (highest open price)
        assert desc_page.candles[0].open == Decimal("119")  # Last candle (minute 19)
        assert desc_page.candles[4].open == Decimal("115")  # 5th newest (minute 15)

    def test_get_total_count_method(self, storage_service: DataStorageService):
        """Test the get_total_count method for efficient counting."""
        # Create test data
        base_date = datetime(2025, 7, 1, 9, 30, tzinfo=timezone.utc)
        candles: List[PriceCandle] = []

        for minute in range(50):
            candle_time = base_date + timedelta(minutes=minute)
            candle = PriceCandle(
                date=candle_time,
                open=Decimal("100.0"),
                high=Decimal("101.0"),
                low=Decimal("99.0"),
                close=Decimal("100.5"),
                volume=Decimal("1000"),
            )
            candles.append(candle)

        series = PriceDataSeries(
            symbol="COUNT_TEST", timeframe=Timeframe.ONE_MIN, candles=candles
        )
        storage_service.store_data(series)

        # Test total count
        total_count = storage_service.get_total_count(
            "COUNT_TEST", Timeframe.ONE_MIN.value
        )
        assert total_count == 50

        # Test count for non-existent symbol
        zero_count = storage_service.get_total_count(
            "NONEXISTENT", Timeframe.ONE_MIN.value
        )
        assert zero_count == 0

    def test_pagination_with_date_filters(self, storage_service: DataStorageService):
        """Test pagination combined with date filtering."""
        # Create test data across multiple days
        base_date = datetime(2025, 7, 1, 9, 30, tzinfo=timezone.utc)
        candles: List[PriceCandle] = []

        # Create 30 candles per day for 3 days (90 total)
        for day in range(3):
            for minute in range(30):
                candle_time = base_date + timedelta(days=day, minutes=minute)
                candle = PriceCandle(
                    date=candle_time,
                    open=Decimal(f"{100 + day:.0f}"),
                    high=Decimal(f"{101 + day:.0f}"),
                    low=Decimal(f"{99 + day:.0f}"),
                    close=Decimal(f"{100 + day:.0f}"),
                    volume=Decimal("1000"),
                )
                candles.append(candle)

        series = PriceDataSeries(
            symbol="DATE_FILTER_TEST", timeframe=Timeframe.ONE_MIN, candles=candles
        )
        storage_service.store_data(series)

        # Test pagination with date filter (only middle day)
        middle_day = date(2025, 7, 2)
        filtered_page = storage_service.load_data(
            "DATE_FILTER_TEST",
            Timeframe.ONE_MIN.value,
            start_date=middle_day,
            end_date=middle_day,
            limit=10,
            offset=0,
            order_by="asc",
        )

        assert len(filtered_page.candles) == 10  # First 10 of the 30 from middle day
        # All candles should be from the middle day (open price = 101)
        for candle in filtered_page.candles:
            assert candle.open == Decimal("101")

        # Test total count with date filter
        filtered_count = storage_service.get_total_count(
            "DATE_FILTER_TEST",
            Timeframe.ONE_MIN.value,
            start_date=middle_day,
            end_date=middle_day,
        )
        assert filtered_count == 30  # Only middle day's data
