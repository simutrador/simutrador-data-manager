"""
Tests for Polygon client functionality.

This module tests the Polygon API client, including:
- Batch size calculation for different timeframes
- Trades endpoint functionality
- Rate limiting and error handling
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import pytest

from services.data_providers.polygon_client import PolygonClient, PolygonError
from services.polygon_url_generator import PolygonUrlGenerator


class TestPolygonClient:
    """Test cases for PolygonClient."""

    @pytest.fixture
    def polygon_client(self):
        """Create a PolygonClient instance for testing."""
        with patch(
            "services.data_providers.polygon_client.get_settings"
        ) as mock_settings:
            mock_settings.return_value.polygon.api_key = "test_key"
            mock_settings.return_value.polygon.base_url = "https://api.polygon.io"
            mock_settings.return_value.polygon.requests_per_minute = 5

            client = PolygonClient()
            return client

    def test_calculate_batch_size_1min(self, polygon_client: PolygonClient) -> None:
        """Test batch size calculation for 1-minute data."""
        # 1-minute data should be limited to 60 days max for better efficiency
        batch_size = polygon_client._calculate_batch_size("1min")  # type: ignore
        assert batch_size == 60, "1-minute data should be limited to 60 days"

    def test_calculate_batch_size_5min(self, polygon_client: PolygonClient) -> None:
        """Test batch size calculation for 5-minute data."""
        # 5-minute data should allow up to 90 days
        batch_size = polygon_client._calculate_batch_size("5min")  # type: ignore
        expected = min(int(45000 / 78), 90)  # 78 candles per day for 5min
        assert batch_size == expected

    def test_calculate_batch_size_15min(self, polygon_client: PolygonClient) -> None:
        """Test batch size calculation for 15-minute data."""
        # 15-minute data should allow up to 90 days
        batch_size = polygon_client._calculate_batch_size("15min")  # type: ignore
        expected = min(int(45000 / 26), 90)  # 26 candles per day for 15min
        assert batch_size == expected

    def test_calculate_batch_size_daily(self, polygon_client: PolygonClient) -> None:
        """Test batch size calculation for daily data."""
        # Daily data should allow up to 365 days
        batch_size = polygon_client._calculate_batch_size("daily")  # type: ignore
        expected = min(int(45000 / 1), 365)  # 1 candle per day for daily
        assert batch_size == expected

    @pytest.mark.asyncio
    async def test_fetch_trades_data_success(
        self, polygon_client: PolygonClient
    ) -> None:
        """Test successful trades data fetching."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        # Mock the trades API response
        mock_response: dict[str, Any] = {
            "results": [
                {
                    "t": 1704117000000000000,  # nanosecond timestamp
                    "p": 150.25,  # price
                    "s": 100,  # size
                    "x": 4,  # exchange
                    "c": [],  # conditions
                    "i": "12345",  # trade ID
                }
            ],
            "status": "OK",
            "request_id": "test",
            "next_url": None,
        }

        with patch.object(
            polygon_client, "_make_trades_request", return_value=mock_response
        ):
            trades = await polygon_client.fetch_trades_data(
                "AAPL", start_time, end_time
            )

            assert len(trades) == 1
            assert (
                abs(trades[0]["price"] - 150.25) < 0.01
            )  # Float comparison with tolerance
            assert trades[0]["size"] == 100
            assert "timestamp" in trades[0]
            assert "timestamp_ns" in trades[0]

    @pytest.mark.asyncio
    async def test_fetch_trades_data_no_results(
        self, polygon_client: PolygonClient
    ) -> None:
        """Test trades data fetching when no trades are found."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        # Mock empty response
        mock_response: dict[str, Any] = {
            "results": [],
            "status": "OK",
            "request_id": "test",
            "next_url": None,
        }

        with patch.object(
            polygon_client, "_make_trades_request", return_value=mock_response
        ):
            trades = await polygon_client.fetch_trades_data(
                "AAPL", start_time, end_time
            )

            assert len(trades) == 0

    @pytest.mark.asyncio
    async def test_fetch_trades_data_error(self, polygon_client: PolygonClient) -> None:
        """Test trades data fetching error handling."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        with patch.object(
            polygon_client, "_make_trades_request", side_effect=Exception("API Error")
        ):
            with pytest.raises(PolygonError, match="Failed to fetch trades data"):
                await polygon_client.fetch_trades_data("AAPL", start_time, end_time)

    def test_trades_timestamp_conversion(
        self, polygon_client: PolygonClient  # noqa: ARG002
    ) -> None:
        """Test that nanosecond timestamps are properly converted."""
        # Test timestamp conversion logic
        # Use a known timestamp: 2024-01-01 14:30:00 UTC
        test_datetime = datetime(2024, 1, 1, 14, 30, 0, tzinfo=UTC)
        test_timestamp_ns = int(test_datetime.timestamp() * 1_000_000_000)

        # Convert back to datetime
        converted_datetime = datetime.fromtimestamp(
            test_timestamp_ns / 1_000_000_000, tz=UTC
        )

        # Should match the original datetime
        assert converted_datetime.year == 2024
        assert converted_datetime.month == 1
        assert converted_datetime.day == 1
        assert converted_datetime.hour == 14
        assert converted_datetime.minute == 30

    @pytest.mark.asyncio
    async def test_trades_limit_enforcement(
        self, polygon_client: PolygonClient
    ) -> None:
        """Test that trades API respects the 50,000 limit."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        # Mock the _make_trades_request to capture the params
        with patch.object(polygon_client, "_make_trades_request") as mock_request:
            mock_request.return_value = {"results": [], "status": "OK"}

            # Test with limit > 50000
            await polygon_client.fetch_trades_data(
                "AAPL", start_time, end_time, limit=75000
            )

            # Should be called with limit capped at 50000
            args, _ = mock_request.call_args
            _, params = args
            assert params["limit"] == 50000

    @pytest.mark.asyncio
    async def test_batch_size_prevents_50k_limit_breach(
        self, polygon_client: PolygonClient
    ) -> None:
        """Test that the new batch size prevents hitting the 50k limit for 1-minute data."""
        # For 1-minute data with 60-day batches:
        # 60 days * 390 candles/day = 23,400 candles (well under 50k)
        batch_size = polygon_client._calculate_batch_size(  # type: ignore
            "1min"
        )  # pyright: ignore[reportPrivateUsage]
        estimated_candles = batch_size * 390  # 390 candles per trading day

        assert estimated_candles < 50000, (
            f"Batch size {batch_size} would generate {estimated_candles} candles, "
            "exceeding 50k limit"
        )
        assert (
            estimated_candles < 45000
        ), f"Batch size {batch_size} should stay under safety margin of 45k candles"


class TestPolygonUrlGenerator:
    """Test cases for Polygon URL generation with trades endpoint."""

    @pytest.fixture
    def url_generator(self):
        """Create a PolygonUrlGenerator instance for testing."""
        with patch("services.polygon_url_generator.get_settings") as mock_settings:
            mock_settings.return_value.polygon.api_key = "test_key"

            from services.polygon_url_generator import PolygonUrlGenerator

            return PolygonUrlGenerator()

    def test_generate_trades_url_for_period(
        self, url_generator: PolygonUrlGenerator
    ) -> None:
        """Test trades URL generation for a specific period."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        url = url_generator.generate_trades_url_for_period("AAPL", start_time, end_time)

        # Check that URL contains expected components
        assert "v3/trades/AAPL" in url
        assert "timestamp.gte=" in url
        assert "timestamp.lte=" in url
        assert "limit=50000" in url
        assert "apikey=test_key" in url

        # Check nanosecond timestamp format
        start_ns = int(start_time.timestamp() * 1_000_000_000)
        end_ns = int(end_time.timestamp() * 1_000_000_000)
        assert f"timestamp.gte={start_ns}" in url
        assert f"timestamp.lte={end_ns}" in url

    def test_generate_trades_urls_for_missing_periods(
        self, url_generator: PolygonUrlGenerator
    ) -> None:
        """Test generating multiple trades URLs for missing periods."""
        periods = [
            (
                datetime(2024, 1, 1, 14, 30, tzinfo=UTC),
                datetime(2024, 1, 1, 15, 30, tzinfo=UTC),
            ),
            (
                datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
                datetime(2024, 1, 2, 15, 30, tzinfo=UTC),
            ),
        ]

        urls = url_generator.generate_trades_urls_for_missing_periods("AAPL", periods)

        assert len(urls) == 2
        for url in urls:
            assert "v3/trades/AAPL" in url
            assert "timestamp.gte=" in url
            assert "timestamp.lte=" in url
