"""
Tests for Financial Modeling Prep API client.
"""

import sys
from collections.abc import AsyncGenerator
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from services.data_providers.financial_modeling_prep_client import (
    AuthenticationError,
    FinancialModelingPrepClient,
    FinancialModelingPrepError,
    RateLimitError,
)


@pytest.fixture
def mock_settings() -> MagicMock:
    """Mock settings for testing."""
    settings_mock = MagicMock()
    settings_mock.financial_modeling_prep.api_key = "test_api_key"
    settings_mock.financial_modeling_prep.base_url = "https://api.test.com"
    settings_mock.financial_modeling_prep.rate_limit_per_minute = 300
    return settings_mock


@pytest.fixture
def sample_api_response() -> list[dict[str, Any]]:
    """Sample API response data."""
    return [
        {
            "date": "2025-07-03 12:59:00",
            "open": "213.57",
            "high": "213.69",
            "low": "213.32",
            "close": "213.58",
            "volume": 670856,
        },
        {
            "date": "2025-07-03 13:00:00",
            "open": "213.58",
            "high": "213.75",
            "low": "213.45",
            "close": "213.62",
            "volume": 450123,
        },
    ]


@pytest_asyncio.fixture
async def client(
    mock_settings: MagicMock,
) -> AsyncGenerator[FinancialModelingPrepClient, None]:
    """Create a client instance for testing."""
    with patch(
        "services.data_providers.financial_modeling_prep_client.get_settings",
        return_value=mock_settings,
    ):
        client = FinancialModelingPrepClient()
        yield client
        await client.client.aclose()


class TestFinancialModelingPrepClient:
    """Test cases for FinancialModelingPrepClient."""

    @pytest.mark.asyncio
    async def test_successful_data_fetch(
        self,
        client: FinancialModelingPrepClient,
        sample_api_response: list[dict[str, Any]],
    ):
        """Test successful data fetching."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = sample_api_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            series = await client.fetch_historical_data(
                symbol="AAPL",
                timeframe="1min",
                from_date=date(2025, 7, 3),
                to_date=date(2025, 7, 3),
            )

            assert series.symbol == "AAPL"
            assert series.timeframe == "1min"
            assert len(series.candles) == 2
            assert series.candles[0].open == Decimal("213.57")
            assert series.candles[0].volume == 670856

    @pytest.mark.asyncio
    async def test_empty_response(self, client: FinancialModelingPrepClient):
        """Test handling of empty API response."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = []
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            series = await client.fetch_historical_data("AAPL", "1min")

            assert series.symbol == "AAPL"
            assert series.timeframe == "1min"
            assert len(series.candles) == 0

    @pytest.mark.asyncio
    async def test_api_authentication_error(self, client: FinancialModelingPrepClient):
        """Test handling of authentication errors."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {"Error Message": "Invalid API key"}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            with pytest.raises(AuthenticationError, match="API authentication failed"):
                await client.fetch_historical_data("AAPL", "1min")

    @pytest.mark.asyncio
    async def test_api_rate_limit_error(self, client: FinancialModelingPrepClient):
        """Test handling of rate limit errors."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {"Error Message": "Rate limit exceeded"}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            with pytest.raises(RateLimitError, match="Rate limit exceeded"):
                await client.fetch_historical_data("AAPL", "1min")

    @pytest.mark.asyncio
    async def test_http_401_error(self, client: FinancialModelingPrepClient):
        """Test handling of HTTP 401 errors."""
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = httpx.HTTPStatusError(
                "Unauthorized",
                request=MagicMock(),
                response=MagicMock(status_code=401, text="Unauthorized"),
            )

            with pytest.raises(AuthenticationError, match="Invalid API key"):
                await client.fetch_historical_data("AAPL", "1min")

    @pytest.mark.asyncio
    async def test_http_429_error(self, client: FinancialModelingPrepClient):
        """Test handling of HTTP 429 errors."""
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = httpx.HTTPStatusError(
                "Too Many Requests",
                request=MagicMock(),
                response=MagicMock(status_code=429, text="Too Many Requests"),
            )

            with pytest.raises(RateLimitError, match="Rate limit exceeded"):
                await client.fetch_historical_data("AAPL", "1min")

    @pytest.mark.asyncio
    async def test_http_500_error(self, client: FinancialModelingPrepClient):
        """Test handling of HTTP 500 errors."""
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = httpx.HTTPStatusError(
                "Internal Server Error",
                request=MagicMock(),
                response=MagicMock(status_code=500, text="Internal Server Error"),
            )

            with pytest.raises(FinancialModelingPrepError, match="HTTP error 500"):
                await client.fetch_historical_data("AAPL", "1min")

    @pytest.mark.asyncio
    async def test_request_error(self, client: FinancialModelingPrepClient):
        """Test handling of request errors."""
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = httpx.RequestError("Connection failed")

            with pytest.raises(FinancialModelingPrepError, match="Request failed"):
                await client.fetch_historical_data("AAPL", "1min")

    @pytest.mark.asyncio
    async def test_invalid_response_format(self, client: FinancialModelingPrepClient):
        """Test handling of invalid response format."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {"unexpected": "format"}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            with pytest.raises(
                FinancialModelingPrepError, match="Unexpected response format"
            ):
                await client.fetch_historical_data("AAPL", "1min")

    @pytest.mark.asyncio
    async def test_invalid_candle_data(self, client: FinancialModelingPrepClient):
        """Test handling of invalid candle data."""
        invalid_response: list[dict[str, Any]] = [
            {
                "date": "2025-07-03 12:59:00",
                "open": "213.57",
                "high": "213.69",
                "low": "213.32",
                "close": "213.58",
                "volume": 670856,
            },
            {
                "date": "invalid-date",  # Invalid date format
                "open": "213.58",
                "high": "213.75",
                "low": "213.45",
                "close": "213.62",
                "volume": 450123,
            },
        ]

        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = invalid_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            series = await client.fetch_historical_data("AAPL", "1min")

            # Should skip invalid candle and return only valid one
            assert len(series.candles) == 1
            assert series.candles[0].open == Decimal("213.57")

    @pytest.mark.asyncio
    async def test_timeframe_mapping_intraday(
        self,
        client: FinancialModelingPrepClient,
        sample_api_response: list[dict[str, Any]],
    ):
        """Test timeframe mapping to API format for intraday data."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = sample_api_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            await client.fetch_historical_data("AAPL", "1h")

            # Check that the correct endpoint was called
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert "historical-chart/1hour" in call_args[0][0]

    @pytest.mark.asyncio
    async def test_daily_data_endpoint(self, client: FinancialModelingPrepClient):
        """Test that daily data uses the correct EOD endpoint."""
        daily_response: list[dict[str, Any]] = [
            {
                "date": "2025-07-03",
                "open": "213.50",
                "high": "214.00",
                "low": "213.00",
                "close": "213.75",
                "volume": 1000000,
            }
        ]

        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = daily_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            series = await client.fetch_historical_data("AAPL", "daily")

            # Check that the correct EOD endpoint was called
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert "historical-price-eod/full" in call_args[0][0]

            # Verify the data was parsed correctly
            assert series.symbol == "AAPL"
            assert series.timeframe == "daily"
            assert len(series.candles) == 1
            assert series.candles[0].close == Decimal("213.75")
            # Date should be set to 4 PM ET for daily data
            assert series.candles[0].date.hour == 16

    @pytest.mark.asyncio
    async def test_daily_data_with_time(self, client: FinancialModelingPrepClient):
        """Test daily data parsing when response includes time."""
        daily_response: list[dict[str, Any]] = [
            {
                "date": "2025-07-03 16:00:00",
                "open": "213.50",
                "high": "214.00",
                "low": "213.00",
                "close": "213.75",
                "volume": 1000000,
            }
        ]

        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = daily_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            series = await client.fetch_historical_data("AAPL", "1day")

            # Check that the correct EOD endpoint was called
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert "historical-price-eod/full" in call_args[0][0]

            # Verify the data was parsed correctly
            assert len(series.candles) == 1
            assert series.candles[0].date == datetime(2025, 7, 3, 16, 0, 0)

    @pytest.mark.asyncio
    async def test_api_key_in_params(
        self,
        client: FinancialModelingPrepClient,
        sample_api_response: list[dict[str, Any]],
    ):
        """Test that API key is added to request parameters."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = sample_api_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            await client.fetch_historical_data("AAPL", "1min")

            # Check that API key was added to params
            call_args = mock_get.call_args
            params = call_args[1]["params"]
            assert params["apikey"] == "test_api_key"

    @pytest.mark.asyncio
    async def test_date_parameters(
        self,
        client: FinancialModelingPrepClient,
        sample_api_response: list[dict[str, Any]],
    ):
        """Test that date parameters are correctly formatted."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = sample_api_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            await client.fetch_historical_data(
                "AAPL", "1min", from_date=date(2025, 7, 1), to_date=date(2025, 7, 3)
            )

            # Check date formatting
            call_args = mock_get.call_args
            params = call_args[1]["params"]
            assert params["from"] == "2025-07-01"
            assert params["to"] == "2025-07-03"

    @pytest.mark.asyncio
    async def test_fetch_latest_data(
        self,
        client: FinancialModelingPrepClient,
        sample_api_response: list[dict[str, Any]],
    ):
        """Test fetching latest data."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = sample_api_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            latest_candle = await client.fetch_latest_data("AAPL", "1min")

            assert latest_candle is not None
            # Should return the latest candle (second one in our sample)
            assert latest_candle.date == datetime(2025, 7, 3, 13, 0, 0)
            assert latest_candle.close == Decimal("213.62")

    @pytest.mark.asyncio
    async def test_fetch_latest_data_no_data(self, client: FinancialModelingPrepClient):
        """Test fetching latest data when no data is available."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = []
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            latest_candle = await client.fetch_latest_data("AAPL", "1min")

            assert latest_candle is None

    @pytest.mark.asyncio
    async def test_rate_limiting(
        self,
        client: FinancialModelingPrepClient,
        sample_api_response: list[dict[str, Any]],
    ):
        """Test rate limiting functionality."""
        with patch.object(client.client, "get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = sample_api_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            # Mock time to control rate limiting
            with patch("asyncio.get_event_loop") as mock_loop:
                mock_loop.return_value.time.return_value = 0.0

                # First request should go through immediately
                await client.fetch_historical_data("AAPL", "1min")

                # Verify request was made
                assert mock_get.call_count == 1

    @pytest.mark.asyncio
    async def test_context_manager(self, mock_settings: MagicMock):
        """Test using client as async context manager."""
        with patch(
            "services.data_providers.financial_modeling_prep_client.get_settings",
            return_value=mock_settings,
        ):
            async with FinancialModelingPrepClient() as client:
                assert client is not None
                # Client should be properly initialized
                assert client.fmp_settings.api_key == "test_api_key"
