"""
Tests for Gap Filling Service functionality.

This module tests the gap filling service, including:
- Trading activity detection using trades endpoint
- Enhanced gap fill results with trading activity information
- Integration with Polygon trades API
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from models.nightly_update_api import GapFillResult
from services.data_providers.polygon_client import PolygonClient
from services.gap_filling_service import GapFillingService


class TestGapFillingService:
    """Test cases for GapFillingService."""

    @pytest.fixture
    def gap_filling_service(self):
        """Create a GapFillingService instance for testing."""
        with patch("services.gap_filling_service.DataStorageService") as mock_storage:
            mock_storage_instance = MagicMock()
            mock_storage.return_value = mock_storage_instance

            service = GapFillingService()
            service.storage_service = mock_storage_instance
            return service

    @pytest.mark.asyncio
    async def test_check_trading_activity_with_trades(
        self, gap_filling_service: GapFillingService
    ) -> None:
        """Test trading activity detection when trades are found."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        # Mock trades data
        mock_trades: list[dict[str, Any]] = [
            {
                "timestamp": start_time,
                "price": 150.25,
                "size": 100,
                "exchange_id": 4,
            }
        ]

        with patch(
            "services.gap_filling_service.DataProviderFactory.create_provider"
        ) as mock_factory:
            mock_client = AsyncMock(spec=PolygonClient)
            mock_client.fetch_trades_data.return_value = mock_trades
            mock_factory.return_value.__aenter__.return_value = mock_client

            has_activity, status_message = (
                await gap_filling_service._check_trading_activity(  # pyright: ignore[reportPrivateUsage]
                    "AAPL", start_time, end_time
                )
            )

            assert has_activity is True
            assert "Trading activity check completed" in status_message
            mock_client.fetch_trades_data.assert_called_once_with(
                "AAPL", start_time, end_time, limit=1
            )

    @pytest.mark.asyncio
    async def test_check_trading_activity_no_trades(
        self, gap_filling_service: GapFillingService
    ) -> None:
        """Test trading activity detection when no trades are found."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        with patch(
            "services.gap_filling_service.DataProviderFactory.create_provider"
        ) as mock_factory:
            mock_client = AsyncMock(spec=PolygonClient)
            mock_client.fetch_trades_data.return_value = []  # No trades
            mock_factory.return_value.__aenter__.return_value = mock_client

            has_activity, status_message = (
                await gap_filling_service._check_trading_activity(  # pyright: ignore[reportPrivateUsage]
                    "AAPL", start_time, end_time
                )
            )

            assert has_activity is False
            assert "Trading activity check completed" in status_message

    @pytest.mark.asyncio
    async def test_check_trading_activity_error_handling(
        self, gap_filling_service: GapFillingService
    ) -> None:
        """Test trading activity detection error handling."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        with patch(
            "services.gap_filling_service.DataProviderFactory.create_provider"
        ) as mock_factory:
            mock_client = AsyncMock(spec=PolygonClient)
            mock_client.fetch_trades_data.side_effect = Exception("API Error")
            mock_factory.return_value.__aenter__.return_value = mock_client

            has_activity, status_message = (
                await gap_filling_service._check_trading_activity(  # pyright: ignore[reportPrivateUsage]
                    "AAPL", start_time, end_time
                )
            )

            assert has_activity is False
            assert "Error checking trades" in status_message

    @pytest.mark.asyncio
    async def test_fill_single_gap_no_candles_with_trading_activity(
        self, gap_filling_service: GapFillingService
    ) -> None:
        """Test gap filling when no candles are found but trading activity exists."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        # Mock the HTTP request to return no candles
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [], "status": "OK"}

        # Mock the trades endpoint call (new gap filling approach)
        with patch(
            "services.gap_filling_service.DataProviderFactory.create_provider"
        ) as mock_factory:
            mock_client = AsyncMock(spec=PolygonClient)
            mock_client.fetch_trades_data.return_value = []  # No trades data
            mock_factory.return_value.__aenter__.return_value = mock_client

            # Mock trading activity check to return True
            with patch.object(
                gap_filling_service, "_check_trading_activity"
            ) as mock_check:
                mock_check.return_value = (
                    True,
                    "Trading activity check completed",
                )

                result = await gap_filling_service._fill_single_gap(  # pyright: ignore[reportPrivateUsage]
                    "AAPL", start_time, end_time
                )

                assert isinstance(result, GapFillResult)
                assert result.success is False
                assert result.candles_recovered == 0
                assert result.vendor_unavailable is True
                assert result.has_trading_activity is True
                assert result.trades_api_url is not None
                assert result.error_message is not None
                assert "Failed to fetch data from vendor" in result.error_message

    @pytest.mark.asyncio
    async def test_fill_single_gap_no_candles_no_trading_activity(
        self, gap_filling_service: GapFillingService
    ) -> None:
        """Test gap filling when no candles are found and no trading activity exists."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        # Mock the HTTP request to return no candles
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [], "status": "OK"}

        # Mock the trades endpoint call (new gap filling approach)
        with patch(
            "services.gap_filling_service.DataProviderFactory.create_provider"
        ) as mock_factory:
            mock_client = AsyncMock(spec=PolygonClient)
            mock_client.fetch_trades_data.return_value = []  # No trades data
            mock_factory.return_value.__aenter__.return_value = mock_client

            # Mock trading activity check to return False
            with patch.object(
                gap_filling_service, "_check_trading_activity"
            ) as mock_check:
                mock_check.return_value = (
                    False,
                    "Trading activity check completed",
                )

                result = await gap_filling_service._fill_single_gap(  # pyright: ignore[reportPrivateUsage]
                    "AAPL", start_time, end_time
                )

                assert isinstance(result, GapFillResult)
                assert result.success is False
                assert result.candles_recovered == 0
                assert result.vendor_unavailable is True
                assert result.has_trading_activity is False
                assert result.trades_api_url is not None
                assert result.error_message is not None
                assert "No trading activity detected" in result.error_message

    @pytest.mark.asyncio
    async def test_fill_single_gap_successful_recovery(
        self, gap_filling_service: GapFillingService
    ) -> None:
        """Test successful gap filling with candle recovery."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        # Mock the HTTP request to return candles
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "t": int(start_time.timestamp() * 1000),  # millisecond timestamp
                    "o": 150.0,
                    "h": 151.0,
                    "l": 149.0,
                    "c": 150.5,
                    "v": 1000,
                }
            ],
            "status": "OK",
        }

        # Mock the trades endpoint call (new gap filling approach)
        with patch(
            "services.gap_filling_service.DataProviderFactory.create_provider"
        ) as mock_factory:
            mock_client = AsyncMock(spec=PolygonClient)
            mock_client.fetch_trades_data.return_value = [
                {
                    "timestamp": start_time,
                    "price": 150.25,
                    "size": 100,
                    "exchange_id": 4,
                }
            ]  # Mock trades data
            mock_factory.return_value.__aenter__.return_value = mock_client

            # Mock the fallback HTTP client for aggregates
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_http_client = AsyncMock()
                mock_http_client.get.return_value = mock_response
                mock_client_class.return_value.__aenter__.return_value = (
                    mock_http_client
                )

                # Mock storage service methods
                with patch.object(
                    gap_filling_service.storage_service,
                    "load_data",
                    return_value=MagicMock(),
                ):
                    with patch.object(
                        gap_filling_service.storage_service,
                        "store_data",
                        return_value=None,
                    ):
                        result = await gap_filling_service._fill_single_gap(  # pyright: ignore[reportPrivateUsage]
                            "AAPL", start_time, end_time
                        )

            assert isinstance(result, GapFillResult)
            assert result.success is True
            assert result.candles_recovered == 1
            assert result.vendor_unavailable is False
            assert (
                result.has_trading_activity is True
            )  # Assume true for successful fills
            assert result.trades_api_url is None  # Not needed for successful fills

    @pytest.mark.asyncio
    async def test_fill_single_gap_exception_handling(
        self, gap_filling_service: GapFillingService
    ) -> None:
        """Test gap filling exception handling."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client_class.side_effect = Exception("Network error")

            result = await gap_filling_service._fill_single_gap(  # pyright: ignore[reportPrivateUsage]
                "AAPL", start_time, end_time
            )

            assert isinstance(result, GapFillResult)
            assert result.success is False
            assert result.candles_recovered == 0
            assert result.vendor_unavailable is False
            assert result.has_trading_activity is None  # Unknown due to error
            assert result.trades_api_url is not None  # Trades URL is still generated
            assert "v3/trades/AAPL" in result.trades_api_url  # Verify correct format
            assert result.error_message is not None
            assert "Network error" in result.error_message

    def test_gap_fill_result_model_fields(self) -> None:
        """Test that GapFillResult model has the new fields."""
        start_time = datetime(2024, 1, 1, 14, 30, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 15, 30, tzinfo=UTC)

        result = GapFillResult(
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            attempted=True,
            success=False,
            candles_recovered=0,
            vendor_unavailable=True,
            polygon_api_url="https://api.polygon.io/v2/aggs/...",
            trades_api_url="https://api.polygon.io/v3/trades/...",
            has_trading_activity=False,
            error_message="No trading activity detected",
        )

        # Verify all fields are accessible
        assert result.trades_api_url is not None
        assert result.has_trading_activity is False
        assert result.error_message is not None
        assert "No trading activity detected" in result.error_message
