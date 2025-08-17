"""
Tests for the data analysis API endpoints.

This module tests the data analysis API including:
- Data completeness analysis
- Data quality validation
- Error handling
"""

from typing import Any, Dict
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from main import app


class TestDataAnalysisAPI:
    """Test data analysis API endpoints."""

    @pytest.fixture
    def client(self) -> TestClient:
        """Create test client."""
        return TestClient(app)

    def test_analyze_data_completeness(self, client: TestClient) -> None:
        """Test data completeness analysis endpoint."""
        with patch(
            "api.data_analysis.StockMarketValidationService"
        ) as mock_service_class:
            mock_service = Mock()

            # Mock completeness data
            mock_completeness_data: Dict[str, Dict[str, Any]] = {
                "AAPL": {
                    "total_trading_days": 5,
                    "valid_days": 5,
                    "invalid_days": 0,
                    "completeness_percentage": 100.0,
                    "total_expected_candles": 1950,  # 5 days * 390 candles
                    "total_actual_candles": 1950,
                    "missing_candles": 0,
                    "validation_results": [],
                },
                "MSFT": {
                    "total_trading_days": 5,
                    "valid_days": 4,
                    "invalid_days": 1,
                    "completeness_percentage": 95.0,
                    "total_expected_candles": 1950,
                    "total_actual_candles": 1853,  # Missing some candles
                    "missing_candles": 97,
                    "validation_results": [],
                },
            }

            mock_service.get_data_completeness_summary.return_value = (
                mock_completeness_data
            )
            mock_service_class.return_value = mock_service

            request_data: Dict[str, Any] = {
                "symbols": ["AAPL", "MSFT"],
                "start_date": "2025-01-13",
                "end_date": "2025-01-17",
                "include_details": False,
            }

            response = client.post("/data-analysis/completeness", json=request_data)

            assert response.status_code == 200
            data = response.json()

            assert "analysis_period" in data
            assert "symbol_completeness" in data
            assert "overall_statistics" in data
            assert "symbols_needing_attention" in data
            assert "recommendations" in data

            # Check overall statistics
            overall = data["overall_statistics"]
            assert overall["total_symbols"] == 2
            assert overall["total_expected_candles"] == 3900  # 2 symbols * 1950 candles
            assert overall["total_actual_candles"] == 3803  # 1950 + 1853

            # MSFT should need attention (< 95% completeness is not true in this case,
            # but let's check the logic)
            # Actually MSFT has 95% which is not < 95%, so it shouldn't be in the attention list
            assert len(data["symbols_needing_attention"]) == 0

    def test_analyze_data_completeness_with_attention_needed(
        self, client: TestClient
    ) -> None:
        """Test data completeness analysis when symbols need attention."""
        with patch(
            "api.data_analysis.StockMarketValidationService"
        ) as mock_service_class:
            mock_service = Mock()

            # Mock completeness data with low completeness
            mock_completeness_data: Dict[str, Dict[str, Any]] = {
                "AAPL": {
                    "total_trading_days": 5,
                    "valid_days": 3,
                    "invalid_days": 2,
                    "completeness_percentage": 85.0,  # Below 95% threshold
                    "total_expected_candles": 1950,
                    "total_actual_candles": 1658,
                    "missing_candles": 292,
                    "validation_results": [],
                }
            }

            mock_service.get_data_completeness_summary.return_value = (
                mock_completeness_data
            )
            mock_service_class.return_value = mock_service

            request_data: Dict[str, Any] = {
                "symbols": ["AAPL"],
                "start_date": "2025-01-13",
                "end_date": "2025-01-17",
                "include_details": False,
            }

            response = client.post("/data-analysis/completeness", json=request_data)

            assert response.status_code == 200
            data = response.json()

            # AAPL should need attention (85% < 95%)
            assert "AAPL" in data["symbols_needing_attention"]
            assert len(data["recommendations"]) > 0
            assert any("less than 95%" in rec for rec in data["recommendations"])

    def test_analyze_data_completeness_error(self, client: TestClient) -> None:
        """Test error handling in data completeness analysis."""
        with patch(
            "api.data_analysis.StockMarketValidationService"
        ) as mock_service_class:
            mock_service = Mock()
            mock_service.get_data_completeness_summary.side_effect = Exception(
                "Analysis failed"
            )
            mock_service_class.return_value = mock_service

            request_data: Dict[str, Any] = {
                "symbols": ["AAPL"],
                "start_date": "2025-01-13",
                "end_date": "2025-01-17",
                "include_details": False,
            }

            response = client.post("/data-analysis/completeness", json=request_data)

            assert response.status_code == 500
            assert "Analysis failed" in response.json()["detail"]

    def test_request_validation(self, client: TestClient) -> None:
        """Test request validation for data analysis endpoints."""
        # Test invalid date format in completeness analysis
        invalid_request: Dict[str, Any] = {
            "symbols": ["AAPL"],
            "start_date": "invalid-date",
            "end_date": "2025-01-17",
            "include_details": False,
        }

        response = client.post("/data-analysis/completeness", json=invalid_request)
        assert response.status_code == 422  # Validation error

    def test_empty_symbols_list(self, client: TestClient) -> None:
        """Test handling of empty symbols list."""
        request_data: Dict[str, Any] = {
            "symbols": [],  # Empty list
            "start_date": "2025-01-13",
            "end_date": "2025-01-17",
            "include_details": False,
        }

        response = client.post("/data-analysis/completeness", json=request_data)
        assert response.status_code == 422  # Should fail validation due to empty list
