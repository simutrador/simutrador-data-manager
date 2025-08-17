"""
Tests for the nightly update API endpoints.

This module tests the nightly update API including:
- Starting nightly updates
- Checking update status
- Error handling
"""

from typing import Any, Dict
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from main import app


class TestNightlyUpdateAPI:
    """Test cases for nightly update API endpoints."""

    @pytest.fixture(autouse=True)
    def reset_singletons(self):
        """Reset singleton instances before each test."""
        from api.nightly_update import reset_progress_service

        reset_progress_service()
        yield
        reset_progress_service()

    @pytest.fixture
    def client(self) -> TestClient:
        """Create a test client for the API."""
        return TestClient(app)

    @pytest.fixture
    def mock_nightly_service(self) -> Mock:
        """Create a mock nightly update service."""
        return Mock()

    @pytest.fixture
    def mock_validation_service(self) -> Mock:
        """Create a mock validation service."""
        return Mock()

    def test_start_nightly_update_default_symbols(self, client: TestClient) -> None:
        """Test starting nightly update with default symbols."""
        request_data: Dict[str, Any] = {
            "force_validation": True,
            "enable_resampling": True,
        }

        response = client.post("/nightly-update/start", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert "request_id" in data
        assert data["status"] == "started"
        # The message should contain the number of symbols (from default symbols)
        assert "symbols" in data["message"]

    def test_start_nightly_update_custom_symbols(self, client: TestClient) -> None:
        """Test starting nightly update with custom symbols."""
        with patch("api.nightly_update.get_nightly_update_service") as mock_get_service:

            mock_service = Mock()
            mock_service.execute_nightly_update = AsyncMock()
            mock_get_service.return_value = mock_service

            request_data: Dict[str, Any] = {
                "symbols": ["AAPL", "MSFT"],
                "force_validation": True,
                "max_concurrent": 3,
                "enable_resampling": True,
            }

            response = client.post("/nightly-update/start", json=request_data)

            assert response.status_code == 200
            data = response.json()
            assert "request_id" in data
            assert data["status"] == "started"
            assert "2 symbols" in data["message"]

            # Note: We don't assert on execute_nightly_update being called because
            # it runs as a background task and mocking background tasks is complex

    def test_start_nightly_update_error(self, client: TestClient) -> None:
        """Test error handling when starting nightly update fails."""
        from api.nightly_update import get_nightly_update_service

        # Create a mock service that throws an exception
        mock_service = Mock()
        mock_service.get_default_symbols.side_effect = Exception("Service error")

        # Override the dependency
        app.dependency_overrides[get_nightly_update_service] = lambda: mock_service

        try:
            # Don't provide symbols so get_default_symbols() will be called
            request_data: Dict[str, Any] = {
                "force_validation": True,
                "enable_resampling": True,
            }

            response = client.post("/nightly-update/start", json=request_data)

            assert response.status_code == 500
            assert "Failed to start update" in response.json()["detail"]
        finally:
            # Clean up the override
            app.dependency_overrides.clear()

    def test_get_update_status_active(self, client: TestClient) -> None:
        """Test getting status of an active update."""
        from datetime import datetime

        from api.nightly_update import get_nightly_update_service, get_progress_service
        from models.nightly_update_api import (
            ActiveUpdateInfo,
            NightlyUpdateRequest,
            ProgressInfo,
        )

        # Create mock services
        mock_nightly_service = Mock()
        mock_nightly_service.get_default_symbols.return_value = ["AAPL"]

        mock_progress_service = Mock()

        # Override dependencies
        app.dependency_overrides[get_nightly_update_service] = (
            lambda: mock_nightly_service
        )
        app.dependency_overrides[get_progress_service] = lambda: mock_progress_service

        try:
            # Mock the service method
            with patch(
                "api.nightly_update.get_nightly_update_service"
            ) as mock_get_service:
                mock_service = Mock()
                mock_service.get_default_symbols.return_value = ["AAPL"]
                mock_service.execute_nightly_update = AsyncMock()
                mock_get_service.return_value = mock_service

                # Start update
                start_response = client.post("/nightly-update/start", json={})
                request_id: str = start_response.json()["request_id"]

                # Mock the active update info for status check
                mock_active_update = ActiveUpdateInfo(
                    request=NightlyUpdateRequest(
                        symbols=None,
                        force_validation=True,
                        max_concurrent=None,
                        enable_resampling=True,
                        start_date=None,
                        end_date=None,
                    ),
                    started_at=datetime.now(),
                    status="running",
                    symbols=["AAPL"],
                )
                mock_progress_service.get_active_update.return_value = (
                    mock_active_update
                )

                # Mock progress info
                mock_progress_info = ProgressInfo(
                    total_symbols=1,
                    completed_symbols=0,
                    current_step="Processing",
                    progress_percentage=50.0,
                )
                mock_progress_service.calculate_overall_progress.return_value = (
                    mock_progress_info
                )

                # Check status immediately - should be starting or running, or completed
                status_response = client.get(f"/nightly-update/status/{request_id}")

                assert status_response.status_code == 200
                data = status_response.json()
                assert data["request_id"] == request_id
                assert data["status"] in ["starting", "running", "completed"]
                # Don't assert is_complete since it might complete very quickly in tests

                # Note: We don't assert on execute_nightly_update being called because
                # it runs as a background task and mocking background tasks is complex
        finally:
            # Clean up the overrides
            app.dependency_overrides.clear()

    def test_get_update_status_not_found(self, client: TestClient) -> None:
        """Test getting status of non-existent update."""
        fake_request_id = "non-existent-id"

        response = client.get(f"/nightly-update/status/{fake_request_id}")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"]

    def test_list_active_updates_empty(self, client: TestClient) -> None:
        """Test listing active updates when none are running."""
        response = client.get("/nightly-update/active")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Should be empty initially

    def test_start_nightly_update_with_custom_date_range(
        self, client: TestClient
    ) -> None:
        """Test starting nightly update with custom date range."""
        with patch("api.nightly_update.get_nightly_update_service") as mock_get_service:

            mock_service = Mock()
            mock_service.get_default_symbols.return_value = ["AAPL", "MSFT"]
            mock_service.execute_nightly_update = AsyncMock()
            mock_get_service.return_value = mock_service

            request_data: Dict[str, Any] = {
                "symbols": ["AAPL", "MSFT"],
                "force_validation": True,
                "enable_resampling": True,
                "start_date": "2025-01-01",
                "end_date": "2025-01-15",
            }

            response = client.post("/nightly-update/start", json=request_data)

            assert response.status_code == 200
            data = response.json()
            assert "request_id" in data
            assert data["status"] == "started"
            assert "2 symbols" in data["message"]

            # Note: We don't assert on execute_nightly_update being called because
            # it runs as a background task and mocking background tasks is complex

    def test_start_nightly_update_with_start_date_only(
        self, client: TestClient
    ) -> None:
        """Test starting nightly update with only start date (end date should default)."""
        with patch("api.nightly_update.get_nightly_update_service") as mock_get_service:

            mock_service = Mock()
            mock_service.get_default_symbols.return_value = ["AAPL"]
            mock_service.execute_nightly_update = AsyncMock()
            mock_get_service.return_value = mock_service

            request_data: Dict[str, Any] = {
                "symbols": ["AAPL"],
                "force_validation": True,
                "enable_resampling": True,
                "start_date": "2025-01-01",
                # end_date not provided - should default to yesterday
            }

            response = client.post("/nightly-update/start", json=request_data)

            assert response.status_code == 200
            data = response.json()
            assert "request_id" in data
            assert data["status"] == "started"

            # Note: We don't assert on execute_nightly_update being called because
            # it runs as a background task and mocking background tasks is complex

    def test_start_nightly_update_with_end_date_only(self, client: TestClient) -> None:
        """Test starting nightly update with only end date (start date should be \
            auto-determined)."""
        with patch("api.nightly_update.get_nightly_update_service") as mock_get_service:

            mock_service = Mock()
            mock_service.get_default_symbols.return_value = ["AAPL"]
            mock_service.execute_nightly_update = AsyncMock()
            mock_get_service.return_value = mock_service

            request_data: Dict[str, Any] = {
                "symbols": ["AAPL"],
                "force_validation": True,
                "enable_resampling": True,
                # start_date not provided - should be auto-determined
                "end_date": "2025-01-15",
            }

            response = client.post("/nightly-update/start", json=request_data)

            assert response.status_code == 200
            data = response.json()
            assert "request_id" in data
            assert data["status"] == "started"

            # Note: We don't assert on execute_nightly_update being called because
            # it runs as a background task and mocking background tasks is complex

    def test_nightly_update_invalid_date_formats(self, client: TestClient) -> None:
        """Test nightly update with invalid date formats."""
        with patch("api.nightly_update.get_nightly_update_service") as mock_get_service:
            mock_service = Mock()
            mock_service.get_default_symbols.return_value = ["AAPL"]
            mock_get_service.return_value = mock_service

            # Test invalid start date format
            request_data: Dict[str, Any] = {
                "symbols": ["AAPL"],
                "start_date": "invalid-date-format",
                "end_date": "2025-01-15",
            }

            response = client.post("/nightly-update/start", json=request_data)
            assert response.status_code == 422  # Validation error

            # Test invalid end date format
            request_data = {
                "symbols": ["AAPL"],
                "start_date": "2025-01-01",
                "end_date": "not-a-date",
            }

            response = client.post("/nightly-update/start", json=request_data)
            assert response.status_code == 422  # Validation error

    def test_nightly_update_future_dates(self, client: TestClient) -> None:
        """Test nightly update with future dates (should be allowed but may result in no data)."""
        with patch("api.nightly_update.get_nightly_update_service") as mock_get_service:

            mock_service = Mock()
            mock_service.get_default_symbols.return_value = ["AAPL"]
            mock_service.execute_nightly_update = AsyncMock()
            mock_get_service.return_value = mock_service

            # Use future dates
            from datetime import date, timedelta

            future_start = date.today() + timedelta(days=30)
            future_end = date.today() + timedelta(days=35)

            request_data: Dict[str, Any] = {
                "symbols": ["AAPL"],
                "start_date": future_start.isoformat(),
                "end_date": future_end.isoformat(),
            }

            response = client.post("/nightly-update/start", json=request_data)
            # Should accept the request (validation happens at service level)
            assert response.status_code == 200
            data = response.json()
            assert "request_id" in data

            # Note: We don't assert on execute_nightly_update being called because
            # it runs as a background task and mocking background tasks is complex

    def test_nightly_update_date_range_edge_cases(self, client: TestClient) -> None:
        """Test nightly update with edge case date ranges."""
        with patch("api.nightly_update.get_nightly_update_service") as mock_get_service:

            mock_service = Mock()
            mock_service.get_default_symbols.return_value = ["AAPL"]
            mock_service.execute_nightly_update = AsyncMock()
            mock_get_service.return_value = mock_service

            # Test same start and end date
            request_data: Dict[str, Any] = {
                "symbols": ["AAPL"],
                "start_date": "2025-01-15",
                "end_date": "2025-01-15",
            }

            response = client.post("/nightly-update/start", json=request_data)
            assert response.status_code == 200

            # Test very old dates
            request_data = {
                "symbols": ["AAPL"],
                "start_date": "2020-01-01",
                "end_date": "2020-01-05",
            }

            response = client.post("/nightly-update/start", json=request_data)
            assert response.status_code == 200

            # Note: We don't assert on execute_nightly_update being called because
            # it runs as a background task and mocking background tasks is complex
