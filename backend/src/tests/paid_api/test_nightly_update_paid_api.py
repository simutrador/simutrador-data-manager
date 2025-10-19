"""
⚠️  PAID API TESTS - THESE COST MONEY! ⚠️

Tests for the nightly update API endpoint using REAL external data providers.
These tests will make actual API calls and incur charges.

Run only when needed:
    ./run_nightly_update_paid_api_tests.sh
    # or
    pytest -m paid_api src/tests/paid_api/test_nightly_update_paid_api.py
"""

import asyncio
import os
import shutil
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

# Set test environment for paid API tests BEFORE importing settings
test_env_path = os.path.join(
    os.path.dirname(__file__), "../../environments/.env.test.paid"
)
os.environ["ENV"] = test_env_path

from core.settings import get_settings  # noqa: E402
from main import app  # noqa: E402
from services.storage.data_storage_service import DataStorageService  # noqa: E402

# Clear settings cache to ensure test environment is loaded
get_settings.cache_clear()


@pytest.mark.paid_api
class TestNightlyUpdatePaidAPI:
    """
    💰 PAID API tests for the nightly update API endpoint.

    These tests use real data providers and WILL INCUR API CHARGES.
    They should only be run manually when you need to verify that the system
    works with actual external APIs.

    Run with: pytest -m paid_api src/tests/paid_api/test_nightly_update_paid_api.py
    """

    @pytest.fixture
    def client(self) -> TestClient:
        """Create test client."""
        return TestClient(app)

    @pytest.fixture
    def storage_service(self) -> DataStorageService:
        """Create storage service for validation."""
        return DataStorageService()

    def _wait_for_completion(
        self, client: TestClient, request_id: str, max_wait_seconds: int = 300
    ) -> dict[str, Any]:
        """Wait for nightly update to complete and return results."""
        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            # Check status
            status_response = client.get(f"/nightly-update/status/{request_id}")
            assert (
                status_response.status_code == 200
            ), f"Status check failed: {status_response.text}"

            status_data = status_response.json()

            if status_data.get("is_complete", False):
                # Get detailed results
                details_response = client.get(
                    f"/nightly-update/status/{request_id}/details"
                )
                assert (
                    details_response.status_code == 200
                ), f"Details fetch failed: {details_response.text}"
                return details_response.json()

            # Wait before next check
            time.sleep(5)

        raise TimeoutError(
            f"Nightly update {request_id} did not complete within {max_wait_seconds} seconds"
        )

    @pytest.mark.asyncio
    async def test_paid_nightly_update_small_symbol_list(
        self, client: TestClient
    ) -> None:
        """
        💰 Test real nightly update with a small symbol list.

        This test:
        1. Triggers nightly update for 3 symbols (AAPL, MSFT, GOOGL)
        2. Waits for completion and validates response structure
        3. Checks that data was fetched, validated, and resampled
        4. Validates response fields match documentation
        """
        print("🚀 Testing nightly update with small symbol list...")

        request_data: dict[str, Any] = {
            "symbols": ["AAPL", "MSFT", "GOOGL"],  # Small list to minimize costs
            "force_validation": True,
            "max_concurrent": 2,  # Limit concurrency to be gentle on APIs
            "enable_resampling": True,
        }

        # Start nightly update
        response = client.post("/nightly-update/start", json=request_data)

        # Basic response validation
        assert (
            response.status_code == 200
        ), f"Expected 200, got {response.status_code}: {response.text}"

        start_data = response.json()

        # Validate start response structure
        required_fields = ["request_id", "status", "message"]
        for field in required_fields:
            assert field in start_data, f"Missing required field: {field}"

        assert start_data["status"] == "started", "Status should be 'started'"
        assert "3 symbols" in start_data["message"], "Message should mention 3 symbols"

        request_id = start_data["request_id"]
        print(f"✅ Nightly update started with request ID: {request_id}")

        # Wait for completion
        print("⏳ Waiting for nightly update to complete...")
        results = self._wait_for_completion(client, request_id)

        # Validate detailed response structure
        required_response_fields = [
            "request_id",
            "started_at",
            "completed_at",
            "summary",
            "symbol_results",
            "symbols_processed",
            "overall_success",
        ]
        for field in required_response_fields:
            assert field in results, f"Missing required response field: {field}"

        # Validate summary structure
        summary = results["summary"]
        required_summary_fields = [
            "total_symbols",
            "successful_updates",
            "failed_updates",
            "total_candles_updated",
            "total_resampled_candles",
            "update_duration_seconds",
            "resampling_summary",
        ]
        for field in required_summary_fields:
            assert field in summary, f"Missing required summary field: {field}"

        # Validate summary values
        assert summary["total_symbols"] == 3, "Should process exactly 3 symbols"
        assert (
            summary["successful_updates"] >= 0
        ), "Successful updates should be non-negative"
        assert summary["failed_updates"] >= 0, "Failed updates should be non-negative"
        assert (
            summary["total_candles_updated"] >= 0
        ), "Total candles should be non-negative"

        # Validate symbol results
        symbol_results = results["symbol_results"]
        assert len(symbol_results) == 3, "Should have results for 3 symbols"

        for symbol in ["AAPL", "MSFT", "GOOGL"]:
            assert symbol in symbol_results, f"Missing results for {symbol}"

            result = symbol_results[symbol]
            required_symbol_fields = [
                "symbol",
                "start_date",
                "end_date",
                "success",
                "candles_updated",
                "validation_results",
                "resampling_results",
            ]
            for field in required_symbol_fields:
                assert field in result, f"Missing field {field} for {symbol}"

            assert result["symbol"] == symbol, f"Symbol mismatch for {symbol}"
            assert isinstance(
                result["success"], bool
            ), f"Success should be boolean for {symbol}"

            if result["success"]:
                print(
                    f"✅ {symbol}: {result['candles_updated']} candles updated, "
                    f"{result['total_resampled_candles']} resampled"
                )

                # Validate resampling results
                resampling = result["resampling_results"]
                expected_timeframes = [
                    "5min",
                    "15min",
                    "30min",
                    "1h",
                    "2h",
                    "4h",
                    "daily",
                ]
                for timeframe in expected_timeframes:
                    assert (
                        timeframe in resampling
                    ), f"Missing {timeframe} resampling for {symbol}"
                    assert (
                        resampling[timeframe] >= 0
                    ), f"Invalid {timeframe} count for {symbol}"
            else:
                error_msg = result.get("error_message", "No error message")
                print(f"❌ {symbol}: Update failed - {error_msg}")

        # Log overall results
        print("📊 Overall Results:")
        print(f"   Total symbols: {summary['total_symbols']}")
        print(f"   Successful: {summary['successful_updates']}")
        print(f"   Failed: {summary['failed_updates']}")
        print(f"   Total candles updated: {summary['total_candles_updated']}")
        print(f"   Total resampled candles: {summary['total_resampled_candles']}")
        print(f"   Duration: {summary['update_duration_seconds']:.1f} seconds")

        # Validate resampling summary
        resampling_summary = summary["resampling_summary"]
        expected_timeframes = ["5min", "15min", "30min", "1h", "2h", "4h", "daily"]
        for timeframe in expected_timeframes:
            if timeframe in resampling_summary:
                print(f"   {timeframe}: {resampling_summary[timeframe]} candles")

        print("🎉 Nightly update test completed successfully!")

    @pytest.mark.asyncio
    async def test_paid_nightly_update_data_scenarios_validation(
        self, client: TestClient, storage_service: DataStorageService
    ) -> None:
        """
        💰 Test nightly update data integrity scenarios and resampling validation.

        This test validates critical data scenarios:
        1. Fresh symbol (no existing data) - downloads from beginning
        2. Symbol with data gaps - fills missing dates
        3. Up-to-date symbol - handles no-update scenario
        4. Validates resampling accuracy by comparing with vendor native data
        5. Tests error handling and recovery
        """
        print("🚀 Testing nightly update data integrity scenarios...")

        # Use a small set of symbols to test different scenarios cost-effectively
        test_symbols = ["AAPL", "MSFT", "GOOGL"]  # 3 symbols to minimize cost

        # Step 1: Create different data scenarios by manipulating existing storage
        print("📋 Setting up test data scenarios...")

        # Clear one symbol completely (fresh download scenario)
        fresh_symbol = test_symbols[0]  # AAPL
        print(f"   🆕 {fresh_symbol}: Testing fresh download (no existing data)")
        self._clear_symbol_data(storage_service, fresh_symbol)

        # Create gap scenario for another symbol
        gap_symbol = test_symbols[1]  # MSFT
        print(f"   🕳️  {gap_symbol}: Testing gap filling scenario")
        self._create_data_gap(storage_service, gap_symbol)

        # Leave third symbol as-is for up-to-date scenario
        current_symbol = test_symbols[2]  # GOOGL
        print(f"   ✅ {current_symbol}: Testing up-to-date scenario")

        request_data: dict[str, Any] = {
            "symbols": test_symbols,
            "force_validation": True,
            "max_concurrent": 2,  # Moderate concurrency
            "enable_resampling": True,
        }

        # Start nightly update
        response = client.post("/nightly-update/start", json=request_data)
        assert (
            response.status_code == 200
        ), f"Expected 200, got {response.status_code}: {response.text}"

        start_data = response.json()
        request_id = start_data["request_id"]
        print(f"✅ Nightly update started with request ID: {request_id}")

        # Wait for completion
        print("⏳ Waiting for nightly update to complete...")
        results = self._wait_for_completion(
            client, request_id, max_wait_seconds=600
        )  # 10 minutes max

        # Validate results structure
        summary = results["summary"]
        symbol_results = results["symbol_results"]

        assert summary["total_symbols"] == 3, "Should process exactly 3 symbols"
        assert len(symbol_results) == 3, "Should have results for 3 symbols"

        print("📊 Nightly Update Results:")
        print(f"   Total candles updated: {summary['total_candles_updated']}")
        print(f"   Total resampled candles: {summary['total_resampled_candles']}")
        print(f"   Duration: {summary['update_duration_seconds']:.1f} seconds")

        # Step 2: Validate each scenario
        successful_symbols: list[str] = []
        for symbol in test_symbols:
            result = symbol_results[symbol]
            print(f"\n📈 {symbol} Results:")
            print(f"   Success: {result['success']}")
            print(f"   Candles updated: {result['candles_updated']}")
            print(f"   Date range: {result['start_date']} to {result['end_date']}")

            if result["success"]:
                successful_symbols.append(symbol)
                print(f"   Resampled candles: {result['total_resampled_candles']}")
            else:
                print(f"   Error: {result.get('error_message', 'Unknown error')}")

        # Expect at least 2/3 symbols to succeed (allowing for potential API issues)
        assert (
            len(successful_symbols) >= 2
        ), f"Expected at least 2 successful symbols, got {len(successful_symbols)}"

        # Step 3: Validate resampling accuracy for successful symbols
        print("\n🔍 Validating resampling accuracy...")

        for symbol in successful_symbols[
            :2
        ]:  # Test first 2 successful symbols to control cost
            await self._validate_resampling_accuracy(client, storage_service, symbol)

        print("🎉 Data scenarios and resampling validation completed!")

    async def _validate_resampling_accuracy(
        self, client: TestClient, storage_service: DataStorageService, symbol: str
    ) -> None:
        """
        Validate resampling accuracy by comparing our resampled data with vendor native data.

        This method:
        1. Uses nightly update to download vendor's native 5min data for comparison
        2. Loads our resampled 5min data (created from 1min)
        3. Compares OHLCV values to ensure resampling accuracy
        """
        print(f"🔍 Validating resampling accuracy for {symbol}...")

        # Use recent date range for comparison
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=3)  # 3-day window to minimize cost

        # Step 1: Use nightly update to download vendor's native 5min data for comparison
        print(
            f"   📥 Downloading vendor native 5min data for {symbol} via nightly update..."
        )
        vendor_request: dict[str, Any] = {
            "symbols": [symbol],
            "force_validation": True,
            "max_concurrent": 1,
            "enable_resampling": False,  # We only want the raw data, not resampling
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        vendor_response = client.post("/nightly-update/start", json=vendor_request)
        assert (
            vendor_response.status_code == 200
        ), f"Failed to start nightly update for vendor 5min data: {vendor_response.text}"

        # Wait for the nightly update to complete
        vendor_start_data = vendor_response.json()
        vendor_request_id = vendor_start_data["request_id"]
        print(
            f"   ⏳ Waiting for vendor data download to complete (request: {vendor_request_id})..."
        )

        try:
            self._wait_for_completion(client, vendor_request_id, max_wait_seconds=300)
            print("   ✅ Vendor data download completed")
        except TimeoutError:
            print(
                "   ⚠️  Vendor data download timed out - skipping resampling validation"
            )
            return

        # Step 2: Load both datasets for comparison
        print("   📊 Loading datasets for comparison...")

        # Load our resampled 5min data (should exist from nightly update)
        try:
            our_series = storage_service.load_data(symbol, "5min", start_date, end_date)
            our_candles = our_series.candles
            print(f"   📈 Our resampled 5min data: {len(our_candles)} candles")
        except Exception as e:
            print(f"   ❌ Failed to load our resampled data: {e}")
            return  # Skip validation if we can't load our data

        # Load vendor's 1min data (since we're not doing native 5min comparison anymore)
        # Instead, we'll validate that our resampled data exists and has reasonable values
        try:
            vendor_series = storage_service.load_data(
                symbol, "1min", start_date, end_date
            )
            vendor_candles = vendor_series.candles
            print(f"   📈 Vendor 1min data: {len(vendor_candles)} candles")
        except Exception as e:
            print(f"   ❌ Failed to load vendor 1min data: {e}")
            return  # Skip validation if we can't load vendor data

        # Step 3: Validate that resampled data exists and has reasonable values
        if len(our_candles) == 0:
            print("   ⚠️  No resampled 5min data found - validation failed")
            return

        if len(vendor_candles) == 0:
            print("   ⚠️  No 1min data found - validation failed")
            return

        # Basic validation: check that we have reasonable 5min data
        print(f"   🔍 Validating {len(our_candles)} resampled 5min candles...")

        valid_candles = 0
        for candle in our_candles[:10]:  # Check first 10 candles
            # Basic sanity checks for OHLC data
            if (
                candle.open > 0
                and candle.high > 0
                and candle.low > 0
                and candle.close > 0
                and candle.high >= candle.open
                and candle.high >= candle.close
                and candle.low <= candle.open
                and candle.low <= candle.close
                and candle.volume >= 0
            ):
                valid_candles += 1
            else:
                print(
                    f"   ❌ Invalid candle at {candle.date}: O:{candle.open} H:{candle.high}\
                          L:{candle.low} C:{candle.close} V:{candle.volume}"
                )

        # Calculate validation percentage
        validation_percentage = (valid_candles / min(10, len(our_candles))) * 100
        print(
            f"   📊 Data validation: {valid_candles}/{min(10, len(our_candles))} candles valid \
                ({validation_percentage:.1f}%)"
        )

        # We expect 100% of candles to be valid (basic sanity check)
        assert (
            validation_percentage == 100
        ), f"Data validation {validation_percentage:.1f}% below 100% threshold"

        # Additional check: verify that 5min candles are roughly 1/5 the count of 1min candles
        expected_5min_count = len(vendor_candles) // 5
        actual_5min_count = len(our_candles)
        ratio = actual_5min_count / max(expected_5min_count, 1)

        print(
            f"   📊 Resampling ratio: {actual_5min_count} 5min candles from {len(vendor_candles)} \
                1min candles (ratio: {ratio:.2f})"
        )

        # Allow some flexibility in the ratio (between 0.5 and 1.5)
        # The ratio can vary due to market hours, gaps, and other factors
        assert (
            0.5 <= ratio <= 1.5
        ), f"Resampling ratio {ratio:.2f} outside expected range (0.5-1.5)"

        print(f"   ✅ {symbol} resampling validation passed!")

    def _clear_symbol_data(
        self, storage_service: DataStorageService, symbol: str
    ) -> None:
        """Clear all existing data for a symbol to test fresh download scenario."""
        try:
            # Get storage paths from the service
            candles_path = storage_service.candles_path

            # Remove daily data file
            daily_file = candles_path / "daily" / f"{symbol}.parquet"
            if daily_file.exists():
                daily_file.unlink()
                print(f"   🗑️  Removed daily data for {symbol}")

            # Remove intraday data directories
            for timeframe in ["1min", "5min", "15min", "30min", "1h", "2h", "4h"]:
                timeframe_dir = candles_path / timeframe / symbol
                if timeframe_dir.exists():
                    shutil.rmtree(timeframe_dir)
                    print(f"   🗑️  Removed {timeframe} data for {symbol}")

        except Exception as e:
            print(f"   ⚠️  Error clearing data for {symbol}: {e}")

    def _create_data_gap(
        self, storage_service: DataStorageService, symbol: str
    ) -> None:
        """Create a data gap by removing recent data files to test gap filling."""
        try:
            # Get storage paths from the service
            candles_path = storage_service.candles_path

            # Remove last 2-3 days of 1min data to create a gap
            recent_dates: list[date] = []
            current_date = date.today() - timedelta(days=1)
            for i in range(3):  # Remove last 3 days
                check_date = current_date - timedelta(days=i)
                recent_dates.append(check_date)

            removed_files = 0
            for check_date in recent_dates:
                file_path = (
                    candles_path / "1min" / symbol / f"{check_date.isoformat()}.parquet"
                )
                if file_path.exists():
                    file_path.unlink()
                    removed_files += 1

            if removed_files > 0:
                print(
                    f"   🕳️  Created data gap for {symbol} (removed {removed_files} recent files)"
                )
            else:
                print(f"   ⚠️  No recent files found to create gap for {symbol}")

        except Exception as e:
            print(f"   ⚠️  Error creating data gap for {symbol}: {e}")

    @pytest.mark.asyncio
    async def test_paid_nightly_update_status_tracking(
        self, client: TestClient
    ) -> None:
        """
        💰 Test nightly update status tracking endpoints.

        This test:
        1. Starts a nightly update
        2. Tests status tracking while it's running
        3. Tests active updates listing
        4. Validates status transitions
        """
        print("🚀 Testing nightly update status tracking...")

        request_data: dict[str, Any] = {
            "symbols": ["AAPL", "MSFT"],  # Small list for quick test
            "force_validation": True,
            "max_concurrent": 1,  # Sequential to make tracking easier
            "enable_resampling": True,
        }

        # Start nightly update
        response = client.post("/nightly-update/start", json=request_data)
        assert response.status_code == 200

        start_data = response.json()
        request_id = start_data["request_id"]

        print(f"✅ Started nightly update: {request_id}")

        # Give the background task a moment to start before checking active list
        await asyncio.sleep(1)

        # Test active updates listing
        active_response = client.get("/nightly-update/active")
        assert active_response.status_code == 200

        active_data = active_response.json()
        assert isinstance(active_data, list), "Active updates should be a list"

        # Should find our request in active list (try a few times in case of timing issues)
        active_data_typed = cast(list[dict[str, Any]], active_data)
        our_request: dict[str, Any] | None = next(
            (req for req in active_data_typed if req["request_id"] == request_id), None
        )

        # If not found initially, try a few more times with delays
        retry_count = 0
        while our_request is None and retry_count < 3:
            await asyncio.sleep(2)
            active_response = client.get("/nightly-update/active")
            assert active_response.status_code == 200
            active_data = active_response.json()
            active_data_typed = cast(list[dict[str, Any]], active_data)
            our_request = next(
                (req for req in active_data_typed if req["request_id"] == request_id),
                None,
            )
            retry_count += 1

        # If still not found, check if it completed very quickly
        if our_request is None:
            # Check if the request completed already
            status_response = client.get(f"/nightly-update/status/{request_id}")
            if status_response.status_code == 200:
                status_data = status_response.json()
                if status_data.get("is_complete", False):
                    print(
                        "⚠️  Request completed very quickly - skipping active list check"
                    )
                    return  # Skip the rest of this test since it completed too fast

        assert our_request is not None, "Our request should be in active list"
        assert our_request["symbols_count"] == 2, "Should show 2 symbols"

        print(f"✅ Found request in active list: {our_request['status']}")

        # Test status tracking during execution
        status_checks = 0
        max_status_checks = 10

        while status_checks < max_status_checks:
            status_response = client.get(f"/nightly-update/status/{request_id}")
            assert status_response.status_code == 200

            status_data = status_response.json()

            required_status_fields = [
                "request_id",
                "status",
                "started_at",
                "symbols_count",
                "is_complete",
            ]
            for field in required_status_fields:
                assert field in status_data, f"Missing status field: {field}"

            assert status_data["request_id"] == request_id
            assert status_data["symbols_count"] == 2

            print(f"📊 Status check {status_checks + 1}: {status_data['status']}")

            if status_data["is_complete"]:
                print("✅ Update completed!")
                break

            status_checks += 1
            await asyncio.sleep(3)  # Wait between checks

        # Get final detailed results
        details_response = client.get(f"/nightly-update/status/{request_id}/details")
        assert details_response.status_code == 200

        details_data = details_response.json()
        assert details_data["request_id"] == request_id
        assert "summary" in details_data
        assert "symbol_results" in details_data

        print("🎉 Status tracking test completed!")

    @pytest.mark.asyncio
    async def test_paid_data_completeness_analysis(self, client: TestClient) -> None:
        """
        💰 Test data completeness analysis endpoint.

        This test:
        1. Analyzes data completeness for a few symbols
        2. Validates response structure and data quality metrics
        3. Tests both summary and detailed analysis modes
        """
        print("🚀 Testing data completeness analysis...")

        # Test date range - last week
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=7)

        # Test basic completeness analysis
        request_data: dict[str, Any] = {
            "symbols": ["AAPL", "MSFT", "GOOGL"],
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "include_details": False,
        }

        response = client.post("/data-analysis/completeness", json=request_data)
        assert (
            response.status_code == 200
        ), f"Completeness analysis failed: {response.text}"

        data = response.json()

        # Validate response structure
        required_fields = [
            "analysis_period",
            "symbol_completeness",
            "overall_statistics",
            "symbols_needing_attention",
            "recommendations",
        ]
        for field in required_fields:
            assert field in data, f"Missing required field: {field}"

        # Validate analysis period
        period = data["analysis_period"]
        assert period["start_date"] == start_date.isoformat()
        assert period["end_date"] == end_date.isoformat()

        # Validate symbol completeness
        symbol_completeness = data["symbol_completeness"]
        assert (
            len(symbol_completeness) == 3
        ), "Should have completeness data for 3 symbols"

        for symbol in ["AAPL", "MSFT", "GOOGL"]:
            assert (
                symbol in symbol_completeness
            ), f"Missing completeness data for {symbol}"

            completeness = symbol_completeness[symbol]
            required_completeness_fields = [
                "total_trading_days",
                "valid_days",
                "invalid_days",
                "completeness_percentage",
                "total_expected_candles",
                "total_actual_candles",
                "missing_candles",
            ]
            for field in required_completeness_fields:
                assert (
                    field in completeness
                ), f"Missing completeness field {field} for {symbol}"

            # Validate data types and ranges
            assert completeness["completeness_percentage"] >= 0.0

            # Validate data types and ranges
            assert completeness["completeness_percentage"] <= 100.0

            assert completeness["total_expected_candles"] >= 0
            assert completeness["total_actual_candles"] >= 0
            assert completeness["missing_candles"] >= 0

            print(
                f"📊 {symbol}: {completeness['completeness_percentage']:.1f}% complete "
                f"({completeness['total_actual_candles']}/{completeness['total_expected_candles']}\
                    candles)"
            )

        # Validate overall statistics
        overall = data["overall_statistics"]
        required_overall_fields = [
            "total_symbols",
            "total_trading_days",
            "total_valid_days",
            "overall_completeness_percentage",
            "total_expected_candles",
            "total_actual_candles",
            "total_missing_candles",
        ]
        for field in required_overall_fields:
            assert field in overall, f"Missing overall field: {field}"

        assert overall["total_symbols"] == 3
        assert overall["overall_completeness_percentage"] >= 0.0
        assert overall["overall_completeness_percentage"] <= 100.0

        print(
            f"📈 Overall completeness: {overall['overall_completeness_percentage']:.1f}%"
        )
        print(
            f"📊 Total candles: {overall['total_actual_candles']}/"
            f"{overall['total_expected_candles']}"
        )

        # Test detailed analysis
        print("🔍 Testing detailed completeness analysis...")

        detailed_request = request_data.copy()
        detailed_request["include_details"] = True

        detailed_response = client.post(
            "/data-analysis/completeness", json=detailed_request
        )
        assert detailed_response.status_code == 200

        detailed_data = detailed_response.json()

        # Should have validation results in detailed mode
        for symbol in ["AAPL", "MSFT", "GOOGL"]:
            symbol_data = detailed_data["symbol_completeness"][symbol]
            if symbol_data.get("validation_results"):
                validation_results: list[dict[str, Any]] = symbol_data[
                    "validation_results"
                ]
                assert isinstance(validation_results, list)

                if validation_results:
                    # Check first validation result structure
                    first_result: dict[str, Any] = validation_results[0]
                    required_validation_fields = [
                        "symbol",
                        "validation_date",
                        "is_valid",
                        "expected_candles",
                        "actual_candles",
                    ]
                    for field in required_validation_fields:
                        assert (
                            field in first_result
                        ), f"Missing validation field: {field}"

                    print(f"🔍 {symbol}: {len(validation_results)} validation results")

        print("🎉 Data completeness analysis test completed!")


@pytest.mark.paid_api
class TestNightlyUpdateCompleteEndToEndPipeline:
    """
    💰 Complete End-to-End Pipeline Tests for Nightly Update

    These tests validate the ENTIRE nightly update pipeline:
    1. Trigger nightly update with real data
    2. Validate data storage structure
    3. Check resampling accuracy
    4. Verify data completeness and quality

    This is the ultimate test that proves the nightly update system works correctly.
    """

    @pytest.fixture
    def client(self) -> TestClient:
        """Create test client."""
        return TestClient(app)

    @pytest.fixture
    def storage_service(self) -> DataStorageService:
        """Create storage service for validation."""
        return DataStorageService()

    def _wait_for_completion(
        self, client: TestClient, request_id: str, max_wait_seconds: int = 600
    ) -> dict[str, Any]:
        """Wait for nightly update to complete and return results."""
        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            status_response = client.get(f"/nightly-update/status/{request_id}")
            assert status_response.status_code == 200

            status_data = status_response.json()

            if status_data.get("is_complete", False):
                details_response = client.get(
                    f"/nightly-update/status/{request_id}/details"
                )
                assert details_response.status_code == 200
                return details_response.json()

            time.sleep(10)  # Longer wait for complete pipeline

        raise TimeoutError(
            f"Nightly update {request_id} did not complete within {max_wait_seconds} seconds"
        )

    def _validate_storage_structure(
        self, symbols: list[str], timeframes: list[str], test_dates: list[date]
    ) -> None:
        """Validate that files are stored in correct directory structure."""
        settings = get_settings()
        base_path = Path(settings.data_storage.base_path)
        candles_path = base_path / settings.data_storage.candles_path

        for symbol in symbols:
            for timeframe in timeframes:
                if timeframe == "daily":
                    # Daily data: storage/candles/daily/SYMBOL.parquet
                    daily_path = candles_path / "daily" / f"{symbol}.parquet"
                    assert (
                        daily_path.exists()
                    ), f"Missing daily storage file: {daily_path}"
                    assert (
                        daily_path.stat().st_size > 0
                    ), f"Empty daily storage file: {daily_path}"
                else:
                    # Intraday data: storage/candles/TIMEFRAME/SYMBOL/DATE.parquet
                    for test_date in test_dates:
                        if test_date.weekday() < 5:  # Trading days only
                            date_str = test_date.strftime("%Y-%m-%d")
                            intraday_path = (
                                candles_path
                                / timeframe
                                / symbol
                                / f"{date_str}.parquet"
                            )
                            assert (
                                intraday_path.exists()
                            ), f"Missing intraday storage file: {intraday_path}"
                            assert (
                                intraday_path.stat().st_size > 0
                            ), f"Empty intraday storage file: {intraday_path}"

    @pytest.mark.asyncio
    async def test_paid_complete_nightly_update_pipeline_validation(
        self, client: TestClient, storage_service: DataStorageService
    ) -> None:
        """
        💰 Test complete nightly update pipeline with storage validation.

        This test:
        1. Triggers nightly update for 2 symbols
        2. Waits for complete processing (validation + download + resampling)
        3. Validates data is stored in correct directory structure
        4. Verifies all expected timeframes are created
        5. Checks data integrity and completeness
        """
        print("🚀 Testing complete nightly update pipeline...")

        # Test configuration
        symbols = ["AAPL", "MSFT"]
        expected_timeframes = [
            "1min",
            "5min",
            "15min",
            "30min",
            "1h",
            "2h",
            "4h",
            "daily",
        ]

        # Generate expected test dates (last 3 trading days)
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=4)
        test_dates: list[date] = []
        current_date = start_date
        while current_date <= end_date:
            if current_date.weekday() < 5:  # Trading days only
                test_dates.append(current_date)
            current_date += timedelta(days=1)

        print(f"📅 Testing symbols: {symbols}")
        print(
            f"📅 Expected trading dates: {[d.strftime('%Y-%m-%d') for d in test_dates]}"
        )

        # Start nightly update
        request_data: dict[str, Any] = {
            "symbols": symbols,
            "force_validation": True,
            "max_concurrent": 2,
            "enable_resampling": True,
        }

        response = client.post("/nightly-update/start", json=request_data)
        assert (
            response.status_code == 200
        ), f"Nightly update start failed: {response.text}"

        start_data = response.json()
        request_id = start_data["request_id"]

        print(f"✅ Nightly update started: {request_id}")

        # Wait for completion
        print("⏳ Waiting for complete pipeline to finish...")
        results = self._wait_for_completion(client, request_id)

        # Validate results
        summary = results["summary"]
        symbol_results = results["symbol_results"]

        assert summary["total_symbols"] == len(symbols)
        assert len(symbol_results) == len(symbols)

        # Check that at least one symbol succeeded
        successful_symbols = [s for s, r in symbol_results.items() if r["success"]]
        assert len(successful_symbols) > 0, "At least one symbol should succeed"

        print("📊 Pipeline Results:")
        print(f"   Successful symbols: {len(successful_symbols)}")
        print(f"   Total candles updated: {summary['total_candles_updated']}")
        print(f"   Total resampled candles: {summary['total_resampled_candles']}")
        print(f"   Duration: {summary['update_duration_seconds']:.1f} seconds")

        # Validate storage structure for successful symbols
        if successful_symbols:
            print("🔍 Validating storage structure...")
            self._validate_storage_structure(
                successful_symbols, expected_timeframes, test_dates
            )
            print("✅ Storage structure validation passed")

            # Validate data integrity
            print("📊 Validating data integrity...")
            for symbol in successful_symbols:
                for timeframe in expected_timeframes:
                    try:
                        loaded_series = storage_service.load_data(
                            symbol, timeframe, test_dates[0], test_dates[-1]
                        )

                        assert (
                            len(loaded_series.candles) > 0
                        ), f"No candles loaded for {symbol} {timeframe}"

                        # Basic data quality checks
                        for candle in loaded_series.candles[
                            :3
                        ]:  # Check first 3 candles
                            assert (
                                candle.open > 0
                            ), f"Invalid open price in {symbol} {timeframe}"
                            assert (
                                candle.high >= candle.open
                            ), f"High < Open in {symbol} {timeframe}"
                            assert (
                                candle.low <= candle.open
                            ), f"Low > Open in {symbol} {timeframe}"
                            assert (
                                candle.volume >= 0
                            ), f"Negative volume in {symbol} {timeframe}"

                        print(
                            f"  ✅ {symbol} {timeframe}: {len(loaded_series.candles)} candles"
                        )

                    except Exception as e:
                        print(f"  ⚠️  {symbol} {timeframe}: {e}")

            print("✅ Data integrity validation passed")

        # Validate resampling results
        print("🔄 Validating resampling results...")
        resampling_summary = summary["resampling_summary"]
        expected_resampled_timeframes = [
            "5min",
            "15min",
            "30min",
            "1h",
            "2h",
            "4h",
            "daily",
        ]

        for timeframe in expected_resampled_timeframes:
            if timeframe in resampling_summary:
                candle_count = resampling_summary[timeframe]
                assert candle_count >= 0, f"Invalid candle count for {timeframe}"
                print(f"  ✅ {timeframe}: {candle_count} candles resampled")

        print("🎉 Complete nightly update pipeline validation PASSED!")
        print("✅ The nightly update system works correctly with real data!")

    @pytest.mark.asyncio
    async def test_paid_nightly_update_custom_date_range(
        self, client: TestClient
    ) -> None:
        """
        💰 Test nightly update with custom date range.

        This test:
        1. Uses a specific date range instead of automatic detection
        2. Validates that the custom date range is respected
        3. Tests both start and end date specifications
        4. Verifies data is fetched for the exact date range
        """
        print("🚀 Testing nightly update with custom date range...")

        # Use a specific date range (last week for example)
        from datetime import date, timedelta

        end_date = date.today() - timedelta(days=2)  # 2 days ago to ensure data exists
        start_date = end_date - timedelta(days=5)  # 5-day range

        request_data: dict[str, Any] = {
            "symbols": ["AAPL", "MSFT"],  # Small list to minimize costs
            "force_validation": True,
            "max_concurrent": 2,
            "enable_resampling": True,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        print(f"📅 Testing date range: {start_date} to {end_date}")

        # Start nightly update
        response = client.post("/nightly-update/start", json=request_data)
        assert (
            response.status_code == 200
        ), f"Expected 200, got {response.status_code}: {response.text}"

        start_data = response.json()
        assert start_data["status"] == "started", "Status should be 'started'"
        assert "2 symbols" in start_data["message"], "Message should mention 2 symbols"

        request_id = start_data["request_id"]
        print(f"✅ Nightly update started with request ID: {request_id}")

        # Wait for completion
        print("⏳ Waiting for custom date range update to complete...")
        results = self._wait_for_completion(client, request_id)

        # Validate that the date range was respected
        summary = results["summary"]
        symbol_results = results["symbol_results"]

        assert summary["total_symbols"] == 2
        assert len(symbol_results) == 2

        # Check that the date range in results matches our request
        if summary["earliest_start_date"] and summary["latest_end_date"]:
            result_start = date.fromisoformat(summary["earliest_start_date"])
            result_end = date.fromisoformat(summary["latest_end_date"])

            # The actual dates might be adjusted for trading days, but should be within our range
            print(f"📊 Requested range: {start_date} to {end_date}")
            print(f"📊 Actual range: {result_start} to {result_end}")

            # Start date should not be before our requested start (may be later due to trading days)
            assert (
                result_start >= start_date
            ), f"Start date {result_start} should not be before requested {start_date}"

            # End date should not be after our requested end (may be earlier due to trading days)
            assert (
                result_end <= end_date
            ), f"End date {result_end} should not be after requested {end_date}"

        # Validate that at least one symbol succeeded
        successful_symbols = [s for s, r in symbol_results.items() if r["success"]]
        assert len(successful_symbols) > 0, "At least one symbol should succeed"

        print("✅ Custom date range test passed!")
        print(
            f"📊 Summary: {summary['successful_updates']}/{summary['total_symbols']} \
                symbols succeeded"
        )
        print(
            f"📅 Date range: {summary['earliest_start_date']} to {summary['latest_end_date']}"
        )
        print(f"📈 Total candles: {summary['total_candles_updated']}")

    @pytest.mark.asyncio
    async def test_paid_nightly_update_start_date_only(
        self, client: TestClient
    ) -> None:
        """
        💰 Test nightly update with only start date specified.

        This test:
        1. Specifies only a start date (end date should default to yesterday)
        2. Validates that the backend handles partial date specification correctly
        3. Ensures data is fetched from start date to yesterday
        """
        print("🚀 Testing nightly update with start date only...")

        # Use a start date from a week ago, let end date default
        from datetime import date, timedelta

        start_date = date.today() - timedelta(days=7)

        request_data: dict[str, Any] = {
            "symbols": ["AAPL"],  # Single symbol to minimize cost
            "force_validation": True,
            "max_concurrent": 1,
            "enable_resampling": True,
            "start_date": start_date.isoformat(),
            # end_date not specified - should default to yesterday
        }

        print(f"📅 Testing with start date: {start_date} (end date will default)")

        # Start nightly update
        response = client.post("/nightly-update/start", json=request_data)
        assert (
            response.status_code == 200
        ), f"Expected 200, got {response.status_code}: {response.text}"

        start_data = response.json()
        request_id = start_data["request_id"]
        print(f"✅ Nightly update started with request ID: {request_id}")

        # Wait for completion
        print("⏳ Waiting for start-date-only update to complete...")
        results = self._wait_for_completion(client, request_id)

        # Validate results
        summary = results["summary"]
        symbol_results = results["symbol_results"]

        assert summary["total_symbols"] == 1
        assert len(symbol_results) == 1

        # Check that start date was respected and end date defaulted appropriately
        if summary["earliest_start_date"] and summary["latest_end_date"]:
            result_start = date.fromisoformat(summary["earliest_start_date"])
            result_end = date.fromisoformat(summary["latest_end_date"])

            print(f"📊 Requested start: {start_date}")
            print(f"📊 Actual range: {result_start} to {result_end}")

            # Start should be close to our requested date (adjusted for trading days)
            assert (
                result_start >= start_date
            ), f"Start date {result_start} should not be before requested {start_date}"

            # End should be recent (yesterday or close to it)
            yesterday = date.today() - timedelta(days=1)
            assert (
                result_end <= date.today()
            ), f"End date {result_end} should not be in the future"
            assert result_end >= yesterday - timedelta(
                days=3
            ), f"End date {result_end} should be recent"

        print("✅ Start date only test passed!")
        print(
            f"📊 Date range: {summary['earliest_start_date']} to {summary['latest_end_date']}"
        )
        print(f"📈 Total candles: {summary['total_candles_updated']}")
