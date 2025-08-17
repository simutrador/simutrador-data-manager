"""
⚠️  OPTIONAL E2E TEST - REQUIRES MANUAL SETUP ⚠️

This test validates resampling accuracy against manually downloaded Polygon reference data.
It will be automatically SKIPPED if reference data is not available.

PURPOSE: Validate that our resampling logic produces results identical to Polygon's
native aggregates.

SETUP (Optional):
1. Manually download reference data from Polygon API for specific dates
2. Store in: backend/test_storage/candles/{timeframe}/AAPL/{date}.json
3. Run tests to validate resampling accuracy

ALTERNATIVE: Use tests/services/resampling/test_resampling_logic.py for
mathematical validation without external dependencies.
"""

import json
import logging
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from services.storage.data_resampling_service import DataResamplingService

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from simutrador_core.models.price_data import PriceCandle, PriceDataSeries, Timeframe

logger = logging.getLogger(__name__)


class TestPolygonReferenceValidation:
    """E2E test validating our resampling against Polygon reference data."""

    @pytest.fixture
    def test_storage_path(self) -> Path:
        """Get the path to test storage directory."""
        return Path(__file__).parent.parent.parent.parent / "test_storage"

    @pytest.fixture
    def temp_storage_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def mock_settings(self, temp_storage_dir: Path):
        """Mock settings with temporary directory."""
        mock_settings = MagicMock()
        mock_settings.data_storage.base_path = str(temp_storage_dir)
        mock_settings.data_storage.candles_path = "candles"
        return mock_settings

    @pytest.fixture
    def resampling_service(self, mock_settings: Any):
        """Create resampling service with temporary storage."""
        with patch(
            "services.storage.data_storage_service.get_settings",
            return_value=mock_settings,
        ):
            return DataResamplingService()

    def load_polygon_data(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load Polygon data from JSON file."""
        if not file_path.exists():
            pytest.skip(f"Reference data file not found: {file_path}")

        with open(file_path, "r") as f:
            data: Any = json.load(f)
        if isinstance(data, dict):
            results = data.get("results", [])  # type: ignore[reportUnknownMemberType]
            return [item for item in results if isinstance(item, dict)]  # type: ignore[reportUnknownVariableType]
        return []

    def polygon_to_price_candles(
        self, polygon_data: List[Dict[str, Any]]
    ) -> List[PriceCandle]:
        """Convert Polygon JSON data to PriceCandle objects."""
        candles: List[PriceCandle] = []
        for item in polygon_data:
            # Convert timestamp from milliseconds to datetime
            timestamp = datetime.fromtimestamp(item["t"] / 1000, tz=timezone.utc)

            candle = PriceCandle(
                date=timestamp,
                open=Decimal(str(item["o"])),
                high=Decimal(str(item["h"])),
                low=Decimal(str(item["l"])),
                close=Decimal(str(item["c"])),
                volume=item["v"],
            )
            candles.append(candle)

        return candles

    def compare_candles(
        self,
        our_candles: List[PriceCandle],
        reference_candles: List[PriceCandle],
        tolerance: Decimal = Decimal("0.01"),
    ) -> Dict[str, Any]:
        """Compare our resampled candles with reference candles."""

        # Create lookup by timestamp
        our_lookup = {candle.date: candle for candle in our_candles}
        ref_lookup = {candle.date: candle for candle in reference_candles}

        # Find common timestamps
        common_timestamps = set(our_lookup.keys()) & set(ref_lookup.keys())

        missing_timestamps: List[datetime] = list(
            set(ref_lookup.keys()) - set(our_lookup.keys())
        )
        extra_timestamps: List[datetime] = list(
            set(our_lookup.keys()) - set(ref_lookup.keys())
        )

        results: Dict[str, Any] = {
            "total_our_candles": len(our_candles),
            "total_reference_candles": len(reference_candles),
            "common_timestamps": len(common_timestamps),
            "matches": 0,
            "mismatches": [],
            "missing_in_our_data": missing_timestamps,
            "extra_in_our_data": extra_timestamps,
        }

        # Compare common candles
        for timestamp in sorted(common_timestamps):
            our_candle = our_lookup[timestamp]
            ref_candle = ref_lookup[timestamp]

            mismatch = {}

            # Compare OHLC values
            for field in ["open", "high", "low", "close"]:
                our_val = getattr(our_candle, field)
                ref_val = getattr(ref_candle, field)
                diff = abs(our_val - ref_val)

                if diff > tolerance:
                    mismatch[field] = {
                        "our_value": float(our_val),
                        "reference_value": float(ref_val),
                        "difference": float(diff),
                    }

            # Compare volume (allow small differences due to rounding)
            volume_diff = abs(our_candle.volume - ref_candle.volume)
            if volume_diff > 0:
                mismatch["volume"] = {
                    "our_value": our_candle.volume,
                    "reference_value": ref_candle.volume,
                    "difference": volume_diff,
                }

            if mismatch:
                mismatch["timestamp"] = timestamp.isoformat()
                results["mismatches"].append(mismatch)
            else:
                results["matches"] += 1

        return results

    def test_5min_resampling_validation(
        self, test_storage_path: Path, resampling_service: DataResamplingService
    ):
        """Test that our 5-minute resampling matches Polygon's native 5-minute data."""

        # Load 1-minute reference data
        min_1_file = test_storage_path / "candles" / "1min" / "AAPL" / "2025-07-11.json"
        min_1_data = self.load_polygon_data(min_1_file)
        min_1_candles = self.polygon_to_price_candles(min_1_data)

        # Load 5-minute reference data
        min_5_file = test_storage_path / "candles" / "5min" / "AAPL" / "2025-07-11.json"
        min_5_data = self.load_polygon_data(min_5_file)
        reference_5min_candles = self.polygon_to_price_candles(min_5_data)

        # Create 1-minute series and store it
        series_1min = PriceDataSeries(
            symbol="AAPL", timeframe=Timeframe.ONE_MIN, candles=min_1_candles
        )
        resampling_service.storage_service.store_data(series_1min)

        # Resample to 5-minute using our service
        our_5min_series = resampling_service.resample_data(
            symbol="AAPL", from_timeframe="1min", to_timeframe="5min"
        )

        # Compare results
        comparison = self.compare_candles(
            our_5min_series.candles, reference_5min_candles
        )

        # Log detailed results
        logger.info("5-minute resampling validation results:")
        logger.info(f"  Our candles: {comparison['total_our_candles']}")
        logger.info(f"  Reference candles: {comparison['total_reference_candles']}")
        logger.info(f"  Common timestamps: {comparison['common_timestamps']}")
        logger.info(f"  Perfect matches: {comparison['matches']}")
        logger.info(f"  Mismatches: {len(comparison['mismatches'])}")

        if comparison["mismatches"]:
            logger.warning("Mismatches found:")
            for mismatch in comparison["mismatches"][:5]:  # Show first 5
                logger.warning(f"  {mismatch}")

        if comparison["missing_in_our_data"]:
            logger.warning(
                f"Missing in our data: {len(comparison['missing_in_our_data'])} timestamps"
            )

        if comparison["extra_in_our_data"]:
            logger.warning(
                f"Extra in our data: {len(comparison['extra_in_our_data'])} timestamps"
            )

        # Assertions
        assert comparison["total_our_candles"] > 0, "No resampled candles produced"
        assert comparison["total_reference_candles"] > 0, "No reference candles loaded"
        assert comparison["common_timestamps"] > 0, "No common timestamps found"

        # Allow for small differences but expect high accuracy
        match_rate = (
            comparison["matches"] / comparison["common_timestamps"]
            if comparison["common_timestamps"] > 0
            else 0
        )
        assert match_rate >= 0.95, f"Match rate {match_rate:.2%} is below 95% threshold"

        # Check that we don't have significant missing data
        missing_rate = (
            len(comparison["missing_in_our_data"])
            / comparison["total_reference_candles"]
            if comparison["total_reference_candles"] > 0
            else 0
        )
        assert (
            missing_rate <= 0.05
        ), f"Missing data rate {missing_rate:.2%} is above 5% threshold"

    def test_15min_resampling_validation(
        self, test_storage_path: Path, resampling_service: DataResamplingService
    ):
        """Test that our 15-minute resampling matches Polygon's native 15-minute data."""

        # Load 1-minute reference data
        min_1_file = test_storage_path / "candles" / "1min" / "AAPL" / "2025-07-11.json"
        min_1_data = self.load_polygon_data(min_1_file)
        min_1_candles = self.polygon_to_price_candles(min_1_data)

        # Load 15-minute reference data
        min_15_file = (
            test_storage_path / "candles" / "15min" / "AAPL" / "2025-07-11.json"
        )
        min_15_data = self.load_polygon_data(min_15_file)
        reference_15min_candles = self.polygon_to_price_candles(min_15_data)

        # Create 1-minute series and store it
        series_1min = PriceDataSeries(
            symbol="AAPL", timeframe=Timeframe.ONE_MIN, candles=min_1_candles
        )
        resampling_service.storage_service.store_data(series_1min)

        # Resample to 15-minute using our service
        our_15min_series = resampling_service.resample_data(
            symbol="AAPL", from_timeframe="1min", to_timeframe="15min"
        )

        # Compare results
        comparison = self.compare_candles(
            our_15min_series.candles, reference_15min_candles
        )

        # Log detailed results
        logger.info("15-minute resampling validation results:")
        logger.info(f"  Our candles: {comparison['total_our_candles']}")
        logger.info(f"  Reference candles: {comparison['total_reference_candles']}")
        logger.info(f"  Common timestamps: {comparison['common_timestamps']}")
        logger.info(f"  Perfect matches: {comparison['matches']}")
        logger.info(f"  Mismatches: {len(comparison['mismatches'])}")

        # Assertions
        assert comparison["total_our_candles"] > 0, "No resampled candles produced"
        assert comparison["total_reference_candles"] > 0, "No reference candles loaded"
        assert comparison["common_timestamps"] > 0, "No common timestamps found"

        # Allow for small differences but expect high accuracy
        match_rate = (
            comparison["matches"] / comparison["common_timestamps"]
            if comparison["common_timestamps"] > 0
            else 0
        )
        assert match_rate >= 0.95, f"Match rate {match_rate:.2%} is below 95% threshold"
