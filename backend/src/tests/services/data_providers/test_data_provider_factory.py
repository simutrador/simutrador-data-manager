"""
Tests for the data provider factory and interface.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from services.data_providers.data_provider_factory import (
    DataProvider,
    DataProviderFactory,
)
from services.data_providers.data_provider_interface import DataProviderInterface
from services.data_providers.financial_modeling_prep_client import (
    FinancialModelingPrepClient,
)
from services.data_providers.polygon_client import PolygonClient
from services.data_providers.tiingo_client import TiingoClient


class TestDataProviderFactory:
    """Test cases for DataProviderFactory."""

    def test_create_financial_modeling_prep_provider(self):
        """Test creating FinancialModelingPrepClient through factory."""
        provider = DataProviderFactory.create_provider(
            DataProvider.FINANCIAL_MODELING_PREP
        )

        assert isinstance(provider, FinancialModelingPrepClient)
        assert isinstance(provider, DataProviderInterface)

    def test_create_polygon_provider(self):
        """Test creating PolygonClient through factory."""
        provider = DataProviderFactory.create_provider(DataProvider.POLYGON)

        assert isinstance(provider, PolygonClient)
        assert isinstance(provider, DataProviderInterface)

    def test_create_tiingo_provider(self):
        """Test creating TiingoClient through factory."""
        provider = DataProviderFactory.create_provider(DataProvider.TIINGO)

        assert isinstance(provider, TiingoClient)
        assert isinstance(provider, DataProviderInterface)

    def test_unsupported_provider_raises_error(self):
        """Test that unsupported provider raises ValueError."""
        # Create a mock enum value that doesn't exist in the factory
        with pytest.raises(ValueError, match="Unsupported provider"):
            # This would fail if we had an enum value not in _provider_classes
            DataProviderFactory.create_provider("invalid_provider")  # type: ignore

    def test_get_available_providers(self):
        """Test getting list of available providers."""
        available = DataProviderFactory.get_available_providers()

        # Should include all three providers
        assert DataProvider.FINANCIAL_MODELING_PREP in available
        assert DataProvider.POLYGON in available
        assert DataProvider.TIINGO in available
        assert len(available) == 3

    def test_is_provider_available(self):
        """Test checking if specific providers are available."""
        assert DataProviderFactory.is_provider_available(
            DataProvider.FINANCIAL_MODELING_PREP
        )
        assert DataProviderFactory.is_provider_available(DataProvider.POLYGON)
        assert DataProviderFactory.is_provider_available(DataProvider.TIINGO)

    @patch(
        "services.data_providers.data_provider_factory.DataProviderFactory._load_provider_class"
    )
    def test_import_error_handling(self, mock_load: Mock):
        """Test handling of import errors."""
        mock_load.side_effect = ImportError("Module not found")

        assert not DataProviderFactory.is_provider_available(DataProvider.POLYGON)

    def test_lazy_loading(self):
        """Test that provider classes are loaded lazily."""
        # Clear any previously loaded classes
        DataProviderFactory._loaded_classes.clear()  # type: ignore

        # Create provider - should trigger loading
        provider = DataProviderFactory.create_provider(
            DataProvider.FINANCIAL_MODELING_PREP
        )

        # Verify class was loaded and cached
        assert (
            DataProvider.FINANCIAL_MODELING_PREP in DataProviderFactory._loaded_classes  # type: ignore
        )
        assert isinstance(provider, FinancialModelingPrepClient)


class TestDataProviderInterface:
    """Test cases for DataProviderInterface compliance."""

    @pytest.mark.asyncio
    async def test_financial_modeling_prep_interface_compliance(self):
        """Test that FinancialModelingPrepClient implements the interface correctly."""
        with patch(
            "services.data_providers.financial_modeling_prep_client.get_settings"
        ) as mock_settings:
            mock_settings.return_value = MagicMock()
            mock_settings.return_value.financial_modeling_prep = MagicMock()
            mock_settings.return_value.financial_modeling_prep.api_key = "test_key"
            mock_settings.return_value.financial_modeling_prep.base_url = (
                "https://test.com"
            )
            mock_settings.return_value.financial_modeling_prep.rate_limit_per_minute = (
                300
            )

            provider = FinancialModelingPrepClient()

            # Test async context manager
            async with provider as client:
                assert client is provider

                # Test that required methods exist
                assert hasattr(client, "fetch_historical_data")
                assert hasattr(client, "fetch_latest_data")
                assert callable(client.fetch_historical_data)
                assert callable(client.fetch_latest_data)

    @pytest.mark.asyncio
    async def test_polygon_interface_compliance(self):
        """Test that PolygonClient implements the interface correctly."""
        with patch(
            "services.data_providers.polygon_client.get_settings"
        ) as mock_settings:
            mock_settings.return_value = MagicMock()
            mock_settings.return_value.polygon = MagicMock()
            mock_settings.return_value.polygon.api_key = "test_key"
            mock_settings.return_value.polygon.base_url = "https://test.com"
            mock_settings.return_value.polygon.rate_limit_requests_per_second = 100

            provider = PolygonClient()

            # Test async context manager
            async with provider as client:
                assert client is provider

                # Test that required methods exist
                assert hasattr(client, "fetch_historical_data")
                assert hasattr(client, "fetch_latest_data")
                assert callable(client.fetch_historical_data)
                assert callable(client.fetch_latest_data)

    @pytest.mark.asyncio
    async def test_tiingo_interface_compliance(self):
        """Test that TiingoClient implements the interface correctly."""
        with patch(
            "services.data_providers.tiingo_client.get_settings"
        ) as mock_settings:
            mock_settings.return_value = MagicMock()
            mock_settings.return_value.tiingo = MagicMock()
            mock_settings.return_value.tiingo.api_key = "test_key"
            mock_settings.return_value.tiingo.base_url = "https://test.com"

            provider = TiingoClient()

            # Test async context manager
            async with provider as client:
                assert client is provider

                # Test that required methods exist
                assert hasattr(client, "fetch_historical_data")
                assert hasattr(client, "fetch_latest_data")
                assert callable(client.fetch_historical_data)
                assert callable(client.fetch_latest_data)


class TestTradingDataUpdatingServiceIntegration:
    """Test integration of TradingDataUpdatingService with different providers."""

    @patch("services.workflows.trading_data_updating_service.DataStorageService")
    def test_service_with_financial_modeling_prep(self, _mock_storage_service):  # type: ignore  # noqa: ARG002
        """Test service initialization with FinancialModelingPrepClient."""
        from services.workflows.trading_data_updating_service import (
            TradingDataUpdatingService,
        )

        service = TradingDataUpdatingService(
            provider_type=DataProvider.FINANCIAL_MODELING_PREP
        )

        assert service.provider_type == DataProvider.FINANCIAL_MODELING_PREP
        assert service.storage_service is not None

    @patch("services.workflows.trading_data_updating_service.DataStorageService")
    def test_service_with_polygon(self, _mock_storage_service):  # type: ignore  # noqa: ARG002
        """Test service initialization with PolygonClient."""
        from services.workflows.trading_data_updating_service import (
            TradingDataUpdatingService,
        )

        service = TradingDataUpdatingService(provider_type=DataProvider.POLYGON)

        assert service.provider_type == DataProvider.POLYGON
        assert service.storage_service is not None

    @patch("services.workflows.trading_data_updating_service.DataStorageService")
    def test_service_default_provider(self, _mock_storage_service):  # type: ignore  # noqa: ARG002
        """Test service uses default provider when none specified."""
        from services.workflows.trading_data_updating_service import (
            TradingDataUpdatingService,
        )

        service = TradingDataUpdatingService()

        assert service.provider_type == DataProvider.POLYGON
        assert service.storage_service is not None
