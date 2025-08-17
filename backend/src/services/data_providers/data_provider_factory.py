"""
Data provider factory for creating trading data provider instances.

This module implements the factory pattern for creating data provider instances,
enabling easy switching between different data vendors and supporting fallback mechanisms.
"""

import logging
from enum import Enum
from typing import Dict, Type

from .data_provider_interface import DataProviderInterface

logger = logging.getLogger(__name__)


class DataProvider(Enum):
    """Enumeration of supported data providers."""

    FINANCIAL_MODELING_PREP = "financial_modeling_prep"
    POLYGON = "polygon"
    TIINGO = "tiingo"


class DataProviderFactory:
    """
    Factory for creating data provider instances.

    This factory uses lazy loading to avoid importing provider classes until needed,
    which helps with startup time and allows for optional dependencies.
    """

    _provider_classes: Dict[DataProvider, str] = {
        DataProvider.FINANCIAL_MODELING_PREP: (
            "services.data_providers.financial_modeling_prep_client.FinancialModelingPrepClient"
        ),
        DataProvider.POLYGON: "services.data_providers.polygon_client.PolygonClient",
        DataProvider.TIINGO: "services.data_providers.tiingo_client.TiingoClient",
    }

    _loaded_classes: Dict[DataProvider, Type[DataProviderInterface]] = {}

    @classmethod
    def create_provider(cls, provider_type: DataProvider) -> DataProviderInterface:
        """
        Create a data provider instance.

        Args:
            provider_type: The type of provider to create

        Returns:
            An instance of the requested data provider

        Raises:
            ValueError: If the provider type is not supported
            ImportError: If the provider class cannot be imported
        """
        if provider_type not in cls._provider_classes:
            raise ValueError(f"Unsupported provider: {provider_type}")

        # Load the class if not already loaded
        if provider_type not in cls._loaded_classes:
            cls._load_provider_class(provider_type)

        provider_class = cls._loaded_classes[provider_type]
        return provider_class()

    @classmethod
    def _load_provider_class(cls, provider_type: DataProvider) -> None:
        """
        Dynamically load a provider class.

        Args:
            provider_type: The provider type to load

        Raises:
            ImportError: If the provider class cannot be imported
        """
        module_path = cls._provider_classes[provider_type]
        module_name, class_name = module_path.rsplit(".", 1)

        try:
            module = __import__(module_name, fromlist=[class_name])
            provider_class = getattr(module, class_name)

            # Verify the class implements the interface
            if not issubclass(provider_class, DataProviderInterface):
                raise ImportError(
                    f"Provider class {class_name} does not implement DataProviderInterface"
                )

            cls._loaded_classes[provider_type] = provider_class
            logger.info(f"Loaded data provider class: {provider_type.value}")

        except ImportError as e:
            logger.error(f"Failed to load provider {provider_type.value}: {e}")
            raise ImportError(f"Cannot import provider {provider_type.value}: {e}")

    @classmethod
    def get_available_providers(cls) -> list[DataProvider]:
        """
        Get a list of available data providers.

        Returns:
            List of available provider types
        """
        available: list[DataProvider] = []
        for provider_type in DataProvider:
            try:
                cls._load_provider_class(provider_type)
                available.append(provider_type)
            except ImportError:
                logger.warning(f"Provider {provider_type.value} is not available")
                continue

        return available

    @classmethod
    def is_provider_available(cls, provider_type: DataProvider) -> bool:
        """
        Check if a specific provider is available.

        Args:
            provider_type: The provider type to check

        Returns:
            True if the provider is available, False otherwise
        """
        try:
            cls._load_provider_class(provider_type)
            return True
        except ImportError:
            return False
