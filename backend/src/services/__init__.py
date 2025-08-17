"""
Services module.

This module provides a structured organization of all services in the trading simulator.
Services are organized by their primary responsibilities:

- data_providers: External data source integrations
- storage: Data persistence and retrieval
- workflows: Business process orchestration
- validation: Data quality assurance
- classification: Asset and symbol analysis
"""

# Re-export commonly used services for backward compatibility
from .classification import (
    AssetClassificationService,
)
from .data_providers import (
    DataProvider,
    DataProviderFactory,
    DataProviderInterface,
    FinancialModelingPrepClient,
    PolygonClient,
    TiingoClient,
)
from .progress import (
    NightlyUpdateProgressService,
)
from .storage import (
    DataResamplingService,
    DataStorageService,
)
from .validation import (
    StockMarketValidationService,
    ValidationResult,
)
from .workflows import (
    StockMarketNightlyUpdateService,
    StockMarketResamplingWorkflow,
    TradingDataUpdatingService,
)

__all__ = [
    # Data providers
    "DataProvider",
    "DataProviderFactory",
    "DataProviderInterface",
    "PolygonClient",
    "FinancialModelingPrepClient",
    "TiingoClient",
    # Progress tracking
    "NightlyUpdateProgressService",
    # Storage
    "DataStorageService",
    "DataResamplingService",
    # Workflows
    "StockMarketNightlyUpdateService",
    "StockMarketResamplingWorkflow",
    "TradingDataUpdatingService",
    # Validation
    "StockMarketValidationService",
    "ValidationResult",
    # Classification
    "AssetClassificationService",
]
