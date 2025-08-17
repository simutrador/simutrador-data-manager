"""
Data providers module.

This module contains all services that interact with external data sources
for fetching trading data.
"""

from .data_provider_factory import DataProvider, DataProviderFactory
from .data_provider_interface import (
    AuthenticationError,
    DataProviderError,
    DataProviderInterface,
    RateLimitError,
)
from .financial_modeling_prep_client import FinancialModelingPrepClient
from .polygon_client import PolygonClient
from .tiingo_client import TiingoClient

__all__ = [
    # Interfaces and base classes
    "DataProviderInterface",
    "DataProviderFactory",
    "DataProvider",
    # Exceptions
    "DataProviderError",
    "AuthenticationError", 
    "RateLimitError",
    # Concrete implementations
    "PolygonClient",
    "FinancialModelingPrepClient",
    "TiingoClient",
]
