"""
Validation module.

This module contains services for data quality assurance and validation.
"""

from .stock_market_validation_service import (
    StockMarketValidationService,
    ValidationResult,
)

__all__ = [
    "StockMarketValidationService",
    "ValidationResult",
]
