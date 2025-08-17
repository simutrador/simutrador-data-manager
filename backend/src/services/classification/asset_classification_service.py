"""
Asset classification service for determining asset types from symbols.

This service analyzes trading symbols to determine their asset type and
corresponding market characteristics for proper resampling alignment.
"""

import logging
import re
from typing import Dict, List

from models.asset_types import AssetType

logger = logging.getLogger(__name__)


class AssetClassificationService:
    """Service for classifying trading symbols into asset types."""

    def __init__(self):
        """Initialize the asset classification service."""
        # Common crypto symbols and patterns
        self.crypto_symbols = {
            "BTC",
            "ETH",
            "ADA",
            "DOT",
            "SOL",
            "AVAX",
            "MATIC",
            "LINK",
            "UNI",
            "AAVE",
            "SUSHI",
            "CRV",
            "YFI",
            "COMP",
            "MKR",
            "SNX",
            "1INCH",
            "DOGE",
            "SHIB",
            "LTC",
            "BCH",
            "XRP",
            "XLM",
            "TRX",
            "EOS",
            "VET",
            "ALGO",
            "ATOM",
            "NEAR",
            "FTM",
            "LUNA",
            "UST",
            "USDC",
            "USDT",
            "DAI",
            "BUSD",
            "FRAX",
            "TUSD",
        }

        # Common forex currency codes
        self.forex_currencies = {
            "USD",
            "EUR",
            "GBP",
            "JPY",
            "CHF",
            "CAD",
            "AUD",
            "NZD",
            "SEK",
            "NOK",
            "DKK",
            "PLN",
            "CZK",
            "HUF",
            "TRY",
            "ZAR",
            "MXN",
            "BRL",
            "CNY",
            "HKD",
            "SGD",
            "KRW",
            "INR",
            "THB",
            "MYR",
            "IDR",
            "PHP",
        }

        # Common commodity symbols
        self.commodity_symbols = {
            "GC",
            "SI",
            "CL",
            "NG",
            "HG",
            "PL",
            "PA",
            "ZC",
            "ZS",
            "ZW",
            "KC",
            "CC",
            "CT",
            "SB",
            "OJ",
            "LB",
            "HE",
            "LE",
            "GF",
            "ZL",
        }

        # Regex patterns for different asset types
        self.crypto_patterns = [
            r"^[A-Z]{2,5}[-/]?USD[TC]?$",  # BTC-USD, ETH-USDT, etc.
            r"^[A-Z]{2,5}[-/]?BTC$",  # ALT-BTC pairs
            r"^[A-Z]{2,5}[-/]?ETH$",  # ALT-ETH pairs
        ]

        self.forex_patterns = [
            r"^[A-Z]{3}[A-Z]{3}$",  # EURUSD, GBPJPY, etc.
            r"^[A-Z]{3}[-/][A-Z]{3}$",  # EUR-USD, GBP/JPY, etc.
        ]

        # Known US equity exchanges and patterns
        self.us_equity_patterns = [
            r"^[A-Z]{1,5}$",  # Simple ticker symbols (AAPL, MSFT, etc.)
        ]

    def classify_symbol(self, symbol: str) -> AssetType:
        """
        Classify a trading symbol into its asset type.

        Args:
            symbol: Trading symbol to classify

        Returns:
            AssetType enum value
        """
        if not symbol:
            return AssetType.UNKNOWN

        symbol = symbol.upper().strip()

        # Check for forex patterns first (before crypto to avoid misclassification)
        if self._is_forex_symbol(symbol):
            return AssetType.FOREX

        # Check for crypto patterns
        if self._is_crypto_symbol(symbol):
            return AssetType.CRYPTO

        # Check for commodity symbols
        if self._is_commodity_symbol(symbol):
            return AssetType.COMMODITY

        # Check for US equity patterns
        if self._is_us_equity_symbol(symbol):
            return AssetType.US_EQUITY

        # Default to unknown if no pattern matches
        logger.warning(f"Could not classify symbol: {symbol}")
        return AssetType.UNKNOWN

    def _is_crypto_symbol(self, symbol: str) -> bool:
        """Check if symbol appears to be a cryptocurrency."""
        # Check against known crypto symbols
        base_symbol = self._extract_base_symbol(symbol)
        if base_symbol in self.crypto_symbols:
            return True

        # Check against crypto patterns
        for pattern in self.crypto_patterns:
            if re.match(pattern, symbol):
                return True

        # Check for common crypto pair formats
        if any(sep in symbol for sep in ["-", "/"]):
            parts = re.split(r"[-/]", symbol)
            if len(parts) == 2:
                base, quote = parts
                if base in self.crypto_symbols or quote in {
                    "USD",
                    "USDT",
                    "USDC",
                    "BTC",
                    "ETH",
                }:
                    return True

        return False

    def _is_forex_symbol(self, symbol: str) -> bool:
        """Check if symbol appears to be a forex pair."""
        # Check against forex patterns
        for pattern in self.forex_patterns:
            if re.match(pattern, symbol):
                # Verify both currencies are known
                if "/" in symbol or "-" in symbol:
                    parts = re.split(r"[-/]", symbol)
                    if len(parts) == 2:
                        base, quote = parts
                        return (
                            base in self.forex_currencies
                            and quote in self.forex_currencies
                        )
                elif len(symbol) == 6:
                    base, quote = symbol[:3], symbol[3:]
                    return (
                        base in self.forex_currencies and quote in self.forex_currencies
                    )

        return False

    def _is_commodity_symbol(self, symbol: str) -> bool:
        """Check if symbol appears to be a commodity."""
        base_symbol = self._extract_base_symbol(symbol)
        return base_symbol in self.commodity_symbols

    def _is_us_equity_symbol(self, symbol: str) -> bool:
        """Check if symbol appears to be a US equity."""
        # Simple heuristic: 1-5 letter symbols that don't match other patterns
        if re.match(r"^[A-Z]{1,5}$", symbol):
            # Make sure it's not a known crypto or commodity
            if (
                symbol not in self.crypto_symbols
                and symbol not in self.commodity_symbols
                and symbol not in self.forex_currencies
            ):
                return True

        return False

    def _extract_base_symbol(self, symbol: str) -> str:
        """Extract the base symbol from a trading pair."""
        # Remove common separators and suffixes
        base = symbol.split("-")[0].split("/")[0]

        # Remove common suffixes
        for suffix in ["USD", "USDT", "USDC", "BTC", "ETH"]:
            if base.endswith(suffix) and len(base) > len(suffix):
                base = base[: -len(suffix)]
                break

        return base

    def classify_symbols(self, symbols: List[str]) -> Dict[str, AssetType]:
        """
        Classify multiple symbols at once.

        Args:
            symbols: List of trading symbols to classify

        Returns:
            Dictionary mapping symbols to their asset types
        """
        return {symbol: self.classify_symbol(symbol) for symbol in symbols}

    def get_symbols_by_type(
        self, symbols: List[str], asset_type: AssetType
    ) -> List[str]:
        """
        Filter symbols by asset type.

        Args:
            symbols: List of symbols to filter
            asset_type: Asset type to filter for

        Returns:
            List of symbols matching the specified asset type
        """
        return [
            symbol for symbol in symbols if self.classify_symbol(symbol) == asset_type
        ]

    def add_custom_mapping(self, symbol: str, asset_type: AssetType) -> None:
        """
        Add a custom symbol-to-asset-type mapping.

        This allows for manual overrides of the automatic classification.

        Args:
            symbol: Trading symbol
            asset_type: Asset type to map to
        """
        # This could be extended to use a database or configuration file
        # For now, we'll add to the appropriate symbol set
        symbol = symbol.upper().strip()

        if asset_type == AssetType.CRYPTO:
            self.crypto_symbols.add(symbol)
        elif asset_type == AssetType.COMMODITY:
            self.commodity_symbols.add(symbol)
        elif asset_type == AssetType.FOREX:
            # For forex, we'd need to handle differently since it's currency pairs
            pass

        logger.info(f"Added custom mapping: {symbol} -> {asset_type}")

    def get_classification_stats(self, symbols: List[str]) -> Dict[AssetType, int]:
        """
        Get statistics on asset type distribution for a list of symbols.

        Args:
            symbols: List of symbols to analyze

        Returns:
            Dictionary with counts for each asset type
        """
        classifications = self.classify_symbols(symbols)
        stats: Dict[AssetType, int] = {}

        for asset_type in AssetType:
            count = sum(1 for t in classifications.values() if t == asset_type)
            if count > 0:
                stats[asset_type] = count

        return stats
