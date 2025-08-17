"""
Asset type classification system for different financial instruments.

This module defines asset types and their market characteristics including
trading hours, resampling alignment strategies, and market session boundaries.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class AssetType(str, Enum):
    """Asset type enumeration for different financial instruments."""
    
    US_EQUITY = "us_equity"
    CRYPTO = "crypto"
    FOREX = "forex"
    COMMODITY = "commodity"
    INTERNATIONAL_EQUITY = "international_equity"
    UNKNOWN = "unknown"


@dataclass
class MarketSession:
    """Market session information for an asset type."""
    
    name: str
    open_utc_hour: int
    open_utc_minute: int
    close_utc_hour: int
    close_utc_minute: int
    timezone: str
    
    @property
    def open_utc_time(self) -> str:
        """Get market open time in HH:MM format."""
        return f"{self.open_utc_hour:02d}:{self.open_utc_minute:02d}"
    
    @property
    def close_utc_time(self) -> str:
        """Get market close time in HH:MM format."""
        return f"{self.close_utc_hour:02d}:{self.close_utc_minute:02d}"
    
    @property
    def resampling_offset(self) -> str:
        """Get the pandas resampling offset for this market session."""
        return f"{self.open_utc_hour}h{self.open_utc_minute:02d}min"


@dataclass
class AssetTypeConfig:
    """Configuration for an asset type including market characteristics."""
    
    asset_type: AssetType
    name: str
    description: str
    market_session: Optional[MarketSession]
    is_24_7: bool
    use_session_alignment: bool
    
    @property
    def resampling_offset(self) -> Optional[str]:
        """Get the resampling offset for this asset type."""
        if self.use_session_alignment and self.market_session:
            return self.market_session.resampling_offset
        return None


# Market session definitions
US_EQUITY_SESSION = MarketSession(
    name="US Equity Regular Hours",
    open_utc_hour=13,
    open_utc_minute=30,
    close_utc_hour=20,
    close_utc_minute=0,
    timezone="America/New_York"
)

LONDON_FOREX_SESSION = MarketSession(
    name="London Forex Session",
    open_utc_hour=8,
    open_utc_minute=0,
    close_utc_hour=17,
    close_utc_minute=0,
    timezone="Europe/London"
)

# Asset type configurations
ASSET_TYPE_CONFIGS = {
    AssetType.US_EQUITY: AssetTypeConfig(
        asset_type=AssetType.US_EQUITY,
        name="US Equity",
        description="US stock market securities (NYSE, NASDAQ)",
        market_session=US_EQUITY_SESSION,
        is_24_7=False,
        use_session_alignment=True
    ),
    
    AssetType.CRYPTO: AssetTypeConfig(
        asset_type=AssetType.CRYPTO,
        name="Cryptocurrency",
        description="Digital currencies trading 24/7",
        market_session=None,
        is_24_7=True,
        use_session_alignment=False
    ),
    
    AssetType.FOREX: AssetTypeConfig(
        asset_type=AssetType.FOREX,
        name="Foreign Exchange",
        description="Currency pairs trading in global sessions",
        market_session=LONDON_FOREX_SESSION,  # Default to London session
        is_24_7=True,  # Forex is nearly 24/5
        use_session_alignment=True
    ),
    
    AssetType.COMMODITY: AssetTypeConfig(
        asset_type=AssetType.COMMODITY,
        name="Commodity",
        description="Physical goods and futures contracts",
        market_session=None,  # Varies by commodity
        is_24_7=False,
        use_session_alignment=False
    ),
    
    AssetType.INTERNATIONAL_EQUITY: AssetTypeConfig(
        asset_type=AssetType.INTERNATIONAL_EQUITY,
        name="International Equity",
        description="Non-US stock market securities",
        market_session=None,  # Varies by country
        is_24_7=False,
        use_session_alignment=False
    ),
    
    AssetType.UNKNOWN: AssetTypeConfig(
        asset_type=AssetType.UNKNOWN,
        name="Unknown Asset",
        description="Asset type could not be determined",
        market_session=None,
        is_24_7=False,
        use_session_alignment=False
    )
}


def get_asset_config(asset_type: AssetType) -> AssetTypeConfig:
    """
    Get the configuration for a specific asset type.
    
    Args:
        asset_type: The asset type to get configuration for
        
    Returns:
        AssetTypeConfig for the specified asset type
    """
    return ASSET_TYPE_CONFIGS[asset_type]


def get_resampling_offset(asset_type: AssetType) -> Optional[str]:
    """
    Get the resampling offset for a specific asset type.
    
    Args:
        asset_type: The asset type to get offset for
        
    Returns:
        Pandas resampling offset string, or None for standard UTC alignment
        
    Examples:
        - US_EQUITY: "13h30min" (9:30 AM ET = 13:30 UTC)
        - CRYPTO: None (standard UTC alignment: 00:00, 00:05, etc.)
        - FOREX: "8h00min" (London session open)
    """
    config = get_asset_config(asset_type)
    return config.resampling_offset


def should_use_session_alignment(asset_type: AssetType) -> bool:
    """
    Check if an asset type should use market session alignment.
    
    Args:
        asset_type: The asset type to check
        
    Returns:
        True if session alignment should be used, False for standard UTC alignment
    """
    config = get_asset_config(asset_type)
    return config.use_session_alignment


def is_24_7_market(asset_type: AssetType) -> bool:
    """
    Check if an asset type trades 24/7.
    
    Args:
        asset_type: The asset type to check
        
    Returns:
        True if the market trades 24/7, False otherwise
    """
    config = get_asset_config(asset_type)
    return config.is_24_7
