import os
from functools import lru_cache
from pathlib import Path
from typing import List

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiSettings(BaseModel):
    """API server configuration."""

    host: str = Field(default="0.0.0.0", description="API host")
    port: int = Field(default=8002, description="API port")
    debug: bool = Field(default=True, description="Debug mode")


class FinancialModelingPrepSettings(BaseModel):
    """Financial Modeling Prep API configuration."""

    api_key: str = Field(default="", description="Financial Modeling Prep API key")
    base_url: str = Field(
        default="https://financialmodelingprep.com/stable",
        description="Base URL for Financial Modeling Prep API",
    )
    rate_limit_per_minute: int = Field(
        default=300, description="Rate limit per minute", gt=0
    )


class PolygonSettings(BaseModel):
    """Polygon API configuration."""

    api_key: str = Field(default="", description="Polygon API key")
    base_url: str = Field(
        default="https://api.polygon.io/v2/aggs/ticker",
        description="Base URL for Polygon API",
    )
    rate_limit_requests_per_second: int = Field(
        default=100, description="Rate limit requests per second", gt=0
    )
    use_trades_endpoint_for_gaps: bool = Field(
        default=False,
        description="Use v3/trades endpoint for gap filling (requires higher-tier plan). "
        "Set POLYGON_USE_TRADES_ENDPOINT_FOR_GAPS=true to enable.",
    )
    max_concurrent_requests: int = Field(
        default=1,
        description="Maximum concurrent requests for large historical downloads. "
        "Set POLYGON_MAX_CONCURRENT_REQUESTS=3 for faster downloads.",
    )


class TiingoSettings(BaseModel):
    """Tiingo API configuration."""

    api_key: str = Field(default="", description="Tiingo API key")
    base_url: str = Field(
        default="https://api.tiingo.com/tiingo",
        description="Base URL for Tiingo API",
    )
    rate_limit_requests_per_hour: int = Field(
        default=50, description="Rate limit requests per hour", gt=0
    )
    rate_limit_requests_per_day: int = Field(
        default=1000, description="Rate limit requests per day", gt=0
    )


class TradingDataProviderSettings(BaseModel):
    """Trading data provider configuration."""

    default_provider: str = Field(
        default="polygon",
        description="Default data provider to use",
    )
    fallback_providers: List[str] = Field(
        default=["financial_modeling_prep", "tiingo"],
        description="Fallback providers if primary fails",
    )
    enable_fallback: bool = Field(
        default=False,
        description="Enable automatic fallback to other providers on failure",
    )


class DataStorageSettings(BaseModel):
    """Data storage configuration."""

    base_path: str = Field(default="storage", description="Base storage directory path")
    candles_path: str = Field(
        default="candles", description="Candles subdirectory path"
    )

    @field_validator("base_path")
    @classmethod
    def resolve_base_path(cls, v: str) -> str:
        """Convert relative paths to absolute paths based on backend directory."""
        if os.path.isabs(v):
            return v

        # Find backend directory (directory containing pyproject.toml)
        current_dir = Path(__file__).parent  # Start from src/core/
        while current_dir != current_dir.parent:
            if (current_dir / "pyproject.toml").exists():
                backend_root = current_dir
                break
            current_dir = current_dir.parent
        else:
            # Fallback: assume we're in src/ and backend root is parent
            backend_root = Path(__file__).parent.parent

        # Resolve relative path against backend root
        absolute_path = backend_root / v
        return str(absolute_path.resolve())


class NightlyUpdateSettings(BaseModel):
    """Nightly update configuration for stock market data."""

    # US Equity symbol lists for different market caps
    large_cap_symbols: List[str] = Field(
        default=[
            "AAPL",
            "MSFT",
            "GOOGL",
            "AMZN",
            "NVDA",
            "META",
            "TSLA",
            "BRK.B",
            "UNH",
            "JNJ",
            "JPM",
            "V",
            "PG",
            "XOM",
            "HD",
            "CVX",
            "MA",
            "PFE",
            "ABBV",
            "BAC",
            "COST",
            "AVGO",
            "WMT",
            "DIS",
            "ADBE",
            "CRM",
            "NFLX",
            "TMO",
            "ACN",
            "VZ",
            "CSCO",
            "ABT",
            "NKE",
            "ORCL",
            "COP",
            "MRK",
            "INTC",
            "AMD",
            "TXN",
            "QCOM",
            "DHR",
            "NEE",
            "UPS",
            "PM",
            "RTX",
            "HON",
            "SPGI",
            "LOW",
            "INTU",
            "IBM",
            "GS",
        ],
        description="Large cap US equity symbols for nightly updates",
    )

    mid_cap_symbols: List[str] = Field(
        default=[
            "ROKU",
            "SNAP",
            "UBER",
            "LYFT",
            "PINS",
            "TWTR",
            "SQ",
            "SHOP",
            "SPOT",
            "ZM",
            "DOCU",
            "CRWD",
            "OKTA",
            "SNOW",
            "PLTR",
            "RBLX",
            "COIN",
            "HOOD",
            "SOFI",
            "UPST",
            "AFRM",
            "PYPL",
            "ETSY",
            "TDOC",
            "PTON",
            "MRNA",
            "BNTX",
            "ZS",
            "DDOG",
            "NET",
            "FSLY",
            "ESTC",
        ],
        description="Mid cap US equity symbols for nightly updates",
    )

    # Market hours configuration (UTC only - eliminates timezone issues)
    # US market hours: 9:30 AM - 4:00 PM ET = 13:30 - 20:00 UTC (standard time)
    # Note: This assumes standard time. For daylight time, it would be 13:30 - 20:00 UTC
    market_open_hour_utc: int = Field(
        default=13, description="Market open hour (UTC) - 9:30 AM ET"
    )
    market_open_minute_utc: int = Field(
        default=30, description="Market open minute (UTC)"
    )
    market_close_hour_utc: int = Field(
        default=20, description="Market close hour (UTC) - 4:00 PM ET"
    )
    market_close_minute_utc: int = Field(
        default=0, description="Market close minute (UTC)"
    )

    # Data validation settings
    expected_candles_per_day: int = Field(
        default=390, description="Expected 1-min candles per full trading day"
    )
    expected_candles_half_day: int = Field(
        default=210, description="Expected 1-min candles per half trading day"
    )
    min_volume_threshold: int = Field(
        default=1000, description="Minimum daily volume for liquid stocks"
    )

    # Update configuration
    max_concurrent_symbols: int = Field(
        default=3, description="Maximum concurrent symbol updates"
    )
    retry_attempts: int = Field(
        default=3, description="Number of retry attempts for failed updates"
    )
    retry_delay_seconds: int = Field(
        default=60, description="Delay between retry attempts"
    )

    # Resampling timeframes
    target_timeframes: List[str] = Field(
        default=["5min", "15min", "30min", "1h", "2h", "4h", "daily"],
        description="Target timeframes for resampling after 1min data update",
    )

    # Enable/disable features
    enable_data_validation: bool = Field(
        default=True, description="Enable data completeness validation"
    )
    enable_market_hours_check: bool = Field(
        default=True, description="Enable market hours validation"
    )
    enable_auto_resampling: bool = Field(
        default=True, description="Enable automatic resampling after updates"
    )


class ProjectSettings(BaseSettings):
    """Central configuration file for Project."""

    model_config = SettingsConfigDict(
        env_file=os.getenv(
            "ENV",
            os.path.join(os.path.dirname(__file__), "../environments/.env.dev"),
        ),
        env_nested_delimiter="__",  # Allows API__HOST=localhost in env files
    )

    # Nested configuration objects
    api: ApiSettings = Field(default_factory=ApiSettings)
    financial_modeling_prep: FinancialModelingPrepSettings = Field(
        default_factory=FinancialModelingPrepSettings
    )
    polygon: PolygonSettings = Field(default_factory=PolygonSettings)
    tiingo: TiingoSettings = Field(default_factory=TiingoSettings)
    trading_data_provider: TradingDataProviderSettings = Field(
        default_factory=TradingDataProviderSettings
    )
    data_storage: DataStorageSettings = Field(default_factory=DataStorageSettings)
    nightly_update: NightlyUpdateSettings = Field(default_factory=NightlyUpdateSettings)


@lru_cache
def get_settings():
    return ProjectSettings()
