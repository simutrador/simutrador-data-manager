"""
Enums for the Trading Simulator API.
"""

from enum import Enum


class OrderType(str, Enum):
    """Order type enumeration."""

    MARKET = "MKT"
    LIMIT = "LMT"


class OrderSide(str, Enum):
    """Order side enumeration."""

    BUY = "buy"
    SELL = "sell"


class TradeResult(str, Enum):
    """Trade result enumeration."""

    TAKE_PROFIT = "tp"
    STOP_LOSS = "sl"
    TIMEOUT = "timeout"
