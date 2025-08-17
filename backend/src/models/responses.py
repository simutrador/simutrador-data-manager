"""
Response models for the Trading Simulator API.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, computed_field
from simutrador_core.models.enums import OrderSide, TradeResult
from simutrador_core.models.price_data import Timeframe


class Trade(BaseModel):
    """
    Individual trade result model.
    """

    entry_time: datetime = Field(
        ..., description="Timestamp when the trade was entered"
    )
    exit_time: datetime = Field(..., description="Timestamp when the trade was exited")
    entry_price: float = Field(
        ..., description="Price at which the trade was entered", gt=0
    )
    exit_price: float = Field(
        ..., description="Price at which the trade was exited", gt=0
    )
    side: OrderSide = Field(..., description="Trade side: buy or sell")
    result: TradeResult = Field(
        ...,
        description="Trade exit reason: tp (take profit), sl (stop loss), or timeout",
    )
    pnl: float = Field(..., description="Profit and loss for this trade")
    holding_minutes: int = Field(
        ..., description="Duration the position was held in minutes", ge=0
    )

    @computed_field
    @property
    def return_percentage(self) -> float:
        """Calculate the return percentage for this trade."""
        if self.side == OrderSide.BUY:
            return ((self.exit_price - self.entry_price) / self.entry_price) * 100
        else:  # SELL
            return ((self.entry_price - self.exit_price) / self.entry_price) * 100

    @computed_field
    @property
    def is_winning_trade(self) -> bool:
        """Determine if this is a winning trade."""
        return self.pnl > 0


class SimulationMetrics(BaseModel):
    """
    Performance metrics for the simulation.
    """

    total_orders: int = Field(..., description="Total number of orders submitted", ge=0)
    executed_orders: int = Field(
        ..., description="Number of orders that were successfully executed", ge=0
    )
    win_rate: float = Field(
        ..., description="Percentage of winning trades (0.0 to 1.0)", ge=0.0, le=1.0
    )
    total_pnl: float = Field(..., description="Total profit and loss across all trades")
    avg_trade_return: float = Field(
        ..., description="Average return per trade as a percentage"
    )
    max_drawdown: float = Field(
        ..., description="Maximum drawdown as a percentage (negative value)"
    )

    @computed_field
    @property
    def execution_rate(self) -> float:
        """Calculate the execution rate (executed orders / total orders)."""
        if self.total_orders == 0:
            return 0.0
        return self.executed_orders / self.total_orders

    @computed_field
    @property
    def loss_rate(self) -> float:
        """Calculate the loss rate (1 - win_rate)."""
        return 1.0 - self.win_rate


class SimulationResponse(BaseModel):
    """
    Complete response model for the trading simulation.
    """

    symbol: str = Field(..., description="Trading symbol that was simulated")
    timeframe: Timeframe = Field(..., description="Timeframe used for the simulation")
    start: datetime = Field(..., description="Start time of the simulation window")
    end: datetime = Field(..., description="End time of the simulation window")
    metrics: SimulationMetrics = Field(
        ..., description="Performance metrics for the simulation"
    )
    trades: list[Trade] = Field(
        ..., description="List of executed trades with their results"
    )

    @computed_field
    @property
    def simulation_duration_days(self) -> float:
        """Calculate the simulation duration in days."""
        duration = self.end - self.start
        return duration.total_seconds() / (24 * 3600)

    @computed_field
    @property
    def trades_per_day(self) -> float:
        """Calculate the average number of trades per day."""
        duration_days = self.simulation_duration_days
        if duration_days == 0:
            return 0.0
        return len(self.trades) / duration_days


class ErrorResponse(BaseModel):
    """
    Error response model for API errors.
    """

    error: str = Field(..., description="Error type or category")
    message: str = Field(..., description="Detailed error message")
    details: Optional[dict[str, str]] = Field(
        None, description="Additional error details"
    )
