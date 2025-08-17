"""
API module for the Trading Simulator.
"""

from .data_analysis import router as data_analysis_router
from .simulation import router as simulation_router
from .trading_data import router as trading_data_router

__all__ = ["simulation_router", "trading_data_router", "data_analysis_router"]
