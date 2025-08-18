"""
Stock market resampling workflow service.

This service orchestrates the complete resampling workflow for stock market data,
ensuring all timeframes are properly generated with correct market session alignment.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional, override

from simutrador_core.models.price_data import Timeframe

from core.settings import get_settings

from ..storage.data_resampling_service import (
    DataResamplingError,
    DataResamplingService,
)

logger = logging.getLogger(__name__)


class ResamplingWorkflowResult:
    """Result of a complete resampling workflow."""

    def __init__(
        self,
        symbol: str,
        source_timeframe: str,
        target_timeframes: List[str],
        success: bool,
        results: Optional[Dict[str, int]] = None,
        errors: Optional[Dict[str, str]] = None,
    ):
        self.symbol = symbol
        self.source_timeframe = source_timeframe
        self.target_timeframes = target_timeframes
        self.success = success
        self.results = results or {}
        self.errors = errors or {}

    @property
    def total_candles_created(self) -> int:
        """Get total number of candles created across all timeframes."""
        return sum(self.results.values())

    @property
    def successful_timeframes(self) -> List[str]:
        """Get list of timeframes that were successfully resampled."""
        return [
            tf
            for tf in self.target_timeframes
            if tf in self.results and self.results[tf] > 0
        ]

    @property
    def failed_timeframes(self) -> List[str]:
        """Get list of timeframes that failed to resample."""
        return [tf for tf in self.target_timeframes if tf in self.errors]

    @override
    def __str__(self) -> str:
        status = "SUCCESS" if self.success else "PARTIAL/FAILED"
        return (
            f"ResamplingWorkflowResult({self.symbol}: {status}, "
            f"{len(self.successful_timeframes)}/{len(self.target_timeframes)} timeframes, "
            f"{self.total_candles_created} total candles)"
        )


class StockMarketResamplingWorkflow:
    """Service for orchestrating complete resampling workflows for stock market data."""

    def __init__(self):
        """Initialize the resampling workflow service."""
        self.settings = get_settings()
        self.nightly_settings = self.settings.nightly_update
        self.resampling_service = DataResamplingService()

        # Define the standard resampling order (from shortest to longest)
        self.standard_timeframe_order = [
            Timeframe.FIVE_MIN.value,  # 5min
            Timeframe.FIFTEEN_MIN.value,  # 15min
            Timeframe.THIRTY_MIN.value,  # 30min
            Timeframe.ONE_HOUR.value,  # 1h
            Timeframe.TWO_HOUR.value,  # 2h
            Timeframe.FOUR_HOUR.value,  # 4h
            Timeframe.DAILY.value,  # daily
        ]

    def get_target_timeframes(
        self, custom_timeframes: Optional[List[str]] = None
    ) -> List[str]:
        """
        Get the list of target timeframes for resampling.

        Args:
            custom_timeframes: Optional custom list of timeframes

        Returns:
            List of timeframes in proper resampling order
        """
        if custom_timeframes is not None:
            # Filter and order custom timeframes
            ordered_timeframes: List[str] = []
            for tf in self.standard_timeframe_order:
                if tf in custom_timeframes:
                    ordered_timeframes.append(tf)
            return ordered_timeframes
        else:
            # Use configured target timeframes
            configured_timeframes = self.nightly_settings.target_timeframes
            ordered_timeframes = []
            for tf in self.standard_timeframe_order:
                if tf in configured_timeframes:
                    ordered_timeframes.append(tf)
            return ordered_timeframes

    def resample_symbol_complete_workflow(
        self,
        symbol: str,
        source_timeframe: str = "1min",
        target_timeframes: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        stop_on_error: bool = False,
    ) -> ResamplingWorkflowResult:
        """
        Execute complete resampling workflow for a single symbol.

        Args:
            symbol: Trading symbol to resample
            source_timeframe: Source timeframe (typically "1min")
            target_timeframes: List of target timeframes (uses default if None)
            start_date: Optional start date for resampling
            end_date: Optional end date for resampling
            stop_on_error: Whether to stop the workflow on first error

        Returns:
            ResamplingWorkflowResult with detailed results
        """
        if target_timeframes is None:
            target_timeframes = self.get_target_timeframes()

        logger.info(
            f"Starting complete resampling workflow for {symbol} from {source_timeframe} "
            f"to {len(target_timeframes)} timeframes"
        )

        results: Dict[str, int] = {}
        errors: Dict[str, str] = {}

        for target_timeframe in target_timeframes:
            try:
                logger.info(
                    f"Resampling {symbol} from {source_timeframe} to {target_timeframe}"
                )

                candles_created = self.resampling_service.resample_and_store(
                    symbol=symbol,
                    from_timeframe=source_timeframe,
                    to_timeframe=target_timeframe,
                    start_date=start_date,
                    end_date=end_date,
                )

                results[target_timeframe] = candles_created
                logger.info(
                    f"Successfully created {candles_created} {target_timeframe} candles "
                    f"for {symbol}"
                )

            except DataResamplingError as e:
                error_msg = str(e)
                errors[target_timeframe] = error_msg
                logger.error(
                    f"Failed to resample {symbol} to {target_timeframe}: {error_msg}"
                )

                if stop_on_error:
                    logger.warning(
                        f"Stopping resampling workflow for {symbol} due to error "
                        f"in {target_timeframe}"
                    )
                    break

            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                errors[target_timeframe] = error_msg
                logger.error(
                    f"Unexpected error resampling {symbol} to {target_timeframe}: {error_msg}"
                )

                if stop_on_error:
                    logger.warning(
                        f"Stopping resampling workflow for {symbol} due to unexpected error"
                    )
                    break

        # Determine overall success
        success = not errors

        workflow_result = ResamplingWorkflowResult(
            symbol=symbol,
            source_timeframe=source_timeframe,
            target_timeframes=target_timeframes,
            success=success,
            results=results,
            errors=errors,
        )

        logger.info(f"Completed resampling workflow for {symbol}: {workflow_result}")
        return workflow_result

    def resample_multiple_symbols_complete_workflow(
        self,
        symbols: List[str],
        source_timeframe: str = "1min",
        target_timeframes: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        stop_on_symbol_error: bool = False,
    ) -> Dict[str, ResamplingWorkflowResult]:
        """
        Execute complete resampling workflow for multiple symbols.

        Args:
            symbols: List of trading symbols to resample
            source_timeframe: Source timeframe (typically "1min")
            target_timeframes: List of target timeframes (uses default if None)
            start_date: Optional start date for resampling
            end_date: Optional end date for resampling
            stop_on_symbol_error: Whether to stop processing other symbols on error

        Returns:
            Dictionary mapping symbol to ResamplingWorkflowResult
        """
        if target_timeframes is None:
            target_timeframes = self.get_target_timeframes()

        logger.info(
            f"Starting complete resampling workflow for {len(symbols)} symbols "
            f"to {len(target_timeframes)} timeframes"
        )

        workflow_results: Dict[str, ResamplingWorkflowResult] = {}

        for symbol in symbols:
            try:
                result = self.resample_symbol_complete_workflow(
                    symbol=symbol,
                    source_timeframe=source_timeframe,
                    target_timeframes=target_timeframes,
                    start_date=start_date,
                    end_date=end_date,
                    stop_on_error=False,  # Don't stop on timeframe errors within a symbol
                )

                workflow_results[symbol] = result

                if not result.success and stop_on_symbol_error:
                    logger.warning(
                        f"Stopping multi-symbol workflow due to errors in {symbol}"
                    )
                    break

            except Exception as e:
                logger.error(f"Failed to process resampling workflow for {symbol}: {e}")

                # Create error result
                workflow_results[symbol] = ResamplingWorkflowResult(
                    symbol=symbol,
                    source_timeframe=source_timeframe,
                    target_timeframes=target_timeframes,
                    success=False,
                    errors={"workflow": f"Workflow failed: {str(e)}"},
                )

                if stop_on_symbol_error:
                    logger.warning(
                        f"Stopping multi-symbol workflow due to workflow error in {symbol}"
                    )
                    break

        # Log summary
        successful_symbols = sum(
            1 for result in workflow_results.values() if result.success
        )
        failed_symbols = len(workflow_results) - successful_symbols
        total_candles = sum(
            result.total_candles_created for result in workflow_results.values()
        )

        logger.info(
            f"Completed multi-symbol resampling workflow: {successful_symbols} successful, "
            f"{failed_symbols} failed, {total_candles} total candles created"
        )

        return workflow_results

    def get_workflow_summary(
        self, workflow_results: Dict[str, ResamplingWorkflowResult]
    ) -> Dict[str, Any]:
        """
        Generate a summary of workflow results.

        Args:
            workflow_results: Dictionary of workflow results

        Returns:
            Summary statistics dictionary
        """
        if not workflow_results:
            return {
                "total_symbols": 0,
                "successful_symbols": 0,
                "failed_symbols": 0,
                "total_candles_created": 0,
                "timeframe_summary": {},
                "error_summary": {},
            }

        successful_symbols = sum(
            1 for result in workflow_results.values() if result.success
        )
        failed_symbols = len(workflow_results) - successful_symbols
        total_candles = sum(
            result.total_candles_created for result in workflow_results.values()
        )

        # Summarize by timeframe
        timeframe_summary: Dict[str, int] = {}
        for result in workflow_results.values():
            for timeframe, count in result.results.items():
                timeframe_summary[timeframe] = (
                    timeframe_summary.get(timeframe, 0) + count
                )

        # Summarize errors
        error_summary: Dict[str, List[str]] = {}
        for result in workflow_results.values():
            for timeframe, error in result.errors.items():
                if timeframe not in error_summary:
                    error_summary[timeframe] = []
                error_summary[timeframe].append(f"{result.symbol}: {error}")

        return {
            "total_symbols": len(workflow_results),
            "successful_symbols": successful_symbols,
            "failed_symbols": failed_symbols,
            "total_candles_created": total_candles,
            "timeframe_summary": timeframe_summary,
            "error_summary": error_summary,
        }

    def resample_daily_background(
        self,
        symbols: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, int]:
        """
        Background task method to resample 1min data to daily candles.

        This method is designed to be called from background tasks and provides
        simplified daily-only resampling with comprehensive error handling.

        Args:
            symbols: List of trading symbols to resample
            start_date: Optional start date for resampling
            end_date: Optional end date for resampling

        Returns:
            Dictionary mapping symbol to number of daily candles created
        """
        try:
            logger.info(
                f"Starting background daily resampling for {len(symbols)} symbols"
            )

            # Use bulk_resample from the resampling service for daily-only resampling
            results = self.resampling_service.bulk_resample(
                symbols=symbols,
                from_timeframe="1min",
                to_timeframe="daily",
                start_date=start_date,
                end_date=end_date,
            )

            total_candles = sum(results.values())
            logger.info(
                f"Background daily resampling completed: {total_candles} daily candles created"
            )

            return results

        except Exception as e:
            logger.error(f"Background daily resampling failed: {e}")
            # Return empty results on failure to maintain consistent return type
            return dict.fromkeys(symbols, 0)
