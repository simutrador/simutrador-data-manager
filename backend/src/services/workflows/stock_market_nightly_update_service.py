"""
Stock market nightly update service.

This service orchestrates the complete nightly update process for stock market data:
- Validates existing data completeness
- Downloads missing 1-minute data
- Resamples to all target timeframes
- Handles market hours and trading day logic
"""

import asyncio
import logging
from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import override

from simutrador_core.models.price_data import DataUpdateStatus, Timeframe

from core.settings import get_settings
from models.nightly_update_api import (
    NightlyUpdateRequest,
    NightlyUpdateResponse,
    NightlyUpdateSummary,
    SymbolUpdateResult,
    ValidationResultModel,
)

from ..progress.nightly_update_progress_service import (
    NightlyUpdateProgressService,
)
from ..storage.data_resampling_service import DataResamplingService
from ..storage.data_storage_service import DataStorageService
from ..validation.stock_market_validation_service import (
    StockMarketValidationService,
    ValidationResult,
)
from .stock_market_resampling_workflow import (
    StockMarketResamplingWorkflow,
)
from .trading_data_updating_service import TradingDataUpdatingService

logger = logging.getLogger(__name__)


class NightlyUpdateResult:
    """Result of a nightly update operation."""

    def __init__(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        success: bool,
        validation_results: list[ValidationResult] | None = None,
        update_statuses: list[DataUpdateStatus] | None = None,
        resampling_results: dict[str, int] | None = None,
        error_message: str | None = None,
    ):
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.success = success
        self.validation_results = validation_results or []
        self.update_statuses = update_statuses or []
        self.resampling_results = resampling_results or {}
        self.error_message = error_message

    @property
    def total_candles_updated(self) -> int:
        """Get total number of 1-minute candles updated."""
        return sum(
            status.records_updated for status in self.update_statuses if status.success
        )

    @property
    def total_resampled_candles(self) -> int:
        """Get total number of resampled candles across all timeframes."""
        return sum(self.resampling_results.values())

    @override
    def __str__(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        return (
            f"NightlyUpdateResult({self.symbol}: {status}, "
            f"{self.total_candles_updated} 1min candles, "
            f"{self.total_resampled_candles} resampled candles)"
        )


class StockMarketNightlyUpdateService:
    """Service for orchestrating nightly stock market data updates."""

    def __init__(self):
        """Initialize the nightly update service."""
        self.settings = get_settings()
        self.nightly_settings = self.settings.nightly_update

        # Initialize dependent services
        self.validation_service = StockMarketValidationService()
        self.updating_service = TradingDataUpdatingService()
        self.resampling_service = DataResamplingService()
        self.resampling_workflow = StockMarketResamplingWorkflow()
        self.storage_service = DataStorageService()

    def get_default_symbols(self) -> list[str]:
        """
        Get the default list of symbols for nightly updates.

        Returns:
            Combined list of large cap and mid cap symbols
        """
        return (
            self.nightly_settings.large_cap_symbols
            + self.nightly_settings.mid_cap_symbols
        )

    def get_update_date_range(
        self,
        symbol: str,
        custom_start_date: date | None = None,
        custom_end_date: date | None = None,
    ) -> tuple[date, date]:
        """
        Determine the date range that needs updating for a symbol.

        Args:
            symbol: Trading symbol to check
            custom_start_date: Optional custom start date to override automatic detection
            custom_end_date: Optional custom end date to override default (yesterday)

        Returns:
            Tuple of (start_date, end_date) for updates
        """
        # Use custom start date if provided, otherwise determine automatically
        if custom_start_date is not None:
            start_date = custom_start_date
        else:
            # Get the last update date from storage
            last_update = self.storage_service.get_last_update_date(
                symbol, Timeframe.ONE_MIN.value
            )

            if last_update is None:
                # No existing data, start from 30 days ago (reasonable default)
                start_date = date.today() - timedelta(days=30)
            else:
                # Start from the last update date to prevent gaps from partial downloads
                start_date = last_update.date()

        # Use custom end date if provided, otherwise default to yesterday
        if custom_end_date is not None:
            end_date = custom_end_date
        else:
            # End date is yesterday (don't update today's incomplete data)
            end_date = date.today() - timedelta(days=1)

        # Ensure we only update trading days
        while start_date <= end_date and not self.validation_service.is_trading_day(
            start_date
        ):
            start_date += timedelta(days=1)

        while end_date >= start_date and not self.validation_service.is_trading_day(
            end_date
        ):
            end_date -= timedelta(days=1)

        return start_date, end_date

    async def update_symbol_data(
        self, symbol: str, force_validation: bool = True
    ) -> NightlyUpdateResult:
        """
        Update data for a single symbol with validation and resampling.

        Args:
            symbol: Trading symbol to update
            force_validation: Whether to validate existing data before updating

        Returns:
            NightlyUpdateResult with detailed update information
        """
        try:
            logger.info(f"Starting nightly update for {symbol}")

            # Determine update date range
            start_date, end_date = self.get_update_date_range(symbol)

            if start_date > end_date:
                logger.info(f"{symbol} is up to date, no updates needed")
                return NightlyUpdateResult(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    success=True,
                    error_message="No updates needed - data is current",
                )

            logger.info(f"Updating {symbol} from {start_date} to {end_date}")

            # Step 1: Validate existing data if requested
            validation_results = []
            if force_validation and self.nightly_settings.enable_data_validation:
                logger.info(f"Validating existing data for {symbol}")
                validation_results = self.validation_service.validate_symbol_data_range(
                    symbol, start_date, end_date
                )

                invalid_days = [r for r in validation_results if not r.is_valid]
                if invalid_days:
                    logger.warning(
                        f"{symbol} has {len(invalid_days)} days with invalid data"
                    )

            # Step 2: Update 1-minute data
            logger.info(f"Updating 1-minute data for {symbol}")
            update_statuses = await self.updating_service.update_symbol_data(
                symbol=symbol,
                timeframes=[Timeframe.ONE_MIN.value],
                start_date=start_date,
                end_date=end_date,
                force_update=False,
            )

            # Check if 1-minute update was successful
            one_min_status = update_statuses[0] if update_statuses else None
            if not one_min_status or not one_min_status.success:
                error_msg = (
                    one_min_status.error_message if one_min_status else "Unknown error"
                )
                logger.error(
                    f"Failed to update 1-minute data for {symbol}: {error_msg}"
                )
                return NightlyUpdateResult(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    success=False,
                    validation_results=validation_results,
                    update_statuses=update_statuses,
                    error_message=f"1-minute data update failed: {error_msg}",
                )

            # Step 3: Resample to all target timeframes using workflow
            resampling_results = {}
            if (
                self.nightly_settings.enable_auto_resampling
                and one_min_status.records_updated > 0
            ):
                logger.info(f"Resampling data for {symbol} to target timeframes")

                workflow_result = (
                    self.resampling_workflow.resample_symbol_complete_workflow(
                        symbol=symbol,
                        source_timeframe=Timeframe.ONE_MIN.value,
                        target_timeframes=None,  # Use default configured timeframes
                        start_date=start_date,
                        end_date=end_date,
                        stop_on_error=False,
                    )
                )

                resampling_results = workflow_result.results

                if workflow_result.errors:
                    for timeframe, error in workflow_result.errors.items():
                        logger.error(
                            f"Failed to resample {symbol} to {timeframe}: {error}"
                        )

            logger.info(f"Completed nightly update for {symbol}")
            return NightlyUpdateResult(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                success=True,
                validation_results=validation_results,
                update_statuses=update_statuses,
                resampling_results=resampling_results,
            )

        except Exception as e:
            logger.error(f"Nightly update failed for {symbol}: {e}")
            return NightlyUpdateResult(
                symbol=symbol,
                start_date=date.today(),
                end_date=date.today(),
                success=False,
                error_message=f"Update failed: {str(e)}",
            )

    async def update_multiple_symbols(
        self, symbols: list[str] | None = None, max_concurrent: int | None = None
    ) -> dict[str, NightlyUpdateResult]:
        """
        Update data for multiple symbols concurrently.

        Args:
            symbols: List of symbols to update (defaults to configured symbols)
            max_concurrent: Maximum concurrent updates (defaults to configured value)

        Returns:
            Dictionary mapping symbol to NightlyUpdateResult
        """
        if symbols is None:
            symbols = self.get_default_symbols()

        if max_concurrent is None:
            max_concurrent = self.nightly_settings.max_concurrent_symbols

        logger.info(
            f"Starting nightly update for {len(symbols)} symbols "
            f"with max_concurrent={max_concurrent}"
        )

        # Create semaphore to limit concurrent updates
        semaphore = asyncio.Semaphore(max_concurrent)

        async def update_with_semaphore(symbol: str) -> tuple[str, NightlyUpdateResult]:
            async with semaphore:
                result = await self.update_symbol_data(symbol)
                return symbol, result

        # Execute updates concurrently
        tasks = [update_with_semaphore(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        update_results: dict[str, NightlyUpdateResult] = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Unexpected error during nightly update: {result}")
                continue

            # result should be a tuple of (symbol, update_result)
            if isinstance(result, tuple) and len(result) == 2:
                symbol, update_result = result
                update_results[symbol] = update_result
            else:
                logger.error(f"Unexpected result format: {result}")

        # Log summary
        successful_updates = sum(
            1 for result in update_results.values() if result.success
        )
        failed_updates = len(update_results) - successful_updates
        total_candles = sum(
            result.total_candles_updated for result in update_results.values()
        )
        total_resampled = sum(
            result.total_resampled_candles for result in update_results.values()
        )

        logger.info(
            f"Nightly update completed: {successful_updates} successful, {failed_updates} failed, "
            f"{total_candles} 1min candles, {total_resampled} resampled candles"
        )

        return update_results

    async def update_multiple_symbols_with_progress(
        self,
        symbols: list[str] | None = None,
        max_concurrent: int | None = None,
        progress_callback: Callable[[str, str, float, str, str | None], None] | None = None,
        request_id: str | None = None,
        custom_start_date: date | None = None,
        custom_end_date: date | None = None,
        force_validation: bool = True,
        enable_resampling: bool = True,
    ) -> dict[str, NightlyUpdateResult]:
        """
        Update data for multiple symbols concurrently with progress tracking.

        Args:
            symbols: List of symbols to update (defaults to configured symbols)
            max_concurrent: Maximum concurrent updates (defaults to configured value)
            progress_callback: Optional callback function for progress updates
                              Signature: (symbol, status, progress_percentage, current_step, \
                                error_message)
            request_id: Optional request ID for progress tracking
            custom_start_date: Optional custom start date to override automatic detection
            custom_end_date: Optional custom end date to override default (yesterday)
            force_validation: Whether to validate existing data before updating
            enable_resampling: Whether to enable automatic resampling

        Returns:
            Dictionary mapping symbol to NightlyUpdateResult
        """
        if symbols is None:
            symbols = self.get_default_symbols()

        if max_concurrent is None:
            max_concurrent = self.nightly_settings.max_concurrent_symbols

        logger.info(
            f"Starting nightly update with progress tracking for {len(symbols)} symbols "
            f"with max_concurrent={max_concurrent}"
        )

        # Create semaphore to limit concurrent updates
        semaphore = asyncio.Semaphore(max_concurrent)

        async def update_symbol_with_progress(
            symbol: str,
        ) -> tuple[str, NightlyUpdateResult]:
            async with semaphore:
                # Initialize dates to avoid unbound variable issues
                start_date = date.today()
                end_date = date.today()

                try:
                    # Update progress: starting
                    if progress_callback and request_id:
                        progress_callback(
                            symbol, "validating", 10.0, "Starting validation", None
                        )

                    # Get the update date range (use custom dates if provided)
                    start_date, end_date = self.get_update_date_range(
                        symbol, custom_start_date, custom_end_date
                    )

                    if start_date > end_date:
                        if progress_callback and request_id:
                            progress_callback(
                                symbol, "completed", 100.0, "No updates needed", None
                            )
                        return symbol, NightlyUpdateResult(
                            symbol=symbol,
                            start_date=start_date,
                            end_date=end_date,
                            success=True,
                            error_message="No updates needed - data is current",
                        )

                    # Update progress: validation
                    if progress_callback and request_id:
                        progress_callback(
                            symbol, "validating", 20.0, "Validating existing data", None
                        )

                    # Validate existing data if requested
                    validation_results = []
                    if (
                        force_validation
                        and self.nightly_settings.enable_data_validation
                    ):
                        validation_results = (
                            self.validation_service.validate_symbol_data_range(
                                symbol, start_date, end_date
                            )
                        )

                    # Update progress: downloading
                    if progress_callback and request_id:
                        progress_callback(
                            symbol,
                            "downloading",
                            40.0,
                            "Downloading 1-minute data",
                            None,
                        )

                    # Update 1-minute data
                    update_statuses = await self.updating_service.update_symbol_data(
                        symbol=symbol,
                        timeframes=[Timeframe.ONE_MIN.value],
                        start_date=start_date,
                        end_date=end_date,
                        force_update=False,
                    )

                    # Check if 1-minute update was successful
                    one_min_status = update_statuses[0] if update_statuses else None
                    if not one_min_status or not one_min_status.success:
                        error_msg = (
                            one_min_status.error_message
                            if one_min_status
                            else "Unknown error"
                        )
                        if progress_callback and request_id:
                            progress_callback(
                                symbol,
                                "failed",
                                100.0,
                                "Failed to download data",
                                error_msg,
                            )
                        return symbol, NightlyUpdateResult(
                            symbol=symbol,
                            start_date=start_date,
                            end_date=end_date,
                            success=False,
                            validation_results=validation_results,
                            update_statuses=update_statuses,
                            error_message=f"1-minute data update failed: {error_msg}",
                        )

                    # Update progress: resampling
                    resampling_results = {}
                    if (
                        self.nightly_settings.enable_auto_resampling
                        and enable_resampling
                        and one_min_status.records_updated > 0
                    ):
                        if progress_callback and request_id:
                            progress_callback(
                                symbol,
                                "resampling",
                                70.0,
                                "Resampling to other timeframes",
                                None,
                            )

                        workflow_result = (
                            self.resampling_workflow.resample_symbol_complete_workflow(
                                symbol=symbol,
                                source_timeframe=Timeframe.ONE_MIN.value,
                                target_timeframes=None,
                                start_date=start_date,
                                end_date=end_date,
                                stop_on_error=False,
                            )
                        )
                        resampling_results = workflow_result.results

                    # Update progress: completed
                    if progress_callback and request_id:
                        progress_callback(
                            symbol, "completed", 100.0, "Processing completed", None
                        )

                    return symbol, NightlyUpdateResult(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        success=True,
                        validation_results=validation_results,
                        update_statuses=update_statuses,
                        resampling_results=resampling_results,
                    )

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Failed to update {symbol}: {error_msg}")
                    if progress_callback and request_id:
                        progress_callback(
                            symbol, "failed", 100.0, "Processing failed", error_msg
                        )
                    return symbol, NightlyUpdateResult(
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                        success=False,
                        error_message=error_msg,
                    )

        # Execute updates concurrently
        tasks = [update_symbol_with_progress(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        update_results: dict[str, NightlyUpdateResult] = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Unexpected error during nightly update: {result}")
                continue

            if isinstance(result, tuple) and len(result) == 2:
                symbol, update_result = result
                update_results[symbol] = update_result
            else:
                logger.error(f"Unexpected result format: {result}")

        # Log summary
        successful_updates = sum(
            1 for result in update_results.values() if result.success
        )
        failed_updates = len(update_results) - successful_updates
        total_candles = sum(
            result.total_candles_updated for result in update_results.values()
        )
        total_resampled = sum(
            result.total_resampled_candles for result in update_results.values()
        )

        logger.info(
            f"Nightly update with progress completed: {successful_updates} successful, "
            f"{failed_updates} failed, {total_candles} 1min candles, {total_resampled} \
                resampled candles"
        )

        return update_results

    async def execute_nightly_update(
        self,
        request_id: str,
        request: NightlyUpdateRequest,
        progress_service: NightlyUpdateProgressService,
        completed_updates_storage: dict[str, NightlyUpdateResponse],
    ) -> None:
        """
        Execute the nightly update process in the background.

        This method orchestrates the complete nightly update workflow including:
        - Progress tracking and status updates
        - Symbol data updates with validation and resampling
        - Result aggregation and response creation
        - Error handling and cleanup

        Args:
            request_id: Unique identifier for this update request
            request: The nightly update request parameters
            progress_service: Service for tracking progress and status
            completed_updates_storage: Dictionary to store completed results
        """
        try:
            # Update status
            active_update = progress_service.get_active_update(request_id)
            if active_update:
                active_update.status = "running"

            start_time = datetime.now()

            # Execute the update with progress tracking using the service method
            def progress_callback(
                symbol: str,
                status: str,
                progress_percentage: float,
                current_step: str,
                error_message: str | None,
            ) -> None:
                progress_service.update_symbol_progress(
                    request_id,
                    symbol,
                    status,
                    progress_percentage,
                    current_step,
                    error_message,
                )

            results = await self.update_multiple_symbols_with_progress(
                symbols=request.symbols,
                max_concurrent=request.max_concurrent,
                progress_callback=progress_callback,
                request_id=request_id,
                custom_start_date=request.start_date,
                custom_end_date=request.end_date,
                force_validation=request.force_validation,
                enable_resampling=request.enable_resampling,
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Convert results to API models
            symbol_results: dict[str, SymbolUpdateResult] = {}
            for symbol, result in results.items():
                # Convert validation results
                validation_models: list[ValidationResultModel] = []
                for val_result in result.validation_results:
                    validation_models.append(
                        ValidationResultModel(
                            symbol=val_result.symbol,
                            validation_date=val_result.validation_date,
                            is_valid=val_result.is_valid,
                            expected_candles=val_result.expected_candles,
                            actual_candles=val_result.actual_candles,
                            missing_periods=[
                                f"{start} to {end}"
                                for start, end in val_result.missing_periods
                            ],
                            errors=val_result.errors,
                            warnings=val_result.warnings,
                        )
                    )

                # Create validation summary
                validation_summary = {
                    "total_validations": len(validation_models),
                    "valid_days": sum(1 for v in validation_models if v.is_valid),
                    "invalid_days": sum(1 for v in validation_models if not v.is_valid),
                    "total_errors": sum(len(v.errors) for v in validation_models),
                    "total_warnings": sum(len(v.warnings) for v in validation_models),
                }

                symbol_results[symbol] = SymbolUpdateResult(
                    symbol=result.symbol,
                    start_date=result.start_date,
                    end_date=result.end_date,
                    success=result.success,
                    candles_updated=result.total_candles_updated,
                    update_duration_seconds=None,  # Individual symbol duration not tracked
                    validation_results=validation_models,
                    validation_summary=validation_summary,
                    resampling_results=result.resampling_results,
                    total_resampled_candles=result.total_resampled_candles,
                    error_message=result.error_message,
                )

            # Create summary
            successful_updates = sum(1 for r in results.values() if r.success)
            failed_updates = len(results) - successful_updates
            total_candles = sum(r.total_candles_updated for r in results.values())
            total_resampled = sum(r.total_resampled_candles for r in results.values())

            # Calculate resampling summary
            resampling_summary: dict[str, int] = {}
            for result in results.values():
                for timeframe, count in result.resampling_results.items():
                    resampling_summary[timeframe] = (
                        resampling_summary.get(timeframe, 0) + count
                    )

            # Calculate date range across all symbols
            earliest_start_date = None
            latest_end_date = None
            if results:
                earliest_start_date = min(r.start_date for r in results.values())
                latest_end_date = max(r.end_date for r in results.values())

            # Calculate validation statistics
            symbols_with_validation_errors = sum(
                1
                for r in results.values()
                if any(not val.is_valid for val in r.validation_results)
            )
            total_validation_errors = sum(
                sum(len(val.errors) for val in r.validation_results)
                for r in results.values()
            )

            summary = NightlyUpdateSummary(
                total_symbols=len(results),
                successful_updates=successful_updates,
                failed_updates=failed_updates,
                total_candles_updated=total_candles,
                total_resampled_candles=total_resampled,
                update_duration_seconds=duration,
                earliest_start_date=earliest_start_date,
                latest_end_date=latest_end_date,
                symbols_with_validation_errors=symbols_with_validation_errors,
                total_validation_errors=total_validation_errors,
                resampling_summary=resampling_summary,
            )

            # Create response
            response = NightlyUpdateResponse(
                request_id=request_id,
                started_at=start_time,
                completed_at=end_time,
                summary=summary,
                symbol_results=symbol_results,
                symbols_requested=request.symbols,
                symbols_processed=list(results.keys()),
                max_concurrent_used=request.max_concurrent
                or self.nightly_settings.max_concurrent_symbols,
                overall_success=failed_updates == 0,
            )

            # Store completed result
            completed_updates_storage[request_id] = response

            # Remove from active updates and clean up progress tracking
            progress_service.remove_active_update(request_id)
            # Keep progress tracking for completed requests for a while
            # In production, you might want to clean this up after some time

            logger.info(
                f"Nightly update {request_id} completed: {successful_updates} successful, "
                f"{failed_updates} failed"
            )

        except Exception as e:
            logger.error(f"Nightly update {request_id} failed: {e}")

            # Update status with error
            active_update = progress_service.get_active_update(request_id)
            if active_update:
                active_update.status = "failed"
                active_update.error = str(e)
