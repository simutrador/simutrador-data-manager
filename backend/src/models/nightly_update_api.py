"""
API models for nightly update endpoints.

This module defines request and response models for the nightly update system,
including detailed status tracking, validation results, and error reporting.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field

# Type alias to avoid forward reference issues
GapFillResultList = list["GapFillResult"]


class ProgressInfo(BaseModel):
    """Progress information for a nightly update."""

    total_symbols: int = Field(..., description="Total number of symbols to process")
    completed_symbols: int = Field(..., description="Number of symbols completed")
    current_symbol: str | None = Field(
        default=None, description="Currently processing symbol"
    )
    current_step: str = Field(..., description="Current processing step")
    progress_percentage: float = Field(
        ..., ge=0, le=100, description="Overall progress percentage"
    )
    estimated_time_remaining_seconds: int | None = Field(
        default=None, description="Estimated time remaining in seconds"
    )
    symbols_in_progress: list[str] = Field(
        default_factory=list, description="List of symbols currently being processed"
    )


class SymbolProgress(BaseModel):
    """Progress information for a single symbol."""

    symbol: str = Field(..., description="Trading symbol")
    status: str = Field(
        ...,
        description="Current status (pending, validating, downloading, resampling, completed,\
              failed)",
    )
    progress_percentage: float = Field(
        ..., ge=0, le=100, description="Symbol progress percentage"
    )
    current_step: str = Field(..., description="Current processing step")
    error_message: str | None = Field(
        default=None, description="Error message if failed"
    )
    started_at: datetime | None = Field(
        default=None, description="When processing started"
    )
    completed_at: datetime | None = Field(
        default=None, description="When processing completed"
    )


class ActiveUpdateInfo(BaseModel):
    """Information about an active nightly update request."""

    request: NightlyUpdateRequest = Field(..., description="Original update request")
    started_at: datetime = Field(..., description="When the update was started")
    status: str = Field(
        ...,
        description="Current status (starting, running, failed)",
    )
    symbols: list[str] = Field(..., description="List of symbols being processed")
    error: str | None = Field(
        default=None, description="Error message if update failed"
    )


class NightlyUpdateRequest(BaseModel):
    """Request model for nightly update endpoint."""

    symbols: list[str] | None = Field(
        None,
        description=(
            "Optional list of symbols to update. If not provided, "
            "uses default symbol lists from settings"
        ),
    )
    force_validation: bool = Field(
        default=True, description="Whether to validate existing data before updating"
    )
    max_concurrent: int | None = Field(
        None,
        gt=0,
        description=(
            "Maximum number of concurrent symbol updates. "
            "If not provided, uses setting default. Must be greater than 0."
        ),
    )
    enable_resampling: bool = Field(
        default=True,
        description="Whether to automatically resample to other timeframes after 1min update",
    )
    start_date: date | None = Field(
        None,
        description=(
            "Optional start date for the update range. If not provided, "
            "automatically determines based on last update date"
        ),
    )
    end_date: date | None = Field(
        None,
        description=(
            "Optional end date for the update range. If not provided, "
            "defaults to yesterday (current date - 1 day)"
        ),
    )


class GapFillResult(BaseModel):
    """Result of attempting to fill a data gap."""

    start_time: str = Field(..., description="Start time of the gap (ISO format)")
    end_time: str = Field(..., description="End time of the gap (ISO format)")
    attempted: bool = Field(..., description="Whether gap filling was attempted")
    success: bool = Field(..., description="Whether gap filling was successful")
    candles_recovered: int = Field(default=0, description="Number of candles recovered")
    vendor_unavailable: bool = Field(
        default=False, description="Whether data is unavailable from vendor"
    )
    polygon_api_url: str | None = Field(
        default=None, description="Polygon API URL used for this gap-filling attempt"
    )
    trades_api_url: str | None = Field(
        default=None,
        description="Polygon Trades API URL used to check trading activity",
    )
    has_trading_activity: bool | None = Field(
        default=None,
        description="Whether trading activity was detected during the gap period",
    )
    error_message: str | None = Field(
        default=None, description="Error message if gap filling failed"
    )


class SymbolCompletenessRawData(BaseModel):
    """Model for raw symbol completeness data from validation service."""

    # Core fields (always present)
    total_trading_days: int = Field(..., description="Total trading days in the period")
    valid_days: int = Field(..., description="Number of days with valid data")
    invalid_days: int = Field(..., description="Number of days with invalid data")
    completeness_percentage: float = Field(
        ..., description="Overall completeness percentage"
    )
    total_expected_candles: int = Field(..., description="Total expected candles")
    total_actual_candles: int = Field(..., description="Total actual candles found")
    missing_candles: int = Field(..., description="Number of missing candles")
    validation_results: list[Any] = Field(
        ..., description="List of ValidationResult objects"
    )

    # Enhanced metrics (optional, added over time)
    full_days_count: int = Field(
        0, description="Number of full trading days (390 candles)"
    )
    half_days_count: int = Field(
        0, description="Number of half trading days (210 candles)"
    )
    days_with_gaps: int = Field(0, description="Number of days with data gaps")
    total_missing_periods: int = Field(0, description="Total number of missing periods")
    average_daily_completeness: float = Field(
        0.0, description="Average daily completeness percentage"
    )
    worst_day_completeness: float = Field(
        100.0, description="Worst single day completeness percentage"
    )
    best_day_completeness: float = Field(
        0.0, description="Best single day completeness percentage"
    )

    # Gap filling fields (optional, only present when gap filling is attempted)
    gap_fill_attempted: bool = Field(
        False, description="Whether gap filling was attempted"
    )
    total_gaps_found: int = Field(0, description="Total number of gaps found")
    gaps_filled_successfully: int = Field(
        0, description="Number of gaps filled successfully"
    )
    gaps_vendor_unavailable: int = Field(
        0, description="Number of gaps where vendor data unavailable"
    )
    candles_recovered: int = Field(
        0, description="Total candles recovered through gap filling"
    )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SymbolCompletenessRawData:
        """Create instance from dictionary with safe field access."""
        return cls(
            total_trading_days=data["total_trading_days"],
            valid_days=data["valid_days"],
            invalid_days=data["invalid_days"],
            completeness_percentage=data["completeness_percentage"],
            total_expected_candles=data["total_expected_candles"],
            total_actual_candles=data["total_actual_candles"],
            missing_candles=data["missing_candles"],
            validation_results=data["validation_results"],
            full_days_count=data.get("full_days_count", 0),
            half_days_count=data.get("half_days_count", 0),
            days_with_gaps=data.get("days_with_gaps", 0),
            total_missing_periods=data.get("total_missing_periods", 0),
            average_daily_completeness=data.get("average_daily_completeness", 0.0),
            worst_day_completeness=data.get("worst_day_completeness", 100.0),
            best_day_completeness=data.get("best_day_completeness", 0.0),
            gap_fill_attempted=data.get("gap_fill_attempted", False),
            total_gaps_found=data.get("total_gaps_found", 0),
            gaps_filled_successfully=data.get("gaps_filled_successfully", 0),
            gaps_vendor_unavailable=data.get("gaps_vendor_unavailable", 0),
            candles_recovered=data.get("candles_recovered", 0),
        )


class ValidationResultModel(BaseModel):
    """Model for data validation results."""

    symbol: str = Field(..., description="Trading symbol")
    validation_date: date = Field(..., description="Date that was validated")
    is_valid: bool = Field(..., description="Whether the data is valid")
    expected_candles: int = Field(..., description="Expected number of candles")
    actual_candles: int = Field(..., description="Actual number of candles found")
    missing_candles: int = Field(
        default=0, description="Number of missing candles for this day"
    )
    completeness_percentage: float = Field(
        default=0.0, description="Completeness percentage for this specific day"
    )
    is_half_day: bool = Field(
        default=False, description="Whether this is a half trading day"
    )
    missing_periods: list[str] = Field(
        default_factory=list,
        description="List of missing time periods (formatted as strings)",
    )
    missing_periods_count: int = Field(
        default=0, description="Number of distinct missing periods"
    )
    largest_gap_minutes: int = Field(
        default=0, description="Largest continuous gap in minutes"
    )
    polygon_urls_for_missing_periods: list[str] = Field(
        default_factory=list,
        description="List of Polygon API URLs for each missing period",
    )
    gap_fill_results: GapFillResultList = Field(  # type: ignore
        default_factory=list, description="Results of gap filling attempts"
    )
    errors: list[str] = Field(
        default_factory=list, description="List of validation errors"
    )
    warnings: list[str] = Field(
        default_factory=list, description="List of validation warnings"
    )


class SymbolUpdateResult(BaseModel):
    """Result of updating a single symbol."""

    symbol: str = Field(..., description="Trading symbol")
    start_date: date = Field(..., description="Start date of update range")
    end_date: date = Field(..., description="End date of update range")
    success: bool = Field(..., description="Whether the update was successful")

    # Data update information
    candles_updated: int = Field(
        default=0, description="Number of 1-minute candles updated"
    )
    update_duration_seconds: float | None = Field(
        None, description="Duration of the update process in seconds"
    )

    # Validation results
    validation_results: list[ValidationResultModel] = Field(  # type: ignore
        default_factory=list, description="Validation results for each trading day"
    )
    validation_summary: dict[str, Any] | None = Field(
        None, description="Summary of validation results"
    )

    # Resampling results
    resampling_results: dict[str, int] = Field(
        default_factory=dict,
        description="Number of candles created for each resampled timeframe",
    )
    total_resampled_candles: int = Field(
        default=0, description="Total number of resampled candles across all timeframes"
    )

    # Error information
    error_message: str | None = Field(
        None, description="Error message if update failed"
    )
    warnings: list[str] = Field(
        default_factory=list, description="List of warnings during update"
    )


class NightlyUpdateSummary(BaseModel):
    """Summary statistics for the nightly update operation."""

    total_symbols: int = Field(..., description="Total number of symbols processed")
    successful_updates: int = Field(..., description="Number of successful updates")
    failed_updates: int = Field(..., description="Number of failed updates")

    total_candles_updated: int = Field(
        default=0, description="Total 1-minute candles updated across all symbols"
    )
    total_resampled_candles: int = Field(
        default=0, description="Total resampled candles created across all symbols"
    )

    update_duration_seconds: float = Field(
        ..., description="Total duration of the update process"
    )

    # Date range information
    earliest_start_date: date | None = Field(
        None, description="Earliest start date across all symbol updates"
    )
    latest_end_date: date | None = Field(
        None, description="Latest end date across all symbol updates"
    )

    # Validation summary
    symbols_with_validation_errors: int = Field(
        default=0, description="Number of symbols with validation errors"
    )
    total_validation_errors: int = Field(
        default=0, description="Total number of validation errors"
    )

    # Resampling summary by timeframe
    resampling_summary: dict[str, int] = Field(
        default_factory=dict,
        description="Total candles created per timeframe across all symbols",
    )


class NightlyUpdateResponse(BaseModel):
    """Response model for nightly update endpoint."""

    request_id: str = Field(
        ..., description="Unique identifier for this update request"
    )
    started_at: datetime = Field(..., description="When the update process started")
    completed_at: datetime = Field(..., description="When the update process completed")

    summary: NightlyUpdateSummary = Field(
        ..., description="Summary of the update operation"
    )

    symbol_results: dict[str, SymbolUpdateResult] = Field(
        ..., description="Detailed results for each symbol"
    )

    # Configuration used
    symbols_requested: list[str] | None = Field(
        None, description="Symbols that were requested for update"
    )
    symbols_processed: list[str] = Field(
        ..., description="Symbols that were actually processed"
    )
    max_concurrent_used: int = Field(
        ..., description="Maximum concurrent updates that were used"
    )

    # Overall status
    overall_success: bool = Field(
        ..., description="Whether the overall operation was successful"
    )
    global_errors: list[str] = Field(
        default_factory=list,
        description="Global errors that affected the entire operation",
    )


class NightlyUpdateStatusRequest(BaseModel):
    """Request model for checking nightly update status."""

    request_id: str | None = Field(
        None, description="Request ID to check status for"
    )
    symbols: list[str] | None = Field(
        None, description="Symbols to check status for"
    )
    include_validation_details: bool = Field(
        default=False, description="Whether to include detailed validation results"
    )


class UpdateStatusResponse(BaseModel):
    """Response model for get_update_status endpoint."""

    request_id: str = Field(..., description="The request ID for this update")
    status: str = Field(..., description="Current status of the update")
    started_at: datetime = Field(..., description="When the update was started")
    symbols_count: int = Field(
        ..., description="Total number of symbols being processed"
    )
    is_complete: bool = Field(..., description="Whether the update is complete")
    progress: ProgressInfo = Field(..., description="Progress information")
    completed_at: datetime | None = Field(
        None, description="When the update completed (only for completed updates)"
    )
    summary: NightlyUpdateSummary | None = Field(
        None, description="Summary information (only for completed updates)"
    )
    overall_success: bool | None = Field(
        None, description="Overall success status (only for completed updates)"
    )


class UpdateProgressDetailsResponse(BaseModel):
    """Response model for get_update_progress_details endpoint."""

    request_id: str = Field(..., description="The request ID for this update")
    overall_progress: ProgressInfo = Field(
        ..., description="Overall progress information"
    )
    symbol_progress: dict[str, SymbolProgress] = Field(
        ..., description="Progress information for each symbol"
    )


class ActiveUpdateSummary(BaseModel):
    """Information about an active nightly update for the list endpoint."""

    request_id: str = Field(..., description="The request ID for this update")
    status: str = Field(..., description="Current status of the update")
    started_at: datetime = Field(..., description="When the update was started")
    symbols_count: int = Field(
        ..., description="Total number of symbols being processed"
    )
    duration_seconds: float = Field(
        ..., description="Duration since the update started in seconds"
    )


class NightlyUpdateStatusResponse(BaseModel):
    """Response model for nightly update status check."""

    symbols_status: dict[str, dict[str, Any]] = Field(
        ..., description="Status information for each symbol"
    )
    last_successful_update: datetime | None = Field(
        None, description="Timestamp of last successful nightly update"
    )
    next_scheduled_update: datetime | None = Field(
        None, description="Timestamp of next scheduled update (if applicable)"
    )
    system_health: dict[str, Any] = Field(
        default_factory=dict, description="Overall system health indicators"
    )


class AnalysisPeriod(BaseModel):
    """Model for analysis period information."""

    start_date: date = Field(..., description="Start date of the analysis period")
    end_date: date = Field(..., description="End date of the analysis period")


class SymbolCompletenessData(BaseModel):
    """Model for symbol-specific completeness data."""

    total_trading_days: int = Field(..., description="Total trading days in the period")
    valid_days: int = Field(..., description="Number of days with valid data")
    invalid_days: int = Field(..., description="Number of days with invalid data")
    completeness_percentage: float = Field(
        ..., description="Percentage of data completeness"
    )
    total_expected_candles: int = Field(
        ..., description="Total expected number of candles"
    )
    total_actual_candles: int = Field(
        ..., description="Total actual number of candles found"
    )
    missing_candles: int = Field(..., description="Number of missing candles")

    # Enhanced completeness metrics
    full_days_count: int = Field(
        default=0, description="Number of full trading days (390 candles expected)"
    )
    half_days_count: int = Field(
        default=0, description="Number of half trading days (210 candles expected)"
    )
    days_with_gaps: int = Field(default=0, description="Number of days with data gaps")
    total_missing_periods: int = Field(
        default=0, description="Total number of distinct missing periods"
    )
    average_daily_completeness: float = Field(
        default=0.0, description="Average completeness percentage per day"
    )
    worst_day_completeness: float = Field(
        default=100.0, description="Worst single day completeness percentage"
    )
    best_day_completeness: float = Field(
        default=0.0, description="Best single day completeness percentage"
    )

    validation_results: list[ValidationResultModel] | None = Field(
        None, description="Detailed validation results (if requested)"
    )

    # Gap filling statistics (if auto_fill_gaps was enabled)
    gap_fill_attempted: bool = Field(
        default=False, description="Whether gap filling was attempted"
    )
    total_gaps_found: int = Field(default=0, description="Total number of gaps found")
    gaps_filled_successfully: int = Field(
        default=0, description="Number of gaps filled successfully"
    )
    gaps_vendor_unavailable: int = Field(
        default=0, description="Number of gaps where vendor data is unavailable"
    )
    candles_recovered: int = Field(
        default=0, description="Total number of candles recovered through gap filling"
    )


class OverallStatistics(BaseModel):
    """Model for overall completeness statistics."""

    total_symbols: int = Field(..., description="Total number of symbols analyzed")
    total_trading_days: int = Field(
        ..., description="Total trading days across all symbols"
    )
    total_valid_days: int = Field(
        ..., description="Total valid days across all symbols"
    )
    overall_completeness_percentage: float = Field(
        ..., description="Overall completeness percentage across all symbols"
    )
    total_expected_candles: int = Field(
        ..., description="Total expected candles across all symbols"
    )
    total_actual_candles: int = Field(
        ..., description="Total actual candles across all symbols"
    )
    total_missing_candles: int = Field(
        ..., description="Total missing candles across all symbols"
    )


class DataCompletenessRequest(BaseModel):
    """Request model for data completeness analysis."""

    symbols: list[str] = Field(..., min_length=1, description="Symbols to analyze")
    start_date: date = Field(..., description="Start date for analysis")
    end_date: date = Field(..., description="End date for analysis")
    include_details: bool = Field(
        default=False, description="Whether to include detailed validation results"
    )
    auto_fill_gaps: bool = Field(
        default=False, description="Automatically attempt to fill detected gaps"
    )
    max_gap_fill_attempts: int = Field(
        default=50, description="Maximum number of gaps to attempt filling per symbol"
    )


class DataCompletenessResponse(BaseModel):
    """Response model for data completeness analysis."""

    analysis_period: AnalysisPeriod = Field(
        ..., description="Start and end dates of the analysis"
    )

    symbol_completeness: dict[str, SymbolCompletenessData] = Field(
        ..., description="Completeness statistics for each symbol"
    )

    overall_statistics: OverallStatistics = Field(
        ..., description="Overall completeness statistics"
    )

    symbols_needing_attention: list[str] = Field(
        default_factory=list,
        description="Symbols that need immediate attention due to data issues",
    )

    recommendations: list[str] = Field(
        default_factory=list,
        description="Recommendations for improving data completeness",
    )
