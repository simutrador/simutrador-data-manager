"""
Data Analysis API endpoints.

This module provides REST API endpoints for:
- Analyzing data completeness for trading symbols
- Validating data quality and integrity
- Generating data quality reports and recommendations
"""

import logging

from fastapi import APIRouter, Depends, HTTPException

from models.nightly_update_api import (
    AnalysisPeriod,
    DataCompletenessRequest,
    DataCompletenessResponse,
    OverallStatistics,
    SymbolCompletenessData,
    SymbolCompletenessRawData,
    ValidationResultModel,
)
from services.validation.stock_market_validation_service import (
    StockMarketValidationService,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data-analysis", tags=["data-analysis"])


def get_validation_service() -> StockMarketValidationService:
    """Dependency to get validation service instance."""
    return StockMarketValidationService()


@router.post("/completeness", response_model=DataCompletenessResponse)
async def analyze_data_completeness(
    request: DataCompletenessRequest,
    validation_service: StockMarketValidationService = Depends(get_validation_service),
) -> DataCompletenessResponse:
    """
    Analyze data completeness for specified symbols and date range.

    This endpoint provides detailed analysis of data completeness including:
    - Missing data periods
    - Validation errors and warnings
    - Completeness percentages
    - Recommendations for improvement
    """
    try:
        logger.info(
            f"Analyzing data completeness for {len(request.symbols)} symbols "
            f"from {request.start_date} to {request.end_date} "
            f"(auto_fill_gaps={request.auto_fill_gaps})"
        )

        # Get completeness summary with optional gap filling
        if request.auto_fill_gaps:
            raw_completeness_data = (
                await validation_service.analyze_completeness_with_gap_filling(
                    symbols=request.symbols,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    auto_fill_gaps=True,
                    max_gap_fill_attempts=request.max_gap_fill_attempts,
                )
            )
        else:
            raw_completeness_data = validation_service.get_data_completeness_summary(
                symbols=request.symbols,
                start_date=request.start_date,
                end_date=request.end_date,
            )

        # Process the data to create proper object instances
        completeness_data: dict[str, SymbolCompletenessData] = {}
        for symbol, symbol_data_dict in raw_completeness_data.items():
            # Convert dictionary to Pydantic model for type safety
            symbol_data = SymbolCompletenessRawData.from_dict(symbol_data_dict)
            # Prepare validation results if details are requested
            validation_results = None
            if request.include_details:
                validation_results_raw = symbol_data.validation_results
                validation_results = [
                    ValidationResultModel(
                        symbol=vr.symbol,
                        validation_date=vr.validation_date,
                        is_valid=vr.is_valid,
                        expected_candles=vr.expected_candles,
                        actual_candles=vr.actual_candles,
                        missing_candles=vr.expected_candles - vr.actual_candles,
                        completeness_percentage=round(
                            (
                                (vr.actual_candles / vr.expected_candles * 100)
                                if vr.expected_candles > 0
                                else 100.0
                            ),
                            2,
                        ),
                        is_half_day=vr.expected_candles == 210,
                        missing_periods=[
                            f"{start} to {end}" for start, end in vr.missing_periods
                        ],
                        missing_periods_count=len(vr.missing_periods),
                        largest_gap_minutes=max(
                            [
                                int((end - start).total_seconds() / 60)
                                for start, end in vr.missing_periods
                            ]
                            + [0]
                        ),
                        polygon_urls_for_missing_periods=getattr(
                            vr, "polygon_urls_for_missing_periods", []
                        ),
                        gap_fill_results=getattr(vr, "gap_fill_results", []),
                        errors=vr.errors,
                        warnings=vr.warnings,
                    )
                    for vr in validation_results_raw
                ]

            # Create SymbolCompletenessData object
            completeness_data[symbol] = SymbolCompletenessData(
                total_trading_days=symbol_data.total_trading_days,
                valid_days=symbol_data.valid_days,
                invalid_days=symbol_data.invalid_days,
                completeness_percentage=symbol_data.completeness_percentage,
                total_expected_candles=symbol_data.total_expected_candles,
                total_actual_candles=symbol_data.total_actual_candles,
                missing_candles=symbol_data.missing_candles,
                full_days_count=symbol_data.full_days_count,
                half_days_count=symbol_data.half_days_count,
                days_with_gaps=symbol_data.days_with_gaps,
                total_missing_periods=symbol_data.total_missing_periods,
                average_daily_completeness=symbol_data.average_daily_completeness,
                worst_day_completeness=symbol_data.worst_day_completeness,
                best_day_completeness=symbol_data.best_day_completeness,
                validation_results=validation_results,
                gap_fill_attempted=symbol_data.gap_fill_attempted,
                total_gaps_found=symbol_data.total_gaps_found,
                gaps_filled_successfully=symbol_data.gaps_filled_successfully,
                gaps_vendor_unavailable=symbol_data.gaps_vendor_unavailable,
                candles_recovered=symbol_data.candles_recovered,
            )

        # Calculate overall statistics using Pydantic models
        symbol_models = [
            SymbolCompletenessRawData.from_dict(data)
            for data in raw_completeness_data.values()
        ]
        total_trading_days = sum(model.total_trading_days for model in symbol_models)
        total_valid_days = sum(model.valid_days for model in symbol_models)
        total_expected_candles = sum(
            model.total_expected_candles for model in symbol_models
        )
        total_actual_candles = sum(
            model.total_actual_candles for model in symbol_models
        )

        overall_completeness = (
            (total_actual_candles / total_expected_candles * 100)
            if total_expected_candles > 0
            else 0
        )

        # Create OverallStatistics object
        overall_statistics = OverallStatistics(
            total_symbols=len(request.symbols),
            total_trading_days=total_trading_days,
            total_valid_days=total_valid_days,
            overall_completeness_percentage=round(overall_completeness, 2),
            total_expected_candles=total_expected_candles,
            total_actual_candles=total_actual_candles,
            total_missing_candles=total_expected_candles - total_actual_candles,
        )

        # Find symbols needing attention (< 95% completeness)
        symbols_needing_attention = [
            symbol
            for symbol, data_dict in raw_completeness_data.items()
            if SymbolCompletenessRawData.from_dict(data_dict).completeness_percentage
            < 95.0
        ]

        # Generate recommendations
        recommendations: list[str] = []
        if symbols_needing_attention:
            recommendations.append(
                f"{len(symbols_needing_attention)} symbols have less than 95% data completeness"
            )
        if overall_completeness < 98.0:
            recommendations.append(
                "Consider running a full data update to improve completeness"
            )
        if total_trading_days > 0 and (total_valid_days / total_trading_days) < 0.9:
            recommendations.append(
                "High number of validation errors detected - investigate data quality issues"
            )

        return DataCompletenessResponse(
            analysis_period=AnalysisPeriod(
                start_date=request.start_date,
                end_date=request.end_date,
            ),
            symbol_completeness=completeness_data,
            overall_statistics=overall_statistics,
            symbols_needing_attention=symbols_needing_attention,
            recommendations=recommendations,
        )

    except Exception as e:
        logger.error(f"Data completeness analysis failed: {e}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")
