"""
Nightly update API endpoints.

This module provides REST API endpoints for:
- Triggering nightly data updates for stock market data
- Checking update status and progress
- Managing update schedules
"""

import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from models.nightly_update_api import (
    ActiveUpdateInfo,
    ActiveUpdateSummary,
    NightlyUpdateRequest,
    NightlyUpdateResponse,
    ProgressInfo,
    UpdateProgressDetailsResponse,
    UpdateStatusResponse,
)
from services.progress.nightly_update_progress_service import (
    NightlyUpdateProgressService,
)
from services.validation.stock_market_validation_service import (
    StockMarketValidationService,
)
from services.workflows.stock_market_nightly_update_service import (
    StockMarketNightlyUpdateService,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/nightly-update", tags=["nightly-update"])

# Global storage for completed updates (in production, use Redis or database)
_completed_updates: Dict[str, NightlyUpdateResponse] = {}

# Global singleton instances (in production, use proper dependency injection)
_progress_service_instance: Optional[NightlyUpdateProgressService] = None


def get_nightly_update_service() -> StockMarketNightlyUpdateService:
    """Dependency to get nightly update service instance."""
    return StockMarketNightlyUpdateService()


def get_validation_service() -> StockMarketValidationService:
    """Dependency to get validation service instance."""
    return StockMarketValidationService()


def get_progress_service() -> NightlyUpdateProgressService:
    """Dependency to get progress tracking service instance (singleton)."""
    global _progress_service_instance
    if _progress_service_instance is None:
        _progress_service_instance = NightlyUpdateProgressService()
    return _progress_service_instance


def reset_progress_service() -> None:
    """Reset the progress service singleton (for testing)."""
    global _progress_service_instance
    _progress_service_instance = None


@router.post("/start", response_model=Dict[str, str])
async def start_nightly_update(
    request: NightlyUpdateRequest,
    background_tasks: BackgroundTasks,
    nightly_service: StockMarketNightlyUpdateService = Depends(
        get_nightly_update_service
    ),
    progress_service: NightlyUpdateProgressService = Depends(get_progress_service),
) -> Dict[str, str]:
    """
    Start a nightly update process for stock market data.

    This endpoint triggers the complete nightly update workflow:
    1. Validates existing data completeness
    2. Downloads missing 1-minute data
    3. Resamples to all target timeframes
    4. Returns immediately with a request ID for status tracking
    """
    try:
        # Generate unique request ID
        request_id = str(uuid.uuid4())

        # Get symbols list - this will throw an exception if get_default_symbols fails
        symbols = request.symbols
        if symbols is None:
            symbols = nightly_service.get_default_symbols()

        # Store request info
        update_info = ActiveUpdateInfo(
            request=request,
            started_at=datetime.now(),
            status="starting",
            symbols=symbols,
        )
        progress_service.store_active_update(request_id, update_info)

        # Initialize progress tracking
        progress_service.initialize_progress_tracking(request_id, symbols)

        logger.info(f"Starting nightly update {request_id} for {len(symbols)} symbols")

        # Schedule background task
        background_tasks.add_task(
            nightly_service.execute_nightly_update,
            request_id,
            request,
            progress_service,
            _completed_updates,
        )

        # Compose message mentioning symbol(s) to satisfy tests and improve clarity
        if len(symbols) == 1:
            msg = f"Nightly update started for symbol {symbols[0]}"
        else:
            preview = ", ".join(symbols[:3])
            if len(symbols) > 3:
                preview += ", ..."
            msg = f"Nightly update started for {len(symbols)} symbols: {preview}"

        return {
            "request_id": request_id,
            "status": "started",
            "message": msg,
        }

    except Exception as e:
        logger.error(f"Failed to start nightly update: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start update: {str(e)}")


@router.get("/status/{request_id}", response_model=UpdateStatusResponse)
async def get_update_status(
    request_id: str,
    progress_service: NightlyUpdateProgressService = Depends(get_progress_service),
) -> UpdateStatusResponse:
    """
    Get the status of a nightly update request with detailed progress information.

    Args:
        request_id: The request ID returned from start_nightly_update

    Returns:
        Status information for the update request including progress details
    """
    try:
        # Check if it's still active
        active_info = progress_service.get_active_update(request_id)
        if active_info:
            progress_info = progress_service.calculate_overall_progress(request_id)

            return UpdateStatusResponse(
                request_id=request_id,
                status=active_info.status,
                started_at=active_info.started_at,
                symbols_count=len(active_info.symbols),
                is_complete=False,
                progress=progress_info,
                completed_at=None,
                summary=None,
                overall_success=None,
            )

        # Check if it's completed
        if request_id in _completed_updates:
            completed_result = _completed_updates[request_id]

            # Create progress info for completed update
            completed_progress = ProgressInfo(
                total_symbols=completed_result.summary.total_symbols,
                completed_symbols=completed_result.summary.total_symbols,
                current_symbol=None,
                current_step="All symbols completed",
                progress_percentage=100.0,
                estimated_time_remaining_seconds=0,
                symbols_in_progress=[],
            )

            return UpdateStatusResponse(
                request_id=request_id,
                status="completed",
                started_at=completed_result.started_at,
                symbols_count=completed_result.summary.total_symbols,
                is_complete=True,
                progress=completed_progress,
                completed_at=completed_result.completed_at,
                summary=completed_result.summary,
                overall_success=completed_result.overall_success,
            )

        # Request ID not found
        raise HTTPException(
            status_code=404, detail=f"Update request {request_id} not found"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get status for {request_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Status check failed: {str(e)}")


@router.get(
    "/status/{request_id}/progress", response_model=UpdateProgressDetailsResponse
)
async def get_update_progress_details(
    request_id: str,
    progress_service: NightlyUpdateProgressService = Depends(get_progress_service),
) -> UpdateProgressDetailsResponse:
    """
    Get detailed progress information for each symbol in a nightly update request.

    Args:
        request_id: The request ID returned from start_nightly_update

    Returns:
        Detailed progress information for each symbol
    """
    try:
        if not progress_service.has_progress_tracking(request_id):
            raise HTTPException(
                status_code=404,
                detail=f"Progress tracking not found for request {request_id}",
            )

        symbol_progresses = progress_service.get_symbol_progress(request_id)
        overall_progress = progress_service.calculate_overall_progress(request_id)

        return UpdateProgressDetailsResponse(
            request_id=request_id,
            overall_progress=overall_progress,
            symbol_progress=symbol_progresses,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get progress details for {request_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Progress retrieval failed: {str(e)}"
        )


@router.get("/status/{request_id}/details", response_model=NightlyUpdateResponse)
async def get_update_details(request_id: str) -> NightlyUpdateResponse:
    """
    Get detailed results of a completed nightly update.

    Args:
        request_id: The request ID returned from start_nightly_update

    Returns:
        Detailed update results including per-symbol information
    """
    try:
        if request_id not in _completed_updates:
            # Check if it's still active using the progress service
            progress_service = get_progress_service()
            if progress_service.get_active_update(request_id):
                raise HTTPException(
                    status_code=202, detail="Update is still in progress"
                )
            else:
                raise HTTPException(
                    status_code=404, detail=f"Update request {request_id} not found"
                )

        return _completed_updates[request_id]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get details for {request_id}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Details retrieval failed: {str(e)}"
        )


@router.get("/active", response_model=List[ActiveUpdateSummary])
async def list_active_updates(
    progress_service: NightlyUpdateProgressService = Depends(get_progress_service),
) -> List[ActiveUpdateSummary]:
    """
    List all currently active nightly update requests.

    Returns:
        List of active update requests with basic information
    """
    try:
        active_list: List[ActiveUpdateSummary] = []
        active_updates = progress_service.get_all_active_updates()
        for request_id, info in active_updates.items():
            active_list.append(
                ActiveUpdateSummary(
                    request_id=request_id,
                    status=info.status,
                    started_at=info.started_at,
                    symbols_count=len(info.symbols),
                    duration_seconds=(datetime.now() - info.started_at).total_seconds(),
                )
            )

        return active_list

    except Exception as e:
        logger.error(f"Failed to list active updates: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to list active updates: {str(e)}"
        )
