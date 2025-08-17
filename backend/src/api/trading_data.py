"""
Trading Data API endpoints.

This module provides REST API endpoints for:
- Retrieving stored trading data
- Listing available symbols
"""

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from simutrador_core.models.price_data import PaginationInfo, PriceDataSeries
from ..services.storage.data_storage_service import DataStorageService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/trading-data", tags=["trading-data"])


# Dependency injection
def get_storage_service() -> DataStorageService:
    """Get data storage service instance."""
    return DataStorageService()


@router.get("/data/{symbol}", response_model=PriceDataSeries)
async def get_trading_data(
    symbol: str,
    timeframe: Optional[str] = Query(default="1min", description="Data timeframe"),
    start_date: Optional[str] = Query(default=None, description="Start date filter"),
    end_date: Optional[str] = Query(default=None, description="End date filter"),
    order_by: str = Query(default="desc", description="Sort order: 'asc' or 'desc'"),
    page: int = Query(default=1, description="Page number (1-based)", ge=1),
    page_size: int = Query(
        default=1000, description="Number of items per page", ge=1, le=10000
    ),
    storage_service: DataStorageService = Depends(get_storage_service),
) -> PriceDataSeries:
    """
    Retrieve stored trading data for a symbol with pagination support.

    Args:
        symbol: Trading symbol (e.g., AAPL, MSFT)
        timeframe: Data timeframe (e.g., 1min, 5min, daily)
        start_date: Start date filter (optional)
        end_date: End date filter (optional)
        order_by: Sort order - 'asc' for ascending, 'desc' for descending (default: desc)
        page: Page number (1-based, default: 1)
        page_size: Number of items per page (default: 1000, max: 10000)

    Returns:
        PriceDataSeries: Trading data for the symbol with pagination information
    """
    try:
        # Parse date strings if provided
        parsed_start_date = None
        parsed_end_date = None

        if start_date:
            try:
                parsed_start_date = datetime.fromisoformat(start_date).date()
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid start_date format: {start_date}. Use YYYY-MM-DD format.",
                )

        if end_date:
            try:
                parsed_end_date = datetime.fromisoformat(end_date).date()
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid end_date format: {end_date}. Use YYYY-MM-DD format.",
                )

        # Get total count efficiently for pagination metadata
        total_items = storage_service.get_total_count(
            symbol=symbol.upper(),
            timeframe=timeframe or "1min",
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        # Calculate pagination info (// finds the largest integer less than or equal to the true \
        # division result.)
        total_pages = (
            (total_items + page_size - 1) // page_size if total_items > 0 else 0
        )
        offset = (page - 1) * page_size

        # Load only the requested page of data
        data_series = storage_service.load_data(
            symbol=symbol.upper(),
            timeframe=timeframe or "1min",
            start_date=parsed_start_date,
            end_date=parsed_end_date,
            order_by=order_by,
            limit=page_size,
            offset=offset,
        )

        # Create pagination info
        pagination_info = PaginationInfo(
            page=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1,
        )

        # Return paginated data series
        return PriceDataSeries(
            symbol=data_series.symbol,
            timeframe=data_series.timeframe,
            candles=data_series.candles,
            start_date=data_series.start_date,
            end_date=data_series.end_date,
            pagination=pagination_info,
        )

    except Exception as e:
        logger.error(f"Failed to retrieve data for {symbol}: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve data: {str(e)}"
        )


@router.get("/symbols", response_model=List[str])
async def list_stored_symbols(
    timeframe: Optional[str] = Query(default="1min", description="Timeframe to check"),
    storage_service: DataStorageService = Depends(get_storage_service),
) -> List[str]:
    """
    List all symbols that have stored data.

    Args:
        timeframe: Timeframe to check for stored data

    Returns:
        List[str]: List of available symbol names
    """
    try:
        symbols = storage_service.list_stored_symbols(timeframe or "1min")
        return sorted(symbols)
    except Exception as e:
        logger.error(f"Failed to list stored symbols: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list symbols: {str(e)}")
