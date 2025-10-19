"""
Nightly update progress tracking service.

This service manages progress tracking for nightly update operations,
providing centralized storage and calculation of progress information.
"""

from datetime import datetime

from simutrador_core.utils import get_default_logger

from models.nightly_update_api import ActiveUpdateInfo, ProgressInfo, SymbolProgress

logger = get_default_logger("nightly_update_progress")


class NightlyUpdateProgressService:
    """Service for tracking progress of nightly update operations."""

    def __init__(self):
        """Initialize the progress tracking service."""
        # In production, these would be stored in Redis or a database
        self._active_updates: dict[str, ActiveUpdateInfo] = {}
        self._progress_tracking: dict[str, dict[str, SymbolProgress]] = {}

    def initialize_progress_tracking(self, request_id: str, symbols: list[str]) -> None:
        """Initialize progress tracking for a request."""
        self._progress_tracking[request_id] = {}
        for symbol in symbols:
            self._progress_tracking[request_id][symbol] = SymbolProgress(
                symbol=symbol,
                status="pending",
                progress_percentage=0.0,
                current_step="Queued for processing",
            )

    def update_symbol_progress(
        self,
        request_id: str,
        symbol: str,
        status: str,
        progress_percentage: float,
        current_step: str,
        error_message: str | None = None,
    ) -> None:
        """Update progress for a specific symbol."""
        if (
            request_id in self._progress_tracking
            and symbol in self._progress_tracking[request_id]
        ):
            progress = self._progress_tracking[request_id][symbol]
            progress.status = status
            progress.progress_percentage = progress_percentage
            progress.current_step = current_step
            if error_message:
                progress.error_message = error_message
            if status == "downloading" and not progress.started_at:
                progress.started_at = datetime.now()
            elif status in ["completed", "failed"]:
                progress.completed_at = datetime.now()

    def calculate_overall_progress(self, request_id: str) -> ProgressInfo:
        """Calculate overall progress for a request."""
        if request_id not in self._progress_tracking:
            return ProgressInfo(
                total_symbols=0,
                completed_symbols=0,
                current_step="Unknown",
                progress_percentage=0.0,
            )

        symbol_progresses = self._progress_tracking[request_id]
        total_symbols = len(symbol_progresses)
        completed_symbols = sum(
            1 for p in symbol_progresses.values() if p.status in ["completed", "failed"]
        )

        # Calculate overall progress as average of all symbol progresses
        if total_symbols > 0:
            overall_percentage = (
                sum(p.progress_percentage for p in symbol_progresses.values())
                / total_symbols
            )
        else:
            overall_percentage = 0.0

        # Find currently processing symbols
        symbols_in_progress = [
            p.symbol
            for p in symbol_progresses.values()
            if p.status not in ["pending", "completed", "failed"]
        ]

        # Determine current step
        if completed_symbols == total_symbols:
            current_step = "All symbols completed"
        elif symbols_in_progress:
            current_step = f"Processing {', '.join(symbols_in_progress[:3])}"
            if len(symbols_in_progress) > 3:
                current_step += f" and {len(symbols_in_progress) - 3} more"
        else:
            current_step = "Starting processing"

        # Estimate time remaining (simple heuristic)
        estimated_time_remaining = None
        if request_id in self._active_updates:
            started_at = self._active_updates[request_id].started_at
            elapsed_seconds = (datetime.now() - started_at).total_seconds()
            if overall_percentage > 5:  # Only estimate after some progress
                estimated_total_time = elapsed_seconds / (overall_percentage / 100)
                estimated_time_remaining = int(estimated_total_time - elapsed_seconds)
                estimated_time_remaining = max(0, estimated_time_remaining)

        return ProgressInfo(
            total_symbols=total_symbols,
            completed_symbols=completed_symbols,
            current_symbol=symbols_in_progress[0] if symbols_in_progress else None,
            current_step=current_step,
            progress_percentage=round(overall_percentage, 1),
            estimated_time_remaining_seconds=estimated_time_remaining,
            symbols_in_progress=symbols_in_progress,
        )

    def get_symbol_progress(self, request_id: str) -> dict[str, SymbolProgress]:
        """Get progress information for all symbols in a request."""
        return self._progress_tracking.get(request_id, {})

    def has_progress_tracking(self, request_id: str) -> bool:
        """Check if progress tracking exists for a request."""
        return request_id in self._progress_tracking

    def store_active_update(
        self, request_id: str, update_info: ActiveUpdateInfo
    ) -> None:
        """Store information about an active update."""
        self._active_updates[request_id] = update_info

    def get_active_update(self, request_id: str) -> ActiveUpdateInfo | None:
        """Get information about an active update."""
        return self._active_updates.get(request_id)

    def remove_active_update(self, request_id: str) -> None:
        """Remove an active update from tracking."""
        if request_id in self._active_updates:
            del self._active_updates[request_id]

    def get_all_active_updates(self) -> dict[str, ActiveUpdateInfo]:
        """Get all active updates."""
        return self._active_updates.copy()

    def cleanup_progress_tracking(self, request_id: str) -> None:
        """Clean up progress tracking for a completed request."""
        # In production, you might want to keep this for a while
        # or move to a different storage for historical tracking
        if request_id in self._progress_tracking:
            del self._progress_tracking[request_id]
