"""
Stock market data validation service.

This service handles validation of stock market data including:
- Market hours validation (13:30 - 20:00 UTC, equivalent to 9:30 AM - 4:00 PM ET)
- Trading day validation (weekdays, excluding holidays)
- Data completeness checks
- Volume and price data integrity validation

All timestamps are handled in UTC to eliminate timezone conversion issues.
"""

import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from typing_extensions import override

# Try to import pandas_market_calendars for official NYSE calendar
try:
    import pandas_market_calendars as mcal  # type: ignore

    _has_market_calendars = True
except ImportError:
    _has_market_calendars = False
    mcal = None

    try:
        from pandas.tseries.holiday import Day, Easter  # type: ignore
    except ImportError:
        # Fallback for different pandas versions
        try:
            from pandas.tseries.offsets import Day, Easter  # type: ignore
        except ImportError:
            # If neither works, create simple fallbacks
            class Easter:  # type: ignore
                def __call__(self) -> Any:
                    return None

            class Day:  # type: ignore
                def __init__(self, offset: int) -> None:
                    self.offset = offset


from simutrador_core.models.price_data import PriceCandle, Timeframe
from simutrador_core.utils import get_default_logger

from core.settings import get_settings

from ..polygon_url_generator import PolygonUrlGenerator
from ..storage.data_storage_service import DataStorageService

logger = get_default_logger("stock_market_validation")


# Only define custom calendar classes if pandas_market_calendars is not available
if not _has_market_calendars:
    # Import holiday classes only when needed
    from pandas.tseries.holiday import (
        AbstractHolidayCalendar,
        Holiday,
        USLaborDay,
        USMartinLutherKingJr,
        USMemorialDay,
        USPresidentsDay,
        USThanksgivingDay,
    )

    def nyse_observance(dt: Any) -> Any:
        """
        NYSE holiday observance rule.

        - If holiday falls on Saturday: observe on preceding Friday
        - If holiday falls on Sunday: observe on following Monday
        - Otherwise: observe on the actual day

        Args:
            dt: datetime or Timestamp object

        Returns:
            Adjusted datetime/Timestamp for NYSE observance
        """
        if dt.weekday() == 5:  # Saturday
            return dt - pd.Timedelta(days=1)
        elif dt.weekday() == 6:  # Sunday
            return dt + pd.Timedelta(days=1)
        return dt

    class USStockMarketCalendar(AbstractHolidayCalendar):
        """
        US Stock Market Holiday Calendar.

        This calendar includes only the holidays when US stock markets (NYSE/NASDAQ) are closed.
        It differs from the federal calendar by:
        - Including Good Friday (not a federal holiday)
        - Excluding Columbus Day and Veterans Day (markets remain open)
        - Juneteenth observed starting 2022 for stock markets
        """

        rules = [
            # New Year's Day
            Holiday("New Year's Day", month=1, day=1, observance=nyse_observance),
            # Martin Luther King Jr. Day (3rd Monday in January)
            USMartinLutherKingJr,
            # Presidents Day (3rd Monday in February)
            USPresidentsDay,
            # Good Friday (Friday before Easter) - CRITICAL for stock markets
            Holiday("Good Friday", month=1, day=1, offset=[Easter(), Day(-2)]),  # type: ignore
            # Memorial Day (Last Monday in May)
            USMemorialDay,
            # Juneteenth (June 19th, observed starting 2022 for stock markets)
            Holiday(
                "Juneteenth",
                month=6,
                day=19,
                start_date="2022-01-01",
                observance=nyse_observance,
            ),
            # Independence Day (July 4th)
            Holiday("Independence Day", month=7, day=4, observance=nyse_observance),
            # Labor Day (1st Monday in September)
            USLaborDay,
            # Thanksgiving Day (4th Thursday in November)
            USThanksgivingDay,
            # Christmas Day (December 25th)
            Holiday("Christmas Day", month=12, day=25, observance=nyse_observance),
        ]


class ValidationError(Exception):
    """Exception raised for data validation errors."""

    pass


class ValidationResult:
    """Result of data validation with detailed information."""

    def __init__(
        self,
        symbol: str,
        validation_date: date,
        is_valid: bool,
        expected_candles: int,
        actual_candles: int,
        missing_periods: Optional[List[Tuple[datetime, datetime]]] = None,
        polygon_urls_for_missing_periods: Optional[List[str]] = None,
        errors: Optional[List[str]] = None,
        warnings: Optional[List[str]] = None,
    ):
        self.symbol = symbol
        self.validation_date = validation_date
        self.is_valid = is_valid
        self.expected_candles = expected_candles
        self.actual_candles = actual_candles
        self.missing_periods = missing_periods or []
        self.polygon_urls_for_missing_periods = polygon_urls_for_missing_periods or []
        self.errors = errors or []
        self.warnings = warnings or []

    @override
    def __str__(self) -> str:
        status = "VALID" if self.is_valid else "INVALID"
        return (
            f"ValidationResult({self.symbol} {self.validation_date}: {status}, "
            f"{self.actual_candles}/{self.expected_candles} candles)"
        )


class StockMarketValidationService:
    """Service for validating stock market data completeness and integrity."""

    def __init__(self):
        """Initialize the validation service."""
        self.settings = get_settings()
        self.nightly_settings = self.settings.nightly_update
        self.storage_service = DataStorageService()
        self.polygon_url_generator = PolygonUrlGenerator()

        # Initialize market calendar - prefer pandas_market_calendars if available
        if _has_market_calendars and mcal is not None:
            # Use official NYSE calendar from pandas_market_calendars
            self.market_calendar = mcal.get_calendar("NYSE")  # type: ignore
            self.use_official_calendar = True
            logger.info("Using official NYSE calendar from pandas_market_calendars")
        else:
            # Fallback to custom calendar
            self.market_calendar = USStockMarketCalendar()
            self.use_official_calendar = False
            logger.warning(
                "pandas_market_calendars not available, using custom calendar"
            )

        # Market hours in UTC (eliminates timezone conversion issues)
        self.market_open_utc = time(
            self.nightly_settings.market_open_hour_utc,
            self.nightly_settings.market_open_minute_utc,
        )
        self.market_close_utc = time(
            self.nightly_settings.market_close_hour_utc,
            self.nightly_settings.market_close_minute_utc,
        )

    def is_trading_day(self, check_date: date) -> bool:
        """
        Check if a given date is a trading day (weekday, not a holiday).

        Args:
            check_date: Date to check

        Returns:
            True if it's a trading day, False otherwise
        """
        if self.use_official_calendar:
            # Use pandas_market_calendars for official NYSE trading days
            schedule = self.market_calendar.schedule(  # type: ignore
                start_date=check_date, end_date=check_date
            )
            return len(schedule) > 0  # type: ignore
        else:
            # Fallback to custom logic
            # Check if it's a weekday (Monday=0, Sunday=6)
            if check_date.weekday() >= 5:  # Saturday or Sunday
                return False

            # Check if it's a stock market holiday
            return not self._is_market_holiday(check_date)

    def _is_market_holiday(self, check_date: date) -> bool:
        """
        Check if a date is a stock market holiday (internal helper to avoid recursion).

        Args:
            check_date: Date to check

        Returns:
            True if it's a market holiday, False otherwise
        """
        start_datetime = datetime.combine(check_date, time.min)
        end_datetime = datetime.combine(check_date, time.max)
        holidays = self.market_calendar.holidays(  # type: ignore
            start_date=start_datetime, end_date=end_datetime  # type: ignore
        )
        return len(holidays) > 0  # type: ignore

    def is_half_trading_day(self, check_date: date) -> bool:
        """
        Check if a given date is a half trading day (early close at 17:00 UTC / 1:00 PM ET).

        Args:
            check_date: Date to check

        Returns:
            True if it's a half trading day, False otherwise
        """
        if self.use_official_calendar:
            # Use pandas_market_calendars for official early close detection
            try:
                schedule = self.market_calendar.schedule(  # type: ignore
                    start_date=check_date, end_date=check_date
                )
                if len(schedule) > 0:  # type: ignore
                    # Check if market close is earlier than normal (4:00 PM ET = 20:00 UTC)
                    market_close = schedule.iloc[0]["market_close"]  # type: ignore
                    # Convert to UTC if needed
                    if hasattr(market_close, "tz") and market_close.tz is not None:  # type: ignore
                        market_close_utc = market_close.tz_convert("UTC")  # type: ignore
                    else:
                        market_close_utc = market_close  # type: ignore

                    # Normal close is 20:00 UTC, early close is typically 18:00 UTC (1:00 PM ET)
                    normal_close_hour = 20
                    actual_close_hour = market_close_utc.hour  # type: ignore

                    return actual_close_hour < normal_close_hour  # type: ignore
                return False
            except Exception as e:
                logger.warning(
                    f"Error checking early close with official calendar: {e}"
                )
                # Fall back to custom logic below

        # Fallback to custom half-day logic
        # Day after Thanksgiving (Black Friday)
        if check_date.month == 11:
            # Find 4th Thursday of November (Thanksgiving)
            first_day = check_date.replace(day=1)
            # Find first Thursday
            days_to_first_thursday = (3 - first_day.weekday()) % 7
            first_thursday = first_day + timedelta(days=days_to_first_thursday)
            # Get 4th Thursday
            thanksgiving = first_thursday + timedelta(days=21)
            # Day after Thanksgiving
            black_friday = thanksgiving + timedelta(days=1)

            if check_date == black_friday and black_friday.weekday() < 5:
                return True

        # Christmas Eve (if it's a weekday and not a full holiday)
        christmas_eve = date(check_date.year, 12, 24)
        if (
            check_date == christmas_eve
            and christmas_eve.weekday() < 5
            and not self._is_market_holiday(christmas_eve)
        ):  # Only if it's not already a full holiday
            return True

        # July 3rd (if July 4th falls on a weekday and July 3rd is a trading day)
        july_3rd = date(check_date.year, 7, 3)
        july_4th = date(check_date.year, 7, 4)
        if (
            check_date == july_3rd
            and july_3rd.weekday() < 5  # July 3rd must be a weekday
            and july_4th.weekday()
            < 5  # July 4th must be a weekday (not observed on weekend)
            and not self._is_market_holiday(july_3rd)
        ):  # Only if July 3rd is not already a full holiday
            return True

        return False

    def get_expected_candle_count(self, validation_date: date) -> int:
        """
        Get expected number of 1-minute candles for a trading day.

        Args:
            validation_date: Date to check

        Returns:
            Expected number of candles (390 for full day, 210 for half day)
        """
        if not self.is_trading_day(validation_date):
            return 0

        if self.is_half_trading_day(validation_date):
            return self.nightly_settings.expected_candles_half_day
        else:
            return self.nightly_settings.expected_candles_per_day

    def validate_trading_day_data(
        self, symbol: str, validation_date: date
    ) -> ValidationResult:
        """
        Validate 1-minute data for a specific symbol and trading day.

        Args:
            symbol: Trading symbol to validate
            validation_date: Date to validate

        Returns:
            ValidationResult with detailed validation information
        """
        expected_candles = self.get_expected_candle_count(validation_date)

        # If not a trading day, return valid with 0 expected candles
        if expected_candles == 0:
            return ValidationResult(
                symbol=symbol,
                validation_date=validation_date,
                is_valid=True,
                expected_candles=0,
                actual_candles=0,
                warnings=["Not a trading day"],
            )

        try:
            # Load 1-minute data for the specific date
            series = self.storage_service.load_data(
                symbol=symbol,
                timeframe=Timeframe.ONE_MIN.value,
                start_date=validation_date,
                end_date=validation_date,
            )

            # Filter candles to only include regular market hours if market hours check is enabled
            if self.nightly_settings.enable_market_hours_check:
                regular_hours_candles = self._filter_regular_market_hours(
                    series.candles, validation_date
                )
                actual_candles = len(regular_hours_candles)
            else:
                actual_candles = len(series.candles)
            errors: List[str] = []
            warnings: List[str] = []
            missing_periods: List[Tuple[datetime, datetime]] = []

            # Find missing time periods FIRST (before early returns)
            if self.nightly_settings.enable_market_hours_check:
                missing_periods = self._find_missing_periods(
                    series.candles, validation_date
                )
                if missing_periods:
                    for start_time, end_time in missing_periods:
                        errors.append(f"Missing data from {start_time} to {end_time}")

            # Check for no data case
            if actual_candles == 0:
                errors.append(f"No data found for {validation_date}")
                # Generate Polygon URLs for missing periods even when no data
                polygon_urls = []
                if missing_periods:
                    polygon_urls = (
                        self.polygon_url_generator.generate_urls_for_missing_periods(
                            symbol, missing_periods
                        )
                    )

                return ValidationResult(
                    symbol=symbol,
                    validation_date=validation_date,
                    is_valid=False,
                    expected_candles=expected_candles,
                    actual_candles=0,
                    errors=errors,
                    missing_periods=missing_periods,
                    polygon_urls_for_missing_periods=polygon_urls,
                )

            # Check candle count
            if actual_candles < expected_candles:
                missing_count = expected_candles - actual_candles
                errors.append(
                    f"Missing {missing_count} candles "
                    f"(expected {expected_candles}, got {actual_candles})"
                )

            # Validate data integrity
            integrity_errors, integrity_warnings = self._validate_data_integrity(
                series.candles
            )
            errors.extend(integrity_errors)  # type: ignore
            warnings.extend(integrity_warnings)  # type: ignore

            is_valid = len(errors) == 0

            # Generate Polygon URLs for all missing periods
            polygon_urls = []
            if missing_periods:
                polygon_urls = (
                    self.polygon_url_generator.generate_urls_for_missing_periods(
                        symbol, missing_periods
                    )
                )

            return ValidationResult(
                symbol=symbol,
                validation_date=validation_date,
                is_valid=is_valid,
                expected_candles=expected_candles,
                actual_candles=actual_candles,
                missing_periods=missing_periods,
                polygon_urls_for_missing_periods=polygon_urls,
                errors=errors,
                warnings=warnings,
            )

        except Exception as e:
            logger.error(f"Validation failed for {symbol} on {validation_date}: {e}")
            return ValidationResult(
                symbol=symbol,
                validation_date=validation_date,
                is_valid=False,
                expected_candles=expected_candles,
                actual_candles=0,
                errors=[f"Validation error: {str(e)}"],
            )

    def _validate_data_integrity(
        self, candles: List[PriceCandle]
    ) -> Tuple[List[str], List[str]]:
        """
        Validate the integrity of price candle data.

        Args:
            candles: List of price candles to validate

        Returns:
            Tuple of (errors, warnings)
        """
        errors: List[str] = []
        warnings: List[str] = []

        for i, candle in enumerate(candles):
            # Validate OHLC relationships
            if candle.high < candle.low:
                errors.append(f"Candle {i}: High ({candle.high}) < Low ({candle.low})")

            if candle.high < candle.open or candle.high < candle.close:
                errors.append(f"Candle {i}: High ({candle.high}) < Open/Close")

            if candle.low > candle.open or candle.low > candle.close:
                errors.append(f"Candle {i}: Low ({candle.low}) > Open/Close")

            # Check for zero or negative prices
            if (
                candle.open <= 0
                or candle.high <= 0
                or candle.low <= 0
                or candle.close <= 0
            ):
                errors.append(f"Candle {i}: Invalid price (zero or negative)")

            # Check for suspicious volume
            if candle.volume < 0:
                errors.append(f"Candle {i}: Negative volume ({candle.volume})")
            elif candle.volume == 0:
                warnings.append(f"Candle {i}: Zero volume")

        return errors, warnings

    def _find_missing_periods(
        self, candles: List[PriceCandle], validation_date: date
    ) -> List[Tuple[datetime, datetime]]:
        """
        Find missing time periods during market hours.
        All times are in UTC to eliminate timezone conversion issues.

        Args:
            candles: List of price candles
            validation_date: Date being validated

        Returns:
            List of (start_time, end_time) tuples for missing periods in UTC
        """
        # Create expected time range for the trading day in UTC
        market_open_utc = datetime.combine(
            validation_date, self.market_open_utc
        ).replace(tzinfo=timezone.utc)

        if self.is_half_trading_day(validation_date):
            # Half day: 3.5 hours (210 minutes) - closes at 17:00 UTC (1:00 PM ET)
            market_close_utc = market_open_utc + timedelta(hours=3, minutes=30)
        else:
            # Full day: 6.5 hours (390 minutes) - closes at 20:00 UTC (4:00 PM ET)
            market_close_utc = market_open_utc + timedelta(hours=6, minutes=30)

        # If no candles at all, the entire trading session is missing
        if not candles:
            return [(market_open_utc, market_close_utc)]

        # Generate expected timestamps (every minute) in UTC
        expected_times: Set[datetime] = set()
        current_time = market_open_utc
        while current_time < market_close_utc:
            expected_times.add(current_time)
            current_time += timedelta(minutes=1)

        # Get actual timestamps from candles, ensuring they're in UTC
        actual_times: Set[datetime] = set()
        for candle in candles:
            candle_time = candle.date.replace(second=0, microsecond=0)
            # Ensure candle time is timezone-aware (assume UTC if naive)
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)
            actual_times.add(candle_time)

        # Find missing times
        missing_times: List[datetime] = sorted(expected_times - actual_times)

        # Group consecutive missing times into periods
        missing_periods: List[Tuple[datetime, datetime]] = []
        if missing_times:
            period_start: datetime = missing_times[0]
            period_end: datetime = missing_times[0]

            for missing_time in missing_times[1:]:
                if missing_time == period_end + timedelta(minutes=1):
                    # Consecutive missing time
                    period_end = missing_time
                else:
                    # Gap in missing times, close current period and start new one
                    missing_periods.append(
                        (period_start, period_end + timedelta(minutes=1))
                    )
                    period_start = missing_time
                    period_end = missing_time

            # Close the last period
            missing_periods.append((period_start, period_end + timedelta(minutes=1)))

        return missing_periods

    def _filter_regular_market_hours(
        self, candles: List[PriceCandle], validation_date: date
    ) -> List[PriceCandle]:
        """
        Filter candles to only include regular market hours (13:30 - 20:00 UTC).

        This method filters out pre-market and after-hours trading data to ensure
        validation only counts candles during regular trading hours.
        All times are in UTC to eliminate timezone conversion issues.

        Args:
            candles: List of price candles to filter
            validation_date: Date being validated

        Returns:
            List of candles within regular market hours
        """
        if not candles:
            return []

        # Create market hours boundaries in UTC
        market_open_utc = datetime.combine(
            validation_date, self.market_open_utc
        ).replace(tzinfo=timezone.utc)

        market_close_utc = datetime.combine(
            validation_date, self.market_close_utc
        ).replace(tzinfo=timezone.utc)

        # Adjust for half trading days
        if self.is_half_trading_day(validation_date):
            # Half day closes at 17:00 UTC (1:00 PM ET)
            market_close_utc = market_open_utc + timedelta(hours=3, minutes=30)

        # Filter candles within market hours
        regular_hours_candles: List[PriceCandle] = []
        for candle in candles:
            # Ensure candle datetime is timezone-aware (assume UTC if naive)
            candle_time = candle.date
            if candle_time.tzinfo is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)

            # Check if candle is within regular market hours
            if market_open_utc <= candle_time < market_close_utc:
                regular_hours_candles.append(candle)

        return regular_hours_candles

    def validate_symbol_data_range(
        self, symbol: str, start_date: date, end_date: date
    ) -> List[ValidationResult]:
        """
        Validate data for a symbol across a date range.

        Args:
            symbol: Trading symbol to validate
            start_date: Start date for validation
            end_date: End date for validation

        Returns:
            List of ValidationResult objects for each trading day
        """
        results: List[ValidationResult] = []
        current_date = start_date

        while current_date <= end_date:
            if self.is_trading_day(current_date):
                result = self.validate_trading_day_data(symbol, current_date)
                results.append(result)
            current_date += timedelta(days=1)

        return results

    def validate_multiple_symbols(
        self, symbols: List[str], validation_date: date
    ) -> Dict[str, ValidationResult]:
        """
        Validate data for multiple symbols on a specific date.

        Args:
            symbols: List of trading symbols to validate
            validation_date: Date to validate

        Returns:
            Dictionary mapping symbol to ValidationResult
        """
        results: Dict[str, ValidationResult] = {}

        for symbol in symbols:
            try:
                result = self.validate_trading_day_data(symbol, validation_date)
                results[symbol] = result

                if not result.is_valid:
                    logger.warning(
                        f"Validation failed for {symbol} on {validation_date}: {result.errors}"
                    )

            except Exception as e:
                logger.error(f"Failed to validate {symbol} on {validation_date}: {e}")
                results[symbol] = ValidationResult(
                    symbol=symbol,
                    validation_date=validation_date,
                    is_valid=False,
                    expected_candles=0,
                    actual_candles=0,
                    errors=[f"Validation exception: {str(e)}"],
                )

        return results

    def get_data_completeness_summary(
        self, symbols: List[str], start_date: date, end_date: date
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get a summary of data completeness for multiple symbols over a date range.

        Args:
            symbols: List of trading symbols
            start_date: Start date for analysis
            end_date: End date for analysis

        Returns:
            Dictionary with completeness statistics per symbol
        """
        summary: Dict[str, Dict[str, Any]] = {}

        for symbol in symbols:
            validation_results = self.validate_symbol_data_range(
                symbol, start_date, end_date
            )

            total_trading_days = len(validation_results)
            valid_days = sum(1 for result in validation_results if result.is_valid)
            invalid_days = total_trading_days - valid_days

            total_expected_candles = sum(
                result.expected_candles for result in validation_results
            )
            total_actual_candles = sum(
                result.actual_candles for result in validation_results
            )

            completeness_percentage = (
                (total_actual_candles / total_expected_candles * 100)
                if total_expected_candles > 0
                else 0
            )

            # Calculate enhanced metrics
            full_days_count = sum(
                1 for result in validation_results if result.expected_candles == 390
            )
            half_days_count = sum(
                1 for result in validation_results if result.expected_candles == 210
            )
            days_with_gaps = sum(
                1
                for result in validation_results
                if result.actual_candles < result.expected_candles
            )
            total_missing_periods = sum(
                len(result.missing_periods) for result in validation_results
            )

            # Calculate daily completeness percentages
            daily_completeness = [
                (
                    (result.actual_candles / result.expected_candles * 100)
                    if result.expected_candles > 0
                    else 100.0
                )
                for result in validation_results
            ]

            average_daily_completeness = (
                sum(daily_completeness) / len(daily_completeness)
                if daily_completeness
                else 0.0
            )
            worst_day_completeness = (
                min(daily_completeness) if daily_completeness else 100.0
            )
            best_day_completeness = (
                max(daily_completeness) if daily_completeness else 0.0
            )

            summary[symbol] = {
                "total_trading_days": total_trading_days,
                "valid_days": valid_days,
                "invalid_days": invalid_days,
                "completeness_percentage": round(completeness_percentage, 2),
                "total_expected_candles": total_expected_candles,
                "total_actual_candles": total_actual_candles,
                "missing_candles": total_expected_candles - total_actual_candles,
                "full_days_count": full_days_count,
                "half_days_count": half_days_count,
                "days_with_gaps": days_with_gaps,
                "total_missing_periods": total_missing_periods,
                "average_daily_completeness": round(average_daily_completeness, 2),
                "worst_day_completeness": round(worst_day_completeness, 2),
                "best_day_completeness": round(best_day_completeness, 2),
                "validation_results": validation_results,
            }

        return summary

    def find_symbols_needing_update(
        self, symbols: List[str], target_date: Optional[date] = None
    ) -> List[str]:
        """
        Find symbols that need data updates based on validation.

        Args:
            symbols: List of symbols to check
            target_date: Date to check (defaults to previous trading day)

        Returns:
            List of symbols that need updates
        """
        if target_date is None:
            # Default to previous trading day
            target_date = date.today() - timedelta(days=1)
            while not self.is_trading_day(target_date):
                target_date -= timedelta(days=1)

        symbols_needing_update: List[str] = []

        for symbol in symbols:
            try:
                result = self.validate_trading_day_data(symbol, target_date)
                if not result.is_valid or result.actual_candles == 0:
                    symbols_needing_update.append(symbol)
                    logger.info(f"{symbol} needs update for {target_date}: {result}")

            except Exception as e:
                logger.error(f"Failed to check {symbol} for {target_date}: {e}")
                symbols_needing_update.append(symbol)

        return symbols_needing_update

    async def analyze_completeness_with_gap_filling(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date,
        auto_fill_gaps: bool = False,
        max_gap_fill_attempts: int = 50,
    ) -> Dict[str, Any]:
        """
        Analyze data completeness with optional automatic gap filling.

        Args:
            symbols: List of symbols to analyze
            start_date: Start date for analysis
            end_date: End date for analysis
            auto_fill_gaps: Whether to attempt automatic gap filling
            max_gap_fill_attempts: Maximum number of gaps to attempt filling per symbol

        Returns:
            Dictionary with completeness analysis and gap filling results
        """
        from services.gap_filling_service import GapFillingService

        # First, run the standard completeness analysis
        summary = self.get_data_completeness_summary(symbols, start_date, end_date)

        if not auto_fill_gaps:
            return summary

        # Initialize gap filling service
        gap_filling_service = GapFillingService()

        # Process each symbol for gap filling
        for symbol in symbols:
            if symbol not in summary:
                continue

            symbol_data = summary[symbol]
            validation_results = symbol_data.get("validation_results", [])

            total_gaps_found = 0
            gaps_filled_successfully = 0
            gaps_vendor_unavailable = 0
            total_candles_recovered = 0

            # Process each day's validation results
            for validation_result in validation_results:
                if not validation_result.missing_periods:
                    continue

                # Convert missing periods to datetime tuples
                missing_periods: List[Tuple[datetime, datetime]] = []
                for start_time, end_time in validation_result.missing_periods:
                    missing_periods.append((start_time, end_time))

                total_gaps_found += len(missing_periods)

                if missing_periods:
                    logger.info(
                        f"Attempting to fill {len(missing_periods)} gaps for {symbol} "
                        f"on {validation_result.validation_date}"
                    )

                    # Attempt to fill gaps
                    gap_fill_results = await gap_filling_service.fill_gaps_for_periods(
                        symbol, missing_periods, max_gap_fill_attempts
                    )

                    # Update statistics
                    for gap_result in gap_fill_results:
                        if gap_result.success:
                            gaps_filled_successfully += 1
                            total_candles_recovered += gap_result.candles_recovered
                        elif gap_result.vendor_unavailable:
                            gaps_vendor_unavailable += 1

                    # Store gap fill results in validation result
                    validation_result.gap_fill_results = gap_fill_results

            # Update symbol data with gap filling statistics
            symbol_data.update(
                {
                    "gap_fill_attempted": True,
                    "total_gaps_found": total_gaps_found,
                    "gaps_filled_successfully": gaps_filled_successfully,
                    "gaps_vendor_unavailable": gaps_vendor_unavailable,
                    "candles_recovered": total_candles_recovered,
                }
            )

            # Re-run validation after gap filling to get updated completeness
            if gaps_filled_successfully > 0:
                logger.info(
                    f"Re-validating {symbol} after filling {gaps_filled_successfully} gaps"
                )
                updated_results = self.validate_symbol_data_range(
                    symbol, start_date, end_date
                )

                # Update the summary with new validation results
                updated_summary = self._calculate_symbol_summary(
                    symbol, updated_results
                )
                symbol_data.update(updated_summary)
                symbol_data["validation_results"] = updated_results

        return summary

    def _calculate_symbol_summary(
        self, symbol: str, validation_results: List[ValidationResult]
    ) -> Dict[str, Any]:
        """
        Calculate summary statistics for a symbol's validation results.

        Args:
            symbol: Trading symbol
            validation_results: List of validation results

        Returns:
            Dictionary with summary statistics
        """
        total_trading_days = len(validation_results)
        valid_days = sum(1 for result in validation_results if result.is_valid)
        invalid_days = total_trading_days - valid_days

        total_expected_candles = sum(
            result.expected_candles for result in validation_results
        )
        total_actual_candles = sum(
            result.actual_candles for result in validation_results
        )

        completeness_percentage = (
            (total_actual_candles / total_expected_candles * 100)
            if total_expected_candles > 0
            else 0
        )

        # Calculate enhanced metrics
        full_days_count = sum(
            1 for result in validation_results if result.expected_candles == 390
        )
        half_days_count = sum(
            1 for result in validation_results if result.expected_candles == 210
        )
        days_with_gaps = sum(
            1
            for result in validation_results
            if result.actual_candles < result.expected_candles
        )
        total_missing_periods = sum(
            len(result.missing_periods) for result in validation_results
        )

        # Calculate daily completeness percentages
        daily_completeness = [
            (
                (result.actual_candles / result.expected_candles * 100)
                if result.expected_candles > 0
                else 100.0
            )
            for result in validation_results
        ]

        average_daily_completeness = (
            sum(daily_completeness) / len(daily_completeness)
            if daily_completeness
            else 0.0
        )
        worst_day_completeness = (
            min(daily_completeness) if daily_completeness else 100.0
        )
        best_day_completeness = max(daily_completeness) if daily_completeness else 0.0

        return {
            "total_trading_days": total_trading_days,
            "valid_days": valid_days,
            "invalid_days": invalid_days,
            "completeness_percentage": round(completeness_percentage, 2),
            "total_expected_candles": total_expected_candles,
            "total_actual_candles": total_actual_candles,
            "missing_candles": total_expected_candles - total_actual_candles,
            "full_days_count": full_days_count,
            "half_days_count": half_days_count,
            "days_with_gaps": days_with_gaps,
            "total_missing_periods": total_missing_periods,
            "average_daily_completeness": round(average_daily_completeness, 2),
            "worst_day_completeness": round(worst_day_completeness, 2),
            "best_day_completeness": round(best_day_completeness, 2),
            "validation_results": validation_results,
        }
