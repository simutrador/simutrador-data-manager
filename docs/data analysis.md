# Data Analysis API

## Endpoint Overview

The Data Analysis API provides endpoints for analyzing trading data quality and completeness:

1. **POST** `/data-analysis/completeness` - Analyze data completeness

## Analyze Data Completeness

**POST** `/data-analysis/completeness`

Analyze data completeness for specified symbols and date range.

This endpoint provides detailed analysis of data completeness including:
- Missing data periods
- Validation errors and warnings
- Completeness percentages
- Recommendations for improvement

### Request Body

```json
{
  "symbols": ["AAPL", "MSFT", "GOOGL"],
  "start_date": "2025-01-01",
  "end_date": "2025-01-31",
  "include_details": false,
  "auto_fill_gaps": false,
  "max_gap_fill_attempts": 50
}
```

#### Parameters

- **symbols** (required): Array of trading symbols to analyze
- **start_date** (required): Start date for analysis (YYYY-MM-DD format)
- **end_date** (required): End date for analysis (YYYY-MM-DD format)
- **include_details** (optional): Whether to include detailed validation results (default: false)
- **auto_fill_gaps** (optional): Automatically attempt to fill detected gaps (default: false)
- **max_gap_fill_attempts** (optional): Maximum number of gaps to attempt filling per symbol (default: 50)

### Response

```json
{
  "analysis_period": {
    "start_date": "2025-01-01",
    "end_date": "2025-01-31"
  },
  "symbol_completeness": {
    "AAPL": {
      "total_trading_days": 22,
      "valid_days": 22,
      "invalid_days": 0,
      "completeness_percentage": 100.0,
      "total_expected_candles": 8580,
      "total_actual_candles": 8580,
      "missing_candles": 0,
      "full_days_count": 22,
      "half_days_count": 0,
      "days_with_gaps": 0,
      "total_missing_periods": 0,
      "average_daily_completeness": 100.0,
      "worst_day_completeness": 100.0,
      "best_day_completeness": 100.0,
      "validation_results": null,
      "gap_fill_attempted": false,
      "total_gaps_found": 0,
      "gaps_filled_successfully": 0,
      "gaps_vendor_unavailable": 0,
      "candles_recovered": 0
    }
  },
  "overall_statistics": {
    "total_symbols": 3,
    "total_trading_days": 66,
    "total_valid_days": 66,
    "overall_completeness_percentage": 98.5,
    "total_expected_candles": 25740,
    "total_actual_candles": 25353,
    "total_missing_candles": 387
  },
  "symbols_needing_attention": ["MSFT"],
  "recommendations": [
    "1 symbols have less than 95% data completeness",
    "Consider running a full data update to improve completeness"
  ]
}
```

#### Response Fields

**analysis_period**
- **start_date**: Start date of the analysis
- **end_date**: End date of the analysis

**symbol_completeness** (per symbol)
- **total_trading_days**: Total number of trading days in the period
- **valid_days**: Number of days with complete data
- **invalid_days**: Number of days with missing or incomplete data
- **completeness_percentage**: Overall completeness percentage for the symbol
- **total_expected_candles**: Total number of 1-minute candles expected
- **total_actual_candles**: Total number of 1-minute candles found
- **missing_candles**: Number of missing candles
- **full_days_count**: Number of full trading days (390 candles)
- **half_days_count**: Number of half trading days (210 candles)
- **days_with_gaps**: Number of days with data gaps
- **total_missing_periods**: Total number of missing time periods
- **average_daily_completeness**: Average completeness percentage per day
- **worst_day_completeness**: Lowest daily completeness percentage
- **best_day_completeness**: Highest daily completeness percentage
- **validation_results**: Detailed validation results (if include_details=true)
- **gap_fill_attempted**: Whether gap filling was attempted
- **total_gaps_found**: Number of gaps found during analysis
- **gaps_filled_successfully**: Number of gaps successfully filled
- **gaps_vendor_unavailable**: Number of gaps where vendor data was unavailable
- **candles_recovered**: Number of candles recovered through gap filling

**overall_statistics**
- **total_symbols**: Number of symbols analyzed
- **total_trading_days**: Total trading days across all symbols
- **total_valid_days**: Total valid days across all symbols
- **overall_completeness_percentage**: Overall completeness percentage
- **total_expected_candles**: Total expected candles across all symbols
- **total_actual_candles**: Total actual candles found across all symbols
- **total_missing_candles**: Total missing candles across all symbols

**symbols_needing_attention**
- Array of symbols with completeness below 95%

**recommendations**
- Array of actionable recommendations to improve data quality

### Error Responses

**400 Bad Request**
```json
{
  "detail": "Invalid request parameters"
}
```

**422 Validation Error**
```json
{
  "detail": [
    {
      "loc": ["body", "symbols"],
      "msg": "ensure this value has at least 1 items",
      "type": "value_error.list.min_items"
    }
  ]
}
```

**500 Internal Server Error**
```json
{
  "detail": "Analysis failed: Database connection error"
}
```

## Usage Examples

### Basic Completeness Analysis

```bash
curl -X POST "http://localhost:8002/data-analysis/completeness" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["AAPL", "MSFT"],
    "start_date": "2025-01-01",
    "end_date": "2025-01-31",
    "include_details": false
  }'
```

### Detailed Analysis with Gap Filling

```bash
curl -X POST "http://localhost:8002/data-analysis/completeness" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["AAPL"],
    "start_date": "2025-01-15",
    "end_date": "2025-01-20",
    "include_details": true,
    "auto_fill_gaps": true,
    "max_gap_fill_attempts": 25
  }'
```

## Integration Notes

- This endpoint was moved from `/nightly-update/data-completeness` to provide better API organization
- The functionality remains identical to the previous implementation
- Frontend applications should update their API calls to use the new endpoint
- The endpoint supports both basic analysis and advanced gap-filling capabilities
- Gap filling requires valid API credentials for the configured data provider
