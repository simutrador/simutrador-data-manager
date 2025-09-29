# Data Analysis API

## Endpoint Overview

The Data Analysis API provides endpoints for analyzing trading data quality and completeness:

1.  **POST** `/data-analysis/completeness` - Analyze data completeness

## Analyze Data Completeness

**POST** `/data-analysis/completeness`

Analyze data completeness for specified symbols and date range.

This endpoint provides detailed analysis of data completeness including:

*   Missing data periods
*   Validation errors and warnings
*   Completeness percentages
*   Recommendations for improvement

### Request Body

```
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

*   **symbols** (required): Array of trading symbols to analyze
*   **start\_date** (required): Start date for analysis (YYYY-MM-DD format)
*   **end\_date** (required): End date for analysis (YYYY-MM-DD format)
*   **include\_details** (optional): Whether to include detailed validation results (default: false)
*   **auto\_fill\_gaps** (optional): Automatically attempt to fill detected gaps (default: false)
*   **max\_gap\_fill\_attempts** (optional): Maximum number of gaps to attempt filling per symbol (default: 50)

### Response

```
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

**analysis\_period**

*   **start\_date**: Start date of the analysis
*   **end\_date**: End date of the analysis

**symbol\_completeness** (per symbol)

*   **total\_trading\_days**: Total number of trading days in the period
*   **valid\_days**: Number of days with complete data
*   **invalid\_days**: Number of days with missing or incomplete data
*   **completeness\_percentage**: Overall completeness percentage for the symbol
*   **total\_expected\_candles**: Total number of 1-minute candles expected
*   **total\_actual\_candles**: Total number of 1-minute candles found
*   **missing\_candles**: Number of missing candles
*   **full\_days\_count**: Number of full trading days (390 candles)
*   **half\_days\_count**: Number of half trading days (210 candles)
*   **days\_with\_gaps**: Number of days with data gaps
*   **total\_missing\_periods**: Total number of missing time periods
*   **average\_daily\_completeness**: Average completeness percentage per day
*   **worst\_day\_completeness**: Lowest daily completeness percentage
*   **best\_day\_completeness**: Highest daily completeness percentage
*   **validation\_results**: Detailed validation results (if include\_details=true)
*   **gap\_fill\_attempted**: Whether gap filling was attempted
*   **total\_gaps\_found**: Number of gaps found during analysis
*   **gaps\_filled\_successfully**: Number of gaps successfully filled
*   **gaps\_vendor\_unavailable**: Number of gaps where vendor data was unavailable
*   **candles\_recovered**: Number of candles recovered through gap filling

**overall\_statistics**

*   **total\_symbols**: Number of symbols analyzed
*   **total\_trading\_days**: Total trading days across all symbols
*   **total\_valid\_days**: Total valid days across all symbols
*   **overall\_completeness\_percentage**: Overall completeness percentage
*   **total\_expected\_candles**: Total expected candles across all symbols
*   **total\_actual\_candles**: Total actual candles found across all symbols
*   **total\_missing\_candles**: Total missing candles across all symbols

**symbols\_needing\_attention**

*   Array of symbols with completeness below 95%

**recommendations**

*   Array of actionable recommendations to improve data quality

### Error Responses

**400 Bad Request**

```
{
  "detail": "Invalid request parameters"
}
```

**422 Validation Error**

```
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

```
{
  "detail": "Analysis failed: Database connection error"
}
```

## Usage Examples

### Basic Completeness Analysis

```
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

```
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

*   This endpoint was moved from `/nightly-update/data-completeness` to provide better API organization
*   The functionality remains identical to the previous implementation
*   Frontend applications should update their API calls to use the new endpoint
*   The endpoint supports both basic analysis and advanced gap-filling capabilities
*   Gap filling requires valid API credentials for the configured data provider