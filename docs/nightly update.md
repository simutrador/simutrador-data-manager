# Nightly Update API

## Endpoint Overview

The Nightly Update API provides 5 endpoints for managing automated stock market data updates:

1. **POST** `/nightly-update/start` - Start nightly update process
2. **GET** `/nightly-update/status/{request_id}` - Get update status
3. **GET** `/nightly-update/status/{request_id}/progress` - Get detailed progress
4. **GET** `/nightly-update/status/{request_id}/details` - Get detailed results
5. **GET** `/nightly-update/active` - List active updates

**Note:** Data completeness analysis has been moved to the Data Analysis API at `/data-analysis/completeness`.

## Start Nightly Update

**POST** `/nightly-update/start`

Triggers automated nightly data updates for stock market data with comprehensive validation, data fetching, and resampling. This endpoint is designed for scheduled operations that ensure data completeness across all configured symbols and timeframes.

## Request Format

### NightlyUpdateRequest

| Field               | Type        | Required | Default | Description                                                                |
| ------------------- | ----------- | -------- | ------- | -------------------------------------------------------------------------- |
| `symbols`           | `List[str]` | ❌       | `null`  | Symbols to update (defaults to configured large+mid cap list)              |
| `force_validation`  | `bool`      | ❌       | `true`  | Whether to validate existing data before updating                          |
| `max_concurrent`    | `int`       | ❌       | `null`  | Maximum concurrent symbol updates (uses setting default if not provided)   |
| `enable_resampling` | `bool`      | ❌       | `true`  | Whether to automatically resample to other timeframes                      |
| `start_date`        | `date`      | ❌       | `null`  | Optional start date for update range (auto-determined if not provided)     |
| `end_date`          | `date`      | ❌       | `null`  | Optional end date for update range (defaults to yesterday if not provided) |

### Default Symbol Lists

When `symbols` is not provided, the system uses preconfigured symbol lists:

**Large Cap Symbols (51 symbols):**

- AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, BRK.B, UNH, JNJ
- JPM, V, PG, XOM, HD, CVX, MA, PFE, ABBV, BAC, COST, AVGO
- WMT, DIS, ADBE, CRM, NFLX, TMO, ACN, VZ, CSCO, ABT, NKE
- ORCL, COP, MRK, INTC, AMD, TXN, QCOM, DHR, NEE, UPS, PM
- RTX, HON, SPGI, LOW, INTU, IBM, GS

**Mid Cap Symbols (32 symbols):**

- ROKU, SNAP, UBER, LYFT, PINS, TWTR, SQ, SHOP, SPOT, ZM
- DOCU, CRWD, OKTA, SNOW, PLTR, RBLX, COIN, HOOD, SOFI, UPST
- AFRM, PYPL, ETSY, TDOC, PTON, MRNA, BNTX, ZS, DDOG, NET, FSLY, ESTC

**Total Default Symbols: 83**

### Example Request

```json
{
  "symbols": ["AAPL", "MSFT", "GOOGL"],
  "force_validation": true,
  "max_concurrent": 3,
  "enable_resampling": true,
  "start_date": "2024-01-01",
  "end_date": "2024-01-31"
}
```

### Example Request (Use Defaults)

```json
{
  "force_validation": true,
  "enable_resampling": true
}
```

## Response Format

### Immediate Response

The endpoint returns immediately with a request ID for tracking:

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "started",
  "message": "Nightly update started for 83 symbols"
}
```

## Workflow Process

The nightly update executes the following workflow for each symbol:

1. **Data Validation**: Validates existing data completeness and identifies gaps
2. **Date Range Calculation**: Determines update range from last update to yesterday
3. **Data Fetching**: Downloads missing 1-minute data from external providers
4. **Data Storage**: Stores new data in partitioned Parquet files
5. **Resampling**: Automatically generates higher timeframes (5min, 15min, 30min, 1h, 2h, 4h, daily)
6. **Validation**: Re-validates updated data for completeness and accuracy

## Status Tracking Endpoints

### Get Update Status

**GET** `/nightly-update/status/{request_id}`

Returns current status of an update request:

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",
  "started_at": "2024-01-15T02:00:00Z",
  "symbols_count": 83,
  "is_complete": false
}
```

### Get Progress Details

**GET** `/nightly-update/status/{request_id}/progress`

Returns detailed progress information for each symbol in a nightly update request:

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "overall_progress": {
    "completed_symbols": 42,
    "total_symbols": 83,
    "completion_percentage": 50.6,
    "current_phase": "downloading",
    "estimated_completion": "2024-01-15T02:30:00Z"
  },
  "symbol_progress": {
    "AAPL": {
      "symbol": "AAPL",
      "status": "completed",
      "progress_percentage": 100.0,
      "current_message": "Update completed successfully"
    },
    "MSFT": {
      "symbol": "MSFT",
      "status": "downloading",
      "progress_percentage": 65.0,
      "current_message": "Downloading 1-minute data"
    }
  }
}
```

### Get Detailed Results

**GET** `/nightly-update/status/{request_id}/details`

Returns comprehensive results after completion (see NightlyUpdateResponse below).

### List Active Updates

**GET** `/nightly-update/active`

Returns all currently running nightly updates:

```json
[
  {
    "request_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "running",
    "started_at": "2024-01-15T02:00:00Z",
    "symbols_count": 83,
    "duration_seconds": 1245.5
  }
]
```

## Detailed Response Format

### NightlyUpdateResponse

| Field                 | Type                            | Description                                      |
| --------------------- | ------------------------------- | ------------------------------------------------ |
| `request_id`          | `str`                           | Unique identifier for this update request        |
| `started_at`          | `datetime`                      | When the update process started                  |
| `completed_at`        | `datetime`                      | When the update process completed                |
| `summary`             | `NightlyUpdateSummary`          | Summary statistics for the entire operation      |
| `symbol_results`      | `Dict[str, SymbolUpdateResult]` | Detailed results for each symbol                 |
| `symbols_requested`   | `List[str]`                     | Symbols that were requested for update           |
| `symbols_processed`   | `List[str]`                     | Symbols that were actually processed             |
| `max_concurrent_used` | `int`                           | Maximum concurrent updates that were used        |
| `global_errors`       | `List[str]`                     | Global errors that affected the entire operation |
| `overall_success`     | `bool`                          | Whether the overall operation was successful     |

### NightlyUpdateSummary

| Field                            | Type             | Description                                       |
| -------------------------------- | ---------------- | ------------------------------------------------- |
| `total_symbols`                  | `int`            | Total number of symbols processed                 |
| `successful_updates`             | `int`            | Number of successful symbol updates               |
| `failed_updates`                 | `int`            | Number of failed symbol updates                   |
| `total_candles_updated`          | `int`            | Total 1-minute candles updated across all symbols |
| `total_resampled_candles`        | `int`            | Total resampled candles created                   |
| `update_duration_seconds`        | `float`          | Total duration of the update process              |
| `earliest_start_date`            | `date`           | Earliest start date across all symbol updates     |
| `latest_end_date`                | `date`           | Latest end date across all symbol updates         |
| `symbols_with_validation_errors` | `int`            | Number of symbols with validation errors          |
| `total_validation_errors`        | `int`            | Total number of validation errors                 |
| `resampling_summary`             | `Dict[str, int]` | Total candles created per timeframe               |

### SymbolUpdateResult

| Field                     | Type                     | Description                                  |
| ------------------------- | ------------------------ | -------------------------------------------- |
| `symbol`                  | `str`                    | Trading symbol                               |
| `start_date`              | `date`                   | Start date of update range                   |
| `end_date`                | `date`                   | End date of update range                     |
| `success`                 | `bool`                   | Whether the update was successful            |
| `candles_updated`         | `int`                    | Number of 1-minute candles updated           |
| `validation_results`      | `List[ValidationResult]` | Validation results for each trading day      |
| `validation_summary`      | `Dict[str, Any]`         | Summary of validation results                |
| `resampling_results`      | `Dict[str, int]`         | Number of candles created for each timeframe |
| `total_resampled_candles` | `int`                    | Total number of resampled candles            |
| `update_duration_seconds` | `float?`                 | Duration of the update process in seconds    |
| `warnings`                | `List[str]`              | List of warnings during update               |
| `error_message`           | `str?`                   | Error details if update failed               |

## Data Completeness Analysis

### Analyze Data Completeness

**POST** `/nightly-update/data-completeness`

Analyzes data completeness for specified symbols and date range without performing updates.

#### DataCompletenessRequest

| Field                   | Type        | Required | Default | Description                                          |
| ----------------------- | ----------- | -------- | ------- | ---------------------------------------------------- |
| `symbols`               | `List[str]` | ✅       | -       | Symbols to analyze                                   |
| `start_date`            | `date`      | ✅       | -       | Start date for analysis (YYYY-MM-DD)                 |
| `end_date`              | `date`      | ✅       | -       | End date for analysis (YYYY-MM-DD)                   |
| `include_details`       | `bool`      | ❌       | `false` | Whether to include detailed validation results       |
| `auto_fill_gaps`        | `bool`      | ❌       | `false` | Automatically attempt to fill detected gaps          |
| `max_gap_fill_attempts` | `int`       | ❌       | `50`    | Maximum number of gaps to attempt filling per symbol |

#### Example Request

```json
{
  "symbols": ["AAPL", "MSFT", "GOOGL"],
  "start_date": "2024-01-01",
  "end_date": "2024-01-31",
  "include_details": true,
  "auto_fill_gaps": true,
  "max_gap_fill_attempts": 25
}
```

#### DataCompletenessResponse

| Field                       | Type                                | Description                                              |
| --------------------------- | ----------------------------------- | -------------------------------------------------------- |
| `analysis_period`           | `AnalysisPeriod`                    | Start and end dates of the analysis                      |
| `symbol_completeness`       | `Dict[str, SymbolCompletenessData]` | Completeness statistics for each symbol                  |
| `overall_statistics`        | `OverallStatistics`                 | Overall completeness statistics                          |
| `symbols_needing_attention` | `List[str]`                         | Symbols that need immediate attention due to data issues |
| `recommendations`           | `List[str]`                         | Recommendations for improving data completeness          |

#### SymbolCompletenessData

| Field                        | Type                          | Description                                        |
| ---------------------------- | ----------------------------- | -------------------------------------------------- |
| `total_trading_days`         | `int`                         | Total trading days in the period                   |
| `valid_days`                 | `int`                         | Number of days with valid data                     |
| `invalid_days`               | `int`                         | Number of days with invalid data                   |
| `completeness_percentage`    | `float`                       | Percentage of data completeness                    |
| `total_expected_candles`     | `int`                         | Total expected number of candles                   |
| `total_actual_candles`       | `int`                         | Total actual number of candles found               |
| `missing_candles`            | `int`                         | Number of missing candles                          |
| `full_days_count`            | `int`                         | Number of full trading days (390 candles expected) |
| `half_days_count`            | `int`                         | Number of half trading days (210 candles expected) |
| `days_with_gaps`             | `int`                         | Number of days with data gaps                      |
| `total_missing_periods`      | `int`                         | Total number of distinct missing periods           |
| `average_daily_completeness` | `float`                       | Average completeness percentage per day            |
| `worst_day_completeness`     | `float`                       | Worst single day completeness percentage           |
| `best_day_completeness`      | `float`                       | Best single day completeness percentage            |
| `validation_results`         | `List[ValidationResultModel]` | Detailed validation results (if requested)         |
| `gap_fill_attempted`         | `bool`                        | Whether gap filling was attempted                  |
| `total_gaps_found`           | `int`                         | Total number of gaps found                         |
| `gaps_filled_successfully`   | `int`                         | Number of gaps filled successfully                 |
| `gaps_vendor_unavailable`    | `int`                         | Number of gaps where vendor data is unavailable    |

## Intelligent Update Strategy

The nightly update system implements several intelligent strategies:

1. **Incremental Updates**: Only fetches data from the last update date to prevent redundant downloads
2. **Trading Day Awareness**: Automatically skips weekends and holidays
3. **Data Validation**: Validates existing data before and after updates
4. **Automatic Resampling**: Generates all higher timeframes from 1-minute data
5. **Concurrent Processing**: Processes multiple symbols simultaneously with rate limiting
6. **Error Recovery**: Individual symbol failures don't affect other symbols

## Data Storage Structure

Updated data is stored in the same partitioned structure as the trading data update API:

- **Daily data**: `storage/candles/daily/{SYMBOL}.parquet`
- **Intraday data**: `storage/candles/{timeframe}/{SYMBOL}/{YYYY-MM-DD}.parquet`

## Background Processing

The nightly update runs as a background task, allowing the API to return immediately while processing continues. Use the status endpoints to monitor progress and retrieve results.

## Error Handling

- **Per-symbol isolation**: Individual symbol failures don't affect other symbols
- **Detailed error reporting**: Each failure includes specific error messages and validation details
- **Graceful degradation**: Partial updates are completed even if some symbols fail
- **Comprehensive logging**: All operations are logged for debugging and monitoring

## Testing

### Paid API Tests

The nightly update API includes comprehensive end-to-end tests that use real data providers. **⚠️ WARNING: These tests incur actual API charges and should only be run when needed.**

#### Running Tests

Use the provided test script to run paid API tests:

```bash
cd backend
./src/tests/run_scripts/run_nightly_update_paid_api_tests.sh
```

The script provides interactive options to control costs:

1. **Core functionality tests** (recommended for first run)

   - Tests 3 symbols with data scenarios
   - Validates fresh downloads, gap filling, resampling accuracy
   - Estimated cost: ~$1-3
   - **Most comprehensive validation per dollar**

2. **Complete pipeline tests**

   - Tests 2 symbols with full pipeline validation
   - Estimated cost: ~$2-5
   - Validates storage structure and data integrity

3. **All tests**
   - Comprehensive testing suite
   - Estimated cost: ~$3-8
   - Full validation with improved cost efficiency

#### Test Classes and Methods

**TestNightlyUpdatePaidAPI** - Core functionality tests:

- `test_paid_nightly_update_small_symbol_list` - Tests 3 symbols (AAPL, MSFT, GOOGL) with validation and resampling
- `test_paid_nightly_update_data_scenarios_validation` - **NEW**: Tests critical data scenarios (fresh downloads, gap filling, resampling accuracy)
- `test_paid_nightly_update_status_tracking` - Tests status tracking endpoints during execution
- `test_paid_data_completeness_analysis` - Tests data completeness analysis endpoint

**TestNightlyUpdateCompleteEndToEndPipeline** - Complete pipeline validation:

- `test_paid_complete_nightly_update_pipeline_validation` - Full pipeline test with storage validation

#### Test Features

- **Real API Integration**: Tests use actual data providers (Polygon, Financial Modeling Prep)
- **Cost Control**: Interactive script prevents accidental high-cost runs
- **Isolated Storage**: Test data stored in `backend/test_storage/` (separate from production)
- **Comprehensive Validation**: Tests API responses, data storage, resampling, and error handling
- **Status Tracking**: Validates real-time status updates during long-running operations

#### Prerequisites

Before running tests, ensure API keys are configured:

```bash
# In your .env file or environment
export POLYGON__API_KEY='your_polygon_key'
export FINANCIAL_MODELING_PREP__API_KEY='your_fmp_key'
```

#### Test Data Cleanup

Test data is stored separately from production data:

- **Test storage**: `backend/test_storage/`
- **Production storage**: `backend/storage/`

Clean up test data after testing:

```bash
rm -rf backend/test_storage/
```

## Example Complete Response

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "started_at": "2024-01-15T02:00:00Z",
  "completed_at": "2024-01-15T02:45:30Z",
  "summary": {
    "total_symbols": 83,
    "successful_updates": 81,
    "failed_updates": 2,
    "total_candles_updated": 125420,
    "total_resampled_candles": 89750,
    "update_duration_seconds": 2730.5,
    "earliest_start_date": "2024-01-10",
    "latest_end_date": "2024-01-14",
    "symbols_with_validation_errors": 3,
    "total_validation_errors": 7,
    "resampling_summary": {
      "5min": 25084,
      "15min": 8361,
      "30min": 4180,
      "1h": 2090,
      "2h": 1045,
      "4h": 522,
      "daily": 420
    }
  },
  "symbol_results": {
    "AAPL": {
      "symbol": "AAPL",
      "start_date": "2024-01-10",
      "end_date": "2024-01-14",
      "success": true,
      "candles_updated": 1950,
      "validation_results": [...],
      "resampling_results": {
        "5min": 390,
        "15min": 130,
        "30min": 65,
        "1h": 32,
        "2h": 16,
        "4h": 8,
        "daily": 5
      },
      "total_resampled_candles": 646,
      "error_message": null
    }
  },
  "symbols_requested": null,
  "symbols_processed": ["AAPL", "MSFT", ...],
  "max_concurrent_used": 3,
  "overall_success": false
}
```
