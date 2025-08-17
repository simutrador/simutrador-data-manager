# OHLCV Manager

The OHLCV Manager is SimuTrador's comprehensive data management system that handles the complete lifecycle of trading data from fetching to storage and analysis. It provides a robust, scalable solution for managing historical price data across multiple timeframes and asset types.

## Overview

The OHLCV Manager orchestrates several key components to provide reliable trading data:

- **Data Fetching**: Efficient 1-minute data retrieval from multiple providers (Polygon.io, Financial Modeling Prep, Tiingo)
- **Data Storage**: Partitioned Parquet file storage with optimized folder structure
- **Data Resampling**: Intelligent conversion from 1-minute to various timeframes (5min, 15min, 30min, 1h, 2h, 4h, daily)
- **Data Validation**: Comprehensive completeness analysis and gap detection
- **Data Analysis**: Quality assessment and reporting tools
- **Automated Updates**: Nightly update workflows for maintaining current data

## Architecture

### Core Components

1. **Data Providers** (`services/data_providers/`)

   - Vendor-agnostic interface for multiple data sources
   - Polygon.io client with intelligent batching and rate limiting
   - Support for Financial Modeling Prep and Tiingo
   - Automatic retry mechanisms and error handling

2. **Storage Service** (`services/storage/data_storage_service.py`)

   - Partitioned Parquet file storage: `storage/candles/timeframe/symbol/date.parquet`
   - Efficient pagination and data retrieval
   - Automatic deduplication and data merging
   - Optimized last update date detection

3. **Resampling Service** (`services/storage/data_resampling_service.py`)

   - Asset-type-aware resampling strategies
   - Pandas-based OHLCV aggregation
   - Market session alignment for different asset classes
   - Bulk resampling capabilities

4. **Validation Service** (`services/validation/stock_market_validation_service.py`)

   - Trading day validation using market calendars
   - Data completeness analysis
   - Gap detection and reporting
   - Market hours and holiday handling

5. **Workflow Services** (`services/workflows/`)
   - Orchestrated nightly update processes
   - Multi-symbol concurrent processing
   - Progress tracking and status reporting
   - Error handling and recovery

## API Endpoints

### Trading Data API (`/trading-data`)

#### Get Trading Data

```http
GET /trading-data/data/{symbol}
```

Retrieve stored trading data for a symbol with pagination support.

**Parameters:**

- `symbol` (path): Trading symbol (e.g., AAPL, MSFT)
- `timeframe` (query): Data timeframe (default: "1min")
  - Supported: `1min`, `5min`, `15min`, `30min`, `1h`, `2h`, `4h`, `daily`
- `start_date` (query): Start date filter (YYYY-MM-DD format)
- `end_date` (query): End date filter (YYYY-MM-DD format)
- `order_by` (query): Sort order - `asc` or `desc` (default: desc)
- `page` (query): Page number (1-based, default: 1)
- `page_size` (query): Items per page (default: 1000, max: 10000)

**Response:**

```json
{
  "symbol": "AAPL",
  "timeframe": "1min",
  "candles": [
    {
      "date": "2024-01-15T20:00:00Z",
      "open": "185.50",
      "high": "186.25",
      "low": "185.30",
      "close": "186.00",
      "volume": "1250000.00000000"
    }
  ],
  "start_date": "2024-01-15T13:30:00Z",
  "end_date": "2024-01-15T20:00:00Z",
  "pagination": {
    "page": 1,
    "page_size": 1000,
    "total_items": 390,
    "total_pages": 1,
    "has_next": false,
    "has_previous": false
  }
}
```

#### List Stored Symbols

```http
GET /trading-data/symbols
```

List all symbols that have stored data.

**Parameters:**

- `timeframe` (query): Timeframe to check (default: "1min")

**Response:**

```json
["AAPL", "GOOGL", "MSFT", "TSLA"]
```

### Nightly Update API (`/nightly-update`)

#### Start Nightly Update

```http
POST /nightly-update/start
```

Trigger the complete nightly update workflow for stock market data.

**Request Body:**

```json
{
  "symbols": ["AAPL", "GOOGL", "MSFT"], // Optional, uses default if null
  "start_date": "2024-01-01", // Optional override
  "end_date": "2024-01-15", // Optional override
  "force_validation": true, // Default: true
  "enable_resampling": true, // Default: true
  "max_concurrent_symbols": 5 // Default: from settings
}
```

**Response:**

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "started",
  "message": "Nightly update started for 3 symbols"
}
```

#### Get Update Status

```http
GET /nightly-update/status/{request_id}
```

Get detailed status and progress information for a nightly update request.

**Response:**

```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "in_progress",
  "started_at": "2024-01-15T02:00:00Z",
  "symbols_count": 3,
  "is_complete": false,
  "progress": {
    "total_symbols": 3,
    "completed_symbols": 1,
    "current_symbol": "GOOGL",
    "current_step": "Downloading 1-minute data",
    "progress_percentage": 33.3,
    "estimated_time_remaining_seconds": 120,
    "symbols_in_progress": ["GOOGL"]
  }
}
```

#### Get Progress Details

```http
GET /nightly-update/status/{request_id}/progress
```

Get detailed progress information for each symbol in the update.

#### Get Update Results

```http
GET /nightly-update/status/{request_id}/details
```

Get complete results of a finished nightly update.

#### List Active Updates

```http
GET /nightly-update/active
```

List all currently running nightly update requests.

### Data Analysis API (`/data-analysis`)

#### Analyze Data Completeness

```http
POST /data-analysis/completeness
```

Analyze data completeness for specified symbols and date range.

**Request Body:**

```json
{
  "symbols": ["AAPL", "GOOGL"],
  "start_date": "2024-01-01",
  "end_date": "2024-01-15",
  "include_details": true,
  "auto_fill_gaps": false,
  "max_gap_fill_attempts": 3
}
```

**Response:**

```json
{
  "analysis_period": {
    "start_date": "2024-01-01",
    "end_date": "2024-01-15"
  },
  "symbol_completeness": {
    "AAPL": {
      "total_trading_days": 10,
      "valid_days": 9,
      "invalid_days": 1,
      "completeness_percentage": 98.5,
      "total_expected_candles": 3900,
      "total_actual_candles": 3841,
      "missing_candles": 59,
      "validation_results": [...]
    }
  },
  "overall_statistics": {
    "total_symbols": 2,
    "total_trading_days": 20,
    "total_valid_days": 18,
    "overall_completeness_percentage": 97.8,
    "total_expected_candles": 7800,
    "total_actual_candles": 7629,
    "total_missing_candles": 171
  },
  "symbols_needing_attention": ["GOOGL"],
  "recommendations": [
    "1 symbols have less than 95% data completeness",
    "Consider running a full data update to improve completeness"
  ]
}
```

## Data Models

### PriceCandle

The core data model representing OHLCV (Open, High, Low, Close, Volume) data:

```python
{
  "date": "2024-01-15T20:00:00Z",      # UTC timestamp
  "open": "185.50",                    # Opening price (Decimal, 2 decimal places)
  "high": "186.25",                    # Highest price (Decimal, 2 decimal places)
  "low": "185.30",                     # Lowest price (Decimal, 2 decimal places)
  "close": "186.00",                   # Closing price (Decimal, 2 decimal places)
  "volume": "1250000.00000000"         # Trading volume (Decimal, 8 decimal places)
}
```

### Supported Timeframes

- `1min` - 1-minute candles (source data)
- `5min` - 5-minute candles
- `15min` - 15-minute candles
- `30min` - 30-minute candles
- `1h` - 1-hour candles
- `2h` - 2-hour candles
- `4h` - 4-hour candles
- `daily` - Daily candles

## Data Storage Structure

The system uses a partitioned Parquet file structure for optimal performance:

```
storage/
└── candles/
    ├── 1min/
    │   ├── AAPL/
    │   │   ├── 2024-01-15.parquet
    │   │   ├── 2024-01-16.parquet
    │   │   └── ...
    │   └── GOOGL/
    │       └── ...
    ├── 5min/
    │   └── ...
    ├── daily/
    │   ├── AAPL.parquet
    │   ├── GOOGL.parquet
    │   └── ...
    └── ...
```

### Storage Benefits

- **Partitioned by timeframe and symbol**: Enables efficient querying
- **Daily files for intraday data**: Optimizes loading and reduces memory usage
- **Single file for daily data**: Simplifies daily candle management
- **Parquet format**: Provides compression and fast columnar access
- **Automatic deduplication**: Prevents duplicate data on updates

## Resampling Strategy

The system uses asset-type-aware resampling to match external provider aggregation patterns:

### US Equity (AAPL, MSFT, etc.)

- **Market Hours**: 09:30-16:00 ET (13:30-20:00 UTC)
- **Short Timeframes** (5min, 15min, 30min): `offset='13h30min'` (market session aligned)
- **Daily Boundary**: Market close (20:00 UTC / 16:00 ET)
- **Rationale**: Matches US market session boundaries

### Crypto (BTC-USD, ETH-USDT, etc.)

- **Market Hours**: 24/7 continuous trading
- **All Timeframes**: Standard UTC alignment (no offset)
- **Daily Boundary**: UTC midnight (00:00 UTC)
- **Rationale**: Matches provider's UTC-based crypto aggregation

### Forex (EURUSD, GBP/USD, etc.)

- **Market Hours**: 24/5 global sessions
- **Short Timeframes**: `offset='8h00min'` (London session aligned)
- **Daily Boundary**: UTC midnight (00:00 UTC)
- **Rationale**: Aligns with major forex trading session

### OHLCV Aggregation Rules

- **Open**: First value in the period
- **High**: Maximum value in the period
- **Low**: Minimum value in the period
- **Close**: Last value in the period
- **Volume**: Sum of all volumes in the period

## Data Validation

### Trading Day Validation

- Uses `pandas_market_calendars` for official NYSE trading days
- Validates market hours: 09:30-16:00 ET (13:30-20:00 UTC)
- Handles market holidays and half-days
- Expected candles per trading day:
  - Full day: 390 candles (6.5 hours × 60 minutes)
  - Half day: 210 candles (3.5 hours × 60 minutes)

### Completeness Analysis

- **Per-symbol analysis**: Individual symbol data quality metrics
- **Gap detection**: Identifies missing time periods
- **Completeness percentage**: Actual vs expected candle counts
- **Quality thresholds**: Flags symbols with <95% completeness
- **Recommendations**: Automated suggestions for data improvement

### Gap Filling

- **Automatic gap detection**: Identifies missing data periods
- **Polygon URL generation**: Creates API URLs for missing data
- **Retry mechanisms**: Handles temporary API failures
- **Progress tracking**: Reports gap filling success rates

## Nightly Update Workflow

The nightly update process ensures data currency through automated workflows:

### Process Steps

1. **Validation Phase**

   - Check existing data completeness
   - Identify symbols needing updates
   - Determine date ranges for updates

2. **Data Fetching Phase**

   - Download missing 1-minute data from providers
   - Handle rate limiting and retries
   - Validate and store new data

3. **Resampling Phase**

   - Generate all target timeframes from 1-minute data
   - Apply asset-type-aware resampling strategies
   - Store resampled data in appropriate partitions

4. **Reporting Phase**
   - Generate update summaries
   - Report success/failure statistics
   - Provide recommendations for data quality

### Concurrency and Performance

- **Concurrent symbol processing**: Configurable parallelism
- **Intelligent batching**: Optimizes API usage
- **Progress tracking**: Real-time status updates
- **Error isolation**: Individual symbol failures don't affect others

## Configuration

### Data Provider Settings

```python
# Default provider priority
providers = ["polygon", "financial_modeling_prep", "tiingo"]

# Rate limiting
polygon_requests_per_minute = 5
max_concurrent_symbols = 5
```

### Storage Settings

```python
# Base storage path
base_path = "storage"
candles_path = "candles"

# Pagination defaults
default_page_size = 1000
max_page_size = 10000
```

### Nightly Update Settings

```python
# Update configuration
enable_auto_resampling = true
max_concurrent_symbols = 5
default_symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]

# Target timeframes for resampling
target_timeframes = ["5min", "15min", "30min", "1h", "2h", "4h", "daily"]
```

## Error Handling

### Provider Errors

- **Rate limiting**: Automatic backoff and retry
- **Authentication failures**: Clear error messages
- **Data unavailability**: Graceful degradation
- **Network issues**: Exponential backoff retry

### Storage Errors

- **Disk space**: Monitoring and alerts
- **File corruption**: Validation and recovery
- **Permission issues**: Clear error reporting
- **Concurrent access**: File locking mechanisms

### Validation Errors

- **Missing data**: Gap identification and filling
- **Data quality**: Outlier detection and reporting
- **Format issues**: Data cleaning and normalization
- **Timezone handling**: Consistent UTC conversion

## Performance Optimization

### Storage Optimizations

- **Columnar storage**: Parquet format for fast queries
- **Partitioning**: Efficient data organization
- **Compression**: Reduced storage footprint
- **Indexing**: Fast symbol and date lookups

### Query Optimizations

- **Pagination**: Memory-efficient data retrieval
- **Date filtering**: Optimized range queries
- **Lazy loading**: Load only required data
- **Caching**: Frequently accessed data caching

### Processing Optimizations

- **Vectorized operations**: Pandas-based processing
- **Batch processing**: Efficient bulk operations
- **Parallel processing**: Multi-symbol concurrency
- **Memory management**: Streaming data processing

## Monitoring and Observability

### Logging

- **Structured logging**: JSON format for analysis
- **Log levels**: Configurable verbosity
- **Performance metrics**: Timing and throughput data
- **Error tracking**: Detailed error context

### Metrics

- **Data completeness**: Per-symbol quality metrics
- **Update performance**: Processing times and throughput
- **API usage**: Rate limiting and quota tracking
- **Storage utilization**: Disk usage and growth trends

### Health Checks

- **Data freshness**: Last update timestamps
- **Service availability**: API endpoint health
- **Storage health**: File system status
- **Provider connectivity**: External API status
