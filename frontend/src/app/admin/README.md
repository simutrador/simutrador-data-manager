# Admin Control Panel

This is an internal admin control panel for managing trading data operations. It provides a web-based interface for administrators to:

## Features

### 1. Data Update Panel

- **On-demand data updates** for specific symbols and timeframes
- **Date range selection** for targeted updates
- **Timeframe selection** (1min, 5min, 15min, 30min, 1h, 2h, 4h, daily)
- **Force update option** to re-fetch existing data
- **Real-time progress tracking** with success/failure counts

### 2. Data Retrieval Panel

- **Query trading data** for specific symbols
- **Timeframe and date filtering**
- **Tabular data display** similar to pandas DataFrame format
- **Export capabilities** (planned)

### 3. Nightly Update Panel

- **Automated data updates** for different asset types
- **Asset type selection** (US Stocks, Crypto, Forex)
- **Schedule management** and status monitoring
- **Force update options** for automated processes

## Architecture Refactoring

The admin control panel has been **refactored from a monolithic component into focused, modular components** for better maintainability and scalability:

### Before (Monolithic)

- Single large component handling all functionality
- Difficult to test individual features
- Hard to reuse functionality across different admin pages

### After (Modular)

- **TradingDataUpdateComponent** - Focused on data updates
- **TradingDataRetrievalComponent** - Focused on data fetching/display
- **NightlyUpdateComponent** - Focused on automated processes
- **AdminControlPanelComponent** - Container orchestrating the components

> 📖 **Detailed component documentation**: See [components/README.md](./components/README.md)

## Technical Implementation

### Components

#### Main Components

- `AdminControlPanelComponent` - Container component orchestrating the admin interface

#### Modular Components (in `components/` directory)

- `TradingDataRetrievalComponent` - Fetches and displays trading data
- `NightlyUpdateComponent` - Manages automated nightly data updates (replaces manual updates)

#### Services

- `AdminService` - Service for API communication
- `AdminGuard` - Route protection (placeholder for authentication)
- `ErrorHandlerInterceptor` - API error handling

### API Integration

The panel integrates with the backend APIs:

- `/nightly-update/start` - Start nightly data updates (replaces manual updates)
- `/nightly-update/status/{request_id}` - Get update status and progress
- `/trading-data/data/{symbol}` - Retrieve trading data
- `/trading-data/symbols` - List available symbols

### Styling

- Uses **DaisyUI** components for consistent styling
- **Tailwind CSS** for responsive design
- **Dark/Light theme support** via DaisyUI theme controller

## Usage

### Starting Data Updates

1. Enter symbols (comma-separated) or leave empty for all symbols
2. Select desired timeframes using checkboxes
3. Optionally set date range
4. Enable "Force update" if needed to re-fetch existing data
5. Click "Start Update" and monitor progress

### Retrieving Data

1. Enter a single symbol (e.g., "AAPL")
2. Select timeframe from dropdown
3. Optionally set date range filters
4. Click "Fetch Data" to retrieve and display data in tabular format

### Monitoring

- System stats refresh automatically every 30 seconds
- Update progress is shown in real-time
- Symbol status indicators update based on latest operations

## Security Note

This is an **internal admin tool** and should only be accessible to authorized administrators. It provides direct access to data management operations and should be properly secured in production environments.

## Future Enhancements

- **Nightly update scheduling** interface
- **Data completeness analysis** tools
- **Bulk operations** for multiple symbols
- **Export functionality** for retrieved data
- **Advanced filtering** and search capabilities
- **Audit logging** for admin actions
