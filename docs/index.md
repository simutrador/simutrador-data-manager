# SimuTrador Data Manager

🎯 **Open-source OHLCV data collection and management system for SimuTrador**

FastAPI backend and Angular frontend providing data ingestion, storage, validation, analysis, and APIs.

### Related Documentation

- Project Overview (architecture): https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/main.md
- WebSocket API v2 (central spec): https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/simutrador-server/ws_api_v2.md
- Core Models (shared library): https://github.com/simutrador/simutrador-core
- API Docs (this repo): docs/ohlcv-manager.md, docs/nightly-update.md, docs/data-analysis.md
- Backend Guide: backend/README.md
- Frontend Guide: frontend/README.md
- AI Index (agent entry): ai-index.md
- STATUS (current status/milestones): STATUS.md

## 🏗️ What's Included

### Core Models (`simutrador_core.models`)

- **Price Data Models**: `PriceCandle`, `PriceDataSeries`, `PriceQuote` - OHLCV data structures
- **WebSocket Models**: Real-time communication models for trading simulation
- **Asset Types**: Classification and metadata for different asset classes
- **Enums**: Order types, sides, timeframes, and other standardized values

### Key Features

- ✅ **Strict Type Safety** - Full Pydantic validation with type hints
- ✅ **Cross-Component Consistency** - Shared models prevent integration issues
- ✅ **Modern Python** - Uses Python 3.11+ features and syntax
- ✅ **Well Documented** - Comprehensive docstrings and examples
- ✅ **Tested & Reliable** - Quality gates ensure stability

## 🚀 Quick Start

### Installation

```bash
# Install from PyPI
pip install simutrador-core

# Or install from TestPyPI (latest development)
pip install -i https://test.pypi.org/simple/ simutrador-core
```

### Basic Usage

```python
from simutrador_core.models import PriceCandle, PriceDataSeries
from simutrador_core.enums import Timeframe, AssetType
from datetime import datetime

# Create a price candle
candle = PriceCandle(
    timestamp=datetime.now(),
    open=100.0,
    high=105.0,
    low=99.0,
    close=103.0,
    volume=1000
)

# Create a price data series
series = PriceDataSeries(
    symbol="AAPL",
    timeframe=Timeframe.FIVE_MIN,
    asset_type=AssetType.US_EQUITY,
    candles=[candle]
)
```

## 📦 Dependencies

- **Python 3.11+** - Modern Python features and performance
- **Pydantic 2.11+** - Data validation and serialization
- **Pandas 2.3+** - Data manipulation and analysis

## 🛠️ Development

### Backend Development

```bash
cd backend
uv run pyright          # Type checking
uv run ruff format      # Code formatting
uv run ruff check       # Linting
uv run pytest          # Run tests
```

### Frontend Development

```bash
cd frontend
ng build                # Build for production
ng test                 # Run unit tests
ng lint                 # Linting
```

## 📚 Documentation

### Local Documentation

- **Data Management Guide:** [`./ohlcv-manager.md`](./ohlcv-manager.md)
- **API Documentation:** [`./data-analysis.md`](./data-analysis.md) & [`./nightly-update.md`](./nightly-update.md)

endpoint details

- **Backend API**: `http://localhost:8002/docs` (when running) - Interactive API documentation
- Frontend Guide: https://github.com/simutrador/simutrador-data-manager/tree/main/frontend
- Backend Guide: https://github.com/simutrador/simutrador-data-manager/tree/main/backend

### Project-Wide Documentation

- **Project Overview**: [SimuTrador Architecture](https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/main.md)
- **WebSocket API**: [WebSocket API v2](https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/simutrador-server/ws_api_v2.md) - Real-time communication spec
- **Core Models**: [simutrador-core](https://github.com/simutrador/simutrador-core) - Shared data models and utilities
- **Migration History**: [Repository Migration Plan](https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/repository-migration-plan.md)

## 🔧 Configuration

Both components support environment-based configuration:

- Backend: Uses `.env` files in `/backend/src/environments/`
- Frontend: Uses Angular environment files

## 📊 Current Status

- ✅ **Backend API Framework**: Complete FastAPI implementation
- ✅ **Data Models**: Comprehensive Pydantic models with validation
- ✅ **Frontend Framework**: Angular application with routing
- ✅ **Type Safety**: Strict type checking for both components
- 🔄 **Integration**: Backend-frontend communication (in progress)
- 🔄 **Simulation Engine**: Core trading logic implementation (in progress)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## 📄 License

See LICENSE file for details.
