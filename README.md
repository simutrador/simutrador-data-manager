# SimuTrador Core

🎯 **Shared models and utilities for the SimuTrador trading simulation ecosystem**

A Python package providing common data models, enums, and utilities used across all SimuTrador components. This library ensures type safety and consistency across the distributed architecture.

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

## 📚 Documentation

### Local Documentation

- **WebSocket API v2**: [`docs/ws_api_v2.md`](docs/ws_api_v2.md) - Real-time communication specification
- **Model Examples**: [`docs/simutrade_ws_models_sample.py`](docs/simutrade_ws_models_sample.py) - Usage examples

### Project-Wide Documentation

- **Project Overview**: [SimuTrador Architecture](https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/main.md)
- **Data Manager**: [simutrador-data-manager](https://github.com/simutrador/simutrador-data-manager) - Data collection and management system
- **Migration History**: [Repository Migration Plan](https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/repository-migration-plan.md)

### API Reference

- **Package Documentation**: Coming soon - comprehensive API reference
- **Type Definitions**: All models include full type hints and docstrings

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

- **Data Management Guide**: [`docs/ohlcv_manager.md`](docs/ohlcv_manager.md) - Comprehensive data management documentation
- **API Documentation**: [`docs/data analysis.md`](docs/data%20analysis.md) & [`docs/nightly update.md`](docs/nightly%20update.md) - API endpoint details
- **Backend API**: `http://localhost:8002/docs` (when running) - Interactive API documentation
- **Frontend Guide**: [`frontend/README.md`](frontend/README.md) - Angular application setup
- **Backend Guide**: [`backend/README.md`](backend/README.md) - FastAPI backend setup

### Project-Wide Documentation

- **Project Overview**: [SimuTrador Architecture](https://github.com/simutrador/simutrador-docs/blob/main/SimuTrador/main.md)
- **WebSocket API**: [WebSocket API v2](https://github.com/simutrador/simutrador-core/blob/main/docs/ws_api_v2.md) - Real-time communication spec
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
