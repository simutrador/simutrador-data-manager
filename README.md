# SimuTrador Data Manager

A comprehensive trading simulation platform consisting of a backend execution engine and frontend interface.

## 🏗️ Architecture

This repository contains two main components:

### Backend (`/backend`)
- **FastAPI-based execution engine** for trading simulation
- **Stateless design** for processing trading orders against historical data
- **RESTful API** with comprehensive validation
- **Integration** with multiple data providers (Polygon, Financial Modeling Prep, Tiingo)

### Frontend (`/frontend`)
- **Angular-based web interface** for trading simulation
- **Interactive charts** and market data visualization
- **Order management** and portfolio tracking
- **Real-time simulation** controls and monitoring

## 🚀 Quick Start

### Backend
```bash
cd backend
uv sync
uv run fastapi dev src/main.py
# Access API at http://localhost:8002
```

### Frontend
```bash
cd frontend
npm install
ng serve
# Access UI at http://localhost:4200
```

## �� Dependencies

The backend uses `simutrador-core` package for shared models and utilities:
- Published to TestPyPI and PyPI
- Provides Pydantic models for price data, orders, and WebSocket communication
- Includes enums for order types, sides, and asset classifications

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

- Backend API documentation: `http://localhost:8002/docs` (when running)
- Frontend documentation: See `/frontend/README.md`
- Backend documentation: See `/backend/README.md`

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
