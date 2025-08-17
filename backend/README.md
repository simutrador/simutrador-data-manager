# Trading Simulator Backend

A stateless execution engine designed to simulate the execution of trading orders against historical market data.

## 🚀 Quick Start

```bash
# Install dependencies
uv sync

# Run development server
uv run fastapi dev src/main.py

# Access the API
# - Main API: http://localhost:8002
# - Interactive docs: http://localhost:8002/docs
```

## 📊 Current Status

- ✅ **API Framework**: FastAPI with comprehensive routing
- ✅ **Data Models**: Complete Pydantic models with validation
- ✅ **Type Safety**: Strict type checking with pyright
- ✅ **Development Environment**: Fully configured with uv, ruff, and VS Code
- 🔄 **Simulation Engine**: Placeholder implementation (core logic pending)

## 📚 Documentation

See [documentation/main.md](documentation/main.md) for detailed technical documentation, API schemas, and development setup.

## 🛠️ Development

```bash
# Type checking
uv run pyright

# Code formatting
uv run ruff format

# Code linting
uv run ruff check
```

## 🏗️ Architecture

- **Framework**: FastAPI 0.115.12+
- **Validation**: Pydantic 2.11.5+
- **Package Manager**: uv
- **Type Checker**: pyright (strict mode)
- **Code Formatter**: ruff
- **Python**: 3.13+ (dev), 3.11+ (target)
