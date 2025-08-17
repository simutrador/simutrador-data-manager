# Test Structure

This directory contains all tests for the trading data system, organized by cost and purpose.

## 🆓 FREE Tests (Run Automatically)

These tests use mocked data and run on every git push. They are completely free and safe to run.

```
src/tests/
├── unit/                    # Pure unit tests
├── integration/             # Integration tests (mocked external services)
├── services/               # Service layer tests (mocked)
├── models/                 # Data model tests
├── api/                    # API endpoint tests (mocked external APIs)
│   ├── test_nightly_update_api.py     # ✅ FREE - Mocked
│   └── test_trading_data_api.py       # ✅ FREE - Mocked
└── e2e/                    # End-to-end tests (mocked external services)
    └── test_complete_resampling_validation.py  # ✅ FREE - Mocked
```

## 💰 PAID Tests (Manual Only)

These tests use REAL external APIs and WILL INCUR CHARGES. They are excluded from automated runs.

```
src/tests/
└── paid_api/               # 💰 PAID - Real external API calls
    └── test_trading_data_paid_api.py   # ⚠️ COSTS MONEY
```

## 🚀 Running Tests

### Regular Development (FREE)
```bash
# Run all free tests (default)
pytest

# Run specific free test categories
pytest src/tests/api/                    # API tests (mocked)
pytest src/tests/unit/                   # Unit tests
pytest src/tests/integration/            # Integration tests
pytest src/tests/e2e/                    # E2E tests (mocked)
```

### Git Push (Automatic - FREE)
The pre-push hook automatically runs:
```bash
pytest src/tests/ -v -m "not paid_api"
```

### Paid API Testing (MANUAL ONLY - COSTS MONEY)
```bash
# Use the safety script (asks for confirmation)
./run_paid_api_tests.sh

# Or run directly (no confirmation)
pytest -m paid_api src/tests/paid_api/ -v -s
```

## 🏷️ Test Markers

| Marker | Description | Auto Run | Cost |
|--------|-------------|----------|------|
| `@pytest.mark.paid_api` | Uses real external APIs | ❌ No | 💰 Costs money |
| `@pytest.mark.e2e` | End-to-end tests | ✅ Yes | 🆓 Free (if mocked) |
| `@pytest.mark.slow` | Slow running tests | ✅ Yes | 🆓 Free |
| `@pytest.mark.integration` | Integration tests | ✅ Yes | 🆓 Free |

## 🛡️ Safety Features

1. **Automatic Exclusion**: Paid tests are excluded from git push hooks
2. **Clear Folder Structure**: Paid tests are in a separate `paid_api/` folder
3. **Confirmation Script**: `run_paid_api_tests.sh` asks for confirmation
4. **Clear Warnings**: All paid tests have 💰 warnings in docstrings
5. **API Key Validation**: Scripts check for required API keys before running

## 📊 Test Categories by Purpose

### API Endpoint Tests
- **FREE**: `src/tests/api/` - Tests API endpoints with mocked external services
- **PAID**: `src/tests/paid_api/` - Tests API endpoints with real external services

### Data Processing Tests  
- **FREE**: `src/tests/e2e/` - Tests complete data workflows with mocked data
- **FREE**: `src/tests/integration/` - Tests service integration with mocks

### Core Logic Tests
- **FREE**: `src/tests/unit/` - Tests individual functions and classes
- **FREE**: `src/tests/services/` - Tests service layer logic
- **FREE**: `src/tests/models/` - Tests data models and validation

## ⚠️ Important Notes

- **Never commit API keys** to the repository
- **Paid tests are excluded by default** - you must explicitly run them
- **Check your API billing** after running paid tests
- **Use small date ranges** in paid tests to minimize costs
- **Paid tests require valid API keys** in environment variables
