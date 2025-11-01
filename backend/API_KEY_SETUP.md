# API Key Setup Guide

## Overview

The SimuTrador Data Manager requires API keys from data providers to download market data. Without these keys, the nightly update and data downloading will fail silently.

## Supported Data Providers

### 1. **Polygon.io** (Default Provider)
- **Website**: https://polygon.io/
- **Free Tier**: Yes (limited requests)
- **API Key Format**: `POLYGON__API_KEY=your_key_here`
- **Rate Limit**: 5 requests/minute (free tier)

### 2. **Financial Modeling Prep** (Fallback Provider)
- **Website**: https://financialmodelingprep.com/
- **Free Tier**: Yes (limited requests)
- **API Key Format**: `FINANCIAL_MODELING_PREP__API_KEY=your_key_here`
- **Rate Limit**: 300 requests/minute (free tier)

### 3. **Tiingo** (Fallback Provider)
- **Website**: https://www.tiingo.com/
- **Free Tier**: Yes (limited requests)
- **API Key Format**: `TIINGO__API_KEY=your_key_here`
- **Rate Limit**: 50 requests/hour (free tier)

## Configuration Methods

### Method 1: Environment Variables (Recommended for Development)

Set environment variables before running the backend:

```bash
export POLYGON__API_KEY='your_polygon_key'
export FINANCIAL_MODELING_PREP__API_KEY='your_fmp_key'
export TIINGO__API_KEY='your_tiingo_key'

# Then run the backend
cd backend/src
python -m uvicorn main:app --reload
```

### Method 2: .env File (Recommended for Local Development)

1. Edit `backend/src/environments/.env.dev`:

```bash
# Polygon API (default provider)
POLYGON__API_KEY=your_polygon_key

# Financial Modeling Prep API (fallback provider)
FINANCIAL_MODELING_PREP__API_KEY=your_fmp_key

# Tiingo API (fallback provider)
TIINGO__API_KEY=your_tiingo_key
```

2. The backend will automatically load this file on startup.

### Method 3: Custom .env File

Use a different .env file by setting the `ENV` environment variable:

```bash
export ENV=/path/to/custom/.env
cd backend/src
python -m uvicorn main:app --reload
```

## Verifying Configuration

### Check if API Keys are Loaded

1. Start the backend:
```bash
cd backend/src
python -m uvicorn main:app --reload
```

2. Check the logs for configuration messages (look for "API key" or "authentication" messages)

3. Try the nightly update endpoint:
```bash
curl -X POST http://localhost:8002/nightly-update/start \
  -H "Content-Type: application/json" \
  -d '{"symbols": ["AAPL"]}'
```

### Troubleshooting

**Error: "Invalid API key" or "Authentication failed"**
- Verify your API key is correct
- Check that the environment variable name matches exactly (case-sensitive)
- Ensure the .env file is in the correct location

**Error: "Rate limit exceeded"**
- You've hit the rate limit for your API tier
- Wait before making more requests
- Consider upgrading to a paid plan

**Error: "No data returned"**
- The API key might be valid but the symbol/date range is invalid
- Check that the symbol exists and has data for the requested date range

## Environment Variable Format

The backend uses Pydantic's nested delimiter format with double underscores (`__`):

```
SECTION__SUBSECTION__KEY=value
```

Examples:
- `POLYGON__API_KEY=key` → `settings.polygon.api_key`
- `FINANCIAL_MODELING_PREP__API_KEY=key` → `settings.financial_modeling_prep.api_key`
- `NIGHTLY_UPDATE__MAX_CONCURRENT_SYMBOLS=5` → `settings.nightly_update.max_concurrent_symbols`

## Provider Selection

The backend uses a provider priority system:

1. **Default Provider**: Polygon (set in `TRADING_DATA_PROVIDER__DEFAULT_PROVIDER`)
2. **Fallback Providers**: Financial Modeling Prep, Tiingo (if enabled)

To enable fallback:
```bash
TRADING_DATA_PROVIDER__ENABLE_FALLBACK=true
```

## Testing API Keys

Run the paid API tests to verify your configuration:

```bash
cd backend/src/tests/run_scripts
bash run_paid_api_tests.sh
```

This script will:
1. Check if API keys are configured
2. Test data fetching from each provider
3. Verify data storage
4. Validate resampling

## Security Notes

- **Never commit .env files** to version control
- **Use environment variables** in production instead of .env files
- **Rotate API keys** regularly
- **Use separate keys** for development and production
- **Monitor API usage** to detect unauthorized access

## Getting API Keys

### Polygon.io
1. Go to https://polygon.io/
2. Sign up for a free account
3. Navigate to your dashboard
4. Copy your API key

### Financial Modeling Prep
1. Go to https://financialmodelingprep.com/
2. Sign up for a free account
3. Navigate to your dashboard
4. Copy your API key

### Tiingo
1. Go to https://www.tiingo.com/
2. Sign up for a free account
3. Navigate to your account settings
4. Copy your API key

## Support

For issues with API keys or data downloading:
1. Check the logs in `backend/src/logs/`
2. Verify your API key is correct
3. Check the provider's status page
4. Review the provider's documentation

