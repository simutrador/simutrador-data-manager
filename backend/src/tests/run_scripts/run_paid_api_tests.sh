#!/bin/bash

# Script to run PAID API tests for trading data API
# ⚠️  WARNING: These tests use real data providers and WILL INCUR CHARGES! ⚠️

echo "💰 Running PAID API tests for trading data API..."
echo "⚠️  WARNING: These tests will make real API calls and WILL INCUR CHARGES"
echo "⚠️  Only run these tests when you need to verify real API integration"
echo "📁 Test data will be stored in backend/test_storage (separate from production data)"
echo ""

# Load environment variables from .env file if it exists
ENV_FILE="../../environments/.env.test.paid"
if [ -f "$ENV_FILE" ]; then
    echo "📁 Loading environment variables from $ENV_FILE"
    # Export variables from .env file (handle both formats)
    export $(grep -v '^#' "$ENV_FILE" | xargs)
    echo "✅ Environment variables loaded"
else
    echo "⚠️  No .env file found at $ENV_FILE"
fi
echo ""

# Check if API keys are configured (using the same format as .env files)
if [ -z "$POLYGON__API_KEY" ] && [ -z "$FINANCIAL_MODELING_PREP__API_KEY" ]; then
    echo "❌ Error: No API keys found in environment variables"
    echo "   Please set POLYGON__API_KEY or FINANCIAL_MODELING_PREP__API_KEY"
    echo ""
    echo "   Example:"
    echo "   export POLYGON__API_KEY='your_polygon_key'"
    echo "   export FINANCIAL_MODELING_PREP__API_KEY='your_fmp_key'"
    echo ""
    echo "   Or make sure your .env file contains:"
    echo "   POLYGON__API_KEY=your_polygon_key"
    echo "   FINANCIAL_MODELING_PREP__API_KEY=your_fmp_key"
    echo ""
    exit 1
fi

echo "✅ API keys found, proceeding with PAID API tests..."
echo "💸 This will cost money - make sure you want to continue!"
echo ""

# Ask for confirmation
read -p "Are you sure you want to run tests that will incur API charges? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cancelled - no charges incurred"
    exit 0
fi

echo "🚀 Running paid API tests..."
echo ""

# Clean up any existing test storage (optional)
if [ -d "../../../test_storage" ]; then
    echo "🧹 Cleaning up existing test storage..."
    rm -rf ../../../test_storage
fi

# Change to backend root directory to run tests
cd ../../../
python -m pytest -m paid_api src/tests/paid_api/ -v -s

echo ""
echo "📁 Test data stored in: test_storage/"
echo "🧹 You can clean up test data with: rm -rf test_storage/"

echo ""
echo "🏁 Paid API tests completed!"
echo "💰 Check your API provider billing for charges incurred"
