#!/bin/bash

# Script to run PAID API tests for nightly update API
# ⚠️  WARNING: These tests use real data providers and WILL INCUR CHARGES! ⚠️

echo "💰 Running PAID API tests for nightly update API..."
echo "⚠️  WARNING: These tests will make real API calls and WILL INCUR CHARGES"
echo "⚠️  Only run these tests when you need to verify real nightly update integration"
echo "📁 Test data will be stored in backend/test_storage (separate from production data)"
echo "⏰ Note: Nightly update tests may take 10+ minutes to complete"
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
echo "📋 Available test classes:"
echo "   1. TestNightlyUpdatePaidAPI - Core functionality & data scenarios (moderate cost)"
echo "   2. TestNightlyUpdateCompleteEndToEndPipeline - Complete pipeline tests (moderate cost)"
echo "   3. All tests - Run everything (higher cost)"
echo ""

# Ask for test selection
echo "Select which tests to run:"
echo "1) Core functionality tests (recommended - includes data scenarios & resampling validation)"
echo "2) Complete pipeline tests only"
echo "3) All nightly update tests"
echo "4) Cancel"
echo ""
read -p "Enter your choice (1-4): " -n 1 -r
echo

case $REPLY in
    1)
        TEST_CLASS="TestNightlyUpdatePaidAPI"
        echo "🎯 Selected: Core functionality tests (data scenarios & resampling validation)"
        ;;
    2)
        TEST_CLASS="TestNightlyUpdateCompleteEndToEndPipeline"
        echo "🎯 Selected: Complete pipeline tests"
        ;;
    3)
        TEST_CLASS=""
        echo "🎯 Selected: All nightly update tests"
        ;;
    4)
        echo "❌ Cancelled - no charges incurred"
        exit 0
        ;;
    *)
        echo "❌ Invalid selection - cancelled"
        exit 1
        ;;
esac

echo ""
echo "💰 FINAL WARNING: This will incur API charges!"
echo "📊 Estimated costs (depends on your API plan):"
echo "   - Core tests: ~$1-3 (3 symbols + data scenarios + resampling validation)"
echo "   - Pipeline tests: ~$2-5 (2 symbols with full pipeline validation)"
echo "   - All tests: ~$3-8 (comprehensive testing with improved efficiency)"
echo ""

# Ask for final confirmation
read -p "Are you sure you want to run tests that will incur API charges? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cancelled - no charges incurred"
    exit 0
fi

echo "🚀 Running nightly update paid API tests..."
echo ""

# Clean up any existing test storage (optional)
if [ -d "../../../test_storage" ]; then
    echo "🧹 Cleaning up existing test storage..."
    rm -rf ../../../test_storage
fi

# Change to backend root directory to run tests
cd ../../../

if [ -z "$TEST_CLASS" ]; then
    # Run all nightly update tests
    echo "🔄 Running ALL nightly update paid API tests..."
    python -m pytest -m paid_api src/tests/paid_api/test_nightly_update_paid_api.py -v -s
else
    # Run specific test class
    echo "🔄 Running $TEST_CLASS tests..."
    python -m pytest -m paid_api src/tests/paid_api/test_nightly_update_paid_api.py::$TEST_CLASS -v -s
fi

TEST_EXIT_CODE=$?

echo ""
echo "📁 Test data stored in: test_storage/"
echo "🧹 You can clean up test data with: rm -rf test_storage/"

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "🎉 Nightly update paid API tests completed successfully!"
else
    echo "❌ Some tests failed - check output above for details"
fi
echo "💰 Check your API provider billing for charges incurred"
echo ""
echo "📋 Next steps:"
echo "   - Review test results above"
echo "   - Check test_storage/ for generated data"
echo "   - Monitor your API usage/billing"
echo "   - Clean up test data when done: rm -rf test_storage/"
