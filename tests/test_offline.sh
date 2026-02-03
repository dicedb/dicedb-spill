#!/bin/bash

# Quick offline test to check if tests would run successfully
# This just validates the test structure and dependencies without running actual tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    local status=$1
    local message=$2
    case $status in
        "INFO")
            echo -e "${YELLOW}[INFO]${NC} $message"
            ;;
        "SUCCESS")
            echo -e "${GREEN}[SUCCESS]${NC} $message"
            ;;
        "ERROR")
            echo -e "${RED}[ERROR]${NC} $message"
            ;;
    esac
}

echo "=== Offline Test Validation ==="
echo ""

# Check if module can be built
print_status "INFO" "Checking if module can be built..."
if make clean && make > /dev/null 2>&1; then
    print_status "SUCCESS" "Module built successfully"
else
    print_status "ERROR" "Module build failed"
    exit 1
fi

# Check unit tests
print_status "INFO" "Validating unit tests..."
if make test-unit > /dev/null 2>&1; then
    print_status "SUCCESS" "Unit tests pass"
else
    print_status "ERROR" "Unit tests failed"
    exit 1
fi

# Check if integration test dependencies are available
print_status "INFO" "Checking integration test dependencies..."

# Create temporary venv to test dependencies
TEMP_VENV=".temp_test_venv"
python3 -m venv "$TEMP_VENV" > /dev/null 2>&1
source "$TEMP_VENV/bin/activate"
pip install --upgrade pip > /dev/null 2>&1

if pip install valkey > /dev/null 2>&1; then
    print_status "SUCCESS" "Valkey client can be installed"
elif pip install redis > /dev/null 2>&1; then
    print_status "SUCCESS" "Redis client can be installed as fallback"
else
    print_status "ERROR" "Neither valkey nor redis client can be installed"
    rm -rf "$TEMP_VENV"
    exit 1
fi

# Test integration script syntax
if python3 -m py_compile test_integration.py; then
    print_status "SUCCESS" "Integration test script syntax is valid"
else
    print_status "ERROR" "Integration test script has syntax errors"
    rm -rf "$TEMP_VENV"
    exit 1
fi

# Cleanup
rm -rf "$TEMP_VENV"

echo ""
print_status "SUCCESS" "All offline tests passed!"
echo ""
print_status "INFO" "To run full integration tests:"
echo "  1. Start DiceDB: dicedb-server --port 6379 --loadmodule ./lib-infcache.so"
echo "  2. Run tests: make test-integration"