#!/bin/bash

# DiceDB Infcache Module Test Runner
# Runs both unit and integration tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
UNIT_TEST_BINARY="test_unit"
INTEGRATION_TEST_SCRIPT="test_integration.py"
MODULE_BINARY="dicedb-infcache.so"

echo "========================================="
echo "  DiceDB Infcache Module Test Suite"
echo "========================================="

# Function to print colored output
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

# Check prerequisites
check_prerequisites() {
    print_status "INFO" "Checking prerequisites..."
    
    # Check if module exists
    if [ ! -f "$MODULE_BINARY" ]; then
        print_status "ERROR" "Module binary '$MODULE_BINARY' not found. Please run 'make' first."
        exit 1
    fi
    
    # Check for Python3 for integration tests
    if ! command -v python3 &> /dev/null; then
        print_status "ERROR" "python3 not found. Required for integration tests."
        exit 1
    fi
    
    # Note: Integration tests assume a DiceDB/Valkey/Redis server is already running on port 6379
    # with the infcache module loaded
    
    print_status "SUCCESS" "All prerequisites satisfied"
}

# Build and run unit tests
run_unit_tests() {
    print_status "INFO" "Building and running unit tests..."
    
    # Compile unit tests
    gcc -std=c99 -o "$UNIT_TEST_BINARY" test_unit.c -DNDEBUG || {
        print_status "ERROR" "Failed to compile unit tests"
        return 1
    }
    
    # Run unit tests
    if ./"$UNIT_TEST_BINARY"; then
        print_status "SUCCESS" "Unit tests passed"
        rm -f "$UNIT_TEST_BINARY"
        return 0
    else
        print_status "ERROR" "Unit tests failed"
        rm -f "$UNIT_TEST_BINARY"
        return 1
    fi
}

# Run integration tests
run_integration_tests() {
    print_status "INFO" "Running integration tests with virtual environment..."
    print_status "INFO" "Note: Integration tests require DiceDB server running on port 6379 with infcache module"
    
    if ./run_integration_tests.sh; then
        print_status "SUCCESS" "Integration tests passed"
        return 0
    else
        print_status "ERROR" "Integration tests failed"
        print_status "INFO" "Make sure DiceDB server is running: dicedb-server --port 6379 --loadmodule ./dicedb-infcache.so"
        return 1
    fi
}

# Parse command line arguments
RUN_UNIT=1
RUN_INTEGRATION=1

while [[ $# -gt 0 ]]; do
    case $1 in
        --unit-only)
            RUN_INTEGRATION=0
            shift
            ;;
        --integration-only)
            RUN_UNIT=0
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --unit-only        Run only unit tests"
            echo "  --integration-only Run only integration tests"
            echo "  --help, -h         Show this help message"
            echo ""
            echo "By default, both unit and integration tests are run."
            exit 0
            ;;
        *)
            print_status "ERROR" "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Main execution
main() {
    local exit_code=0
    
    check_prerequisites
    
    echo ""
    echo "Starting test execution..."
    echo ""
    
    # Run unit tests
    if [ $RUN_UNIT -eq 1 ]; then
        if ! run_unit_tests; then
            exit_code=1
        fi
        echo ""
    fi
    
    # Run integration tests
    if [ $RUN_INTEGRATION -eq 1 ]; then
        if ! run_integration_tests; then
            exit_code=1
        fi
        echo ""
    fi
    
    # Summary
    echo "========================================="
    if [ $exit_code -eq 0 ]; then
        print_status "SUCCESS" "All tests passed!"
    else
        print_status "ERROR" "Some tests failed!"
    fi
    echo "========================================="
    
    exit $exit_code
}

# Run main function
main "$@"