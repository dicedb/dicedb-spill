#!/bin/bash

# Comprehensive Test Runner for DiceDB Infcache Module
# Runs unit tests, integration tests, and edge case tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
UNIT_TEST_BINARY="test_unit"
INTEGRATION_TEST_SCRIPT="test_integration.py"
EDGE_CASE_TEST_SCRIPT="test_edge_cases.py"
MODULE_BINARY="dicedb-infcache.so"

echo "================================================="
echo "  DiceDB Infcache Comprehensive Test Suite"
echo "  Testing ABSTTL functionality and edge cases"
echo "================================================="

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "INFO")
            echo -e "${BLUE}[INFO]${NC} $message"
            ;;
        "SUCCESS")
            echo -e "${GREEN}[SUCCESS]${NC} $message"
            ;;
        "ERROR")
            echo -e "${RED}[ERROR]${NC} $message"
            ;;
        "WARNING")
            echo -e "${YELLOW}[WARNING]${NC} $message"
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

    # Check for GCC for unit tests
    if ! command -v gcc &> /dev/null; then
        print_status "ERROR" "gcc not found. Required for unit tests."
        exit 1
    fi

    print_status "SUCCESS" "All prerequisites satisfied"
}

# Build and run unit tests
run_unit_tests() {
    print_status "INFO" "=== RUNNING UNIT TESTS ==="
    print_status "INFO" "Building and running unit tests for ABSTTL functionality..."

    # Compile unit tests
    gcc -std=c99 -o "$UNIT_TEST_BINARY" test_unit.c -DNDEBUG || {
        print_status "ERROR" "Failed to compile unit tests"
        return 1
    }

    # Run unit tests
    if ./"$UNIT_TEST_BINARY"; then
        print_status "SUCCESS" "Unit tests passed (19 tests including ABSTTL tests)"
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
    print_status "INFO" "=== RUNNING INTEGRATION TESTS ==="
    print_status "INFO" "Running integration tests with ABSTTL functionality..."
    print_status "WARNING" "Note: Integration tests require DiceDB server running on port 6379 with infcache module"

    if ./run_integration_tests.sh; then
        print_status "SUCCESS" "Integration tests passed (16 tests including ABSTTL tests)"
        return 0
    else
        print_status "ERROR" "Integration tests failed"
        print_status "INFO" "Make sure DiceDB server is running: dicedb-server --port 6379 --loadmodule ./dicedb-infcache.so"
        return 1
    fi
}

# Run edge case tests
run_edge_case_tests() {
    print_status "INFO" "=== RUNNING EDGE CASE TESTS ==="
    print_status "INFO" "Running edge case and error condition tests..."

    # Create virtual environment if it doesn't exist
    if [ ! -d "venv" ]; then
        print_status "INFO" "Creating virtual environment for edge case tests..."
        python3 -m venv venv
    fi

    # Activate virtual environment and install dependencies
    source venv/bin/activate

    # Install redis/valkey client
    pip install redis valkey 2>/dev/null || pip install redis 2>/dev/null || {
        print_status "WARNING" "Could not install redis client, using system version"
    }

    # Run edge case tests
    if python3 "$EDGE_CASE_TEST_SCRIPT"; then
        print_status "SUCCESS" "Edge case tests passed"
        deactivate
        return 0
    else
        print_status "ERROR" "Edge case tests failed"
        deactivate
        return 1
    fi
}

# Run performance tests (basic)
run_performance_tests() {
    print_status "INFO" "=== RUNNING BASIC PERFORMANCE TESTS ==="
    print_status "INFO" "Running basic performance tests for ABSTTL operations..."

    # Create a simple performance test
    python3 << 'EOF'
import redis
import time
import sys

try:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    r.ping()

    print("  Testing ABSTTL restoration performance...")

    # Set up test keys
    start_time = time.time()
    for i in range(1000):
        r.setex(f'perf_key_{i}', 3600, f'perf_value_{i}')
    setup_time = time.time() - start_time

    print(f"  Set 1000 keys with TTL in {setup_time:.2f} seconds")

    # Trigger evictions
    start_time = time.time()
    for i in range(2000):
        r.set(f'filler_perf_{i}', 'x' * 5000)
    eviction_time = time.time() - start_time

    print(f"  Triggered evictions in {eviction_time:.2f} seconds")

    # Count evicted keys
    evicted = 0
    for i in range(1000):
        if r.get(f'perf_key_{i}') is None:
            evicted += 1

    print(f"  {evicted} keys were evicted")

    if evicted > 0:
        # Test restoration performance
        start_time = time.time()
        restored = 0
        for i in range(1000):
            if r.get(f'perf_key_{i}') is None:
                try:
                    result = r.execute_command('infcache.restore', f'perf_key_{i}')
                    if result == 'OK':
                        restored += 1
                except:
                    pass
        restore_time = time.time() - start_time

        print(f"  Restored {restored} keys in {restore_time:.2f} seconds")
        if restored > 0:
            print(f"  Average restoration time: {(restore_time/restored)*1000:.2f} ms per key")

    print("  Basic performance test completed")
    sys.exit(0)

except Exception as e:
    print(f"  Performance test failed: {e}")
    sys.exit(1)
EOF

    if [ $? -eq 0 ]; then
        print_status "SUCCESS" "Basic performance tests passed"
        return 0
    else
        print_status "ERROR" "Basic performance tests failed"
        return 1
    fi
}

# Parse command line arguments
RUN_UNIT=1
RUN_INTEGRATION=1
RUN_EDGE_CASES=1
RUN_PERFORMANCE=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --unit-only)
            RUN_INTEGRATION=0
            RUN_EDGE_CASES=0
            RUN_PERFORMANCE=0
            shift
            ;;
        --integration-only)
            RUN_UNIT=0
            RUN_EDGE_CASES=0
            RUN_PERFORMANCE=0
            shift
            ;;
        --edge-cases-only)
            RUN_UNIT=0
            RUN_INTEGRATION=0
            RUN_PERFORMANCE=0
            shift
            ;;
        --with-performance)
            RUN_PERFORMANCE=1
            shift
            ;;
        --all)
            RUN_UNIT=1
            RUN_INTEGRATION=1
            RUN_EDGE_CASES=1
            RUN_PERFORMANCE=1
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --unit-only          Run only unit tests"
            echo "  --integration-only   Run only integration tests"
            echo "  --edge-cases-only    Run only edge case tests"
            echo "  --with-performance   Include performance tests"
            echo "  --all                Run all tests including performance"
            echo "  --help, -h           Show this help message"
            echo ""
            echo "By default, unit, integration, and edge case tests are run."
            echo "Performance tests are optional due to their longer runtime."
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
    local tests_run=0
    local tests_passed=0

    check_prerequisites

    echo ""
    print_status "INFO" "Starting comprehensive test execution..."
    echo ""

    # Run unit tests
    if [ $RUN_UNIT -eq 1 ]; then
        tests_run=$((tests_run + 1))
        if run_unit_tests; then
            tests_passed=$((tests_passed + 1))
        else
            exit_code=1
        fi
        echo ""
    fi

    # Run integration tests
    if [ $RUN_INTEGRATION -eq 1 ]; then
        tests_run=$((tests_run + 1))
        if run_integration_tests; then
            tests_passed=$((tests_passed + 1))
        else
            exit_code=1
        fi
        echo ""
    fi

    # Run edge case tests
    if [ $RUN_EDGE_CASES -eq 1 ]; then
        tests_run=$((tests_run + 1))
        if run_edge_case_tests; then
            tests_passed=$((tests_passed + 1))
        else
            exit_code=1
        fi
        echo ""
    fi

    # Run performance tests
    if [ $RUN_PERFORMANCE -eq 1 ]; then
        tests_run=$((tests_run + 1))
        if run_performance_tests; then
            tests_passed=$((tests_passed + 1))
        else
            exit_code=1
        fi
        echo ""
    fi

    # Summary
    echo "================================================="
    print_status "INFO" "COMPREHENSIVE TEST SUMMARY"
    echo "================================================="
    print_status "INFO" "Test suites run: $tests_run"
    print_status "INFO" "Test suites passed: $tests_passed"

    if [ $exit_code -eq 0 ]; then
        print_status "SUCCESS" "ALL TEST SUITES PASSED!"
        print_status "SUCCESS" "ABSTTL functionality is working correctly"
    else
        print_status "ERROR" "SOME TEST SUITES FAILED!"
        print_status "ERROR" "Please review the test output above"
    fi
    echo "================================================="

    exit $exit_code
}

# Run main function
main "$@"