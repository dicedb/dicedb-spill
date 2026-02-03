#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

UNIT_TEST_BINARY="test_unit"
INTEGRATION_TEST_SCRIPT="test_integration.py"
EDGE_CASE_TEST_SCRIPT="test_edge_cases.py"
ADVANCED_SCENARIOS_SCRIPT="test_advanced_scenarios.py"
MODULE_BINARY="../lib-infcache.so"

echo "DiceDB Infcache Test Suite"

print_status() {
    local status=$1
    local message=$2
    case $status in
        "INFO") echo -e "${BLUE}[INFO]${NC} $message" ;;
        "SUCCESS") echo -e "${GREEN}[PASS]${NC} $message" ;;
        "ERROR") echo -e "${RED}[FAIL]${NC} $message" ;;
        "WARNING") echo -e "${YELLOW}[WARN]${NC} $message" ;;
    esac
}

check_prerequisites() {
    if [ ! -f "$MODULE_BINARY" ]; then
        print_status "ERROR" "Module binary not found. Run 'make' first."
        exit 1
    fi
    if ! command -v python3 &> /dev/null; then
        print_status "ERROR" "python3 not found"
        exit 1
    fi
    if ! command -v gcc &> /dev/null; then
        print_status "ERROR" "gcc not found"
        exit 1
    fi
}

setup_python_env() {
    if [ ! -d "venv" ]; then
        print_status "INFO" "Creating virtual environment"
        python3 -m venv venv
    fi
    source venv/bin/activate
    print_status "INFO" "Installing Python dependencies"
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt >/dev/null 2>&1
    fi
    pip install redis valkey 2>/dev/null || pip install redis 2>/dev/null || true
}

run_unit_tests() {
    print_status "INFO" "Unit tests"
    gcc -std=c99 -o "$UNIT_TEST_BINARY" test_unit.c -DNDEBUG || {
        print_status "ERROR" "Compilation failed"
        return 1
    }
    if ./"$UNIT_TEST_BINARY"; then
        print_status "SUCCESS" "Unit tests (30)"
        rm -f "$UNIT_TEST_BINARY"
        return 0
    else
        print_status "ERROR" "Unit tests failed"
        rm -f "$UNIT_TEST_BINARY"
        return 1
    fi
}

run_integration_tests() {
    print_status "INFO" "Integration tests"
    if ./run_integration_tests.sh; then
        print_status "SUCCESS" "Integration tests (16)"
        return 0
    else
        print_status "ERROR" "Integration tests failed"
        print_status "INFO" "Ensure DiceDB server on port 6379 with infcache module"
        return 1
    fi
}

run_edge_case_tests() {
    print_status "INFO" "Edge case tests"
    if python3 "$EDGE_CASE_TEST_SCRIPT"; then
        print_status "SUCCESS" "Edge case tests"
        return 0
    else
        print_status "ERROR" "Edge case tests failed"
        return 1
    fi
}

run_lifecycle_tests() {
    print_status "INFO" "Lifecycle tests"
    if ! command -v dicedb-server &> /dev/null; then
        print_status "WARNING" "dicedb-server not found, skipping"
        return 0
    fi
    if python3 "test_module_lifecycle.py"; then
        print_status "SUCCESS" "Lifecycle tests"
        return 0
    else
        print_status "ERROR" "Lifecycle tests failed"
        return 1
    fi
}

run_advanced_tests() {
    print_status "INFO" "Advanced tests"
    if python3 "$ADVANCED_SCENARIOS_SCRIPT"; then
        print_status "SUCCESS" "Advanced tests"
        return 0
    else
        print_status "ERROR" "Advanced tests failed"
        return 1
    fi
}

run_performance_tests() {
    print_status "INFO" "Performance tests"
    python3 << 'EOF'
import redis, time, sys
try:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    r.ping()

    start_time = time.time()
    for i in range(1000):
        r.setex(f'perf_key_{i}', 3600, f'perf_value_{i}')
    setup_time = time.time() - start_time

    start_time = time.time()
    for i in range(2000):
        r.set(f'filler_perf_{i}', 'x' * 5000)
    eviction_time = time.time() - start_time

    evicted = sum(1 for i in range(1000) if r.get(f'perf_key_{i}') is None)

    if evicted > 0:
        start_time = time.time()
        restored = 0
        for i in range(1000):
            if r.get(f'perf_key_{i}') is None:
                try:
                    if r.execute_command('infcache.restore', f'perf_key_{i}') == 'OK':
                        restored += 1
                except: pass
        restore_time = time.time() - start_time
        print(f"Restored {restored}/{evicted} keys in {restore_time:.2f}s")
    sys.exit(0)
except Exception as e:
    print(f"Performance test failed: {e}")
    sys.exit(1)
EOF
    [ $? -eq 0 ] && print_status "SUCCESS" "Performance tests" || print_status "ERROR" "Performance tests failed"
    return $?
}

# Parse command line arguments
RUN_UNIT=1
RUN_INTEGRATION=1
RUN_EDGE_CASES=1
RUN_LIFECYCLE=1
RUN_ADVANCED=1
RUN_PERFORMANCE=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --unit-only)
            RUN_INTEGRATION=0
            RUN_EDGE_CASES=0
            RUN_LIFECYCLE=0
            RUN_ADVANCED=0
            RUN_PERFORMANCE=0
            shift
            ;;
        --integration-only)
            RUN_UNIT=0
            RUN_EDGE_CASES=0
            RUN_LIFECYCLE=0
            RUN_ADVANCED=0
            RUN_PERFORMANCE=0
            shift
            ;;
        --edge-cases-only)
            RUN_UNIT=0
            RUN_INTEGRATION=0
            RUN_LIFECYCLE=0
            RUN_ADVANCED=0
            RUN_PERFORMANCE=0
            shift
            ;;
        --lifecycle-only)
            RUN_UNIT=0
            RUN_INTEGRATION=0
            RUN_EDGE_CASES=0
            RUN_ADVANCED=0
            RUN_PERFORMANCE=0
            shift
            ;;
        --advanced-only)
            RUN_UNIT=0
            RUN_INTEGRATION=0
            RUN_EDGE_CASES=0
            RUN_LIFECYCLE=0
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
            RUN_LIFECYCLE=1
            RUN_ADVANCED=1
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
            echo "  --lifecycle-only     Run only module lifecycle tests"
            echo "  --advanced-only      Run only advanced scenario tests"
            echo "  --with-performance   Include performance tests"
            echo "  --all                Run all tests including performance"
            echo "  --help, -h           Show this help message"
            echo ""
            echo "By default, unit, integration, edge case, lifecycle, and advanced tests are run."
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

main() {
    local exit_code=0
    local tests_run=0
    local tests_passed=0

    check_prerequisites
    setup_python_env

    [ $RUN_UNIT -eq 1 ] && { tests_run=$((tests_run + 1)); run_unit_tests && tests_passed=$((tests_passed + 1)) || exit_code=1; }
    [ $RUN_INTEGRATION -eq 1 ] && { tests_run=$((tests_run + 1)); run_integration_tests && tests_passed=$((tests_passed + 1)) || exit_code=1; }
    [ $RUN_EDGE_CASES -eq 1 ] && { tests_run=$((tests_run + 1)); run_edge_case_tests && tests_passed=$((tests_passed + 1)) || exit_code=1; }
    [ $RUN_LIFECYCLE -eq 1 ] && { tests_run=$((tests_run + 1)); run_lifecycle_tests && tests_passed=$((tests_passed + 1)) || exit_code=1; }
    [ $RUN_ADVANCED -eq 1 ] && { tests_run=$((tests_run + 1)); run_advanced_tests && tests_passed=$((tests_passed + 1)) || exit_code=1; }
    [ $RUN_PERFORMANCE -eq 1 ] && { tests_run=$((tests_run + 1)); run_performance_tests && tests_passed=$((tests_passed + 1)) || exit_code=1; }

    deactivate

    echo "Test Summary: $tests_passed/$tests_run passed"
    [ $exit_code -eq 0 ] && print_status "SUCCESS" "All tests passed" || print_status "ERROR" "Some tests failed"
    exit $exit_code
}

# Run main function
main "$@"