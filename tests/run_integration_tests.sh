#!/bin/bash

# Integration test runner with virtual environment
# Sets up venv, installs dependencies, runs tests, and cleans up

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR=".test_venv"
PYTHON_SCRIPT="test_integration.py"

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

# Cleanup function
cleanup() {
    if [ -d "$VENV_DIR" ]; then
        print_status "INFO" "Cleaning up virtual environment..."
        rm -rf "$VENV_DIR"
    fi
}

# Set up cleanup trap
trap cleanup EXIT

print_status "INFO" "Setting up virtual environment for integration tests..."

# Check if python3 is available
if ! command -v python3 &> /dev/null; then
    print_status "ERROR" "python3 not found. Please install Python 3."
    exit 1
fi

# Create virtual environment
python3 -m venv "$VENV_DIR"

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Upgrade pip
pip install --upgrade pip > /dev/null 2>&1

# Install dependencies
print_status "INFO" "Installing Python dependencies..."
pip install valkey > /dev/null 2>&1 || {
    print_status "INFO" "Valkey not available, installing redis as fallback..."
    pip install redis > /dev/null 2>&1
}

# Run the integration tests
print_status "INFO" "Running integration tests with virtual environment..."
if python3 "$PYTHON_SCRIPT"; then
    print_status "SUCCESS" "Integration tests completed successfully"
    exit_code=0
else
    print_status "ERROR" "Integration tests failed"
    exit_code=1
fi

# Cleanup is handled by trap
exit $exit_code