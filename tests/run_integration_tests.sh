#!/bin/bash
cd "$(dirname "$0")"

! command -v python3 &> /dev/null && { echo "Error: Python 3 required"; exit 1; }

# Check if we're already in a virtual environment
if [ -z "$VIRTUAL_ENV" ]; then
    [ ! -d "venv" ] && python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt >/dev/null 2>&1
    SHOULD_DEACTIVATE=1
else
    SHOULD_DEACTIVATE=0
fi

! python3 -c "import redis" 2>/dev/null && { echo "Error: redis-py required"; [ $SHOULD_DEACTIVATE -eq 1 ] && deactivate; exit 1; }

if ! python3 -c "import redis; redis.Redis(host='localhost', port=6379).ping()" 2>/dev/null; then
    echo "Error: Cannot connect to DiceDB on localhost:6379"
    [ $SHOULD_DEACTIVATE -eq 1 ] && deactivate
    exit 1
fi

if ! python3 -c "import redis; r = redis.Redis(host='localhost', port=6379); r.execute_command('spill.stats')" 2>/dev/null; then
    echo "Error: Spill module not loaded"
    [ $SHOULD_DEACTIVATE -eq 1 ] && deactivate
    exit 1
fi

python3 integration_test.py
TEST_EXIT_CODE=$?

[ $SHOULD_DEACTIVATE -eq 1 ] && deactivate
exit $TEST_EXIT_CODE