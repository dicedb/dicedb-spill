#!/bin/bash
cd "$(dirname "$0")"

! command -v python3 &> /dev/null && { echo "Error: Python 3 required"; exit 1; }

[ ! -d "venv" ] && python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt >/dev/null 2>&1

! python3 -c "import redis" 2>/dev/null && { echo "Error: redis-py required"; deactivate; exit 1; }

if ! python3 -c "import redis; redis.Redis(host='localhost', port=8379).ping()" 2>/dev/null; then
    echo "Error: Cannot connect to DiceDB on localhost:8379"
    deactivate
    exit 1
fi

if ! python3 -c "import redis; r = redis.Redis(host='localhost', port=8379); r.execute_command('infcache.stats')" 2>/dev/null; then
    echo "Error: Infcache module not loaded"
    deactivate
    exit 1
fi

python3 integration_test.py
TEST_EXIT_CODE=$?

deactivate
exit $TEST_EXIT_CODE