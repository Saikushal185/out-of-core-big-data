#!/usr/bin/env bash
# Generate a large fact table, then process it out-of-core and benchmark memory.
set -e
cd "$(dirname "$0")"

pip install -r requirements.txt
python3 src/generate_data.py
python3 src/process.py
echo ""
echo "Scale it up:  BIGDATA_ROWS=10000000 python3 src/generate_data.py"
echo "See reports/memory.png and reports/metrics.json"
