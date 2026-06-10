"""Generate a large fact table that we deliberately process WITHOUT loading whole.

Writes a transactions CSV row-group by row-group so even *creating* it never
holds all rows in memory — the same discipline the processing engine uses. Set
ROWS via the environment variable BIGDATA_ROWS to scale it up.
"""
import csv
import os
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
ROWS = int(os.environ.get("BIGDATA_ROWS", 1_500_000))
CATEGORIES = ["grocery", "electronics", "fuel", "dining", "travel", "health"]
CHUNK = 100_000


