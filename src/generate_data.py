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


def main():
    DATA.mkdir(exist_ok=True)
    rng = np.random.default_rng(0)
    fact = DATA / "transactions.csv"
    with fact.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["txn_id", "merchant_id", "category", "amount", "is_fraud"])
        written = 0
        while written < ROWS:
            n = min(CHUNK, ROWS - written)
            mids = rng.integers(0, 5000, n)
            cats = rng.integers(0, len(CATEGORIES), n)
            amt = np.round(rng.gamma(2.0, 25.0, n), 2)
            fraud = (rng.random(n) < 0.012).astype(int)
            for i in range(n):
                w.writerow([written + i, int(mids[i]), CATEGORIES[cats[i]],
                            amt[i], int(fraud[i])])
            written += n
