"""Benchmark out-of-core streaming vs naive full-load on the same queries.

We run an identical category-level aggregation two ways — load-everything pandas
vs the chunked streaming engine — and measure peak Python memory (tracemalloc)
and wall time for each. Then we run a broadcast join the streaming way. The
results match to the cent; the memory profile does not.
"""
import json
import time
import tracemalloc
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from engine import LazyPipeline, broadcast_join_agg

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
FACT = DATA / "transactions.csv"
