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
CHUNK = 200_000


def measure(fn):
    tracemalloc.start()
    t0 = time.perf_counter()
    out = fn()
    dt = time.perf_counter() - t0
    peak = tracemalloc.get_traced_memory()[1] / 1e6
    tracemalloc.stop()
    return out, dt, peak


def naive_full_load():
    df = pd.read_csv(FACT)
    g = df.groupby("category")["amount"].agg(["sum", "count"])
    g["mean"] = g["sum"] / g["count"]
    return g.reset_index().sort_values("category").reset_index(drop=True)


def streaming():
    return (LazyPipeline(FACT, chunksize=CHUNK, usecols=["category", "amount"])
            .groupby_agg("category", "amount"))


def main():
    REPORTS.mkdir(exist_ok=True)
    full, t_full, m_full = measure(naive_full_load)
    strm, t_strm, m_strm = measure(streaming)

    # correctness: totals must match
    match = bool(abs(full["sum"].sum() - strm["sum"].sum()) < 1e-3)

    # a join the streaming way (per-region fraud-amount aggregate)
    join_out, t_join, m_join = measure(
        lambda: broadcast_join_agg(FACT, DATA / "merchants.csv",
                                   "merchant_id", "region", "amount", CHUNK))

    metrics = {
        "rows": int(full["count"].sum()),
        "full_load": {"seconds": round(t_full, 2), "peak_mb": round(m_full, 1)},
        "streaming": {"seconds": round(t_strm, 2), "peak_mb": round(m_strm, 1)},
        "broadcast_join": {"seconds": round(t_join, 2), "peak_mb": round(m_join, 1)},
        "results_match": match,
