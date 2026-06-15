# 🗄️ Out-of-Core / Big-Data Processing

Processing a dataset that *doesn't fit the way you'd like it to* — the skill
behind Spark and Dask, implemented from first principles in plain pandas. The
engine streams a large fact table in bounded chunks and combines partial
results, so **peak memory stays flat** no matter how big the file gets.

## What this project demonstrates
- **Streaming / chunked execution** — read the file in row-groups, never the
  whole thing; the same discipline is used even to *write* the data.
- **Map-reduce aggregation** — per-chunk partial group aggregates merged into a
  global result; totals match a full-load pandas groupby to the cent.
- **Lazy pipelines** — `filter`/`assign` steps are recorded and only executed
  during the streaming pass (deferred execution, like a query plan).
- **Broadcast join** — keep the small dimension table in memory, join it against
  each streamed fact chunk — the canonical way to avoid a giant shuffle.
- **Measured, not claimed** — `tracemalloc` reports real peak memory per method.

## Demo

```text
$ python3 src/process.py
rows=1,500,000  results_match=True
full   : 0.50s  peak 123.2 MB
stream : 0.39s  peak 14.0 MB  (8.8x less)
join   : 0.52s  peak 26.7 MB
```

Identical results, ~9× lower peak memory — and the gap widens as the file grows
(`BIGDATA_ROWS=10000000`). `reports/memory.png` plots the memory profiles.

## Components

| Piece | Where | Idea |
|---|---|---|
| Generator | `src/generate_data.py` | write a large fact + small dim table |
| Engine | `src/engine.py` | streaming, lazy pipeline, map-reduce, broadcast join |
| Benchmark | `src/process.py` | full-load vs streaming, memory + correctness |

## Project structure
```
