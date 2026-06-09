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
