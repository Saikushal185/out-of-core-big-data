"""A tiny out-of-core processing engine — the ideas Spark/Dask automate.

Everything streams the fact table in bounded-size chunks and combines partial
results, so peak memory stays flat regardless of file size:

  * lazy pipeline   — operations are recorded and only run on `.collect()`
  * streaming groupby — partial aggregates per chunk, merged (map-reduce)
  * broadcast join  — the small dimension table is held in memory and joined
                      against each streamed chunk (avoids a giant shuffle)
"""
import pandas as pd


def stream(path, chunksize=200_000, usecols=None):
    """Yield DataFrame chunks; never materializes the whole file."""
    for chunk in pd.read_csv(path, chunksize=chunksize, usecols=usecols):
        yield chunk


class LazyPipeline:
    """Records map/filter steps; executes them chunk-by-chunk on collect()."""
    def __init__(self, path, chunksize=200_000, usecols=None):
        self.path, self.chunksize, self.usecols = path, chunksize, usecols
        self.ops = []

    def filter(self, fn):
        self.ops.append(("filter", fn)); return self

    def assign(self, **kwargs):
        self.ops.append(("assign", kwargs)); return self

    def _apply(self, chunk):
        for kind, payload in self.ops:
            if kind == "filter":
                chunk = chunk[payload(chunk)]
            elif kind == "assign":
                chunk = chunk.assign(**payload)
        return chunk

    def groupby_agg(self, key, value, aggs=("sum", "count")):
        """Streaming groupby: accumulate per-chunk partials, then finalize."""
        acc = {}
        for chunk in stream(self.path, self.chunksize, self.usecols):
            chunk = self._apply(chunk)
            g = chunk.groupby(key)[value].agg(["sum", "count"])
            for k, row in g.iterrows():
                if k not in acc:
                    acc[k] = [0.0, 0]
                acc[k][0] += row["sum"]; acc[k][1] += int(row["count"])
        out = pd.DataFrame(
            [{key: k, "sum": v[0], "count": v[1],
              "mean": v[0] / v[1] if v[1] else 0.0} for k, v in acc.items()]
        ).sort_values(key).reset_index(drop=True)
        return out[[key] + list(aggs) + (["mean"] if "mean" not in aggs else [])]


def broadcast_join_agg(fact_path, dim_path, join_key, group_key, value,
                       chunksize=200_000):
    """Join each streamed fact chunk to an in-memory dim table, then aggregate."""
    dim = pd.read_csv(dim_path).set_index(join_key)
    acc = {}
    for chunk in stream(fact_path, chunksize):
        merged = chunk.join(dim, on=join_key)
        g = merged.groupby(group_key)[value].agg(["sum", "count"])
        for k, row in g.iterrows():
            acc.setdefault(k, [0.0, 0])
            acc[k][0] += row["sum"]; acc[k][1] += int(row["count"])
    return pd.DataFrame(
        [{group_key: k, "sum": v[0], "count": v[1],
          "mean": v[0] / v[1]} for k, v in acc.items()]
    ).sort_values(group_key).reset_index(drop=True)
