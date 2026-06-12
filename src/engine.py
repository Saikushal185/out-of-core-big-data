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

