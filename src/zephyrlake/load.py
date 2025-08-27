# src/zephyrlake/load.py
# Write one Parquet file per day, skipping duplicates.

import hashlib
from pathlib import Path
from typing import List

import pandas as pd


def _partition_hash(event_date: str, part_df: pd.DataFrame) -> str:
    """Compact signature: day + row count + min/max timestamp."""
    if part_df.empty:
        return "empty"
    row_count = len(part_df)
    tmin = part_df["date_utc"].min().isoformat()
    tmax = part_df["date_utc"].max().isoformat()
    sig = f"{event_date}:{row_count}:{tmin}:{tmax}".encode()
    return hashlib.md5(sig).hexdigest()[:12]


def write_parquet_idempotent(df: pd.DataFrame, out_dir: Path) -> List[Path]:
    """
    Write one Parquet per day under event_date=YYYY-MM-DD/.
    Skip if a file with the same signature already exists.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []
    if df.empty:
        return written

    # observed=True avoids creating empty groups when event_date is categorical
    for event_date, part_df in df.groupby("event_date", observed=True):
        part_dir = out_dir / f"event_date={event_date}"
        part_dir.mkdir(parents=True, exist_ok=True)

        file_hash = _partition_hash(str(event_date), part_df)
        dest = part_dir / f"part-{file_hash}.parquet"
        if dest.exists():
            continue  # already have this day's file for these rows

        tmp = part_dir / f".tmp-{file_hash}.parquet"
        part_df.to_parquet(tmp, index=False)
        tmp.replace(dest)
        written.append(dest)

    return written
