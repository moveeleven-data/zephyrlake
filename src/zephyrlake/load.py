# src/zephyrlake/load.py
# Write one Parquet file per day, skipping duplicates.

import hashlib
from pathlib import Path

import pandas as pd


def _partition_hash(event_date: str, day_frame: pd.DataFrame) -> str:
    """Return compact hash for one day’s rows (order-insensitive)."""
    if day_frame.empty:
        return "empty"

    # Restrict to a stable subset of columns and sort them
    cols = ["sensor_id", "parameter", "unit", "value", "date_utc"]
    ordered = (
        day_frame[cols]
        .sort_values(["date_utc", "sensor_id", "parameter"])
        .reset_index(drop=True)
    )

    # Create a hash object seeded with the event_date
    partition_hash = hashlib.md5(event_date.encode())

    # Generate a numeric hash for each row
    row_hashes = pd.util.hash_pandas_object(ordered, index=False)

    # Update the partition hash with the row-level hashes
    partition_hash.update(row_hashes.values.tobytes())

    # Return the first 12 characters as the partition signature
    partition_signature = partition_hash.hexdigest()[:12]

    return partition_signature


def _write_atomic_parquet(frame: pd.DataFrame, dest: Path) -> None:
    """
    Save a DataFrame as Parquet.

    Writing directly to the final file (e.g. `part-abc123.parquet`)
    risks leaving behind a broken file if the process is interrupted.

    Instead, we write to a temporary file (e.g. `.tmp-part-abc123.parquet`)
    and only rename it if the write succeeds.
    """

    # Create a temporary path with a ".tmp-" prefix in the same folder as dest
    temp_path = dest.with_name(f".tmp-{dest.stem}.parquet")

    # Write the DataFrame to the temporary file
    frame.to_parquet(temp_path, index=False)

    # If write succeeds, rename the temp file to the final name
    temp_path.replace(dest)


def write_parquet_idempotent(sensor_frame: pd.DataFrame, output_dir: Path) -> list[Path]:
    """
    Write one Parquet per event_date under output_dir/event_date=YYYY-MM-DD/.
    Skip writing if a file with the same signature already exists.
    Return list of newly written file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    written_files: list[Path] = []

    if sensor_frame.empty:
        return written_files

    # Break the DataFrame into one group per event_date
    groups = sensor_frame.groupby("event_date", observed=True)

    for event_date, day_frame in groups:
        part_dir = output_dir / f"event_date={event_date}"
        part_dir.mkdir(parents=True, exist_ok=True)

        # Compute a unique file name for this partition
        file_hash = _partition_hash(str(event_date), day_frame)
        dest = part_dir / f"part-{file_hash}.parquet"

        # If an identical file already exists, skip writing it
        if dest.exists():
            continue

        # Otherwise, write the new Parquet file safely
        _write_atomic_parquet(day_frame, dest)
        written_files.append(dest)

    return written_files