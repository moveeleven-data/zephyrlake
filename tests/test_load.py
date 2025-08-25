# tests/test_load.py
# Purpose: Prove the Parquet writer is idempotent.

from pathlib import Path

import pandas as pd

from zephyrlake.load import write_parquet_idempotent


def test_idempotent_write(tmp_path: Path):
    """Re-running the same data should not create a new Parquet file."""
    df = pd.DataFrame(
        {
            "city": ["X"],
            "country": ["US"],
            "location": ["Loc"],
            "parameter": ["pm25"],
            "unit": ["µg/m³"],
            "value": [1.0],
            "date_utc": pd.to_datetime(["2025-08-01T00:00:00Z"], utc=True),
            "event_date": ["2025-08-01"],
        }
    )

    # First write creates a file
    first_write = write_parquet_idempotent(df, tmp_path)
    assert len(first_write) == 1

    # Second write detects same partition/signature → no new file
    second_write = write_parquet_idempotent(df, tmp_path)
    assert len(second_write) == 0
