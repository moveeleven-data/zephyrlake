# tests/test_load.py
# Prove the Parquet writer is idempotent.

from pathlib import Path
import pandas as pd

from zephyrlake.load import write_parquet_idempotent

def test_idempotent_write(tmp_path: Path):
    # Given a single-day DataFrame
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

    # When we write it the first time
    first = write_parquet_idempotent(df, tmp_path)

    # Then exactly one file exists under the correct partition path
    assert len(first) == 1
    assert first[0].parent.name == "event_date=2025-08-01"
    assert first[0].name.startswith("part-") and first[0].suffix == ".parquet"

    # When we write the same rows again in a different order
    second = write_parquet_idempotent(df.sample(frac=1, random_state=0), tmp_path)

    # Then nothing new is written
    assert second == []
