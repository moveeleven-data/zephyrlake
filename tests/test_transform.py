# tests/test_transform.py
# Purpose: Prove that raw JSON rows transform into a valid, typed DataFrame.

import json
from pathlib import Path

import pandas as pd

from zephyrlake.transform import to_frame


def test_transform_basic():
    """Raw JSON fixture should become a typed, non-empty DataFrame."""
    sample_path = (
        Path(__file__).parent.parent / "data" / "samples" / "measurements_sample.json"
    )
    rows = json.loads(sample_path.read_text())

    df = to_frame(rows)

    # Should not be empty
    assert not df.empty

    # Required columns are present
    expected = {
        "city",
        "country",
        "location",
        "parameter",
        "unit",
        "value",
        "date_utc",
        "event_date",
    }
    assert expected.issubset(df.columns)

    # Timestamp column should be timezone-aware
    assert isinstance(df["date_utc"].dtype, pd.DatetimeTZDtype)