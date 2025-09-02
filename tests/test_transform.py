# tests/test_transform.py
# Prove that raw JSON rows transform into a valid, typed DataFrame.

import json
from pathlib import Path
import pandas as pd

from zephyrlake.transform import build_sensor_dataframe  # ← align with src

def test_transform_basic():
    # Given raw JSON fixture from OpenAQ
    sample_path = Path(__file__).parent.parent / "data" / "samples" / "measurements_sample.json"
    rows = json.loads(sample_path.read_text())

    # When we normalize into a DataFrame
    df = build_sensor_dataframe(rows, sensor_id=359)

    # Then the frame is non-empty and has the expected columns
    assert not df.empty
    expected = {
        "sensor_id", "parameter", "unit", "value", "date_utc", "event_date",
    }
    assert expected == set(df.columns)

    # Then timestamp is timezone-aware and event_date is a string partition key
    assert isinstance(df["date_utc"].dtype, pd.DatetimeTZDtype)
    assert df["event_date"].dtype == "string"

    # Then sensor_id is carried and typed as string, and value is numeric
    assert df["sensor_id"].map(type).eq(str).all()
    assert pd.api.types.is_numeric_dtype(df["value"])
