# src/zephyrlake/transform.py
# Purpose: Normalize raw records into a typed DataFrame.

from typing import Dict, List

import pandas as pd

# Define a type alias: each row contains sensor measurement fields.
# e.g. {"location": "359", "parameter": "pm25", "value": 12.3}
Row = Dict[str, object]


def to_frame(rows: List[Row]) -> pd.DataFrame:
    """
    Normalize raw records (list of dictionaries) into a DataFrame.

    Returns
    -------
    pd.DataFrame
        - location   (string ID for the sensor)
        - parameter  (what was measured)
        - unit       (unit of measure)
        - value      (numeric measurement)
        - date_utc   (UTC timestamp)
        - event_date (string, YYYY-MM-DD, derived from date_utc)
    """
    df = pd.DataFrame.from_records(rows)

    # Return early if no data.
    if df.empty:
        return df

    # Enforce UTC; coerce bad values to NaT.
    df["date_utc"] = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["date_utc"])

    # Derive event_date partition key: YYYY-MM-DD as string.
    df["event_date"] = df["date_utc"].dt.date.astype("string")

    # Convert sensor readings to float64.
    # 1. Sensor readings are usually ±1 µg/m³. Base-10 is not needed.
    # 2. Float cols are backed by Numpy arrays and vectorized.
    df["value"] = pd.to_numeric(df["value"], errors="coerce").astype("float64")

    # Return columns in a fixed order. `df.reindex` fills missing cols with NaN.
    df = df.reindex(
        columns=[
            "location",
            "parameter",
            "unit",
            "value",
            "date_utc",
            "event_date",
        ]
    )

    return df