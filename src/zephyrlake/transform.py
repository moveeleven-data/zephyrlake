# src/zephyrlake/transform.py
# Purpose: Normalize raw rows into a typed DataFrame.

from __future__ import annotations

from typing import Dict, List

import pandas as pd

Row = Dict[str, object]


def to_frame(rows: List[Row]) -> pd.DataFrame:
    """
    Normalize raw rows into a tidy DataFrame with columns:
    city, country, location, parameter, unit, value, date_utc, event_date
    """
    df = pd.DataFrame.from_records(rows)
    if df.empty:
        return df

    # Parse timestamps, drop invalids, derive event_date
    df["date_utc"] = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["date_utc"])
    df["event_date"] = df["date_utc"].dt.date.astype("string")

    # Make sure numeric values are numeric
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Return columns in a fixed order to keep downstream simple
    return df[
        [
            "city",
            "country",
            "location",
            "parameter",
            "unit",
            "value",
            "date_utc",
            "event_date",
        ]
    ]