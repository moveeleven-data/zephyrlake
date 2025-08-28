# src/zephyrlake/transform.py
# Normalize raw records into a typed DataFrame.

import pandas as pd

# Type alias for a sensor measurement record
# e.g. {"sensor_id": 359, "parameter": "pm25", "value": 12.3}
Measurement = dict[str, object]


def normalize_measurement(raw_item, sensor_id) -> Measurement:
    """Convert one raw record from the API 'results' list into a Measurement dict."""

    # Extract nested fields
    param_info    = raw_item.get("parameter")       or {}
    period_info   = raw_item.get("period")          or {}
    datetime_info = period_info.get("datetimeFrom") or {}

    # Map the raw fields into a consistent format
    measurement = {
        "sensor_id": str(sensor_id),
        "parameter": param_info.get("name"),
        "unit":      param_info.get("units"),
        "value":     raw_item.get("value"),
        "date_utc":  datetime_info.get("utc")
    }
    return measurement


def build_sensor_dataframe(rows: list[dict[str, object]], sensor_id: int) -> pd.DataFrame:
    """
    Normalize raw records (list of dictionaries) into a DataFrame.

    Returns
    -------
    pd.DataFrame
        - sensor_id  (string ID for the sensor)
        - parameter  (what was measured)
        - unit       (unit of measure)
        - value      (numeric measurement)
        - date_utc   (UTC timestamp)
        - event_date (string, YYYY-MM-DD, derived from date_utc)
    """

    # Convert raw API results to normalized dictionaries
    norm_rows = [normalize_measurement(item, sensor_id) for item in rows]

    df = pd.DataFrame.from_records(norm_rows)
    if df.empty:
        return df

    # Enforce UTC; coerce bad values to NaT.
    df["date_utc"] = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["date_utc"])

    # Derive event_date partition key: YYYY-MM-DD as string.
    df["event_date"] = df["date_utc"].dt.date.astype("string")

    # Sensor readings are usually ±1 µg/m³. Decimal precision is not needed.
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Return columns in a fixed order. `df.reindex` fills missing cols with NaN.
    df = df.reindex(
        columns=[
            "sensor_id",
            "parameter",
            "unit",
            "value",
            "date_utc",
            "event_date",
        ]
    )

    return df