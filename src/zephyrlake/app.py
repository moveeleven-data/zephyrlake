# src/zephyrlake/app.py
# Pipeline orchestration utilities: load env, run ETL, format output.

from pathlib import Path

from .extract import collect_sensor_data
from .transform import build_sensor_dataframe
from .load import write_parquet_idempotent


def load_env() -> None:
    """Load .env for local development."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


def run_pipeline(
    sensor_id: int,
    start_time: str,
    output_dir: Path,
    max_pages: int,
) -> dict[str, int]:
    """
    Execute one ETL run:
      - Extract: fetch raw rows from OpenAQ
      - Transform: normalize into typed DataFrame
      - Load: write idempotent Parquet partitions
    Return summary stats for reporting.
    """
    # Extract
    start_iso = start_time if "T" in start_time else f"{start_time}T00:00:00Z"
    raw_rows = collect_sensor_data(
        sensor_id=sensor_id,
        start_time=start_iso,
        max_pages=max_pages,
    )

    # Transform
    df = build_sensor_dataframe(raw_rows, sensor_id=sensor_id)

    # Load
    files = write_parquet_idempotent(df, output_dir)
    day_count = int(df["event_date"].nunique()) if not df.empty else 0

    # Build stats dictionary
    stats = {
        "rows": len(raw_rows),
        "kept": len(df),
        "files": len(files),
        "days": day_count,
    }
    return stats


def summarize_run(
    stats: dict[str, int],
    sensor_id: int,
    start_time: str,
    output_dir: Path,
) -> str:
    """Return a formatted summary of the ETL run."""
    rows = stats["rows"]
    kept = stats["kept"]
    files = stats["files"]
    days = stats["days"]

    summary = (
        f"Fetched {rows} rows from sensor {sensor_id} since {start_time}\n"
        f"Wrote {files} file(s) to {output_dir} across {days} day(s); "
        f"kept {kept}/{rows} rows"
    )
    return summary