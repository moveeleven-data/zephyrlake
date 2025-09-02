# src/zephyrlake/cli.py
# Define CLI flags for the Zephyrlake pipeline.

import argparse
from pathlib import Path


def parse_cli_args() -> argparse.Namespace:
    """Parse and return CLI arguments for one ETL run."""
    parser = argparse.ArgumentParser(
        prog="zephyrlake",
        description="Fetch, normalize, and store OpenAQ sensor data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # OpenAQ sensor id (integer key for device)
    parser.add_argument(
        "--sensor", required=True, type=int, dest="sensor_id",
        help="OpenAQ sensor id (e.g., 359)",
    )

    # Start date/time boundary for measurements
    parser.add_argument(
        "--since", required=True, dest="start_time",
        help="Start time: YYYY-MM-DD or ISO 8601 UTC",
    )

    # Output directory for partitioned Parquet files
    parser.add_argument(
        "--out", required=True, type=Path, dest="output_dir",
        help="Output folder (e.g., data/out)",
    )

    # Number of API pages to fetch
    parser.add_argument(
        "--pages", type=int, default=3, dest="max_pages",
        help="Pages to fetch",
    )

    return parser.parse_args()
