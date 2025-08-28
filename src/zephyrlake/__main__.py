# src/zephyrlake/__main__.py
# Purpose: CLI entrypoint that runs extract → transform → load.

import argparse
from pathlib import Path

# Optional .env support; ignored if not installed.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from .extract import collect_sensor_data
from .transform import build_sensor_dataframe
from .load import write_parquet_idempotent


def main() -> None:
    parser = argparse.ArgumentParser("zephyrlake-micro")
    parser.add_argument("--sensor", required=True, type=int, help="OpenAQ sensor id (e.g., 359)")
    parser.add_argument("--since", required=True, help="YYYY-MM-DD (UTC)")
    parser.add_argument("--out", required=True, help="Output folder (e.g., data/out)")
    parser.add_argument("--pages", type=int, default=3, help="Pages to fetch (default 3)")
    args = parser.parse_args()

    # Extract
    # Coerce plain dates (YYYY-MM-DD) to UTC midnight (YYYY-MM-DDT00:00:00Z)
    # See docs/time-boundaries.md for why.
    since = args.since if "T" in args.since else f"{args.since}T00:00:00Z"
    rows = collect_sensor_data(
        sensor_id=args.sensor,
        start_time=since,
        max_pages=args.pages
    )
    print(f"Fetched {len(rows)} rows from sensor {args.sensor} since {args.since}")

    # Transform
    df = build_sensor_dataframe(rows, sensor_id=args.sensor)

    # Load (idempotent write)
    files = write_parquet_idempotent(df, Path(args.out))
    days = df["event_date"].nunique() if not df.empty else 0
    print(f"Wrote {len(files)} file(s) to {args.out} across {days} day(s); kept {len(df)}/{len(rows)} rows")


if __name__ == "__main__":
    main()
