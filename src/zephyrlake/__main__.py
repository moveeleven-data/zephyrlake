# src/zephyrlake/app.py
# CLI entrypoint: run extract → transform → load for one sensor/date range.

from dotenv import load_dotenv

from .cli import parse_cli_args
from .app import run_pipeline, summarize_run


def main() -> None:
    # Load OpenAQ key into environment
    load_dotenv()

    # Parse CLI arguments into Namespace
    args = parse_cli_args()

    # Run the Zephyrlake pipeline
    stats = run_pipeline(args.sensor_id, args.start_time, args.output_dir, args.max_pages)

    # Print summary
    print(summarize_run(stats, args.sensor_id, args.start_time, args.output_dir))


if __name__ == "__main__":
    main()
