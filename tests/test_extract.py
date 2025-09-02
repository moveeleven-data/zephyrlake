# tests/test_extract.py
# Prove that collect_sensor_data is a no-op when zero pages are requested.

import os
from zephyrlake import extract


def test_collect_no_pages_noop():
    # Given an API key in the environment and max_pages set to 0
    old = os.environ.pop("OPENAQ_API_KEY", None)
    try:
        os.environ["OPENAQ_API_KEY"] = "test-key"

        # When we call collect_sensor_data with zero pages
        rows = extract.collect_sensor_data(
            sensor_id=359,
            start_time="2025-08-01T00:00:00Z",
            max_pages=0,
        )

        # Then it returns an empty list without attempting any HTTP work
        assert rows == []
    finally:
        if old is None:
            os.environ.pop("OPENAQ_API_KEY", None)
        else:
            os.environ["OPENAQ_API_KEY"] = old
