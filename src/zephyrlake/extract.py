# src/zephyrlake/extract.py
# Fetch measurements for a single sensor from OpenAQ API

import os
import time

import requests

# API Configuration
API_BASE_URL = "https://api.openaq.org/v3"

# Request Settings
REQUEST_TIMEOUT    = 10                         # Each request timeout
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}  # Retry on transient errors
RETRY_DELAYS       = (1, 2, 3)                  # Wait times between retries
RESULTS_PER_PAGE   = 100                        # Max rows per page
PAGE_DELAY         = 5                          # Delay between page fetches

# Type alias for a sensor measurement record
# e.g. {"location": "359", "parameter": "pm25", "value": 12.3}
Measurement = dict[str, object]


def _create_api_session() -> requests.Session:
    """Create and return an API session with the key attached."""
    api_key = os.getenv("OPENAQ_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAQ_API_KEY must be set in your environment.")

    # Create a session and attach the API key for all requests
    session = requests.Session()
    session.headers.update({"X-API-Key": api_key})
    return session


def _fetch_raw_page(session, url, params) -> dict[str, object]:
    """Fetch one page of raw measurement records from the OpenAQ API.

    Returns dict with 'results' (raw records) and 'meta' (paging info)
    """

    # Try once, then retry on transient errors
    for attempt in range(len(RETRY_DELAYS) + 1):
        if attempt > 0:
            time.sleep(RETRY_DELAYS[attempt - 1])

        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if response.ok:
            return response.json()  # Success, return raw page

        # Stop retrying and fail if out of attempts or error is not retryable
        non_retryable_error = response.status_code not in RETRY_STATUS_CODES
        retries_exhausted   = attempt == len(RETRY_DELAYS)
        if retries_exhausted or non_retryable_error:
            response.raise_for_status()


def _normalize_measurement(raw_item, sensor_id) -> Measurement:
    """Convert one raw record from the API 'results' list into a Measurement dict."""

    # Extract nested fields
    param_info    = raw_item.get("parameter")       or {}
    period_info   = raw_item.get("period")          or {}
    datetime_info = period_info.get("datetimeFrom") or {}

    # Map the raw fields into a consistent format
    measurement = {
        "location":  str(sensor_id),
        "parameter": param_info.get("name"),
        "unit":      param_info.get("units"),
        "value":     raw_item.get("value"),
        "date_utc":  datetime_info.get("utc")
    }
    return measurement


def collect_sensor_measurements(sensor_id, start_time, max_pages=50) -> list[Measurement]:
    """Return normalized measurements for one sensor from `start_time` onward."""
    all_measurements = []

    endpoint_url = f"{API_BASE_URL}/sensors/{sensor_id}/measurements"

    with _create_api_session() as session:
        # Iterate through pages until exhausted or max reached
        for page_num in range(1, max_pages + 1):
            params = {
                "datetime_from": start_time,  # Lower bound on timestamp
                "limit": RESULTS_PER_PAGE,    # Max number of records per page
                "page": page_num              # Current page number
            }

            response_json = _fetch_raw_page(session, endpoint_url, params)
            raw_items = response_json.get("results", [])
            if not raw_items: # No more data
                break

            # Normalize and collect records
            for raw_item in raw_items:
                measurement = _normalize_measurement(raw_item, sensor_id)
                all_measurements.append(measurement)

            time.sleep(PAGE_DELAY)  # Avoid rate limits

    return all_measurements  # Full list of normalized records