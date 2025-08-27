# src/zephyrlake/extract.py
# Fetch measurements for a single sensor from OpenAQ

import os
import time

import requests

# API Configuration
API_BASE = "https://api.openaq.org/v3"

# Request Settings (timeouts, retries, pagination)
PAGE_SIZE         = 100                        # Max rows per page
REQUEST_TIMEOUT_S = 30                         # Timeout in seconds
MAX_RETRIES       = 3                          # Retries after first attempt
RETRY_DELAYS_S    = (1, 2, 3)                  # Retry delays in seconds
RETRY_ON_STATUS   = (429, 500, 502, 503, 504)  # Retry on these codes
PAGE_COOLDOWN_S   = 5                          # Pause between page fetches

# Type alias for a sensor measurement record
# e.g. {"location": "359", "parameter": "pm25", "value": 12.3}
Row = dict[str, object]


def _new_session() -> requests.Session:
    """
    Open a new session for the OpenAQ API.

    - Reads OPENAQ_API_KEY from the environment.
    - Fails immediately if the key is missing (can’t talk to API without it).
    - Adds the key to the X-API-Key header so every request is authorized.
    """
    api_key = os.getenv("OPENAQ_API_KEY")

    if not api_key:
        raise RuntimeError("Set OPENAQ_API_KEY in your environment (or .env).")

    session = requests.Session()
    session.headers.update({"X-API-Key": api_key})
    return session


def fetch(sensor_id: int, since_date: str, pages: int) -> list[Row]:
    """
    Fetch measurements for one sensor starting at `since_date`.

    - Collects pages of data from the API.
    - Keeps only the key details that matter.
    - Returns a list of rows with location, parameter, unit, value, date_utc.
    """
    session = _new_session()

    # Endpoint for this sensor’s measurements
    url = f"{API_BASE}/sensors/{sensor_id}/measurements"

    # Storage for all measurement rows
    collected: list[Row] = []

    # ─── Page loop ──────────────────────────────────────────────────
    # Step through all pages, then return the full set of results.
    for page in range(1, pages + 1):
        params = {"datetime_from": since_date, "limit": PAGE_SIZE, "page": page}

        # ─── Retry loop for request errors ───────────────
        # Attempt the request; stop if it succeeds
        for attempt in range(MAX_RETRIES + 1):
            # Send request and save the raw response
            resp = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT_S,
            )

            if resp.ok:
                break

            # Error - wait and retry if tries remain
            if resp.status_code in RETRY_ON_STATUS and attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAYS_S[attempt])
                continue

            # Permanent error or no retries left; raise exception
            resp.raise_for_status()

        # ─── Parse results ───────────────
        results = (resp.json() or {}).get("results", [])
        if not results:
            break  # no more results, stop paging

        # ── Keep required fields ──
        for item in results:
            # Parameter details (name + units)
            param = item.get("parameter") or {}

            # Period field holds the time window of the measurement
            period = item.get("period") or {}
            # Extract the UTC start time from the period (if available)
            utc = (period.get("datetimeFrom") or {}).get("utc")

            # append normalized record into our collection
            collected.append({
                "location":  str(sensor_id),
                "parameter": param.get("name"),
                "unit":      param.get("units"),
                "value":     item.get("value"),
                "date_utc":  utc,
            })

        # ── Pause between pages ──
        # Throttle requests to avoid hitting API rate limits
        time.sleep(PAGE_COOLDOWN_S)

    return collected