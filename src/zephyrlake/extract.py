# src/zephyrlake/extract.py
# Purpose: fetch measurements for one sensor from OpenAQ with a tiny, readable retry.

from __future__ import annotations

import os
import time
from typing import Dict, List

import requests

API_BASE = "https://api.openaq.org/v3"

# Request tuning (explicit, no magic numbers)
PAGE_SIZE: int = 100                         # results per page
REQUEST_TIMEOUT_S: float = 30.0              # timeout per request (s)
RETRY_ON_STATUS = (429, 500, 502, 503, 504)  # retry on these
RETRY_DELAYS_S = (0.5, 1.0, 2.0)             # backoff schedule (s)
PAGE_COOLDOWN_S: float = 1.0                 # pause between pages (s)

Row = Dict[str, object]


def _new_session() -> requests.Session:
    """Return a session with API key header; fail fast if the key is missing."""
    api_key = os.getenv("OPENAQ_API_KEY")
    if not api_key:
        raise RuntimeError("Set OPENAQ_API_KEY in your environment (or .env).")
    session = requests.Session()
    session.headers.update({"X-API-Key": api_key})
    return session


def fetch(sensor_id: int, since_date: str, pages: int) -> List[Row]:
    """
    Fetch up to `pages` of measurements for one sensor since `since_date`.
    Returns a flat list of rows containing only fields used downstream.
    """
    session = _new_session()
    collected: List[Row] = []

    for page in range(1, pages + 1):
        # Minimal retry on 429/5xx using RETRY_DELAYS_S
        resp = None
        for attempt_idx, delay_s in enumerate(RETRY_DELAYS_S, start=1):
            resp = session.get(
                f"{API_BASE}/sensors/{sensor_id}/measurements",
                params={"datetime_from": since_date, "limit": PAGE_SIZE, "page": page},
                timeout=REQUEST_TIMEOUT_S,
            )
            if resp.status_code in RETRY_ON_STATUS and attempt_idx < len(RETRY_DELAYS_S):
                time.sleep(delay_s)
                continue
            resp.raise_for_status()
            break

        results = (resp.json() or {}).get("results", [])
        if not results:
            break  # no more data

        # Keep only fields needed downstream; make timestamp flat as `date_utc`
        for item in results:
            par = item.get("parameter") or {}
            period = item.get("period") or {}
            utc = (period.get("datetimeFrom") or {}).get("utc")
            collected.append(
                {
                    "city": None,
                    "country": None,
                    "location": str(sensor_id),  # simple sensor tag
                    "parameter": par.get("name"),
                    "unit": par.get("units"),
                    "value": item.get("value"),
                    "date_utc": utc,
                }
            )

        if PAGE_COOLDOWN_S:
            time.sleep(PAGE_COOLDOWN_S)

    return collected