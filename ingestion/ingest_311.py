"""Day 1: pull one day of NYC 311 service requests and save it as Parquet.

Usage (from the project root, with the virtual environment active):
    python ingestion/ingest_311.py --date 2026-11-10

Why it is written this way:
  * Incremental: we pull ONE day at a time, not the whole 40M+ row table.
  * Idempotent: running the same date twice overwrites the same file.
  * Partitioned: files land in data/raw/created_date=YYYY-MM-DD/
  * Stable paging: we sort by unique_key so pages never overlap or skip rows.
"""
import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 50_000
FIELDS = [
    "unique_key", "created_date", "closed_date", "due_date",
    "agency", "agency_name", "complaint_type", "descriptor",
    "status", "borough", "incident_zip", "latitude", "longitude",
    "open_data_channel_type", "resolution_description",
]


def fetch_day(day: date, session: requests.Session) -> pd.DataFrame:
    """Download every request created on `day`, page by page."""
    start = day.isoformat() + "T00:00:00"
    end = (day + timedelta(days=1)).isoformat() + "T00:00:00"
    where = f"created_date >= '{start}' AND created_date < '{end}'"
    rows, offset = [], 0
    while True:
        params = {
            "$select": ",".join(FIELDS),
            "$where": where,
            "$order": "unique_key",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        for attempt in range(1, 4):  # simple retry with backoff
            try:
                resp = session.get(ENDPOINT, params=params, timeout=60)
                resp.raise_for_status()
                break
            except requests.RequestException as exc:
                if attempt == 3:
                    raise
                wait = 2 ** attempt
                print(f"  request failed ({exc}); retrying in {wait}s", file=sys.stderr)
                time.sleep(wait)
        page = resp.json()
        rows.extend(page)
        print(f"  fetched {len(page):>6} rows (offset {offset})")
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return pd.DataFrame(rows, columns=FIELDS)


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest one day of NYC 311 data")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (created_date)")
    parser.add_argument("--out", default="data/raw", help="output folder")
    args = parser.parse_args()
    day = datetime.strptime(args.date, "%Y-%m-%d").date()

    session = requests.Session()
    token = os.getenv("SOCRATA_APP_TOKEN")  # optional; raises rate limits
    if token:
        session.headers["X-App-Token"] = token

    print(f"Pulling 311 requests created on {day} ...")
    df = fetch_day(day, session)
    if df.empty:
        print("No rows returned. Check the date (the data lags by a day or so).")
        return 1

    out_dir = Path(args.out) / f"created_date={day.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "311.parquet"
    df.to_parquet(out_file, index=False)  # overwrite = idempotent
    print(f"Saved {len(df):,} rows -> {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
