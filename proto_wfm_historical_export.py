#!/usr/bin/env python3
"""
wfm_historical_export_nconversations_test.py

DIAGNOSTIC VARIANT -- exact copy of wfm_historical_export.py with one change:
the "Offered" column is now sourced from the `nConversations` metric instead
of `nOffered`, to test the hypothesis that `nOffered` counts raw queue-offer
EVENTS (which can include requeues of the same call, e.g. after a bullseye
routing timeout), while `nConversations` might count distinct/unique
conversations that touched the queue -- which could better match what the
Queue Performance Detail view shows.

IMPORTANT CAVEAT: Genesys Cloud's public documentation does not clearly
define what `nConversations` means or how it differs from `nOffered` -- the
only community discussion found on this was informal and inconclusive. This
script exists purely so you can run it against the same queue/interval as
your original export and compare the numbers side by side against the
Queue Performance Detail view, rather than guessing further from
documentation that doesn't clearly exist.

Run this the same way as the original script, for the same queue and a
short date range covering 15 Sep, then compare its "Offered" column for
11:00-11:15 against both the original script's output and the report page.

Everything else (Handled/tHandle logic, CSV columns, credential handling,
timezone alignment) is unchanged from wfm_historical_export.py.
"""

import csv
import getpass
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

MAX_WEEKS = 12
CHUNK_DAYS = 7          # keep each aggregates query request to a safe size
GRANULARITY = "PT15M"   # 15-minute buckets

MEDIA_TYPE = "voice"    # hardcoded per requirements
LANGUAGE = "english"    # hardcoded per requirements


# --------------------------------------------------------------------------
# Auth / credentials
# --------------------------------------------------------------------------

def get_credentials():
    """Read region/client id/secret from env vars, falling back to prompts.
    The secret is always requested with getpass so it never echoes and is
    never written to disk by this script."""
    region = os.environ.get("GENESYSCLOUD_REGION")
    if not region:
        region = input(
            "Genesys Cloud region/environment (e.g. mypurecloud.com, "
            "mypurecloud.ie, euw2.pure.cloud): "
        ).strip()

    client_id = os.environ.get("GENESYSCLOUD_CLIENT_ID")
    if not client_id:
        client_id = input("Client ID: ").strip()

    client_secret = os.environ.get("GENESYSCLOUD_CLIENT_SECRET")
    if not client_secret:
        client_secret = getpass.getpass("Client Secret (hidden): ").strip()

    if not (region and client_id and client_secret):
        sys.exit("Region, Client ID and Client Secret are all required.")

    return region, client_id, client_secret


def get_access_token(region, client_id, client_secret):
    url = f"https://login.{region}/oauth/token"
    resp = requests.post(
        url,
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    if resp.status_code != 200:
        sys.exit(
            f"Authentication failed ({resp.status_code}): {resp.text}\n"
            "Check the region, Client ID, Client Secret, and that the "
            "client's Grant Type is set to Client Credentials."
        )
    return resp.json()["access_token"]


class GenesysClient:
    def __init__(self, region, token):
        self.base = f"https://api.{region}"
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )

    def request(self, method, path, **kwargs):
        url = f"{self.base}{path}"
        for attempt in range(6):
            resp = self.session.request(method, url, timeout=60, **kwargs)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", "5"))
                print(f"  rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                sys.exit(
                    f"403 Forbidden calling {path}. The OAuth client's "
                    "assigned role likely lacks the required permission "
                    "(e.g. Analytics > Conversation Aggregate > View, "
                    "Routing > Queue > View)."
                )
            if resp.status_code >= 400:
                sys.exit(f"API error {resp.status_code} calling {path}: {resp.text}")
            return resp.json() if resp.text else {}
        sys.exit(f"Gave up after repeated rate limiting calling {path}.")

    def get(self, path, **kwargs):
        return self.request("GET", path, **kwargs)

    def post(self, path, **kwargs):
        return self.request("POST", path, **kwargs)


# --------------------------------------------------------------------------
# Queue lookup
# --------------------------------------------------------------------------

def resolve_queue(client, queue_name):
    data = client.get(
        "/api/v2/routing/queues",
        params={"name": queue_name, "pageSize": 25},
    )
    entities = data.get("entities", [])
    if not entities:
        sys.exit(f"No queue found matching name '{queue_name}'.")
    exact = [e for e in entities if e["name"].lower() == queue_name.lower()]
    match = exact[0] if exact else entities[0]
    if len(entities) > 1 and not exact:
        print(f"  multiple queues matched '{queue_name}', using '{match['name']}'")
    print(f"  resolved queue: {match['name']} ({match['id']})")
    return match["id"], match["name"]


# --------------------------------------------------------------------------
# Aggregates query
# --------------------------------------------------------------------------

def date_chunks(start, end, chunk_days):
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=chunk_days), end)
        yield cur, nxt
        cur = nxt


def fmt_iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def fetch_aggregates(client, queue_id, chunk_start, chunk_end, tz_name):
    body = {
        "interval": f"{fmt_iso(chunk_start)}/{fmt_iso(chunk_end)}",
        "granularity": GRANULARITY,
        "timeZone": tz_name,
        "filter": {
            "type": "and",
            "predicates": [
                {"type": "dimension", "dimension": "queueId", "value": queue_id},
                {"type": "dimension", "dimension": "mediaType", "value": MEDIA_TYPE},
            ],
        },
        # DIAGNOSTIC CHANGE: nOffered swapped for nConversations.
        "metrics": ["nConversations", "tHandle"],
    }
    data = client.post("/api/v2/analytics/conversations/aggregates/query", json=body)
    return data.get("results", [])


def parse_interval_start(interval_str, tz):
    """Genesys interval strings look like
    '2026-09-15T09:00:00.000Z/2026-09-15T09:15:00.000Z' when no timeZone is
    given, but return the LOCAL offset (e.g. '...+01:00') once a timeZone
    parameter is supplied on the query, so we can't assume 'Z'. Parse
    whichever form comes back and return the start, converted to the
    requested local time zone, naive for display."""
    start_str = interval_str.split("/")[0]
    # datetime.fromisoformat handles both 'Z' (after normalizing to
    # '+00:00') and explicit '+HH:MM' offsets, across Python 3.9+.
    normalized = start_str[:-1] + "+00:00" if start_str.endswith("Z") else start_str
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Unrecognized interval format: {interval_str!r}") from exc
    return dt.astimezone(tz).replace(tzinfo=None)


def process_results(results, tz, rows):
    """rows: dict keyed on interval_start -> {"offered":..,"handled":..,"handle_time":..}"""
    for result in results:
        for entry in result.get("data", []):
            interval_start = parse_interval_start(entry["interval"], tz)
            row = rows[interval_start]
            for metric in entry.get("metrics", []):
                name = metric.get("metric")
                stats = metric.get("stats", {})
                # DIAGNOSTIC CHANGE: reading nConversations instead of nOffered.
                if name == "nConversations":
                    row["offered"] += stats.get("count", 0) or 0
                elif name == "tHandle":
                    row["handled"] += stats.get("count", 0) or 0
                    # Genesys Cloud analytics durations are in milliseconds.
                    row["handle_time"] += (stats.get("sum", 0) or 0) / 1000.0


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    print("=== WFM Historical Data Export -- DIAGNOSTIC: nConversations instead of nOffered ===\n")

    region, client_id, client_secret = get_credentials()
    print("\nAuthenticating...")
    token = get_access_token(region, client_id, client_secret)
    client = GenesysClient(region, token)
    print("  authenticated OK.\n")

    queue_name_input = input("Queue name (exact or partial): ").strip()
    queue_id, queue_name = resolve_queue(client, queue_name_input)

    while True:
        weeks_input = input(f"Number of weeks back to retrieve (max {MAX_WEEKS}): ").strip()
        try:
            weeks = int(weeks_input)
            if 1 <= weeks <= MAX_WEEKS:
                break
        except ValueError:
            pass
        print(f"  please enter a whole number between 1 and {MAX_WEEKS}.")

    tz_input = input(
        "Time zone for interval bucketing, as an IANA name matching what "
        "your Genesys Cloud report is displayed in (e.g. Europe/London) "
        "[default Europe/London]: "
    ).strip()
    tz_name = tz_input or "Europe/London"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        sys.exit(
            f"Could not load time zone '{tz_name}'. On Windows you may need "
            "to run: pip install tzdata"
        )

    # Round the query range to clean 15-minute boundaries in LOCAL time
    # before building the query (see wfm_historical_export.py for why).
    now_local = datetime.now(tz)
    end_local = now_local.replace(second=0, microsecond=0)
    end_local = end_local.replace(minute=(end_local.minute // 15) * 15)
    start_local = end_local - timedelta(weeks=weeks)

    rows = defaultdict(lambda: {"offered": 0, "handled": 0, "handle_time": 0.0})

    print(f"\nQuerying aggregates from {start_local} to {end_local} "
          f"(local time, {tz_name})...")
    for chunk_start_local, chunk_end_local in date_chunks(start_local, end_local, CHUNK_DAYS):
        print(f"  chunk {chunk_start_local.date()} -> {chunk_end_local.date()} ...")
        chunk_start_utc = chunk_start_local.astimezone(timezone.utc)
        chunk_end_utc = chunk_end_local.astimezone(timezone.utc)
        results = fetch_aggregates(client, queue_id, chunk_start_utc, chunk_end_utc, tz_name)
        process_results(results, tz, rows)

    if not rows:
        print("\nNo data found for this queue/date range. No file written.")
        return

    out_path = f"historical_data_{queue_name.replace(' ', '_')}_nConversations_test.csv"
    total_offered = total_handled = 0
    total_handle_time = 0.0

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Interval Start Date",
                "Queue",
                "Media Type",
                "Language",
                "Offered (from nConversations)",
                "Interactions Handled",
                "Total Handle Time",
            ]
        )
        for interval_start, row in sorted(rows.items()):
            handle_time_seconds = int(round(row["handle_time"]))
            writer.writerow(
                [
                    interval_start.strftime("%Y-%m-%d %H:%M"),
                    queue_name,
                    MEDIA_TYPE,
                    LANGUAGE,
                    row["offered"],
                    row["handled"],
                    handle_time_seconds,
                ]
            )
            total_offered += row["offered"]
            total_handled += row["handled"]
            total_handle_time += handle_time_seconds

    print(f"\nDone. Wrote {len(rows)} rows to {out_path}")
    print(
        f"\nTotals across the full range -- "
        f"Offered (nConversations): {total_offered}, Handled: {total_handled}, "
        f"Total Handle Time: {int(total_handle_time)}s "
        f"({total_handle_time / 3600:.1f} hours)"
    )
    print(
        "\nCompare this file's 'Offered (from nConversations)' column for "
        "11:00-11:15 on 15 Sep against both the original export's Offered "
        "number and the Queue Performance Detail report in Genesys Cloud."
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled.")
