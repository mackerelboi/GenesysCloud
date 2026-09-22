#!/usr/bin/env python3
"""
wfm_historical_export.py

Pulls per-30-minute-interval historical Offered / Handled / Total Handle Time
data for a single Genesys Cloud queue (voice media only), for use in the WFM
"historical_data" import template:

    Interval Start Date, Queue, Media Type, Language, Offered,
    Interactions Handled, Total Handle Time

This version uses Genesys Cloud's own pre-computed analytics AGGREGATES
endpoint (POST /api/v2/analytics/conversations/aggregates/query) -- the same
endpoint the Genesys Cloud "Queue Performance Detail" view itself is built
on -- rather than reconstructing counts from raw conversation details. That
means the numbers this script produces should match what you see in the
Genesys Cloud UI for the same queue/date range/media type, because they come
from the same source metrics.

Metrics used, and why
----------------------
  - "Offered"             -> metric `nOffered`,  statistic: count
  - "Interactions Handled" -> metric `tHandle`,   statistic: count
  - "Total Handle Time"    -> metric `tHandle`,   statistic: sum (ms -> s)

Note: Genesys Cloud draws a real distinction between "Answered" (nConnected
-- an agent accepted the interaction) and "Handle" (tHandle -- talk + hold +
after-call-work all completed, e.g. after wrap-up is submitted). These can
differ, especially around transfers: a transferred call can be Answered by
one agent and Handled (wrap-up completed) by another. Since you asked for
"Interactions Handled", this script uses tHandle's count, not nConnected.
If what you actually want to match is an "Answered" column instead, that
would be `nConnected` -- let me know and this is a one-line change.

How it works
------------
1. Authenticates using an OAuth Client Credentials grant (Client ID +
   Client Secret), requested at runtime and never written to disk. If
   GENESYSCLOUD_REGION / GENESYSCLOUD_CLIENT_ID / GENESYSCLOUD_CLIENT_SECRET
   env vars are already set, those are used instead and you won't be
   prompted.
2. Resolves the queue name you type to its queue ID.
3. Runs the aggregates query in <=7-day chunks (to stay well under any
   per-request interval/bucket-count limit) across the requested number of
   weeks back (max 12), with granularity PT30M and a filter on this queue's
   ID and mediaType=voice, and timeZone set to the IANA zone you provide so
   interval boundaries land on local-time half-hours.
4. Writes a CSV, one row per 30-minute interval that had any activity, plus
   a totals line printed to the console so you can do a quick sanity check
   against the Queue Performance Detail view before trusting the file.

IMPORTANT -- validate before importing into a real forecast
-------------------------------------------------------------
Even using Genesys's own aggregate metrics, please spot-check: pick one day
from the output CSV, sum that day's Offered / Interactions Handled / Total
Handle Time, and compare against the Queue Performance Detail view for that
same queue, day and media type (voice) in the Genesys Cloud UI. They should
match closely. If they don't, the most likely causes are:
  - Time zone: this script buckets using the IANA zone you enter; make sure
    it's the same zone your report view is displaying in.
  - Genesys Cloud's own "recalculation window" can revise recent data for a
    short period after conversations complete -- a same-day comparison for
    "today" can show small drift; compare a fully completed prior day.
  - If the report shows "Answered" rather than "Handled" for its interaction
    count column, it will differ slightly from this script's tHandle-based
    count (see note above) -- switch to nConnected if that's the column you
    are matching against.

Requirements
------------
    pip install requests
    (Windows only, if you hit a timezone error: pip install tzdata)

Usage
-----
    python3 wfm_historical_export.py
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
GRANULARITY = "PT30M"   # 30-minute buckets
INTERVAL_MINUTES = 30   # must match GRANULARITY, used for boundary rounding

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
        "metrics": ["nOffered", "tHandle"],
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
                if name == "nOffered":
                    row["offered"] += stats.get("count", 0) or 0
                elif name == "tHandle":
                    row["handled"] += stats.get("count", 0) or 0
                    # Genesys Cloud analytics durations are in milliseconds.
                    row["handle_time"] += (stats.get("sum", 0) or 0) / 1000.0


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    print("=== WFM Historical Data Export (Offered / Handled / Handle Time) ===\n")

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

    # Round the query range to clean 30-minute boundaries in LOCAL time
    # before building the query. If the range starts/ends at an arbitrary
    # instant (e.g. "now" with odd seconds/microseconds), every 30-minute
    # bucket Genesys returns inherits that same offset instead of landing
    # on :00/:30 -- which is what produced misaligned intervals like
    # '10:44:22' instead of '11:00:00'.
    now_local = datetime.now(tz)
    end_local = now_local.replace(second=0, microsecond=0)
    end_local = end_local.replace(minute=(end_local.minute // INTERVAL_MINUTES) * INTERVAL_MINUTES)
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

    out_path = f"historical_data_{queue_name.replace(' ', '_')}.csv"
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
                "Offered",
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
        f"Offered: {total_offered}, Handled: {total_handled}, "
        f"Total Handle Time: {int(total_handle_time)}s "
        f"({total_handle_time / 3600:.1f} hours)"
    )
    print(
        "\nBefore importing: pick one completed day from the CSV, sum its "
        "Offered/Handled/Handle Time, and compare against the Queue "
        "Performance Detail view in Genesys Cloud for that queue, day and "
        "voice media type -- see the notes at the top of this script if "
        "they don't line up."
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled.")
