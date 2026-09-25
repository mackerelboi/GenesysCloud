#!/usr/bin/env python3
"""
wfm_historical_export.py

Pulls per-15-minute-interval historical Offered / Handled / Total Handle Time
data for one or more Genesys Cloud queues (voice media only), for use in the
WFM "historical_data" import template:

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
2. Prompts for one or more queue names, one at a time -- press Enter on a
   blank prompt to finish adding queues and move on. Each queue name is
   resolved to its queue ID as you enter it, so a typo is caught
   immediately rather than after the whole run.
3. Asks once (applies to every queue in this run) for the number of weeks
   back to pull (max 52).
4. Runs the aggregates query per queue, in <=7-day chunks (to stay well
   under any per-request interval/bucket-count limit), with granularity
   PT15M and a filter on that queue's ID and mediaType=voice. No timeZone
   parameter is sent, so Genesys Cloud returns interval boundaries and
   timestamps natively in UTC ('Z'-suffixed) -- this removes the need to
   pick a time zone up front, and means the output file can be plugged
   straight into WFM later without worrying about local offsets or DST.
5. Writes ONE combined CSV covering every queue you entered (each row still
   carries its own Queue name, so they can be told apart/filtered), plus a
   totals breakdown printed to the console -- overall and per queue -- so
   you can do a quick sanity check against the Queue Performance Detail
   view before trusting the file.

Output file naming
-------------------
The output file name encodes the actual date range queried (UTC dates),
not the queue name, since one run can now cover several queues:

    15_Min_Intervals_<start-date>_to_<end-date>.csv

e.g. 15_Min_Intervals_2026-09-25_to_2026-12-25.csv

IMPORTANT -- validate before importing into a real forecast
-------------------------------------------------------------
Even using Genesys's own aggregate metrics, please spot-check: pick one day
from the output CSV, sum that day's Offered / Interactions Handled / Total
Handle Time for one queue, and compare against the Queue Performance Detail
view for that same queue, day and media type (voice) in the Genesys Cloud
UI. They should match closely. If they don't, the most likely causes are:
  - Time zone: this script's output is in UTC, but the Queue Performance
    Detail view in the Genesys Cloud UI is normally displayed in your local
    time zone. Convert one or the other before comparing a given day, or
    the day boundary will be off by an hour (BST) or won't line up at all.
  - Genesys Cloud's own "recalculation window" can revise recent data for a
    short period after conversations complete -- a same-day comparison for
    "today" can show small drift; compare a fully completed prior day.
  - If the report shows "Answered" rather than "Handled" for its interaction
    count column, it will differ slightly from this script's tHandle-based
    count (see note above) -- switch to nConnected if that's the column you
    are matching against.
  - Interval size: this script buckets in 15-minute intervals. If you're
    comparing against a Queue Performance Detail view configured for
    30-minute intervals, each of its rows will look roughly double this
    script's per-row numbers -- that's not a bug, it's two adjacent
    15-minute buckets added together. Compare like-for-like interval sizes,
    or sum pairs of this script's rows before comparing.
  - Long ranges (up to 52 weeks): Genesys Cloud's own data retention and
    any org-specific analytics retention policy may not go back a full
    year -- if a chunk near the start of a 52-week range comes back empty
    while later chunks have data, that's most likely retention, not a bug.

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

import requests

MAX_WEEKS = 52
CHUNK_DAYS = 7          # keep each aggregates query request to a safe size
GRANULARITY = "PT15M"   # 15-minute buckets
INTERVAL_MINUTES = 15   # must match GRANULARITY, used for boundary rounding

MEDIA_TYPE = "voice"    # hardcoded per requirements
LANGUAGE = "english"    # hardcoded per requirements

REQUEST_PACING_SECONDS = 0.5   # small pause between chunk requests
QUEUE_PACING_SECONDS = 1.0     # small pause between queues


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
        max_attempts = 8
        for attempt in range(1, max_attempts + 1):
            try:
                resp = self.session.request(method, url, timeout=60, **kwargs)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                if attempt == max_attempts:
                    sys.exit(f"Network error calling {path} after {max_attempts} attempts: {exc}")
                wait = min(30, 2 ** attempt)
                print(f"  network error ({exc.__class__.__name__}), retrying in {wait}s "
                      f"(attempt {attempt}/{max_attempts})...")
                time.sleep(wait)
                continue

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
            if resp.status_code in (500, 502, 503, 504):
                if attempt == max_attempts:
                    sys.exit(
                        f"API error {resp.status_code} calling {path} after "
                        f"{max_attempts} attempts: {resp.text}"
                    )
                wait = min(30, 2 ** attempt)
                print(f"  Genesys returned {resp.status_code} (likely a transient "
                      f"service issue), retrying in {wait}s (attempt {attempt}/{max_attempts})...")
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                sys.exit(f"API error {resp.status_code} calling {path}: {resp.text}")
            return resp.json() if resp.text else {}
        sys.exit(f"Gave up after repeated retries calling {path}.")

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
        print(f"  no queue found matching '{queue_name}' -- skipping this one.")
        return None
    exact = [e for e in entities if e["name"].lower() == queue_name.lower()]
    match = exact[0] if exact else entities[0]
    if len(entities) > 1 and not exact:
        print(f"  multiple queues matched '{queue_name}', using '{match['name']}'")
    print(f"  resolved queue: {match['name']} ({match['id']})")
    return match["id"], match["name"]


def collect_queues(client):
    """Prompt for one or more queue names, one at a time. The first queue is
    required; after that, a blank entry finishes the list. Returns a list of
    (queue_id, queue_name) tuples, in the order they were successfully
    resolved."""
    queues = []

    first = input("Queue name (exact or partial): ").strip()
    while not first:
        print("  please enter at least one queue name.")
        first = input("Queue name (exact or partial): ").strip()
    resolved = resolve_queue(client, first)
    if resolved:
        queues.append(resolved)

    while True:
        more = input(
            f"Queue name to add ({len(queues)} added so far) "
            "-- or press Enter to finish: "
        ).strip()
        if not more:
            break
        resolved = resolve_queue(client, more)
        if resolved:
            queues.append(resolved)

    if not queues:
        sys.exit("No valid queues were resolved -- nothing to export.")

    return queues


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


def fetch_aggregates(client, queue_id, chunk_start, chunk_end):
    body = {
        "interval": f"{fmt_iso(chunk_start)}/{fmt_iso(chunk_end)}",
        "granularity": GRANULARITY,
        # No "timeZone" param -- Genesys Cloud then returns interval
        # boundaries natively in UTC ('Z'-suffixed), which is what we want
        # for a timezone-free, DST-proof output file.
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


def parse_interval_start(interval_str):
    """Genesys interval strings look like
    '2026-09-15T09:00:00.000Z/2026-09-15T09:15:00.000Z'. Since no timeZone
    parameter is sent on the query, these always come back 'Z'-suffixed
    UTC. Parsed defensively so an explicit offset would also still work."""
    start_str = interval_str.split("/")[0]
    # datetime.fromisoformat handles both 'Z' (after normalizing to
    # '+00:00') and explicit '+HH:MM' offsets, across Python 3.9+.
    normalized = start_str[:-1] + "+00:00" if start_str.endswith("Z") else start_str
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Unrecognized interval format: {interval_str!r}") from exc
    return dt.astimezone(timezone.utc)


def process_results(results, rows):
    """rows: dict keyed on interval_start (UTC) -> {"offered":..,"handled":..,"handle_time":..}"""
    for result in results:
        for entry in result.get("data", []):
            interval_start = parse_interval_start(entry["interval"])
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


def fetch_queue_rows(client, queue_id, start_utc, end_utc):
    """Run the chunked aggregates query for one queue across the full date
    range and return its rows dict, keyed on UTC interval_start."""
    rows = defaultdict(lambda: {"offered": 0, "handled": 0, "handle_time": 0.0})
    for i, (chunk_start_utc, chunk_end_utc) in enumerate(date_chunks(start_utc, end_utc, CHUNK_DAYS)):
        if i > 0 and REQUEST_PACING_SECONDS:
            time.sleep(REQUEST_PACING_SECONDS)
        print(f"    chunk {chunk_start_utc.date()} -> {chunk_end_utc.date()} (UTC) ...")
        results = fetch_aggregates(client, queue_id, chunk_start_utc, chunk_end_utc)
        process_results(results, rows)
    return rows


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

    queues = collect_queues(client)
    print(f"\n{len(queues)} queue(s) queued up for export: "
          + ", ".join(name for _, name in queues))

    while True:
        weeks_input = input(f"\nNumber of weeks back to retrieve (max {MAX_WEEKS}): ").strip()
        try:
            weeks = int(weeks_input)
            if 1 <= weeks <= MAX_WEEKS:
                break
        except ValueError:
            pass
        print(f"  please enter a whole number between 1 and {MAX_WEEKS}.")

    # Round the query range to clean 15-minute boundaries in UTC before
    # building the query. If the range starts/ends at an arbitrary instant
    # (e.g. "now" with odd seconds/microseconds), every 15-minute bucket
    # Genesys returns inherits that same offset instead of landing on
    # :00/:15/:30/:45 -- which is what produced misaligned intervals like
    # '10:44:22' instead of '10:45:00'.
    now_utc = datetime.now(timezone.utc)
    end_utc = now_utc.replace(second=0, microsecond=0)
    end_utc = end_utc.replace(minute=(end_utc.minute // INTERVAL_MINUTES) * INTERVAL_MINUTES)
    start_utc = end_utc - timedelta(weeks=weeks)

    print(f"\nQuerying aggregates from {start_utc} to {end_utc} (UTC)...")

    # combined_rows: list of (interval_start, queue_name, offered, handled, handle_time_seconds)
    combined_rows = []
    per_queue_totals = []

    for i, (queue_id, queue_name) in enumerate(queues):
        if i > 0 and QUEUE_PACING_SECONDS:
            time.sleep(QUEUE_PACING_SECONDS)
        print(f"\n  -- {queue_name} --")
        rows = fetch_queue_rows(client, queue_id, start_utc, end_utc)

        if not rows:
            print(f"    no data found for '{queue_name}' in this date range.")
            per_queue_totals.append((queue_name, 0, 0, 0, 0.0))
            continue

        q_total_offered = q_total_handled = 0
        q_total_handle_time = 0.0
        for interval_start, row in sorted(rows.items()):
            handle_time_seconds = int(round(row["handle_time"]))
            combined_rows.append(
                (interval_start, queue_name, row["offered"], row["handled"], handle_time_seconds)
            )
            q_total_offered += row["offered"]
            q_total_handled += row["handled"]
            q_total_handle_time += handle_time_seconds

        per_queue_totals.append(
            (queue_name, len(rows), q_total_offered, q_total_handled, q_total_handle_time)
        )

    if not combined_rows:
        print("\nNo data found for any queue in this date range. No file written.")
        return

    # Sort the combined output by queue, then chronologically within each queue.
    combined_rows.sort(key=lambda r: (r[1], r[0]))

    start_date_str = start_utc.date().isoformat()
    end_date_str = end_utc.date().isoformat()
    out_path = f"15_Min_Intervals_{start_date_str}_to_{end_date_str}.csv"

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
        for interval_start, queue_name, offered, handled, handle_time_seconds in combined_rows:
            writer.writerow(
                [
                    interval_start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    queue_name,
                    MEDIA_TYPE,
                    LANGUAGE,
                    offered,
                    handled,
                    handle_time_seconds,
                ]
            )

    print(f"\nDone. Wrote {len(combined_rows)} rows across {len(queues)} queue(s) to {out_path}")

    print("\nPer-queue totals:")
    grand_offered = grand_handled = 0
    grand_handle_time = 0.0
    for queue_name, n_rows, q_offered, q_handled, q_handle_time in per_queue_totals:
        print(
            f"  {queue_name}: {n_rows} rows -- Offered: {q_offered}, "
            f"Handled: {q_handled}, Total Handle Time: {int(q_handle_time)}s "
            f"({q_handle_time / 3600:.1f} hours)"
        )
        grand_offered += q_offered
        grand_handled += q_handled
        grand_handle_time += q_handle_time

    print(
        f"\nGrand totals across all queues -- "
        f"Offered: {grand_offered}, Handled: {grand_handled}, "
        f"Total Handle Time: {int(grand_handle_time)}s "
        f"({grand_handle_time / 3600:.1f} hours)"
    )
    print(
        "\nBefore importing: pick one completed day for one queue from the "
        "CSV, sum its Offered/Handled/Handle Time, and compare against the "
        "Queue Performance Detail view in Genesys Cloud for that queue, day "
        "and voice media type -- see the notes at the top of this script if "
        "they don't line up."
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled.")
