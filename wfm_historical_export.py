#!/usr/bin/env python3
"""
wfm_historical_export.py

Pulls per-15-minute-interval historical interaction data for a single Genesys
Cloud queue and writes it out in a layout matching the WFM "historical_data"
import template:

    Interval Start Date, Queue, Media Type, Skill Set, Language,
    Offered, Interactions Handled, Total Handle Time

How it works
------------
1. Authenticates against Genesys Cloud using an OAuth Client Credentials grant
   (Client ID + Client Secret) -- these are requested at runtime, never stored
   on disk. If GENESYSCLOUD_CLIENT_ID / GENESYSCLOUD_CLIENT_SECRET /
   GENESYSCLOUD_REGION env vars are already set in your shell, those are used
   instead and you won't be prompted.
2. Resolves the queue name you type to its queue ID.
3. Pulls conversation details (POST /api/v2/analytics/conversations/details/query)
   for the requested number of weeks back (max 12), in <=7-day chunks (the
   API's max query interval), paginating with the cursor until exhausted.
4. Walks each conversation's participants/sessions/segments to work out:
      - when the interaction was offered to this queue (segment purpose "queue")
      - when/if it was actually handled by an agent (purpose "user", i.e.
        connected talk/hold/wrap-up time) tied to this queue
      - the skills requested on that interaction (requestedRoutingSkillIds)
5. Buckets everything into 15-minute local-time intervals and aggregates
   Offered / Interactions Handled / Total Handle Time (seconds), split out by
   the skill-set combination on each interaction.
6. Writes a CSV.

IMPORTANT -- please read before trusting the output
----------------------------------------------------
Genesys Cloud's own "offered" / "handled" / "handle time" analytics metrics
(nOffered, nHandled, tHandle, etc.) are computed by Genesys internally from
the full segment model, and are exposed cleanly via the *aggregates* endpoint
-- but that endpoint doesn't break results out by skill set, which is why
this script reconstructs the numbers itself from raw conversation *details*.

The reconstruction logic below is a reasonable, documented best-effort
mapping (see the comments in `process_conversation`), but it has NOT been
validated against Genesys's own aggregate totals. Before relying on this data
for a real forecast:

    1. Run this script for a queue and week.
    2. Separately pull `gc analytics conversations aggregates query` (or the
       Performance > Queue Activity view) totals for the same queue/week.
    3. Compare "Offered" and "Handled" totals and total handle time. If they
       don't match within a small tolerance, the segment-matching logic below
       will need adjusting for your org's routing configuration (e.g. IVR
       hand-offs, transfers, conferences) before it's safe to import.

Requirements
------------
    pip install requests

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

MAX_WEEKS = 12
INTERVAL_MINUTES = 15
CHUNK_DAYS = 7  # max span per details query request
PAGE_SIZE = 100  # conversation details query page size (max 100)

MEDIA_TYPE = "voice"     # hardcoded per requirements
LANGUAGE = "english"     # hardcoded per requirements


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
                    "(e.g. Analytics > Conversation Detail > View, "
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
# Queue / skill lookups
# --------------------------------------------------------------------------

def resolve_queue(client, queue_name):
    data = client.get(
        "/api/v2/routing/queues",
        params={"name": queue_name, "pageSize": 25},
    )
    entities = data.get("entities", [])
    if not entities:
        sys.exit(f"No queue found matching name '{queue_name}'.")
    # Prefer an exact (case-insensitive) match if there is one
    exact = [e for e in entities if e["name"].lower() == queue_name.lower()]
    match = exact[0] if exact else entities[0]
    if len(entities) > 1 and not exact:
        print(f"  multiple queues matched '{queue_name}', using '{match['name']}'")
    print(f"  resolved queue: {match['name']} ({match['id']})")
    return match["id"], match["name"]


def load_skill_names(client):
    """Returns {skillId: skillName} for all routing skills in the org."""
    skills = {}
    page = 1
    while True:
        data = client.get(
            "/api/v2/routing/skills", params={"pageSize": 500, "pageNumber": page}
        )
        for entity in data.get("entities", []):
            skills[entity["id"]] = entity["name"]
        if page >= data.get("pageCount", 1):
            break
        page += 1
    return skills


# --------------------------------------------------------------------------
# Conversation details fetch + processing
# --------------------------------------------------------------------------

def date_chunks(start, end, chunk_days):
    """Yield (chunk_start, chunk_end) datetime tuples covering [start, end)."""
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=chunk_days), end)
        yield cur, nxt
        cur = nxt


def fetch_conversations(client, queue_id, interval_start, interval_end):
    """Paginate through conversation details for one date chunk."""
    conversations = []
    cursor = None
    while True:
        body = {
            "interval": (
                f"{interval_start.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z/"
                f"{interval_end.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z"
            ),
            "order": "asc",
            "orderBy": "conversationStart",
            "paging": {"pageSize": PAGE_SIZE},
            "segmentFilters": [
                {
                    "type": "and",
                    "predicates": [{"dimension": "queueId", "value": queue_id}],
                }
            ],
        }
        if cursor:
            body["cursor"] = cursor
        data = client.post("/api/v2/analytics/conversations/details/query", json=body)
        conversations.extend(data.get("conversations", []))
        cursor = data.get("cursor")
        if not cursor:
            break
    return conversations


def floor_to_interval(dt_utc, tz_offset_minutes):
    """Floor a UTC datetime to the start of its N-minute local interval,
    returning a naive local datetime (for display in the output)."""
    local = dt_utc + timedelta(minutes=tz_offset_minutes)
    floored_minute = (local.minute // INTERVAL_MINUTES) * INTERVAL_MINUTES
    return local.replace(minute=floored_minute, second=0, microsecond=0)


def parse_ts(ts):
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def skill_set_label(skill_ids, skill_names):
    if not skill_ids:
        return "None"
    names = sorted(skill_names.get(sid, sid) for sid in skill_ids)
    return "|".join(names)


def process_conversation(conv, queue_id, skill_names, tz_offset_minutes, buckets):
    """
    Update `buckets` (a dict keyed on (interval_start, skill_label)) with
    Offered / Handled / TotalHandleTime contributions from this conversation.

    Segment-matching logic (see module docstring for validation guidance):
      - "Offered": the first segment with purpose == "queue" and
        queueId == our queue marks one offered interaction, bucketed at that
        segment's startTime.
      - "Handled": if the same conversation also has a segment with
        purpose == "user" tied to an agent participant whose associated
        queueId == our queue, it counts as handled, bucketed at the *queue*
        segment's startTime (i.e. when it was offered) -- change to the
        agent-connect time if your WFM process expects that instead.
      - "Total Handle Time": sum of segment durations with
        purpose in {"user", "hold"} (talk + hold) plus any purpose == "wrapup"
        segment durations, for agent participants tied to our queue, in
        seconds.
      - Skills: requestedRoutingSkillIds found on the customer/acd
        participant's session, if present.
    """
    skill_ids = set()
    offered_hit = None
    handled = False
    handle_time_seconds = 0

    for participant in conv.get("participants", []):
        purpose = participant.get("purpose")
        for session in participant.get("sessions", []):
            for rid in session.get("requestedRoutingSkillIds") or []:
                skill_ids.add(rid)

            for segment in session.get("segments", []):
                seg_purpose = segment.get("segmentType") or segment.get("purpose")
                seg_queue = segment.get("queueId")
                if seg_queue != queue_id:
                    continue

                start = segment.get("segmentStart") or segment.get("startTime")
                end = segment.get("segmentEnd") or segment.get("endTime")

                if seg_purpose == "queue" and offered_hit is None and start:
                    offered_hit = parse_ts(start)

                if purpose == "agent" and seg_purpose in ("user", "hold", "wrapup"):
                    handled = True
                    if start and end:
                        delta = (parse_ts(end) - parse_ts(start)).total_seconds()
                        handle_time_seconds += max(delta, 0)

    if offered_hit is None:
        # No matching "queue" segment for this queue in this conversation;
        # nothing to attribute here.
        return

    bucket_time = floor_to_interval(offered_hit, tz_offset_minutes)
    label = skill_set_label(skill_ids, skill_names)
    key = (bucket_time, label)

    row = buckets[key]
    row["offered"] += 1
    if handled:
        row["handled"] += 1
        row["handle_time"] += handle_time_seconds


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    print("=== WFM Historical Data Export ===\n")

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
        "Local timezone offset from UTC in minutes for bucketing "
        "(e.g. 60 for UTC+1, 0 for UTC) [default 0]: "
    ).strip()
    tz_offset_minutes = int(tz_input) if tz_input else 0

    print("\nLoading routing skill names...")
    skill_names = load_skill_names(client)
    print(f"  loaded {len(skill_names)} skills.\n")

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(weeks=weeks)

    buckets = defaultdict(lambda: {"offered": 0, "handled": 0, "handle_time": 0.0})

    print(f"Pulling conversation details from {start_dt.date()} to {end_dt.date()}...")
    for chunk_start, chunk_end in date_chunks(start_dt, end_dt, CHUNK_DAYS):
        print(f"  chunk {chunk_start.date()} -> {chunk_end.date()} ...")
        conversations = fetch_conversations(client, queue_id, chunk_start, chunk_end)
        print(f"    {len(conversations)} conversations returned")
        for conv in conversations:
            process_conversation(conv, queue_id, skill_names, tz_offset_minutes, buckets)

    if not buckets:
        print("\nNo data found for this queue/date range. No file written.")
        return

    out_path = f"historical_data_{queue_name.replace(' ', '_')}.csv"
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Interval Start Date",
                "Queue",
                "Media Type",
                "Skill Set",
                "Language",
                "Offered",
                "Interactions Handled",
                "Total Handle Time",
            ]
        )
        for (interval_start, skill_label), row in sorted(buckets.items()):
            writer.writerow(
                [
                    interval_start.strftime("%Y-%m-%d %H:%M"),
                    queue_name,
                    MEDIA_TYPE,
                    skill_label,
                    LANGUAGE,
                    row["offered"],
                    row["handled"],
                    int(round(row["handle_time"])),
                ]
            )

    print(f"\nDone. Wrote {len(buckets)} rows to {out_path}")
    print(
        "\nReminder: cross-check Offered/Handled/Handle Time totals against "
        "the Genesys aggregates query or Performance view for the same "
        "queue and date range before using this in a real forecast import "
        "-- see the notes at the top of this script."
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled.")
