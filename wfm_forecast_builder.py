#!/usr/bin/env python3
"""
wfm_forecast_builder.py

Takes the combined multi-queue export produced by
wfm_historical_export_15min.py (UTC "Interval Start Date" timestamps) and
builds a short-term WFM forecast import file for one or more queues /
planning groups, over a date range you choose.

Method
------
For each queue found in the input file:
  - You give it a Planning Group name (or press Enter to reuse the queue
    name as-is).
  - You give it a volume multiplier (e.g. 1.0 for like-for-like, 1.2, 1.5,
    2x, etc). This is applied to Offered and Interactions Handled only --
    Average Handle Time is never scaled by it.
  - A FLAT (equal-weight) average is taken across all historical weeks for
    each (weekday, local time-of-day) slot to get the forecast Offered
    figure for that slot.
  - Average Handle Time uses a VOLUME-WEIGHTED average
    (sum(Total Handle Time) / sum(Interactions Handled), not an average of
    per-interval averages), falling back to that queue's overall
    volume-weighted AHT if a slot never had a single handled interaction
    historically.
  - A forecast row is only generated for a (weekday, time-of-day) slot that
    actually appears somewhere in that queue's own history -- e.g. if a
    queue never operated on a Saturday historically, no Saturday rows are
    fabricated for it. Different queues can therefore end up with
    different operating patterns in the output, which is intentional.

Operating-hours cap and Sundays
--------------------------------
Regardless of what the historical data shows, the output is capped to a
fixed operating-hours window, expressed directly in UTC:
    Monday-Friday : 07:00-21:00 UTC
    Saturday      : 08:00-15:00 UTC
    Sunday        : 08:00-15:00 UTC
Any Monday-Saturday historical slot that would fall outside its day's
window is dropped. Sundays are handled differently: every Sunday in the
forecast range gets a full set of 15-minute rows across 08:00-15:00 UTC
for every planning group, with Offered, Interactions Handled and Average
Handle Time all set to 0 -- i.e. Sundays are deliberately left blank for
now, not derived from history.

Time zone handling
-------------------
Both the input and output files are pure UTC -- there is no time zone
prompt. Internally, this script still converts UTC to Europe/London
(hardcoded) purely to build sensible (weekday, local time-of-day) pattern
keys and to project the forecast onto the correct UTC instant for each
slot across the British Summer Time <-> GMT clock change -- otherwise a
slot's UTC offset would be wrong on one side of the DST boundary. This is
invisible bucketing/projection logic only; nothing about time zones needs
to be entered or considered by you.

Input file location
--------------------
Just give the file name (e.g. 15_Min_Intervals_2026-06-29_to_2026-09-25.csv)
-- it's expected to be in the same folder as this script.

Output
------
CSV with columns matching the standard forecast import template exactly:
    "Interval Start UTC Date","Planning Group","Offered","Average Handle Time"
sorted by UTC timestamp, then Planning Group.

Usage
-----
    python3 wfm_forecast_builder.py
"""

import csv
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, date as date_cls
from zoneinfo import ZoneInfo

DEFAULT_WEEKS = 6

# Used only internally, to bucket UTC timestamps into sensible (weekday,
# local time-of-day) pattern keys and to project the forecast back onto
# the correct UTC instant either side of the BST/GMT clock change. Not
# asked about or exposed anywhere -- input and output are both pure UTC.
INTERNAL_TZ_NAME = "Europe/London"
INTERNAL_TZ = ZoneInfo(INTERNAL_TZ_NAME)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Operating hours the output is capped to, expressed directly in UTC clock
# time (half-open: start included, end excluded), per weekday. Any
# generated slot outside its day's window is dropped.
OPERATING_WINDOWS_UTC = {
    "Monday": ("07:00", "21:00"),
    "Tuesday": ("07:00", "21:00"),
    "Wednesday": ("07:00", "21:00"),
    "Thursday": ("07:00", "21:00"),
    "Friday": ("07:00", "21:00"),
    "Saturday": ("08:00", "15:00"),
    "Sunday": ("08:00", "15:00"),
}
SUNDAY_INTERVAL_MINUTES = 15  # Sundays are zero-filled at this granularity

MONTH_NAMES = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


# --------------------------------------------------------------------------
# Input loading
# --------------------------------------------------------------------------

def parse_utc_timestamp(value):
    """Parses the export file's UTC 'Interval Start Date' values. Accepts
    full ISO8601 ('2026-09-25T13:00:00.000Z' or with an explicit offset)
    as produced by the current export script, and also falls back to the
    older 'YYYY-MM-DD HH:MM' local-naive format in case an older export
    file is used, treating that fallback as already being in the chosen
    local time zone (handled by the caller)."""
    value = value.strip()
    try:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None  # caller decides how to handle a non-ISO value


def load_input_file(path, tz):
    """Reads the combined export CSV. Returns dict: queue_name -> list of
    (weekday_name, local_time_str 'HH:MM', offered, handled, handle_time)."""
    by_queue = defaultdict(list)
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {"Interval Start Date", "Queue", "Offered",
                    "Interactions Handled", "Total Handle Time"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            sys.exit(
                f"Input file is missing expected column(s): {', '.join(sorted(missing))}\n"
                f"Found columns: {reader.fieldnames}"
            )
        row_count = 0
        unparsed = 0
        for r in reader:
            row_count += 1
            raw_ts = r["Interval Start Date"]
            dt = parse_utc_timestamp(raw_ts)
            if dt is None:
                # Fallback: older local-naive export format.
                try:
                    dt_naive = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M")
                    dt_local = dt_naive.replace(tzinfo=tz)
                except ValueError:
                    unparsed += 1
                    continue
            elif dt.tzinfo is None:
                dt_local = dt.replace(tzinfo=tz)
            else:
                dt_local = dt.astimezone(tz)

            queue_name = r["Queue"]
            weekday = dt_local.strftime("%A")
            time_str = dt_local.strftime("%H:%M")
            offered = int(float(r["Offered"]))
            handled = int(float(r["Interactions Handled"]))
            handle_time = int(float(r["Total Handle Time"]))
            by_queue[queue_name].append((weekday, time_str, offered, handled, handle_time))

        if unparsed:
            print(f"  warning: {unparsed} of {row_count} row(s) had an unparseable "
                  f"'Interval Start Date' value and were skipped.")

    if not by_queue:
        sys.exit("No usable rows found in the input file.")
    return by_queue


# --------------------------------------------------------------------------
# Pattern building (flat average for Offered, volume-weighted for AHT)
# --------------------------------------------------------------------------

def build_pattern(rows):
    """Aggregate historical rows into a per-(weekday, time) forecast
    pattern. Returns dict: (weekday, time_str) -> {"offered": float, "aht": float}."""
    buckets = defaultdict(lambda: {"offered_sum": 0, "offered_n": 0, "handled_sum": 0, "handle_time_sum": 0})
    overall_handled_sum = 0
    overall_handle_time_sum = 0

    for weekday, time_str, offered, handled, handle_time in rows:
        b = buckets[(weekday, time_str)]
        b["offered_sum"] += offered
        b["offered_n"] += 1
        b["handled_sum"] += handled
        b["handle_time_sum"] += handle_time
        overall_handled_sum += handled
        overall_handle_time_sum += handle_time

    overall_aht = (overall_handle_time_sum / overall_handled_sum) if overall_handled_sum > 0 else 0.0

    pattern = {}
    for key, b in buckets.items():
        mean_offered = b["offered_sum"] / b["offered_n"]  # flat average
        if b["handled_sum"] > 0:
            aht = b["handle_time_sum"] / b["handled_sum"]  # volume-weighted
        else:
            aht = overall_aht
        pattern[key] = {"offered": mean_offered, "aht": aht}
    return pattern


# --------------------------------------------------------------------------
# Forecast projection
# --------------------------------------------------------------------------

def forecast_dates(start, weeks):
    """Yield every calendar date across `weeks` weeks starting on `start`.
    Which of these actually get a row depends on the queue's own detected
    historical weekdays -- no day-of-week assumption is made here."""
    for day_offset in range(weeks * 7):
        yield start + timedelta(days=day_offset)


def to_utc_iso(local_date, time_str, tz):
    hour, minute = map(int, time_str.split(":"))
    local_naive = datetime(local_date.year, local_date.month, local_date.day, hour, minute)
    local_aware = local_naive.replace(tzinfo=tz)
    utc_dt = local_aware.astimezone(ZoneInfo("UTC"))
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def in_operating_window(weekday, utc_time_str):
    """weekday: 'Monday'..'Sunday'. utc_time_str: 'HH:MM' in UTC. Half-open
    [start, end) comparison works directly on zero-padded 'HH:MM' strings."""
    window = OPERATING_WINDOWS_UTC.get(weekday)
    if not window:
        return False
    start_str, end_str = window
    return start_str <= utc_time_str < end_str


def sunday_utc_slot_times():
    """All 'HH:MM' slot start times (UTC) within the Sunday window, at
    SUNDAY_INTERVAL_MINUTES spacing."""
    start_str, end_str = OPERATING_WINDOWS_UTC["Sunday"]
    start_h, start_m = map(int, start_str.split(":"))
    end_h, end_m = map(int, end_str.split(":"))
    start_total = start_h * 60 + start_m
    end_total = end_h * 60 + end_m
    times = []
    t = start_total
    while t < end_total:
        h, m = divmod(t, 60)
        times.append(f"{h:02d}:{m:02d}")
        t += SUNDAY_INTERVAL_MINUTES
    return times


# --------------------------------------------------------------------------
# Prompts
# --------------------------------------------------------------------------

def prompt_input_filename():
    """Asks for just a file name (no path) and resolves it against the
    folder this script itself is running from."""
    while True:
        name = input(
            "Historical export CSV file name (must be in the same folder "
            "as this script): "
        ).strip().strip('"')
        if not name:
            print("  please enter a file name.")
            continue
        full_path = os.path.join(SCRIPT_DIR, name)
        if not os.path.isfile(full_path):
            print(f"  couldn't find '{name}' in {SCRIPT_DIR} -- check the "
                  "file is there and the name (including .csv) is exact.")
            continue
        return full_path


def parse_flexible_date(text):
    """Accepts 'YYYY-MM-DD', 'DD/MM/YYYY', or loose forms like
    '5th of October 2026' / '5 October 2026' / 'October 5 2026'."""
    text = text.strip()

    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        pass
    try:
        return datetime.strptime(text, "%d/%m/%Y").date()
    except ValueError:
        pass

    cleaned = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", text, flags=re.IGNORECASE)
    cleaned = cleaned.replace(",", " ")
    cleaned = re.sub(r"\bof\b", " ", cleaned, flags=re.IGNORECASE)
    tokens = [t for t in re.split(r"\s+", cleaned.strip()) if t]

    day = month = year = None
    for tok in tokens:
        low = tok.lower()
        if low in MONTH_NAMES:
            month = MONTH_NAMES[low]
        elif re.fullmatch(r"\d{4}", tok):
            year = int(tok)
        elif re.fullmatch(r"\d{1,2}", tok):
            day = int(tok)

    if day and month and year:
        try:
            return date_cls(year, month, day)
        except ValueError:
            pass
    return None


def prompt_start_date():
    while True:
        text = input(
            "Forecast start date (e.g. 2026-10-05 or '5th of October 2026'): "
        ).strip()
        d = parse_flexible_date(text)
        if d:
            return d
        print("  couldn't understand that date -- try e.g. 2026-10-05 or '5 October 2026'.")


def prompt_weeks():
    while True:
        text = input(f"Number of weeks to forecast [default {DEFAULT_WEEKS}]: ").strip()
        if not text:
            return DEFAULT_WEEKS
        try:
            weeks = int(text)
            if weeks >= 1:
                return weeks
        except ValueError:
            pass
        print("  please enter a whole number of weeks (or press Enter for the default).")


def prompt_planning_group(queue_name):
    text = input(
        f"Planning group name for '{queue_name}' "
        f"[press Enter to use '{queue_name}']: "
    ).strip()
    return text or queue_name


def prompt_multiplier(planning_group):
    while True:
        text = input(
            f"Volume multiplier for '{planning_group}' -- applies to Offered/Handled "
            f"only, not Average Handle Time (e.g. 1.0 for like-for-like, 1.2, 1.5x, 2x) "
            f"[default 1.0]: "
        ).strip()
        if not text:
            return 1.0
        cleaned = text.rstrip("xX").strip()
        try:
            value = float(cleaned)
            if value >= 0:
                return value
        except ValueError:
            pass
        print("  please enter a number, optionally with a trailing 'x' (e.g. 1.5 or 1.5x).")


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    print("=== WFM Forecast Builder (flat average, per-queue multiplier) ===\n")

    input_path = prompt_input_filename()

    print(f"\nReading '{os.path.basename(input_path)}'...")
    by_queue = load_input_file(input_path, INTERNAL_TZ)
    queue_names = sorted(by_queue.keys())
    print(f"  found {len(queue_names)} queue(s): {', '.join(queue_names)}")

    start_date = prompt_start_date()
    weeks = prompt_weeks()
    print(f"\nForecast will run from {start_date.isoformat()} for {weeks} week(s) "
          f"({weeks * 7} calendar days), all times in UTC.\n")

    queue_configs = []  # (queue_name, planning_group, multiplier)
    for queue_name in queue_names:
        planning_group = prompt_planning_group(queue_name)
        multiplier = prompt_multiplier(planning_group)
        queue_configs.append((queue_name, planning_group, multiplier))
        print()

    all_output_rows = []  # (utc_iso, planning_group, offered, aht)

    for queue_name, planning_group, multiplier in queue_configs:
        rows = by_queue[queue_name]
        pattern = build_pattern(rows)

        # Group pattern keys by weekday so we only touch weekdays that
        # actually occurred in this queue's history.
        weekdays_present = sorted({k[0] for k in pattern})

        n_rows_for_group = 0
        n_sunday_zero_rows = 0
        n_dropped_outside_window = 0
        for d in forecast_dates(start_date, weeks):
            weekday = d.strftime("%A")

            # Sundays are always zero-filled at the fixed 08:00-15:00 UTC
            # window, regardless of what the queue's own history shows.
            if weekday == "Sunday":
                for time_str in sunday_utc_slot_times():
                    utc_iso = f"{d.isoformat()}T{time_str}:00.000Z"
                    all_output_rows.append((utc_iso, planning_group, 0, 0))
                    n_rows_for_group += 1
                    n_sunday_zero_rows += 1
                continue

            if weekday not in weekdays_present:
                continue
            matching_keys = [k for k in pattern if k[0] == weekday]
            for (wd, time_str) in matching_keys:
                utc_iso = to_utc_iso(d, time_str, INTERNAL_TZ)
                utc_time_str = utc_iso[11:16]  # 'HH:MM' from the ISO string
                if not in_operating_window(weekday, utc_time_str):
                    n_dropped_outside_window += 1
                    continue
                stats = pattern[(wd, time_str)]
                offered_forecast = max(0, round(stats["offered"] * multiplier))
                aht_forecast = max(0, round(stats["aht"]))  # never scaled by multiplier
                all_output_rows.append((utc_iso, planning_group, offered_forecast, aht_forecast))
                n_rows_for_group += 1

        print(f"{planning_group} (queue '{queue_name}'): {n_rows_for_group} forecast rows "
              f"generated from {len(pattern)} distinct historical (weekday, time) slots "
              f"across historical weekdays: {', '.join(weekdays_present)}; "
              f"multiplier x{multiplier:g} "
              f"({n_sunday_zero_rows} Sunday zero-filled rows, "
              f"{n_dropped_outside_window} historical slots dropped for falling "
              f"outside the operating-hours window)")

    if not all_output_rows:
        sys.exit("\nNo forecast rows were generated -- nothing to write.")

    all_output_rows.sort(key=lambda r: (r[0], r[1]))

    start_str = all_output_rows[0][0][:10]
    end_str = all_output_rows[-1][0][:10]
    out_path = f"forecast_{start_str}_to_{end_str}.csv"

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["Interval Start UTC Date", "Planning Group", "Offered", "Average Handle Time"])
        for utc_iso, group, offered, aht in all_output_rows:
            writer.writerow([utc_iso, group, str(offered), str(aht)])

    print(f"\nDone. Wrote {len(all_output_rows)} total rows to {out_path}")
    print(
        "\nBefore importing: spot-check a couple of rows against the source "
        "historical data (accounting for the multiplier applied), and confirm "
        "the forecast start/end dates fall within your WFM business unit's "
        "configured planning period."
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled.")
