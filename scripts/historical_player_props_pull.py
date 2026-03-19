# scripts/historical_player_props_pull.py
#
# Pulls historical AFL player prop odds (disposals) from The Odds API
# Snapshots taken Wed/Thu/Fri at 19:00 AEST (09:00 UTC) for each AFL season
# Date range: 1 March -> 1 October, seasons 2023-2026
# Player props data available from 2023-05-03 onwards
#
# Cost: 10 credits per event (1 market x 1 region x 10)
# Budget cap: 15,000 credits
# Outputs: outputs/afl_snapshots/historical_player_props.csv
#
# Usage:
#   python scripts/historical_player_props_pull.py
#   ODDS_API_KEY=<key> python scripts/historical_player_props_pull.py

import csv
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
API_KEY = os.getenv("ODDS_API_KEY", "")
SPORT = "aussierules_afl"
REGION = "au"
MARKET = "player_disposals"
ODDS_FMT = "decimal"
CREDIT_CAP = 15_000
COST_PER_EVENT = 10  # 10 x 1 market x 1 region

# AFL season window per year: 1 March -> 1 October
SEASON_START_MMDD = (3, 1)
SEASON_END_MMDD = (10, 1)
# Earliest date player prop history exists
PROP_DATA_START = datetime(2023, 5, 3, tzinfo=timezone.utc)

# Snapshot days of week (0=Mon ... 6=Sun) and hour in UTC
# Wed=2, Thu=3, Fri=4 at 19:00 AEST = 09:00 UTC
SNAP_WEEKDAYS = {2, 3, 4}
SNAP_HOUR_UTC = 9

# Years to cover
YEARS = [2023, 2024, 2025, 2026]

OUTPUT_DIR = Path("outputs/afl_snapshots")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV = OUTPUT_DIR / "historical_player_props.csv"

BASE_URL = "https://api.the-odds-api.com/v4"
HIST_URL = "https://api.the-odds-api.com/v4/historical"

CSV_FIELDS = [
    "event_id", "home_team", "away_team", "commence_time",
    "snap_date", "snapshot_ts",
    "bookmaker", "bookmaker_title",
    "market", "player", "outcome_name", "price", "point",
]

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
credits_used = 0


def api_get(url, params, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 15))
                print(f"    [429] rate limited, sleeping {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 422:
                print(f"    [422] unprocessable - skipping")
                return None
            r.raise_for_status()
            remaining = r.headers.get("x-requests-remaining", "?")
            last = r.headers.get("x-requests-last", "?")
            print(f"    [{r.status_code}] cost={last} remaining={remaining}")
            return r
        except requests.RequestException as e:
            print(f"    [ERROR] attempt {attempt + 1}: {e}")
            time.sleep(5)
    return None


def generate_snap_dates(years):
    """Generate Wed/Thu/Fri 09:00 UTC snapshots within AFL season windows."""
    snaps = []
    now_utc = datetime.now(timezone.utc)
    for year in years:
        start = datetime(year, *SEASON_START_MMDD, SNAP_HOUR_UTC, tzinfo=timezone.utc)
        end = datetime(year, *SEASON_END_MMDD, SNAP_HOUR_UTC, tzinfo=timezone.utc)
        # Cap start to prop data availability
        start = max(start, PROP_DATA_START)
        cur = start
        while cur <= end and cur <= now_utc:
            if cur.weekday() in SNAP_WEEKDAYS:
                snaps.append(cur)
            cur += timedelta(days=1)
    return snaps


def fetch_events_at(snap_dt):
    """Get historical event list at snapshot datetime (costs 1 credit)."""
    global credits_used
    r = api_get(
        f"{HIST_URL}/sports/{SPORT}/events",
        {"apiKey": API_KEY, "date": snap_dt.strftime("%Y-%m-%dT%H:%M:%SZ")},
    )
    if r is None:
        return [], 0
    cost = int(r.headers.get("x-requests-last", 1))
    credits_used += cost
    data = r.json()
    return data.get("data", []), cost


def fetch_event_props(event_id, snap_dt):
    """Fetch historical player disposals for one event (costs COST_PER_EVENT)."""
    global credits_used
    r = api_get(
        f"{HIST_URL}/sports/{SPORT}/events/{event_id}/odds",
        {
            "apiKey": API_KEY,
            "regions": REGION,
            "markets": MARKET,
            "oddsFormat": ODDS_FMT,
            "dateFormat": "iso",
            "date": snap_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    if r is None:
        return None, 0
    cost = int(r.headers.get("x-requests-last", COST_PER_EVENT))
    credits_used += cost
    return r.json(), cost


def rows_from_event(event_data, snap_dt):
    """Flatten bookmaker/market/outcome data into CSV rows."""
    rows = []
    if not event_data:
        return rows
    snap_ts = snap_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    snap_date = snap_dt.strftime("%Y-%m-%d")
    eid = event_data.get("id")
    home = event_data.get("home_team")
    away = event_data.get("away_team")
    commence = event_data.get("commence_time")
    for bk in event_data.get("bookmakers", []):
        bk_key = bk.get("key")
        bk_title = bk.get("title")
        for mkt in bk.get("markets", []):
            mkt_key = mkt.get("key")
            for outcome in mkt.get("outcomes", []):
                rows.append({
                    "event_id": eid,
                    "home_team": home,
                    "away_team": away,
                    "commence_time": commence,
                    "snap_date": snap_date,
                    "snapshot_ts": snap_ts,
                    "bookmaker": bk_key,
                    "bookmaker_title": bk_title,
                    "market": mkt_key,
                    "player": outcome.get("description", outcome.get("name")),
                    "outcome_name": outcome.get("name"),
                    "price": outcome.get("price"),
                    "point": outcome.get("point"),
                })
    return rows


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    global credits_used

    if not API_KEY:
        print("ERROR: ODDS_API_KEY env var not set", file=sys.stderr)
        sys.exit(1)

    print(f"=== AFL Historical Player Props Pull ===")
    print(f"Market: {MARKET} | Region: {REGION} | Budget: {CREDIT_CAP} credits")
    print(f"Cost per event: {COST_PER_EVENT} credits\n")

    snap_dates = generate_snap_dates(YEARS)
    print(f"Generated {len(snap_dates)} snapshot dates (Wed/Thu/Fri, Mar-Oct, 2023-2026)")

    # Phase 1: Collect unique event IDs
    print("\n=== PHASE 1: Discovering events ===")
    seen_ids = {}
    for snap_dt in snap_dates:
        if credits_used >= CREDIT_CAP:
            print("Budget cap hit during discovery - stopping.")
            break
        events, cost = fetch_events_at(snap_dt)
        for ev in events:
            eid = ev["id"]
            if eid not in seen_ids:
                seen_ids[eid] = {
                    "id": eid,
                    "home_team": ev.get("home_team"),
                    "away_team": ev.get("away_team"),
                    "commence_time": ev.get("commence_time"),
                    "snap_dt": snap_dt,
                }
        print(f"  {snap_dt.strftime('%Y-%m-%d %a')} -> {len(events)} events | "
              f"unique: {len(seen_ids)} | credits used: {credits_used}")
        time.sleep(0.3)

    print(f"\nDiscovered {len(seen_ids)} unique AFL events")

    # Phase 2: Pull props per event
    print("\n=== PHASE 2: Pulling player disposals ===")
    budget_left = CREDIT_CAP - credits_used
    max_events = budget_left // COST_PER_EVENT
    print(f"Budget remaining: {budget_left} credits -> can pull ~{max_events} events")

    events_sorted = sorted(seen_ids.values(), key=lambda x: x["commence_time"])

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        total_rows = 0

        for i, ev in enumerate(events_sorted):
            if credits_used + COST_PER_EVENT > CREDIT_CAP:
                print(f"\n[BUDGET CAP] Stopping at {credits_used} credits used.")
                break

            eid = ev["id"]
            home = ev["home_team"]
            away = ev["away_team"]
            ct = ev["commence_time"]
            snap_dt = ev["snap_dt"]

            print(f"  [{i+1}/{len(events_sorted)}] {away} @ {home} ({ct[:10]}) ", end="")

            result, cost = fetch_event_props(eid, snap_dt)
            if result is None:
                print("SKIP")
                time.sleep(0.5)
                continue

            event_data = result.get("data")
            if not event_data:
                print(f"EMPTY (cost={cost})")
                time.sleep(0.5)
                continue

            rows = rows_from_event(event_data, snap_dt)
            writer.writerows(rows)
            total_rows += len(rows)
            print(f"-> {len(rows)} rows (cost={cost}, total_credits={credits_used})")
            time.sleep(0.5)

    print(f"\n=== DONE ===")
    print(f"Total credits used: {credits_used}")
    print(f"Total rows written: {total_rows}")
    print(f"Output: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
