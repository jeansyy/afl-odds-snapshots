# scripts/snapshot_afl_odds.py
#
# Captures two types of AFL data in one run:
#   1. Game-level markets (h2h, spreads, totals) for all upcoming AFL games
#   2. Player props (disposals O/U, disposals over, anytime goal scorer)
#      fetched per-event via the /events/{id}/odds endpoint
#
# Outputs two CSVs per run into --out directory:
#   afl_game_odds_<ts>.csv
#   afl_player_props_<ts>.csv
#
# Modes:
#   --label <name>     add a suffix to the timestamp (e.g. "daily")
#   --pregame-only     only snapshot if now is within a pre-game window

import argparse
import os
import sys
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = "https://api.the-odds-api.com/v4"
SPORT = "aussierules_afl"

# Game-level markets fetched in one bulk call
GAME_MARKETS = ["h2h", "spreads", "totals"]

# Player prop markets fetched per event
PLAYER_MARKETS = [
    "player_disposals",
    "player_disposals_over",
    "player_goal_scorer_anytime",
]

# Pre-game snapshot windows in minutes before kick-off
PREGAME_WINDOWS = [120, 60, 30, 15, 10, 5]
WINDOW_TOLERANCE_MINS = 2.5  # match if within +/- 2.5 min of a target window


def get(url, params):
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    remaining = resp.headers.get("x-requests-remaining", "?")
    used = resp.headers.get("x-requests-used", "?")
    print(f"  [{resp.status_code}] {url.split('/v4/')[-1]} | used={used} remaining={remaining}")
    return resp.json()


def is_within_pregame_window(commence_time_str, now_utc):
    """Return (True, window_mins) if now is within tolerance of a pre-game window."""
    commence = datetime.fromisoformat(commence_time_str.replace("Z", "+00:00"))
    mins_to_kick = (commence - now_utc).total_seconds() / 60
    for window in PREGAME_WINDOWS:
        if abs(mins_to_kick - window) <= WINDOW_TOLERANCE_MINS:
            return True, window
    return False, None


def snapshot_game_odds(api_key, ts, out_path, events):
    """Write game-level odds CSV for the given events list."""
    rows = []
    for event in events:
        eid = event.get("id")
        home = event.get("home_team")
        away = event.get("away_team")
        commence = event.get("commence_time")
        for bk in event.get("bookmakers", []):
            bk_key = bk.get("key")
            bk_title = bk.get("title")
            last_update = bk.get("last_update")
            for market in bk.get("markets", []):
                mkt = market.get("key")
                for outcome in market.get("outcomes", []):
                    rows.append({
                        "event_id": eid,
                        "home_team": home,
                        "away_team": away,
                        "commence_time": commence,
                        "bookmaker": bk_key,
                        "bookmaker_title": bk_title,
                        "last_update": last_update,
                        "market": mkt,
                        "outcome_name": outcome.get("name"),
                        "price": outcome.get("price"),
                        "point": outcome.get("point"),
                        "snapshot_utc": ts,
                    })
    csv_path = out_path / f"afl_game_odds_{ts}.csv"
    _write_csv(csv_path, rows, [
        "event_id", "home_team", "away_team", "commence_time",
        "bookmaker", "bookmaker_title", "last_update",
        "market", "outcome_name", "price", "point", "snapshot_utc",
    ])
    print(f"  -> Wrote {len(rows)} game-odds rows to {csv_path.name}")


def snapshot_player_props(api_key, ts, out_path, events):
    """Per-event fetch of player props."""
    rows = []
    for event in events:
        eid = event.get("id")
        home = event.get("home_team")
        away = event.get("away_team")
        commence = event.get("commence_time")
        print(f"  Fetching player props: {away} @ {home}")
        try:
            data = get(
                f"{BASE}/sports/{SPORT}/events/{eid}/odds",
                {
                    "apiKey": api_key,
                    "regions": "au",
                    "markets": ",".join(PLAYER_MARKETS),
                    "oddsFormat": "decimal",
                },
            )
        except requests.HTTPError as e:
            print(f"  Warning: {e} - skipping")
            time.sleep(1)
            continue
        for bk in data.get("bookmakers", []):
            bk_key = bk.get("key")
            bk_title = bk.get("title")
            last_update = bk.get("last_update")
            for market in bk.get("markets", []):
                mkt = market.get("key")
                for outcome in market.get("outcomes", []):
                    rows.append({
                        "event_id": eid,
                        "home_team": home,
                        "away_team": away,
                        "commence_time": commence,
                        "bookmaker": bk_key,
                        "bookmaker_title": bk_title,
                        "last_update": last_update,
                        "market": mkt,
                        "player": outcome.get("description", outcome.get("name")),
                        "outcome_name": outcome.get("name"),
                        "price": outcome.get("price"),
                        "point": outcome.get("point"),
                        "snapshot_utc": ts,
                    })
        time.sleep(0.5)  # be polite between per-event calls
    csv_path = out_path / f"afl_player_props_{ts}.csv"
    _write_csv(csv_path, rows, [
        "event_id", "home_team", "away_team", "commence_time",
        "bookmaker", "bookmaker_title", "last_update",
        "market", "player", "outcome_name", "price", "point", "snapshot_utc",
    ])
    print(f"  -> Wrote {len(rows)} player-prop rows to {csv_path.name}")


def _write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def fetch_all_events(api_key):
    """Fetch all upcoming AFL events with full game-level markets."""
    return get(
        f"{BASE}/sports/{SPORT}/odds",
        {
            "apiKey": api_key,
            "regions": "au",
            "markets": ",".join(GAME_MARKETS),
            "oddsFormat": "decimal",
        },
    )


def main(out_dir, pregame_only=False, label=None):
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        print("Missing ODDS_API_KEY env var", file=sys.stderr)
        sys.exit(1)

    now_utc = datetime.now(timezone.utc)
    ts = now_utc.strftime("%Y%m%d_%H%M%S")
    if label:
        ts = f"{ts}_{label}"

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    print("=== Fetching AFL events ===")
    all_events = fetch_all_events(api_key)

    if not all_events:
        print("No upcoming AFL events found - nothing to snapshot.")
        return

    if pregame_only:
        matched = []
        for event in all_events:
            hit, window = is_within_pregame_window(event["commence_time"], now_utc)
            if hit:
                game = f"{event['away_team']} @ {event['home_team']}"
                print(f"  Matched T-{window} min window: {game} (kicks off {event['commence_time']})")
                matched.append(event)
        if not matched:
            print("No games in a pre-game snapshot window right now - exiting.")
            return
        events_to_snapshot = matched
    else:
        events_to_snapshot = all_events

    print(f"\n=== Step 1: Game odds for {len(events_to_snapshot)} event(s) ===")
    snapshot_game_odds(api_key, ts, out_path, events_to_snapshot)

    print(f"\n=== Step 2: Player props for {len(events_to_snapshot)} event(s) ===")
    snapshot_player_props(api_key, ts, out_path, events_to_snapshot)

    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Snapshot AFL odds from The Odds API")
    parser.add_argument("--out", required=True, help="Output directory for CSV files")
    parser.add_argument("--pregame-only", action="store_true",
                        help="Only snapshot if within a pre-game time window")
    parser.add_argument("--label", default=None,
                        help="Optional label suffix added to timestamp (e.g. 'daily')")
    args = parser.parse_args()
    main(args.out, pregame_only=args.pregame_only, label=args.label)
