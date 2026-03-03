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

import os
import sys
import csv
import time
from datetime import datetime
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


def get(url, params):
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    remaining = resp.headers.get("x-requests-remaining", "?")
    used = resp.headers.get("x-requests-used", "?")
    print(f"  [{resp.status_code}] {url.split('/v4/')[-1]} | used={used} remaining={remaining}")
    return resp.json()


def snapshot_game_odds(api_key, ts, out_path):
    """Bulk fetch h2h / spreads / totals for all AFL events."""
    data = get(
        f"{BASE}/sports/{SPORT}/odds",
        {
            "apiKey": api_key,
            "regions": "au",
            "markets": ",".join(GAME_MARKETS),
            "oddsFormat": "decimal",
        },
    )

    rows = []
    for event in data:
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
    return data  # return events so we can reuse their IDs


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
            print(f"    Warning: {e} — skipping")
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


def main(out_dir):
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        print("Missing ODDS_API_KEY env var", file=sys.stderr)
        sys.exit(1)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    print("=== Step 1: Game odds (h2h / spreads / totals) ===")
    events = snapshot_game_odds(api_key, ts, out_path)

    if not events:
        print("No upcoming AFL events found — skipping player props.")
        return

    print(f"\n=== Step 2: Player props for {len(events)} events ===")
    snapshot_player_props(api_key, ts, out_path, events)

    print("\nDone.")


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--out":
        main(sys.argv[2])
    else:
        print("Usage: python snapshot_afl_odds.py --out <output_dir>", file=sys.stderr)
        sys.exit(1)
