#!/usr/bin/env python3
"""
ESPN Stats Scraper for Non-Sidearm Schools
============================================
Scrapes stats for D1 baseball schools that don't use the Sidearm platform.
Since there's no direct stats API, this scraper gets what IS accessible
via the ESPN College Baseball API:

  - Team record from ESPN API
  - Most recent completed game (date, opponent, result)
  - Player list from players.json

Per-player batting/pitching season stats are NOT available here.

Usage:
    python3 scrape_espn_stats.py                    # scrape all schools
    python3 scrape_espn_stats.py --slug lsu         # one school by slug
    python3 scrape_espn_stats.py --list             # show all schools
"""

import argparse
import json
import os
import re
import sys
import time

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

DELAY = 0.5
PLAYERS_JSON = os.path.join(os.path.dirname(__file__), "players.json")
STATS_DIR = os.path.join(os.path.dirname(__file__), "stats")
INDEX_PATH = os.path.join(STATS_DIR, "index.json")

# Non-Sidearm schools with players in players.json
# Format: (school_name, slug, espn_id)
ESPN_SCHOOLS = [
    ("Central Connecticut State University", "central-connecticut-state-university", 159),
    ("Florida Gulf Coast University", "florida-gulf-coast-university", 291),
    ("Gardner-Webb University", "gardner-webb-university", 356),
    ("Georgia Institute of Technology", "georgia-institute-of-technology", 77),
    ("Louisiana State University", "louisiana-state-university", 85),
    ("Tennessee Technological University", "tennessee-technological-university", 441),
    ("University of Arizona", "university-of-arizona", 60),
    ("University of Illinois Urbana-Champaign", "university-of-illinois-urbana-champaign", 153),
    ("University of Kentucky", "university-of-kentucky", 82),
    ("University of Miami (Florida)", "university-of-miami-florida", 176),
    ("University of Notre Dame", "university-of-notre-dame", 81),
    ("University of San Francisco", "university-of-san-francisco", 267),
    ("University of South Carolina, Columbia", "university-of-south-carolina-columbia", 193),
    ("Vanderbilt University", "vanderbilt-university", 120),
    ("Wofford College", "wofford-college", 208),
]

session = requests.Session()
session.headers.update(HEADERS)


def fetch_json(url: str, retries: int = 3) -> dict | list | None:
    """Fetch JSON from URL, return parsed data or None on failure."""
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"  Error fetching {url}: {e}", file=sys.stderr)
    return None


def get_espn_record(espn_id: int) -> str:
    """Get team record from ESPN API."""
    data = fetch_json(
        f"https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{espn_id}"
    )
    if not data:
        return ""
    record_items = data.get("team", {}).get("record", {}).get("items", [])
    for item in record_items:
        if item.get("type") == "total":
            return item.get("summary", "")
    return ""


def get_espn_schedule(espn_id: int) -> list[dict]:
    """Get team schedule from ESPN API, returning completed games sorted newest-first."""
    data = fetch_json(
        f"https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{espn_id}/schedule"
    )
    if not data:
        return []

    events = data.get("events", [])
    completed = []
    wins = 0
    losses = 0
    record_fallback = ""

    for ev in events:
        for comp in ev.get("competitions", []):
            if not comp.get("status", {}).get("type", {}).get("completed"):
                continue
            date_str = ev.get("date", "")[:10]  # YYYY-MM-DD

            # Skip non-2026 games
            if not date_str.startswith("2026"):
                continue

            our_score = opp_score = None
            opponent = "Unknown"
            is_winner = False

            for team in comp.get("competitors", []):
                team_id = team.get("team", {}).get("id", "")
                score_val = team.get("score", {})
                if isinstance(score_val, dict):
                    score_num = score_val.get("value")
                elif isinstance(score_val, (int, float)):
                    score_num = score_val
                else:
                    score_num = None

                if str(team_id) == str(espn_id):
                    our_score = score_num
                    is_winner = bool(team.get("winner"))
                else:
                    opp_score = score_num
                    opponent = team["team"].get("displayName", "Unknown")

            # Track wins/losses for fallback record
            if is_winner:
                wins += 1
            else:
                losses += 1

            result = ""
            if our_score is not None and opp_score is not None:
                if is_winner:
                    result = f"W {int(our_score)}-{int(opp_score)}"
                else:
                    result = f"L {int(our_score)}-{int(opp_score)}"

            completed.append({
                "date": date_str,
                "opponent": opponent,
                "result": result,
            })

    completed.sort(key=lambda g: g["date"], reverse=True)
    return completed, f"{wins}-{losses}" if completed else ""


def normalize_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z\s]", "", name)
    return re.sub(r"\s+", " ", name).strip()


def scrape_school(school_name: str, slug: str, espn_id: int) -> bool:
    """Scrape stats for one school via ESPN. Returns True on success."""
    print(f"\n{'='*60}")
    print(f"Scraping: {school_name}")
    print(f"  ESPN ID: {espn_id}")
    print(f"{'='*60}")

    # Load players for this school
    with open(PLAYERS_JSON) as f:
        all_players = json.load(f)
    school_players = [p for p in all_players if p.get("school") == school_name]
    print(f"  Players in players.json: {len(school_players)}")

    if not school_players:
        print("  SKIP: No players in players.json")
        return False

    # Get record
    print(f"  Fetching record from ESPN (id={espn_id})...")
    record = get_espn_record(espn_id)
    time.sleep(DELAY)

    # Get schedule
    print(f"  Fetching schedule from ESPN...")
    games, fallback_record = get_espn_schedule(espn_id)
    time.sleep(DELAY)

    if not record and fallback_record:
        record = fallback_record
        print(f"  Record (computed from schedule): {record}")
    elif record:
        print(f"  Record: {record}")
    else:
        print(f"  Record: (not found)")

    print(f"  2026 completed games from ESPN: {len(games)}")

    # Find most recent game
    most_recent = None
    if games:
        g = games[0]
        most_recent = {
            "date": g["date"],
            "opponent": g["opponent"],
            "result": g["result"],
        }
        print(f"  Most recent: {g['date']} vs {g['opponent']} — {g['result'] or '(no score)'}")

    if not most_recent and not record:
        print("  SKIP: No record and no games found")
        return False

    # Build player dict (keyed by player ID, same format as other stats files)
    player_entries = {}
    for p in school_players:
        pid = p.get("id", "")
        if pid:
            player_entries[pid] = {
                "id": pid,
                "name": p.get("name", ""),
                "fn": p.get("fn", ""),
                "ln": p.get("ln", ""),
                "num": p.get("num"),
                "pos": p.get("pos", ""),
                "yr": p.get("yr", ""),
                "school": school_name,
                "season_batting": None,
                "season_pitching": None,
                "last_game": None,
                "narrative": None,
            }

    # Build output
    out = {
        "school": school_name,
        "record": record,
        "most_recent_game": most_recent,
        "players": player_entries,
        "_note": (
            "Stats source: ESPN API only. This school does not use the Sidearm platform. "
            "Per-player batting/pitching season stats are not available."
        ),
    }

    out_path = os.path.join(STATS_DIR, f"{slug}.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)

    print(f"\n  Output: {out_path}")
    print(f"  Record: {record}")
    print(f"  Players: {len(player_entries)}")
    if most_recent:
        print(f"  Most recent game: {most_recent['date']}")

    update_index(slug)
    return True


def update_index(slug: str) -> None:
    """Add slug to stats/index.json if not already present."""
    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH) as f:
            idx = json.load(f)
    else:
        idx = []

    if slug not in idx:
        idx.append(slug)
        idx.sort()
        with open(INDEX_PATH, "w") as f:
            json.dump(idx, f, indent=2)
        print(f"\n  Added {slug} to index.json")


def main():
    parser = argparse.ArgumentParser(description="ESPN stats scraper for non-Sidearm schools")
    parser.add_argument("--slug", help="Scrape only this school slug")
    parser.add_argument("--list", action="store_true", help="List all schools")
    args = parser.parse_args()

    if args.list:
        for name, slug, espn_id in ESPN_SCHOOLS:
            print(f"{slug:55}  ESPN:{espn_id}")
        return

    schools = ESPN_SCHOOLS
    if args.slug:
        schools = [(n, s, e) for n, s, e in ESPN_SCHOOLS if s == args.slug]
        if not schools:
            print(f"Unknown slug: {args.slug}")
            sys.exit(1)

    successes = []
    failures = []

    for school_name, slug, espn_id in schools:
        ok = scrape_school(school_name, slug, espn_id)
        if ok:
            successes.append(slug)
        else:
            failures.append(slug)

    print(f"\n{'='*60}")
    print(f"ESPN SCRAPE COMPLETE")
    print(f"  Successes: {len(successes)}")
    print(f"  Failures: {len(failures)}")
    print(f"{'='*60}")
    if failures:
        print("Failed slugs:")
        for s in failures:
            print(f"  {s}")


if __name__ == "__main__":
    main()
