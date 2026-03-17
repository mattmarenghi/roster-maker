#!/usr/bin/env python3
"""
WMT Sidearm Stats Scraper
==========================
Scrapes stats for schools using the new WMT-based Sidearm platform.
These schools use a client-side SPA for stats (wmt.games) that can't be
scraped without a browser. This scraper gets what IS accessible:

  - Team record from ESPN API
  - Most recent game info from website-api schedule events
  - Player IDs from players.json

Per-player batting/pitching season stats are NOT available without a browser.

Usage:
    python3 scrape_wmt_stats.py                    # scrape all WMT schools
    python3 scrape_wmt_stats.py --slug purdue-university  # one school
    python3 scrape_wmt_stats.py --list             # show all WMT schools
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime

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

# Schools using WMT-based Sidearm platform (website-api available, no HTML stats tables)
# Format: (school_name, base_url, slug, espn_id, baseball_schedule_id_or_None)
WMT_SCHOOLS = [
    ("Auburn University", "https://auburntigers.com", "auburn-university", 55, None),
    ("Boise State University", "https://www.broncosports.com", "boise-state-university", 1139, None),
    ("Boston University", "https://goterriers.com", "boston-university", 323, None),
    ("Brigham Young University", "https://byucougars.com", "brigham-young-university", 127, None),
    ("Clemson University", "https://clemsontigers.com", "clemson-university", 117, None),
    ("Old Dominion University", "https://odusports.com", "old-dominion-university", 140, None),
    ("Pennsylvania State University", "https://gopsusports.com", "pennsylvania-state-university", 414, None),
    ("Purdue University", "https://purduesports.com", "purdue-university", 189, None),
    ("San Diego State University", "https://goaztecs.com", "san-diego-state-university", 62, None),
    ("San Jose State University", "https://sjsuspartans.com", "san-jose-state-university", None, None),
    ("Seattle University", "https://goseattleu.com", "seattle-university", 428, None),
    ("Stanford University", "https://gostanford.com", "stanford-university", 64, None),
    ("Temple University", "https://www.owlsports.com", "temple-university", 114, None),
    ("University of Central Florida", "https://ucfknights.com", "university-of-central-florida", 160, None),
    ("University of Cincinnati", "https://gobearcats.com", "university-of-cincinnati", 161, None),
    ("University of Iowa", "https://hawkeyesports.com", "university-of-iowa", 167, None),
    ("University of Nebraska-Lincoln", "https://huskers.com", "university-of-nebraska-lincoln", 99, None),
    ("University of New Mexico", "https://golobos.com", "university-of-new-mexico", 104, None),
    ("University of Texas at San Antonio", "https://goutsa.com", "university-of-texas-at-san-antonio", 297, None),
    ("University of Virginia", "https://virginiasports.com", "university-of-virginia", 131, None),
    ("Virginia Polytechnic Institute and State University", "https://hokiesports.com",
     "virginia-polytechnic-institute-and-state-university", 132, None),
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
    if not espn_id:
        return ""
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
    if not espn_id:
        return []
    data = fetch_json(
        f"https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{espn_id}/schedule"
    )
    if not data:
        return []

    events = data.get("events", [])
    completed = []
    for ev in events:
        for comp in ev.get("competitions", []):
            if not comp.get("status", {}).get("type", {}).get("completed"):
                continue
            date_str = ev.get("date", "")[:10]  # YYYY-MM-DD
            # Find our team and opponent
            our_score = opp_score = None
            opponent = "Unknown"
            for team in comp.get("competitors", []):
                if team.get("homeAway") == "home":
                    our_school_loc = team["team"].get("location", "")
                else:
                    opponent = team["team"].get("displayName", "Unknown")
                score_val = team.get("score", {})
                if isinstance(score_val, dict):
                    score_num = score_val.get("value")
                elif isinstance(score_val, (int, float)):
                    score_num = score_val
                else:
                    score_num = None

                if team.get("homeAway") == "home":
                    our_score = score_num
                else:
                    opp_score = score_num
                    opponent = team["team"].get("displayName", "Unknown")

            result = ""
            if our_score is not None and opp_score is not None:
                if our_score > opp_score:
                    result = f"W {int(our_score)}-{int(opp_score)}"
                elif opp_score > our_score:
                    result = f"L {int(our_score)}-{int(opp_score)}"
                else:
                    result = f"T {int(our_score)}-{int(opp_score)}"

            # Get game links
            links = ev.get("links", [])
            recap_url = None
            for link in links:
                href = link.get("href", "")
                if "recap" in href or "story" in href:
                    recap_url = href
                    break

            completed.append({
                "date": date_str,
                "opponent": opponent,
                "result": result,
                "boxscore_url": None,
                "recap_url": recap_url,
            })

    completed.sort(key=lambda g: g["date"], reverse=True)
    return completed


def find_baseball_schedule_id(base_url: str) -> int | None:
    """Find the schedule ID for baseball via website-api."""
    data = fetch_json(f"{base_url}/website-api/sports?per_page=50")
    if not data:
        return None
    for sport in data.get("data", []):
        if sport.get("slug") == "baseball":
            return sport.get("schedule_id")
    return None


def get_website_api_games(base_url: str, schedule_id: int) -> list[dict]:
    """Get completed games from website-api schedule events, newest-first."""
    data = fetch_json(
        f"{base_url}/website-api/schedule-events"
        f"?filter[schedule_id]={schedule_id}&per_page=100&sort=-datetime"
    )
    if not data:
        return []

    events = data.get("data", [])
    completed = []
    for e in events:
        if e.get("status") != "completed":
            continue
        date_str = e.get("datetime", "")[:10]
        if not date_str.startswith("2026"):
            continue  # Only current season
        opponent = e.get("opponent_name", "Unknown")
        boxscore_url = e.get("box_score_url")
        article_id = e.get("post_event_article_id")
        recap_url = f"{base_url}/article/{article_id}" if article_id else None

        completed.append({
            "date": date_str,
            "opponent": opponent,
            "result": "",  # No score in the event data
            "boxscore_url": boxscore_url,
            "recap_url": recap_url,
            "wmt_stats2_game_id": e.get("wmt_stats2_game_id"),
        })

    return completed  # already sorted newest-first from API


def compute_record_from_boxscores(base_url: str, games: list[dict]) -> str:
    """Compute W-L record by parsing each game's boxscore page."""
    wins = losses = 0
    for g in games:
        bsurl = g.get("boxscore_url")
        if not bsurl:
            continue
        result = get_boxscore_result(base_url, bsurl)
        if result.startswith("W"):
            wins += 1
        elif result.startswith("L"):
            losses += 1
        time.sleep(0.2)
    if wins + losses == 0:
        return ""
    return f"{wins}-{losses}"


def get_boxscore_result(base_url: str, boxscore_url: str) -> str:
    """Extract game result from a WMT Sidearm boxscore page NUXT data."""
    if not boxscore_url:
        return ""
    try:
        resp = session.get(boxscore_url, timeout=12)
        if resp.status_code != 200:
            return ""
        html = resp.text
        match = re.search(
            r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
        )
        if not match:
            return ""
        data = json.loads(match.group(1).strip())

        # The result dict has structure: {result: idx, winning_score: idx, losing_score: idx}
        for item in data:
            if not isinstance(item, str):
                continue
            try:
                parsed = json.loads(item.replace("'", '"'))
            except Exception:
                continue
            # Look for dict with winning_score key
            if isinstance(parsed, dict) and "winning_score" in parsed and "result" in parsed:
                result_idx = parsed["result"]
                win_idx = parsed["winning_score"]
                lose_idx = parsed["losing_score"]
                if all(isinstance(i, int) and i < len(data) for i in [result_idx, win_idx, lose_idx]):
                    result_val = data[result_idx]
                    win_score = float(data[win_idx] or 0)
                    lose_score = float(data[lose_idx] or 0)
                    wl = "W" if result_val == "win" else "L"
                    return f"{wl} {int(win_score)}-{int(lose_score)}"

        # Alternative: find result dict directly in NUXT
        for i, item in enumerate(data):
            if isinstance(item, dict):
                r_idx = item.get("result")
                w_idx = item.get("winning_score")
                l_idx = item.get("losing_score")
                if r_idx is not None and w_idx is not None and l_idx is not None:
                    r_val = data[r_idx] if isinstance(r_idx, int) and r_idx < len(data) else r_idx
                    w_val = data[w_idx] if isinstance(w_idx, int) and w_idx < len(data) else w_idx
                    l_val = data[l_idx] if isinstance(l_idx, int) and l_idx < len(data) else l_idx
                    if r_val in ("win", "loss") and w_val is not None:
                        wl = "W" if r_val == "win" else "L"
                        try:
                            return f"{wl} {int(float(w_val))}-{int(float(l_val))}"
                        except (ValueError, TypeError):
                            pass
    except Exception as e:
        print(f"  Boxscore parse error: {e}", file=sys.stderr)
    return ""


def normalize_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z\s]", "", name)
    return re.sub(r"\s+", " ", name).strip()


def scrape_school(school_name: str, base_url: str, slug: str, espn_id: int | None) -> bool:
    """Scrape stats for one WMT school. Returns True on success."""
    print(f"\n{'='*60}")
    print(f"Scraping: {school_name}")
    print(f"  Base URL: {base_url}")
    print(f"  ESPN ID: {espn_id or 'N/A'}")
    print(f"{'='*60}")

    # Load players for this school
    with open(PLAYERS_JSON) as f:
        all_players = json.load(f)
    school_players = [p for p in all_players if p.get("school") == school_name]
    print(f"  Players in players.json: {len(school_players)}")

    if not school_players:
        print(f"  SKIP: No players found for {school_name}")
        return False

    # Get record from ESPN
    record = ""
    if espn_id:
        print(f"  Fetching record from ESPN (id={espn_id})...")
        record = get_espn_record(espn_id)
        time.sleep(DELAY)
    print(f"  Record: {record or '(not found)'}")

    # Try to get recent games
    most_recent = None
    games = []

    # First try website-api (WMT schools)
    has_website_api = True
    schedule_id = find_baseball_schedule_id(base_url)
    time.sleep(DELAY)
    if schedule_id:
        print(f"  Baseball schedule_id: {schedule_id}")
        games = get_website_api_games(base_url, schedule_id)
        time.sleep(DELAY)
        print(f"  2026 completed games: {len(games)}")
    else:
        has_website_api = False
        print(f"  website-api not available, trying ESPN schedule...")

    # Fallback to ESPN schedule
    if not games and espn_id:
        espn_games = get_espn_schedule(espn_id)
        time.sleep(DELAY)
        # Filter to 2026 spring season
        games = [g for g in espn_games if g["date"].startswith("2026-0")]
        print(f"  2026 games from ESPN: {len(games)}")

    if games:
        most_recent = games[0]
        print(f"  Most recent: {most_recent['date']} vs {most_recent['opponent']}")

        # Try to get game result if not available
        if not most_recent.get("result") and most_recent.get("boxscore_url") and has_website_api:
            print(f"  Fetching game result from boxscore...")
            result = get_boxscore_result(base_url, most_recent["boxscore_url"])
            if result:
                most_recent["result"] = result
                print(f"  Result: {result}")
            time.sleep(DELAY)

    if most_recent is None:
        print(f"  No game data found — will create minimal stats file")
        # Create a minimal file with just record and player list
        # We need at least some data to be useful
        if not record:
            print(f"  SKIP: No record and no games found")
            return False

    # If we still don't have a record, compute it from boxscores
    if not record and games and has_website_api:
        print(f"  Computing record from {len(games)} boxscore pages...")
        record = compute_record_from_boxscores(base_url, games)
        if record:
            print(f"  Record (from boxscores): {record}")

    # Also try ESPN schedule for result if website-api gave no result
    if most_recent and not most_recent.get("result") and espn_id:
        espn_games = get_espn_schedule(espn_id)
        time.sleep(DELAY)
        espn_2026 = [g for g in espn_games if g["date"].startswith("2026-0")]
        if espn_2026:
            recent = espn_2026[0]
            if recent.get("result"):
                most_recent["result"] = recent["result"]
                print(f"  Result from ESPN: {recent['result']}")

    # Build player records (no stats, just IDs and basic info)
    players_out = {}
    for p in school_players:
        player_record = {
            "id": p["id"],
            "name": p.get("name", f"{p.get('fn', '')} {p.get('ln', '')}".strip()),
            "fn": p.get("fn", ""),
            "ln": p.get("ln", ""),
            "pos": p.get("pos", ""),
            "yr": p.get("yr", ""),
            "num": p.get("num"),
            "school": school_name,
            "season_batting": None,
            "season_pitching": None,
            "last_game": None,
            "narrative": None,
        }
        players_out[p["id"]] = player_record

    # Build output
    if most_recent:
        most_recent_game = {
            "date": most_recent["date"],
            "opponent": most_recent["opponent"],
            "result": most_recent.get("result", ""),
            "boxscore_url": most_recent.get("boxscore_url"),
            "recap_url": most_recent.get("recap_url"),
            "recap_headline": "",
            "recap_text": "",
        }
    else:
        most_recent_game = None

    output = {
        "school": school_name,
        "base_url": base_url,
        "season": "2026",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "record": record,
        "most_recent_game": most_recent_game,
        "players": players_out,
        "_note": "WMT Sidearm platform: season batting/pitching stats not available without browser",
    }

    # Write file
    os.makedirs(STATS_DIR, exist_ok=True)
    out_path = os.path.join(STATS_DIR, f"{slug}.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n  Output: {out_path}")
    print(f"  Record: {record}")
    print(f"  Players: {len(players_out)}")
    print(f"  Most recent game: {most_recent['date'] if most_recent else 'none'}")

    return True


def update_index(slug: str):
    """Add slug to stats/index.json."""
    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH) as f:
            index = set(json.load(f))
    else:
        index = set()
    index.add(slug)
    with open(INDEX_PATH, "w") as f:
        json.dump(sorted(index), f, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Scrape WMT Sidearm school stats")
    parser.add_argument("--slug", help="Scrape a single school by slug")
    parser.add_argument("--list", action="store_true", help="List all WMT schools")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip schools already in index.json")
    args = parser.parse_args()

    if args.list:
        print("WMT Sidearm schools:")
        for school, base, slug, espn_id, _ in WMT_SCHOOLS:
            print(f"  {slug}: {school}")
        return

    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH) as f:
            already_done = set(json.load(f))
    else:
        already_done = set()

    if args.slug:
        # Single school
        school_info = next((s for s in WMT_SCHOOLS if s[2] == args.slug), None)
        if not school_info:
            print(f"Unknown slug: {args.slug}", file=sys.stderr)
            sys.exit(1)
        name, base, slug, espn_id, _ = school_info
        success = scrape_school(name, base, slug, espn_id)
        if success:
            update_index(slug)
            print(f"\nAdded {slug} to index.json")
        return

    # Scrape all WMT schools
    successes = []
    failures = []

    for name, base, slug, espn_id, _ in WMT_SCHOOLS:
        if args.skip_existing and slug in already_done:
            print(f"  SKIP (already done): {name}")
            continue

        try:
            success = scrape_school(name, base, slug, espn_id)
            if success:
                successes.append(slug)
                update_index(slug)
                print(f"\n  Added {slug} to index.json")
            else:
                failures.append(slug)
        except Exception as e:
            print(f"  ERROR scraping {name}: {e}", file=sys.stderr)
            failures.append(slug)

        time.sleep(DELAY)

    print(f"\n{'='*60}")
    print(f"WMT SCRAPE COMPLETE")
    print(f"  Successes: {len(successes)}")
    print(f"  Failures: {len(failures)}")
    print(f"{'='*60}")
    if failures:
        print("Failed slugs:")
        for slug in failures:
            print(f"  {slug}")


if __name__ == "__main__":
    main()
