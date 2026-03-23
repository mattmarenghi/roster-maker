#!/usr/bin/env python3
"""
NCAA stats.ncaa.org Game Log Scraper
======================================
Fallback game log source for non-SIDEARM schools (the 15 ESPN schools that
lack per-player data from their athletic sites).

stats.ncaa.org is the NCAA's own data portal and covers every D1 program.
We use it only as a fallback — it's slower and requires an extra ID-discovery
step that SIDEARM avoids via its direct API.

Key URL patterns (from reverse-engineering / baseballr package docs):
  Team stats page:    https://stats.ncaa.org/team/{org_id}/stats?id={year_id}
  Player game log:    https://stats.ncaa.org/player/index?id={year_id}&stats_player_seq={player_seq}

The year_id is a season-specific integer assigned by the NCAA system.
It changes every year and must be discovered from the main stats page.

Usage:
    python3 scrape_ncaa_stats.py --school "Louisiana State University" --player "Dylan Crews"
    python3 scrape_ncaa_stats.py --list-schools
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://stats.ncaa.org/",
}

CACHE_DIR = os.path.join(os.path.dirname(__file__), ".ncaa_cache")
NCAA_BASE  = "https://stats.ncaa.org"

# NCAA org_ids for non-SIDEARM (ESPN) schools.
# These are stable identifiers assigned by the NCAA — they don't change year to year.
# Discovered from stats.ncaa.org team pages; verified against known URLs.
ESPN_SCHOOL_NCAA_IDS = {
    "Central Connecticut State University":    364,
    "Florida Gulf Coast University":           520,
    "Gardner-Webb University":                 2438,
    "Georgia Institute of Technology":         143,
    "Louisiana State University":              168,
    "Tennessee Technological University":      2464,
    "University of Arizona":                   23,
    "University of Illinois Urbana-Champaign": 145,
    "University of Kentucky":                  161,
    "University of Miami (Florida)":           178,
    "University of Notre Dame":                193,
    "University of San Francisco":             1388,
    "University of South Carolina, Columbia":  218,
    "Vanderbilt University":                   238,
    "Wofford College":                         2476,
}

session = requests.Session()
session.headers.update(HEADERS)


def fetch(url: str, retries: int = 3, delay: float = 1.5) -> str | None:
    """Fetch URL with retries and delay to respect stats.ncaa.org rate limits."""
    for attempt in range(retries):
        try:
            time.sleep(delay)
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                return resp.text
            print(f"  NCAA HTTP {resp.status_code}: {url}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"  NCAA attempt {attempt+1} failed ({e}): {url}", file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# Year ID discovery
# ---------------------------------------------------------------------------

_year_id_cache: dict[str, int] = {}


def get_year_id(sport_code: str = "MBA", season: str = "2026") -> int | None:
    """
    Discover the NCAA stats year_id for the given sport + season.

    stats.ncaa.org assigns an internal year_id to each sport/season combination.
    We cache the result so we only do this lookup once per run.
    """
    cache_key = f"{sport_code}_{season}"
    if cache_key in _year_id_cache:
        return _year_id_cache[cache_key]

    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(CACHE_DIR, f"year_id_{cache_key}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as fh:
                data = json.load(fh)
            if data.get("season") == season and data.get("year_id"):
                _year_id_cache[cache_key] = data["year_id"]
                return data["year_id"]
        except Exception:
            pass

    # Hit the NCAA stats landing page to find year_id for this sport/season
    # URL: /rankings/change_sport_year_div — returns HTML with a select of year_ids
    url = f"{NCAA_BASE}/rankings/change_sport_year_div?sport_code={sport_code}&academic_year={season}&division=1"
    html = fetch(url, delay=1.0)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    # Look for <select> containing year options, or links with id= param
    year_id = None

    # Method 1: hidden input or option with the year value
    for tag in soup.find_all(["input", "option"]):
        val = tag.get("value", "")
        if val.isdigit() and int(val) > 15000:  # NCAA year IDs are in 15000-20000 range
            year_id = int(val)
            break

    # Method 2: find it in a URL parameter on the page
    if not year_id:
        for a in soup.find_all("a", href=True):
            m = re.search(r"[?&]id=(\d{5,})", a["href"])
            if m:
                year_id = int(m.group(1))
                break

    if year_id:
        _year_id_cache[cache_key] = year_id
        with open(cache_file, "w") as fh:
            json.dump({"season": season, "year_id": year_id}, fh)
        return year_id

    return None


# ---------------------------------------------------------------------------
# Team player index (name → player_seq mapping)
# ---------------------------------------------------------------------------

def get_team_player_index(org_id: int, year_id: int) -> dict[str, int]:
    """
    Fetch the team stats page and build a mapping of last_name → player_seq.

    The team stats page at stats.ncaa.org/team/{org_id}/stats?id={year_id}
    shows a batting stats table with player names linked to their game logs.
    """
    cache_file = os.path.join(CACHE_DIR, f"team_{org_id}_{year_id}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as fh:
                return json.load(fh)
        except Exception:
            pass

    url = f"{NCAA_BASE}/team/{org_id}/stats?id={year_id}"
    html = fetch(url)
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    players = {}

    # Parse batting stats table — each row has a player link like:
    # /player/index?id={year_id}&stats_player_seq={seq}
    for a in soup.find_all("a", href=re.compile(r"stats_player_seq=\d+")):
        m = re.search(r"stats_player_seq=(\d+)", a["href"])
        if not m:
            continue
        player_seq = int(m.group(1))
        name = a.get_text(strip=True)
        if name:
            players[name.lower()] = player_seq
            # Also index by last name for fuzzy matching
            parts = name.split(",")
            if len(parts) >= 1:
                ln = parts[0].strip().lower()
                if ln not in players:
                    players[ln] = player_seq

    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(cache_file, "w") as fh:
        json.dump(players, fh)

    return players


def _find_player_seq(player_index: dict, full_name: str, last_name: str) -> int | None:
    """Fuzzy-match a player name to a stats_player_seq."""
    # Try: "LastName, FirstName" format (NCAA standard)
    ncaa_key = full_name.lower()
    if ncaa_key in player_index:
        return player_index[ncaa_key]

    # Try last name only
    ln = last_name.lower()
    if ln in player_index:
        return player_index[ln]

    # Try reversed: "First Last" → "Last, First"
    parts = full_name.split()
    if len(parts) >= 2:
        reversed_key = f"{parts[-1]}, {' '.join(parts[:-1])}".lower()
        if reversed_key in player_index:
            return player_index[reversed_key]

    return None


# ---------------------------------------------------------------------------
# Game log fetch + parse
# ---------------------------------------------------------------------------

def _safe_int(val) -> int:
    try:
        return int(val) if val not in (None, "", "-", "--") else 0
    except (ValueError, TypeError):
        return 0


def _parse_ncaa_date(text: str) -> datetime | None:
    text = text.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _ip_float(ip_str: str) -> float:
    try:
        s = str(ip_str or "0")
        parts = s.split(".")
        if len(parts) == 2:
            return int(parts[0]) + int(parts[1]) / 3
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def fetch_player_game_log_raw(player_seq: int, year_id: int) -> str | None:
    """Fetch the raw HTML for a player's game log page."""
    url = f"{NCAA_BASE}/player/index?id={year_id}&stats_player_seq={player_seq}"
    return fetch(url)


def parse_ncaa_batting_log(html: str) -> list[dict]:
    """
    Parse a stats.ncaa.org player batting game log.

    The page has a table with columns:
    Date | Opponent | Result | G | R | AB | H | 2B | 3B | TB | HR | RBI | BB | HBP | ...
    """
    soup = BeautifulSoup(html, "html.parser")
    games = []

    # Find the game log table (has Date column)
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if "date" not in headers or "ab" not in headers:
            continue

        col = {h: i for i, h in enumerate(headers)}
        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 6:
                continue

            date_text = cells[col.get("date", 0)]
            date_obj  = _parse_ncaa_date(date_text)
            if not date_obj:
                continue

            ab = _safe_int(cells[col["ab"]]) if "ab" in col else 0
            if ab == 0:
                continue

            opp_raw = cells[col.get("opponent", 1)]
            opp = re.sub(r"^(at\s+|@|\*)", "", opp_raw, flags=re.IGNORECASE).strip()
            result  = cells[col.get("result", 2)].strip()

            games.append({
                "date":    date_obj.strftime("%Y-%m-%d"),
                "opponent": opp or "Unknown",
                "result":  result,
                "ab":      ab,
                "r":       _safe_int(cells[col.get("r",  -1)]) if "r"   in col else 0,
                "h":       _safe_int(cells[col.get("h",  -1)]) if "h"   in col else 0,
                "doubles": _safe_int(cells[col.get("2b", -1)]) if "2b"  in col else 0,
                "triples": _safe_int(cells[col.get("3b", -1)]) if "3b"  in col else 0,
                "hr":      _safe_int(cells[col.get("hr", -1)]) if "hr"  in col else 0,
                "rbi":     _safe_int(cells[col.get("rbi",-1)]) if "rbi" in col else 0,
                "bb":      _safe_int(cells[col.get("bb", -1)]) if "bb"  in col else 0,
                "so":      _safe_int(cells[col.get("so", -1)]) if "so"  in col else 0,
                "sb":      _safe_int(cells[col.get("sb", -1)]) if "sb"  in col else 0,
            })
        break  # use first matching table

    return sorted(games, key=lambda x: x["date"])


def parse_ncaa_pitching_log(html: str) -> list[dict]:
    """
    Parse a stats.ncaa.org player pitching game log.

    The page has columns: Date | Opponent | Result | ... | IP | H | R | ER | BB | SO | ...
    The pitching log is on the same page as batting but in a second table.
    """
    soup = BeautifulSoup(html, "html.parser")
    games = []

    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if "date" not in headers or "ip" not in headers:
            continue

        col = {h: i for i, h in enumerate(headers)}
        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 6:
                continue

            date_text = cells[col.get("date", 0)]
            date_obj  = _parse_ncaa_date(date_text)
            if not date_obj:
                continue

            ip_str = cells[col["ip"]] if "ip" in col else "0"
            ipf    = _ip_float(ip_str)
            if ipf == 0.0:
                continue

            opp_raw = cells[col.get("opponent", 1)]
            opp = re.sub(r"^(at\s+|@|\*)", "", opp_raw, flags=re.IGNORECASE).strip()
            result  = cells[col.get("result", 2)].strip()

            games.append({
                "date":     date_obj.strftime("%Y-%m-%d"),
                "opponent": opp or "Unknown",
                "result":   result,
                "ip":       ip_str,
                "ip_float": ipf,
                "h":        _safe_int(cells[col.get("h",  -1)]) if "h"  in col else 0,
                "r":        _safe_int(cells[col.get("r",  -1)]) if "r"  in col else 0,
                "er":       _safe_int(cells[col.get("er", -1)]) if "er" in col else 0,
                "bb":       _safe_int(cells[col.get("bb", -1)]) if "bb" in col else 0,
                "so":       _safe_int(cells[col.get("so", -1)]) if "so" in col else 0,
                "w":        _safe_int(cells[col.get("w",  -1)]) if "w"  in col else 0,
                "l":        _safe_int(cells[col.get("l",  -1)]) if "l"  in col else 0,
                "sv":       _safe_int(cells[col.get("sv", -1)]) if "sv" in col else 0,
                "gs":       _safe_int(cells[col.get("gs", -1)]) if "gs" in col else 0,
            })
        # Take the last matching table (pitching is usually after batting)

    return sorted(games, key=lambda x: x["date"])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_ncaa_full_game_log(
    full_name: str,
    last_name: str,
    school_name: str,
    season: str = "2026",
) -> dict:
    """
    Fetch a player's full game log from stats.ncaa.org.

    Returns the same dict structure as parse_bio_full_game_log():
      {"batting": [...], "pitching": [...]}
    or empty dicts on failure.

    This is intentionally slow (respects ncaa.org rate limits via per-request delay).
    Use only as a fallback for non-SIDEARM schools.
    """
    empty = {"batting": [], "pitching": []}

    org_id = ESPN_SCHOOL_NCAA_IDS.get(school_name)
    if not org_id:
        print(f"  NCAA fallback: no org_id for '{school_name}'", file=sys.stderr)
        return empty

    year_id = get_year_id(season=season)
    if not year_id:
        print(f"  NCAA fallback: could not discover year_id for season {season}", file=sys.stderr)
        return empty

    player_index = get_team_player_index(org_id, year_id)
    if not player_index:
        print(f"  NCAA fallback: empty player index for {school_name}", file=sys.stderr)
        return empty

    player_seq = _find_player_seq(player_index, full_name, last_name)
    if not player_seq:
        print(f"  NCAA fallback: player '{full_name}' not found in index for {school_name}", file=sys.stderr)
        return empty

    html = fetch_player_game_log_raw(player_seq, year_id)
    if not html:
        return empty

    batting  = parse_ncaa_batting_log(html)
    pitching = parse_ncaa_pitching_log(html)

    return {"batting": batting, "pitching": pitching}


def get_schools_list() -> list[str]:
    return sorted(ESPN_SCHOOL_NCAA_IDS.keys())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NCAA stats.ncaa.org game log for a player")
    parser.add_argument("--school", help="School name (must be in ESPN_SCHOOL_NCAA_IDS)")
    parser.add_argument("--player", help="Player full name")
    parser.add_argument("--season", default="2026", help="Season year (default: 2026)")
    parser.add_argument("--list-schools", action="store_true", help="List supported schools")
    args = parser.parse_args()

    if args.list_schools:
        print("Supported schools:")
        for s in get_schools_list():
            print(f"  {s} (org_id={ESPN_SCHOOL_NCAA_IDS[s]})")
        sys.exit(0)

    if not args.school or not args.player:
        parser.error("--school and --player are required")

    ln = args.player.split()[-1]
    result = fetch_ncaa_full_game_log(args.player, ln, args.school, args.season)
    print(json.dumps(result, indent=2))
