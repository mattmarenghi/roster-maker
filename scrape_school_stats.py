#!/usr/bin/env python3
"""
School Baseball Stats Scraper
==============================
Scrapes season stats and per-player last-game data for a SIDEARM-hosted school.

Per-player last game strategy:
  - Players with a profile_url have a rosterPlayerId (trailing number in the URL).
    For these players the SIDEARM /api/v2/stats/bio endpoint is called directly,
    returning a full season game log — no box score scraping needed.
  - Players without a profile_url fall back to the old box-score scanning approach
    (scrape up to max_games_back box scores and match by last name).

Usage:
    python3 scrape_school_stats.py                          # Davidson (default)
    python3 scrape_school_stats.py --school davidson        # explicit
    python3 scrape_school_stats.py --base-url https://www.davidsonwildcats.com

Output:
    stats/{school_slug}.json

Requires ANTHROPIC_API_KEY env var for AI narratives (falls back to template).
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

DELAY = 0.75  # seconds between requests
PLAYERS_JSON = os.path.join(os.path.dirname(__file__), "players.json")
STATS_DIR = os.path.join(os.path.dirname(__file__), "stats")

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

session = requests.Session()
session.headers.update(HEADERS)


def fetch(url: str, retries: int = 3) -> str | None:
    """Fetch URL, return HTML string or None on failure."""
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                return resp.text
            print(f"  HTTP {resp.status_code}: {url}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"  Attempt {attempt+1} failed ({e}): {url}", file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# NUXT dedup-array resolver
# ---------------------------------------------------------------------------

def resolve_nuxt(data: list, idx, depth: int = 0):
    """Resolve NUXT server-rendered dedup array references."""
    if depth > 10 or not isinstance(idx, int) or idx >= len(data):
        return idx
    val = data[idx]
    if isinstance(val, list) and val and val[0] in ("ShallowReactive", "Reactive", "Set", "Map"):
        return resolve_nuxt(data, val[1], depth + 1)
    return val


def extract_nuxt_data(html: str) -> list | None:
    """Extract and parse the __NUXT_DATA__ script tag from a SIDEARM page."""
    match = re.search(
        r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
    )
    if not match:
        return None
    try:
        return json.loads(match.group(1).strip())
    except json.JSONDecodeError:
        return None


# ---------------------------------------------------------------------------
# SIDEARM bio API — per-player season game log
# ---------------------------------------------------------------------------

def get_roster_player_id(profile_url: str) -> str | None:
    """Extract rosterPlayerId from the trailing numeric segment of a profile_url.

    e.g. ".../roster/zandt-payne/10763"  →  "10763"
    """
    if not profile_url:
        return None
    last = profile_url.rstrip("/").split("/")[-1]
    return last if last.isdigit() else None


def fetch_bio_api(base_url: str, roster_player_id: str, year: str = "2026") -> dict | None:
    """Call SIDEARM /api/v2/stats/bio for one player. Returns parsed JSON or None."""
    url = (
        f"{base_url}/api/v2/stats/bio"
        f"?rosterPlayerId={roster_player_id}&sport=baseball&year={year}"
    )
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (400, 404):
                return None
            print(f"  Bio API HTTP {resp.status_code}: {url}", file=sys.stderr)
            return None
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                print(f"  Bio API error (rosterPlayerId={roster_player_id}): {e}", file=sys.stderr)
    return None


def _parse_bio_date(date_str) -> datetime | None:
    """Parse common date formats returned by SIDEARM bio API."""
    if not date_str:
        return None
    s = str(date_str).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_bio_last_game(bio: dict) -> dict | None:
    """Parse a SIDEARM /api/v2/stats/bio response to find the player's most recent game.

    Typical SIDEARM bio response shape:
        {
          "batting":  { "gameLog": [{date, opponent, result, ab, r, h, rbi, bb, so, ...}, ...] },
          "pitching": { "gameLog": [{date, opponent, result, ip, h, r, er, bb, so, ...}, ...] }
        }
    Field names vary by school/sport; this function tries common variants.

    Returns {date, opponent, result, boxscore_url, batting, pitching} or None.
    """
    if not isinstance(bio, dict):
        return None

    def find_log(section_key: str) -> list | None:
        section = bio.get(section_key)
        if isinstance(section, dict):
            for key in ("gameLog", "gameLogs", "game_log", "games", "log"):
                if isinstance(section.get(key), list):
                    return section[key]
        return None

    bat_log = find_log("batting")
    pitch_log = find_log("pitching")

    def safe_int(val) -> int:
        try:
            return int(val) if val not in (None, "", "-") else 0
        except (ValueError, TypeError):
            return 0

    def get_field(entry: dict, *keys):
        for k in keys:
            v = entry.get(k)
            if v is not None:
                return v
        return None

    def most_recent_with_activity(log: list | None, activity_fn) -> tuple:
        """Return (date_obj, entry) for the most recent game where player appeared."""
        if not log:
            return None, None
        best_date, best_entry = None, None
        for entry in log:
            if not isinstance(entry, dict) or not activity_fn(entry):
                continue
            raw_date = get_field(entry, "date", "gameDate", "Date", "eventDate")
            date_obj = _parse_bio_date(raw_date)
            if not date_obj:
                continue
            if best_date is None or date_obj > best_date:
                best_date, best_entry = date_obj, entry
        return best_date, best_entry

    bat_date, bat_entry = most_recent_with_activity(
        bat_log,
        lambda e: safe_int(get_field(e, "ab", "atBats", "AB")) > 0,
    )
    pitch_date, pitch_entry = most_recent_with_activity(
        pitch_log,
        lambda e: str(get_field(e, "ip", "inningsPitched", "IP") or "0") not in ("0", "0.0"),
    )

    if bat_entry is None and pitch_entry is None:
        return None

    # Use the most recently played game as the primary source for date/opponent/result
    if bat_date and pitch_date:
        primary_entry = bat_entry if bat_date >= pitch_date else pitch_entry
        primary_date = max(bat_date, pitch_date)
    elif bat_date:
        primary_entry, primary_date = bat_entry, bat_date
    else:
        primary_entry, primary_date = pitch_entry, pitch_date

    date_out = primary_date.strftime("%Y-%m-%d")
    opponent_raw = str(get_field(primary_entry, "opponent", "Opponent", "opponentName", "team") or "")
    opponent = opponent_raw.lstrip("@").strip() or "Unknown"
    result = str(get_field(primary_entry, "result", "Result", "wl", "gameResult") or "")

    # Build batting stats dict
    bat_data = None
    if bat_entry:
        ab = safe_int(get_field(bat_entry, "ab", "atBats", "AB"))
        if ab > 0:
            bat_data = {
                "ab":      ab,
                "r":       safe_int(get_field(bat_entry, "r", "runs", "R")),
                "h":       safe_int(get_field(bat_entry, "h", "hits", "H")),
                "rbi":     safe_int(get_field(bat_entry, "rbi", "RBI")),
                "bb":      safe_int(get_field(bat_entry, "bb", "walks", "BB")),
                "so":      safe_int(get_field(bat_entry, "so", "k", "strikeouts", "SO", "K")),
                "hr":      safe_int(get_field(bat_entry, "hr", "homeRuns", "HR")),
                "doubles": safe_int(get_field(bat_entry, "2b", "doubles", "2B")),
                "triples": safe_int(get_field(bat_entry, "3b", "triples", "3B")),
                "pos":     str(get_field(bat_entry, "pos", "position", "Pos") or ""),
            }

    # Build pitching stats dict
    pitch_data = None
    if pitch_entry:
        ip_raw = get_field(pitch_entry, "ip", "inningsPitched", "IP") or "0.0"
        ip_str = str(ip_raw)
        if ip_str not in ("0", "0.0"):
            pitch_data = {
                "ip":  ip_str,
                "h":   safe_int(get_field(pitch_entry, "h", "hitsAllowed", "H")),
                "r":   safe_int(get_field(pitch_entry, "r", "runsAllowed", "R")),
                "er":  safe_int(get_field(pitch_entry, "er", "earnedRuns", "ER")),
                "bb":  safe_int(get_field(pitch_entry, "bb", "walksAllowed", "BB")),
                "so":  safe_int(get_field(pitch_entry, "so", "k", "strikeouts", "SO", "K")),
                "wp":  safe_int(get_field(pitch_entry, "wp", "wildPitches", "WP")),
                "hbp": safe_int(get_field(pitch_entry, "hbp", "hitBatters", "HBP")),
            }

    if bat_data is None and pitch_data is None:
        return None

    return {
        "date": date_out,
        "opponent": opponent,
        "result": result,
        "boxscore_url": None,  # bio API does not expose boxscore URLs
        "batting": bat_data,
        "pitching": pitch_data,
    }


# ---------------------------------------------------------------------------
# Season stats scrapers
# ---------------------------------------------------------------------------

def stats_url(base_url: str, season: str, no_season: bool = False) -> str:
    """Build the stats page URL, handling schools that omit the season."""
    if no_season:
        return f"{base_url}/sports/baseball/stats"
    return f"{base_url}/sports/baseball/stats/{season}"


def scrape_season_batting(base_url: str, season: str = "2026", no_season: bool = False) -> list[dict]:
    """Scrape cumulative season batting stats from the HTML stats table."""
    url = stats_url(base_url, season, no_season)
    print(f"  Fetching season batting: {url}")
    html = fetch(url)
    if not html and not no_season:
        # Retry without season in URL
        url = stats_url(base_url, season, True)
        print(f"  Retrying without season: {url}")
        html = fetch(url)
    if not html:
        return []
    time.sleep(DELAY)

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return []

    # Find the batting stats table (has AVG column)
    batting_table = None
    for t in tables:
        headers = [th.get_text(strip=True).lower() for th in t.find_all("th")]
        if "avg" in headers and "ab" in headers:
            batting_table = t
            break
    if not batting_table:
        return []

    raw_headers = [th.get_text(strip=True) for th in batting_table.find_all("th")]
    players = []
    for row in batting_table.find_all("tr")[1:]:  # skip header
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if not cells or len(cells) < 5:
            continue
        # Skip totals rows
        name_raw = cells[1] if len(cells) > 1 else ""
        if "total" in name_raw.lower() or not name_raw:
            continue

        # Parse "Last, First" format
        if "," in name_raw:
            ln, fn = [x.strip() for x in name_raw.split(",", 1)]
        else:
            parts = name_raw.split()
            fn, ln = (parts[0], " ".join(parts[1:])) if len(parts) > 1 else (name_raw, "")

        def cell(col_name):
            try:
                col_name_lower = col_name.lower()
                for i, h in enumerate(raw_headers):
                    if h.lower() == col_name_lower and i < len(cells):
                        return cells[i]
            except Exception:
                pass
            return None

        # Parse GP-GS "X - Y" format
        gp_gs = cell("GP-GS") or ""
        gp, gs = 0, 0
        if "-" in gp_gs:
            parts = gp_gs.replace(" ", "").split("-")
            try:
                gp, gs = int(parts[0]), int(parts[1])
            except (ValueError, IndexError):
                pass

        # Parse sb-att "X - Y" format
        sb_att = cell("sb-att") or ""
        sb, sb_att_n = 0, 0
        if "-" in sb_att:
            parts = sb_att.replace(" ", "").split("-")
            try:
                sb, sb_att_n = int(parts[0]), int(parts[1])
            except (ValueError, IndexError):
                pass

        def safe_int(val):
            try:
                return int(val) if val not in (None, "", "-") else 0
            except ValueError:
                return 0

        def safe_float(val):
            try:
                return float(val) if val not in (None, "", "-") else None
            except ValueError:
                return None

        players.append({
            "fn": fn,
            "ln": ln,
            "jersey": safe_int(cells[0]),
            "season_batting": {
                "g": gp,
                "gs": gs,
                "avg": safe_float(cell("AVG")),
                "ops": safe_float(cell("OPS")),
                "ab": safe_int(cell("AB")),
                "r": safe_int(cell("r")),
                "h": safe_int(cell("h")),
                "doubles": safe_int(cell("2B")),
                "triples": safe_int(cell("3b")),
                "hr": safe_int(cell("hr")),
                "rbi": safe_int(cell("rbi")),
                "tb": safe_int(cell("tb")),
                "slg": safe_float(cell("slg%")),
                "bb": safe_int(cell("bb")),
                "hbp": safe_int(cell("hbp")),
                "so": safe_int(cell("so")),
                "gdp": safe_int(cell("gdp")),
                "obp": safe_float(cell("ob%")),
                "sf": safe_int(cell("sf")),
                "sh": safe_int(cell("sh")),
                "sb": sb,
                "sb_att": sb_att_n,
            },
        })

    print(f"  Found {len(players)} batters")
    return players


def scrape_season_pitching(base_url: str, season: str = "2026", no_season: bool = False) -> list[dict]:
    """Extract season pitching stats from the NUXT embedded data."""
    url = stats_url(base_url, season, no_season)
    print(f"  Fetching season pitching from NUXT data: {url}")
    html = fetch(url)
    if not html and not no_season:
        url = stats_url(base_url, season, True)
        print(f"  Retrying without season: {url}")
        html = fetch(url)
    if not html:
        return []
    time.sleep(DELAY)

    nuxt_data = extract_nuxt_data(html)
    if not nuxt_data:
        print("  Could not extract NUXT data for pitching stats", file=sys.stderr)
        return []

    def rv(idx):
        return resolve_nuxt(nuxt_data, idx)

    # Navigate: statsSeason -> cumulativeStats -> (key) -> overallIndividualStats
    # -> individualStats -> individualPitchingStats
    stats_season_idx = None
    for i, item in enumerate(nuxt_data):
        if isinstance(item, dict) and "statsSeason" in item:
            stats_season_idx = item["statsSeason"]
            break
    if stats_season_idx is None:
        return []

    stats_season = rv(stats_season_idx)
    if not isinstance(stats_season, dict):
        return []

    cumul_idx = stats_season.get("cumulativeStats")
    if cumul_idx is None:
        return []

    cumul = rv(cumul_idx)
    if not isinstance(cumul, dict):
        return []

    # cumul is keyed by a hash; get the first value
    first_key = next(iter(cumul), None)
    if first_key is None:
        return []

    main_stats = rv(cumul[first_key])
    if not isinstance(main_stats, dict):
        return []

    overall_idx = main_stats.get("overallIndividualStats")
    if overall_idx is None:
        return []

    overall = rv(overall_idx)
    if not isinstance(overall, dict):
        return []

    indiv_idx = overall.get("individualStats")
    if indiv_idx is None:
        return []

    indiv = rv(indiv_idx)
    if not isinstance(indiv, dict):
        return []

    pitching_list_idx = indiv.get("individualPitchingStats")
    if pitching_list_idx is None:
        return []

    pitching_list = rv(pitching_list_idx)
    if not isinstance(pitching_list, list):
        return []

    pitchers = []
    for pitcher_idx in pitching_list:
        p = rv(pitcher_idx)
        if not isinstance(p, dict):
            continue

        name_raw = rv(p.get("playerName"))
        if not name_raw or str(name_raw).lower() in ("totals", "opponents"):
            continue

        name_str = str(name_raw)
        if "," in name_str:
            ln, fn = [x.strip() for x in name_str.split(",", 1)]
        else:
            parts = name_str.split()
            fn, ln = (parts[0], " ".join(parts[1:])) if len(parts) > 1 else (name_str, "")

        def safe_num(key, cast=float):
            val = rv(p.get(key))
            if val is None:
                return None
            try:
                return cast(val)
            except (ValueError, TypeError):
                return None

        pitchers.append({
            "fn": fn,
            "ln": ln,
            "season_pitching": {
                "era": safe_num("earnedRunAverage"),
                "whip": safe_num("whip"),
                "w": safe_num("wins", int),
                "l": safe_num("losses", int),
                "sv": safe_num("saves", int),
                "g": safe_num("appearances", int),
                "gs": safe_num("gamesStarted", int),
                "ip": safe_num("inningsPitched"),
                "h": safe_num("hitsAllowed", int),
                "r": safe_num("runsAllowed", int),
                "er": safe_num("earnedRunsAllowed", int),
                "bb": safe_num("walksAllowed", int),
                "so": safe_num("strikeouts", int),
                "hr": safe_num("homeRunsAllowed", int),
                "hbp": safe_num("hitBatters", int),
                "wp": safe_num("wildPitches", int),
                "opp_avg": rv(p.get("opponentsBattingAverage")),
            },
        })

    print(f"  Found {len(pitchers)} pitchers")
    if pitchers:
        return pitchers

    # --- Fallback: parse pitching stats from HTML table (old SIDEARM format) ---
    print("  Falling back to HTML table pitching parser")
    soup = BeautifulSoup(html, "html.parser")
    pitching_table = None
    for t in soup.find_all("table"):
        hdr = [th.get_text(strip=True).lower() for th in t.find_all("th")]
        if "era" in hdr and "ip" in hdr and "w-l" in hdr:
            pitching_table = t
            break
    if not pitching_table:
        return []

    raw_headers = [th.get_text(strip=True) for th in pitching_table.find_all("th")]
    pitchers = []
    for row in pitching_table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if not cells or len(cells) < 10:
            continue

        # First cell is jersey #, second is player name in some old formats
        # But in UCSB format, the headers include player names. Rows start with jersey #.
        name_raw = ""
        # Check if we can find a link with the player name
        name_link = row.find("a")
        if name_link:
            name_raw = name_link.get_text(strip=True)
        if not name_raw:
            # Try getting from the row text before the first numeric cell
            all_text = [td.get_text(strip=True) for td in row.find_all("td")]
            if all_text and not all_text[0].replace(".", "").replace("-", "").isdigit():
                name_raw = all_text[0]

        if not name_raw or name_raw.lower() in ("totals", "opponents", ""):
            continue

        if "," in name_raw:
            ln, fn = [x.strip() for x in name_raw.split(",", 1)]
        else:
            parts = name_raw.split()
            fn, ln = (parts[0], " ".join(parts[1:])) if len(parts) > 1 else (name_raw, "")

        def cell(col_name):
            try:
                col_lower = col_name.lower()
                for ci, h in enumerate(raw_headers):
                    if h.lower() == col_lower and ci < len(cells):
                        return cells[ci]
            except Exception:
                pass
            return None

        def safe_int(val):
            try:
                return int(val) if val not in (None, "", "-") else 0
            except ValueError:
                return 0

        def safe_float(val):
            try:
                return float(val) if val not in (None, "", "-") else None
            except ValueError:
                return None

        # Parse W-L field
        wl = cell("W-L") or "0-0"
        w, l = 0, 0
        if "-" in wl:
            parts = wl.replace(" ", "").split("-")
            try:
                w, l = int(parts[0]), int(parts[1])
            except (ValueError, IndexError):
                pass

        # Parse APP-GS
        app_gs = cell("APP-GS") or "0-0"
        app, gs = 0, 0
        if "-" in app_gs:
            parts = app_gs.replace(" ", "").split("-")
            try:
                app, gs = int(parts[0]), int(parts[1])
            except (ValueError, IndexError):
                pass

        pitchers.append({
            "fn": fn,
            "ln": ln,
            "season_pitching": {
                "era": safe_float(cell("ERA")),
                "whip": safe_float(cell("WHIP")),
                "w": w,
                "l": l,
                "sv": safe_int(cell("SV")),
                "g": app,
                "gs": gs,
                "ip": safe_float(cell("IP")),
                "h": safe_int(cell("H")),
                "r": safe_int(cell("R")),
                "er": safe_int(cell("ER")),
                "bb": safe_int(cell("BB")),
                "so": safe_int(cell("SO")),
                "hr": safe_int(cell("HR")),
                "hbp": safe_int(cell("HBP")),
                "wp": safe_int(cell("WP")),
                "opp_avg": cell("B/AVG"),
            },
        })

    print(f"  Found {len(pitchers)} pitchers (HTML fallback)")
    return pitchers


# ---------------------------------------------------------------------------
# W/L record scraper
# ---------------------------------------------------------------------------

def scrape_record(base_url: str, season: str = "2026", no_season: bool = False) -> str:
    """
    Extract the team's W-L record from the NUXT data on the stats page.
    Looks for the overallRecord field or counts finished games.
    Returns a string like "12-5" or "" if not found.
    """
    url = stats_url(base_url, season, no_season)
    html = fetch(url)
    if not html and not no_season:
        url = stats_url(base_url, season, True)
        html = fetch(url)
    if not html:
        return ""
    time.sleep(DELAY)

    nuxt_data = extract_nuxt_data(html)
    if not nuxt_data:
        return ""

    def rv(idx):
        return resolve_nuxt(nuxt_data, idx)

    # Search for overallRecord or wins/losses fields
    for i, item in enumerate(nuxt_data):
        if isinstance(item, dict):
            if "overallRecord" in item:
                rec = rv(item["overallRecord"])
                if isinstance(rec, str) and "-" in rec:
                    return rec
            if "wins" in item and "losses" in item:
                w = rv(item["wins"])
                l = rv(item["losses"])
                if isinstance(w, (int, float)) and isinstance(l, (int, float)):
                    return f"{int(w)}-{int(l)}"

    # Fallback: look for "X-Y" pattern near "Overall" or "Record" strings
    for i, item in enumerate(nuxt_data):
        if isinstance(item, str) and re.match(r"^\d{1,2}-\d{1,2}(-\d{1,2})?$", item):
            # Check surrounding items for context clues
            for j in range(max(0, i - 5), min(len(nuxt_data), i + 5)):
                if isinstance(nuxt_data[j], str) and nuxt_data[j].lower() in ("overall", "record"):
                    return item
            # If no context but it looks like a record, tentatively use it
            return item

    return ""


def scrape_record_from_gamelog(base_url: str, season: str, no_season: bool = False) -> str:
    """Fallback: compute W-L from the HTML game log table."""
    url = stats_url(base_url, season, no_season)
    html = fetch(url)
    if not html and not no_season:
        url = stats_url(base_url, season, True)
        html = fetch(url)
    if not html:
        return ""
    time.sleep(DELAY)

    soup = BeautifulSoup(html, "html.parser")
    for t in soup.find_all("table"):
        hdr = [th.get_text(strip=True).lower() for th in t.find_all("th")]
        if "date" in hdr and "w/l" in hdr:
            wl_idx = hdr.index("w/l")
            wins, losses = 0, 0
            for row in t.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if wl_idx < len(cells):
                    v = cells[wl_idx].upper().strip()
                    if v.startswith("W"):
                        wins += 1
                    elif v.startswith("L"):
                        losses += 1
            if wins or losses:
                return f"{wins}-{losses}"
    return ""


# ---------------------------------------------------------------------------
# Box score / game log scrapers
# ---------------------------------------------------------------------------

def find_game_urls(base_url: str, season: str = "2026", no_season: bool = False) -> list[dict]:
    """
    Extract box score URLs and dates from NUXT data on the stats page.
    Box score paths are stored as raw strings inside the NUXT JSON, along
    with the date and opponent in nearby array positions.
    Returns list of {date, opponent, url} sorted newest-first.
    """
    url = stats_url(base_url, season, no_season)
    html = fetch(url)
    if not html and not no_season:
        url = stats_url(base_url, season, True)
        html = fetch(url)
    if not html:
        return []
    time.sleep(DELAY)

    nuxt_data = extract_nuxt_data(html)

    # Boxscore paths are raw strings like "/sports/baseball/stats/2026/lehigh/boxscore/10233"
    # In the NUXT array the pattern is typically: date_str, opponent_str, boxscore_path_str
    # Find all boxscore paths and then look at adjacent items for date/opponent
    boxscore_path_re = re.compile(r"^/sports/baseball/stats/\d+/[^/]+/boxscore/\d+$")
    date_re = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")

    seen = set()
    games = []

    if nuxt_data:
        for i, item in enumerate(nuxt_data):
            if not isinstance(item, str) or not boxscore_path_re.match(item):
                continue
            if item in seen:
                continue
            seen.add(item)

            # Look backwards for date and opponent (typically within ~5 positions)
            date_str = None
            opponent = None
            for j in range(max(0, i - 6), i):
                candidate = nuxt_data[j]
                if isinstance(candidate, str):
                    if date_re.match(candidate) and not date_str:
                        date_str = candidate
                    elif candidate and not date_re.match(candidate) and len(candidate) < 80 and not opponent:
                        # Avoid picking up team IDs or other noise
                        if re.match(r"^[A-Za-z\s\-\.&']+$", candidate) and len(candidate) > 2:
                            opponent = candidate

            if not date_str:
                continue

            try:
                date_obj = datetime.strptime(date_str, "%m/%d/%Y")
            except ValueError:
                continue

            # Derive opponent from URL path as reliable fallback
            # e.g. "/sports/baseball/stats/2026/lehigh/boxscore/10233" -> "Lehigh"
            url_parts = item.strip("/").split("/")
            opp_from_url = url_parts[-3].replace("-", " ").title() if len(url_parts) >= 3 else None

            games.append({
                "date": date_obj.strftime("%Y-%m-%d"),
                "date_obj": date_obj,
                "opponent": opponent or opp_from_url or "Unknown",
                "boxscore_url": urljoin(base_url, item),
                "boxscore_path": item,
            })

    games.sort(key=lambda g: g["date_obj"], reverse=True)
    if games:
        return games

    # --- Fallback: parse game log from HTML table (old SIDEARM format) ---
    print("  Falling back to HTML game log parser")
    soup = BeautifulSoup(html, "html.parser")
    for t in soup.find_all("table"):
        hdr = [th.get_text(strip=True).lower() for th in t.find_all("th")]
        if "date" in hdr and "opponent" in hdr and ("w/l" in hdr or "score" in hdr):
            for row in t.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) < 4:
                    continue

                # Find date
                date_str = cells[0]
                try:
                    date_obj = datetime.strptime(date_str, "%m/%d/%Y")
                except ValueError:
                    continue

                # Opponent
                opp_idx = next((i for i, h in enumerate(hdr) if h == "opponent"), 2)
                opponent = cells[opp_idx] if opp_idx < len(cells) else "Unknown"

                # W/L and Score
                wl_idx = next((i for i, h in enumerate(hdr) if h == "w/l"), None)
                score_idx = next((i for i, h in enumerate(hdr) if h == "score"), None)
                wl = cells[wl_idx] if wl_idx and wl_idx < len(cells) else ""
                score = cells[score_idx] if score_idx and score_idx < len(cells) else ""
                result = f"{wl} {score}".strip() if wl else score

                # Box score link
                box_link = None
                for a in row.find_all("a", href=True):
                    href = a["href"]
                    if "boxscore" in href.lower():
                        box_link = urljoin(base_url, href)
                        break

                if not box_link:
                    continue

                games.append({
                    "date": date_obj.strftime("%Y-%m-%d"),
                    "date_obj": date_obj,
                    "opponent": opponent,
                    "boxscore_url": box_link,
                    "boxscore_path": box_link,
                    "result_from_log": result,
                })

            break

    games.sort(key=lambda g: g["date_obj"], reverse=True)
    return games


def scrape_boxscore(url: str) -> dict | None:
    """
    Parse a SIDEARM box score page.
    Returns {line_score, home_batting, away_batting, home_pitching, away_pitching, scoring_plays}.
    The "home" team is determined by the page URL (the URL's school is Davidson = home perspective).
    """
    print(f"  Fetching box score: {url}")
    html = fetch(url)
    if not html:
        return None
    time.sleep(DELAY)

    soup = BeautifulSoup(html, "html.parser")

    # Extract game title for home/away context
    title = soup.title.string if soup.title else ""
    # "Baseball vs Lehigh on 3/8/2026 - Box Score - Davidson College Athletics"
    # "vs" = home, "at" = away (Davidson is home when "vs", away when "at")
    our_team_is_home = " vs " in title

    tables = soup.find_all("table")
    result = {
        "title": title,
        "our_team_is_home": our_team_is_home,
        "line_score": None,
        "our_batting": [],
        "opponent_batting": [],
        "our_pitching": [],
        "opponent_pitching": [],
        "scoring_plays": [],
    }

    def parse_batting_table(table) -> list[dict]:
        rows = table.find_all("tr")
        out = []
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 5:
                continue
            name = cells[0]
            if not name or name.lower() in ("totals", ""):
                continue

            def si(idx):
                try:
                    return int(cells[idx])
                except (ValueError, IndexError):
                    return 0

            out.append({
                "name": name,
                "pos": cells[1] if len(cells) > 1 else "",
                "ab": si(2), "r": si(3), "h": si(4),
                "rbi": si(5), "bb": si(6), "so": si(8),
                "lob": si(9) if len(cells) > 9 else 0,
            })
        return out

    def parse_pitching_table(table) -> list[dict]:
        rows = table.find_all("tr")
        out = []
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 6:
                continue
            name = cells[0]
            if not name or name.lower() in ("totals", ""):
                continue

            def sv(idx, cast=float):
                try:
                    return cast(cells[idx])
                except (ValueError, IndexError):
                    return 0

            out.append({
                "name": name,
                "ip": cells[1] if len(cells) > 1 else "0.0",
                "h": sv(2, int), "r": sv(3, int), "er": sv(4, int),
                "bb": sv(5, int), "so": sv(6, int),
                "wp": sv(7, int), "hbp": sv(9, int) if len(cells) > 9 else 0,
            })
        return out

    # Identify tables: line score (has "R", "H", "E"), batting (has "AB"), pitching (has "IP")
    batting_tables = []
    pitching_tables = []
    for table in tables:
        headers = [th.get_text(strip=True).upper() for th in table.find_all("th")]
        if "IP" in headers and "ER" in headers:
            pitching_tables.append(table)
        elif "AB" in headers and "RBI" in headers:
            batting_tables.append(table)
        elif (
            any("TEAM" in h for h in headers)
            and any("R" in h or "RUNS" in h for h in headers)
            and "AB" not in headers and "IP" not in headers
            and "INNING" not in " ".join(headers).upper()
            and not any("INNING" == h for h in headers)
        ):
            # Line score — headers may be "RunsR", "HitsH", "ErrorsE" or plain "R", "H", "E"
            # Exclude scoring plays table which has "TEAM" + "INNING" headers
            rows = table.find_all("tr")
            if 2 <= len(rows) <= 5:  # line score has header + 2-3 team rows
                teams = []
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                    if cells:
                        teams.append(cells)
                result["line_score"] = teams
        elif "TEAM" in headers and "INNING" in headers:
            # Scoring plays
            for row in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) >= 3 and cells[2]:
                    result["scoring_plays"].append({
                        "inning": cells[1],
                        "description": cells[2],
                    })

    # Assign batting/pitching tables (first = away/opponent, second = our team)
    if len(batting_tables) >= 2:
        result["opponent_batting"] = parse_batting_table(batting_tables[0])
        result["our_batting"] = parse_batting_table(batting_tables[1])
    elif len(batting_tables) == 1:
        result["our_batting"] = parse_batting_table(batting_tables[0])

    if len(pitching_tables) >= 2:
        result["opponent_pitching"] = parse_pitching_table(pitching_tables[0])
        result["our_pitching"] = parse_pitching_table(pitching_tables[1])
    elif len(pitching_tables) == 1:
        result["our_pitching"] = parse_pitching_table(pitching_tables[0])

    return result


# ---------------------------------------------------------------------------
# Game recap scraper
# ---------------------------------------------------------------------------

def find_recap_url(base_url: str, game_date: str, opponent: str) -> str | None:
    """
    Find a game recap URL from the news archive.
    Searches the baseball news listing for a story matching date and opponent.
    """
    news_url = f"{base_url}/sports/baseball/archives"
    print(f"  Searching for recap on {game_date} vs {opponent}")
    html = fetch(news_url)
    if not html:
        return None
    time.sleep(DELAY)

    soup = BeautifulSoup(html, "html.parser")
    # Look for news links containing date path matching game_date
    date_path = game_date.replace("-", "/")  # "2026/03/08"
    year, month, day = game_date.split("-")
    month_int = str(int(month))   # "3" not "03"
    day_int = str(int(day))       # "8" not "08"
    date_path_short = f"{year}/{month_int}/{day_int}"  # "2026/3/8"

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/news/" in href and (date_path in href or date_path_short in href):
            return urljoin(base_url, href)

    # Fallback: try schedule page
    schedule_html = fetch(f"{base_url}/sports/baseball/schedule")
    if schedule_html:
        soup2 = BeautifulSoup(schedule_html, "html.parser")
        for a in soup2.find_all("a", href=True):
            href = a["href"]
            if "/news/" in href and (date_path in href or date_path_short in href):
                return urljoin(base_url, href)

    return None


def scrape_recap(url: str) -> dict:
    """
    Scrape a SIDEARM game recap page.
    Returns {headline, article_text, game_score, players_mentioned, date}.
    """
    print(f"  Fetching recap: {url}")
    html = fetch(url)
    if not html:
        return {}
    time.sleep(DELAY)

    soup = BeautifulSoup(html, "html.parser")

    # Try specific story headline selector first, fall back to h1
    headline_el = (
        soup.select_one(".story-page__content__title-box h1")
        or soup.select_one(".s-page-title")
        or soup.select_one("h1.s-title")
        or soup.select_one("h1")
    )
    headline_raw = headline_el.get_text(strip=True) if headline_el else ""
    # Strip date/sport suffix often appended e.g. "Title | 2/24/2026|Baseball"
    headline = re.split(r"\s*[\|]\s*\d", headline_raw)[0].strip() if headline_raw else ""

    body_el = soup.select_one(".story-page__content__body")
    article_text = body_el.get_text(separator="\n", strip=True) if body_el else ""

    # Game bug / score card
    gamebug_el = soup.select_one(".s-game-bug-card__postgame-content")
    game_score_raw = gamebug_el.get_text(separator=" | ", strip=True) if gamebug_el else ""

    # People mentioned section (has inline stats for key players)
    players_mentioned = []
    seen_names = set()
    for item in soup.select(".s-people__content__item"):
        text = item.get_text(separator=" ", strip=True)
        if not text or text in seen_names:
            continue
        seen_names.add(text)
        # Parse "12 Jamie Daly cf AB 4 R 2 H 2 RBI 2" format
        stat_match = re.match(
            r"(\d+)\s+(.+?)\s+(\w+)\s+AB\s+(\d+)\s+R\s+(\d+)\s+H\s+(\d+)\s+RBI\s+(\d+)",
            text, re.IGNORECASE
        )
        if stat_match:
            players_mentioned.append({
                "jersey": int(stat_match.group(1)),
                "name": stat_match.group(2).strip(),
                "pos": stat_match.group(3),
                "ab": int(stat_match.group(4)),
                "r": int(stat_match.group(5)),
                "h": int(stat_match.group(6)),
                "rbi": int(stat_match.group(7)),
            })
        else:
            # Just a name mention (pitcher, etc.)
            players_mentioned.append({"name": text, "jersey": None})

    return {
        "headline": headline,
        "article_text": article_text,
        "game_score_raw": game_score_raw,
        "players_mentioned": players_mentioned,
    }


# ---------------------------------------------------------------------------
# Name matching
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    name = name.lower()
    name = re.sub(r"[^a-z\s]", "", name)
    return re.sub(r"\s+", " ", name).strip()


def match_player(fn: str, ln: str, player_db: list[dict]) -> dict | None:
    """
    Match a name from stats to an entry in players.json.
    Tries exact last-name match first, then fuzzy by checking initials.
    """
    ln_norm = normalize_name(ln)
    fn_norm = normalize_name(fn)

    # 1. Exact last + first
    for p in player_db:
        if normalize_name(p["ln"]) == ln_norm and normalize_name(p["fn"]) == fn_norm:
            return p

    # 2. Exact last + first initial
    if fn_norm:
        fi = fn_norm[0]
        candidates = [
            p for p in player_db
            if normalize_name(p["ln"]) == ln_norm and normalize_name(p["fn"]).startswith(fi)
        ]
        if len(candidates) == 1:
            return candidates[0]

    # 3. Exact last name only (if unique)
    candidates = [p for p in player_db if normalize_name(p["ln"]) == ln_norm]
    if len(candidates) == 1:
        return candidates[0]

    return None


def parse_boxscore_name(raw: str) -> tuple[str, str]:
    """
    Parse box score name formats:
      "F Lietz"   -> fn="F", ln="Lietz"
      "Lietz, F"  -> fn="F", ln="Lietz"
      "Ed Hall"   -> fn="Ed", ln="Hall"
    """
    raw = raw.strip()
    if "," in raw:
        ln, fn = [x.strip() for x in raw.split(",", 1)]
    else:
        parts = raw.split()
        if len(parts) >= 2:
            fn, ln = parts[0], " ".join(parts[1:])
        else:
            fn, ln = "", raw
    return fn, ln


# ---------------------------------------------------------------------------
# Narrative generation
# ---------------------------------------------------------------------------

def generate_narrative_claude(player: dict, game_info: dict, article_text: str) -> str:
    """Generate a 2-3 sentence narrative using Claude API."""
    try:
        import anthropic
    except ImportError:
        return generate_narrative_template(player, game_info)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return generate_narrative_template(player, game_info)

    client = anthropic.Anthropic(api_key=api_key)

    # Build stats context
    last_game = player.get("last_game", {})
    batting = last_game.get("batting")
    pitching = last_game.get("pitching")

    if batting:
        stats_str = (
            f"Batting: {batting['h']}-for-{batting['ab']}, "
            f"{batting['r']} run(s), {batting['rbi']} RBI, {batting['bb']} BB, {batting['so']} SO"
        )
        if batting.get("hr", 0):
            stats_str += f", {batting['hr']} HR"
    elif pitching:
        stats_str = (
            f"Pitching: {pitching['ip']} IP, {pitching['h']} H, "
            f"{pitching['r']} R, {pitching['er']} ER, {pitching['bb']} BB, {pitching['so']} K"
        )
    else:
        return ""  # player didn't appear in this game

    opponent = game_info.get("opponent", "the opponent")
    date = game_info.get("date", "")
    result_str = game_info.get("result", "")

    prompt = f"""You are a college baseball beat writer. Write a 2-3 sentence narrative description
of this player's performance in their most recent game. Be specific, vivid, and use the article text
for context if relevant. Do not start with the player's name. Write in past tense.

Player: {player['name']}
Position: {player.get('pos', 'N/A')}
Game: {date} vs {opponent} ({result_str})
Stats: {stats_str}

Game recap article for context:
{article_text[:2000]}

Write ONLY the 2-3 sentence narrative, nothing else."""

    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()
    except Exception as e:
        print(f"  Claude API error for {player['name']}: {e}", file=sys.stderr)
        return generate_narrative_template(player, game_info)


def generate_narrative_template(player: dict, game_info: dict) -> str:
    """Fallback template-based narrative when Claude API is unavailable."""
    last_game = player.get("last_game", {})
    batting = last_game.get("batting")
    pitching = last_game.get("pitching")
    opponent = game_info.get("opponent", "the opponent")
    result = game_info.get("result", "")

    # Pitching takes priority for pitchers (meaningful IP)
    has_meaningful_pitch = pitching and str(pitching.get("ip", "0")) not in ("0", "0.0")
    has_meaningful_bat = batting and batting.get("ab", 0) > 0

    if has_meaningful_pitch:
        # Pitching narrative (batting will be ignored even if present with 0 AB)
        pass
    elif has_meaningful_bat:
        batting = batting  # use batting below
    else:
        return ""

    if has_meaningful_pitch and not has_meaningful_bat:
        batting = None  # suppress batting template for pure pitchers

    if batting and has_meaningful_bat:
        avg = f"{batting['h']}/{batting['ab']}" if batting["ab"] > 0 else "0/0"
        parts = [f"Went {avg} with {batting['rbi']} RBI against {opponent}."]
        if batting.get("hr", 0):
            parts.append(f"Hit {batting['hr']} home run(s).")
        if batting["ab"] > 0:
            ba = batting["h"] / batting["ab"]
            if ba >= 0.400:
                parts.append(f"Had an outstanding performance at the plate ({ba:.3f}).")
            elif ba >= 0.250:
                parts.append(f"Contributed at the plate in the team's {result}.")
            else:
                parts.append(f"Struggled at the plate but saw {batting['bb']} walk(s)." if batting.get("bb") else "Struggled to make contact.")
        return " ".join(parts)

    if has_meaningful_pitch:
        try:
            ip_float = float(str(pitching["ip"]).split("+")[0])
        except (ValueError, TypeError):
            ip_float = 0.0
        parts = [f"Took the mound against {opponent}, throwing {pitching['ip']} innings."]
        if ip_float >= 5:
            parts.append(
                f"Struck out {pitching['so']} while allowing just {pitching['h']} hit(s) "
                f"and {pitching['er']} earned run(s) in a quality start."
            )
        else:
            parts.append(f"Allowed {pitching['er']} earned run(s) with {pitching['so']} strikeout(s).")
        return " ".join(parts)

    return ""


# ---------------------------------------------------------------------------
# Main assembly
# ---------------------------------------------------------------------------

def parse_result_from_box(box: dict | None) -> str:
    """Extract W/L result string from a parsed box score."""
    if not box or not box.get("line_score"):
        return ""
    ls = box["line_score"]
    if len(ls) < 2:
        return ""
    try:
        # Line score: row 0 = away, row 1 = home. R column is 3rd from end.
        # our_team_is_home from title: "vs" = home, "at" = away
        away_r = int(ls[0][-3])
        home_r = int(ls[1][-3])
        if box.get("our_team_is_home"):
            score_our, score_opp = home_r, away_r
        else:
            score_our, score_opp = away_r, home_r
        wl = "W" if score_our > score_opp else ("L" if score_opp > score_our else "T")
        return f"{wl} {score_our}-{score_opp}"
    except (ValueError, IndexError):
        return ""


def run(base_url: str, school_slug: str, season: str = "2026",
        school_name: str = "", no_season: bool = False, max_games_back: int = 5):
    os.makedirs(STATS_DIR, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Scraping stats for: {base_url}")
    print(f"Season: {season}")
    print(f"{'='*60}\n")

    # Load players.json for this school
    print("Loading players.json...")
    with open(PLAYERS_JSON) as f:
        all_players = json.load(f)

    # Filter to this school — try exact school_name first, then slug matching
    if school_name:
        school_players = [p for p in all_players if p.get("school") == school_name]
    else:
        school_players = []

    if not school_players:
        slug_words = set(school_slug.lower().replace("-", " ").split())
        school_players = [
            p for p in all_players
            if slug_words.issubset(set(p.get("school", "").lower().split()))
            or any(w in (p.get("profile_url") or "").lower() for w in slug_words if len(w) > 4)
        ]
    print(f"  Found {len(school_players)} players in players.json for '{school_name or school_slug}'")

    # --- Season batting stats ---
    print("\n[1/6] Season batting stats")
    batting_stats = scrape_season_batting(base_url, season, no_season)

    # --- Season pitching stats ---
    print("\n[2/6] Season pitching stats")
    pitching_stats = scrape_season_pitching(base_url, season, no_season)

    # --- W/L Record ---
    print("\n[3/6] Team record")
    record = scrape_record(base_url, season, no_season)
    if not record:
        record = scrape_record_from_gamelog(base_url, season, no_season)
    print(f"  Record: {record or '(not found)'}")

    # --- Find game URLs ---
    print("\n[4/6] Finding games")
    games = find_game_urls(base_url, season, no_season)
    most_recent = games[0] if games else None
    if games:
        print(f"  Found {len(games)} games; most recent: {most_recent['date']} vs {most_recent['opponent']}")
    else:
        print("  No games found via stats page (bio API will supply per-player data)")

    # --- Per-player last game: bio API (primary) + box score (fallback) ---
    # Players that have a profile_url also have a rosterPlayerId (the trailing number).
    # For those players we call the SIDEARM bio API directly, which returns a full
    # season game log — no box score fetching needed.
    # Players without a profile_url fall back to box score scanning as before.

    bio_pairs = []       # (player_record, roster_player_id)
    bio_player_ids = set()
    for sp in school_players:
        rid = get_roster_player_id(sp.get("profile_url"))
        if rid:
            bio_pairs.append((sp, rid))
            bio_player_ids.add(sp["id"])

    fallback_count = len(school_players) - len(bio_pairs)
    print(f"  Bio API eligible: {len(bio_pairs)}, box-score fallback: {fallback_count}")

    # --- [5a/6] Bio API: fetch per-player game logs ---
    bio_last_game = {}  # player_id -> last_game_data dict
    if bio_pairs:
        print(f"\n[5a/6] Bio API: fetching game logs for {len(bio_pairs)} players")
        for i, (sp, roster_id) in enumerate(bio_pairs):
            bio = fetch_bio_api(base_url, roster_id, season)
            if bio:
                lg = parse_bio_last_game(bio)
                if lg:
                    bio_last_game[sp["id"]] = lg
            time.sleep(0.3)
            if (i + 1) % 20 == 0 or (i + 1) == len(bio_pairs):
                print(f"  {i+1}/{len(bio_pairs)} fetched — {len(bio_last_game)} with last-game data")

    # --- [5b/6] Box score scan: fallback for players without rosterPlayerId ---
    player_last_game = {}  # ln_key -> last_game_data dict
    game_recaps = {}       # date_str -> {recap, recap_url}

    if (fallback_count > 0 or not bio_pairs) and games:
        print(f"\n[5b/6] Box score scan for {fallback_count} fallback players (up to {max_games_back} games)")
        games_to_scan = games[:max_games_back]

        for gi, game in enumerate(games_to_scan):
            print(f"\n  --- Game {gi+1}/{len(games_to_scan)}: {game['date']} vs {game['opponent']} ---")
            box = scrape_boxscore(game["boxscore_url"])
            if not box:
                print(f"  Could not parse box score, skipping")
                continue

            result_str = parse_result_from_box(box) or game.get("result_from_log", "")
            print(f"  Result: {result_str or '(unknown)'}")

            def index_box(rows):
                idx = {}
                for row in rows:
                    fn, ln = parse_boxscore_name(row["name"])
                    key = normalize_name(ln)
                    idx[key] = {"fn": fn, "ln": ln, **{k: v for k, v in row.items() if k != "name"}}
                return idx

            bat_idx = index_box(box.get("our_batting", []))
            pitch_idx = index_box(box.get("our_pitching", []))

            game_player_keys = set(bat_idx.keys()) | set(pitch_idx.keys())
            new_found = 0
            for ln_key in game_player_keys:
                if ln_key in player_last_game:
                    continue

                bat_data = None
                if ln_key in bat_idx:
                    b = bat_idx[ln_key]
                    bat_data = {
                        "ab": b.get("ab", 0), "r": b.get("r", 0), "h": b.get("h", 0),
                        "rbi": b.get("rbi", 0), "bb": b.get("bb", 0), "so": b.get("so", 0),
                        "pos": b.get("pos", ""),
                    }

                pitch_data = None
                if ln_key in pitch_idx:
                    pt = pitch_idx[ln_key]
                    pitch_data = {
                        "ip": pt.get("ip", "0.0"), "h": pt.get("h", 0), "r": pt.get("r", 0),
                        "er": pt.get("er", 0), "bb": pt.get("bb", 0), "so": pt.get("so", 0),
                        "wp": pt.get("wp", 0), "hbp": pt.get("hbp", 0),
                    }

                player_last_game[ln_key] = {
                    "date": game["date"],
                    "opponent": game["opponent"],
                    "result": result_str,
                    "boxscore_url": game["boxscore_url"],
                    "batting": bat_data,
                    "pitching": pitch_data,
                }
                new_found += 1

            print(f"  New players found: {new_found} (total tracked: {len(player_last_game)})")

            if game["date"] not in game_recaps:
                recap_url = find_recap_url(base_url, game["date"], game["opponent"])
                recap = scrape_recap(recap_url) if recap_url else {}
                game_recaps[game["date"]] = {"recap": recap, "recap_url": recap_url}

    # Fetch recaps for any bio API game dates not yet in the cache
    if bio_last_game:
        bio_dates = {(lg["date"], lg["opponent"]) for lg in bio_last_game.values() if lg.get("date")}
        for date_str, opp in sorted(bio_dates, reverse=True):
            if date_str not in game_recaps:
                recap_url = find_recap_url(base_url, date_str, opp)
                recap = scrape_recap(recap_url) if recap_url else {}
                game_recaps[date_str] = {"recap": recap, "recap_url": recap_url}

    # If we have no most_recent from the stats page, infer it from bio data
    if most_recent is None and bio_last_game:
        best = max(bio_last_game.values(), key=lambda lg: lg.get("date", ""))
        if best.get("date"):
            most_recent = {
                "date": best["date"],
                "opponent": best.get("opponent", "Unknown"),
                "boxscore_url": None,
            }

    # Guard: nothing to output at all
    if most_recent is None:
        print("  No game data found via stats page or bio API.", file=sys.stderr)
        return

    # --- [6/6] Assemble player records ---
    print("\n[6/6] Assembling player records")

    batting_by_ln = {normalize_name(b["ln"]): b for b in batting_stats}
    pitching_by_ln = {normalize_name(p["ln"]): p for p in pitching_stats}

    players_out = {}
    api_key_available = bool(os.environ.get("ANTHROPIC_API_KEY"))

    for sp in school_players:
        player_id = sp["id"]
        fn = sp.get("fn", "")
        ln = sp.get("ln", "")
        ln_key = normalize_name(ln)

        season_bat = batting_by_ln.get(ln_key, {}).get("season_batting")
        season_pitch = pitching_by_ln.get(ln_key, {}).get("season_pitching")

        # Bio API data takes priority; fall back to box score matching by last name
        last_game_data = bio_last_game.get(player_id)
        if last_game_data is None:
            plg = player_last_game.get(ln_key)
            if plg:
                last_game_data = {
                    "date": plg["date"],
                    "opponent": plg["opponent"],
                    "result": plg["result"],
                    "boxscore_url": plg["boxscore_url"],
                    "batting": plg["batting"],
                    "pitching": plg["pitching"],
                }

        if not season_bat and not season_pitch and not last_game_data:
            continue

        player_record = {
            "id": player_id,
            "name": sp.get("name", f"{fn} {ln}"),
            "fn": fn,
            "ln": ln,
            "pos": sp.get("pos", ""),
            "yr": sp.get("yr", ""),
            "num": sp.get("num"),
            "school": sp.get("school", ""),
            "season_batting": season_bat,
            "season_pitching": season_pitch,
            "last_game": last_game_data,
            "narrative": None,
        }

        if last_game_data:
            had_meaningful_bat = last_game_data.get("batting") and last_game_data["batting"].get("ab", 0) > 0
            had_meaningful_pitch = (
                last_game_data.get("pitching")
                and str(last_game_data["pitching"].get("ip", "0")) not in ("0.0", "0", 0)
            )

            if had_meaningful_bat or had_meaningful_pitch:
                game_date = last_game_data["date"]
                game_recap_data = game_recaps.get(game_date, {})
                recap = game_recap_data.get("recap", {})

                game_info = {
                    "date": game_date,
                    "opponent": last_game_data["opponent"],
                    "result": last_game_data["result"],
                }

                print(f"  Generating narrative for {sp.get('name', fn + ' ' + ln)}...", end=" ")
                if api_key_available:
                    narrative = generate_narrative_claude(
                        {**player_record, "name": sp.get("name", "")},
                        game_info,
                        recap.get("article_text", ""),
                    )
                else:
                    narrative = generate_narrative_template(player_record, game_info)
                player_record["narrative"] = narrative
                print("done" if narrative else "no narrative")

        players_out[player_id] = player_record

    # --- Get recap for most recent team game (for top-level output) ---
    most_recent_recap = game_recaps.get(most_recent["date"], {})
    recap = most_recent_recap.get("recap", {})
    recap_url = most_recent_recap.get("recap_url")

    # Derive the most recent result from any player's last-game data on that date
    most_recent_result = ""
    all_last_games = list(bio_last_game.values()) + list(player_last_game.values())
    for lg in all_last_games:
        if lg.get("date") == most_recent["date"] and lg.get("result"):
            most_recent_result = lg["result"]
            break

    # --- Output ---
    output = {
        "school": school_name or school_slug.replace("-", " ").title(),
        "base_url": base_url,
        "season": season,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "record": record,
        "most_recent_game": {
            "date": most_recent["date"],
            "opponent": most_recent["opponent"],
            "result": most_recent_result,
            "boxscore_url": most_recent["boxscore_url"],
            "recap_url": recap_url,
            "recap_headline": recap.get("headline", ""),
            "recap_text": recap.get("article_text", ""),
        },
        "players": players_out,
        "narratives_generated_by": "claude-haiku-4-5-20251001" if api_key_available else "template",
    }

    out_path = os.path.join(STATS_DIR, f"{school_slug}.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Output written to: {out_path}")
    print(f"Record: {record}")
    print(f"Players with stats: {len(players_out)}")
    print(f"Per-player last-game source: bio API={len(bio_last_game)}, box score={len(player_last_game)}")
    print(f"Narratives: {'AI-generated' if api_key_available else 'template-based (set ANTHROPIC_API_KEY for AI)'}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

KNOWN_SCHOOLS = {
    "davidson": {
        "base_url": "https://www.davidsonwildcats.com",
        "slug": "davidson-college",
        "school_name": "Davidson College",
    },
    "ucsb": {
        "base_url": "https://ucsbgauchos.com",
        "slug": "uc-santa-barbara",
        "school_name": "University of California, Santa Barbara",
    },
    "stanford": {
        "base_url": "https://gostanford.com",
        "slug": "stanford-university",
        "school_name": "Stanford University",
        "stats_url_no_season": True,
    },
    "florida": {
        "base_url": "https://floridagators.com",
        "slug": "university-of-florida",
        "school_name": "University of Florida",
    },
    "tcu": {
        "base_url": "https://gofrogs.com",
        "slug": "texas-christian-university",
        "school_name": "Texas Christian University",
    },
    "texas": {
        "base_url": "https://texassports.com",
        "slug": "university-of-texas-at-austin",
        "school_name": "University of Texas at Austin",
    },
    "mizzou": {
        "base_url": "https://mutigers.com",
        "slug": "university-of-missouri-columbia",
        "school_name": "University of Missouri, Columbia",
    },
}


def main():
    parser = argparse.ArgumentParser(description="Scrape baseball stats for a school")
    parser.add_argument("--school", default="davidson", help="School shortname (e.g. davidson, ucsb, stanford)")
    parser.add_argument("--base-url", help="Override base URL")
    parser.add_argument("--slug", help="Override output file slug")
    parser.add_argument("--school-name", help="Exact school name (for player matching)")
    parser.add_argument("--season", default="2026", help="Season year (default: 2026)")
    parser.add_argument("--max-games-back", type=int, default=5,
                        help="Max box scores to scan for per-player last-game (default: 5)")
    parser.add_argument("--no-season-url", action="store_true",
                        help="Omit season year from stats URL")
    args = parser.parse_args()

    if args.school in KNOWN_SCHOOLS:
        cfg = KNOWN_SCHOOLS[args.school]
        base_url = args.base_url or cfg["base_url"]
        slug = args.slug or cfg["slug"]
        school_name = args.school_name or cfg.get("school_name", "")
        no_season = args.no_season_url or cfg.get("stats_url_no_season", False)
    else:
        base_url = args.base_url
        slug = args.slug or args.school
        school_name = args.school_name or ""
        no_season = args.no_season_url
        if not base_url:
            print(f"Unknown school '{args.school}'. Provide --base-url.", file=sys.stderr)
            sys.exit(1)

    run(base_url, slug, args.season, school_name=school_name,
        no_season=no_season, max_games_back=args.max_games_back)


if __name__ == "__main__":
    main()
