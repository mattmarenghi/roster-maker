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
    """Parse common date formats returned by SIDEARM bio API.

    SIDEARM v2 returns dates with a time component and a narrow no-break space
    before AM/PM, e.g. "2/13/2026 3:30:00\u202fPM".  Only the date portion is
    needed, so we normalise and strip everything after the first space.
    """
    if not date_str:
        return None
    # Normalise narrow no-break space (U+202F) → regular space, then take date only
    s = str(date_str).strip().replace("\u202f", " ").split()[0]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_bio_last_game(bio: dict, base_url: str = "") -> dict | None:
    """Parse a SIDEARM /api/v2/stats/bio response to find the player's most recent game.

    Actual SIDEARM bio response shape:
        {
          "currentStats": {
            "hittingStats": [{date, opponent, result, atBats, runsScored, hits, ...}, ...],
            "pitchingStats": [{date, opponent, result, inningsPitched, hitsAllowed, ...}, ...]
          }
        }
    Field names vary by school; this function tries common variants.

    Returns {date, opponent, result, boxscore_url, batting, pitching} or None.
    """
    if not isinstance(bio, dict):
        return None

    def find_log(hitting_key: str, pitching_key: str) -> list | None:
        # Primary path: currentStats.hittingStats / currentStats.pitchingStats
        current = bio.get("currentStats")
        if isinstance(current, dict):
            log = current.get(hitting_key) or current.get(pitching_key)
            if isinstance(log, list):
                return log
        # Legacy fallback: top-level "batting"/"pitching" with nested gameLog
        section = bio.get(hitting_key) or bio.get(pitching_key)
        if isinstance(section, dict):
            for key in ("gameLog", "gameLogs", "game_log", "games", "log"):
                if isinstance(section.get(key), list):
                    return section[key]
        return None

    bat_log = find_log("hittingStats", "batting")
    pitch_log = find_log("pitchingStats", "pitching")

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
    # Strip leading "at " (away game) or "@" prefix
    opponent = re.sub(r"^(at\s+|@)", "", opponent_raw, flags=re.IGNORECASE).strip() or "Unknown"
    result = str(get_field(primary_entry, "result", "Result", "wl", "gameResult") or "")

    # Boxscore URL from bio API (relative path like /sports/baseball/stats/2026/team/boxscore/12345)
    bs_relative = get_field(primary_entry, "boxscoreUrl", "boxscore_url", "boxscoreLink")
    boxscore_url = urljoin(base_url, bs_relative) if bs_relative and base_url else None

    # Build batting stats dict
    bat_data = None
    if bat_entry:
        ab = safe_int(get_field(bat_entry, "ab", "atBats", "AB"))
        if ab > 0:
            bat_data = {
                "ab":      ab,
                "r":       safe_int(get_field(bat_entry, "r", "runsScored", "runs", "R")),
                "h":       safe_int(get_field(bat_entry, "h", "hits", "H")),
                "rbi":     safe_int(get_field(bat_entry, "rbi", "runsBattedIn", "RBI")),
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
                "er":  safe_int(get_field(pitch_entry, "er", "earnedRunsAllowed", "earnedRuns", "ER")),
                "bb":  safe_int(get_field(pitch_entry, "bb", "walksAllowed", "BB")),
                "so":  safe_int(get_field(pitch_entry, "so", "k", "strikeouts", "SO", "K")),
                "wp":  safe_int(get_field(pitch_entry, "wp", "wildPitches", "WP")),
                "hbp": safe_int(get_field(pitch_entry, "hbp", "hitBatters", "HBP")),
                "w":   safe_int(get_field(pitch_entry, "w", "wins", "pitchingWins", "W")),
                "l":   safe_int(get_field(pitch_entry, "l", "losses", "pitchingLosses", "L")),
                "sv":  safe_int(get_field(pitch_entry, "sv", "saves", "SV")),
            }

    if bat_data is None and pitch_data is None:
        return None

    return {
        "date": date_out,
        "opponent": opponent,
        "result": result,
        "boxscore_url": boxscore_url,
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
        # Old SIDEARM uses <th scope="row"> for the player name cell; new SIDEARM uses <td>.
        # Including both keeps column indices aligned with raw_headers for both formats.
        raw_cells = row.find_all(["td", "th"])
        cells = [c.get_text(strip=True) for c in raw_cells]
        if not cells or len(cells) < 5:
            continue

        # Extract player name from the name cell (index 1).
        # Old SIDEARM embeds a mobile jersey-number span inside the <th>, producing
        # garbled get_text() like "Parks, Sam5Parks, Sam". Use the <a> link text
        # when available to get a clean name.
        name_raw = ""
        if len(raw_cells) > 1:
            link = raw_cells[1].find("a")
            name_raw = link.get_text(strip=True) if link else cells[1]

        # Skip totals rows
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

    def rv(idx):
        return resolve_nuxt(nuxt_data, idx) if nuxt_data else None

    # Navigate: statsSeason -> cumulativeStats -> (key) -> overallIndividualStats
    # -> individualStats -> individualPitchingStats
    # Any missing step sets pitching_list=None and falls through to the HTML fallback.
    pitching_list = None
    if nuxt_data:
        stats_season_idx = None
        for i, item in enumerate(nuxt_data):
            if isinstance(item, dict) and "statsSeason" in item:
                stats_season_idx = item["statsSeason"]
                break

        stats_season = rv(stats_season_idx) if stats_season_idx is not None else None
        cumul_idx = stats_season.get("cumulativeStats") if isinstance(stats_season, dict) else None
        cumul = rv(cumul_idx) if cumul_idx is not None else None
        first_key = next(iter(cumul), None) if isinstance(cumul, dict) else None
        main_stats = rv(cumul[first_key]) if first_key is not None else None
        overall_idx = main_stats.get("overallIndividualStats") if isinstance(main_stats, dict) else None
        overall = rv(overall_idx) if overall_idx is not None else None
        indiv_idx = overall.get("individualStats") if isinstance(overall, dict) else None
        indiv = rv(indiv_idx) if indiv_idx is not None else None
        pitching_list_idx = indiv.get("individualPitchingStats") if isinstance(indiv, dict) else None
        candidate = rv(pitching_list_idx) if pitching_list_idx is not None else None
        if isinstance(candidate, list):
            pitching_list = candidate

    pitchers = []
    for pitcher_idx in (pitching_list or []):
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
        # Include <th scope="row"> name cells so column indices align with raw_headers
        # (same fix as scrape_season_batting — old SIDEARM uses th for the name cell)
        raw_cells = row.find_all(["td", "th"])
        cells = [c.get_text(strip=True) for c in raw_cells]
        if not cells or len(cells) < 10:
            continue

        # Extract player name: prefer <a> link (avoids mobile-span clutter in old SIDEARM)
        name_raw = ""
        if len(raw_cells) > 1:
            link = raw_cells[1].find("a")
            name_raw = link.get_text(strip=True) if link else cells[1]
        if not name_raw:
            link = row.find("a")
            name_raw = link.get_text(strip=True) if link else ""

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

def _normalize_time_et(time_str: str) -> str:
    """Convert SIDEARM time string (e.g. '3 p.m.', '12:30 p.m.') to '3:00 PM ET' format."""
    if not time_str:
        return ""
    s = time_str.strip()
    if s.upper() in ("TBA", "TBD"):
        return "TBA"
    s = s.replace("p.m.", "PM").replace("a.m.", "AM").replace("P.M.", "PM").replace("A.M.", "AM")
    # Add :00 if no colon (e.g. "3 PM" -> "3:00 PM")
    s = re.sub(r"^(\d+)\s+(AM|PM)$", r"\1:00 \2", s.strip())
    s = re.sub(r"\s+", " ", s).strip()
    if s and not s.endswith("ET"):
        s += " ET"
    return s


_WATCH_URL_RE = re.compile(
    r"(espn\.com|espnplus\.com|watchespn\.com|flotrack\.org|stretchinternet\.com|"
    r"nfhsnetwork\.com|conferenceusa\.com|sunbelttv|bigtennetwork|espn\+)",
    re.IGNORECASE,
)


def _extract_watch_url_nuxt(item: dict, rv_local) -> str | None:
    """Try to extract a streaming/watch URL from a NUXT schedule game object.

    SIDEARM stores the watch link at: game.media -> {video: ...} -> {url: "https://espn.com/..."}
    Falls back to scanning all top-level fields for URL strings.
    """
    # Primary: media.video.url (SIDEARM standard location for ESPN/streaming links)
    media_raw = item.get("media")
    if media_raw is not None:
        media = rv_local(media_raw)
        if isinstance(media, dict):
            video_raw = media.get("video")
            if video_raw is not None:
                video = rv_local(video_raw)
                if isinstance(video, dict):
                    url_raw = video.get("url")
                    if url_raw is not None:
                        url = rv_local(url_raw)
                        if isinstance(url, str) and url.startswith("http"):
                            return url

    # Fallback: scan all top-level fields for URL strings or link lists
    for key, raw_val in item.items():
        val = rv_local(raw_val)
        # Direct string URL
        if isinstance(val, str) and val.startswith("http") and _WATCH_URL_RE.search(val):
            return val
        # List of link objects e.g. [{label: "Watch", href: "https://espn.com/..."}]
        if isinstance(val, list):
            for link_raw in val:
                link = rv_local(link_raw)
                if isinstance(link, dict):
                    href = rv_local(link.get("href") or link.get("url") or "")
                    if isinstance(href, str) and href.startswith("http") and _WATCH_URL_RE.search(href):
                        return href
                elif isinstance(link, str) and link.startswith("http") and _WATCH_URL_RE.search(link):
                    return link
    return None


def _extract_watch_url_html_row(raw_cells) -> str | None:
    """Extract a streaming/watch URL from HTML table row cells."""
    for cell in raw_cells:
        for a in cell.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True).lower()
            if href.startswith("http") and _WATCH_URL_RE.search(href):
                return href
            if text in ("watch", "espn+", "espn", "live", "stream") and href.startswith("http"):
                return href
    return None


def scrape_next_game(base_url: str) -> tuple[dict | None, str | None]:
    """
    Scrape the team schedule page to find the next upcoming game and the
    most recently played game's watch URL (if any).
    Returns (next_game_dict, most_recent_watch_url).
    next_game_dict has keys: date, opponent, home_away, time_et, watch_url (may be None).
    most_recent_watch_url is a streaming URL string or None.
    """
    schedule_url = f"{base_url}/sports/baseball/schedule"
    print(f"  Fetching schedule: {schedule_url}")
    html = fetch(schedule_url)
    if not html:
        return None, None
    time.sleep(DELAY)

    today = datetime.utcnow().date()

    # --- NUXT data approach (new SIDEARM Nuxt platform) ---
    # Game objects in NUXT are dicts with keys: date, opponent, result, location_indicator, time
    # Field values are indices into the NUXT array (dedup format).
    nuxt_data = extract_nuxt_data(html)
    if nuxt_data:
        def rv_local(idx, depth=0):
            if depth > 10 or not isinstance(idx, (int, str)):
                return idx
            try:
                i = int(idx)
            except (ValueError, TypeError):
                return idx
            if i >= len(nuxt_data):
                return idx
            val = nuxt_data[i]
            if isinstance(val, list) and val and val[0] in ("ShallowReactive", "Reactive", "Set", "Map"):
                return rv_local(val[1], depth + 1)
            return val

        future_games = []
        past_games = []
        for item in nuxt_data:
            if not isinstance(item, dict):
                continue
            # SIDEARM schedule game objects have these keys
            if not all(k in item for k in ["date", "opponent", "result", "location_indicator"]):
                continue
            date_val = rv_local(item["date"])
            if not isinstance(date_val, str):
                continue
            try:
                game_date = datetime.fromisoformat(date_val.split("T")[0]).date()
            except (ValueError, AttributeError):
                continue
            result_val = rv_local(item["result"])
            result_status = ""
            if isinstance(result_val, dict):
                result_status = rv_local(result_val.get("status", "")) or ""

            watch_url = _extract_watch_url_nuxt(item, rv_local)

            if game_date >= today and not result_status:
                # Upcoming game
                opponent_val = rv_local(item["opponent"])
                opp_name = ""
                if isinstance(opponent_val, dict):
                    opp_name = str(rv_local(opponent_val.get("title", "")) or "").strip()
                if not opp_name:
                    continue
                loc_raw = rv_local(item.get("location_indicator", "H")) or "H"
                loc_map = {"H": "home", "A": "away", "N": "neutral"}
                home_away = loc_map.get(str(loc_raw).upper(), "home")
                time_raw = str(rv_local(item.get("time", "")) or "")
                future_games.append({
                    "date": game_date.strftime("%Y-%m-%d"),
                    "date_obj": game_date,
                    "opponent": opp_name,
                    "home_away": home_away,
                    "time_et": _normalize_time_et(time_raw),
                    "watch_url": watch_url,
                })
            elif game_date < today and result_status:
                # Already played — track for most_recent_watch_url
                past_games.append({
                    "date_obj": game_date,
                    "watch_url": watch_url,
                })

        most_recent_watch_url = None
        if past_games:
            past_games.sort(key=lambda g: g["date_obj"], reverse=True)
            most_recent_watch_url = past_games[0]["watch_url"]

        if future_games:
            future_games.sort(key=lambda g: g["date_obj"])
            best = {k: v for k, v in future_games[0].items() if k != "date_obj"}
            print(f"  Next game (NUXT): {best['date']} {best['home_away']} {best['time_et']} vs {best['opponent']}")
            if best.get("watch_url"):
                print(f"  Next game watch URL: {best['watch_url']}")
            if most_recent_watch_url:
                print(f"  Most recent game watch URL: {most_recent_watch_url}")
            return best, most_recent_watch_url

        return None, most_recent_watch_url

    # --- HTML table fallback (older SIDEARM sites) ---
    soup = BeautifulSoup(html, "html.parser")
    most_recent_watch_url = None
    for table in soup.find_all("table"):
        header_cells = table.find_all("th")
        if not header_cells:
            continue
        headers = [th.get_text(strip=True).lower() for th in header_cells]
        if "date" not in headers:
            continue
        opp_idx = next((i for i, h in enumerate(headers) if "opponent" in h or h == "opp"), None)
        if opp_idx is None:
            continue
        date_idx = headers.index("date")
        result_idx = next(
            (i for i, h in enumerate(headers) if h in ("result", "score", "w/l", "wl", "w-l")), None
        )
        loc_idx = next(
            (i for i, h in enumerate(headers) if h in ("location", "h/a", "home/away", "site", "loc")), None
        )
        time_idx = next(
            (i for i, h in enumerate(headers) if h in ("time", "start time")), None
        )
        next_game_result = None
        for row in table.find_all("tr")[1:]:
            raw_cells = row.find_all(["td", "th"])
            cells = [c.get_text(strip=True) for c in raw_cells]
            if len(cells) <= max(date_idx, opp_idx):
                continue
            date_str = cells[date_idx]
            game_date = None
            for fmt in ("%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
                try:
                    game_date = datetime.strptime(date_str, fmt).date()
                    break
                except ValueError:
                    pass
            if not game_date:
                continue
            row_watch_url = _extract_watch_url_html_row(raw_cells)
            if game_date < today:
                # Track most recently played game watch URL
                if row_watch_url:
                    most_recent_watch_url = row_watch_url
                continue
            if result_idx is not None and result_idx < len(cells):
                result = cells[result_idx].strip()
                if result and result not in ("-", "\u2013", "\u2014", "TBD", ""):
                    if re.match(r"^[WLT]\s", result, re.IGNORECASE):
                        if row_watch_url:
                            most_recent_watch_url = row_watch_url
                        continue
            if next_game_result is not None:
                continue  # Already found next game; keep scanning for most_recent_watch_url
            opponent_raw = cells[opp_idx]
            if len(raw_cells) > opp_idx:
                link = raw_cells[opp_idx].find("a")
                if link:
                    opponent_raw = link.get_text(strip=True)
            home_away = "home"
            if re.match(r"^(at\s+|@)", opponent_raw, re.IGNORECASE):
                home_away = "away"
                opponent_raw = re.sub(r"^(at\s+|@\s*)", "", opponent_raw, flags=re.IGNORECASE).strip()
            elif loc_idx is not None and loc_idx < len(cells):
                loc = cells[loc_idx].lower().strip()
                if loc in ("a", "away", "at", "@"):
                    home_away = "away"
                elif loc in ("n", "neutral"):
                    home_away = "neutral"
            opponent = opponent_raw.strip()
            if not opponent or opponent.lower() in ("tbd", ""):
                continue
            time_raw = cells[time_idx].strip() if time_idx is not None and time_idx < len(cells) else ""
            print(f"  Next game (HTML): {game_date} {home_away} vs {opponent}")
            next_game_result = {
                "date": game_date.strftime("%Y-%m-%d"),
                "opponent": opponent,
                "home_away": home_away,
                "time_et": _normalize_time_et(time_raw),
                "watch_url": row_watch_url,
            }
        if next_game_result is not None:
            return next_game_result, most_recent_watch_url

    return None, most_recent_watch_url


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
            raw_cells = row.find_all(["td", "th"])
            cells = [td.get_text(strip=True) for td in raw_cells]
            if len(cells) < 5:
                continue

            name = cells[0]
            pos = cells[1] if len(cells) > 1 else ""

            # Old SIDEARM (.aspx) box scores embed position abbreviations in cells[0]
            # rather than (or in addition to) player names.  Several variants exist:
            #   Format A: cells[0]="cf", cells[1]="cfTruitt, Brantley" (prefix repeated)
            #   Format B: cells[0]="cf", cells[1]="Truitt, Brantley"   (no prefix)
            #   Format C: cells[0]="3b", cells[1]="3bBarbour, Jake"    (digit prefix)
            #   Format D: cells[0]="PH/rf", cells[1]="PH/rfPolk, Landon" (multi-pos)
            # New SIDEARM: cells[0]="Truitt, Brantley", cells[1]="cf"  (name first)
            # Detect any position-in-cells[0] case and extract the clean player name.
            _pos_cell = (
                name
                and (
                    pos.lower().startswith(name.lower())  # Format A/C/D: prefix match
                    or (not name[0].isupper() and len(name) <= 4)  # Format B: short lowercase
                    or "/" in name  # Format D: multi-position slash
                    or (len(name) <= 3 and any(c.isdigit() for c in name))  # Format C: "3b"
                )
                and not ("," in name or " " in name)  # exclude real names like "Smith, J"
            )
            if _pos_cell:
                pos = name
                link = raw_cells[1].find("a") if len(raw_cells) > 1 else None
                if link:
                    name = link.get_text(strip=True)
                elif pos.lower() and cells[1].lower().startswith(pos.lower()):
                    name = cells[1][len(pos):]  # strip leading position prefix (Format A/C/D)
                else:
                    name = cells[1]  # Format B: cells[1] is the bare name

            if not name or name.lower() in ("totals", ""):
                continue

            def si(idx):
                try:
                    return int(cells[idx])
                except (ValueError, IndexError):
                    return 0

            out.append({
                "name": name,
                "pos": pos,
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

            # Extract pitcher decision from name cell, e.g. "Hoyt (S, 4)" or "Smith (W, 2-1)"
            dec_w = dec_l = dec_sv = 0
            dec_match = re.search(r'\(([WLS]|SV)(?:[,\s]|$)', name, re.IGNORECASE)
            if dec_match:
                dec = dec_match.group(1).upper()
                name = name[:dec_match.start()].strip()
                if dec == "W":
                    dec_w = 1
                elif dec in ("S", "SV"):
                    dec_sv = 1
                elif dec == "L":
                    dec_l = 1

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
                "w": dec_w, "l": dec_l, "sv": dec_sv,
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

    # Assign batting/pitching tables.
    # In a standard box score the away team is listed first, home team second.
    # However older SIDEARM (.aspx) sites do not reliably reflect this in the
    # page title, so we populate both orderings and let the caller pick the
    # one with more matching players (see run()).
    if len(batting_tables) >= 2:
        result["opponent_batting"] = parse_batting_table(batting_tables[0])
        result["our_batting"]      = parse_batting_table(batting_tables[1])
        result["alt_batting"]      = parse_batting_table(batting_tables[0])  # swap candidate
    elif len(batting_tables) == 1:
        result["our_batting"] = parse_batting_table(batting_tables[0])

    if len(pitching_tables) >= 2:
        result["opponent_pitching"] = parse_pitching_table(pitching_tables[0])
        result["our_pitching"]      = parse_pitching_table(pitching_tables[1])
        result["alt_pitching"]      = parse_pitching_table(pitching_tables[0])  # swap candidate
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
    # Supports both old SIDEARM (.story-page__content__title-box) and new Nuxt platform (.article-head)
    headline_el = (
        soup.select_one(".story-page__content__title-box h1")
        or soup.select_one(".article-head h1")
        or soup.select_one(".s-page-title")
        or soup.select_one("h1.s-title")
        or soup.select_one("h1")
    )
    headline_raw = headline_el.get_text(strip=True) if headline_el else ""
    # Strip date/sport suffix often appended e.g. "Title | 2/24/2026|Baseball"
    headline = re.split(r"\s*[\|]\s*\d", headline_raw)[0].strip() if headline_raw else ""

    # Article body: old SIDEARM uses .story-page__content__body;
    # new Nuxt-based SIDEARM uses .embed-html inside .text-content
    body_el = (
        soup.select_one(".story-page__content__body")
        or soup.select_one(".text-content .embed-html")
        or soup.select_one(".embed-html")
        or soup.select_one(".content-blocks")
    )
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
    """Generate a compact narrative using Claude API."""
    try:
        import anthropic
    except ImportError:
        return generate_narrative_template(player, game_info)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return generate_narrative_template(player, game_info)

    client = anthropic.Anthropic(api_key=api_key)

    # Build last-game stats string
    last_game = player.get("last_game", {})
    batting = last_game.get("batting")
    pitching = last_game.get("pitching")

    if batting and batting.get("ab", 0) > 0:
        b = batting
        parts = [f"{b['h']}-for-{b['ab']}"]
        if b.get("doubles"): parts.append(f"{b['doubles']} 2B")
        if b.get("triples"): parts.append(f"{b['triples']} 3B")
        if b.get("hr"):      parts.append(f"{b['hr']} HR")
        if b.get("rbi"):     parts.append(f"{b['rbi']} RBI")
        if b.get("r"):       parts.append(f"{b['r']} R")
        if b.get("bb"):      parts.append(f"{b['bb']} BB")
        if b.get("so"):      parts.append(f"{b['so']} K")
        stats_str = "Batting: " + ", ".join(parts)
    elif pitching and str(pitching.get("ip", "0")) not in ("0", "0.0"):
        p = pitching
        stats_str = (
            f"Pitching: {p['ip']} IP, {p['h']} H, {p['er']} ER, {p['bb']} BB, {p['so']} K"
        )
        if p.get("wp"):  stats_str += f", {p['wp']} WP"
        if p.get("hbp"): stats_str += f", {p['hbp']} HBP"
        if p.get("w"):   stats_str += " (W)"
        elif p.get("l"): stats_str += " (L)"
        elif p.get("sv"): stats_str += " (SV)"
    else:
        return ""  # player didn't appear in this game

    # Build season context string
    sb = player.get("season_batting") or {}
    sp = player.get("season_pitching") or {}
    season_lines = []
    if sb.get("ab", 0) > 0:
        ba = sb["h"] / sb["ab"]
        season_lines.append(
            f"{sb.get('g', '?')} G, .{int(ba * 1000):03d} AVG, "
            f"{sb['h']} H, {sb.get('hr', 0)} HR, {sb['rbi']} RBI, {sb.get('bb', 0)} BB"
        )
    if sp.get("ip") and str(sp.get("ip", "0")) not in ("0", "0.0"):
        try:
            ip_f = float(str(sp["ip"]).split("+")[0])
            era = round(sp.get("er", 0) * 9 / ip_f, 2) if ip_f > 0 else 0.0
            k9  = round(sp.get("so", 0) * 9 / ip_f, 1) if ip_f > 0 else 0.0
        except (ValueError, TypeError):
            era = 0.0
            k9  = 0.0
        season_lines.append(
            f"{sp['ip']} IP, {sp.get('so', 0)} K, {era:.2f} ERA, {k9:.1f} K/9"
        )
    season_str = ("Season: " + " | ".join(season_lines)) if season_lines else ""

    opponent  = game_info.get("opponent", "the opponent")
    date      = game_info.get("date", "")
    result    = game_info.get("result", "")
    last_name = player["name"].split()[-1]

    prompt = f"""You are writing a compact player spotlight for a college baseball scouting database, read on mobile. Brevity matters.

Player: {player['name']} (last name: {last_name}) | {player.get('pos', '')} | {player.get('yr', '')} | {player.get('school', '')}
Game ({date}): vs {opponent} — {"W" if result == "W" else "L"}
{stats_str}
{season_str}

Game recap:
{article_text[:2500] if article_text else '[No recap available]'}

Write 2 tight sentences — 3 only if a third adds something a scout genuinely needs. Lead with the most specific detail available: pull a situation or moment from the recap (a key at-bat, a jam, a specific inning) rather than just restating the stat line. The final sentence anchors the season context concisely.

Rules:
- No negative or critical evaluations (no "struggled", "failed to", "couldn't", "inconsistent", "hasn't translated", "unraveled")
- Positive-leaning observations are fine if supported by the stats
- Never refer to the player by position abbreviation (RHP, LHP, 1B, etc.) — use last name or a natural pronoun
- Do not open with a generic "went X-for-Y" — find a more specific entry point when possible
- Facts and stats only; no scouting opinions or projections
- No markdown. Past tense."""

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

    has_meaningful_pitch = pitching and str(pitching.get("ip", "0")) not in ("0", "0.0")
    has_meaningful_bat = batting and batting.get("ab", 0) > 0

    if not has_meaningful_pitch and not has_meaningful_bat:
        return ""

    # Build season context suffix
    sb = player.get("season_batting") or {}
    sp = player.get("season_pitching") or {}
    season_note = ""
    if has_meaningful_bat and sb.get("ab", 0) > 0:
        ba = sb["h"] / sb["ab"]
        season_note = f" Hitting .{int(ba * 1000):03d} with {sb.get('hr', 0)} HR through {sb.get('g', '?')} games."
    elif has_meaningful_pitch and sp.get("ip") and str(sp.get("ip", "0")) not in ("0", "0.0"):
        try:
            ip_f = float(str(sp["ip"]).split("+")[0])
            era = round(sp.get("er", 0) * 9 / ip_f, 2) if ip_f > 0 else 0.0
            season_note = f" At {era:.2f} ERA through {sp['ip']} innings on the season."
        except (ValueError, TypeError):
            pass

    if has_meaningful_bat and not (has_meaningful_pitch and not has_meaningful_bat):
        avg = f"{batting['h']}/{batting['ab']}"
        hr_note = f", {batting['hr']} HR" if batting.get("hr", 0) else ""
        line = f"{avg} with {batting['rbi']} RBI{hr_note} vs. {opponent} ({result})."
        return line + season_note

    if has_meaningful_pitch:
        try:
            ip_float = float(str(pitching["ip"]).split("+")[0])
        except (ValueError, TypeError):
            ip_float = 0.0
        if ip_float >= 5:
            line = (
                f"{pitching['ip']} IP, {pitching['so']} K, {pitching['er']} ER "
                f"vs. {opponent} ({result})."
            )
        else:
            line = (
                f"{pitching['ip']} IP, {pitching['so']} K vs. {opponent} ({result})."
            )
        return line + season_note

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

    # Load existing stats file to cache narratives for players whose last game hasn't changed
    existing_path = os.path.join(STATS_DIR, f"{school_slug}.json")
    narrative_cache = {}  # player_id -> {date, narrative}
    if os.path.exists(existing_path):
        try:
            with open(existing_path) as f:
                existing = json.load(f)
            for pid, p in existing.get("players", {}).items():
                lg = p.get("last_game") or {}
                if lg.get("date") and p.get("narrative"):
                    narrative_cache[pid] = {"date": lg["date"], "narrative": p["narrative"]}
            if narrative_cache:
                print(f"  Narrative cache loaded: {len(narrative_cache)} existing narratives")
        except Exception:
            pass  # missing or corrupt existing file — regenerate everything

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

    # --- Next upcoming game ---
    print("\n[3b/6] Next upcoming game")
    next_game, most_recent_watch_url = scrape_next_game(base_url)
    if next_game:
        print(f"  Next game: {next_game['date']} {next_game['home_away']} vs {next_game['opponent']}")
    else:
        print("  Next game: (not found)")

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
                lg = parse_bio_last_game(bio, base_url)
                if lg:
                    bio_last_game[sp["id"]] = lg
            time.sleep(0.3)
            if (i + 1) % 20 == 0 or (i + 1) == len(bio_pairs):
                print(f"  {i+1}/{len(bio_pairs)} fetched — {len(bio_last_game)} with last-game data")

    # --- [5b/6] Box score scan: fallback for players without rosterPlayerId ---
    player_last_game = {}  # ln_key -> last_game_data dict
    game_recaps = {}       # date_str -> {recap, recap_url}

    # Detect bio API lag: the bio API's per-player game logs often haven't been updated
    # for the most recent game yet when the morning scrape runs, even though the stats
    # page and game URL list are already current.  When this happens, supplement with
    # a box score scan of just the latest game so players get fresh last-game data.
    bio_most_recent_date = max(
        (lg.get("date", "") for lg in bio_last_game.values()),
        default="",
    ) if bio_last_game else ""
    bio_is_stale = (
        bool(bio_last_game)
        and most_recent is not None
        and bio_most_recent_date < most_recent["date"]
    )
    if bio_is_stale:
        print(
            f"  Bio API lag detected: newest bio date={bio_most_recent_date}, "
            f"most recent game={most_recent['date']} — supplementing with box score scan"
        )

    # Also run the box score scan when the bio API yielded nothing — this happens for
    # schools on the older SIDEARM platform (boxscore.aspx URLs) where the /api/v2/stats/bio
    # endpoint is unavailable or returns an incompatible format.  Without this, enriched
    # schools (all players have profile_url → fallback_count==0 and bio_pairs is full) would
    # silently skip box-score scanning even though bio API returned zero results.
    bio_api_yielded_nothing = bool(bio_pairs) and not bio_last_game
    if (fallback_count > 0 or not bio_pairs or bio_api_yielded_nothing or bio_is_stale) and games:
        # When bio data is merely stale (API did return data, just not for the latest game)
        # and there are no fallback players, only scan the single most recent game — that is
        # all we need to fill the gap.  For other cases scan up to max_games_back.
        if bio_is_stale and not bio_api_yielded_nothing and fallback_count == 0:
            games_to_scan = games[:1]
        else:
            games_to_scan = games[:max_games_back]
        print(f"\n[5b/6] Box score scan for {fallback_count} fallback players ({len(games_to_scan)} game(s))")

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

            # Determine which batting/pitching table belongs to our school.
            # The default assignment (tables[1]=ours) works for SIDEARM v3+ and for
            # new-format home games. Old SIDEARM (.aspx) pages may put our team in
            # tables[0] and don't reliably indicate home/away in the page title.
            # Compare name-match counts against school_players to pick the right one.
            school_lns = {normalize_name(p["ln"]) for p in school_players}

            def count_school_matches(rows):
                return sum(
                    1 for r in rows
                    if normalize_name(parse_boxscore_name(r["name"])[1]) in school_lns
                )

            our_bat_rows  = box.get("our_batting", [])
            alt_bat_rows  = box.get("alt_batting", [])
            our_pit_rows  = box.get("our_pitching", [])
            alt_pit_rows  = box.get("alt_pitching", [])

            if count_school_matches(alt_bat_rows) > count_school_matches(our_bat_rows):
                our_bat_rows = alt_bat_rows
            if count_school_matches(alt_pit_rows) > count_school_matches(our_pit_rows):
                our_pit_rows = alt_pit_rows

            bat_idx   = index_box(our_bat_rows)
            pitch_idx = index_box(our_pit_rows)

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
                        "w": pt.get("w", 0), "l": pt.get("l", 0), "sv": pt.get("sv", 0),
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

        # Bio API data is preferred, but box score data wins when it shows a more recent
        # game — this handles the case where the bio API hasn't been updated for the
        # most recently played game yet (common when the daily scrape runs before the
        # API syncs overnight games).
        last_game_data = bio_last_game.get(player_id)
        plg = player_last_game.get(ln_key)
        if plg and (last_game_data is None or plg["date"] > last_game_data["date"]):
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

                # Reuse cached narrative if last game date hasn't changed
                cached = narrative_cache.get(player_id)
                if cached and cached["date"] == game_date:
                    player_record["narrative"] = cached["narrative"]
                    print(f"  Narrative cached for {sp.get('name', fn + ' ' + ln)} (no new game)")
                else:
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
            "watch_url": most_recent_watch_url or None,
        },
        "next_game": next_game,
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
