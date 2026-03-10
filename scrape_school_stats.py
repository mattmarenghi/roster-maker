#!/usr/bin/env python3
"""
School Baseball Stats Scraper
==============================
Scrapes season stats, most recent game box scores, and game recap narratives
for a given SIDEARM-hosted school's baseball program.

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
# Season stats scrapers
# ---------------------------------------------------------------------------

def scrape_season_batting(base_url: str, season: str = "2026") -> list[dict]:
    """Scrape cumulative season batting stats from the HTML stats table."""
    url = f"{base_url}/sports/baseball/stats/{season}"
    print(f"  Fetching season batting: {url}")
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


def scrape_season_pitching(base_url: str, season: str = "2026") -> list[dict]:
    """Extract season pitching stats from the NUXT embedded data."""
    url = f"{base_url}/sports/baseball/stats/{season}"
    print(f"  Fetching season pitching from NUXT data: {url}")
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
    return pitchers


# ---------------------------------------------------------------------------
# Box score / game log scrapers
# ---------------------------------------------------------------------------

def find_game_urls(base_url: str, season: str = "2026") -> list[dict]:
    """
    Extract box score URLs and dates from NUXT data on the stats page.
    Box score paths are stored as raw strings inside the NUXT JSON, along
    with the date and opponent in nearby array positions.
    Returns list of {date, opponent, url} sorted newest-first.
    """
    url = f"{base_url}/sports/baseball/stats/{season}"
    html = fetch(url)
    if not html:
        return []
    time.sleep(DELAY)

    nuxt_data = extract_nuxt_data(html)
    if not nuxt_data:
        return []

    # Boxscore paths are raw strings like "/sports/baseball/stats/2026/lehigh/boxscore/10233"
    # In the NUXT array the pattern is typically: date_str, opponent_str, boxscore_path_str
    # Find all boxscore paths and then look at adjacent items for date/opponent
    boxscore_path_re = re.compile(r"^/sports/baseball/stats/\d+/[^/]+/boxscore/\d+$")
    date_re = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")

    seen = set()
    games = []

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
        elif "R" in headers and "H" in headers and "E" in headers and "TEAM" in headers:
            # Line score
            rows = table.find_all("tr")
            if len(rows) >= 3:
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
                parts.append(f"Contributed at the plate in Davidson's {result}.")
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

def run(base_url: str, school_slug: str, season: str = "2026"):
    os.makedirs(STATS_DIR, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Scraping stats for: {base_url}")
    print(f"Season: {season}")
    print(f"{'='*60}\n")

    # Load players.json for this school
    print("Loading players.json...")
    with open(PLAYERS_JSON) as f:
        all_players = json.load(f)

    # Filter to this school by matching base_url domain fragment or school slug
    # Match by school name words (e.g. "davidson-college" slug matches "Davidson College")
    slug_words = set(school_slug.lower().replace("-", " ").split())
    school_players = [
        p for p in all_players
        if slug_words.issubset(set(p.get("school", "").lower().split()))
        or any(w in (p.get("profile_url") or "").lower() for w in slug_words if len(w) > 4)
    ]
    print(f"  Found {len(school_players)} players in players.json for '{school_slug}'")

    # --- Season batting stats ---
    print("\n[1/5] Season batting stats")
    batting_stats = scrape_season_batting(base_url, season)

    # --- Season pitching stats ---
    print("\n[2/5] Season pitching stats")
    pitching_stats = scrape_season_pitching(base_url, season)

    # --- Find most recent game ---
    print("\n[3/5] Finding most recent game")
    games = find_game_urls(base_url, season)
    if not games:
        print("  No games found!", file=sys.stderr)
        return

    most_recent = games[0]
    print(f"  Most recent: {most_recent['date']} vs {most_recent['opponent']}")

    # --- Box score ---
    print("\n[4/5] Box score")
    box = scrape_boxscore(most_recent["boxscore_url"])

    # Parse result from line score
    result_str = ""
    if box and box.get("line_score"):
        ls = box["line_score"]
        # ls[0] = away row, ls[1] = home row (typically)
        # But we need to figure out which is ours
        if len(ls) >= 2:
            try:
                score_our = int(ls[1][-3])  # R column (3rd from end)
                score_opp = int(ls[0][-3])
                result_str = f"{'W' if score_our > score_opp else 'L'} {score_our}-{score_opp}"
            except (ValueError, IndexError):
                pass

    # --- Recap article ---
    print("\n[5/5] Game recap")
    recap_url = find_recap_url(base_url, most_recent["date"], most_recent["opponent"])
    recap = {}
    if recap_url:
        recap = scrape_recap(recap_url)
        print(f"  Recap found: {recap_url}")
        print(f"  Article length: {len(recap.get('article_text',''))} chars")
    else:
        print(f"  No recap found for {most_recent['date']} vs {most_recent['opponent']}")

    # --- Assemble player data ---
    print("\n[Assembling player records...]")

    # Index batting/pitching season stats by last name
    batting_by_ln = {normalize_name(b["ln"]): b for b in batting_stats}
    pitching_by_ln = {normalize_name(p["ln"]): p for p in pitching_stats}

    # Index box score batting by normalized last name
    def index_boxscore_players(rows):
        idx = {}
        for row in rows:
            fn, ln = parse_boxscore_name(row["name"])
            key = normalize_name(ln)
            idx[key] = {"fn": fn, "ln": ln, **{k: v for k, v in row.items() if k != "name"}}
        return idx

    our_batting_box = index_boxscore_players(box.get("our_batting", []) if box else [])
    our_pitching_box = index_boxscore_players(box.get("our_pitching", []) if box else [])

    players_out = {}
    api_key_available = bool(os.environ.get("ANTHROPIC_API_KEY"))

    for sp in school_players:
        player_id = sp["id"]
        fn = sp.get("fn", "")
        ln = sp.get("ln", "")
        ln_key = normalize_name(ln)

        # Season batting
        season_bat = None
        if ln_key in batting_by_ln:
            season_bat = batting_by_ln[ln_key]["season_batting"]

        # Season pitching
        season_pitch = None
        if ln_key in pitching_by_ln:
            season_pitch = pitching_by_ln[ln_key]["season_pitching"]

        # Last game batting
        last_bat = None
        if ln_key in our_batting_box:
            b = our_batting_box[ln_key]
            last_bat = {
                "ab": b.get("ab", 0), "r": b.get("r", 0), "h": b.get("h", 0),
                "rbi": b.get("rbi", 0), "bb": b.get("bb", 0), "so": b.get("so", 0),
                "pos": b.get("pos", ""),
            }

        # Last game pitching
        last_pitch = None
        if ln_key in our_pitching_box:
            pt = our_pitching_box[ln_key]
            last_pitch = {
                "ip": pt.get("ip", "0.0"), "h": pt.get("h", 0), "r": pt.get("r", 0),
                "er": pt.get("er", 0), "bb": pt.get("bb", 0), "so": pt.get("so", 0),
                "wp": pt.get("wp", 0), "hbp": pt.get("hbp", 0),
            }

        # Skip players with no stats at all
        if not season_bat and not season_pitch and not last_bat and not last_pitch:
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
            "last_game": {
                "date": most_recent["date"],
                "opponent": most_recent["opponent"],
                "result": result_str,
                "boxscore_url": most_recent["boxscore_url"],
                "batting": last_bat,
                "pitching": last_pitch,
            },
            "narrative": None,
        }

        # Generate narrative only for players who meaningfully appeared in the game
        # (skip 0 AB / 0 IP appearances like pinch runners or defensive replacements with no PA)
        had_meaningful_bat = last_bat and last_bat.get("ab", 0) > 0
        had_meaningful_pitch = last_pitch and last_pitch.get("ip", "0") not in ("0.0", "0", 0)

        if had_meaningful_bat or had_meaningful_pitch:
            print(f"  Generating narrative for {sp.get('name', fn + ' ' + ln)}...", end=" ")
            if api_key_available:
                narrative = generate_narrative_claude(
                    {**player_record, "name": sp.get("name", "")},
                    {
                        "date": most_recent["date"],
                        "opponent": most_recent["opponent"],
                        "result": result_str,
                    },
                    recap.get("article_text", ""),
                )
            else:
                narrative = generate_narrative_template(
                    player_record,
                    {
                        "date": most_recent["date"],
                        "opponent": most_recent["opponent"],
                        "result": result_str,
                    },
                )
            player_record["narrative"] = narrative
            print("done" if narrative else "no narrative")
        elif last_bat or last_pitch:
            # Appeared but no meaningful stats (pinch runner, etc.) - no narrative
            pass

        players_out[player_id] = player_record

    # --- Output ---
    output = {
        "school": school_slug.replace("-", " ").title(),
        "base_url": base_url,
        "season": season,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "record": "7-7",  # will be updated from live data ideally
        "most_recent_game": {
            "date": most_recent["date"],
            "opponent": most_recent["opponent"],
            "result": result_str,
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
    print(f"Players with stats: {len(players_out)}")
    print(f"Narratives: {'AI-generated' if api_key_available else 'template-based (set ANTHROPIC_API_KEY for AI)'}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

KNOWN_SCHOOLS = {
    "davidson": {
        "base_url": "https://www.davidsonwildcats.com",
        "slug": "davidson-college",
    },
}


def main():
    parser = argparse.ArgumentParser(description="Scrape baseball stats for a school")
    parser.add_argument("--school", default="davidson", help="School shortname (e.g. davidson)")
    parser.add_argument("--base-url", help="Override base URL")
    parser.add_argument("--slug", help="Override output file slug")
    parser.add_argument("--season", default="2026", help="Season year (default: 2026)")
    args = parser.parse_args()

    if args.school in KNOWN_SCHOOLS:
        cfg = KNOWN_SCHOOLS[args.school]
        base_url = args.base_url or cfg["base_url"]
        slug = args.slug or cfg["slug"]
    else:
        base_url = args.base_url
        slug = args.slug or args.school
        if not base_url:
            print(f"Unknown school '{args.school}'. Provide --base-url.", file=sys.stderr)
            sys.exit(1)

    run(base_url, slug, args.season)


if __name__ == "__main__":
    main()
