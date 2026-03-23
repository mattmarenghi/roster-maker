#!/usr/bin/env python3
"""
Conference Standings Scraper
==============================
Scrapes baseball standings for all major D1 conferences and saves a dated snapshot.

Output: standings/YYYY-MM-DD.json
Format:
  {
    "date": "2026-03-23",
    "conferences": {
      "ACC": [
        {
          "team": "Duke Blue Devils",
          "conf_w": 5, "conf_l": 1, "conf_rank": 1,
          "overall_w": 16, "overall_l": 8,
          "games_back": 0.0, "streak": "W3"
        },
        ...
      ],
      "SEC": [...],
      ...
    }
  }

Usage:
    python3 scrape_standings.py                   # today's date
    python3 scrape_standings.py --date 2026-03-17
    python3 scrape_standings.py --conf ACC         # single conference
    python3 scrape_standings.py --print            # dump to stdout
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import date

import requests
from bs4 import BeautifulSoup

STANDINGS_DIR = os.path.join(os.path.dirname(__file__), "standings")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Conference → standings URL mapping
# Almost all use SIDEARM's .aspx?path=baseball pattern.
# A few use different CMSes — those are noted.
CONFERENCE_URLS = {
    "ACC":                  "https://theacc.com/standings.aspx?path=baseball",
    "SEC":                  "https://secsports.com/standings/baseball",
    "Big 12":               "https://big12sports.com/standings.aspx?path=baseball",
    "Big Ten":              "https://bigten.org/standings.aspx?path=baseball",
    "Pac-12":               "https://pac-12.com/standings.aspx?path=baseball",
    "American Athletic":    "https://theamerican.org/standings.aspx?path=baseball",
    "Atlantic 10":          "https://atlantic10.com/standings.aspx?path=baseball",
    "Big West":             "https://bigwestsports.com/standings.aspx?path=baseball",
    "Conference USA":       "https://conferenceusa.com/standings.aspx?path=baseball",
    "Mountain West":        "https://themw.com/standings.aspx?path=baseball",
    "Sun Belt":             "https://sunbeltsports.org/standings.aspx?path=baseball",
    "Missouri Valley":      "https://mvcsports.com/standings.aspx?path=baseball",
    "Southern":             "https://socon.org/standings.aspx?path=baseball",
    "Southland":            "https://southland.org/standings.aspx?path=baseball",
    "Southeastern":         "https://secsports.com/standings/baseball",
    "Western Athletic":     "https://wacsports.com/standings.aspx?path=baseball",
    "Big South":            "https://bigsouthsports.com/standings.aspx?path=baseball",
    "Horizon League":       "https://horizonleague.org/standings.aspx?path=baseball",
    "MAAC":                 "https://maacsports.com/standings.aspx?path=baseball",
    "MAC":                  "https://mac-sports.com/standings.aspx?path=baseball",
    "Ohio Valley":          "https://ovcsports.com/standings.aspx?path=baseball",
    "Patriot League":       "https://patriotleague.org/standings.aspx?path=baseball",
    "SoCon":                "https://socon.org/standings.aspx?path=baseball",
    "Summit League":        "https://thesummitleague.org/standings.aspx?path=baseball",
    "America East":         "https://americaeast.com/standings.aspx?path=baseball",
    "ASUN":                 "https://asunsports.org/standings.aspx?path=baseball",
    "CAA":                  "https://caasports.com/standings.aspx?path=baseball",
    "NEC":                  "https://northeastconference.org/standings.aspx?path=baseball",
    "SWAC":                 "https://swac.org/standings.aspx?path=baseball",
    "MEAC":                 "https://meacsports.com/standings.aspx?path=baseball",
    "Ivy League":           "https://ivyleaguesports.com/standings.aspx?path=baseball",
    "Big East":             "https://bigeast.com/standings.aspx?path=baseball",
}

session = requests.Session()
session.headers.update(HEADERS)


def fetch(url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                return resp.text
            print(f"  HTTP {resp.status_code}: {url}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"  Attempt {attempt+1} failed ({e}): {url}", file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def _safe_int(val) -> int | None:
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    try:
        s = str(val).strip()
        if s in ("-", ""):
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_overall(text: str) -> tuple[int | None, int | None]:
    """Parse 'W-L' or 'W/L' format into (wins, losses)."""
    m = re.match(r"(\d+)[-/](\d+)", text.strip())
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def _parse_streak(text: str) -> str:
    """Normalise streak strings: 'W3', 'L1', etc."""
    t = text.strip()
    m = re.match(r"([WL])[\s\-]?(\d+)", t, re.I)
    if m:
        return f"{m.group(1).upper()}{m.group(2)}"
    return t


def scrape_sidearm_standings(html: str) -> list[dict]:
    """
    Parse a SIDEARM Sports conference standings page.

    SIDEARM renders a <table class="standings_table"> with headers like:
      School | Conf W | Conf L | Pct | Overall | ...
    Column order varies, so we detect columns by header text.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Try SIDEARM's standard standings table class first
    table = (
        soup.find("table", class_=re.compile(r"standings", re.I))
        or soup.find("table")
    )
    if not table:
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    # Detect column indices from header row
    header_cells = rows[0].find_all(["th", "td"])
    headers = [h.get_text(strip=True).lower() for h in header_cells]

    def col(names: list[str]) -> int | None:
        for name in names:
            for i, h in enumerate(headers):
                if name in h:
                    return i
        return None

    col_team    = col(["school", "team", "member"])
    col_cw      = col(["conf w", "cw", "conference w"])
    col_cl      = col(["conf l", "cl", "conference l"])
    col_overall = col(["overall", "total"])
    col_gb      = col(["gb", "games back"])
    col_streak  = col(["streak", "strk", "last"])

    # Fallback: if no team column found, assume column 0
    if col_team is None:
        col_team = 0

    entries = []
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        # Team name — prefer anchor text
        team_cell = cells[col_team] if col_team < len(cells) else cells[0]
        anchor = team_cell.find("a")
        team_name = (anchor or team_cell).get_text(strip=True)
        if not team_name or team_name.lower() in ("team", "school", "member"):
            continue

        conf_w = _safe_int(cells[col_cw].get_text(strip=True)) if col_cw and col_cw < len(cells) else None
        conf_l = _safe_int(cells[col_cl].get_text(strip=True)) if col_cl and col_cl < len(cells) else None

        overall_w, overall_l = None, None
        if col_overall and col_overall < len(cells):
            overall_w, overall_l = _parse_overall(cells[col_overall].get_text(strip=True))

        games_back = None
        if col_gb and col_gb < len(cells):
            games_back = _safe_float(cells[col_gb].get_text(strip=True))

        streak = ""
        if col_streak and col_streak < len(cells):
            streak = _parse_streak(cells[col_streak].get_text(strip=True))

        entries.append({
            "team": team_name,
            "conf_w": conf_w,
            "conf_l": conf_l,
            "overall_w": overall_w,
            "overall_l": overall_l,
            "games_back": games_back,
            "streak": streak,
        })

    # Assign conf_rank by position in table (standings pages are pre-sorted)
    for i, entry in enumerate(entries):
        entry["conf_rank"] = i + 1

    # If games_back wasn't in the table, compute it from conf W-L
    if all(e.get("games_back") is None for e in entries) and entries:
        leader = entries[0]
        lw, ll = leader.get("conf_w") or 0, leader.get("conf_l") or 0
        for entry in entries:
            ew, el = entry.get("conf_w") or 0, entry.get("conf_l") or 0
            entry["games_back"] = round(((lw - ew) + (el - ll)) / 2, 1) if entry != leader else 0.0

    return entries


def scrape_conference_standings(conf_name: str, url: str) -> list[dict]:
    """Fetch and parse standings for one conference."""
    print(f"  [{conf_name}] {url}")
    html = fetch(url)
    if not html:
        print(f"  [{conf_name}] fetch failed", file=sys.stderr)
        return []
    entries = scrape_sidearm_standings(html)
    if not entries:
        print(f"  [{conf_name}] no entries parsed (page layout may differ)", file=sys.stderr)
    return entries


def run(
    snapshot_date: str | None = None,
    conf_filter: str | None = None,
    print_only: bool = False,
) -> dict:
    if not snapshot_date:
        snapshot_date = date.today().isoformat()

    target_confs = CONFERENCE_URLS
    if conf_filter:
        target_confs = {k: v for k, v in CONFERENCE_URLS.items() if conf_filter.lower() in k.lower()}
        if not target_confs:
            print(f"No conference matched '{conf_filter}'", file=sys.stderr)
            sys.exit(1)

    print(f"Scraping standings for {len(target_confs)} conference(s) — {snapshot_date}")
    conferences = {}
    for conf_name, url in target_confs.items():
        entries = scrape_conference_standings(conf_name, url)
        if entries:
            conferences[conf_name] = entries
        time.sleep(0.5)

    snapshot = {
        "date": snapshot_date,
        "conferences": conferences,
    }

    if print_only:
        print(json.dumps(snapshot, indent=2))
        return snapshot

    os.makedirs(STANDINGS_DIR, exist_ok=True)
    out_path = os.path.join(STANDINGS_DIR, f"{snapshot_date}.json")

    # If a file already exists for today, merge (don't overwrite conferences that succeeded
    # on a previous run if today's run filtered to a subset)
    if os.path.exists(out_path) and not conf_filter:
        pass  # full run: just overwrite
    elif os.path.exists(out_path) and conf_filter:
        with open(out_path) as fh:
            existing = json.load(fh)
        existing["conferences"].update(conferences)
        snapshot = existing
        snapshot["date"] = snapshot_date

    with open(out_path, "w") as fh:
        json.dump(snapshot, fh, indent=2)

    total_teams = sum(len(v) for v in conferences.values())
    print(f"Saved {len(conferences)} conferences, {total_teams} teams → {out_path}")

    # Keep last 60 days of snapshots
    all_files = sorted(f for f in os.listdir(STANDINGS_DIR) if f.endswith(".json"))
    for old in all_files[:-60]:
        os.remove(os.path.join(STANDINGS_DIR, old))

    return snapshot


# ---------------------------------------------------------------------------
# Public helper used by scrape_school_stats.py
# ---------------------------------------------------------------------------

def load_standings_context(school_name: str, conf_name: str) -> dict:
    """
    Return a standings context dict for `school_name` in `conf_name`.

    Loads the two most recent snapshots and diffs them.
    Returns:
      {
        "available": True/False,
        "conf_rank": 3,
        "conf_w": 5, "conf_l": 2,
        "games_back": 1.5,
        "total_teams": 14,
        "streak": "W2",
        "conf_rank_change": 2,         # positive = moved up; None if no previous data
        "tight_race": False,           # True if games_back <= 1.5
        "in_first": False,
        "in_last": False,
        "narrative": "moved up to 3rd in the ACC",  # or None
      }
    """
    empty = {
        "available": False,
        "conf_rank": None, "conf_w": None, "conf_l": None,
        "games_back": None, "total_teams": None, "streak": "",
        "conf_rank_change": None, "tight_race": False,
        "in_first": False, "in_last": False, "narrative": None,
    }

    if not os.path.isdir(STANDINGS_DIR):
        return empty

    files = sorted(f for f in os.listdir(STANDINGS_DIR) if f.endswith(".json"))
    if not files:
        return empty

    def _match_team(entries: list[dict], name: str) -> dict | None:
        name_lower = name.lower()
        # Exact match
        for e in entries:
            if e["team"].lower() == name_lower:
                return e
        # Word-overlap match (ignore generic words)
        stop = {"university", "college", "state", "the", "of", "at", "a&m",
                "polytechnic", "technological", "tech"}
        words = {w for w in name_lower.split() if w not in stop and len(w) > 2}
        best, best_score = None, 0
        for e in entries:
            t_words = {w for w in e["team"].lower().split() if w not in stop and len(w) > 2}
            score = len(words & t_words)
            if score > best_score:
                best_score, best = score, e
        return best if best_score >= 1 else None

    def _load_conf(path: str, conf: str) -> list[dict]:
        try:
            with open(path) as fh:
                data = json.load(fh)
            # Try exact conference name first, then partial match
            if conf in data.get("conferences", {}):
                return data["conferences"][conf]
            for key, entries in data.get("conferences", {}).items():
                if conf.lower() in key.lower() or key.lower() in conf.lower():
                    return entries
        except Exception:
            pass
        return []

    # Current snapshot
    current_entries = _load_conf(os.path.join(STANDINGS_DIR, files[-1]), conf_name)
    current = _match_team(current_entries, school_name)
    if not current:
        return empty

    total_teams = len(current_entries)

    # Previous snapshot (second-to-last file)
    prev_rank = None
    if len(files) >= 2:
        prev_entries = _load_conf(os.path.join(STANDINGS_DIR, files[-2]), conf_name)
        prev = _match_team(prev_entries, school_name)
        if prev:
            prev_rank = prev.get("conf_rank")

    conf_rank = current.get("conf_rank")
    rank_change = (prev_rank - conf_rank) if prev_rank and conf_rank else None  # positive = improvement
    games_back = current.get("games_back")
    tight_race = (games_back is not None and 0 < games_back <= 1.5)
    in_first = (conf_rank == 1)
    in_last = (conf_rank == total_teams) if total_teams else False

    # Build narrative fragment
    narrative = None
    if rank_change and abs(rank_change) >= 1 and conf_rank:
        ordinal = _ordinal(conf_rank)
        short_conf = _shorten_conf(conf_name)
        if rank_change > 0:
            narrative = f"moved up to {ordinal} in the {short_conf}"
        else:
            narrative = f"slid to {ordinal} in the {short_conf}"
    elif conf_rank and conf_rank <= 3:
        ordinal = _ordinal(conf_rank)
        short_conf = _shorten_conf(conf_name)
        narrative = f"sitting {ordinal} in the {short_conf}"

    return {
        "available": True,
        "conf_rank": conf_rank,
        "conf_w": current.get("conf_w"),
        "conf_l": current.get("conf_l"),
        "games_back": games_back,
        "total_teams": total_teams,
        "streak": current.get("streak", ""),
        "conf_rank_change": rank_change,
        "tight_race": tight_race,
        "in_first": in_first,
        "in_last": in_last,
        "narrative": narrative,
    }


def _ordinal(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _shorten_conf(name: str) -> str:
    """Return a short display name for a conference."""
    shorts = {
        "Atlantic Coast Conference": "ACC",
        "Atlantic 10": "A-10",
        "American Athletic": "AAC",
        "Southeastern Conference": "SEC",
        "Big 12 Conference": "Big 12",
        "Big Ten Conference": "Big Ten",
        "Pac-12 Conference": "Pac-12",
        "Mountain West Conference": "Mountain West",
        "Sun Belt Conference": "Sun Belt",
        "Conference USA": "C-USA",
        "Missouri Valley Conference": "Missouri Valley",
        "Western Athletic Conference": "WAC",
        "Big South Conference": "Big South",
        "Horizon League": "Horizon",
        "MAAC": "MAAC",
        "Mid-American Conference": "MAC",
        "Ohio Valley Conference": "OVC",
        "Patriot League": "Patriot",
        "Summit League": "Summit",
        "Southern Conference": "SoCon",
        "America East Conference": "America East",
        "ASUN Conference": "ASUN",
        "Colonial Athletic Association": "CAA",
        "Northeast Conference": "NEC",
        "Southwestern Athletic Conference": "SWAC",
        "Mid-Eastern Athletic Conference": "MEAC",
    }
    for full, short in shorts.items():
        if full.lower() in name.lower() or name.lower() in full.lower():
            return short
    return name  # return as-is if no match


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape D1 conference standings")
    parser.add_argument("--date", help="Snapshot date (YYYY-MM-DD), default=today")
    parser.add_argument("--conf", dest="conf_filter", help="Filter to one conference (partial name match)")
    parser.add_argument("--print", dest="print_only", action="store_true",
                        help="Print to stdout instead of saving")
    args = parser.parse_args()
    run(snapshot_date=args.date, conf_filter=args.conf_filter, print_only=args.print_only)
