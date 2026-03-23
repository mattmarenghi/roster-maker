#!/usr/bin/env python3
"""
D1Baseball Top 25 Rankings Scraper
====================================
Scrapes the D1Baseball Top 25 poll and saves a dated snapshot.

Output: rankings/YYYY-MM-DD.json
Format:
  {
    "date": "2026-03-23",
    "rankings": [
      {"rank": 1, "team": "Texas", "slug": "texas", "record": "18-1"},
      ...
    ]
  }

Usage:
    python3 scrape_rankings.py                  # today's date
    python3 scrape_rankings.py --date 2026-03-17
    python3 scrape_rankings.py --print          # dump to stdout only
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

RANKINGS_DIR = os.path.join(os.path.dirname(__file__), "rankings")
D1BASEBALL_RANKINGS_URL = "https://d1baseball.com/rankings/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://d1baseball.com/",
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


def _slug_from_url(href: str) -> str:
    """Extract team slug from a d1baseball.com team URL."""
    # e.g. /team/texas/2026/ → "texas"
    parts = [p for p in href.strip("/").split("/") if p]
    if len(parts) >= 2 and parts[0] == "team":
        return parts[1]
    return ""


def scrape_d1baseball_rankings(html: str) -> list[dict]:
    """
    Parse the D1Baseball rankings page.

    The page renders a table with columns: Rank, Team, Record, Previous.
    There may be multiple polls (D1Baseball, NCBWA, USA Today) — we take
    the first / primary Top 25 table.
    """
    soup = BeautifulSoup(html, "html.parser")
    rankings = []

    # Try structured table first (most common page layout)
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue
        # Check if this looks like a rankings table by inspecting the header
        header_text = " ".join(th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"]))
        if "rank" not in header_text and "team" not in header_text:
            continue

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            # Rank is the first numeric cell
            rank_text = cells[0].get_text(strip=True).lstrip("#").strip()
            if not rank_text.isdigit():
                continue
            rank = int(rank_text)
            if rank > 30:  # sanity cap
                continue

            # Team name — prefer the anchor text, fall back to cell text
            team_cell = cells[1]
            anchor = team_cell.find("a")
            team_name = (anchor or team_cell).get_text(strip=True)
            slug = _slug_from_url(anchor["href"]) if anchor and anchor.get("href") else ""

            # Record (3rd column if present)
            record = cells[2].get_text(strip=True) if len(cells) > 2 else ""

            rankings.append({"rank": rank, "team": team_name, "slug": slug, "record": record})

        if rankings:
            return sorted(rankings, key=lambda r: r["rank"])

    # Fallback: look for list-based rankings (div/li structure)
    # D1Baseball sometimes uses card layouts for mobile
    ranking_items = soup.select("[class*='ranking'], [class*='poll-row'], [class*='rankings-row']")
    for item in ranking_items:
        rank_el = item.find(class_=re.compile(r"rank|position|number", re.I))
        team_el = item.find(class_=re.compile(r"team|school|name", re.I))
        if not rank_el or not team_el:
            continue
        rank_text = rank_el.get_text(strip=True).lstrip("#").strip()
        if not rank_text.isdigit():
            continue
        rank = int(rank_text)
        anchor = team_el.find("a")
        team_name = (anchor or team_el).get_text(strip=True)
        slug = _slug_from_url(anchor["href"]) if anchor and anchor.get("href") else ""
        rankings.append({"rank": rank, "team": team_name, "slug": slug, "record": ""})

    return sorted(rankings, key=lambda r: r["rank"])


def load_latest_snapshot() -> dict | None:
    """Load the most recent rankings snapshot from disk."""
    if not os.path.isdir(RANKINGS_DIR):
        return None
    files = sorted(
        f for f in os.listdir(RANKINGS_DIR) if f.endswith(".json")
    )
    if not files:
        return None
    with open(os.path.join(RANKINGS_DIR, files[-1])) as fh:
        return json.load(fh)


def enrich_with_movement(current: list[dict], previous_snapshot: dict | None) -> list[dict]:
    """Add rank_change field by diffing current vs previous snapshot."""
    if not previous_snapshot:
        return current
    prev_by_team = {r["team"]: r["rank"] for r in previous_snapshot.get("rankings", [])}
    for entry in current:
        prev_rank = prev_by_team.get(entry["team"])
        if prev_rank is None:
            entry["rank_change"] = "new"   # entered rankings
        else:
            delta = prev_rank - entry["rank"]  # positive = moved up
            entry["rank_change"] = delta       # 0 = unchanged
    return current


def run(snapshot_date: str | None = None, print_only: bool = False) -> dict:
    if not snapshot_date:
        snapshot_date = date.today().isoformat()

    print(f"Fetching D1Baseball Top 25 for {snapshot_date}...")
    html = fetch(D1BASEBALL_RANKINGS_URL)
    if not html:
        print("ERROR: Could not fetch D1Baseball rankings page.", file=sys.stderr)
        sys.exit(1)

    rankings = scrape_d1baseball_rankings(html)
    if not rankings:
        print("WARNING: No rankings found in page — layout may have changed.", file=sys.stderr)

    previous = load_latest_snapshot()
    rankings = enrich_with_movement(rankings, previous)

    snapshot = {
        "date": snapshot_date,
        "source": D1BASEBALL_RANKINGS_URL,
        "rankings": rankings,
    }

    if print_only:
        print(json.dumps(snapshot, indent=2))
        return snapshot

    os.makedirs(RANKINGS_DIR, exist_ok=True)
    out_path = os.path.join(RANKINGS_DIR, f"{snapshot_date}.json")
    with open(out_path, "w") as fh:
        json.dump(snapshot, fh, indent=2)
    print(f"Saved {len(rankings)} teams → {out_path}")

    # Prune old snapshots, keep last 4 weeks
    all_files = sorted(f for f in os.listdir(RANKINGS_DIR) if f.endswith(".json"))
    for old in all_files[:-28]:
        os.remove(os.path.join(RANKINGS_DIR, old))

    return snapshot


# ---------------------------------------------------------------------------
# Public helper used by scrape_school_stats.py
# ---------------------------------------------------------------------------

def load_rankings_context(school_name: str) -> dict:
    """
    Return a rankings context dict for `school_name`.

    Loads the two most recent snapshots. Returns:
      {
        "ranked": True/False,
        "rank": 5,               # current rank (None if unranked)
        "rank_change": 2,        # positive = moved up; "new" = just entered; None = was/still unranked
        "display": "#5",         # ready-to-use string, or None
      }
    """
    if not os.path.isdir(RANKINGS_DIR):
        return {"ranked": False, "rank": None, "rank_change": None, "display": None}

    files = sorted(f for f in os.listdir(RANKINGS_DIR) if f.endswith(".json"))
    if not files:
        return {"ranked": False, "rank": None, "rank_change": None, "display": None}

    def _match_team(rankings: list[dict], name: str) -> dict | None:
        """Fuzzy match school name to a rankings entry."""
        name_lower = name.lower()
        # Try progressively looser matches
        for entry in rankings:
            t = entry["team"].lower()
            if t == name_lower:
                return entry
        # Partial: ranked team name is a substring of school name or vice versa
        words = set(name_lower.split())
        for entry in rankings:
            t_words = set(entry["team"].lower().split())
            overlap = words & t_words - {"university", "college", "state", "the", "of", "at"}
            if len(overlap) >= 2:
                return entry
        return None

    current_snapshot = None
    with open(os.path.join(RANKINGS_DIR, files[-1])) as fh:
        current_snapshot = json.load(fh)

    entry = _match_team(current_snapshot.get("rankings", []), school_name)
    if not entry:
        return {"ranked": False, "rank": None, "rank_change": None, "display": None}

    rank = entry["rank"]
    rank_change = entry.get("rank_change")  # set by enrich_with_movement
    return {
        "ranked": True,
        "rank": rank,
        "rank_change": rank_change,
        "display": f"#{rank}",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape D1Baseball Top 25 rankings")
    parser.add_argument("--date", help="Snapshot date (YYYY-MM-DD), default=today")
    parser.add_argument("--print", dest="print_only", action="store_true",
                        help="Print to stdout instead of saving")
    args = parser.parse_args()
    run(snapshot_date=args.date, print_only=args.print_only)
