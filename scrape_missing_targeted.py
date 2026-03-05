#!/usr/bin/env python3
"""
Targeted scraper for schools in missing_schools.csv.

Tries all confirmed_missing schools first, then retries needs_investigation
schools using cloudscraper (for Cloudflare bypass) and JSON-script parsing.
Appends successful results to d1_baseball_rosters_2026.csv and regenerates
missing_schools.csv.
"""

import csv
import json
import os
import re
import time

import cloudscraper
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Reuse helpers from the main scraper
from scrape_d1_baseball import (
    HEADERS,
    parse_name,
    parse_sidearm_roster,
    parse_script_json_roster,
    parse_wmt_digital_roster,
    parse_sidearm_nextgen_api,
    normalize_url,
)

CSV_PATH = '/home/user/roster-maker/d1_baseball_rosters_2026.csv'
MISSING_CSV = '/home/user/roster-maker/missing_schools.csv'
DELAY = 1.0  # seconds between requests

COL_ORDER = [
    'first_name', 'last_name', 'number', 'position', 'bats_throws',
    'class_year', 'height', 'weight', 'team', 'school', 'conference',
    'state', 'hometown', 'high_school',
]


def fetch_roster_page(url, use_cloudscraper=False):
    """Fetch a URL, optionally via cloudscraper. Returns (response, error_str)."""
    if use_cloudscraper:
        try:
            scraper = cloudscraper.create_scraper()
            r = scraper.get(url, timeout=30)
            return r, None
        except Exception as e:
            return None, str(e)

    try:
        r = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
        return r, None
    except requests.exceptions.TooManyRedirects:
        # Fall back to cloudscraper automatically
        return fetch_roster_page(url, use_cloudscraper=True)
    except requests.exceptions.SSLError:
        try:
            r = requests.get(url.replace('https://', 'http://'), headers=HEADERS,
                             timeout=20, allow_redirects=True)
            return r, None
        except Exception as e:
            return None, f"SSL+HTTP error: {e}"
    except Exception as e:
        return None, str(e)


def try_scrape(school_row, nickname=''):
    """
    Try to scrape a school's baseball roster.
    Returns (players_list, status_msg).
    """
    base_url = school_row['base_url'].rstrip('/')
    roster_url = school_row.get('roster_url', '').strip() or f"{base_url}/sports/baseball/roster"

    school_name = school_row['school']
    conference = school_row['conference']
    state = school_row['state']
    team_name = f"{school_name} {nickname}".strip() if nickname else school_name

    # Try the roster URL; for redirect-loop schools, start with cloudscraper
    use_cs = 'redirect' in school_row.get('notes', '').lower() or \
             'redirect' in school_row.get('status', '').lower()

    r, err = fetch_roster_page(roster_url, use_cloudscraper=use_cs)

    if err:
        return [], f"Fetch error: {err}"
    if r.status_code == 404:
        return [], "HTTP 404"
    if r.status_code == 403:
        return [], "HTTP 403"
    if r.status_code != 200:
        return [], f"HTTP {r.status_code}"

    # Validate we're on a baseball page
    text_lower = r.text[:8000].lower()
    final_url_lower = r.url.lower()
    if 'baseball' not in final_url_lower and 'baseball' not in text_lower:
        return [], "Redirected away from baseball page"

    soup = BeautifulSoup(r.text, 'html.parser')

    # Try HTML table, JSON-in-script, WMT Digital, then Sidearm next-gen API
    players = parse_sidearm_roster(soup, team_name, conference, school_name, state)
    if not players:
        players = parse_script_json_roster(soup, team_name, conference, school_name, state)
    if not players:
        players = parse_wmt_digital_roster(soup, team_name, conference, school_name, state)
    if not players:
        players = parse_sidearm_nextgen_api(base_url, team_name, conference, school_name, state)

    if players:
        return players, f"OK ({len(players)} players)"

    return [], "No players parsed"


def load_existing_schools():
    """Return set of school names already in the CSV."""
    schools = set()
    if not os.path.exists(CSV_PATH):
        return schools
    with open(CSV_PATH) as f:
        reader = csv.DictReader(f)
        for row in reader:
            schools.add(row['school'].strip())
    return schools


def append_to_csv(new_players):
    """Append player records to the main CSV."""
    if not new_players:
        return
    df_new = pd.DataFrame(new_players)
    for c in COL_ORDER:
        if c not in df_new.columns:
            df_new[c] = ''
    df_new = df_new[COL_ORDER]

    if os.path.exists(CSV_PATH):
        df_existing = pd.read_csv(CSV_PATH)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    df_combined.to_csv(CSV_PATH, index=False)
    print(f"  CSV updated: {len(df_combined)} total players")


def regen_missing_schools():
    """Re-run the missing schools check and overwrite missing_schools.csv."""
    print("\nRe-running missing schools check...")
    os.system(f"cd /home/user/roster-maker && python3 find_missing_schools.py")


def main():
    print("=" * 70)
    print("Targeted scraper — missing schools")
    print("=" * 70)

    # Load missing schools
    rows = []
    with open(MISSING_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    confirmed = [r for r in rows if r['status'] == 'confirmed_missing']
    investigate = [r for r in rows if r['status'] == 'needs_investigation']

    print(f"\nConfirmed missing (have baseball): {len(confirmed)}")
    print(f"Needs investigation:               {len(investigate)}")

    existing_schools = load_existing_schools()
    all_new_players = []
    results = {}  # school -> (success, msg)

    # --- Pass 1: confirmed missing schools ---
    print("\n--- Pass 1: confirmed_missing schools ---\n")
    for s in confirmed:
        name = s['school']
        if name in existing_schools:
            print(f"  SKIP (already in DB): {name}")
            results[name] = (True, 'already in DB')
            continue
        print(f"  {name}...", end=' ', flush=True)
        players, msg = try_scrape(s)
        print(msg)
        results[name] = (bool(players), msg)
        if players:
            all_new_players.extend(players)
            existing_schools.add(name)
        time.sleep(DELAY)

    # --- Pass 2: needs_investigation schools ---
    print("\n--- Pass 2: needs_investigation schools (with cloudscraper) ---\n")
    for s in investigate:
        name = s['school']
        if name in existing_schools:
            print(f"  SKIP (already in DB): {name}")
            results[name] = (True, 'already in DB')
            continue
        print(f"  {name}...", end=' ', flush=True)
        players, msg = try_scrape(s)
        print(msg)
        results[name] = (bool(players), msg)
        if players:
            all_new_players.extend(players)
            existing_schools.add(name)
        time.sleep(DELAY)

    # --- Save results ---
    print(f"\n{'='*70}")
    newly_added = [n for n, (ok, _) in results.items() if ok and _ != 'already in DB']
    print(f"Newly scraped: {len(newly_added)} schools, {len(all_new_players)} players")

    if newly_added:
        print("\nSchools added:")
        for n in newly_added:
            print(f"  + {n}  ({results[n][1]})")
        append_to_csv(all_new_players)

    still_missing = [n for n, (ok, msg) in results.items() if not ok and msg != 'already in DB']
    if still_missing:
        print(f"\nStill could not scrape ({len(still_missing)} schools):")
        for n in still_missing:
            print(f"  - {n}  ({results[n][1]})")

    # Regenerate missing_schools.csv
    regen_missing_schools()


if __name__ == '__main__':
    main()
