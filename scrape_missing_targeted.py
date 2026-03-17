#!/usr/bin/env python3
"""
Targeted scraper for schools in missing_schools.csv.

Tries all confirmed_missing schools first, then retries needs_investigation
schools using cloudscraper (for Cloudflare bypass) and JSON-script parsing.
Appends successful results to d1_baseball_rosters_2026.csv and regenerates
missing_schools.csv.

Strategy order per school:
  1. Fetch roster page (cloudscraper for redirect-loop schools)
  2. parse_sidearm_roster / parse_old_sidearm_table / parse_script_json_roster
  3. parse_wmt_digital_roster (OAS/WMT platforms)
  4. parse_roster_list_items_rich (SIDEARM Nuxt3 card layout)
  5. parse_nuxt_roster (OAS Sports __NUXT_DATA__)
  6. parse_roster_list_items (simple li text parser)
  7. parse_sidearm_nextgen_api (REST API, tries multiple season formats)
  8. scrape_espn_roster (last resort for redirect-blocked schools)
"""

import csv
import datetime
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
    parse_roster_list_items_rich,
    parse_sidearm_roster,
    parse_old_sidearm_table,
    parse_script_json_roster,
    parse_wmt_digital_roster,
    parse_sidearm_nextgen_api,
    normalize_url,
)

# Reuse Nuxt / list-item / ESPN helpers from the retry scraper
from scrape_failed_schools import (
    parse_nuxt_roster,
    parse_roster_list_items,
    scrape_espn_roster,
)

CSV_PATH = '/home/user/roster-maker/d1_baseball_rosters_2026.csv'
MISSING_CSV = '/home/user/roster-maker/missing_schools.csv'
DELAY = 1.0  # seconds between requests

COL_ORDER = [
    'first_name', 'last_name', 'number', 'position', 'bats_throws',
    'class_year', 'height', 'weight', 'team', 'school', 'conference',
    'state', 'hometown', 'high_school',
]

# ── Playwright availability check ─────────────────────────────────────────────
try:
    from playwright.sync_api import sync_playwright
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False


def fetch_with_playwright(url, wait_for='networkidle', timeout_ms=30000):
    """
    Fetch a page using a headless Chromium browser via Playwright.

    This bypasses Imperva and Cloudflare JS challenges that defeat requests
    and cloudscraper, because a real browser executes the challenge JS and
    returns the cookies before following the redirect.

    Returns (html_str, None) on success or (None, error_str) on failure.
    Requires: pip install playwright && playwright install chromium
    """
    if not _PLAYWRIGHT_AVAILABLE:
        return None, 'Playwright not installed'
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/121.0.0.0 Safari/537.36'
                )
            )
            page = ctx.new_page()
            page.goto(url, wait_until=wait_for, timeout=timeout_ms)
            html = page.content()
            browser.close()
        return html, None
    except Exception as e:
        return None, str(e)


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


def _sidearm_api_multi_season(base_url, team_name, conference, school_name, state):
    """
    Try SIDEARM NextGen API across multiple season-title formats.
    The main scraper only tries "2026"; this also tries "2025-26", "2026-27",
    and the API's own /list endpoint to find the right season title.
    """
    api_headers = {**HEADERS, 'Accept': 'application/json'}
    current_year = datetime.date.today().year
    season_candidates = [
        str(current_year),
        f"{current_year}-{str(current_year + 1)[-2:]}",
        f"{current_year - 1}-{str(current_year)[-2:]}",
        str(current_year - 1),
    ]

    # Try the list endpoint first to get the real season title
    try:
        r = requests.get(f"{base_url}/api/v2/Rosters/list?sport=baseball",
                         headers=api_headers, timeout=10)
        if r.status_code == 200:
            rosters = r.json()
            if rosters and isinstance(rosters, list):
                title = rosters[0].get('seasonTitle', '')
                if title:
                    season_candidates.insert(0, title)
    except Exception:
        pass

    for season in season_candidates:
        try:
            r = requests.get(
                f"{base_url}/api/v2/Rosters/bySport/baseball?season={season}",
                headers=api_headers, timeout=10,
            )
        except Exception:
            continue
        if r.status_code != 200:
            continue
        try:
            data = r.json()
        except Exception:
            continue

        raw_players = data.get('players', [])
        if not raw_players:
            continue

        players = []
        for p in raw_players:
            first = (p.get('firstName') or '').strip()
            last  = (p.get('lastName')  or '').strip()
            if not first and not last:
                continue
            hf = p.get('heightFeet') or ''
            hi = p.get('heightInches') or ''
            height = f"{hf}'{hi}\"" if hf and hi else (f"{hf}'" if hf else '')
            wt = p.get('weight')
            weight = f'{wt} lbs' if wt else ''
            class_year = p.get('academicYearLong') or p.get('academicYearShort') or ''
            players.append({
                'first_name': first,
                'last_name':  last,
                'team': team_name, 'school': school_name,
                'conference': conference, 'state': state,
                'number':    p.get('jerseyNumber') or '',
                'position':  p.get('positionShort') or p.get('positionLong') or '',
                'bats_throws': p.get('custom1') or '',
                'height':    height,
                'weight':    weight,
                'class_year': class_year,
                'hometown':  p.get('hometown') or '',
                'high_school': p.get('highSchool') or '',
            })
        if players:
            return players
    return []


def _parse_html(html, team_name, conference, school_name, state):
    """Run all HTML parsers against a page string. Returns player list or []."""
    soup = BeautifulSoup(html, 'html.parser')
    for parser in (
        parse_sidearm_roster,
        parse_old_sidearm_table,
        parse_script_json_roster,
        parse_wmt_digital_roster,
        parse_roster_list_items_rich,
        parse_nuxt_roster,
        parse_roster_list_items,
    ):
        players = parser(soup, team_name, conference, school_name, state)
        if players:
            return players
    return []


def try_scrape(school_row, nickname=''):
    """
    Try to scrape a school's baseball roster.
    Returns (players_list, status_msg).

    Strategy order:
      1. Fetch HTML with requests / cloudscraper → run all HTML parsers
      2. SIDEARM NextGen API (multi-season) — server-side, bypasses CDN JS challenges
      3. Playwright headless browser — executes Imperva/Cloudflare JS challenges;
         the right tool for schools like Gardner-Webb where the page IS there in a
         browser but requests/cloudscraper can't get past the JS wall
      4. ESPN roster API — names + jersey + position only; last resort because ESPN
         data can be stale (alumni) rather than the current D1 roster
    """
    base_url = school_row['base_url'].rstrip('/')

    # Fix common URL encoding issues (e.g. Arkansas Little Rock)
    if base_url.startswith('https:////'):
        base_url = 'https://' + base_url[len('https:////'):]
    elif base_url.startswith('http:////'):
        base_url = 'http://' + base_url[len('http:////'):]

    roster_url = school_row.get('roster_url', '').strip() or f"{base_url}/sports/baseball/roster"

    school_name = school_row['school']
    conference  = school_row['conference']
    state       = school_row['state']
    team_name   = f"{school_name} {nickname}".strip() if nickname else school_name

    notes = school_row.get('notes', '').lower()
    is_redirect_loop = 'redirect' in notes or 'redirect' in school_row.get('status', '').lower()
    is_404 = '404' in notes or '404' in school_row.get('status', '').lower()

    current_year = datetime.date.today().year
    alt_urls = [
        f"{base_url}/sports/bsb/{current_year}-{str(current_year+1)[-2:]}/roster",
        f"{base_url}/sports/bsb/{current_year-1}-{str(current_year)[-2:]}/roster",
        f"{base_url}/sports/mbaseball/roster",
        f"{base_url}/sports/m-baseball/roster",
        f"{base_url}/baseball/roster",
        f"{base_url}/sports/baseball/roster/{current_year}",
    ]

    # ── Attempt 1: fetch HTML with requests / cloudscraper ───────────────────
    r, err = fetch_roster_page(roster_url, use_cloudscraper=is_redirect_loop)

    # On 404 or invalid URL, try alt URL patterns
    if (err and ('404' in str(err) or 'host' in str(err).lower())) or \
       (r and r.status_code == 404) or is_404:
        for alt_url in alt_urls:
            r_alt, err_alt = fetch_roster_page(alt_url)
            if not err_alt and r_alt and r_alt.status_code == 200:
                text_check = r_alt.text[:8000].lower()
                if 'baseball' in text_check or 'bsb' in alt_url:
                    r, err = r_alt, None
                    roster_url = alt_url
                    break

    html_fetched = (r is not None and r.status_code == 200 and not err)

    if html_fetched:
        text_lower = r.text[:8000].lower()
        final_url_lower = r.url.lower()
        if 'baseball' not in final_url_lower and 'bsb' not in final_url_lower \
                and 'baseball' not in text_lower:
            html_fetched = False  # redirected away from baseball

    if html_fetched:
        players = _parse_html(r.text, team_name, conference, school_name, state)
        if players:
            return players, f"OK ({len(players)} players)"

    # ── Attempt 2: SIDEARM NextGen API ───────────────────────────────────────
    # Server-side endpoint — not subject to CDN JS challenges. Try early for
    # "200 but no content" (JS-rendered pages) AND for redirect-blocked schools.
    api_players = _sidearm_api_multi_season(
        base_url, team_name, conference, school_name, state)
    if api_players:
        return api_players, f"OK via API ({len(api_players)} players)"

    # ── Attempt 3: Playwright headless browser ────────────────────────────────
    # Real Chromium executes Imperva/Cloudflare JS challenges transparently.
    # Used when requests/cloudscraper hit a redirect loop or return no players.
    # Skipped (with a warning) if Playwright is not installed.
    if not _PLAYWRIGHT_AVAILABLE:
        print(f'    [playwright not available — install with: '
              f'pip install playwright && playwright install chromium]')
    else:
        print(f'    [trying Playwright for {school_name}...]')
        pw_html, pw_err = fetch_with_playwright(roster_url)
        if pw_html:
            players = _parse_html(pw_html, team_name, conference, school_name, state)
            if players:
                return players, f"OK via Playwright ({len(players)} players)"
            # Page loaded but still no players — might need a different URL
            for alt_url in alt_urls:
                pw_html2, _ = fetch_with_playwright(alt_url)
                if pw_html2:
                    players = _parse_html(pw_html2, team_name, conference, school_name, state)
                    if players:
                        return players, f"OK via Playwright alt-URL ({len(players)} players)"
        else:
            print(f'    [Playwright failed: {pw_err}]')

    # ── Attempt 4: ESPN — last resort ─────────────────────────────────────────
    # ESPN data can be stale (alumni, not current D1 roster). Only use when
    # every other strategy has failed. A thin or wrong ESPN result is still
    # better than nothing for coverage, but flag it clearly.
    espn_players = scrape_espn_roster(school_name, team_name, conference, state)
    if espn_players:
        return espn_players, f"OK via ESPN ({len(espn_players)} players) [verify roster accuracy]"

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
