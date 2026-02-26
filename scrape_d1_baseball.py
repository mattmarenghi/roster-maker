#!/usr/bin/env python3
"""
Scrape all NCAA Division 1 college baseball rosters (2026 season).

Data source: Official school athletic websites (SIDEARM Sports platform).
School list: NCAA web3 directory API.

Output: d1_baseball_rosters_2026.csv
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import sys
import re
import os
from urllib.parse import urlparse

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

OUTPUT_CSV = '/home/user/d1_baseball_rosters_2026.csv'
PROGRESS_FILE = '/home/user/scrape_progress.json'
ERRORS_FILE = '/home/user/scrape_errors.log'
DELAY_BETWEEN_REQUESTS = 0.75  # seconds


def parse_name(full_name):
    """Split full name into first and last name."""
    parts = full_name.strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    elif len(parts) == 2:
        return parts[0], parts[1]
    else:
        return parts[0], " ".join(parts[1:])


def normalize_url(raw_url):
    """Ensure URL has https:// scheme."""
    url = raw_url.strip().rstrip('/')
    if not url:
        return None
    if not url.startswith('http'):
        url = 'https://' + url
    return url


def detect_columns(header_row):
    """Parse SIDEARM table header to detect column names."""
    headers_th = header_row.find_all('th')
    columns = []
    for th in headers_th:
        # SIDEARM uses aria-label on spans inside th
        label_span = th.find('span', {'aria-label': True})
        if label_span:
            columns.append(label_span['aria-label'])
        else:
            text = th.get_text(strip=True)
            columns.append(text if text else 'Unknown')
    return columns


def parse_sidearm_roster(soup, team_name, conference, school_name, state):
    """Parse a SIDEARM Sports roster table."""
    tables = soup.find_all('table')
    if not tables:
        return []

    # The roster table is typically the first and largest table
    roster_table = None
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) > 5:  # Must have more than a few rows to be a roster
            roster_table = table
            break

    if roster_table is None:
        # Try first table if no large one found
        roster_table = tables[0]

    rows = roster_table.find_all('tr')
    if len(rows) < 2:
        return []

    columns = detect_columns(rows[0])

    # Build a column name mapping for common variations
    col_map = {}
    for i, col in enumerate(columns):
        cl = col.lower()
        if 'jersey' in cl or col == '#' or col == '-' and i == 0:
            col_map['number'] = i
        elif 'first name' in cl and 'last name' in cl:
            col_map['full_name'] = i
        elif 'name' in cl and 'full' in cl:
            col_map['full_name'] = i
        elif cl == 'name' or cl == 'player':
            col_map['full_name'] = i
        elif 'position' in cl or cl == 'pos' or cl == 'pos.':
            col_map['position'] = i
        elif 'custom field' in cl or cl == 'b/t' or cl == 'bat/throw':
            col_map['bats_throws'] = i
        elif cl == 'height' or cl == 'ht.' or cl == 'ht':
            col_map['height'] = i
        elif cl == 'weight' or cl == 'wt.' or cl == 'wt':
            col_map['weight'] = i
        elif 'academic' in cl or cl == 'yr.' or cl == 'yr' or cl == 'class' or cl == 'cl.':
            col_map['class_year'] = i
        elif 'hometown' in cl or cl == 'hometown/high school':
            col_map['hometown'] = i
        elif 'high school' in cl or 'previous school' in cl or 'last school' in cl:
            col_map['high_school'] = i

    # If column 0 wasn't identified and the second column looks like names,
    # assume column 0 is jersey number
    if 'number' not in col_map and 'full_name' in col_map and col_map['full_name'] == 1:
        col_map['number'] = 0

    # If we didn't find full_name but col 1 seems like it, map it
    if 'full_name' not in col_map and len(columns) > 1:
        # Default: col 0 = number, col 1 = name
        if 'number' not in col_map:
            col_map['number'] = 0
        col_map['full_name'] = 1

    players = []
    for row in rows[1:]:
        cells = row.find_all('td')
        if len(cells) < 2:
            continue

        cell_texts = [cell.get_text(strip=True) for cell in cells]

        # Skip empty rows or header-like rows
        if all(not t for t in cell_texts):
            continue

        def get_field(key, default=''):
            idx = col_map.get(key)
            if idx is not None and idx < len(cell_texts):
                return cell_texts[idx]
            return default

        full_name = get_field('full_name', '')
        if not full_name:
            continue

        first_name, last_name = parse_name(full_name)

        record = {
            'first_name': first_name,
            'last_name': last_name,
            'team': team_name,
            'school': school_name,
            'conference': conference,
            'state': state,
            'number': get_field('number'),
            'position': get_field('position'),
            'bats_throws': get_field('bats_throws'),
            'height': get_field('height'),
            'weight': get_field('weight'),
            'class_year': get_field('class_year'),
            'hometown': get_field('hometown'),
            'high_school': get_field('high_school'),
        }
        players.append(record)

    return players


def scrape_team_roster(base_url, team_name, conference, school_name, state):
    """Attempt to scrape a team's baseball roster. Returns list of player dicts."""
    url = f"{base_url}/sports/baseball/roster"

    try:
        r = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
    except requests.exceptions.SSLError:
        # Try http fallback
        try:
            http_url = url.replace('https://', 'http://')
            r = requests.get(http_url, headers=HEADERS, timeout=20, allow_redirects=True)
        except Exception as e:
            return None, f"SSL+HTTP error: {e}"
    except requests.exceptions.ConnectionError as e:
        return None, f"Connection error: {e}"
    except requests.exceptions.Timeout:
        return None, "Timeout"
    except Exception as e:
        return None, f"Request error: {e}"

    if r.status_code == 404:
        return None, "404 - no baseball program or different URL"
    elif r.status_code == 403:
        return None, "403 - access denied"
    elif r.status_code != 200:
        return None, f"HTTP {r.status_code}"

    # Check if we landed on a valid baseball roster page
    if 'baseball' not in r.url.lower() and 'baseball' not in r.text[:5000].lower():
        return None, "Redirected away from baseball page"

    soup = BeautifulSoup(r.text, 'html.parser')

    # Check page title for year
    title = soup.find('title')
    title_text = title.get_text() if title else ''

    players = parse_sidearm_roster(soup, team_name, conference, school_name, state)

    if not players:
        return None, "No players parsed from page"

    return players, None


def load_progress():
    """Load scraping progress from file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {'completed': [], 'all_players': []}


def save_progress(progress):
    """Save scraping progress to file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)


def log_error(msg):
    """Append error message to error log."""
    with open(ERRORS_FILE, 'a') as f:
        f.write(msg + '\n')


def main():
    print("=" * 80)
    print("NCAA Division 1 Baseball Roster Scraper — 2026 Season")
    print("=" * 80)

    # Step 1: Load school list from NCAA API
    print("\n[1/3] Fetching D1 school list from NCAA directory API...")
    try:
        r = requests.get(
            'https://web3.ncaa.org/directory/api/directory/memberList?type=12&division=I',
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=15
        )
        schools = r.json()
    except Exception as e:
        # Fall back to cached file
        print(f"  API error: {e}, loading from cache...")
        with open('/home/user/ncaa_d1_schools.json') as f:
            schools = json.load(f)

    print(f"  Found {len(schools)} D1 member schools")

    # Build team list with URLs
    teams = []
    for s in schools:
        url = s.get('athleticWebUrl', '')
        if not url:
            continue
        base_url = normalize_url(url)
        if not base_url:
            continue

        name = s.get('nameOfficial', '')
        nickname = s.get('nickname', '')
        conf = s.get('conferenceName', '') or 'Unknown'
        state = ''
        if s.get('memberOrgAddress'):
            state = s['memberOrgAddress'].get('state', '') or ''

        # Use school name + nickname as team display name if possible
        team_display = name
        if nickname:
            team_display = f"{name} {nickname}"

        teams.append({
            'school_name': name,
            'team_name': team_display,
            'conference': conf,
            'state': state,
            'base_url': base_url,
            'org_id': s.get('orgId', ''),
        })

    teams.sort(key=lambda x: x['school_name'])
    print(f"  {len(teams)} schools with athletic URLs")

    # Step 2: Scrape rosters
    print(f"\n[2/3] Scraping baseball rosters...")
    print(f"  Rate limit: {DELAY_BETWEEN_REQUESTS}s between requests")
    print(f"  Progress saved to: {PROGRESS_FILE}")
    print()

    # Load previous progress
    progress = load_progress()
    completed_schools = set(progress['completed'])
    all_players = progress['all_players']

    if completed_schools:
        print(f"  Resuming: {len(completed_schools)} schools already done, {len(all_players)} players collected")

    # Clear error log on fresh start
    if not completed_schools:
        with open(ERRORS_FILE, 'w') as f:
            f.write("Scrape Errors Log\n" + "=" * 60 + "\n")

    success_count = len([s for s in completed_schools if not s.startswith('ERROR:')])
    skip_count = len([s for s in completed_schools if s.startswith('ERROR:')])
    total = len(teams)

    for i, team in enumerate(teams):
        school = team['school_name']

        # Skip if already done
        if school in completed_schools or f"ERROR:{school}" in completed_schools:
            continue

        pct = (len(completed_schools) / total) * 100
        print(f"  [{len(completed_schools)+1}/{total}] ({pct:.0f}%) {school}...", end=' ', flush=True)

        players, error = scrape_team_roster(
            team['base_url'], team['team_name'], team['conference'],
            team['school_name'], team['state']
        )

        if error:
            print(f"SKIP ({error})")
            log_error(f"{school} | {team['base_url']} | {error}")
            completed_schools.add(f"ERROR:{school}")
            skip_count += 1
        else:
            print(f"OK — {len(players)} players")
            all_players.extend(players)
            completed_schools.add(school)
            success_count += 1

        # Save progress every 10 teams
        if (len(completed_schools)) % 10 == 0:
            progress['completed'] = list(completed_schools)
            progress['all_players'] = all_players
            save_progress(progress)

        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Final save
    progress['completed'] = list(completed_schools)
    progress['all_players'] = all_players
    save_progress(progress)

    # Step 3: Write CSV
    print(f"\n[3/3] Writing CSV...")
    print(f"  Teams scraped successfully: {success_count}")
    print(f"  Teams skipped (no baseball / error): {skip_count}")
    print(f"  Total players: {len(all_players)}")

    if all_players:
        df = pd.DataFrame(all_players)
        # Order columns nicely
        col_order = ['first_name', 'last_name', 'number', 'position', 'bats_throws',
                     'class_year', 'height', 'weight', 'team', 'school', 'conference',
                     'state', 'hometown', 'high_school']
        for c in col_order:
            if c not in df.columns:
                df[c] = ''
        df = df[col_order]
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"\n  Saved to: {OUTPUT_CSV}")
        print(f"  File size: {os.path.getsize(OUTPUT_CSV) / 1024:.1f} KB")

        # Quick stats
        print(f"\n  --- Quick Stats ---")
        print(f"  Total players: {len(df)}")
        print(f"  Teams represented: {df['school'].nunique()}")
        print(f"  Conferences represented: {df['conference'].nunique()}")
        print(f"  Players per team (avg): {len(df) / df['school'].nunique():.1f}")
    else:
        print("  No players scraped!")

    print(f"\n  Error log: {ERRORS_FILE}")
    print("  Done!")


if __name__ == '__main__':
    main()
