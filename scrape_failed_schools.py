#!/usr/bin/env python3
"""
Retry scraper for failed D1 baseball schools.

Handles cases the original scraper missed:
1. Nuxt3 OAS Sports SSR sites  - parse __NUXT_DATA__ JSON
2. roster-list-item HTML sites  - parse <li class="roster-list-item"> elements
3. SIDEARM JSON-LD fallback     - schema.org Person structured data
4. SIDEARM NextGen API          - /api/v2/Rosters/bySport/baseball?season=YYYY
5. Imperva-blocked schools      - ESPN college baseball roster API (names only)
"""

import requests
import json
import re
import time
import os
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

ERRORS_FILE = '/home/user/scrape_errors.log'
OUTPUT_CSV_EXISTING = '/home/user/d1_baseball_rosters_2026.csv'
OUTPUT_CSV_NEW = '/home/user/d1_baseball_rosters_2026_retry.csv'
DELAY = 0.75


# ─────────────────────────────────────────────────────────────────────────────
# NUXT3 __NUXT_DATA__ PARSER  (OAS Sports / auburntigers.com-style)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_idx(data, idx, depth=0):
    if depth > 8:
        return None
    if not isinstance(idx, int) or idx < 0 or idx >= len(data):
        return idx
    item = data[idx]
    if isinstance(item, (str, float, bool)) or item is None:
        return item
    if isinstance(item, int):
        return resolve_idx(data, item, depth + 1)
    if isinstance(item, list):
        if (len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], int)
                and item[0] in ('ShallowReactive', 'Reactive', 'Ref', 'ShallowRef')):
            return resolve_idx(data, item[1], depth + 1)
        return [resolve_idx(data, x, depth + 1) if isinstance(x, int) else x for x in item]
    if isinstance(item, dict):
        return {k: resolve_idx(data, v, depth + 1) if isinstance(v, int) else v
                for k, v in item.items()}
    return item


def parse_nuxt_roster(soup, team_name, conference, school_name, state):
    script_tag = soup.find('script', id='__NUXT_DATA__')
    if not script_tag:
        # Some sites put it inline without the id attribute
        for s in soup.find_all('script'):
            if s.string and s.string.strip().startswith('[["ShallowReactive"'):
                script_tag = s
                break
    if not script_tag or not script_tag.string:
        return []

    try:
        data = json.loads(script_tag.string)
    except Exception:
        return []

    # Find state map with roster players list
    players_store_idx = None
    for item in data:
        if isinstance(item, dict):
            for k, v in item.items():
                if isinstance(k, str) and 'players-list' in k and isinstance(v, int):
                    players_store_idx = v
                    break
        if players_store_idx is not None:
            break

    if players_store_idx is None:
        return []

    roster_store = data[players_store_idx] if players_store_idx < len(data) else None
    if not isinstance(roster_store, dict) or 'players' not in roster_store:
        return []

    players_list_idx = roster_store['players']
    if not isinstance(players_list_idx, int) or players_list_idx >= len(data):
        return []

    players_indices = data[players_list_idx]
    if not isinstance(players_indices, list):
        return []

    players = []
    for p_idx in players_indices:
        if not isinstance(p_idx, int) or p_idx >= len(data):
            continue
        entry = data[p_idx]
        if not isinstance(entry, dict):
            continue

        player_sub = resolve_idx(data, entry.get('player')) if isinstance(entry.get('player'), int) else entry.get('player')

        pos_raw = resolve_idx(data, entry.get('player_position')) if isinstance(entry.get('player_position'), int) else entry.get('player_position')
        position = ''
        if isinstance(pos_raw, dict):
            pos_abbr = resolve_idx(data, pos_raw.get('abbreviation')) if isinstance(pos_raw.get('abbreviation'), int) else pos_raw.get('abbreviation')
            pos_name = resolve_idx(data, pos_raw.get('name')) if isinstance(pos_raw.get('name'), int) else pos_raw.get('name')
            position = pos_abbr or pos_name or ''
        elif isinstance(pos_raw, str):
            position = pos_raw

        class_raw = resolve_idx(data, entry.get('class_level')) if isinstance(entry.get('class_level'), int) else entry.get('class_level')
        class_year = ''
        if isinstance(class_raw, dict):
            c_name = resolve_idx(data, class_raw.get('name')) if isinstance(class_raw.get('name'), int) else class_raw.get('name')
            class_year = c_name or ''
        elif isinstance(class_raw, str):
            class_year = class_raw

        def field(key, from_dict=None):
            d = from_dict if from_dict is not None else entry
            v = d.get(key)
            if isinstance(v, int):
                v = resolve_idx(data, v)
            return '' if v is None else str(v)

        jersey = field('jersey_number')
        weight_val = field('weight')
        height_feet = field('height_feet')
        height_inches = field('height_inches')

        height = ''
        if height_feet and height_inches:
            height = f"{height_feet}'{height_inches}\""
        elif height_feet:
            height = f"{height_feet}'"

        weight = f'{weight_val} lbs' if weight_val else ''

        first_name = last_name = hometown = high_school = ''
        if isinstance(player_sub, dict):
            first_name = field('first_name', player_sub)
            last_name = field('last_name', player_sub)
            hometown = field('hometown', player_sub)
            high_school = field('high_school', player_sub)

        if not first_name and not last_name:
            continue

        players.append({
            'first_name': first_name,
            'last_name': last_name,
            'team': team_name,
            'school': school_name,
            'conference': conference,
            'state': state,
            'number': jersey,
            'position': position,
            'bats_throws': '',
            'height': height,
            'weight': weight,
            'class_year': class_year,
            'hometown': hometown,
            'high_school': high_school,
        })

    return players


# ─────────────────────────────────────────────────────────────────────────────
# ROSTER-LIST-ITEM HTML PARSER
# ─────────────────────────────────────────────────────────────────────────────

def parse_roster_list_items(soup, team_name, conference, school_name, state):
    items = soup.find_all('li', class_='roster-list-item')
    if not items:
        return []

    players = []
    for li in items:
        text_parts = [t.strip() for t in li.get_text(separator='|').split('|') if t.strip()]
        seen = set()
        unique_parts = []
        for p in text_parts:
            if p not in seen:
                seen.add(p)
                unique_parts.append(p)

        social_noise = {'Instagram', 'Opens in a new window', 'Twitter', 'Facebook',
                        'Full Bio', 'View Bio', 'View Profile'}
        parts = [p for p in unique_parts if p not in social_noise]

        if not parts:
            continue

        jersey = name = class_year = height = weight = position = hometown = high_school = ''

        i = 0
        if parts and re.match(r'^#?\d+$', parts[0]):
            jersey = parts[0].lstrip('#')
            i = 1

        if i < len(parts):
            name = parts[i]
            i += 1

        for part in parts[i:]:
            pl = part.lower()
            if pl in ('freshman', 'sophomore', 'junior', 'senior', 'graduate', 'grad',
                      'rs-freshman', 'rs-sophomore', 'rs-junior', 'rs-senior',
                      'redshirt freshman', 'redshirt sophomore', 'redshirt junior',
                      'redshirt senior', 'fifth year', '5th year', 'graduate student'):
                class_year = part
            elif re.match(r"^\d[\u2019'\u2032][\d]{1,2}[\u201d\"\u2033]?$", part):
                height = part
            elif re.match(r'^\d{2,3}\s*lbs?\.?$', part, re.I):
                weight = part
            elif any(pos in pl for pos in ['pitcher', 'catcher', 'infield', 'outfield',
                                            'utility', 'rhp', 'lhp', 'if', 'of',
                                            'first base', 'second base', 'third base',
                                            'shortstop', 'dh']):
                position = part
            elif ',' in part and not hometown:
                hometown = part
            elif hometown and not high_school and not re.match(r'^\d', part):
                high_school = part

        if not name:
            continue

        name_parts = name.strip().split()
        first_name = name_parts[0] if name_parts else ''
        last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''

        players.append({
            'first_name': first_name,
            'last_name': last_name,
            'team': team_name,
            'school': school_name,
            'conference': conference,
            'state': state,
            'number': jersey,
            'position': position,
            'bats_throws': '',
            'height': height,
            'weight': weight,
            'class_year': class_year,
            'hometown': hometown,
            'high_school': high_school,
        })

    return players


# ─────────────────────────────────────────────────────────────────────────────
# SIDEARM JSON-LD FALLBACK
# ─────────────────────────────────────────────────────────────────────────────

def parse_jsonld_roster(soup, team_name, conference, school_name, state):
    players = []
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            ld = json.loads(script.string or '')
        except Exception:
            continue

        items = []
        if isinstance(ld, dict) and 'item' in ld:
            items = ld['item']
        elif isinstance(ld, list):
            items = ld

        for item in items:
            if not isinstance(item, dict) or item.get('@type') != 'Person':
                continue
            name = item.get('name', '').strip()
            if not name:
                continue
            parts = name.split()
            first_name = parts[0] if parts else ''
            last_name = ' '.join(parts[1:]) if len(parts) > 1 else ''
            players.append({
                'first_name': first_name, 'last_name': last_name,
                'team': team_name, 'school': school_name,
                'conference': conference, 'state': state,
                'number': '', 'position': '', 'bats_throws': '',
                'height': '', 'weight': '', 'class_year': '',
                'hometown': '', 'high_school': '',
            })

    return players


# ─────────────────────────────────────────────────────────────────────────────
# SIDEARM NEXTGEN API  (/api/v2/Rosters/bySport/baseball?season=YYYY)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_sidearm_nextgen(base_url, team_name, conference, school_name, state):
    """
    Try the SIDEARM NextGen REST API to find and fetch a baseball roster.
    Returns list of player dicts or empty list.
    """
    api_headers = {
        'User-Agent': HEADERS['User-Agent'],
        'Accept': 'application/json',
    }

    # 1. Get list of available rosters
    list_url = f'{base_url}/api/v2/Rosters/list?sport=baseball'
    try:
        r = requests.get(list_url, headers=api_headers, timeout=12)
    except Exception:
        return []

    if r.status_code != 200:
        return []

    try:
        rosters = r.json()
    except Exception:
        return []

    if not rosters:
        return []

    # 2. Pick most recent roster (first in the list)
    latest = rosters[0]
    season_title = latest.get('seasonTitle', '')
    roster_slug = 'baseball'

    roster_url = f'{base_url}/api/v2/Rosters/bySport/{roster_slug}?season={season_title}'
    try:
        r2 = requests.get(roster_url, headers=api_headers, timeout=12)
    except Exception:
        return []

    if r2.status_code != 200:
        return []

    try:
        roster_data = r2.json()
    except Exception:
        return []

    raw_players = roster_data.get('players', [])
    if not raw_players:
        return []

    players = []
    for p in raw_players:
        first_name = p.get('firstName', '').strip()
        last_name = p.get('lastName', '').strip()
        if not first_name and not last_name:
            continue

        hf = p.get('heightFeet') or ''
        hi = p.get('heightInches') or ''
        height = f"{hf}'{hi}\"" if hf and hi else (f"{hf}'" if hf else '')

        wt = p.get('weight')
        weight = f'{wt} lbs' if wt else ''

        class_year = p.get('academicYearLong') or p.get('academicYearShort') or ''

        players.append({
            'first_name': first_name,
            'last_name': last_name,
            'team': team_name,
            'school': school_name,
            'conference': conference,
            'state': state,
            'number': p.get('jerseyNumber') or '',
            'position': p.get('positionShort') or p.get('positionLong') or '',
            'bats_throws': p.get('custom1') or '',
            'height': height,
            'weight': weight,
            'class_year': class_year,
            'hometown': p.get('hometown') or '',
            'high_school': p.get('highSchool') or '',
        })

    return players


# ─────────────────────────────────────────────────────────────────────────────
# ESPN COLLEGE BASEBALL API FALLBACK  (for Imperva-blocked schools)
# ─────────────────────────────────────────────────────────────────────────────

_espn_team_map = None

def _build_espn_map():
    global _espn_team_map
    if _espn_team_map is not None:
        return _espn_team_map
    try:
        r = requests.get(
            'https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams',
            headers={'User-Agent': HEADERS['User-Agent'], 'Accept': 'application/json'},
            timeout=15, params={'limit': 500}
        )
        if r.status_code == 200:
            teams = r.json().get('sports', [{}])[0].get('leagues', [{}])[0].get('teams', [])
            _espn_team_map = {t['team']['displayName']: t['team']['id'] for t in teams}
        else:
            _espn_team_map = {}
    except Exception:
        _espn_team_map = {}
    return _espn_team_map


def scrape_espn_roster(school_name, team_name, conference, state):
    """
    Try ESPN's college baseball API. Returns list of player dicts (names only).
    """
    espn_map = _build_espn_map()

    # Find team ID by fuzzy matching school name
    team_id = None
    school_lower = school_name.lower()
    for espn_name, tid in espn_map.items():
        # Try to match key words from school name
        words = [w for w in school_lower.split() if len(w) > 3
                 and w not in ('university', 'college', 'the', 'state')]
        if words and any(w in espn_name.lower() for w in words):
            team_id = tid
            break

    if not team_id:
        return []

    try:
        r = requests.get(
            f'https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}/roster',
            headers={'User-Agent': HEADERS['User-Agent'], 'Accept': 'application/json'},
            timeout=15
        )
    except Exception:
        return []

    if r.status_code != 200:
        return []

    try:
        athletes = r.json().get('athletes', [])
    except Exception:
        return []

    players = []
    for a in athletes:
        item = a.get('items', [a])[0] if 'items' in a else a
        first_name = item.get('firstName', '').strip()
        last_name = item.get('lastName', '').strip()
        # Skip if first name is just an initial
        if len(first_name) <= 2:
            continue
        if not first_name and not last_name:
            continue

        players.append({
            'first_name': first_name,
            'last_name': last_name,
            'team': team_name,
            'school': school_name,
            'conference': conference,
            'state': state,
            'number': item.get('jersey', ''),
            'position': item.get('position', {}).get('abbreviation', '')
                        if isinstance(item.get('position'), dict) else '',
            'bats_throws': '',
            'height': '',
            'weight': '',
            'class_year': '',
            'hometown': '',
            'high_school': '',
        })

    return players


# ─────────────────────────────────────────────────────────────────────────────
# MAIN FETCH FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def scrape_school(base_url, team_name, conference, school_name, state):
    """Try all strategies. Returns (players, strategy, error)."""
    url = base_url.rstrip('/') + '/sports/baseball/roster'

    try:
        r = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
    except Exception as e:
        # Could be redirect loop (Imperva) - try ESPN fallback
        espn_players = scrape_espn_roster(school_name, team_name, conference, state)
        if espn_players:
            return espn_players, 'espn', None
        return [], None, f'Request error: {e}'

    if r.status_code == 404:
        return [], None, '404 - no baseball program or different URL'
    elif r.status_code != 200:
        return [], None, f'HTTP {r.status_code}'

    if 'baseball' not in r.url.lower() and 'baseball' not in r.text[:3000].lower():
        return [], None, 'Redirected away from baseball page'

    soup = BeautifulSoup(r.text, 'html.parser')

    # Strategy 1: Nuxt3 __NUXT_DATA__
    players = parse_nuxt_roster(soup, team_name, conference, school_name, state)
    if players:
        return players, 'nuxt3', None

    # Strategy 2: roster-list-item HTML
    players = parse_roster_list_items(soup, team_name, conference, school_name, state)
    if players:
        return players, 'roster-list-item', None

    # Strategy 3: SIDEARM NextGen REST API
    players = scrape_sidearm_nextgen(base_url, team_name, conference, school_name, state)
    if players:
        return players, 'sidearm-nextgen', None

    # Strategy 4: JSON-LD schema.org Person
    players = parse_jsonld_roster(soup, team_name, conference, school_name, state)
    if players:
        return players, 'jsonld', None

    return [], None, 'No players parsed from page (all strategies failed)'


# ─────────────────────────────────────────────────────────────────────────────
# IMPERVA-BLOCKED SCHOOLS  (separate pass using only ESPN)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_blocked_school(base_url, team_name, conference, school_name, state):
    """For Imperva-blocked schools, use ESPN directly."""
    players = scrape_espn_roster(school_name, team_name, conference, state)
    if players:
        return players, 'espn', None
    return [], None, 'ESPN: no data found'


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    import pandas as pd

    print('=' * 80)
    print('NCAA D1 Baseball Retry Scraper v2 — failed schools')
    print('=' * 80)

    # Parse error log
    failed = {}
    with open(ERRORS_FILE) as f:
        for line in f:
            line = line.strip()
            if '|' not in line or line.startswith('Scrape') or line.startswith('==='):
                continue
            parts = line.split('|')
            if len(parts) >= 3:
                school = parts[0].strip()
                url = parts[1].strip().rstrip('/')
                error = '|'.join(parts[2:]).strip()
                if not url.startswith('http'):
                    url = 'https://' + url
                failed[school] = {'url': url, 'error': error}

    # Split into categories
    no_players = {k: v for k, v in failed.items()
                  if 'no players' in v['error'].lower()}
    redirect_loop = {k: v for k, v in failed.items()
                     if 'redirect' in v['error'].lower()}
    not_found = {k: v for k, v in failed.items()
                 if '404' in v['error'] or 'no baseball' in v['error'].lower()}

    print(f'\nNo-players schools: {len(no_players)}')
    print(f'Redirect-loop (Imperva) schools: {len(redirect_loop)}')
    print(f'404/no-baseball schools: {len(not_found)} (skipping)')

    # Load school metadata
    with open('/home/user/ncaa_d1_schools.json') as f:
        schools_json = json.load(f)

    school_meta = {}
    for s in schools_json:
        name = s.get('nameOfficial', '')
        nickname = s.get('nickname', '') or ''
        conf = s.get('conferenceName', '') or 'Unknown'
        state_code = s.get('memberOrgAddress', {}).get('state', '') or '' \
            if s.get('memberOrgAddress') else ''
        team_display = f'{name} {nickname}'.strip() if nickname else name
        school_meta[name] = {'team_name': team_display, 'conference': conf, 'state': state_code}

    all_players = []
    results = {'success': [], 'fail': []}

    # --- Pass 1: no-players schools (try all web strategies) ---
    print(f'\n[Pass 1] No-players schools ({len(no_players)} schools)...')
    for i, (school, info) in enumerate(sorted(no_players.items())):
        meta = school_meta.get(school, {})
        team_name = meta.get('team_name', school)
        conference = meta.get('conference', 'Unknown')
        state = meta.get('state', '')

        pct = (i / len(no_players)) * 100
        print(f'  [{i+1}/{len(no_players)}] ({pct:.0f}%) {school}...', end=' ', flush=True)

        players, strategy, error = scrape_school(
            info['url'], team_name, conference, school, state
        )

        if players:
            print(f'OK — {len(players)} players [{strategy}]')
            all_players.extend(players)
            results['success'].append({'school': school, 'count': len(players), 'strategy': strategy})
        else:
            print(f'FAIL ({error})')
            results['fail'].append({'school': school, 'error': error, 'type': 'no-players'})

        time.sleep(DELAY)

    # --- Pass 2: Imperva-blocked schools (ESPN only) ---
    print(f'\n[Pass 2] Imperva-blocked schools ({len(redirect_loop)} schools)...')
    for i, (school, info) in enumerate(sorted(redirect_loop.items())):
        meta = school_meta.get(school, {})
        team_name = meta.get('team_name', school)
        conference = meta.get('conference', 'Unknown')
        state = meta.get('state', '')

        pct = (i / len(redirect_loop)) * 100
        print(f'  [{i+1}/{len(redirect_loop)}] ({pct:.0f}%) {school}...', end=' ', flush=True)

        players, strategy, error = scrape_blocked_school(
            info['url'], team_name, conference, school, state
        )

        if players:
            print(f'OK — {len(players)} players [{strategy}]')
            all_players.extend(players)
            results['success'].append({'school': school, 'count': len(players), 'strategy': strategy})
        else:
            print(f'FAIL ({error})')
            results['fail'].append({'school': school, 'error': error, 'type': 'imperva'})

        time.sleep(0.3)  # ESPN is more lenient

    # --- Summary ---
    print(f'\n{"─"*60}')
    print(f'Successfully scraped: {len(results["success"])} schools')
    print(f'Failed: {len(results["fail"])} schools')
    print(f'Total new players: {len(all_players)}')

    by_strategy = {}
    for r in results['success']:
        by_strategy[r['strategy']] = by_strategy.get(r['strategy'], 0) + 1
    print(f'By strategy: {by_strategy}')

    if results['fail']:
        print(f'\nStill failing ({len(results["fail"])} schools):')
        for r in results['fail'][:20]:
            print(f'  [{r["type"]}] {r["school"]}: {r["error"]}')

    # --- Write output ---
    if all_players:
        col_order = ['first_name', 'last_name', 'number', 'position', 'bats_throws',
                     'class_year', 'height', 'weight', 'team', 'school', 'conference',
                     'state', 'hometown', 'high_school']
        df_new = pd.DataFrame(all_players)
        for c in col_order:
            if c not in df_new.columns:
                df_new[c] = ''
        df_new = df_new[col_order]

        df_existing = pd.read_csv(OUTPUT_CSV_EXISTING)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined.drop_duplicates(subset=['first_name', 'last_name', 'school'], inplace=True)
        df_combined.to_csv(OUTPUT_CSV_NEW, index=False)

        print(f'\nExisting players: {len(df_existing)}')
        print(f'New players added: {len(df_new)}')
        print(f'Combined total: {len(df_combined)}')
        print(f'Teams in combined: {df_combined["school"].nunique()}')
        print(f'Saved to: {OUTPUT_CSV_NEW}')

    print('\nDone.')


if __name__ == '__main__':
    main()
