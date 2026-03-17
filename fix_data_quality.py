#!/usr/bin/env python3
"""
fix_data_quality.py

Clean up known data quality issues in players.json and d1_baseball_rosters_2026.csv:

1. Iowa State University / University of Northern Iowa — same 18 ESPN-scraped
   players attributed to both schools. These are old alumni data, not current
   2026 D1 rosters. Remove both sets so the schools appear in missing_schools.csv
   for proper re-scraping.

2. University of San Francisco — 2 players scraped from ESPN who are MLB veterans
   (not current D1 students). Remove them.

3. Any school with < 5 players AND no profile_urls AND no bt/yr data is likely
   a bad ESPN scrape.  Flag these for review but don't auto-remove.

After running this script, run:
    python3 scrape_missing_targeted.py   # to attempt recovery
    python3 generate_ids.py              # to refresh stable IDs
"""

import csv
import json
import os
import sys

PLAYERS_JSON = '/home/user/roster-maker/players.json'
CSV_PATH     = '/home/user/roster-maker/d1_baseball_rosters_2026.csv'
MISSING_CSV  = '/home/user/roster-maker/missing_schools.csv'

# Schools to fully remove from the database.
# These have confirmed bad data (wrong players or ESPN duplicate attributions).
REMOVE_SCHOOLS = {
    'Iowa State University',           # same 18 players as Northern Iowa
    'University of Northern Iowa',     # same 18 players as Iowa State
    'University of San Francisco',     # 2 MLB vets (Cimber, Zimmer) — not D1
}


def load_missing_csv():
    rows = []
    with open(MISSING_CSV) as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def save_missing_csv(rows):
    fields = ['school', 'conference', 'state', 'base_url', 'roster_url', 'status', 'notes']
    with open(MISSING_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    print('=' * 60)
    print('Data Quality Fix')
    print('=' * 60)

    # ── Load players ─────────────────────────────────────────────
    with open(PLAYERS_JSON) as f:
        players = json.load(f)
    print(f'\nLoaded {len(players)} players')

    # ── Remove bad-data schools ───────────────────────────────────
    removed_by_school = {}
    clean_players = []
    for p in players:
        if p['school'] in REMOVE_SCHOOLS:
            removed_by_school.setdefault(p['school'], []).append(p['name'])
        else:
            clean_players.append(p)

    for school, names in sorted(removed_by_school.items()):
        print(f'\nRemoved {len(names)} players from {school}:')
        for name in names[:5]:
            print(f'  - {name}')
        if len(names) > 5:
            print(f'  ... and {len(names) - 5} more')

    removed_total = len(players) - len(clean_players)
    print(f'\nTotal removed: {removed_total} players '
          f'({len(players)} → {len(clean_players)})')

    # ── Detect other suspicious thin rosters ─────────────────────
    from collections import defaultdict, Counter
    school_counts = Counter(p['school'] for p in clean_players)
    school_no_profile = defaultdict(int)
    school_no_bt = defaultdict(int)
    for p in clean_players:
        if not p.get('profile_url'):
            school_no_profile[p['school']] += 1
        if not p.get('bt'):
            school_no_bt[p['school']] += 1

    print('\nSuspicious thin rosters (< 20 players, no profile_url, no bt):')
    flagged = []
    for school, count in sorted(school_counts.items(), key=lambda x: x[1]):
        if count < 20:
            no_prof = school_no_profile[school] == count  # ALL missing profile
            no_bt   = school_no_bt[school] == count       # ALL missing bt
            if no_prof and no_bt:
                flagged.append((school, count))
                print(f'  {school}: {count} players (all missing profile+bt)')
    if not flagged:
        print('  None found.')

    # ── Save players.json ─────────────────────────────────────────
    with open(PLAYERS_JSON, 'w') as f:
        json.dump(clean_players, f, indent=2)
    print(f'\nSaved players.json ({len(clean_players)} players)')

    # ── Update CSV ────────────────────────────────────────────────
    import pandas as pd
    if os.path.exists(CSV_PATH):
        df = pd.read_csv(CSV_PATH)
        df_clean = df[~df['school'].isin(REMOVE_SCHOOLS)]
        df_clean.to_csv(CSV_PATH, index=False)
        print(f'Saved CSV ({len(df_clean)} rows, removed {len(df) - len(df_clean)})')

    # ── Update missing_schools.csv ────────────────────────────────
    # Add removed schools back as needs_investigation so they get re-scraped
    existing_missing = load_missing_csv()
    already_in_missing = {r['school'] for r in existing_missing}

    # Known base URLs for the removed schools
    school_urls = {
        'Iowa State University':          ('Big 12 Conference', 'IA', 'https://www.cyclones.com'),
        'University of Northern Iowa':    ('Missouri Valley Conference', 'IA', 'https://unipanthers.com'),
        'University of San Francisco':    ('West Coast Conference', 'CA', 'https://usfdons.com/index.aspx'),
    }

    added = 0
    for school in REMOVE_SCHOOLS:
        if school in already_in_missing:
            continue
        if school in school_urls:
            conf, state, base_url = school_urls[school]
            existing_missing.append({
                'school': school,
                'conference': conf,
                'state': state,
                'base_url': base_url,
                'roster_url': '',
                'status': 'needs_investigation',
                'notes': 'Bad ESPN data removed — needs re-scrape',
            })
            added += 1

    existing_missing.sort(key=lambda r: r['school'])
    save_missing_csv(existing_missing)
    print(f'Updated missing_schools.csv ({added} schools added back)')

    print('\nDone. Next steps:')
    print('  1. python3 scrape_missing_targeted.py  — re-scrape removed schools')
    print('  2. python3 generate_ids.py             — refresh stable IDs')


if __name__ == '__main__':
    main()
