#!/usr/bin/env python3
"""
Find D1 schools with baseball programs not in our roster database.
Outputs missing_schools.csv for tracking.
"""

import csv
import json
import time
import requests
from urllib.parse import urlparse

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

def normalize_url(raw_url):
    if not raw_url:
        return None
    url = raw_url.strip().rstrip('/')
    if not url:
        return None
    if not url.startswith('http'):
        url = 'https://' + url
    return url

def has_baseball_roster(base_url):
    """Return (found, status, final_url) for a school's baseball roster page."""
    url = f"{base_url}/sports/baseball/roster"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
    except requests.exceptions.SSLError:
        try:
            url = url.replace('https://', 'http://')
            r = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        except Exception as e:
            return False, f"SSL+HTTP error: {e}", url
    except Exception as e:
        return False, f"Error: {e}", url

    if r.status_code == 200:
        text_lower = r.text[:8000].lower()
        if 'baseball' in text_lower and ('roster' in text_lower or 'player' in text_lower):
            return True, f"HTTP {r.status_code}", r.url
        else:
            return False, f"HTTP {r.status_code} but no baseball/roster content", r.url
    else:
        return False, f"HTTP {r.status_code}", url

def main():
    # Load schools already in our database
    have_schools = set()
    with open('d1_baseball_rosters_2026.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            have_schools.add(row['school'].strip())

    print(f"Schools already in database: {len(have_schools)}")

    # Fetch full D1 school list from NCAA API
    print("Fetching D1 school list from NCAA API...")
    try:
        r = requests.get(
            'https://web3.ncaa.org/directory/api/directory/memberList?type=12&division=I',
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=15
        )
        schools = r.json()
        print(f"  Got {len(schools)} D1 schools")
    except Exception as e:
        print(f"  API error: {e}")
        return

    # Build list of schools not in our database
    missing = []
    for s in schools:
        name = s.get('nameOfficial', '').strip()
        if not name:
            continue
        if name in have_schools:
            continue
        url = normalize_url(s.get('athleticWebUrl', ''))
        if not url:
            continue
        conf = s.get('conferenceName', '') or 'Unknown'
        state = ''
        if s.get('memberOrgAddress'):
            state = s['memberOrgAddress'].get('state', '') or ''
        missing.append({
            'school': name,
            'conference': conf,
            'state': state,
            'base_url': url,
        })

    missing.sort(key=lambda x: x['school'])
    print(f"Schools not in database: {len(missing)}")
    print()
    print("Checking each for an active baseball roster page...")
    print()

    confirmed_missing = []
    no_baseball = []

    for i, s in enumerate(missing):
        print(f"  [{i+1}/{len(missing)}] {s['school']}...", end=' ', flush=True)
        found, status, final_url = has_baseball_roster(s['base_url'])
        if found:
            print(f"HAS BASEBALL — {status}")
            confirmed_missing.append({
                'school': s['school'],
                'conference': s['conference'],
                'state': s['state'],
                'base_url': s['base_url'],
                'roster_url': final_url,
                'status': 'confirmed_missing',
                'notes': '',
            })
        else:
            print(f"no baseball ({status})")
            no_baseball.append({
                'school': s['school'],
                'conference': s['conference'],
                'state': s['state'],
                'base_url': s['base_url'],
                'status': status,
            })
        time.sleep(0.5)

    print()
    print(f"Confirmed missing (have baseball, not in DB): {len(confirmed_missing)}")
    print(f"No baseball program found: {len(no_baseball)}")

    # Combine all rows: confirmed missing + needs investigation
    all_rows = confirmed_missing + [
        {
            'school': s['school'],
            'conference': s['conference'],
            'state': s['state'],
            'base_url': s['base_url'],
            'roster_url': '',
            'status': 'needs_investigation',
            'notes': s['status'],
        }
        for s in no_baseball
    ]
    all_rows.sort(key=lambda x: x['school'])

    # Write missing_schools.csv
    out_fields = ['school', 'conference', 'state', 'base_url', 'roster_url', 'status', 'notes']
    with open('missing_schools.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nSaved {len(all_rows)} schools to missing_schools.csv")
    print(f"  {len(confirmed_missing)} confirmed missing (have baseball, not in DB)")
    print(f"  {len(no_baseball)} needs_investigation (redirect loop, non-standard URL, or no baseball)")

    if confirmed_missing:
        print("\nConfirmed missing schools:")
        for s in confirmed_missing:
            print(f"  {s['school']} ({s['conference']}, {s['state']})")

if __name__ == '__main__':
    main()
