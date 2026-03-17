#!/usr/bin/env python3
"""
enrich_player_profiles.py

Second-pass enrichment: visit individual SIDEARM player profile pages to fill
missing bt (bats/throws), yr (class year), ht (height), wt (weight), and
hometown for players that already have a profile_url in players.json.

Priority: schools where bt is 100% missing (highest impact on ID stability).

Strategy per player:
  1. Try SIDEARM NextGen API  (/api/v2/Rosters/player/{id})  — fastest, most complete
  2. Parse HTML profile page  (dl/dt/dd, JSON-LD, inline JSON)  — HTML fallback

After this script completes, run:
    python3 generate_ids.py
to refresh stable IDs using the newly filled bt/hometown data.

Checkpoint: enrich_profiles_progress.json (safe to re-run if interrupted).
"""

import json
import os
import re
import time
from collections import defaultdict

import requests
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}
API_HEADERS = {**HEADERS, 'Accept': 'application/json'}

_DIR           = os.path.dirname(os.path.abspath(__file__))
PLAYERS_JSON   = os.path.join(_DIR, 'players.json')
PROGRESS_FILE  = os.path.join(_DIR, 'enrich_profiles_progress.json')
DELAY          = 0.4   # seconds between profile page fetches
BATCH_SAVE     = 20    # save players.json every N schools


# ─── Bio field parsers ────────────────────────────────────────────────────────

# Map of recognised label strings → canonical field name
_LABEL_MAP = {
    # bats/throws
    'bats/throws': 'bt', 'b/t': 'bt', 'bats': 'bt', 'bat/throw': 'bt',
    'bats-throws': 'bt', 'bat/throws': 'bt',
    # class year
    'class': 'yr', 'yr.': 'yr', 'yr': 'yr', 'year': 'yr',
    'academic year': 'yr', 'eligibility': 'yr',
    # height
    'height': 'ht', 'ht.': 'ht', 'ht': 'ht',
    # weight
    'weight': 'wt', 'wt.': 'wt', 'wt': 'wt',
    # hometown
    'hometown': 'hometown', 'home town': 'hometown',
    'hometown/high school': 'hometown',
    # high school / previous school
    'high school': 'hs', 'previous school': 'hs', 'last school': 'hs',
    'prep school': 'hs', 'high school/previous school': 'hs',
}

# Normalise a class year string into the short form used in players.json
_YR_NORM = {
    'freshman': 'Fr.', 'fr.': 'Fr.', 'fr': 'Fr.',
    'redshirt freshman': 'Fr.', 'rs-freshman': 'Fr.', 'rs freshman': 'Fr.',
    'sophomore': 'So.', 'so.': 'So.', 'so': 'So.',
    'redshirt sophomore': 'So.', 'rs-sophomore': 'So.', 'rs sophomore': 'So.',
    'junior': 'Jr.', 'jr.': 'Jr.', 'jr': 'Jr.',
    'redshirt junior': 'Jr.', 'rs-junior': 'Jr.', 'rs junior': 'Jr.',
    'senior': 'Sr.', 'sr.': 'Sr.', 'sr': 'Sr.',
    'redshirt senior': 'Sr.', 'rs-senior': 'Sr.', 'rs senior': 'Sr.',
    'graduate': 'Gr.', 'grad': 'Gr.', 'gr.': 'Gr.',
    'graduate student': 'Gr.', 'fifth year': 'Gr.', '5th year': 'Gr.',
}


def _norm_yr(raw):
    """Normalise a raw class year string."""
    if not raw:
        return ''
    v = raw.strip()
    lower = v.lower()
    if lower in _YR_NORM:
        return _YR_NORM[lower]
    # Already short form?
    for norm_val in ('Fr.', 'So.', 'Jr.', 'Sr.', 'Gr.'):
        if v == norm_val:
            return v
    # Return as-is if non-empty but unrecognised
    return v


def _label_to_field(label_text):
    """Map an HTML label string to a canonical field name, or None."""
    lower = label_text.strip().rstrip(':').lower()
    return _LABEL_MAP.get(lower)


def parse_bio_from_html(html, url=''):
    """
    Extract bio fields from a SIDEARM player profile HTML page.
    Returns a dict with any subset of: bt, yr, ht, wt, hometown, hs.
    """
    soup = BeautifulSoup(html, 'html.parser')
    result = {}

    # ── Strategy 1: definition lists  <dl> <dt>label</dt> <dd>value</dd>
    for dl in soup.find_all('dl'):
        dts = dl.find_all('dt')
        dds = dl.find_all('dd')
        for dt, dd in zip(dts, dds):
            field = _label_to_field(dt.get_text())
            if not field:
                continue
            val = dd.get_text(separator=' ', strip=True)
            if val and field not in result:
                result[field] = val

    # ── Strategy 2: SIDEARM info-table  <table> with label/value rows
    if not result:
        for table in soup.find_all('table'):
            for row in table.find_all('tr'):
                cells = row.find_all(['th', 'td'])
                if len(cells) == 2:
                    field = _label_to_field(cells[0].get_text())
                    if field:
                        val = cells[1].get_text(separator=' ', strip=True)
                        if val and field not in result:
                            result[field] = val

    # ── Strategy 3: span/div pairs with a label class
    #    e.g. <span class="c-bio__label">Bats/Throws</span>
    #         <span class="c-bio__value">R/R</span>
    if 'bt' not in result:
        for el in soup.find_all(['span', 'div', 'p'],
                                class_=lambda c: c and any(
                                    'label' in ' '.join(c) for c in ([c] if isinstance(c, str) else c)
                                )):
            label_text = el.get_text(strip=True)
            field = _label_to_field(label_text)
            if not field:
                continue
            # Next sibling element is the value
            sibling = el.find_next_sibling()
            if sibling:
                val = sibling.get_text(separator=' ', strip=True)
                if val and field not in result:
                    result[field] = val

    # ── Strategy 4: SIDEARM profile data attributes / aria patterns
    #    <li data-label="Bats/Throws" data-value="R/R">
    for li in soup.find_all(lambda tag: tag.has_attr('data-label')):
        field = _label_to_field(li.get('data-label', ''))
        if field:
            val = li.get('data-value', '') or li.get_text(strip=True)
            if val and field not in result:
                result[field] = val

    # ── Strategy 5: specific SIDEARM class patterns
    #    <div class="sidearm-roster-player-*">
    for div in soup.find_all(True, class_=lambda c: c and any(
        'sidearm-roster-player-' in x for x in (c if isinstance(c, list) else c.split())
    )):
        class_str = ' '.join(div.get('class', []))
        field = None
        if 'custom_1' in class_str or 'custom-1' in class_str:
            field = 'bt'
        elif 'academic' in class_str or 'class' in class_str:
            field = 'yr'
        elif 'height' in class_str:
            field = 'ht'
        elif 'weight' in class_str:
            field = 'wt'
        elif 'hometown' in class_str:
            field = 'hometown'
        elif 'highschool' in class_str or 'high_school' in class_str:
            field = 'hs'
        if field:
            val = div.get_text(strip=True)
            if val and field not in result:
                result[field] = val

    # ── Strategy 6: JSON-LD schema.org/Person
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            ld = json.loads(script.string or '')
        except Exception:
            continue
        if not isinstance(ld, dict) or ld.get('@type') != 'Person':
            continue
        for key, field in [('alternateName', None), ('description', None)]:
            pass
        # Custom fields sometimes embedded in JSON-LD additionalProperty
        for prop in ld.get('additionalProperty', []):
            if not isinstance(prop, dict):
                continue
            field = _label_to_field(prop.get('name', ''))
            if field:
                val = str(prop.get('value', '')).strip()
                if val and field not in result:
                    result[field] = val

    # ── Strategy 7: inline player JSON objects
    #    Many SIDEARM pages embed player data in a <script> tag
    for script in soup.find_all('script'):
        text = script.string or ''
        if 'batsThrows' not in text and 'custom1' not in text and 'bats_throws' not in text:
            continue
        # Try to extract the player JSON object
        for pattern in [
            r'\{[^{}]*"batsThrows"\s*:\s*"([^"]+)"',
            r'\{[^{}]*"custom1"\s*:\s*"([^"]+)"',
            r'"bats_throws"\s*:\s*"([^"]+)"',
        ]:
            m = re.search(pattern, text)
            if m and 'bt' not in result:
                val = m.group(1).strip()
                if val:
                    result['bt'] = val

        for pattern in [
            r'"academicYearLong"\s*:\s*"([^"]+)"',
            r'"academicYearShort"\s*:\s*"([^"]+)"',
            r'"class_year"\s*:\s*"([^"]+)"',
        ]:
            m = re.search(pattern, text)
            if m and 'yr' not in result:
                val = m.group(1).strip()
                if val:
                    result['yr'] = val

        for pattern in [
            r'"heightDisplay"\s*:\s*"([^"]+)"',
            r'"height"\s*:\s*"([^"]+)"',
        ]:
            m = re.search(pattern, text)
            if m and 'ht' not in result:
                val = m.group(1).strip()
                if val:
                    result['ht'] = val

        for pattern in [
            r'"weight"\s*:\s*(\d+)',
        ]:
            m = re.search(pattern, text)
            if m and 'wt' not in result:
                val = m.group(1).strip()
                if val:
                    result['wt'] = val

        for pattern in [
            r'"hometown"\s*:\s*"([^"]+)"',
        ]:
            m = re.search(pattern, text)
            if m and 'hometown' not in result:
                val = m.group(1).strip()
                if val:
                    result['hometown'] = val

    # ── Post-processing ───────────────────────────────────────────────────────

    # Normalise bt to "X/X" format
    if 'bt' in result:
        bt = result['bt'].strip()
        # Filter out noise like "0" or long strings that aren't B/T
        if len(bt) > 10 or not re.match(r'^[LRS]/[LR]$', bt, re.I):
            # Try to extract B/T substring
            m = re.search(r'\b([LRS]/[LR])\b', bt, re.I)
            if m:
                bt = m.group(1).upper()
                result['bt'] = bt
            else:
                del result['bt']
        else:
            result['bt'] = bt.upper()

    # Normalise yr
    if 'yr' in result:
        result['yr'] = _norm_yr(result['yr'])
        if not result['yr']:
            del result['yr']

    # Normalise wt — keep only numeric part
    if 'wt' in result:
        m = re.search(r'(\d+)', result['wt'])
        if m:
            result['wt'] = m.group(1)
        else:
            del result['wt']

    # Normalise ht — standardise to "5' 10''" format
    if 'ht' in result:
        ht = result['ht'].strip()
        # Try to parse various height formats
        m = re.search(r"(\d+)['\u2019\u2032]\s*(\d+)", ht)
        if m:
            result['ht'] = f"{m.group(1)}' {m.group(2)}''"
        elif re.match(r"^\d+'?\s*\d*''?$", ht):
            pass  # already reasonable format
        elif not ht:
            del result['ht']

    return result


# ─── SIDEARM API lookup ───────────────────────────────────────────────────────

def _extract_player_id(profile_url):
    """Extract the numeric player ID from a SIDEARM profile URL."""
    if not profile_url:
        return None
    # Typical format: /sports/baseball/roster/firstname-lastname/12345
    m = re.search(r'/(\d+)/?$', profile_url)
    if m:
        return m.group(1)
    return None


def _extract_base_url(profile_url):
    """Extract the scheme+host from a profile URL."""
    from urllib.parse import urlparse
    parsed = urlparse(profile_url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return None


def fetch_via_api(profile_url):
    """
    Try the SIDEARM API to get player bio.
    Returns a bio dict or {}.
    """
    player_id = _extract_player_id(profile_url)
    base_url   = _extract_base_url(profile_url)
    if not player_id or not base_url:
        return {}

    api_url = f"{base_url}/api/v2/Rosters/player/{player_id}"
    try:
        r = requests.get(api_url, headers=API_HEADERS, timeout=10)
    except Exception:
        return {}

    if r.status_code != 200:
        return {}

    try:
        data = r.json()
    except Exception:
        return {}

    if not isinstance(data, dict):
        return {}

    result = {}

    bt = (data.get('custom1') or data.get('batsThrows') or
          data.get('bats_throws') or '').strip()
    if bt and re.match(r'^[LRS]/[LR]$', bt, re.I):
        result['bt'] = bt.upper()

    yr_raw = (data.get('academicYearLong') or data.get('academicYearShort') or
              data.get('classYear') or data.get('class_year') or '').strip()
    if yr_raw:
        norm = _norm_yr(yr_raw)
        if norm:
            result['yr'] = norm

    hf = str(data.get('heightFeet') or '').strip()
    hi = str(data.get('heightInches') or '').strip()
    if hf and hi:
        result['ht'] = f"{hf}' {hi}''"
    elif hf:
        result['ht'] = f"{hf}'"

    wt_raw = data.get('weight') or data.get('weightPounds')
    if wt_raw:
        m = re.search(r'(\d+)', str(wt_raw))
        if m:
            result['wt'] = m.group(1)

    hometown = (data.get('hometown') or '').strip()
    if hometown:
        result['hometown'] = hometown

    hs = (data.get('highSchool') or data.get('previousSchool') or '').strip()
    if hs:
        result['hs'] = hs

    return result


# ─── School prioritisation ────────────────────────────────────────────────────

def score_school(players_in_school):
    """
    Compute an enrichment priority score for a school.
    Higher = more worth visiting.
    """
    score = 0
    for p in players_in_school:
        if p.get('profile_url'):
            if not p.get('bt'):
                score += 3   # bt is most valuable (drives ID stability)
            if not p.get('yr'):
                score += 1
            if not p.get('wt'):
                score += 1
            if not p.get('ht'):
                score += 1
            if not p.get('hometown'):
                score += 2   # hometown also drives ID stability
    return score


# ─── Core enrichment logic ────────────────────────────────────────────────────

def enrich_player(player, progress_cache):
    """
    Fetch bio fields for one player.
    progress_cache: {profile_url: {field: value, ...}} — avoids refetching.
    Returns a dict of newly discovered fields (may be empty).
    """
    profile_url = player.get('profile_url', '')
    if not profile_url:
        return {}

    # Already attempted this URL in this run?
    if profile_url in progress_cache:
        cached = progress_cache[profile_url]
        return {k: v for k, v in cached.items() if not player.get(k)}

    new_fields = {}

    # ── Try SIDEARM API first
    new_fields = fetch_via_api(profile_url)

    # ── If API didn't give us bt, fall back to HTML
    if 'bt' not in new_fields:
        try:
            r = requests.get(profile_url, headers=HEADERS, timeout=15,
                             allow_redirects=True)
        except Exception:
            progress_cache[profile_url] = new_fields
            return {k: v for k, v in new_fields.items() if not player.get(k)}

        if r.status_code == 200:
            html_fields = parse_bio_from_html(r.text, profile_url)
            # Merge — HTML fields fill gaps not covered by API
            for k, v in html_fields.items():
                if k not in new_fields:
                    new_fields[k] = v

    progress_cache[profile_url] = new_fields
    return {k: v for k, v in new_fields.items() if not player.get(k)}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print('=' * 70)
    print('Player Profile Enrichment — bt / yr / ht / wt / hometown')
    print('=' * 70)

    # Load players
    with open(PLAYERS_JSON) as f:
        players = json.load(f)
    print(f'\nLoaded {len(players)} players from players.json')

    # Stats before
    def count_missing(field):
        return sum(1 for p in players if not p.get(field))
    print(f'\nBefore enrichment:')
    print(f'  Missing bt:       {count_missing("bt"):,}')
    print(f'  Missing yr:       {count_missing("yr"):,}')
    print(f'  Missing ht:       {count_missing("ht"):,}')
    print(f'  Missing wt:       {count_missing("wt"):,}')
    print(f'  Missing hometown: {count_missing("hometown"):,}')

    # Load progress checkpoint
    progress_cache = {}  # profile_url → {fields}
    school_done = set()
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            saved = json.load(f)
        progress_cache = saved.get('url_cache', {})
        school_done    = set(saved.get('schools_done', []))
        print(f'\nResuming — {len(school_done)} schools already done, '
              f'{len(progress_cache)} URLs cached')

    # Group players by school
    by_school = defaultdict(list)
    for p in players:
        by_school[p['school']].append(p)

    # Score and sort schools
    school_scores = [(s, score_school(ps), ps)
                     for s, ps in by_school.items()
                     if s not in school_done]
    school_scores.sort(key=lambda x: -x[1])

    # Only process schools where there's something to gain
    schools_to_do = [(s, ps) for s, score, ps in school_scores if score > 0]

    print(f'\nSchools to enrich: {len(schools_to_do)} '
          f'(of {len(by_school)} total)')
    print(f'Skipping {len(school_done)} already-completed schools')

    total_updated = 0
    schools_improved = 0
    field_gains = defaultdict(int)

    # Build an index: player id → position in players list for fast update
    player_index = {p['id']: i for i, p in enumerate(players)}

    for school_num, (school, school_players) in enumerate(schools_to_do, 1):
        pct = (school_num / len(schools_to_do)) * 100
        needs = sum(1 for p in school_players
                    if p.get('profile_url') and
                    not all(p.get(f) for f in ('bt', 'yr', 'ht', 'wt', 'hometown')))
        print(f'\n[{school_num}/{len(schools_to_do)}] ({pct:.1f}%) '
              f'{school} — {needs} players need enrichment')

        school_gained = 0
        for p in school_players:
            # Skip if all fields already present or no profile URL
            if not p.get('profile_url'):
                continue
            if all(p.get(f) for f in ('bt', 'yr', 'ht', 'wt', 'hometown')):
                continue

            new_fields = enrich_player(p, progress_cache)
            if new_fields:
                idx = player_index.get(p['id'])
                if idx is not None:
                    for k, v in new_fields.items():
                        players[idx][k] = v
                        field_gains[k] += 1
                school_gained += len(new_fields)
                total_updated += 1
                print(f'  + {p["name"]}: {new_fields}')
            time.sleep(DELAY)

        if school_gained:
            schools_improved += 1
            print(f'  → {school}: gained {school_gained} field values')

        school_done.add(school)

        # Save checkpoint periodically
        if school_num % BATCH_SAVE == 0:
            with open(PROGRESS_FILE, 'w') as f:
                json.dump({'url_cache': progress_cache,
                           'schools_done': list(school_done)}, f)
            with open(PLAYERS_JSON, 'w') as f:
                json.dump(players, f, indent=2)
            print(f'  [checkpoint saved — {total_updated} players updated so far]')

    # Final save
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({'url_cache': progress_cache,
                   'schools_done': list(school_done)}, f)
    with open(PLAYERS_JSON, 'w') as f:
        json.dump(players, f, indent=2)

    # Summary
    print(f'\n{"=" * 70}')
    print(f'Enrichment complete')
    print(f'  Schools improved:  {schools_improved}')
    print(f'  Players updated:   {total_updated:,}')
    print(f'\nField gains:')
    for field, count in sorted(field_gains.items(), key=lambda x: -x[1]):
        print(f'  {field:<12} +{count:,}')
    print(f'\nAfter enrichment:')
    print(f'  Missing bt:       {count_missing("bt"):,}')
    print(f'  Missing yr:       {count_missing("yr"):,}')
    print(f'  Missing ht:       {count_missing("ht"):,}')
    print(f'  Missing wt:       {count_missing("wt"):,}')
    print(f'  Missing hometown: {count_missing("hometown"):,}')
    print(f'\nNext step: run  python3 generate_ids.py  to refresh stable IDs.')


if __name__ == '__main__':
    main()
