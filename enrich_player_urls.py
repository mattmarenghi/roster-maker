#!/usr/bin/env python3
"""
Enrich players.json and d1_baseball_rosters_2026.csv with profile_url and photo_url.

Strategy:
  1. Load players.json, identify unique schools
  2. Fetch school base URLs from NCAA API
  3. For each school, hit roster page and extract name → (profile_url, photo_url)
     using all available parsing strategies
  4. Match to existing player records by normalized name + school
  5. Write updated players.json and CSV

Run time: ~5-15 minutes depending on network.
Checkpoint file: enrich_progress.json (safe to re-run if interrupted).
"""

import datetime
import json
import os
import re
import time

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

PLAYERS_JSON = '/home/user/roster-maker/players.json'
CSV_PATH     = '/home/user/roster-maker/d1_baseball_rosters_2026.csv'
PROGRESS_FILE = '/home/user/roster-maker/enrich_progress.json'
DELAY = 0.75
CURRENT_YEAR = datetime.date.today().year


# ─── Helpers ──────────────────────────────────────────────────────────────────

def norm(name):
    """Normalize a player name for matching: lowercase, collapse whitespace."""
    return ' '.join(str(name).lower().split())


def abs_url(path, base_url):
    """Convert relative path to absolute URL using base_url."""
    if not path:
        return ''
    path = str(path).strip()
    if path.startswith('http'):
        return path
    if path.startswith('//'):
        return 'https:' + path
    return urljoin(base_url.rstrip('/') + '/', path.lstrip('/'))


def is_placeholder(url):
    """Return True if image URL looks like a silhouette/placeholder."""
    lower = url.lower()
    return any(x in lower for x in ('silhouette', 'placeholder', 'default-', 'no-photo',
                                     'no_photo', 'nophoto', 'blank', 'generic'))


# ─── Extraction strategies ─────────────────────────────────────────────────────

def extract_from_api(base_url):
    """
    SIDEARM NextGen API — most reliable source for photo + profile URLs.
    Endpoint: /api/v2/Rosters/bySport/baseball?season=YYYY
    """
    api_headers = {**HEADERS, 'Accept': 'application/json'}
    for season in [str(CURRENT_YEAR), str(CURRENT_YEAR - 1)]:
        url = f"{base_url}/api/v2/Rosters/bySport/baseball?season={season}"
        try:
            r = requests.get(url, headers=api_headers, timeout=15)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        try:
            data = r.json()
        except Exception:
            continue

        players = data.get('players', [])
        if not players:
            continue

        result = {}
        for p in players:
            first = (p.get('firstName') or '').strip()
            last  = (p.get('lastName')  or '').strip()
            if not first and not last:
                continue
            name = norm(f"{first} {last}")

            # Profile URL — try explicit field, then construct from rosterPlayerId
            profile = (p.get('profileUrl') or p.get('profileLink') or
                       p.get('urlSlug') or p.get('link') or '')
            if not profile and p.get('rosterPlayerId'):
                slug = re.sub(r'[^a-z0-9]+', '-',
                              f"{first} {last}".lower()).strip('-')
                profile = f"/sports/baseball/roster/{slug}/{p['rosterPlayerId']}"
            if profile and not profile.startswith('http'):
                profile = abs_url(profile, base_url)

            # Photo URL — handle both string fields and the SIDEARM 'image' dict
            photo_raw = (p.get('image') or p.get('headShoulderPhoto') or
                         p.get('headShoulder') or p.get('photoUrl') or
                         p.get('imageUrl') or p.get('actionPhoto') or
                         p.get('headshot') or p.get('photo') or '')
            if isinstance(photo_raw, dict):
                photo = (photo_raw.get('absoluteUrl') or
                         photo_raw.get('url') or '')
            else:
                photo = str(photo_raw) if photo_raw else ''
            if photo and not photo.startswith('http'):
                photo = abs_url(photo, base_url)
            if photo and is_placeholder(photo):
                photo = ''

            result[name] = (profile, photo)

        if result:
            return result, 'api'

    return {}, None


def extract_from_cards(soup, base_url):
    """
    Modern SIDEARM / card-style roster pages.
    Players are rendered as cards (li or div) with photo + name link.
    """
    result = {}

    # Try several card selectors used by different SIDEARM themes
    cards = []
    for selector in [
        lambda s: s.find_all('li', class_=lambda c: c and any('s-roster' in x for x in c)),
        lambda s: s.find_all('div', class_=lambda c: c and any('s-roster' in x for x in c)),
        lambda s: s.find_all('li', class_='roster-list-item'),
        lambda s: s.find_all('article', class_=lambda c: c and 'roster' in ' '.join(c or [])),
        lambda s: s.find_all('div', class_=lambda c: c and 'roster-card' in ' '.join(c or [])),
        lambda s: s.find_all('div', class_=lambda c: c and 'player-card' in ' '.join(c or [])),
    ]:
        try:
            cards = selector(soup)
        except Exception:
            cards = []
        if cards:
            break

    for card in cards:
        # Find player name — look for named link or heading
        name_el = None
        for sel in [
            lambda c: c.find('a', class_=lambda x: x and 'name' in ' '.join(x or [])),
            lambda c: c.find(['h2', 'h3', 'h4'], class_=lambda x: x and 'name' in ' '.join(x or [])),
            lambda c: c.find('span', class_=lambda x: x and 'name' in ' '.join(x or [])),
            lambda c: c.find('a', href=True),
        ]:
            try:
                name_el = sel(card)
            except Exception:
                name_el = None
            if name_el:
                break

        if not name_el:
            continue

        name_text = name_el.get_text(strip=True)
        if not name_text or len(name_text) < 3:
            continue
        name = norm(name_text)

        # Profile URL from first <a> tag with href
        link_el = card.find('a', href=True)
        profile = abs_url(link_el['href'], base_url) if link_el else ''

        # Photo URL from <img>, prefer data-src for lazy-loaded images
        photo = ''
        img = card.find('img')
        if img:
            src = img.get('data-src') or img.get('src') or ''
            if src and not is_placeholder(src):
                photo = abs_url(src, base_url)

        if name:
            result[name] = (profile, photo)

    return (result, 'cards') if result else ({}, None)


def extract_from_table(soup, base_url):
    """
    SIDEARM HTML table — profile URL comes from <a> in the name cell.
    Photos are sometimes in a thumbnail column.
    """
    result = {}
    tables = soup.find_all('table')

    for table in tables:
        rows = table.find_all('tr')
        if len(rows) < 5:
            continue

        # Detect name column index from header
        header_cells = rows[0].find_all('th')
        name_col = None
        for i, th in enumerate(header_cells):
            label = th.find('span', {'aria-label': True})
            text  = (label['aria-label'] if label else th.get_text(strip=True)).lower()
            if any(x in text for x in ('name', 'player', 'first name')):
                name_col = i
                break
        if name_col is None and len(header_cells) > 1:
            name_col = 1  # sensible default for SIDEARM

        for row in rows[1:]:
            cells = row.find_all('td')
            if not cells or name_col is None or name_col >= len(cells):
                continue

            name_cell = cells[name_col]
            a = name_cell.find('a', href=True)
            if not a:
                continue

            name_text = a.get_text(strip=True)
            if not name_text:
                continue
            name = norm(name_text)
            profile = abs_url(a['href'], base_url)

            photo = ''
            img = row.find('img')
            if img:
                src = img.get('data-src') or img.get('src') or ''
                if src and not is_placeholder(src):
                    photo = abs_url(src, base_url)

            if name:
                result[name] = (profile, photo)

        if result:
            return result, 'table'

    return {}, None


def extract_from_old_sidearm(soup, base_url):
    """
    Old SIDEARM format: <th class="name"><a href="/roster/...">First\nLast</a></th>
    """
    result = {}
    for row in soup.find_all('tr'):
        name_th = row.find('th', class_='name')
        if not name_th:
            continue
        a = name_th.find('a', href=True)
        if not a:
            continue

        texts = [t.strip() for t in a.stripped_strings if t.strip()]
        if not texts:
            continue

        full_name = ' '.join(texts)
        name = norm(full_name)
        profile = abs_url(a['href'], base_url)

        photo = ''
        img = row.find('img')
        if img:
            src = img.get('data-src') or img.get('src') or ''
            if src and not is_placeholder(src):
                photo = abs_url(src, base_url)

        if name:
            result[name] = (profile, photo)

    return (result, 'old_sidearm') if result else ({}, None)


def extract_from_script_json(soup, base_url):
    """
    SIDEARM inline JS player arrays containing displayName + profile/photo fields.
    """
    result = {}
    for script in soup.find_all('script'):
        text = script.string or ''
        if 'displayName' not in text:
            continue

        for match in re.finditer(
            r'\[\s*\{[^\[\]]*"displayName"[^\[\]]*\}\s*(?:,\s*\{[^\[\]]*\}\s*)*\]',
            text, re.DOTALL
        ):
            try:
                arr = json.loads(match.group(0))
            except Exception:
                continue
            if not isinstance(arr, list):
                continue

            for p in arr:
                if not isinstance(p, dict):
                    continue
                name_text = p.get('displayName', '').strip()
                if not name_text:
                    continue
                name = norm(name_text)

                profile = (p.get('profileUrl') or p.get('urlSlug') or p.get('link') or '')
                if profile and not profile.startswith('http'):
                    profile = abs_url(profile, base_url)

                photo = (p.get('headShoulderPhoto') or p.get('photoUrl') or
                         p.get('imageUrl') or p.get('photo') or '')
                if photo and not photo.startswith('http'):
                    photo = abs_url(photo, base_url)
                if photo and is_placeholder(photo):
                    photo = ''

                if name:
                    result[name] = (profile, photo)

            if result:
                return result, 'script_json'

    return {}, None


def extract_from_nuxt(soup, base_url):
    """
    Nuxt3 __NUXT_DATA__ embedded JSON (OAS Sports / some newer SIDEARM sites).
    Looks for photo and profile URL fields inside the nested data structure.
    """
    script_tag = soup.find('script', id='__NUXT_DATA__')
    if not script_tag or not script_tag.string:
        return {}, None

    try:
        data = json.loads(script_tag.string)
    except Exception:
        return {}, None

    def resolve(v, seen=None, depth=0):
        """Recursively resolve Nuxt turbo payload references, including nested dicts/lists."""
        if seen is None:
            seen = set()
        if depth > 15 or not isinstance(v, int) or v < 0 or v >= len(data):
            return v
        if v in seen:
            return None  # circular reference
        seen = seen | {v}
        item = data[v]
        if item is None or isinstance(item, (str, float, bool)):
            return item
        if isinstance(item, int):
            return resolve(item, seen, depth + 1)
        if isinstance(item, list):
            if (len(item) == 2 and isinstance(item[0], str)
                    and item[0] in ('ShallowReactive', 'Reactive', 'Ref', 'ShallowRef')
                    and isinstance(item[1], int)):
                return resolve(item[1], seen, depth + 1)
            return [resolve(i, seen, depth + 1) if isinstance(i, int) else i
                    for i in item]
        if isinstance(item, dict):
            return {k: resolve(rv, seen, depth + 1) if isinstance(rv, int) else rv
                    for k, rv in item.items()}
        return item

    # Find players list in pinia store
    players_list = None
    for item in data:
        if not isinstance(item, dict):
            continue
        for k, v in item.items():
            if isinstance(k, str) and 'players-list' in k:
                container = resolve(v)
                if isinstance(container, dict) and 'players' in container:
                    pl = container['players']
                    if isinstance(pl, int):
                        pl = resolve(pl)
                    players_list = pl
                    break
        if players_list is not None:
            break

    if not isinstance(players_list, list):
        return {}, None

    result = {}
    for p_idx in players_list:
        p = resolve(p_idx) if isinstance(p_idx, int) else p_idx
        if not isinstance(p, dict):
            continue

        # Resolve the nested player sub-dict
        player_raw = p.get('player')
        player_sub = resolve(player_raw) if isinstance(player_raw, int) else player_raw
        if not isinstance(player_sub, dict):
            continue

        def field(key, d=None):
            d = d or p
            v = d.get(key)
            if isinstance(v, int):
                v = resolve(v)
            return '' if v is None else str(v)

        first = field('first_name', player_sub)
        last  = field('last_name',  player_sub)
        if not first and not last:
            continue
        name = norm(f"{first} {last}")

        # --- Profile URL ---
        # Try explicit fields first, then construct from slug + roster entry ID
        profile = field('profile_url') or field('profileUrl') or field('url_slug') or ''
        if not profile:
            slug = field('slug', player_sub)
            roster_id = p.get('id')
            if isinstance(roster_id, int) and roster_id < len(data):
                roster_id = resolve(roster_id)
            if slug and roster_id:
                profile = f"/sports/baseball/roster/{slug}/{roster_id}"
        if profile and not profile.startswith('http'):
            profile = abs_url(profile, base_url)

        # --- Photo URL ---
        # Try explicit string fields first
        photo = (field('head_shoulder_photo') or field('headShoulderPhoto') or
                 field('photo_url') or field('photoUrl') or field('image_url') or '')
        # OAS Sports stores photo as a dict with 'url' key
        if not photo:
            photo_obj = p.get('photo')
            if isinstance(photo_obj, int):
                photo_obj = resolve(photo_obj)
            if isinstance(photo_obj, dict):
                photo = photo_obj.get('url', '')
                if isinstance(photo, int):
                    photo = resolve(photo)
                if not photo:
                    photo = ''
                else:
                    photo = str(photo)
        if photo and not photo.startswith('http'):
            photo = abs_url(photo, base_url)
        if photo and is_placeholder(photo):
            photo = ''

        if name:
            result[name] = (profile, photo)

    return (result, 'nuxt') if result else ({}, None)


# ─── School enrichment orchestrator ───────────────────────────────────────────

def enrich_school(base_url):
    """
    Try all strategies to build a name → (profile_url, photo_url) map for a school.
    Returns (result_dict, strategy_name).
    """
    # 1. SIDEARM NextGen API — richest data, no HTML parsing needed
    result, strategy = extract_from_api(base_url)
    if result:
        return result, strategy

    # 2. Fetch HTML roster page
    roster_urls = [
        f"{base_url}/sports/baseball/roster",
        f"{base_url}/sports/bsb/{CURRENT_YEAR}-{str(CURRENT_YEAR+1)[-2:]}/roster",
        f"{base_url}/sports/bsb/{CURRENT_YEAR-1}-{str(CURRENT_YEAR)[-2:]}/roster",
        f"{base_url}/sports/mbaseball/roster",
        f"{base_url}/sports/m-baseball/roster",
    ]

    r = None
    for url in roster_urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
            if resp.status_code == 200:
                r = resp
                break
        except Exception:
            continue

    if not r:
        return {}, 'fetch_failed'

    text_lower = r.text[:8000].lower()
    if 'baseball' not in r.url.lower() and 'baseball' not in text_lower:
        return {}, 'redirected_away'

    soup = BeautifulSoup(r.text, 'html.parser')

    # 3. Card layout (modern SIDEARM)
    result, strategy = extract_from_cards(soup, base_url)
    if result:
        return result, strategy

    # 4. HTML table with name links
    result, strategy = extract_from_table(soup, base_url)
    if result:
        return result, strategy

    # 5. Old SIDEARM <th class="name">
    result, strategy = extract_from_old_sidearm(soup, base_url)
    if result:
        return result, strategy

    # 6. Sidearm inline JS displayName arrays
    result, strategy = extract_from_script_json(soup, base_url)
    if result:
        return result, strategy

    # 7. Nuxt3 __NUXT_DATA__
    result, strategy = extract_from_nuxt(soup, base_url)
    if result:
        return result, strategy

    return {}, 'no_urls_found'


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("Player URL Enrichment — profile_url + photo_url")
    print("=" * 70)

    # Load existing players
    with open(PLAYERS_JSON) as f:
        players = json.load(f)
    print(f"\nLoaded {len(players)} players from players.json")

    # Build school → base_url map from NCAA API
    print("\nFetching school list from NCAA API...")
    school_urls = {}
    try:
        r = requests.get(
            'https://web3.ncaa.org/directory/api/directory/memberList?type=12&division=I',
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=15
        )
        schools = r.json()
        for s in schools:
            name = s.get('nameOfficial', '')
            url  = (s.get('athleticWebUrl') or '').strip().rstrip('/')
            if name and url:
                if not url.startswith('http'):
                    url = 'https://' + url
                school_urls[name] = url
        print(f"  {len(school_urls)} schools with URLs from NCAA API")
    except Exception as e:
        print(f"  NCAA API failed: {e}")

    # Supplement with missing_schools.csv base_urls if it exists
    missing_csv = '/home/user/roster-maker/missing_schools.csv'
    if os.path.exists(missing_csv):
        import csv
        with open(missing_csv) as f:
            for row in csv.DictReader(f):
                name = row.get('school', '').strip()
                url  = row.get('base_url', '').strip().rstrip('/')
                if name and url and name not in school_urls:
                    school_urls[name] = url

    # Get unique schools present in players.json
    unique_schools = sorted({p['school'] for p in players})
    print(f"  {len(unique_schools)} unique schools in players.json")

    # Load checkpoint
    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            progress = json.load(f)
        print(f"  Resuming — {len(progress)} schools already done")

    # --- Enrichment pass ---
    print(f"\nEnriching {len(unique_schools)} schools...\n")
    url_map = {}  # school → {norm_name → (profile_url, photo_url)}
    strategy_counts = {}

    for i, school in enumerate(unique_schools):
        if school in progress:
            url_map[school] = {entry[0]: (entry[1], entry[2])
                               for entry in progress[school]['entries']}
            strat = progress[school].get('strategy', 'cached')
            strategy_counts[strat] = strategy_counts.get(strat, 0) + 1
            continue

        base_url = school_urls.get(school, '')
        if not base_url:
            print(f"  [{i+1}/{len(unique_schools)}] SKIP (no URL): {school}")
            progress[school] = {'entries': [], 'strategy': 'no_url'}
            continue

        pct = (i / len(unique_schools)) * 100
        print(f"  [{i+1}/{len(unique_schools)}] ({pct:.0f}%) {school}...", end=' ', flush=True)

        result, strategy = enrich_school(base_url)
        strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        if result:
            url_map[school] = result
            print(f"{len(result)} players [{strategy}]")
        else:
            print(f"no URLs [{strategy}]")

        # Save checkpoint
        progress[school] = {
            'strategy': strategy,
            'entries': [[name, prof, photo] for name, (prof, photo) in result.items()],
        }
        if (i + 1) % 10 == 0:
            with open(PROGRESS_FILE, 'w') as f:
                json.dump(progress, f)

        time.sleep(DELAY)

    # Final checkpoint save
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)

    # --- Match URLs to players ---
    print(f"\nMatching URLs to player records...")
    matched = 0
    profile_only = 0
    photo_only = 0
    both = 0
    unmatched = 0

    for p in players:
        school = p['school']
        name   = norm(f"{p['fn']} {p['ln']}")
        school_map = url_map.get(school, {})

        profile_url, photo_url = school_map.get(name, ('', ''))

        p['profile_url'] = profile_url
        p['photo_url']   = photo_url

        if profile_url and photo_url:
            both += 1
            matched += 1
        elif profile_url:
            profile_only += 1
            matched += 1
        elif photo_url:
            photo_only += 1
            matched += 1
        else:
            unmatched += 1

    print(f"  Both URLs:      {both:,}")
    print(f"  Profile only:   {profile_only:,}")
    print(f"  Photo only:     {photo_only:,}")
    print(f"  No URLs found:  {unmatched:,}")
    print(f"  Total matched:  {matched:,} / {len(players):,}")

    # --- Update players.json ---
    print(f"\nWriting players.json...")
    with open(PLAYERS_JSON, 'w') as f:
        json.dump(players, f, indent=2)
    print(f"  Done — {len(players)} players")

    # --- Update CSV ---
    print(f"\nUpdating CSV...")
    import pandas as pd
    df = pd.read_csv(CSV_PATH)

    # Build lookup: (first_name, last_name, school) → (profile_url, photo_url)
    player_lookup = {}
    for p in players:
        key = (p['fn'].strip(), p['ln'].strip(), p['school'].strip())
        player_lookup[key] = (p.get('profile_url', ''), p.get('photo_url', ''))

    def get_profile(row):
        key = (str(row['first_name']).strip(), str(row['last_name']).strip(),
               str(row['school']).strip())
        return player_lookup.get(key, ('', ''))[0]

    def get_photo(row):
        key = (str(row['first_name']).strip(), str(row['last_name']).strip(),
               str(row['school']).strip())
        return player_lookup.get(key, ('', ''))[1]

    df['profile_url'] = df.apply(get_profile, axis=1)
    df['photo_url']   = df.apply(get_photo,   axis=1)
    df.to_csv(CSV_PATH, index=False)
    print(f"  CSV updated: {len(df)} rows, columns now include profile_url + photo_url")

    # --- Summary ---
    print(f"\n{'─'*70}")
    print(f"Strategy breakdown:")
    for strat, count in sorted(strategy_counts.items(), key=lambda x: -x[1]):
        print(f"  {strat:<20} {count:>4} schools")

    print(f"\nDone!")


if __name__ == '__main__':
    main()
