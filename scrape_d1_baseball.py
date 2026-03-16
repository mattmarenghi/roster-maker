#!/usr/bin/env python3
"""
Scrape all NCAA Division 1 college baseball rosters (2026 season).

Data source: Official school athletic websites (SIDEARM Sports platform).
School list: NCAA web3 directory API.

Output: d1_baseball_rosters_2026.csv
"""

import requests
import cloudscraper
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

    fn_idx = col_map.get('full_name', 1)  # header index where name lives

    players = []
    for row in rows[1:]:
        cells_td = row.find_all('td')

        # ------------------------------------------------------------------
        # Handle the <th scope="row"> pattern used by some schools (e.g. UCF,
        # BYU).  In these tables the header row has all <th> elements with the
        # normal column order, but each data row places the player name in a
        # <th scope="row"> and the remaining data in <td> cells.  The <td>
        # cells therefore correspond to header columns 0, 2, 3, 4 … (skipping
        # the name column).  We detect this pattern and adjust accordingly.
        # ------------------------------------------------------------------
        row_th = row.find('th', attrs={'scope': 'row'})
        if row_th is not None and len(cells_td) >= 2:
            full_name = row_th.get_text(strip=True)
            profile_link = row_th.find('a', href=True)

            cell_texts = [c.get_text(strip=True) for c in cells_td]

            def get_field_th(key, default=''):
                idx = col_map.get(key)
                if idx is None:
                    return default
                if idx == fn_idx:
                    return default  # name came from <th>
                # td cells skip the name column, so shift indices above fn_idx down by 1
                td_idx = idx if idx < fn_idx else idx - 1
                return cell_texts[td_idx] if td_idx < len(cell_texts) else default

            if not full_name:
                continue
            first_name, last_name = parse_name(full_name)

            position_raw = get_field_th('position', '')
            bats_throws = get_field_th('bats_throws', '')

            # Some schools (e.g. BYU) embed B/T inside the position cell as
            # "Outfielder (L/L)" — extract and strip it if bats_throws is empty.
            if not bats_throws and position_raw:
                bt_match = re.search(r'\(([LRSS]/[LRSS])\)', position_raw, re.IGNORECASE)
                if bt_match:
                    bats_throws = bt_match.group(1).upper()
                    position_raw = re.sub(r'\s*\([LRSS]/[LRSS]\)', '', position_raw, flags=re.IGNORECASE).strip()

            record = {
                'first_name': first_name,
                'last_name': last_name,
                'team': team_name,
                'school': school_name,
                'conference': conference,
                'state': state,
                'number': get_field_th('number'),
                'position': position_raw,
                'bats_throws': bats_throws,
                'height': get_field_th('height'),
                'weight': get_field_th('weight'),
                'class_year': get_field_th('class_year'),
                'hometown': get_field_th('hometown'),
                'high_school': get_field_th('high_school'),
                'profile_url': profile_link['href'] if profile_link else '',
            }
            players.append(record)
            continue

        # ------------------------------------------------------------------
        # Normal path: name is in a <td>
        # ------------------------------------------------------------------
        if len(cells_td) < 2:
            continue

        cell_texts = [cell.get_text(strip=True) for cell in cells_td]

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

        # Guard: if 'full_name' cell looks like a position/stat rather than a
        # real name (e.g. "Infield", "6-2"), skip this row — it means column
        # detection failed and we'd produce garbage records.
        POS_WORDS = ('infield', 'outfield', 'pitcher', 'catcher', 'shortstop',
                     'first base', 'second base', 'third base', 'utility',
                     'right-handed', 'left-handed', 'rhp', 'lhp')
        if any(pw in full_name.lower() for pw in POS_WORDS):
            return []  # bail out — this parse attempt is garbage

        # Guard: height-like value in the name column is also a bad sign
        if re.match(r'^\d[\'-]\d', full_name):
            return []

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
            'profile_url': '',
        }
        players.append(record)

    return players


def parse_old_sidearm_table(soup, team_name, conference, school_name, state):
    """
    Parse the old Sidearm Sports roster table format (pre-Nuxt).

    In this format each data row has:
      <th class="name"><a>FirstName\nLastName</a></th>
      <td class="number"><span class="label">No.:</span>1</td>
      <td class="pos"><span class="label">Pos.:</span>RHP</td>
      ... (bats_throws, height, weight, year, hometown_hs)
    """
    def cell_value(td):
        """Return cell text with the <span class='label'> stripped out."""
        label = td.find('span', class_='label')
        if label:
            label.decompose()
        return td.get_text(separator=' ', strip=True)

    players = []
    for row in soup.find_all('tr'):
        name_th = row.find('th', class_='name')
        if name_th is None:
            continue
        # Extract first/last name from the link text
        a = name_th.find('a')
        if a:
            texts = [t.strip() for t in a.stripped_strings]
        else:
            texts = [t.strip() for t in name_th.stripped_strings]
        # texts[0] = first name, texts[1] = last name (usually)
        texts = [t for t in texts if t]
        if not texts:
            continue
        first_name = texts[0] if len(texts) >= 1 else ''
        last_name = texts[1] if len(texts) >= 2 else ''

        tds = row.find_all('td')
        def get_td_class(cls):
            td = row.find('td', class_=cls)
            return cell_value(td) if td else ''

        number = get_td_class('number')
        position = get_td_class('position') or get_td_class('pos')
        bats_throws = get_td_class('bats_throws') or get_td_class('bat-throw') or get_td_class('bt')
        height = get_td_class('height') or get_td_class('ht')
        weight = get_td_class('weight') or get_td_class('wt')
        class_year = get_td_class('academic_year') or get_td_class('year') or get_td_class('cl')
        hometown_hs = get_td_class('hometown_highschool') or get_td_class('hometown') or get_td_class('hs')

        # Fall back to ordered TDs if class-based lookup returns nothing
        if not position and len(tds) >= 2:
            ordered = [cell_value(td) for td in tds]
            # try to map by order: number, pos, bt, ht, wt, cl, hometown
            if len(ordered) >= 1 and not number:
                number = ordered[0]
            if len(ordered) >= 2 and not position:
                position = ordered[1]
            if len(ordered) >= 3 and not bats_throws:
                bats_throws = ordered[2]
            if len(ordered) >= 4 and not height:
                height = ordered[3]
            if len(ordered) >= 5 and not weight:
                weight = ordered[4]
            if len(ordered) >= 6 and not class_year:
                class_year = ordered[5]
            if len(ordered) >= 7 and not hometown_hs:
                hometown_hs = ordered[6]

        # Split hometown/HS if combined
        hometown = hometown_hs
        high_school = ''
        if '/' in hometown_hs:
            parts = hometown_hs.split('/', 1)
            hometown = parts[0].strip()
            high_school = parts[1].strip()

        if not first_name and not last_name:
            continue

        players.append({
            'first_name': first_name,
            'last_name': last_name,
            'team': team_name,
            'school': school_name,
            'conference': conference,
            'state': state,
            'number': number,
            'position': position,
            'bats_throws': bats_throws,
            'height': height,
            'weight': weight,
            'class_year': class_year,
            'hometown': hometown,
            'high_school': high_school,
        })

    return players


def parse_sidearm_nextgen_api(base_url, team_name, conference, school_name, state):
    """
    Query the Sidearm next-gen (Nuxt 3) roster API directly.

    Endpoint: /api/v2/Rosters/bySport/baseball?season={year}
    Tries the current season first, falls back to previous season.
    Returns list of player dicts, or empty list if unavailable.
    """
    import datetime
    current_year = datetime.date.today().year
    seasons_to_try = [str(current_year), str(current_year - 1)]

    for season in seasons_to_try:
        url = f"{base_url}/api/v2/Rosters/bySport/baseball?season={season}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
        except Exception:
            continue
        if r.status_code != 200 or not r.text.strip():
            continue
        try:
            d = r.json()
        except Exception:
            continue
        raw_players = d.get('players', [])
        if not raw_players:
            continue

        players = []
        for p in raw_players:
            first = (p.get('firstName') or '').strip()
            last = (p.get('lastName') or '').strip()
            if not first and not last:
                continue
            ht_ft = p.get('heightFeet') or ''
            ht_in = p.get('heightInches') or ''
            height = f"{ht_ft}' {ht_in}\"" if ht_ft else ''
            players.append({
                'first_name': first,
                'last_name': last,
                'team': team_name,
                'school': school_name,
                'conference': conference,
                'state': state,
                'number': str(p.get('jerseyNumber') or ''),
                'position': str(p.get('positionShort') or ''),
                'bats_throws': str(p.get('custom1') or ''),
                'height': height,
                'weight': str(p.get('weight') or ''),
                'class_year': str(p.get('academicYearShort') or ''),
                'hometown': str(p.get('hometown') or ''),
                'high_school': str(p.get('highSchool') or p.get('previousSchool') or ''),
            })
        if players:
            return players

    return []


def parse_wmt_digital_roster(soup, team_name, conference, school_name, state):
    """
    Parse roster data from WMT Digital / OAS platform pages.

    These sites embed a large flat Nuxt array in an inline <script> tag.
    Player data is keyed under 'roster-{id}-players-list-page-1' in the
    pinia store, with values stored as index references into the flat array.
    """
    for script in soup.find_all('script'):
        text = script.string or ''
        if len(text) < 200000 or 'roster-' not in text:
            continue
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(data, list):
            continue

        def follow(v, depth=0):
            if depth > 8 or not isinstance(v, int) or v >= len(data):
                return v
            inner = data[v]
            if isinstance(inner, list) and len(inner) == 2 and inner[0] in ('Reactive', 'ShallowReactive'):
                return follow(inner[1], depth + 1)
            return inner

        # Find pinia store dict with 'roster-NNN-players-list-page-1' key
        players_list = None
        for item in data:
            if not isinstance(item, dict):
                continue
            for k in item.keys():
                if re.match(r'roster-\d+-players-list-page-\d+$', str(k)) and 'page-1' in str(k):
                    container = follow(item[k])
                    if isinstance(container, dict):
                        players_list = follow(container.get('players'))
                    break
            if players_list is not None:
                break

        if not isinstance(players_list, list) or not players_list:
            continue

        players = []
        for p_idx in players_list:
            p = follow(p_idx)
            if not isinstance(p, dict):
                continue

            # player sub-object has name, hometown, high_school
            player_sub = follow(p.get('player'))
            if not isinstance(player_sub, dict):
                continue

            first = follow(player_sub.get('first_name')) or ''
            last = follow(player_sub.get('last_name')) or ''
            if not first and not last:
                continue

            hometown = follow(player_sub.get('hometown')) or ''
            high_school = follow(player_sub.get('high_school')) or \
                          follow(player_sub.get('previous_school')) or ''

            # height / weight / jersey from roster_player object
            hf = follow(p.get('height_feet')) or ''
            hi = follow(p.get('height_inches'))
            height = f"{hf}' {hi}\"" if hf else ''
            weight = str(follow(p.get('weight')) or '')
            jersey = str(follow(p.get('jersey_number')) or '')

            # position
            pos_obj = follow(p.get('player_position'))
            pos = ''
            if isinstance(pos_obj, dict):
                pos = str(follow(pos_obj.get('abbreviation')) or '')

            # class level
            cl_obj = follow(p.get('class_level'))
            class_year = ''
            if isinstance(cl_obj, dict):
                class_year = str(follow(cl_obj.get('abbreviation')) or
                                 follow(cl_obj.get('name')) or '')

            players.append({
                'first_name': str(first),
                'last_name': str(last),
                'team': team_name,
                'school': school_name,
                'conference': conference,
                'state': state,
                'number': jersey,
                'position': pos,
                'bats_throws': '',
                'height': height,
                'weight': weight,
                'class_year': class_year,
                'hometown': str(hometown),
                'high_school': str(high_school),
            })

        if players:
            return players

    return []


def parse_roster_list_items_rich(soup, team_name, conference, school_name, state):
    """
    Parse SIDEARM NextGen (Nuxt 3) roster pages that render players as
    <li class="roster-list-item"> cards rather than an HTML table.

    Two layout variants handled:
      A) Stanford / VTech / UNM style — profile fields carry BEM modifier classes:
           roster-player-list-profile-field--position
           roster-player-list-profile-field--class-level
           roster-player-list-profile-field--height / --weight / --hometown / --high-school
         Name in .roster-list-item__title or .roster-list-item__title-link.
         Jersey in .roster-list-item__jersey-number or .roster-list-item__number.
      B) UTSA style — profile fields use generic class
           roster-players-list-item__profile-field
         classified by content pattern (height "5-9", weight digits, etc.).
         Position in <strong> inside .roster-players-list-item__position.
    """
    items = soup.find_all('li', class_='roster-list-item')
    if not items:
        return []

    HEIGHT_RE = re.compile(r"^\d+[''\-]\d+")
    WEIGHT_RE = re.compile(r'^\d{2,3}$')
    BT_RE = re.compile(r'^[LRS]/[LRS]$', re.IGNORECASE)
    CLASS_WORDS = {'fr.', 'so.', 'jr.', 'sr.', 'gr.', 'freshman', 'sophomore',
                   'junior', 'senior', 'graduate', 'fr', 'so', 'jr', 'sr', 'gr'}

    def field_text(tag):
        if tag is None:
            return ''
        # Collect text, skipping any child elements whose class contains 'label'
        # (e.g. <span class="roster-player-list-profile-field-label">Height</span>)
        parts = []
        for child in tag.children:
            # Skip label spans
            if hasattr(child, 'get') and child.get('class'):
                if any('label' in c for c in child.get('class', [])):
                    continue
            text = child.get_text(strip=True) if hasattr(child, 'get_text') else str(child).strip()
            if text:
                parts.append(text)
        return ' '.join(parts).strip()

    players = []
    for item in items:
        # --- Name ---
        name_tag = (item.find(class_='roster-list-item__title-link') or
                    item.find(class_='roster-list-item__title') or
                    item.find(class_='roster-list-item__name'))
        full_name = field_text(name_tag) if name_tag else ''
        if not full_name:
            continue
        first_name, last_name = parse_name(full_name)

        # --- Profile URL + photo ---
        img_wrapper = item.find(class_='roster-list-item__image-wrapper')
        profile_url = ''
        photo_url = ''
        if img_wrapper:
            a = img_wrapper.find('a', href=True)
            if a:
                profile_url = a['href']
            img = img_wrapper.find('img')
            if img:
                photo_url = img.get('data-src') or img.get('src') or ''

        # Fallback: link on the name itself
        if not profile_url and name_tag:
            a_tag = name_tag if name_tag.name == 'a' else name_tag.find('a', href=True)
            if a_tag:
                profile_url = a_tag.get('href', '')

        # --- Jersey number ---
        jersey_tag = (item.find(class_='roster-list-item__jersey-number') or
                      item.find(class_='roster-list-item__number') or
                      item.find(class_=lambda c: c and 'jersey' in c))
        jersey = field_text(jersey_tag)

        # --- Variant A: BEM modifier classes (--position, --class-level, etc.) ---
        def bem_field(suffix):
            tag = item.find(class_=lambda c: c and f'profile-field--{suffix}' in c)
            return field_text(tag)

        position   = bem_field('position')
        class_year = bem_field('class-level') or bem_field('academic-year') or bem_field('class')
        height     = bem_field('height')
        weight     = bem_field('weight')
        bats_throws = bem_field('bats-throws') or bem_field('bat-throw') or bem_field('bats')
        hometown   = bem_field('hometown')
        high_school = bem_field('high-school') or bem_field('previous-school')

        # --- Variant B: UTSA-style explicit position tag ---
        if not position:
            pos_tag = item.find(class_=lambda c: c and 'position' in c and 'profile-field' not in c)
            if pos_tag:
                strong = pos_tag.find('strong')
                position = field_text(strong or pos_tag)

        # --- Variant B: classify unlabeled generic profile-field spans by content ---
        if not height or not weight or not class_year:
            generic_tags = item.find_all(class_=lambda c: c and 'profile-field' in c and '--' not in c)
            for gtag in generic_tags:
                val = field_text(gtag)
                if not val:
                    continue
                if not height and HEIGHT_RE.match(val):
                    height = val
                elif not weight and WEIGHT_RE.match(val):
                    weight = val
                elif not bats_throws and BT_RE.match(val):
                    bats_throws = val
                elif not class_year and val.lower() in CLASS_WORDS:
                    class_year = val
                elif not position:
                    position = val

        # Some schools embed B/T in position string "Outfielder (L/L)"
        if not bats_throws and position:
            bt_match = re.search(r'\(([LRS]/[LRS])\)', position, re.IGNORECASE)
            if bt_match:
                bats_throws = bt_match.group(1).upper()
                position = re.sub(r'\s*\([LRS]/[LRS]\)', '', position, flags=re.IGNORECASE).strip()

        players.append({
            'first_name': first_name,
            'last_name': last_name,
            'team': team_name,
            'school': school_name,
            'conference': conference,
            'state': state,
            'number': jersey,
            'position': position,
            'bats_throws': bats_throws,
            'height': height,
            'weight': weight,
            'class_year': class_year,
            'hometown': hometown,
            'high_school': high_school,
            'profile_url': profile_url,
            'photo_url': photo_url,
        })

    return players


def parse_script_json_roster(soup, team_name, conference, school_name, state):
    """
    Extract roster data from JSON embedded in <script> tags.

    Handles two Sidearm Sports patterns:
      1. JSON-LD (<script type="application/ld+json">): ListItem of Person objects.
         Usually contains only names + profile URLs.
      2. Inline JS with Sidearm player objects containing displayName, jerseyNum,
         pos, bats, throws, height, weight, academicYear, hometown, prevSchool.
    """
    def _make_record(first, last, p=None):
        p = p or {}
        bt = p.get('batsThrows', '')
        if not bt and p.get('bats') and p.get('throws'):
            bt = f"{p['bats']}/{p['throws']}"
        return {
            'first_name': first,
            'last_name': last,
            'team': team_name,
            'school': school_name,
            'conference': conference,
            'state': state,
            'number': p.get('jerseyNum', ''),
            'position': p.get('pos', ''),
            'bats_throws': bt,
            'height': p.get('height', ''),
            'weight': str(p.get('weight', '')),
            'class_year': p.get('academicYear', ''),
            'hometown': p.get('hometown', ''),
            'high_school': p.get('prevSchool', ''),
        }

    # --- Strategy 1: JSON-LD scripts ---
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '')
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue
        items = data if isinstance(data, list) else [data]
        players = []
        for item in items:
            if item.get('@type') == 'ListItem' and isinstance(item.get('item'), list):
                for person in item['item']:
                    if not isinstance(person, dict) or person.get('@type') != 'Person':
                        continue
                    name = person.get('name', '').strip()
                    if name:
                        first, last = parse_name(name)
                        players.append(_make_record(first, last))
        if players:
            return players

    # --- Strategy 2: Sidearm inline JS with displayName field ---
    for script in soup.find_all('script'):
        text = script.string or ''
        if 'displayName' not in text:
            continue
        # Sidearm embeds player arrays like: [{"displayName":"...", "jerseyNum":"...", ...}]
        # Use a greedy approach: find the outermost [...] blocks containing displayName
        for match in re.finditer(r'\[\s*\{[^\[\]]*"displayName"[^\[\]]*\}\s*(?:,\s*\{[^\[\]]*\}\s*)*\]', text, re.DOTALL):
            try:
                arr = json.loads(match.group(0))
            except (json.JSONDecodeError, ValueError):
                continue
            if not isinstance(arr, list):
                continue
            players = []
            for p in arr:
                if not isinstance(p, dict):
                    continue
                name = p.get('displayName', '').strip()
                if not name:
                    continue
                first, last = parse_name(name)
                players.append(_make_record(first, last, p))
            if players:
                return players

    return []


def scrape_team_roster(base_url, team_name, conference, school_name, state):
    """Attempt to scrape a team's baseball roster. Returns list of player dicts."""
    url = f"{base_url}/sports/baseball/roster"

    r = None
    try:
        r = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True)
    except requests.exceptions.TooManyRedirects:
        # Cloudflare / bot-protection redirect loop — try cloudscraper
        try:
            scraper = cloudscraper.create_scraper()
            r = scraper.get(url, timeout=30)
        except Exception as e:
            return None, f"Redirect loop (cloudscraper failed: {e})"
    except requests.exceptions.SSLError:
        # First try: bypass SSL verification (handles cert-not-yet-valid errors)
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            r = requests.get(url, headers=HEADERS, timeout=20, allow_redirects=True, verify=False)
        except Exception:
            # Second try: fall back to plain HTTP
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

    if r is None or r.status_code == 404:
        # Try old Sidearm /sports/bsb/YEAR/roster URL pattern before giving up
        import datetime
        year = datetime.date.today().year
        alt_urls = [
            f"{base_url}/sports/bsb/{year}-{str(year+1)[-2:]}/roster",
            f"{base_url}/sports/bsb/{year-1}-{str(year)[-2:]}/roster",
            f"{base_url}/sports/mbaseball/roster",
            f"{base_url}/sports/m-baseball/roster",
        ]
        r = None
        for alt_url in alt_urls:
            try:
                r_alt = requests.get(alt_url, headers=HEADERS, timeout=20, allow_redirects=True)
                if r_alt.status_code == 200 and 'baseball' in r_alt.text[:8000].lower():
                    r = r_alt
                    url = alt_url
                    break
            except Exception:
                continue
        if r is None:
            return None, "404 - no baseball program or different URL"
    elif r.status_code == 403:
        return None, "403 - access denied"
    elif r.status_code != 200:
        return None, f"HTTP {r.status_code}"

    # Check if we landed on a valid baseball roster page
    if 'baseball' not in r.url.lower() and 'baseball' not in r.text[:5000].lower():
        return None, "Redirected away from baseball page"

    soup = BeautifulSoup(r.text, 'html.parser')

    # Parser chain — order matters:
    # 1. NUXT3 card layout (<li class="roster-list-item">) — must run before table parser
    #    because some NUXT pages also contain a small/empty table that triggers false positives.
    # 2. SIDEARM HTML table (modern + <th scope="row"> variant)
    # 3. Legacy SIDEARM table (<th class="name">)
    # 4. Inline JS with displayName fields
    # 5. WMT Digital NUXT flat-array format
    # 6. Sidearm next-gen JSON API (network call, last resort)
    players = parse_roster_list_items_rich(soup, team_name, conference, school_name, state)
    if not players:
        players = parse_sidearm_roster(soup, team_name, conference, school_name, state)
    if not players:
        players = parse_old_sidearm_table(soup, team_name, conference, school_name, state)
    if not players:
        players = parse_script_json_roster(soup, team_name, conference, school_name, state)
    if not players:
        players = parse_wmt_digital_roster(soup, team_name, conference, school_name, state)
    if not players:
        players = parse_sidearm_nextgen_api(base_url, team_name, conference, school_name, state)

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
