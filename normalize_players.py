#!/usr/bin/env python3
"""Normalize pos and yr fields in players.json at the data layer."""

import json
import re

# ── Position normalization ───────────────────────────────────────────────────

FUSED = {
    'cinf': 'C/INF',
    'cif':  'C/IF',
}

PART_MAP = {
    # Right-handed pitcher
    'rhp': 'RHP', 'right handed pitcher': 'RHP', 'right-handed pitcher': 'RHP',
    'right hand pitcher': 'RHP', 'right-hand pitcher': 'RHP',
    'right handed pitching': 'RHP', 'right handed pitchers': 'RHP',
    'right-handed pitching': 'RHP',
    # Left-handed pitcher
    'lhp': 'LHP', 'left handed pitcher': 'LHP', 'left-handed pitcher': 'LHP',
    'left hand pitcher': 'LHP', 'left-hand pitcher': 'LHP',
    'left handed pitching': 'LHP', 'left-handed pitching': 'LHP',
    # Generic pitcher
    'pitcher': 'P', 'p': 'P', 'sp': 'SP', 'rp': 'RP',
    # Catcher
    'catcher': 'C', 'c': 'C',
    # Outfield
    'outfielder': 'OF', 'outfield': 'OF', 'of': 'OF',
    'center field': 'CF', 'centerfield': 'CF', 'cf': 'CF',
    'left field': 'LF', 'leftfield': 'LF', 'lf': 'LF',
    'right field': 'RF', 'rightfield': 'RF', 'rf': 'RF',
    # Infield generic
    'infielder': 'INF', 'infield': 'INF', 'inf': 'INF', 'inf.': 'INF',
    'indielder': 'INF',  # typo
    'if': 'IF',
    'middle infielder': 'MIF', 'mif': 'MIF',
    'corner infielder': 'INF',
    # Specific infield
    'first base': '1B', 'first baseman': '1B', '1b': '1B', 'fb': '1B',
    'second base': '2B', 'second baseman': '2B', '2b': '2B',
    'third base': '3B', 'third baseman': '3B', '3b': '3B',
    'shortstop': 'SS', 'ss': 'SS',
    # Utility
    'utility': 'UTL', 'utl': 'UTL', 'util': 'UTL', 'ut': 'UTL',
    'uti': 'UTL', 'ult': 'UTL', 'ult.': 'UTL',
    # DH
    'designated hitter': 'DH', 'dh': 'DH',
}

def normalize_pos(pos):
    if not pos:
        return pos
    # Strip scraper prefix
    s = re.sub(r'^Pos\.:\s*', '', pos, flags=re.IGNORECASE).strip()
    # Fused abbreviations
    fused = FUSED.get(s.lower())
    if fused:
        return fused
    # Split on "/" with optional surrounding spaces
    parts = re.split(r'\s*/\s*', s)
    normalized = []
    for raw in parts:
        key = raw.strip().lower()
        normalized.append(PART_MAP.get(key, raw.strip()))
    return '/'.join(normalized)


# ── Year normalization ───────────────────────────────────────────────────────

YR_MAP = {
    # Freshman
    'fr.': 'Fr.', 'freshman': 'Fr.', 'fy.': 'Fr.', 'fy': 'Fr.', '*fr.': 'Fr.',
    'r-fr.': 'Fr.', 'redshirt freshman': 'Fr.', 'rs-freshman': 'Fr.', 'rs fr.': 'Fr.',
    'cl.:fr.': 'Fr.', 'cl.:r-fr.': 'Fr.', 'rf.': 'Fr.',
    # Sophomore
    'so.': 'So.', 'sophomore': 'So.', 'r-so.': 'So.', 'redshirt sophomore': 'So.',
    'rs so.': 'So.', 'rs-so.': 'So.', 'rs-sophomore': 'So.', 'cl.:so.': 'So.',
    'cl.:so': 'So.', 'cl.:r-so.': 'So.', 'cl.:rs so.': 'So.', 'cl.:rs. so.': 'So.',
    # Junior
    'jr.': 'Jr.', 'junior': 'Jr.', '*jr.': 'Jr.', 'r-jr.': 'Jr.', 'redshirt junior': 'Jr.',
    'rs jr.': 'Jr.', 'rs-junior': 'Jr.', 'rs-jr.': 'Jr.', 'cl.:jr.': 'Jr.',
    'cl.:jr': 'Jr.', 'cl.:r-jr.': 'Jr.',
    # Senior
    'sr.': 'Sr.', 'senior': 'Sr.', '*sr.': 'Sr.', 'r-sr.': 'Sr.', 'redshirt senior': 'Sr.',
    'rs sr.': 'Sr.', 'rs-sr.': 'Sr.', 'cl.:sr.': 'Sr.', 'cl.:sr': 'Sr.', 'cl.:rs sr.': 'Sr.',
    # Graduate / 5th+ year
    'gr.': 'Gr.', 'graduate student': 'Gr.', 'graduate': 'Gr.', '5th': 'Gr.', '6th': 'Gr.',
    '5th-year senior': 'Gr.', 'fifth year': 'Gr.', 'fifth-year senior': 'Gr.',
    'red 5th': 'Gr.', '**sr. (graduate)': 'Gr.', '*jr. (graduate)': 'Gr.',
    'sr. (graduate)': 'Gr.',
}

def normalize_yr(yr):
    if not yr:
        return yr
    return YR_MAP.get(yr.strip().lower(), yr)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    with open('players.json', 'r') as f:
        players = json.load(f)

    pos_changed = 0
    yr_changed = 0

    for p in players:
        old_pos = p.get('pos', '')
        new_pos = normalize_pos(old_pos)
        if new_pos != old_pos:
            p['pos'] = new_pos
            pos_changed += 1

        old_yr = p.get('yr', '')
        new_yr = normalize_yr(old_yr)
        if new_yr != old_yr:
            p['yr'] = new_yr
            yr_changed += 1

    with open('players.json', 'w') as f:
        json.dump(players, f, separators=(',', ':'))

    print(f'pos normalized: {pos_changed} players')
    print(f'yr  normalized: {yr_changed} players')

if __name__ == '__main__':
    main()
