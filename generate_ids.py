#!/usr/bin/env python3
"""
Generate stable player IDs and update players.json.

ID strategy:
  - Hash of (name + hometown + bt) using SHA-256, first 8 hex chars
  - Stable across transfers, year changes, roster updates
  - Fallback for players with missing data: also include school (id_stable=False)
"""

import json
import hashlib
from collections import Counter

def player_hash(fields):
    raw = "|".join(fields)
    return hashlib.sha256(raw.encode()).hexdigest()[:8]

def generate_id(p, use_school_fallback=False):
    fields = [p['name'], p['hometown'], p['bt']]
    if use_school_fallback:
        fields.append(p['school'])
    return player_hash(fields)

with open('players.json') as f:
    data = json.load(f)

# First pass: assign primary IDs
primary_key = lambda p: (p['name'], p['hometown'], p['bt'])
key_counts = Counter(primary_key(p) for p in data)

# Identify which players need fallback (missing data or collision)
def needs_fallback(p):
    if not p['hometown'] or not p['bt']:
        return True
    if key_counts[primary_key(p)] > 1:
        return True
    return False

# Assign IDs
id_counts = Counter()
for p in data:
    fallback = needs_fallback(p)
    pid = generate_id(p, use_school_fallback=fallback)
    p['id'] = pid
    p['id_stable'] = not fallback
    id_counts[pid] += 1

# Check for any remaining collisions (shouldn't happen, but verify)
dupes = {pid: cnt for pid, cnt in id_counts.items() if cnt > 1}
if dupes:
    print(f"WARNING: {len(dupes)} hash collisions remain after fallback — investigate!")
    for pid, cnt in list(dupes.items())[:5]:
        players = [p for p in data if p['id'] == pid]
        for pl in players:
            print(f"  {pl['name']} | {pl['school']} | {pl['hometown']}")
else:
    print("All IDs unique.")

stable = sum(1 for p in data if p['id_stable'])
unstable = len(data) - stable
print(f"Stable IDs: {stable} / {len(data)}")
print(f"Unstable (school-dependent): {unstable} / {len(data)}")

with open('players.json', 'w') as f:
    json.dump(data, f, indent=2)

print("players.json updated.")

# Regenerate coverage report now that players.json is fresh
try:
    from generate_coverage_report import generate as gen_report
    print("\nGenerating coverage report …")
    gen_report(verbose=False)
    print("Coverage report written to COVERAGE_REPORT.md")
except Exception as e:
    print(f"(Coverage report skipped: {e})")
