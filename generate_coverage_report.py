#!/usr/bin/env python3
"""
Roster Coverage Report Generator
==================================
Produces ROSTER_COVERAGE.md summarising the current state of player roster
data: field completeness, thin rosters, schools with gaps, and schools that
are still missing entirely.

Run standalone after any roster scrape:
    python3 generate_coverage_report.py

Also called automatically at the end of:
    scrape_d1_baseball.py, scrape_failed_schools.py, generate_ids.py

Stats coverage is a separate report — see generate_stats_coverage.py.
"""

import csv
import json
import os
import statistics
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
PLAYERS_JSON  = os.path.join(ROOT, "players.json")
MISSING_CSV   = os.path.join(ROOT, "missing_schools.csv")
OUTPUT_REPORT = os.path.join(ROOT, "ROSTER_COVERAGE.md")

THIN_ROSTER_THRESHOLD = 25
FIELD_WARN_THRESHOLD  = 0.50
KEY_FIELDS = ["pos", "bt", "yr", "ht", "wt", "hometown"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def pct(n, total):
    return 0.0 if total == 0 else 100.0 * n / total


def field_present(player, field):
    v = player.get(field)
    if v is None:
        return False
    s = str(v).strip()
    return bool(s) and s not in ("0", "N/A", "-")


def load_players():
    with open(PLAYERS_JSON) as f:
        return json.load(f)


def load_missing_schools():
    rows = []
    if not os.path.exists(MISSING_CSV):
        return rows
    with open(MISSING_CSV, newline="") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyze_rosters(players):
    by_school = {}
    for p in players:
        s = p.get("school", "")
        if s not in by_school:
            by_school[s] = {"count": 0, "field_missing": {f: 0 for f in KEY_FIELDS}}
        by_school[s]["count"] += 1
        for f in KEY_FIELDS:
            if not field_present(p, f):
                by_school[s]["field_missing"][f] += 1

    roster_sizes = [d["count"] for d in by_school.values()]

    thin_rosters = sorted(
        [(s, d["count"]) for s, d in by_school.items()
         if d["count"] < THIN_ROSTER_THRESHOLD],
        key=lambda x: x[1]
    )

    total = len(players)
    global_field_pct = {
        f: pct(sum(1 for p in players if field_present(p, f)), total)
        for f in KEY_FIELDS
    }

    stable   = sum(1 for p in players if p.get("id_stable"))
    unstable = total - stable

    schools_with_gaps = {}
    for s, d in by_school.items():
        gaps = {
            f: pct(d["field_missing"][f], d["count"])
            for f in KEY_FIELDS
            if pct(d["field_missing"][f], d["count"]) > FIELD_WARN_THRESHOLD * 100
        }
        if gaps:
            schools_with_gaps[s] = {"count": d["count"], "gaps": gaps}

    return {
        "total_players":    total,
        "total_schools":    len(by_school),
        "stable_ids":       stable,
        "unstable_ids":     unstable,
        "roster_sizes":     roster_sizes,
        "thin_rosters":     thin_rosters,
        "global_field_pct": global_field_pct,
        "schools_with_gaps": schools_with_gaps,
    }


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def render_report(roster, missing_schools):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    sizes = roster["roster_sizes"]
    lines = []
    w = lines.append

    w("# D1 Baseball Roster Coverage Report")
    w("")
    w(f"_Generated: {now}_")
    w("")

    # ── Summary ───────────────────────────────────────────────────────────────
    w("## Summary")
    w("")
    w("| Metric | Value |")
    w("|--------|-------|")
    w(f"| Total players | {roster['total_players']:,} |")
    w(f"| Schools with rosters | {roster['total_schools']} |")
    w(f"| Schools confirmed missing | {len(missing_schools)} |")
    w(f"| Median roster size | {statistics.median(sizes):.0f} players |")
    w(f"| Smallest roster | {min(sizes)} players |")
    w(f"| Largest roster | {max(sizes)} players |")
    w(f"| Stable player IDs | {roster['stable_ids']:,} ({pct(roster['stable_ids'], roster['total_players']):.1f}%) |")
    w(f"| Unstable player IDs | {roster['unstable_ids']:,} ({pct(roster['unstable_ids'], roster['total_players']):.1f}%) |")
    w("")

    # ── Field Completeness ────────────────────────────────────────────────────
    w(f"## Roster Field Completeness (all {roster['total_players']:,} players)")
    w("")
    w("| Field | % Complete | Note |")
    w("|-------|-----------|------|")
    field_notes = {
        "pos":      "Position",
        "bt":       "Bats/Throws — key for scouting and ID stability",
        "yr":       "Class year",
        "ht":       "Height",
        "wt":       "Weight",
        "hometown": "Hometown — used for stable player ID",
    }
    for f in KEY_FIELDS:
        pct_val = roster["global_field_pct"][f]
        flag = " ⚠️" if pct_val < 80 else ""
        w(f"| `{f}` | {pct_val:.1f}%{flag} | {field_notes.get(f, '')} |")
    w("")

    # ── Thin Rosters ──────────────────────────────────────────────────────────
    w(f"## Thin Rosters (< {THIN_ROSTER_THRESHOLD} players)")
    w("")
    if roster["thin_rosters"]:
        w("| School | Players |")
        w("|--------|---------|")
        for school, count in roster["thin_rosters"]:
            w(f"| {school} | {count} |")
    else:
        w(f"_All schools have {THIN_ROSTER_THRESHOLD}+ players._")
    w("")

    # ── Schools with Field Gaps ───────────────────────────────────────────────
    w("## Schools with Significant Field Gaps (>50% missing a key field)")
    w("")
    gaps = roster["schools_with_gaps"]
    if gaps:
        bt_only    = {s: d for s, d in gaps.items() if list(d["gaps"].keys()) == ["bt"]}
        other_gaps = {s: d for s, d in gaps.items() if s not in bt_only}

        if other_gaps:
            w("### Multiple field gaps")
            w("")
            w("| School | Players | Missing fields |")
            w("|--------|---------|----------------|")
            for s, d in sorted(other_gaps.items(), key=lambda x: -x[1]["count"]):
                gap_str = ", ".join(
                    f"`{f}` ({v:.0f}%)" for f, v in sorted(d["gaps"].items())
                )
                w(f"| {s} | {d['count']} | {gap_str} |")
            w("")

        w(f"### Missing `bt` (Bats/Throws) only — {len(bt_only)} schools")
        w("")
        w(f"These schools publish rosters without bats/throws data. Affects "
          f"~{sum(d['count'] for d in bt_only.values()):,} players and makes "
          f"their IDs unstable if they transfer.")
        w("")
        w("| School | Players |")
        w("|--------|---------|")
        for s, d in sorted(bt_only.items(), key=lambda x: -x[1]["count"]):
            w(f"| {s} | {d['count']} |")
    else:
        w("_No schools with major field gaps._")
    w("")

    # ── Missing Rosters ───────────────────────────────────────────────────────
    w(f"## Schools with No Roster Scraped ({len(missing_schools)})")
    w("")
    if missing_schools:
        w("| School | Conference | Status | Notes |")
        w("|--------|------------|--------|-------|")
        for row in sorted(missing_schools, key=lambda r: r.get("school", "")):
            school = row.get("school", "")
            conf   = row.get("conference", "")
            status = row.get("status", "")
            notes  = row.get("notes", "").replace("|", "\\|")
            w(f"| {school} | {conf} | {status} | {notes} |")
    else:
        w("_No confirmed missing schools._")
    w("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def generate(verbose=True):
    if verbose:
        print("Loading players.json …", end=" ", flush=True)
    players = load_players()
    if verbose:
        print(f"{len(players):,} players")

    if verbose:
        print("Analyzing rosters …", end=" ", flush=True)
    roster = analyze_rosters(players)
    if verbose:
        print("done")

    missing = load_missing_schools()

    if verbose:
        print("Writing ROSTER_COVERAGE.md …", end=" ", flush=True)
    report = render_report(roster, missing)
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)
    if verbose:
        print("done")
        print(f"\n✓ ROSTER_COVERAGE.md written")
        print(f"  {roster['total_players']:,} players / {roster['total_schools']} schools")
        print(f"  {roster['stable_ids']:,} stable IDs "
              f"({pct(roster['stable_ids'], roster['total_players']):.1f}%)")
        print(f"  {len(roster['thin_rosters'])} thin rosters, "
              f"{len(roster['schools_with_gaps'])} schools with field gaps")


if __name__ == "__main__":
    generate()
