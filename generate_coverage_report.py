#!/usr/bin/env python3
"""
Coverage Report Generator
=========================
Produces COVERAGE_REPORT.md summarizing the current state of roster and stats
data: what's complete, what's thin, and exactly where the gaps are.

Run standalone after any scrape:
    python3 generate_coverage_report.py

Also called automatically at the end of scrape_d1_baseball.py,
scrape_failed_schools.py, and batch_scrape_stats.py.
"""

import csv
import json
import os
import statistics
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
PLAYERS_JSON    = os.path.join(ROOT, "players.json")
MISSING_CSV     = os.path.join(ROOT, "missing_schools.csv")
STATS_DIR       = os.path.join(ROOT, "stats")
PROBE_RESULTS   = os.path.join(STATS_DIR, "probe_results.json")
STATS_INDEX     = os.path.join(STATS_DIR, "index.json")
OUTPUT_REPORT   = os.path.join(ROOT, "COVERAGE_REPORT.md")

# Thresholds
THIN_ROSTER_THRESHOLD = 25      # players
FIELD_WARN_THRESHOLD  = 0.50    # <50% populated = flag school
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
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def load_probe_results():
    if not os.path.exists(PROBE_RESULTS):
        return []
    with open(PROBE_RESULTS) as f:
        return json.load(f)


def load_stats_index():
    if not os.path.exists(STATS_INDEX):
        return []
    with open(STATS_INDEX) as f:
        return json.load(f)


def load_stats_file(slug):
    path = os.path.join(STATS_DIR, f"{slug}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyze_rosters(players):
    """Return roster-level analysis dict."""
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
        [(s, d["count"]) for s, d in by_school.items() if d["count"] < THIN_ROSTER_THRESHOLD],
        key=lambda x: x[1]
    )

    # Per-field global completeness
    total = len(players)
    global_field_pct = {}
    for f in KEY_FIELDS:
        filled = sum(1 for p in players if field_present(p, f))
        global_field_pct[f] = pct(filled, total)

    # Schools with poor field coverage (any KEY_FIELD >50% missing)
    schools_with_gaps = {}
    for s, d in by_school.items():
        gaps = {}
        for f in KEY_FIELDS:
            miss_pct = pct(d["field_missing"][f], d["count"])
            if miss_pct > FIELD_WARN_THRESHOLD * 100:
                gaps[f] = miss_pct
        if gaps:
            schools_with_gaps[s] = {"count": d["count"], "gaps": gaps}

    return {
        "total_players": total,
        "total_schools": len(by_school),
        "roster_sizes": roster_sizes,
        "thin_rosters": thin_rosters,
        "global_field_pct": global_field_pct,
        "schools_with_gaps": schools_with_gaps,
        "by_school": by_school,
    }


def analyze_stats(players, probe_results, stats_index):
    """Return stats-level analysis dict."""
    # Map school name -> slug from probe results
    school_to_slug = {r["school"]: r["slug"] for r in probe_results}
    compatible_schools = {r["school"] for r in probe_results if r.get("compatible")}
    incompatible_schools = {r["school"]: r.get("error") for r in probe_results if not r.get("compatible")}

    scraped_slugs = set(stats_index)

    # Schools in players.json
    roster_schools = {p["school"] for p in players}

    stats_detail = {}
    for slug in scraped_slugs:
        data = load_stats_file(slug)
        if data is None:
            continue
        school = data.get("school", slug)
        ps = data.get("players", {})
        has_batting  = any("season_batting"  in p for p in ps.values())
        has_pitching = any("season_pitching" in p for p in ps.values())
        has_record   = bool(data.get("record"))
        has_game     = bool(data.get("most_recent_game"))
        has_recap    = bool(
            data.get("most_recent_game", {}) and
            data["most_recent_game"].get("recap_text")
        )
        stats_detail[school] = {
            "slug": slug,
            "player_count": len(ps),
            "has_batting": has_batting,
            "has_pitching": has_pitching,
            "has_record": has_record,
            "has_game": has_game,
            "has_recap": has_recap,
        }

    # Compatible but not yet scraped
    compatible_not_scraped = []
    for school in compatible_schools:
        slug = school_to_slug.get(school, "")
        if slug not in scraped_slugs:
            compatible_not_scraped.append(school)
    compatible_not_scraped.sort()

    # Scraped but missing batting or pitching
    missing_batting  = [s for s, d in stats_detail.items() if not d["has_batting"]]
    missing_pitching = [s for s, d in stats_detail.items() if not d["has_pitching"]]
    missing_recap    = [s for s, d in stats_detail.items() if not d["has_recap"]]
    missing_record   = [s for s, d in stats_detail.items() if not d["has_record"]]

    return {
        "total_probed": len(probe_results),
        "total_compatible": len(compatible_schools),
        "total_incompatible": len(incompatible_schools),
        "total_scraped": len(scraped_slugs),
        "compatible_not_scraped": compatible_not_scraped,
        "incompatible_schools": dict(sorted(incompatible_schools.items())),
        "stats_detail": stats_detail,
        "missing_batting": sorted(missing_batting),
        "missing_pitching": sorted(missing_pitching),
        "missing_recap": sorted(missing_recap),
        "missing_record": sorted(missing_record),
    }


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def render_report(roster, stats, missing_schools):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    sizes = roster["roster_sizes"]
    lines = []
    w = lines.append

    w(f"# D1 Baseball Coverage Report")
    w(f"")
    w(f"_Generated: {now}_")
    w(f"")

    # ── Summary ──────────────────────────────────────────────────────────────
    w(f"## Summary")
    w(f"")
    w(f"| Metric | Value |")
    w(f"|--------|-------|")
    w(f"| Total players | {roster['total_players']:,} |")
    w(f"| Schools with rosters | {roster['total_schools']} |")
    w(f"| Schools confirmed missing | {len(missing_schools)} |")
    w(f"| Median roster size | {statistics.median(sizes):.0f} players |")
    w(f"| Smallest roster | {min(sizes)} players |")
    w(f"| Largest roster | {max(sizes)} players |")
    w(f"| Schools probed for stats | {stats['total_probed']} |")
    w(f"| Stats-compatible schools | {stats['total_compatible']} |")
    w(f"| Schools with stats scraped | {stats['total_scraped']} |")
    w(f"")

    # ── Roster Field Completeness ─────────────────────────────────────────────
    w(f"## Roster Field Completeness (all {roster['total_players']:,} players)")
    w(f"")
    w(f"| Field | % Complete | Note |")
    w(f"|-------|-----------|------|")
    field_notes = {
        "pos":      "Position",
        "bt":       "Bats/Throws — key for scouting; many schools don't publish",
        "yr":       "Class year",
        "ht":       "Height",
        "wt":       "Weight",
        "hometown": "Hometown — used for stable player ID",
    }
    for f in KEY_FIELDS:
        pct_val = roster["global_field_pct"][f]
        flag = " ⚠️" if pct_val < 80 else ""
        w(f"| `{f}` | {pct_val:.1f}%{flag} | {field_notes.get(f, '')} |")
    w(f"")

    # ── Thin Rosters ──────────────────────────────────────────────────────────
    w(f"## Thin Rosters (< {THIN_ROSTER_THRESHOLD} players)")
    w(f"")
    if roster["thin_rosters"]:
        w(f"| School | Players |")
        w(f"|--------|---------|")
        for school, count in roster["thin_rosters"]:
            w(f"| {school} | {count} |")
    else:
        w(f"_All schools have {THIN_ROSTER_THRESHOLD}+ players._")
    w(f"")

    # ── Schools with Field Gaps ───────────────────────────────────────────────
    w(f"## Schools with Significant Field Gaps (>50% missing a key field)")
    w(f"")
    gaps = roster["schools_with_gaps"]
    if gaps:
        # Group by which fields are missing (most common: bt)
        bt_only = {s: d for s, d in gaps.items() if list(d["gaps"].keys()) == ["bt"]}
        other_gaps = {s: d for s, d in gaps.items() if s not in bt_only}

        if other_gaps:
            w(f"### Multiple field gaps")
            w(f"")
            w(f"| School | Players | Missing fields |")
            w(f"|--------|---------|----------------|")
            for s, d in sorted(other_gaps.items(), key=lambda x: -x[1]["count"]):
                gap_str = ", ".join(f"`{f}` ({v:.0f}%)" for f, v in sorted(d["gaps"].items()))
                w(f"| {s} | {d['count']} | {gap_str} |")
            w(f"")

        w(f"### Missing `bt` (Bats/Throws) only — {len(bt_only)} schools")
        w(f"")
        w(f"These schools publish rosters without bats/throws data. This affects ~"
          f"{sum(d['count'] for d in bt_only.values()):,} players and makes "
          f"their IDs unstable if they transfer.")
        w(f"")
        w(f"| School | Players |")
        w(f"|--------|---------|")
        for s, d in sorted(bt_only.items(), key=lambda x: -x[1]["count"]):
            w(f"| {s} | {d['count']} |")
    else:
        w(f"_No schools with major field gaps._")
    w(f"")

    # ── Stats Coverage ────────────────────────────────────────────────────────
    w(f"## Stats Coverage")
    w(f"")
    w(f"| Metric | Count |")
    w(f"|--------|-------|")
    w(f"| Compatible schools (Sidearm) | {stats['total_compatible']} |")
    w(f"| Schools with stats scraped | {stats['total_scraped']} |")
    w(f"| Compatible but not yet scraped | {len(stats['compatible_not_scraped'])} |")
    w(f"| Incompatible (no Sidearm stats) | {stats['total_incompatible']} |")
    w(f"| Scraped — missing batting stats | {len(stats['missing_batting'])} |")
    w(f"| Scraped — missing pitching stats | {len(stats['missing_pitching'])} |")
    w(f"| Scraped — missing game recap | {len(stats['missing_recap'])} |")
    w(f"| Scraped — missing season record | {len(stats['missing_record'])} |")
    w(f"")

    if stats["compatible_not_scraped"]:
        w(f"### Compatible but not yet scraped ({len(stats['compatible_not_scraped'])})")
        w(f"")
        for s in stats["compatible_not_scraped"]:
            w(f"- {s}")
        w(f"")

    if stats["missing_batting"]:
        w(f"### Scraped but missing batting stats ({len(stats['missing_batting'])})")
        w(f"")
        for s in stats["missing_batting"]:
            w(f"- {s}")
        w(f"")

    if stats["missing_recap"]:
        w(f"### Scraped but missing game recaps ({len(stats['missing_recap'])})")
        w(f"")
        for s in stats["missing_recap"]:
            w(f"- {s}")
        w(f"")

    # ── Incompatible Schools ──────────────────────────────────────────────────
    w(f"## Schools Without Sidearm Stats ({stats['total_incompatible']})")
    w(f"")
    w(f"These schools don't use the Sidearm platform (or had errors during probing). "
      f"Stats must be sourced from an alternative platform.")
    w(f"")
    w(f"| School | Error |")
    w(f"|--------|-------|")
    for school, err in sorted(stats["incompatible_schools"].items()):
        err_str = str(err or "—").replace("|", "\\|")[:80]
        w(f"| {school} | {err_str} |")
    w(f"")

    # ── Missing Rosters ───────────────────────────────────────────────────────
    w(f"## Schools with No Roster Scraped ({len(missing_schools)})")
    w(f"")
    if missing_schools:
        w(f"| School | Conference | Status | Notes |")
        w(f"|--------|------------|--------|-------|")
        for row in sorted(missing_schools, key=lambda r: r.get("school", "")):
            school = row.get("school", "")
            conf   = row.get("conference", "")
            status = row.get("status", "")
            notes  = row.get("notes", "").replace("|", "\\|")
            w(f"| {school} | {conf} | {status} | {notes} |")
    else:
        w(f"_No confirmed missing schools._")
    w(f"")

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

    if verbose:
        print("Loading stats data …", end=" ", flush=True)
    probe   = load_probe_results()
    index   = load_stats_index()
    stats   = analyze_stats(players, probe, index)
    if verbose:
        print(f"{stats['total_scraped']} schools")

    missing = load_missing_schools()

    if verbose:
        print("Writing COVERAGE_REPORT.md …", end=" ", flush=True)
    report = render_report(roster, stats, missing)
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)
    if verbose:
        print("done")
        print(f"\n✓ Report written to {OUTPUT_REPORT}")
        print(f"  {roster['total_players']:,} players / {roster['total_schools']} schools")
        print(f"  {len(roster['thin_rosters'])} thin rosters, "
              f"{len(roster['schools_with_gaps'])} schools with field gaps")
        print(f"  {stats['total_scraped']} stats files, "
              f"{len(stats['compatible_not_scraped'])} compatible but not scraped")


if __name__ == "__main__":
    generate()
