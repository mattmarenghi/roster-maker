#!/usr/bin/env python3
"""
Stats Coverage Report Generator
=================================
Produces STATS_COVERAGE.md summarising the current state of per-school stats
data: which schools have been scraped, what's missing, and which schools are
incompatible with the Sidearm stats platform.

Run standalone after any stats scrape:
    python3 generate_stats_coverage.py

Also called automatically at the end of batch_scrape_stats.py.

Roster coverage is a separate report — see generate_coverage_report.py.
"""

import json
import os
from datetime import datetime, timezone

ROOT          = os.path.dirname(os.path.abspath(__file__))
STATS_DIR     = os.path.join(ROOT, "stats")
PROBE_RESULTS = os.path.join(STATS_DIR, "probe_results.json")
STATS_INDEX   = os.path.join(STATS_DIR, "index.json")
OUTPUT_REPORT = os.path.join(ROOT, "STATS_COVERAGE.md")


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

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

def analyze_stats(probe_results, stats_index):
    school_to_slug   = {r["school"]: r["slug"] for r in probe_results}
    compatible       = {r["school"] for r in probe_results if r.get("compatible")}
    incompatible     = {r["school"]: r.get("error") for r in probe_results
                        if not r.get("compatible")}
    scraped_slugs    = set(stats_index)

    stats_detail = {}
    for slug in scraped_slugs:
        data = load_stats_file(slug)
        if data is None:
            continue
        school  = data.get("school", slug)
        players = data.get("players", {})
        ps_vals = list(players.values()) if isinstance(players, dict) else (
            players if isinstance(players, list) else []
        )
        stats_detail[school] = {
            "slug":         slug,
            "player_count": len(players),
            "has_batting":  any("season_batting"  in p for p in ps_vals),
            "has_pitching": any("season_pitching" in p for p in ps_vals),
            "has_record":   bool(data.get("record")),
            "has_game":     bool(data.get("most_recent_game")),
            "has_recap":    bool(
                data.get("most_recent_game") and
                data["most_recent_game"].get("recap_text")
            ),
        }

    compatible_not_scraped = sorted(
        s for s in compatible
        if school_to_slug.get(s, "") not in scraped_slugs
    )

    return {
        "total_probed":             len(probe_results),
        "total_compatible":         len(compatible),
        "total_incompatible":       len(incompatible),
        "total_scraped":            len(scraped_slugs),
        "compatible_not_scraped":   compatible_not_scraped,
        "incompatible_schools":     dict(sorted(incompatible.items())),
        "stats_detail":             stats_detail,
        "missing_batting":  sorted(s for s, d in stats_detail.items() if not d["has_batting"]),
        "missing_pitching": sorted(s for s, d in stats_detail.items() if not d["has_pitching"]),
        "missing_recap":    sorted(s for s, d in stats_detail.items() if not d["has_recap"]),
        "missing_record":   sorted(s for s, d in stats_detail.items() if not d["has_record"]),
    }


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------

def render_report(stats):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = []
    w = lines.append

    w("# D1 Baseball Stats Coverage Report")
    w("")
    w(f"_Generated: {now}_")
    w("")

    # ── Summary ───────────────────────────────────────────────────────────────
    w("## Summary")
    w("")
    w("| Metric | Count |")
    w("|--------|-------|")
    w(f"| Schools probed for stats | {stats['total_probed']} |")
    w(f"| Sidearm-compatible schools | {stats['total_compatible']} |")
    w(f"| Schools with stats scraped | {stats['total_scraped']} |")
    w(f"| Compatible but not yet scraped | {len(stats['compatible_not_scraped'])} |")
    w(f"| Incompatible (no Sidearm stats) | {stats['total_incompatible']} |")
    w(f"| Scraped — missing batting stats | {len(stats['missing_batting'])} |")
    w(f"| Scraped — missing pitching stats | {len(stats['missing_pitching'])} |")
    w(f"| Scraped — missing game recap | {len(stats['missing_recap'])} |")
    w(f"| Scraped — missing season record | {len(stats['missing_record'])} |")
    w("")

    # ── Compatible but not yet scraped ────────────────────────────────────────
    if stats["compatible_not_scraped"]:
        w(f"## Compatible but Not Yet Scraped ({len(stats['compatible_not_scraped'])})")
        w("")
        for s in stats["compatible_not_scraped"]:
            w(f"- {s}")
        w("")

    # ── Missing recaps ────────────────────────────────────────────────────────
    if stats["missing_recap"]:
        w(f"## Scraped but Missing Game Recaps ({len(stats['missing_recap'])})")
        w("")
        for s in stats["missing_recap"]:
            w(f"- {s}")
        w("")

    # ── Incompatible schools ──────────────────────────────────────────────────
    w(f"## Schools Without Sidearm Stats ({stats['total_incompatible']})")
    w("")
    w("These schools don't use the Sidearm platform (or had errors during "
      "probing). Stats must be sourced from an alternative platform.")
    w("")

    alt_covered = {s: e for s, e in stats["incompatible_schools"].items()
                   if s in stats["stats_detail"]}
    not_covered = {s: e for s, e in stats["incompatible_schools"].items()
                   if s not in stats["stats_detail"]}

    if alt_covered:
        w(f"### Covered via alternative source ({len(alt_covered)})")
        w("")
        w("| School | Record | Most Recent Game |")
        w("|--------|--------|-----------------|")
        for school in sorted(alt_covered):
            sd   = stats["stats_detail"].get(school, {})
            slug = sd.get("slug", "")
            record = mrg = ""
            if slug:
                data = load_stats_file(slug)
                if data:
                    record = data.get("record", "")
                    g = data.get("most_recent_game")
                    if g:
                        mrg = (f"{g.get('date','')} vs {g.get('opponent','')} "
                               f"{g.get('result','')}")
            w(f"| {school} | {record} | {mrg} |")
        w("")

    if not_covered:
        w(f"### Not yet covered ({len(not_covered)})")
        w("")
        w("| School | Error |")
        w("|--------|-------|")
        for school, err in sorted(not_covered.items()):
            err_str = str(err or "—").replace("|", "\\|")[:80]
            w(f"| {school} | {err_str} |")
        w("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def generate(verbose=True):
    if verbose:
        print("Loading stats data …", end=" ", flush=True)
    probe = load_probe_results()
    index = load_stats_index()
    if verbose:
        print(f"{len(index)} schools scraped")

    if verbose:
        print("Analyzing stats coverage …", end=" ", flush=True)
    stats = analyze_stats(probe, index)
    if verbose:
        print("done")

    if verbose:
        print("Writing STATS_COVERAGE.md …", end=" ", flush=True)
    report = render_report(stats)
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)
    if verbose:
        print("done")
        print(f"\n✓ STATS_COVERAGE.md written")
        print(f"  {stats['total_scraped']} schools scraped / "
              f"{stats['total_compatible']} compatible")
        print(f"  {len(stats['compatible_not_scraped'])} compatible but not scraped")
        print(f"  {stats['total_incompatible']} incompatible schools")


if __name__ == "__main__":
    generate()
