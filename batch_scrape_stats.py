#!/usr/bin/env python3
"""
Batch Stats Scraper
====================
Discovers which schools have Sidearm-compatible stats pages,
then scrapes stats for all compatible schools.

Usage:
    python3 batch_scrape_stats.py --probe       # Phase 1: discover compatible schools
    python3 batch_scrape_stats.py --scrape      # Phase 2: scrape all compatible schools
    python3 batch_scrape_stats.py --scrape-one SLUG  # Scrape a single school by slug
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from urllib.parse import urlparse

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
}

PLAYERS_JSON = os.path.join(os.path.dirname(__file__), "players.json")
STATS_DIR = os.path.join(os.path.dirname(__file__), "stats")
PROBE_RESULTS = os.path.join(os.path.dirname(__file__), "stats", "probe_results.json")

# Extra base URLs for schools not in the CSV
EXTRA_URLS = {
    "Arizona State University": "https://thesundevils.com",
    "Auburn University": "https://auburntigers.com",
    "Boston University": "https://goterriers.com",
    "Bradley University": "https://bradleybraves.com",
    "Brigham Young University": "https://byucougars.com",
    "Canisius University": "https://gogriffs.com",
    "Florida Gulf Coast University": "https://fgcuathletics.com",
    "Furman University": "https://furmanpaladins.com",
    "Gardner-Webb University": "https://gwusports.com",
    "George Mason University": "https://gomason.com",
    "George Washington University": "https://gwsports.com",
    "Georgia Institute of Technology": "https://ramblinwreck.com",
    "Georgia State University": "https://georgiastatesports.com",
    "Gonzaga University": "https://gozags.com",
    "Grambling State University": "https://gsutigers.com",
    "Iona University": "https://ionagaels.com",
    "Jacksonville State University": "https://jsugamecocksports.com",
    "Kennesaw State University": "https://ksuowls.com",
    "Longwood University": "https://longwoodlancers.com",
    "Loyola University Chicago": "https://loyolaramblers.com",
    "New Mexico State University": "https://nmstatesports.com",
    "North Dakota State University": "https://gobison.com",
    "Northeastern University": "https://nuhuskies.com",
    "Northern Kentucky University": "https://nkunorse.com",
    "Oakland University": "https://goldengrizzlies.com",
    "Old Dominion University": "https://odusports.com",
    "Oral Roberts University": "https://oruathletics.com",
    "Pennsylvania State University": "https://gopsusports.com",
    "Purdue University": "https://purduesports.com",
    "San Diego State University": "https://goaztecs.com",
    "San Jose State University": "https://sjsuspartans.com",
    "Seattle University": "https://goseattleu.com",
    "Stanford University": "https://gostanford.com",
    "Stetson University": "https://gohatters.com",
    "U.S. Military Academy": "https://goarmywestpoint.com",
    "University at Buffalo, the State University of New York": "https://ubbulls.com",
    "University of Arizona": "https://arizonawildcats.com",
    "University of California, Irvine": "https://ucirvinesports.com",
    "University of Central Florida": "https://ucfknights.com",
    "University of Cincinnati": "https://gobearcats.com",
    "University of Illinois Chicago": "https://uicflames.com",
    "University of Illinois Urbana-Champaign": "https://fightingillini.com",
    "University of Iowa": "https://hawkeyesports.com",
    "University of Nebraska at Omaha": "https://omavs.com",
    "University of Nebraska-Lincoln": "https://huskers.com",
    "University of New Mexico": "https://golobos.com",
    "University of North Carolina Wilmington": "https://uncwsports.com",
    "University of North Dakota": "https://undsports.com",
    "University of San Diego": "https://usdtoreros.com",
    "University of San Francisco": "https://usfdons.com",
    "University of Texas at San Antonio": "https://goutsa.com",
    "University of Virginia": "https://virginiasports.com",
    "University of Washington": "https://gohuskies.com",
    "Virginia Commonwealth University": "https://vcuathletics.com",
    "Virginia Polytechnic Institute and State University": "https://hokiesports.com",
    "Western Michigan University": "https://wmubroncos.com",
    "Wichita State University": "https://goshockers.com",
    "Wofford College": "https://woffordterriers.com",
    "Yale University": "https://yalebulldogs.com",
}


def slugify(school_name):
    """Convert school name to URL-friendly slug."""
    s = school_name.lower()
    s = re.sub(r'[^a-z0-9\s-]', '', s)
    s = re.sub(r'[\s]+', '-', s).strip('-')
    s = re.sub(r'-+', '-', s)
    return s


def get_school_urls():
    """Build school -> base_url mapping from CSV + extras."""
    school_urls = {}

    # From CSV profile_url
    csv_path = os.path.join(os.path.dirname(__file__), "d1_baseball_rosters_2026.csv")
    if os.path.exists(csv_path):
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                school = row['school']
                url = row.get('profile_url', '')
                if url and school not in school_urls:
                    parsed = urlparse(url)
                    base = f'{parsed.scheme}://{parsed.netloc}'
                    school_urls[school] = base

    # Add extras
    school_urls.update(EXTRA_URLS)

    return school_urls


def probe_school(school_name, base_url, session):
    """Check if a school has Sidearm-compatible stats. Returns dict with results."""
    result = {
        "school": school_name,
        "base_url": base_url,
        "slug": slugify(school_name),
        "compatible": False,
        "has_nuxt": False,
        "has_html_tables": False,
        "stats_url": None,
        "no_season_url": False,
        "error": None,
    }

    # Try with season year first
    for url_variant, no_season in [
        (f"{base_url}/sports/baseball/stats/2026", False),
        (f"{base_url}/sports/baseball/stats", True),
    ]:
        try:
            resp = session.get(url_variant, timeout=12, allow_redirects=True)
            if resp.status_code != 200:
                continue

            html = resp.text
            has_nuxt = "__NUXT" in html or "window.__NUXT" in html
            # Check for stats tables (batting table with AVG column)
            has_tables = bool(re.search(r'<th[^>]*>\s*AVG\s*</th>', html, re.IGNORECASE))
            # Also check for game log links
            has_gamelog = "/sports/baseball/stats/2026/" in html or "/sports/baseball/stats/" in html

            if has_nuxt or has_tables:
                result["compatible"] = True
                result["has_nuxt"] = has_nuxt
                result["has_html_tables"] = has_tables
                result["stats_url"] = url_variant
                result["no_season_url"] = no_season
                return result
        except Exception as e:
            result["error"] = str(e)

    return result


def phase_probe():
    """Probe all schools for Sidearm compatibility."""
    school_urls = get_school_urls()

    with open(PLAYERS_JSON) as f:
        players = json.load(f)
    all_schools = sorted(set(p['school'] for p in players))

    # Load existing results to resume
    existing = {}
    if os.path.exists(PROBE_RESULTS):
        with open(PROBE_RESULTS) as f:
            for entry in json.load(f):
                existing[entry["school"]] = entry

    session = requests.Session()
    session.headers.update(HEADERS)

    results = []
    no_url = []
    compatible_count = 0
    incompatible_count = 0

    for i, school in enumerate(all_schools):
        # Skip if already probed
        if school in existing:
            results.append(existing[school])
            if existing[school]["compatible"]:
                compatible_count += 1
            continue

        base_url = school_urls.get(school)
        if not base_url:
            no_url.append(school)
            results.append({
                "school": school,
                "base_url": None,
                "slug": slugify(school),
                "compatible": False,
                "error": "no_url",
            })
            continue

        print(f"[{i+1}/{len(all_schools)}] Probing {school} ({base_url})...", end=" ", flush=True)
        r = probe_school(school, base_url, session)
        results.append(r)

        if r["compatible"]:
            compatible_count += 1
            print(f"OK (NUXT={r['has_nuxt']}, tables={r['has_html_tables']})")
        else:
            incompatible_count += 1
            print(f"SKIP ({r.get('error', 'no stats page')})")

        time.sleep(0.3)

        # Save progress every 20 schools
        if (i + 1) % 20 == 0:
            with open(PROBE_RESULTS, "w") as f:
                json.dump(results, f, indent=2)

    # Final save
    with open(PROBE_RESULTS, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Total schools: {len(all_schools)}")
    print(f"Compatible: {compatible_count}")
    print(f"Incompatible: {incompatible_count}")
    print(f"No URL: {len(no_url)}")
    print(f"Results saved to: {PROBE_RESULTS}")
    print(f"{'='*60}")


def phase_scrape(max_schools=None):
    """Scrape stats for all compatible schools."""
    if not os.path.exists(PROBE_RESULTS):
        print("Run --probe first!", file=sys.stderr)
        sys.exit(1)

    with open(PROBE_RESULTS) as f:
        probes = json.load(f)

    # Already scraped
    if os.path.exists(os.path.join(STATS_DIR, "index.json")):
        with open(os.path.join(STATS_DIR, "index.json")) as f:
            already_done = set(json.load(f))
    else:
        already_done = set()

    compatible = [p for p in probes if p.get("compatible")]
    to_scrape = [p for p in compatible if p["slug"] not in already_done]

    if max_schools:
        to_scrape = to_scrape[:max_schools]

    print(f"Compatible schools: {len(compatible)}")
    print(f"Already scraped: {len(already_done)}")
    print(f"To scrape: {len(to_scrape)}")

    successes = []
    failures = []

    for i, school in enumerate(to_scrape):
        slug = school["slug"]
        base_url = school["base_url"]
        school_name = school["school"]
        no_season = school.get("no_season_url", False)

        print(f"\n{'='*60}")
        print(f"[{i+1}/{len(to_scrape)}] Scraping: {school_name}")
        print(f"  URL: {base_url}")
        print(f"  Slug: {slug}")
        print(f"{'='*60}")

        cmd = [
            sys.executable, "scrape_school_stats.py",
            "--school", "custom",
            "--base-url", base_url,
            "--slug", slug,
            "--school-name", school_name,
            "--max-games-back", "3",
        ]
        if no_season:
            cmd.append("--no-season-url")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=180,
            )

            # Check output
            output = result.stdout + result.stderr
            out_path = os.path.join(STATS_DIR, f"{slug}.json")

            if os.path.exists(out_path):
                with open(out_path) as f:
                    data = json.load(f)
                player_count = len(data.get("players", {}))
                record = data.get("record", "?")

                if player_count > 0:
                    successes.append({
                        "school": school_name,
                        "slug": slug,
                        "players": player_count,
                        "record": record,
                    })
                    already_done.add(slug)
                    print(f"  SUCCESS: {player_count} players, record {record}")
                else:
                    failures.append({
                        "school": school_name,
                        "slug": slug,
                        "reason": "0 players matched",
                        "record": record,
                    })
                    print(f"  FAILED: 0 players matched (record {record})")
            else:
                failures.append({
                    "school": school_name,
                    "slug": slug,
                    "reason": "no output file",
                })
                print(f"  FAILED: no output file")

        except subprocess.TimeoutExpired:
            failures.append({
                "school": school_name,
                "slug": slug,
                "reason": "timeout",
            })
            print(f"  FAILED: timeout")
        except Exception as e:
            failures.append({
                "school": school_name,
                "slug": slug,
                "reason": str(e),
            })
            print(f"  FAILED: {e}")

        # Update index.json after each success
        index_path = os.path.join(STATS_DIR, "index.json")
        with open(index_path, "w") as f:
            json.dump(sorted(already_done), f, indent=2)

    # Summary
    print(f"\n{'='*60}")
    print(f"BATCH SCRAPE COMPLETE")
    print(f"  Successes: {len(successes)}")
    print(f"  Failures: {len(failures)}")
    print(f"  Total in index: {len(already_done)}")
    print(f"{'='*60}")

    if failures:
        print(f"\nFailed schools:")
        for f_item in failures:
            print(f"  {f_item['school']}: {f_item['reason']}")

    # Save failure report
    report_path = os.path.join(STATS_DIR, "scrape_report.json")
    with open(report_path, "w") as f:
        json.dump({
            "successes": successes,
            "failures": failures,
            "total_in_index": len(already_done),
        }, f, indent=2)
    print(f"\nReport saved to: {report_path}")


def scrape_one(slug):
    """Scrape a single school by slug."""
    if not os.path.exists(PROBE_RESULTS):
        print("Run --probe first!", file=sys.stderr)
        sys.exit(1)

    with open(PROBE_RESULTS) as f:
        probes = json.load(f)

    school = None
    for p in probes:
        if p.get("slug") == slug:
            school = p
            break

    if not school:
        print(f"School with slug '{slug}' not found in probe results", file=sys.stderr)
        sys.exit(1)

    if not school.get("compatible"):
        print(f"School '{school['school']}' is not Sidearm-compatible", file=sys.stderr)
        sys.exit(1)

    cmd = [
        sys.executable, "scrape_school_stats.py",
        "--school", "custom",
        "--base-url", school["base_url"],
        "--slug", slug,
        "--max-games-back", "3",
    ]
    os.execvp(sys.executable, cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe", action="store_true", help="Phase 1: probe all schools")
    parser.add_argument("--scrape", action="store_true", help="Phase 2: scrape compatible schools")
    parser.add_argument("--scrape-one", type=str, help="Scrape a single school by slug")
    parser.add_argument("--max", type=int, help="Max schools to scrape in one run")
    args = parser.parse_args()

    if args.probe:
        phase_probe()
    elif args.scrape:
        phase_scrape(max_schools=args.max)
    elif args.scrape_one:
        scrape_one(args.scrape_one)
    else:
        parser.print_help()
