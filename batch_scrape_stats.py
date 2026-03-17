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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def phase_scrape(max_schools=None, refresh=False, workers=4):
    """Scrape stats for all compatible schools.

    When refresh=True, all compatible schools are scraped regardless of index.json.
    Use this for daily runs. Without refresh, already-scraped schools are skipped
    (useful for resuming an interrupted initial scrape).
    workers controls how many schools are scraped in parallel.
    """
    if not os.path.exists(PROBE_RESULTS):
        print("Run --probe first!", file=sys.stderr)
        sys.exit(1)

    with open(PROBE_RESULTS) as f:
        probes = json.load(f)

    # In refresh mode, ignore index.json so every school gets re-scraped.
    # In normal mode, skip schools already in index.json (resume behavior).
    if not refresh and os.path.exists(os.path.join(STATS_DIR, "index.json")):
        with open(os.path.join(STATS_DIR, "index.json")) as f:
            already_done = set(json.load(f))
    else:
        already_done = set()

    compatible = [p for p in probes if p.get("compatible")]
    to_scrape = compatible if refresh else [p for p in compatible if p["slug"] not in already_done]

    if max_schools:
        to_scrape = to_scrape[:max_schools]

    print(f"Compatible schools: {len(compatible)}")
    print(f"Already scraped: {len(already_done)}")
    print(f"To scrape: {len(to_scrape)}")
    print(f"Workers: {workers}")

    successes = []
    failures = []
    index_lock = threading.Lock()
    counter = {"n": 0}

    def scrape_one_school(school):
        slug = school["slug"]
        base_url = school["base_url"]
        school_name = school["school"]
        no_season = school.get("no_season_url", False)

        with index_lock:
            counter["n"] += 1
            i = counter["n"]
        print(f"\n{'='*60}")
        print(f"[{i}/{len(to_scrape)}] Scraping: {school_name}")
        print(f"  URL: {base_url}")
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
            subprocess.run(cmd, capture_output=True, text=True, timeout=180)

            out_path = os.path.join(STATS_DIR, f"{slug}.json")
            if os.path.exists(out_path):
                with open(out_path) as f:
                    data = json.load(f)
                player_count = len(data.get("players", {}))
                record = data.get("record", "?")

                if player_count > 0:
                    print(f"  [{slug}] SUCCESS: {player_count} players, record {record}")
                    return ("success", {"school": school_name, "slug": slug,
                                        "players": player_count, "record": record})
                else:
                    print(f"  [{slug}] FAILED: 0 players matched")
                    return ("failure", {"school": school_name, "slug": slug,
                                        "reason": "0 players matched", "record": record})
            else:
                print(f"  [{slug}] FAILED: no output file")
                return ("failure", {"school": school_name, "slug": slug,
                                    "reason": "no output file"})

        except subprocess.TimeoutExpired:
            print(f"  [{slug}] FAILED: timeout")
            return ("failure", {"school": school_name, "slug": slug, "reason": "timeout"})
        except Exception as e:
            print(f"  [{slug}] FAILED: {e}")
            return ("failure", {"school": school_name, "slug": slug, "reason": str(e)})

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scrape_one_school, school): school for school in to_scrape}
        for future in as_completed(futures):
            status, result = future.result()
            if status == "success":
                successes.append(result)
                with index_lock:
                    already_done.add(result["slug"])
                    index_path = os.path.join(STATS_DIR, "index.json")
                    with open(index_path, "w") as f:
                        json.dump(sorted(already_done), f, indent=2)
            else:
                failures.append(result)

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

    # Generate coverage report so gaps are visible on GitHub
    try:
        from generate_coverage_report import generate as gen_report
        print("\nGenerating coverage report …")
        gen_report(verbose=False)
        print("Coverage report written to COVERAGE_REPORT.md")
    except Exception as e:
        print(f"(Coverage report skipped: {e})")


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
    parser.add_argument("--scrape", action="store_true", help="Phase 2: scrape compatible schools (skips already-done)")
    parser.add_argument("--refresh", action="store_true", help="Phase 2: scrape ALL compatible schools, ignoring index.json (use for daily runs)")
    parser.add_argument("--scrape-one", type=str, help="Scrape a single school by slug")
    parser.add_argument("--max", type=int, help="Max schools to scrape in one run")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers for scraping (default: 4)")
    args = parser.parse_args()

    if args.probe:
        phase_probe()
    elif args.refresh:
        phase_scrape(max_schools=args.max, refresh=True, workers=args.workers)
    elif args.scrape:
        phase_scrape(max_schools=args.max, workers=args.workers)
    elif args.scrape_one:
        scrape_one(args.scrape_one)
    else:
        parser.print_help()
