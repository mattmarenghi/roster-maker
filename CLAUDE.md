# NCAA D1 Baseball Roster Database — Project Context

## What This Project Is
A scraped database of all NCAA Division 1 college baseball player rosters for the 2026 season, with a search UI. The goal is to allow scouts/fans to search and bookmark players across all D1 programs.

## Current State
- **11,139 players** across **290 schools** and **32 conferences**
- Primary dataset: `players.json`
- Search UI: `index.html` (client-side, loads players.json directly)
- Raw CSVs archived as gzipped files

## File Overview
| File | Purpose |
|---|---|
| `players.json` | Main player database (source of truth) |
| `index.html` | Client-side search/browse UI |
| `scrape_d1_baseball.py` | Primary scraper — hits SIDEARM Sports roster pages |
| `scrape_failed_schools.py` | Retry scraper for schools that failed in primary run |
| `generate_ids.py` | Regenerates stable player IDs (run after re-scraping) |
| `ncaa_d1_schools.json` | List of all D1 schools with roster URLs |
| `missing_rosters.csv` | Schools that have baseball but couldn't be scraped |
| `scrape_progress.json` | Scraper checkpoint file |
| `d1_baseball_rosters_2026.csv.gz` | Raw CSV from primary scrape |
| `d1_baseball_rosters_2026_retry.csv.gz` | Raw CSV from retry scrape |

## Player Record Schema
```json
{
  "id": "6f32872b",
  "fn": "JT",
  "ln": "Thompson",
  "name": "JT Thompson",
  "num": 0,
  "pos": "INF",
  "bt": "L/R",
  "yr": "Sr.",
  "ht": "5' 8''",
  "wt": "150",
  "school": "Abilene Christian University",
  "conf": "Western Athletic Conference",
  "st": "TX",
  "hometown": "Abilene, Texas",
  "hs": "Rutgers",
  "id_stable": true
}
```

| Field | Description |
|---|---|
| `id` | 8-char SHA-256 hex hash — stable unique player ID |
| `fn` / `ln` | First / last name |
| `name` | Full name (fn + ln joined) |
| `num` | Jersey number |
| `pos` | Position(s) |
| `bt` | Bats/Throws (e.g. "R/R", "L/R", "S/R") |
| `yr` | Class year (Fr., So., Jr., Sr., Gr.) |
| `ht` | Height |
| `wt` | Weight |
| `school` | Current school (full name) |
| `conf` | Conference (full name) |
| `st` | State of school (2-letter abbreviation) |
| `hometown` | Player's hometown (city, state) |
| `hs` | Previous school — high school for freshmen, transfer-from college for transfers |
| `id_stable` | true if ID will survive transfers/re-scrapes; false if school is baked into hash |

## Stable Player ID Design
IDs are designed to persist across seasons, transfers, and re-scrapes so bookmarks don't break.

**Stable ID** (`id_stable: true` — 6,787 players):
- Hash of `name + hometown + bt`
- None of these change over a player's career
- Will remain the same if player transfers, changes jersey number, etc.

**Unstable ID** (`id_stable: false` — 4,352 players):
- Hash of `name + hometown + bt + school`
- Used when hometown or bt is missing from source data, or when name+hometown+bt collides
- May break if a player transfers (school changes)
- This is a data quality limitation from the source — can't do better without more data

To regenerate IDs after a re-scrape:
```bash
python3 generate_ids.py
```

## Bookmarking Players
Use the `id` field as the bookmark key. For `id_stable: true` players, the ID is safe to store long-term. For `id_stable: false` players, note that a transfer could change their ID in future datasets.

## Re-scraping
1. Run `scrape_d1_baseball.py` to scrape all schools
2. Run `scrape_failed_schools.py` for any that failed
3. Rebuild `players.json` from the CSVs
4. Run `generate_ids.py` to assign stable IDs
5. Commit everything

## Git Branch
Active branch: `claude/retry-scraper-1f26c9d4`
