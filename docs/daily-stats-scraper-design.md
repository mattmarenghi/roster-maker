# Daily Stats Scraper — Scalability Design

## The Question

Can we run `scrape_school_stats.py` for all ~300 D1 baseball teams on a nightly schedule, producing one JSON file per team? What does that look like in terms of resources, cost, and infrastructure?

## Current Per-School Workload

The stats scraper makes **6–7 HTTP requests** per school:

| Request | Target |
|---------|--------|
| 1 | Season batting stats page |
| 2 | Season pitching stats page |
| 3 | Schedule page (find most recent game) |
| 4 | Most recent game page (find box score + recap links) |
| 5 | Box score page |
| 6 | Recap/news article page |
| 7 | (occasional) Alternate game page or fallback |

With the current 0.75s delay between requests, that's **~5 seconds of scrape time per school**.

## Scaling to 300 Teams

### Request Volume

| Metric | Value |
|--------|-------|
| Schools | ~300 |
| Requests per school | ~7 |
| **Total requests per run** | **~2,100** |
| Delay between requests (sequential) | 0.75s |
| **Sequential wall time** | **~26 minutes** |
| Data per school JSON | ~30–40 KB |
| **Total data written per run** | **~10–12 MB** |

**This is small.** 2,100 requests once per day is nothing — most websites serve that many requests in seconds. For context, the original roster scrape hit similar numbers and ran fine.

### With Parallelism (Recommended)

Running requests sequentially against 300 *different* school domains means you're rarely hitting the same server twice in a row. You can safely parallelize across schools:

| Parallelism | Wall Time | Notes |
|-------------|-----------|-------|
| Sequential (1 worker) | ~26 min | Simple, safe, plenty fast |
| 4 workers | ~7 min | Sweet spot — polite to servers, fast enough |
| 10 workers | ~3 min | Still fine since each worker hits different domains |
| 30 workers | ~1 min | Aggressive but viable since requests scatter across 300 different hosts |

**Recommendation: 4–10 workers using `asyncio` + `aiohttp` or `concurrent.futures.ThreadPoolExecutor`.** This keeps you well under any rate limit concern since you'd only hit each school's domain ~7 times total per day with natural spacing.

### Resource Usage

| Resource | Impact |
|----------|--------|
| CPU | Negligible — HTML parsing is lightweight |
| Memory | < 200 MB — parsing 300 small HTML pages |
| Network | ~50–100 MB download total (HTML pages) |
| Disk | ~10–12 MB of JSON output |
| Bandwidth cost | Effectively zero |

**Bottom line: this is a very light workload.** A $5/month VPS could handle it.

## Cost Breakdown by Hosting Option

### Option A: GitHub Actions (Free Tier) — Recommended to Start

| | |
|---|---|
| **Cost** | **$0** (2,000 free minutes/month, this uses ~30 min/day = ~15 hrs/month) |
| **Setup** | Add a `.github/workflows/daily-stats.yml` with a cron schedule |
| **Pros** | Zero cost, no server to maintain, built-in cron, results commit directly to repo |
| **Cons** | 6-hour max job time (not an issue here), no persistent state between runs |

```yaml
# .github/workflows/daily-stats.yml
name: Daily Stats Scrape
on:
  schedule:
    - cron: '0 6 * * *'  # 6 AM UTC daily (during baseball season)
  workflow_dispatch: {}   # manual trigger

jobs:
  scrape:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install requests beautifulsoup4 cloudscraper
      - run: python3 scrape_all_stats.py --workers 4
      - name: Commit updated stats
        run: |
          git config user.name "Stats Bot"
          git config user.email "stats-bot@users.noreply.github.com"
          git add stats/
          git diff --staged --quiet || git commit -m "Daily stats update $(date -u +%Y-%m-%d)"
          git push
```

### Option B: Cheap VPS (DigitalOcean / Hetzner / Railway)

| | |
|---|---|
| **Cost** | **$4–6/month** |
| **Setup** | Cron job on a small VM |
| **Pros** | Full control, can run on any schedule, persistent disk |
| **Cons** | Server to maintain, need to handle uptime |

### Option C: AWS Lambda + EventBridge

| | |
|---|---|
| **Cost** | **~$0.01–0.05/month** (Lambda free tier covers this easily) |
| **Setup** | Lambda function triggered by EventBridge cron rule |
| **Pros** | True serverless, auto-scaling, near-zero cost |
| **Cons** | 15-min timeout per invocation (would need to batch schools across invocations or use Step Functions), more complex deployment |

### Option D: Cloud Run Jobs (GCP) or Azure Container Apps Jobs

| | |
|---|---|
| **Cost** | **~$0** (generous free tiers for scheduled jobs) |
| **Setup** | Container with cron trigger |
| **Pros** | No timeout issues, runs in a container so easy to replicate locally |
| **Cons** | Slightly more setup than GitHub Actions |

## AI Narrative Cost (If Enabled)

If you use Claude API for per-player narratives:

| Metric | Value |
|--------|-------|
| Players with meaningful game appearances per school | ~15–20 |
| Total players narrated per run | ~5,000–6,000 |
| Tokens per narrative (input + output) | ~500 |
| Total tokens per run | ~2.5–3M |
| **Claude Haiku cost per run** | **~$0.75–1.00** |
| **Monthly cost (30 days)** | **~$22–30** |

**To keep costs near zero:** Use the template-based narrative fallback (already built in) instead of the Claude API. The template narratives are decent and cost nothing.

## Recommended Architecture

```
┌─────────────────────────────────────────────────┐
│  GitHub Actions  (cron: daily at 6 AM UTC)      │
│                                                 │
│  1. Checkout repo                               │
│  2. pip install dependencies                    │
│  3. python3 scrape_all_stats.py --workers 4     │
│     ├── Fetches NCAA school list                │
│     ├── For each school (4 parallel workers):   │
│     │   ├── Scrape season batting stats         │
│     │   ├── Scrape season pitching stats        │
│     │   ├── Scrape most recent game box score   │
│     │   ├── Scrape recap article                │
│     │   └── Write stats/{school-slug}.json      │
│     └── Write stats/index.json                  │
│  4. git add stats/ && git commit && git push    │
└─────────────────────────────────────────────────┘
```

### What Needs to Be Built

1. **`scrape_all_stats.py`** — Orchestrator script that:
   - Loads the school list from `ncaa_d1_schools.json` or the NCAA API
   - Maps each school to its SIDEARM base URL
   - Runs `scrape_school_stats.py` logic per school with a thread pool
   - Handles failures gracefully (log and skip, don't abort the whole run)
   - Writes a summary report (how many succeeded, failed, skipped)

2. **Generalize `scrape_school_stats.py`** — Currently hardcoded for Davidson via `KNOWN_SCHOOLS`. Needs to:
   - Accept any school's base URL + slug dynamically
   - Handle the variety of SIDEARM page formats (already partially there from the roster scraper's experience)
   - Fail gracefully when a school uses a non-SIDEARM platform

3. **`stats/index.json`** — Already exists, just needs to be updated with all schools after each run.

### File Structure After Full Run

```
stats/
├── index.json                    # ["abilene-christian", "air-force", ...]
├── abilene-christian.json        # ~35 KB
├── air-force.json
├── alabama.json
├── ...
├── davidson-college.json
├── ...
└── yale.json                     # ~300 files total, ~10-12 MB
```

## Key Design Decisions

### One JSON file per school vs. one big file

**One file per school is the right call.** Reasons:
- Each file is ~35 KB — small, fast to load on demand
- The UI can lazy-load only the school the user clicks on
- Git diffs are readable (you see which schools changed)
- Parallel writes don't conflict
- A single `stats/all.json` at 10 MB would be too large for the UI to load upfront

### How to handle non-SIDEARM schools

~10–15% of D1 schools don't use SIDEARM Sports. Options:
- **Skip them** for now and note them in a `stats/unsupported.json` list
- **Add ESPN API fallback** for basic stats (the retry scraper already does this for rosters)
- These can be filled in incrementally over time

### Seasonal scheduling

Only run during baseball season (Feb–June, plus fall ball Aug–Nov). Add a simple date check or just disable the cron in the off-season.

## Summary

| Question | Answer |
|----------|--------|
| Is 300 teams daily a lot of load? | **No.** ~2,100 requests/day is trivial. |
| Is it expensive? | **No.** $0 on GitHub Actions free tier. |
| Where should it run? | **GitHub Actions** to start. Move to a VPS only if you outgrow it. |
| One file per team? | **Yes.** Right approach for lazy-loading and git diffs. |
| What about non-SIDEARM schools? | Skip initially, backfill with ESPN API later. |
| What about AI narratives? | Use template fallback to keep cost at $0. Add Claude API optionally (~$25/month). |
| What needs to be built? | An orchestrator (`scrape_all_stats.py`) + generalize the existing scraper. |
