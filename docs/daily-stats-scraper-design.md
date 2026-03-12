# Daily Stats Scraper — Scalability Design

## The Question

Can we run `scrape_school_stats.py` for all ~300 D1 baseball teams on a nightly schedule, producing one JSON file per team? What does that look like in terms of resources, cost, and infrastructure? And should we use an API like Highlightly instead of scraping?

---

## Option 1: Highlightly API (NCAA Baseball)

[Highlightly](https://highlightly.net/documentation/baseball/) offers a structured REST API covering both MLB and NCAA D1 baseball via RapidAPI.

### What It Provides

| Endpoint | Data | Refresh Rate |
|----------|------|-------------|
| `GET /matches` | Game scores, states, inning-by-inning lines | Every minute |
| `GET /matches/{id}` | Full game detail: venue, weather, plays, rosters, predictions | Every minute |
| `GET /matches/{id}/box-score` | Player-by-player batting + pitching game stats | Post-game |
| `GET /statistics/{id}` | Detailed match statistics (K, H, E, etc.) | Post-game |
| `GET /teams/statistics/{id}` | Team season stats (W/L, runs scored/allowed, home/away splits) | After each game |
| `GET /players/{id}/statistics` | Career + seasonal player stats | After each game |
| `GET /players/{id}/summary` | Player profile (position, number, etc.) | Daily |
| `GET /standings` | Conference/division standings | Within 1 hour |
| `GET /lineups/{matchId}` | Starting lineups + rosters | Pre-game / live |
| `GET /highlights` | Video clips with metadata + embed URLs | Every minute |
| `GET /odds` | Pre-match + live odds from 50+ sportsbooks | Every 10 min |
| `GET /head-2-head` | Last 10 meetings between two teams | On demand |
| `GET /last-five-games` | Five most recent completed games for a team | On demand |

### What the Scraper Cannot Do That Highlightly Can

- **Play-by-play data** — pitch type, velocity, count, result per at-bat
- **Live in-game scores** — updated every minute during games
- **Video highlights** — embedded clips from YouTube, ESPN, etc.
- **Betting odds** — pre-match and live from 50+ bookmakers
- **Weather forecasts** — for upcoming games
- **Umpire assignments** — per game
- **Head-to-head history** — last 10 meetings between any two teams
- **Predictions** — pre-match win probability
- **Standings** — structured conference/division standings
- **Consistent schema** — clean JSON, no HTML parsing fragility

### What the Scraper Does That Highlightly Might Not

- **Game recap articles** — full text narratives from school news
- **AI-generated player narratives** — custom storytelling per player
- **Previous school / high school** — transfer history from roster pages
- **Hometown data** — for the roster database (already in players.json)
- **Guaranteed coverage of every D1 school** — Highlightly's NCAA coverage breadth is unclear; SIDEARM scraping hits schools directly

### Pricing

Exact pricing is not publicly listed — it's gated behind the [RapidAPI pricing page](https://rapidapi.com/highlightly-api-highlightly-api-default/api/mlb-college-baseball-api/pricing) and the Highlightly dashboard. What we know:

- **Free tier** exists with limited requests (RapidAPI standard is ~500 req/month)
- **Paid tiers** unlock odds, geo-restriction data, and higher request quotas
- **Up to 40% discount** for annual plans
- Positioned as a budget-friendly API for startups
- Custom plans available directly through Highlightly (not on RapidAPI)

**Daily request estimate if using Highlightly for 300 teams:**

| Data needed | Requests | Frequency |
|-------------|----------|-----------|
| Team stats (300 teams) | 300 | Daily |
| Player stats (~30 players × 300 teams) | 9,000 | Daily |
| Most recent match per team | 300 | Daily |
| Box scores for recent matches | ~150 | Daily |
| Standings | ~32 | Daily |
| **Total** | **~10,000/day = ~300,000/month** |

This is significantly more API calls than the scraper approach (2,100/day) because the API returns data per-player rather than per-team-page. A free tier of 500 requests/month wouldn't come close. You'd need a paid plan, and at 300K requests/month, the cost depends entirely on their tier pricing.

---

## Scraper vs. API — Head-to-Head Comparison

| Factor | Scraper | Highlightly API |
|--------|---------|-----------------|
| **Monthly cost** | $0 (GitHub Actions) | Unknown paid tier — likely $20–100+/month for 300K requests |
| **Data freshness** | Once daily (batch) | Near real-time (every minute) |
| **Data richness** | Season stats + box score + recap | All of that + play-by-play, odds, highlights, predictions, weather |
| **Schema stability** | Fragile — HTML layouts change, parsers break | Stable — structured JSON contract with versioning |
| **Coverage** | Every SIDEARM school (~85-90% of D1) | Unclear — may not cover all 300 D1 schools |
| **Maintenance burden** | High — 5 parser strategies, Cloudflare bypasses, format changes | Low — just API calls |
| **Setup effort** | Already built (needs orchestrator) | Need to rewrite data layer to use API |
| **Vendor dependency** | None — you own the scraping code | Full — if Highlightly goes down/changes pricing, you're stuck |
| **Recap narratives** | Yes (scraped from school news) | No |
| **AI narratives** | Yes (Claude API or templates) | No |
| **Offline/self-hosted** | Yes | No |

---

## Recommendation

**Use a hybrid approach: Highlightly API as the primary data source, scraper as the fallback.**

### Why

1. **The scraper is brittle at scale.** It works for Davidson because we hand-tuned it. Running it across 300 schools with 5 different SIDEARM page formats, Cloudflare blocks, and schools that don't use SIDEARM at all means constant maintenance. The roster scraper already proved this — it needed a retry scraper, a targeted missing-schools scraper, and multiple fallback strategies.

2. **The API gives you data you can't scrape.** Play-by-play, live scores, video highlights, odds, and predictions are genuinely useful features for a scouting app. You can't get pitch velocity and type from a box score HTML page.

3. **But the scraper fills gaps the API can't.** Game recap articles, AI narratives, transfer history, and guaranteed school coverage are things only the scraper provides. Keep it as a supplement.

### Proposed Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Daily Job (GitHub Actions or VPS cron)                  │
│                                                          │
│  Phase 1: Highlightly API (primary)                      │
│  ├── Fetch team stats for all 300 teams                  │
│  ├── Fetch player season stats                           │
│  ├── Fetch most recent game box scores                   │
│  ├── Fetch standings                                     │
│  └── Write stats/{school-slug}.json                      │
│                                                          │
│  Phase 2: Scraper (supplement)                           │
│  ├── For each school with a game that day:               │
│  │   ├── Scrape recap article from school news           │
│  │   └── Generate AI narratives (optional)               │
│  └── Merge into stats/{school-slug}.json                 │
│                                                          │
│  Phase 3: Commit & push                                  │
└──────────────────────────────────────────────────────────┘
```

### Next Steps

1. **Sign up for Highlightly's free tier** and test coverage — confirm how many D1 schools they actually have data for
2. **Check request pricing** at the paid tier level needed for ~300K requests/month
3. **If affordable (<$50/month):** Go API-first with scraper fallback
4. **If too expensive or poor D1 coverage:** Stick with the scraper approach (it works, it's free, it just needs more maintenance)

The decision hinges on Highlightly's pricing and NCAA coverage depth — both of which you can only confirm by signing up and testing.

---

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
