# MarketBuddy (formerly "VA Report Watcher")

This repository now contains a small product suite of related services for monitoring, analyzing, and proposing changes to financial/healthcare data. The original watcher/scraper is still present, but the project scope has expanded to three products:

1. Watchers (scrapers) — lightweight site-specific scrapers that detect and persist newly published reports (VA and Sundhedsdatabank).
2. Index Rebalancer — pulls index constituent data (X), computes rebalancing proposals, and stores proposals for review/execution.
3. AI Analyst — ingests reports and proposals, generates summaries and embeddings, and answers natural-language queries over stored documents.

Notes: the original watcher functionality (VA / Sundhedsdatabank) is preserved under `projects/analyst-scraper/watchers`. The new products live under `workers/` (rebalancer) and `services/ai-analyst/` (AI tooling).

---

## 🔧 Features

* Lightweight web scrapers using `cheerio`
* Email notifications with `nodemailer` + Gmail
* Automatically attaches `.xlsx` reports
* Hourly or bi-daily cron jobs with `node-cron`
* Persistent report tracking via **Supabase** tables
* Web server with health/test/scrape routes
* No local file saving (buffer-based email attachments)
* Modular script architecture per site

---

## 🚀 Quick Start

### 1. Clone Repo

```bash
git clone https://github.com/Gudbjerg/marketbuddy.git
cd marketbuddy
```

### 2. Install Dependencies

```bash
npm install
```

## 3. Set Environment Variables

Create a `.env` file. Recommended env vars (use Render dashboard for production):

```env
# Shared
EMAIL_USER=smtp.user@gmail.com   # SMTP username (used to auth with nodemailer)
EMAIL_PASS=your-app-password     # SMTP app password
FROM_EMAIL=alerts@yourdomain.com # Envelope From address (defaults to EMAIL_USER if not set)
TO_EMAIL=recipient@example.com   # Comma-separated recipient list for general alerts
PORT=3000
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key

# Site-specific overrides (optional)
ESUNDHED_FROM_EMAIL=other.sender@gmail.com  # optional override From for eSundhed alerts
ESUNDHED_TO_EMAIL=other.recipient@example.com
```

### Per-watcher recipient lists

This project supports per-watcher recipient lists so individual scrapers can notify different groups.

- ESUNDHED_TO_EMAIL — comma-separated recipients for the Sundhedsdatabank watcher
- VA_TO_EMAIL — comma-separated recipients for the VA watcher
- ESUNDHED_FROM_EMAIL / VA_FROM_EMAIL — optional From address overrides for each watcher
- TO_EMAIL — global fallback recipient list used when a watcher-specific list is not set

Behavior notes:

- Values are parsed as comma-separated lists and trimmed — e.g. "a@example.com, b@example.com".
- If a watcher-specific TO is unset, it falls back to `TO_EMAIL`, and then to `EMAIL_USER` (useful for local tests).
- `FROM_EMAIL` is optional; if unset the code uses `EMAIL_USER` as the envelope From.
- `DISABLE_EMAIL` must be set to the literal string `true` (lowercase) to disable sending. The app checks `process.env.DISABLE_EMAIL === 'true'`.

Example per-watcher envs (Render dashboard):

ESUNDHED_TO_EMAIL="alice@example.com,bob@example.com"
VA_TO_EMAIL="charlie@example.com"
FROM_EMAIL="reports@yourdomain.com"

Keep `EMAIL_USER`/`EMAIL_PASS` for SMTP auth even if `FROM_EMAIL` matches — `EMAIL_USER` is used to authenticate with your SMTP provider.

When you migrate providers or are testing on production, keep `DISABLE_EMAIL=true` until you've verified the new provider and recipient lists.

> 💡 Gmail App Passwords are required for nodemailer
> 🔐 Use Supabase **service role** key for database writes

### 4. Run Locally

```bash
npm start
```

---

## 🌐 Web Interface

* `/` → Status
* `/ping` → UptimeRobot compatible
* `/scrape/va` → Manual scrape VA.gov
* `/scrape/sundhedsdatabank` → Manual scrape Sundhedsdatabank.dk (alias: `/scrape/esundhed`)

---

## 📦 Project Structure

Current repo layout (trimmed to relevant files/folders):

```
.                         # project root (run server here)
├── index.js            # server entry + scheduler and routes
├── .env                # local env (not checked in)
├── package.json
├── lib/                # shared helpers (eg. lib/sendEmail.js)
├── scripts/            # developer scripts (check-watchers, helpers)
├── debug/              # optional debug helpers
├── projects/           # new multi-project layout
│   ├── analyst-scraper/
│   │   └── watchers/   # real watcher implementations (va, sundhedsdatabank)
│   ├── ai-analyst/
│   │   └── watchers/   # placeholder/dummy watcher for now
│   └── kaxcap-index/
│       └── watchers/   # placeholder/dummy watcher for now
├── archive/            # archived original copies (safe rollback)
└── README.md
```

Notes:

- `projects/*/watchers/*.js` is the canonical runtime location going forward. Each watcher exports the same minimal interface (eg. `runWatcher()` or `checkEsundhedUpdate()`), so the scheduler can call them uniformly.
- Legacy `watchers/` path has been retired — the runtime now loads watchers from `projects/*/watchers/*`.
- `archive/watchers/` contains original copies (safety snapshot). We keep `archive/` for rollback; remove it later when you're confident.
- `projects/*/watchers/*` may include placeholder/dummy watchers (e.g. `kaxcap-index`) so the scheduler won't fail if that project is not yet implemented; these are deliberate and can be replaced with real scrapers later.

---

## ☁️ Deployment Tips

### ✅ Render (Free Tier)

* Add env vars in Render dashboard (do NOT store API keys or secrets in the repo)
* Start command: `npm start`
* Set `DISABLE_EMAIL=true` while migrating providers
* UptimeRobot pings `/ping` every 5 min

---

## 🧪 Debugging Tips

* Use `/scrape/va` or `/scrape/sundhedsdatabank`
* Check Supabase tables: `va_report`, `esundhed_report`
* Inspect console logs

### Developer utilities

- `node scripts/check-watchers.js` — checks that the watcher modules resolve from the project root and prints their resolved paths. Run it from repo root (the script resolves project-root-relative paths).
- When testing quick module loads in `node -e`, make sure you pass only JS code to `node -e` — avoid pasting shell commands (like `git commit`) into the `-e` string (that causes the SyntaxError you saw). Example correct usage:

```bash
node -e "console.log(require('./projects/analyst-scraper/watchers/va.js') ? 'va loaded' : 'va missing')"
```

If you accidentally run shell commands inside `node -e` you'll see errors like `SyntaxError: missing ) after argument list` — that's because the shell/Git commands are not valid JS source.

---

## CI & Tests

This repo includes a small GitHub Actions smoke workflow and local test helpers.

- Unit tests (fast, run in CI):
  - Run locally: `npm test` (uses Jest)
- Integration tests (touch Supabase, guarded in CI):
  - Run locally (requires Supabase env vars): `npm run test:integration`
- Smoke server (start app without sending emails):
  - Run locally: `npm run smoke` (sets `DISABLE_EMAIL=true`)

CI behavior (summary): the workflow starts the app with `DISABLE_EMAIL=true`, pings `/ping`, runs the unit tests always, and only runs integration tests when Supabase secrets are provided to the runner. This gives quick pre-merge feedback while keeping secrets out of forked PRs.


---

## 📄 License

MIT

## Legal disclaimer

This repository contains internal tooling and experimental code developed for use by ABG Sundal Collier and collaborators. It is provided for internal evaluation and development purposes only and is not an official product or offering of ABG Sundal Collier. See `LEGAL.md` for the full disclaimer.

---

## 💌 Credits

Built by [Gudbjerg](https://github.com/Gudbjerg)


# AI Quarterly Reports

This project is a comprehensive system for analyzing quarterly financial reports of companies using AI-powered tools. It automates the extraction of financial data, compares it to analyst consensus, and generates summaries, comparisons, and insights for internal reporting.

---

## Project Structure

### Root Directory

* `.vscode/settings.json`: Editor configuration.
* `.DS_Store`: System file, can be ignored.

### /AI-Quarterly-Reports

Main project folder containing backend and frontend components.

---

## Backend Structure - `/backend`

### Core Files

* `compare_and_analyze.js`: Compares actual vs consensus data using mappings and prompts, categorizes differences, and summarizes insights.
* `extractor.js`: Extracts key financial metrics from uploaded earnings report text.
* `consensus_extractor.js`: Extracts structured consensus data from analyst expectation documents.
* `gammelfunker.js`: Legacy functions retained for reference.
* `multi_ticker_scheduler.js`: Runs the pipeline across multiple tickers in batch.
* `run_pipeline.js`: Master orchestrator. Sequentially runs:

  * `scraper.js`: Download earnings PDFs
  # MarketBuddy — ABG internal

  MarketBuddy is an internal ABG toolkit for monitoring index rebalances, scraping watcher reports, and producing rapid AI-driven commentary for the sales desk.

  Key components

  - Watchers — site-specific scrapers that detect and persist newly published reports (VA, eSundhed, etc.).
  - Index Rebalancer — computes and stores rebalancing proposals for review and execution.
  - AI Analyst — generates concise, plain-language comments on earnings deviations and rebalances so sales can react fast.

  Usage (quick)

  1. Install dependencies

  ```bash
  npm install
  ```

  2. Configure environment

  Create a `.env` with the minimum required variables. For local dev, at least set `PORT` and `DISABLE_EMAIL=true` to avoid accidental emails.

  Required (recommended for local dev):

  ```env
  PORT=3000
  DISABLE_EMAIL=true
  ```

  Optional (production / Render):

  ```env
  SUPABASE_URL=...            # optional — used for persisted proposals and reports
  SUPABASE_KEY=...
  EMAIL_USER=...              # SMTP user (if you enable email)
  EMAIL_PASS=...
  ```

  3. Start locally

  ```bash
  npm start
  ```

  Web UI

  - `/` — Dashboard (watchers, memes, quick links)
  - `/watchers` — List available watchers and run them manually
  - `/product/rebalancer` — Rebalancer proposals (requires Supabase to persist)
  - `/index` — Index overview (KAXCAP/HEL/STO) with two tables per index
      Seeding for local verification
      - Use the seed script to insert mock rows (since X calls won’t work locally without allowlisted IPs):
        - `node scripts/seed-index.js`
        - Then open `/index` to verify Quarterly + Daily tables render.
    - Quarterly Proforma: see Methodology below (uncapped ranking → assign exception caps and 4.5% cap, delta vs current capped, AUM-driven cash/volume/DTC).
    - Daily Status: shows current capped ranking and rule tracking, including per-issuer flags, delta vs current capped, AUM-driven cash/volume/DTC.
    - Refresh button per index calls `/api/kaxcap/run?region=...&indexId=...` to trigger the Python worker.

  Data flow
  - Python worker (X Formula) fetches, computes daily/quarterly, upserts to Supabase.
  - Node server reads latest snapshots and renders JSON + tables. If local IP isn’t allow‑listed for X, pages show empty until Supabase has rows.

---

## 📐 Index Methodology (Daily vs Quarterly)

Inputs
- X Formula API provides `price`, `shares`, `shares_capped`, `avg_vol_30d` (or variants), and optional `omx_weight`/`omx_weight_capped`.

Caps and thresholds
- Base cap: 4.5%.
- Exception cap (10% rule): issuers with initial uncapped weight >10% are capped at 7% (CPH/HEL) or 9% (STO).
- Aggregate (>5%) rule: sum of all issuers whose current capped weight exceeds 5% must be ≤ 40%.

Quarterly (Proforma)
- Ranking: by uncapped market cap (largest → smallest).
- Assignment:
  - Set exception cap (7% CPH/HEL, 9% STO) for top names as long as they warrant it by mass/rank.
  - Allocate remaining proportionally subject to a 4.5% cap; iteratively cap violators until stable.
- Delta: `proforma_weight − current_capped_weight`.
- Cash/volume: `Delta (CCY) = AUM × Delta (fraction)`, `Shares = Delta (CCY)/price`, `DTC = abs(Shares)/avg_daily_volume`.
- UI ordering: by `mcap_uncapped`.

Daily (Status + Rule tracking)
- Ranking: by current capped weight (this reflects the live index order).
- Rule monitoring:
  - 10% exception: if an issuer’s initial uncapped >10% and current capped > exception cap, it’s flagged as a 10% breach.
  - 40% aggregate: if the sum of names with current capped >5% exceeds 40%, the smallest name in that set (excluding exception issuers) is the cut candidate to 4.5%.
- Proposed weights: computed under daily rules; `delta = proposed_capped − current_capped`.
- Cash/volume: computed as per Quarterly.
- Flags: UI shows `10% breach` and/or `40% breach — cut to 4.5%`. If no explicit flag but a cap applies, UI shows `capped`.

Defaults (AUM / currency)
- CPH: 110bn DKK, HEL: 22bn EUR, STO: 450bn SEK. These drive `Delta (CCY)` and `DTC` in the UI; adjust in `index.js` if needed.

API shapes
- Daily: `/api/index/:indexId/constituents` (ordered by `capped_weight` desc).
- Quarterly: `/api/index/:indexId/quarterly` (ordered by `mcap_uncapped` desc), maps current capped and target weights and includes derived deltas.

  Supabase tables (expected)
  - Daily: `index_constituents` (index_id, ticker, name, price, shares, mcap, weight, avg_vol_30d, as_of; optional issuer, region, capped_weight).
  - Quarterly: one of `index_quarterly`, `index_quarterly_status`, or `index_quarterly_proforma` (columns typically: ticker, issuer, old_weight, new_weight, flags, as_of).

  Contact

  Maintained by Tobias Gudbjerg for ABG internal use. Contact Tobias via LinkedIn: https://www.linkedin.com/in/tobias-gudbjerg-59b893249/

