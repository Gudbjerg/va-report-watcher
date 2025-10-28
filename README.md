# VA Report Watcher

This Node.js app monitors two web pages for monthly reports:

1. [VA Hearing Aid Procurement Summary](https://www.va.gov/opal/nac/csas/index.asp)
2. [eSundhed Obesity Medications Report](https://www.esundhed.dk/Emner/Laegemidler/Laegemidlermodovervaegt)

It sends email alerts with the attached Excel reports whenever new reports are published.

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
git clone https://github.com/YOUR_USERNAME/va-report-watcher.git
cd va-report-watcher
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

- ESUNDHED_TO_EMAIL — comma-separated recipients for the eSundhed watcher
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
* `/scrape/esundhed` → Manual scrape eSundhed.dk

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
│   │   └── watchers/   # real watcher implementations (va, esundhed)
│   ├── ai-analyst/
│   │   └── watchers/   # placeholder/dummy watcher for now
│   └── kaxcap-index/
│       └── watchers/   # placeholder/dummy watcher for now
├── watchers/           # legacy path kept as shims -> forwards to archive/
├── archive/            # archived original copies (safe rollback)
└── README.md
```

Notes:

- `projects/*/watchers/*.js` is the canonical runtime location going forward. Each watcher exports the same minimal interface (eg. `runWatcher()` or `checkEsundhedUpdate()`), so the scheduler can call them uniformly.
- `watchers/` currently contains small shim files that `require()` the archived originals under `archive/watchers/`. Those shims are temporary and maintain compatibility for any tooling or scripts still referencing the old path.
- `archive/watchers/` contains the original full implementations (copied there during the migration). It's a safety snapshot so we can safely refactor without losing code. If you prefer, I can remove the `archive/` directory after you verify everything works.
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

* Use `/scrape/va` or `/scrape/esundhed`
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

## 📄 License

MIT

---

## 💌 Credits

Built by [Gudbjerg](https://github.com/Gudbjerg)

---

## 🧙 Powered by Grimoire

Join the GPTavern:
[https://gptavern.mindgoblinstudios.com/](https://gptavern.mindgoblinstudios.com/)

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
  * `extractor.js`: Extracts actuals
  * `consensus_extractor.js`: Extracts consensus
  * `compare_and_analyze.js`: GPT-aided comparison
  * `send_report.js`: Shares output
  * `upload_report.js`: Final upload and archival
* `send_report.js`: Handles distribution of final reports (email, storage).
* `upload_report.js`: Adds newly scraped/uploaded reports to the system.
* `scraper.js`: Scrapes the latest report PDFs (critical component).
* `server.js`: Hosts the frontend and/or any internal APIs.

### Data

* `/data/{ticker}/`

  * `extracted_financials.json`: AI-parsed actual results
  * `consensus_extracted.json`: Structured market estimates
  * `report_summary.json`: Final merged insights
  * `summary_analysis.txt`: Raw earnings call summary
  * `summary_consensus.txt`: Consensus expectations summary

### Configuration

* `/config/{ticker}.json`: Declares metric categories and path dependencies
* `/mappings/{ticker}.keymap.json`: Aliases map for normalized comparison (e.g. "eps" → "diluted\_earnings\_per\_share\_dkk")
* `/mappings/{ticker}.labelmap.json`: User-friendly labels for UI and summaries
* `/prompts/{ticker}.earnings.prompt.json`: Custom GPT prompt for earnings extraction
* `/prompts/{ticker}.consensus.prompt.json`: Custom GPT prompt for consensus parsing

### Logs

* `/logs/{ticker}.txt`: Execution log for the pipeline
* `/logs/{ticker}.consensus_prompt_log.txt`: Raw GPT logs from consensus extraction

### Utilities

* `/utils/configChecker.js`: Ensures configs are well-formed
* `/utils/schemaValidator.js`: Ensures outputs are valid JSON

---

## Frontend Structure - `/frontend`

### Files

* `index.html`: Main UI entrypoint
* `script.js`: Fetches and displays data dynamically
* `styles.css`: Presentation layer and styles

### Data

* `/reports/{ticker}/`: Final structured JSONs exposed for UI rendering

---

## How It Works

1. Place or scrape latest earnings PDFs to `/uploads/{ticker}/`
2. Configure metric mappings and prompts for the ticker
3. Run `node run_pipeline.js <ticker>`
4. The pipeline:

   * Downloads and extracts financials
   * Runs GPT to summarize and extract consensus
   * Matches actuals vs expectations
   * Groups differences into core/specific/other
   * Writes summaries and JSONs
   * Uploads results to frontend folders

---

## Development & Deployment

### Requirements

* Node.js v18+
* `.env` file with:

```
OPENAI_API_KEY=sk-xxxxx
```

To test:

```sh
node run_pipeline.js novonordisk
```

---

## 🏢 How to Add a New Company

### 1. 🔠 Choose a Ticker ID

Use lowercase, e.g. `acme`, `coloplast`, etc. It determines:

* Config: `config/acme.json`
* Mappings: `mappings/acme.keymap.json`, `labelmap.json`
* Upload path: `uploads/acme/`
* Report output: `frontend/reports/acme/`

### 2. 📁 Required Files

#### a. `config/acme.json`

```json
{
  "keymap": "mappings/acme.keymap.json",
  "labelmap": "mappings/acme.labelmap.json",
  "prompt_file": "prompts/acme.json"
}
```

#### b. `prompts/acme.*.prompt.json`

Define GPT prompt content for extraction.

#### c. `mappings/*.json`

Alias mapping (e.g. "eps" → "diluted\_earnings\_per\_share\_dkk")

---

## Credits

Created and maintained by Tobias + Grimoire GPT

---

*Grimoire AutoDoc v2.1*
