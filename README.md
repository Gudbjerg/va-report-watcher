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

### 3. Set Environment Variables

Create a `.env` file:

```env
# Shared
EMAIL_PASS=your-app-password
PORT=3000
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key

# VA Watcher
EMAIL_USER=va.sender@gmail.com
EMAIL_TO=recipient@example.com

# eSundhed Watcher (optional override)
ESUNDHED_FROM_EMAIL=other.sender@gmail.com
ESUNDHED_TO_EMAIL=other.recipient@example.com
```

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

```
.
├── index.js
├── .env
├── package.json
└── watchers
    ├── va.js
    └── esundhed.js
```

---

## ☁️ Deployment Tips

### ✅ Render (Free Tier)

* Add env vars in Render dashboard
* Start command: `npm start`
* UptimeRobot pings `/ping` every 5 min

---

## 🧪 Debugging Tips

* Use `/scrape/va` or `/scrape/esundhed`
* Check Supabase tables: `va_report`, `esundhed_report`
* Inspect console logs

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
