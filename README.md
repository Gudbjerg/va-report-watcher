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
