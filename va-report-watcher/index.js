require('dotenv').config();
const puppeteer = require('puppeteer');
const fs = require('fs');
const cron = require('node-cron');
const nodemailer = require('nodemailer');
const express = require('express');
const path = require('path');
const https = require('https');

const app = express();
const PORT = process.env.PORT || 3000;

const URL = 'https://www.va.gov/opal/nac/csas/index.asp';
const STORAGE_FILE = './lastReport.json';
const LOG_FILE = './log.json';
const DOWNLOAD_FILE = './report.xlsx';

['EMAIL_USER', 'EMAIL_PASS', 'EMAIL_TO'].forEach(key => {
  if (!process.env[key]) {
    throw new Error(`Missing ENV: ${key}`);
  }
});

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS
  }
});

function logMessage(message) {
  const timestamp = new Date().toISOString();
  const entry = { timestamp, message };
  const logs = fs.existsSync(LOG_FILE) ? JSON.parse(fs.readFileSync(LOG_FILE)) : [];
  logs.unshift(entry);
  fs.writeFileSync(LOG_FILE, JSON.stringify(logs.slice(0, 50), null, 2));
}

async function fetchLatestReport() {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  const page = await browser.newPage();
  await page.goto(URL, { waitUntil: 'networkidle2', timeout: 60000 });

  const result = await page.evaluate(() => {
    const para = Array.from(document.querySelectorAll('p'))
      .find(p => p.textContent.includes('Hearing Aid Procurement Summary') && p.textContent.includes('report'));
    let reportedMonth = null;
    if (para) {
      const match = para.textContent.match(/\((January|February|March|April|May|June|July|August|September|October|November|December)\)/i);
      if (match) reportedMonth = match[1];
    }

    const link = Array.from(document.querySelectorAll('a'))
      .find(a => a.href.includes('summaryVAhearingAidProcurement.xlsx'));
    const xlsxURL = link ? link.href : null;

    if (!reportedMonth && xlsxURL) {
      const nameMatch = xlsxURL.match(/_(January|February|March|April|May|June|July|August|September|October|November|December)/i);
      reportedMonth = nameMatch ? nameMatch[1] : null;
    }

    return { reportedMonth, xlsxURL };
  });

  await browser.close();
  return result;
}

function getLastSavedReport() {
  if (!fs.existsSync(STORAGE_FILE)) return null;
  const data = fs.readFileSync(STORAGE_FILE);
  return JSON.parse(data).month;
}

function saveLatestReport(month) {
  fs.writeFileSync(STORAGE_FILE, JSON.stringify({ month }, null, 2));
}

function downloadFile(fileURL, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    https.get(fileURL, response => {
      if (response.statusCode !== 200) return reject('Failed to download');
      response.pipe(file);
      file.on('finish', () => {
        file.close(resolve);
      });
    }).on('error', reject);
  });
}

async function notifyNewReport(url, attachmentPath) {
  await transporter.sendMail({
    from: process.env.EMAIL_USER,
    to: process.env.EMAIL_TO,
    subject: 'New VA Hearing Aid Report Available',
    text: `A new report is available: ${url}`,
    attachments: attachmentPath ? [{ path: attachmentPath }] : []
  });
  logMessage(`📧 Email sent for new report: ${url}`);
}

async function checkForUpdate() {
  try {
    logMessage('🔍 Checking for updated month...');
    const { reportedMonth, xlsxURL } = await fetchLatestReport();
    if (!reportedMonth) return logMessage('❌ Could not find report month on page.');

    const now = new Date();
    now.setMonth(now.getMonth() - 1);
    const expectedMonth = now.toLocaleString('default', { month: 'long' });

    if (reportedMonth.toLowerCase() !== expectedMonth.toLowerCase()) {
      return logMessage(`⏳ Report not updated yet. Found "${reportedMonth}", expected "${expectedMonth}".`);
    }

    const lastMonthSaved = getLastSavedReport();
    if (reportedMonth !== lastMonthSaved) {
      logMessage(`✅ New month detected: ${reportedMonth}`);
      let attachmentPath = null;
      if (xlsxURL) {
        await downloadFile(xlsxURL, DOWNLOAD_FILE);
        attachmentPath = DOWNLOAD_FILE;
      }
      await notifyNewReport(`New Hearing Aid Summary report detected for ${reportedMonth}. View it here: ${URL}`, attachmentPath);
      saveLatestReport(reportedMonth);
    } else {
      logMessage(`🟰 Report already recorded for ${reportedMonth}`);
    }
  } catch (err) {
    logMessage(`🔥 Error checking for update: ${err}`);
  }
}

cron.schedule('0 9 * * *', checkForUpdate);

app.get('/', (_, res) => {
  const logs = fs.existsSync(LOG_FILE) ? JSON.parse(fs.readFileSync(LOG_FILE)) : [];
  const html = `
    <html>
      <head><title>VA Watcher</title></head>
      <body>
        <h1>✅ VA Watcher is live!</h1>
        <h2>Logs</h2>
        <ul>
          ${logs.map(log => `<li>[${log.timestamp}] ${log.message}</li>`).join('')}
        </ul>
      </body>
    </html>
  `;
  res.send(html);
});

app.get('/ping', (_, res) => res.send('pong'));
app.get('/scrape', async (_, res) => {
  await checkForUpdate();
  res.send('Scrape complete!');
});

app.listen(PORT, () => {
  logMessage(`🌐 Web server running on port ${PORT}`);
  checkForUpdate();
});
