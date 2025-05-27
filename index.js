require('dotenv').config();
const puppeteer = require('puppeteer');
const fs = require('fs');
const cron = require('node-cron');
const nodemailer = require('nodemailer');

const URL = 'https://www.va.gov/opal/nac/csas/index.asp';
const STORAGE_FILE = './lastReport.json';

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS
  }
});

async function fetchLatestReport() {
  const browser = await puppeteer.launch({ headless: 'new' });
  const page = await browser.newPage();
  await page.goto(URL, { waitUntil: 'networkidle2' });

  const result = await page.evaluate(() => {
    const para = Array.from(document.querySelectorAll('p'))
      .find(p => p.textContent.includes('Hearing Aid Procurement Summary') && p.textContent.includes('report'));
    if (!para) return null;

    const match = para.textContent.match(/\((January|February|March|April|May|June|July|August|September|October|November|December)\)/i);
    return match ? match[1] : null;
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

async function notifyNewReport(newUrl) {
  await transporter.sendMail({
    from: process.env.EMAIL_USER,
    to: process.env.EMAIL_TO,
    subject: 'New VA Hearing Aid Report Available',
    text: `A new report is available:\n${newUrl}`
  });
  console.log(`[📧] Email sent for new report: ${newUrl}`);
}

async function checkForUpdate() {
  try {
    console.log('[🔍] Checking for updated month...');
    const reportedMonth = await fetchLatestReport();
    if (!reportedMonth) return console.log('[❌] Could not find report month on page.');

    const now = new Date();
    now.setMonth(now.getMonth() - 1);
    const expectedMonth = now.toLocaleString('default', { month: 'long' });

    if (reportedMonth.toLowerCase() !== expectedMonth.toLowerCase()) {
      console.log(`[⏳] Report not updated yet. Found "${reportedMonth}", expected "${expectedMonth}".`);
      return;
    }

    const lastMonthSaved = getLastSavedReport();
    if (reportedMonth !== lastMonthSaved) {
      console.log(`[✅] New month detected: ${reportedMonth}`);
      await notifyNewReport('https://www.va.gov/opal/docs/nac/csas/summaryVAhearingAidProcurement.xlsx');
      saveLatestReport(reportedMonth);
    } else {
      console.log(`[🟰] Report already recorded for ${reportedMonth}`);
    }
  } catch (err) {
    console.error('[🔥] Error checking for update:', err);
  }
}

cron.schedule('0 9 * * *', checkForUpdate);

(async () => {
  await checkForUpdate();
})();
