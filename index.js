require('dotenv').config();
const fetch = require('node-fetch');
const cheerio = require('cheerio');
const https = require('https');
const cron = require('node-cron');
const nodemailer = require('nodemailer');
const express = require('express');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const PORT = process.env.PORT || 3000;

const BASE_URL = 'https://www.va.gov/opal/nac/csas/index.asp';
const FILE_DOWNLOAD = './latest.xlsx';

['EMAIL_USER', 'EMAIL_PASS', 'EMAIL_TO', 'SUPABASE_URL', 'SUPABASE_KEY'].forEach(key => {
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

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const retry = async (fn, attempts = 3) => {
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i === attempts - 1) throw err;
    }
  }
};

async function fetchLatestReport() {
  const res = await fetch(BASE_URL, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; VA-Watcher/1.0)'
    }
  });
  const html = await res.text();
  const $ = cheerio.load(html);

  const paragraph = $('p').filter((_, p) =>
    $(p).text().includes('Hearing Aid Procurement Summary') &&
    $(p).text().includes('report')
  ).first().text();

  const link = $('a').filter((_, a) => $(a).attr('href')?.endsWith('.xlsx')).first().attr('href');
  const match = paragraph.match(/\((January|February|March|April|May|June|July|August|September|October|November|December)\)/i);

  return { month: match ? match[1] : null, href: link ? new URL(link, BASE_URL).href : null };
}

async function getLastSavedReport() {
  const { data, error } = await supabase.from('va_report').select('month').eq('id', 1).single();
  return error ? null : data.month;
}

async function saveLatestReport(month) {
  await supabase.from('va_report').upsert({ id: 1, month });
}

function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const file = require('fs').createWriteStream(dest);
    https.get(url, response => {
      if (response.statusCode !== 200) {
        return reject(new Error(`Failed to get '${url}' (${response.statusCode})`));
      }
      response.pipe(file);
      file.on('finish', () => file.close(resolve));
    }).on('error', reject);
  });
}

async function notifyNewReport(url, filePath) {
  await transporter.sendMail({
    from: process.env.EMAIL_USER,
    to: process.env.EMAIL_TO,
    subject: 'New VA Hearing Aid Report Available',
    text: `A new report is available: ${url}`,
    attachments: filePath ? [{ filename: 'summary.xlsx', path: filePath }] : []
  });
  console.log(`[📧] Email sent for new report: ${url}`);
}

async function checkForUpdate() {
  try {
    console.log('[🔍] Checking for updated month...');
    const result = await fetchLatestReport();
    const reportedMonth = result.month;
    const fileUrl = result.href;
    if (!reportedMonth) return console.log('[❌] Could not find report month on page.');

    const now = new Date();
    now.setMonth(now.getMonth() - 1);
    const expectedMonth = now.toLocaleString('default', { month: 'long' });

    if (reportedMonth.toLowerCase() !== expectedMonth.toLowerCase()) {
      return console.log(`[⏳] Report not updated yet. Found "${reportedMonth}", expected "${expectedMonth}".`);
    }

    const lastMonthSaved = await getLastSavedReport();
    if (reportedMonth !== lastMonthSaved) {
      console.log(`[✅] New month detected: ${reportedMonth}`);
      let filePath = null;
      if (fileUrl) {
        try {
          await retry(() => downloadFile(fileUrl, FILE_DOWNLOAD));
          filePath = FILE_DOWNLOAD;
        } catch (err) {
          console.log(`⚠️ Failed to download file: ${err.message}`);
        }
      }
      await retry(() => notifyNewReport(fileUrl || BASE_URL, filePath));
      await saveLatestReport(reportedMonth);
    } else {
      console.log(`[🟰] Report already recorded for ${reportedMonth}`);
    }
  } catch (err) {
    console.log(`[🔥] Error checking for update: ${err}`);
  }
}

cron.schedule('0 12-18 * * *', checkForUpdate);

app.get('/', (_, res) => {
  res.send('<h1>✅ VA Watcher is live!</h1>');
});

app.get('/ping', (_, res) => res.send('pong'));
app.get('/scrape', async (_, res) => {
  await checkForUpdate();
  res.send('Scrape complete!');
});

app.get('/test-email', async (_, res) => {
  try {
    await transporter.sendMail({
      from: process.env.EMAIL_USER,
      to: process.env.EMAIL_TO,
      subject: '[TEST] VA Watcher Email',
      text: 'Test email from VA Watcher ✔️'
    });
    res.send('✅ Test email sent!');
  } catch (err) {
    res.send(`❌ Failed to send test email: ${err.message}`);
  }
});

app.listen(PORT, () => {
  console.log(`🌐 Web server running on port ${PORT}`);
  checkForUpdate();
});
