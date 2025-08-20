// watchers/va.js
require('dotenv').config();
const fetch = require('node-fetch');
const cheerio = require('cheerio');
const https = require('https');
const { createClient } = require('@supabase/supabase-js');
const nodemailer = require('nodemailer');

const BASE_URL = 'https://www.va.gov/opal/nac/csas/index.asp';
const FILE_TABLE = 'va_report';

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS
  }
});

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
  return retry(async () => {
    const res = await fetch(BASE_URL, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/115.0.0.0 Safari/537.36'
      }
    });

    if (!res.ok) {
      throw new Error(`[❌] VA.gov fetch failed: ${res.status} ${res.statusText}`);
    }

    const html = await res.text();
    const $ = cheerio.load(html);

    const paragraph = $('p').filter((_, p) =>
      $(p).text().includes('Hearing Aid Procurement Summary') &&
      $(p).text().includes('report')
    ).first().text();

    const link = $('a').filter((_, a) => $(a).attr('href')?.endsWith('.xlsx')).first().attr('href');
    const match = paragraph.match(/\((January|February|March|April|May|June|July|August|September|October|November|December)\)/i);

    return {
      month: match ? match[1] : null,
      href: link ? new URL(link, BASE_URL).href : null
    };
  });
}

async function getLastSavedReport() {
  const { data, error } = await supabase.from(FILE_TABLE).select('month').eq('id', 1).single();
  return error ? null : data?.month;
}

async function saveLatestReport(month) {
  const payload = { id: 1, month };
  console.log('[📥] Upserting to Supabase:', payload);
  const { error } = await supabase.from(FILE_TABLE).upsert(payload);
  if (error) {
    console.log('[❌] Supabase upsert error:', error);
  } else {
    console.log('[✅] Supabase record saved');
  }
}

const getFileBuffer = url => {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      if (res.statusCode !== 200) return reject(new Error(`Failed to fetch: ${res.statusCode}`));
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
    }).on('error', reject);
  });
};

async function notifyNewReport(url) {
  const buffer = await getFileBuffer(url);
  await transporter.sendMail({
    from: process.env.EMAIL_USER,
    to: process.env.EMAIL_TO,
    subject: 'New VA Hearing Aid Report Available',
    text: `A new report is available: ${url}`,
    attachments: [{
      filename: 'va-latest.xlsx',
      content: buffer
    }]
  });
  console.log(`[📧] VA email sent: ${url}`);
}

async function runWatcher() {
  try {
    console.log('[🔍] VA: Checking for updated month...');
    const { month, href } = await fetchLatestReport();

    if (!month) return console.log('[❌] VA: No month found.');

    const now = new Date();
    now.setMonth(now.getMonth() - 1);
    const expectedMonth = now.toLocaleString('default', { month: 'long' });

    if (month.toLowerCase() !== expectedMonth.toLowerCase()) {
      return console.log(`[⏳] VA: Found "${month}", expected "${expectedMonth}".`);
    }

    const last = await getLastSavedReport();
    if (month !== last) {
      console.log(`[✅] VA: New month detected: ${month}`);
      await retry(() => notifyNewReport(href || BASE_URL));
      await saveLatestReport(month);
    } else {
      console.log(`[🟰] VA: Already recorded for ${month}`);
    }
  } catch (err) {
    console.log(`[🔥] VA: Error: ${err.message}`);
  }
}

module.exports = { runWatcher };
