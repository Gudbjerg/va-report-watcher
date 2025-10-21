require('dotenv').config();
const fetch = require('node-fetch');
const cheerio = require('cheerio');
const https = require('https');
const crypto = require('crypto');
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
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });

    if (!res.ok) throw new Error(`VA fetch failed: ${res.status}`);

    const html = await res.text();
    const $ = cheerio.load(html);

    const paragraph = $('p').filter((_, p) =>
      $(p).text().includes('Hearing Aid Procurement Summary') &&
      $(p).text().includes('report')
    ).first().text();

    const link = $('a[href$=".xlsx"]').first().attr('href');
    const match = paragraph.match(/\((January|February|March|April|May|June|July|August|September|October|November|December)\)/i);

    return {
      month: match ? match[1] : null,
      href: link ? new URL(link, BASE_URL).href : null
    };
  });
}

const getFileBuffer = url => new Promise((resolve, reject) => {
  https.get(url, res => {
    if (res.statusCode !== 200) return reject(new Error(`Fetch failed: ${res.statusCode}`));
    const chunks = [];
    res.on('data', chunk => chunks.push(chunk));
    res.on('end', () => resolve(Buffer.concat(chunks)));
  }).on('error', reject);
});

const getHash = buffer => crypto.createHash('sha256').update(buffer).digest('hex');

async function getLastSavedReport() {
  const { data, error } = await supabase.from(FILE_TABLE).select('month, hash, updated_at').eq('id', 1).maybeSingle();
  return error ? null : data;
}

async function saveLatestReport(month, hash) {
  const payload = {
    id: 1,
    month,
    hash,
    updated_at: new Date().toISOString()
  };
  console.log('[📥] Upserting to Supabase:', payload);
  const { error } = await supabase.from(FILE_TABLE).upsert(payload);
  if (error) {
    console.log('[❌] Supabase upsert error:', error);
  } else {
    console.log('[✅] Supabase record saved');
  }
}

async function notifyNewReport(url, buffer) {
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
    const { month, href } = await retry(() => fetchLatestReport());
    if (!month || !href) return { month: null };

    const buffer = await getFileBuffer(href);
    const hash = getHash(buffer);
    console.log(`[🧮] VA Hash: ${hash}`);

    const last = await getLastSavedReport();

    if (!last || last.hash !== hash) {
      console.log(`[✅] VA: New report or updated contents: ${month}`);
      await retry(() => notifyNewReport(href, buffer));
      await saveLatestReport(month, hash);
    } else {
      console.log(`[🟰] VA: Already recorded for ${month}`);
    }

    return { month };
  } catch (err) {
    console.log(`[🔥] VA: Error: ${err.message}`);
    return { month: null };
  }
}

module.exports = { runWatcher };
