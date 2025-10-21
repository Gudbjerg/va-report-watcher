// watchers/esundhed.js
require('dotenv').config();
const fetch = require('node-fetch');
const cheerio = require('cheerio');
const nodemailer = require('nodemailer');
const https = require('https');
const path = require('path');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');

const BASE_URL = 'https://sundhedsdatabank.dk/medicin/medicintyper';
const FILE_TABLE = 'esundhed_report';

// Prefer service role key for backend operations
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY
);

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.ESUNDHED_FROM_EMAIL || process.env.EMAIL_USER,
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

async function fetchLatestEsundhedReport() {
  const res = await fetch(BASE_URL);
  const html = await res.text();
  const $ = cheerio.load(html);

  console.log('[🧾] Loaded new sundhedsdatabank page');

  const linkTag = $('a.icon[href$=".XLSX"]')
    .filter((_, el) => $(el).text().trim().includes('Vægttabs- og diabetesmedicin'))
    .first();

  const relativeHref = linkTag.attr('href');
  if (!relativeHref) {
    console.log('[❌] No XLSX link found with expected content');
    return null;
  }

  const fullUrl = new URL(relativeHref, BASE_URL).href;
  const fileName = decodeURIComponent(path.basename(relativeHref));

  console.log(`[📁] Found file: ${fileName}`);
  console.log(`[🔗] Full URL: ${fullUrl}`);

  return { fileName, fullUrl };
}

async function getLastEsundhedRecord() {
  const { data, error } = await supabase
    .from(FILE_TABLE)
    .select('filename, hash, updated_at')
    .eq('id', 1)
    .maybeSingle();

  if (error) {
    console.error('[❌] Failed to fetch last record:', error);
    return null;
  }

  return data;
}

async function saveEsundhedRecord(filename, hash) {
  console.log('[📥] Upserting to Supabase:', { filename, hash });
  const { data, error, status } = await supabase
    .from(FILE_TABLE)
    .upsert({
      id: 1,
      filename,
      hash,
      updated_at: new Date().toISOString()
    })
    .select();

  console.log('[supabase] upsert result', { status, error, data });
  if (error) {
    console.error('[❌] Supabase upsert error:', error);
    return false;
  }

  console.log('[✅] Supabase record saved:', data);
  return true;
}

const getFileBuffer = url => {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      if (res.statusCode !== 200) return reject(new Error(`Failed to fetch: ${res.statusCode}`));
      const data = [];
      res.on('data', chunk => data.push(chunk));
      res.on('end', () => resolve(Buffer.concat(data)));
    }).on('error', reject);
  });
};

const getHash = buffer => crypto.createHash('sha256').update(buffer).digest('hex');

async function notifyNewEsundhedReport(url, buffer) {
  try {
    if (process.env.SENDINBLUE_API_KEY) {
      const { sendViaSendinblue } = require('../lib/sendViaSendinblue');
      // Support comma-separated recipient lists in env var
      const toEnv = process.env.ESUNDHED_TO_EMAIL || process.env.EMAIL_TO;
      const to = toEnv && toEnv.includes(',') ? toEnv.split(',').map(s => s.trim()) : toEnv;
      await sendViaSendinblue({
        from: process.env.ESUNDHED_FROM_EMAIL || process.env.EMAIL_USER,
        to,
        subject: 'New eSundhed Report Available',
        text: `A new report is available: ${url}`,
        attachments: [{ filename: 'esundhed-latest.xlsx', content: buffer }]
      });
      console.log(`[📧] Email sent via Sendinblue for new eSundhed report: ${url}`);
    } else {
      const toEnv = process.env.ESUNDHED_TO_EMAIL || process.env.EMAIL_TO;
      const to = toEnv && toEnv.includes(',') ? toEnv.split(',').map(s => s.trim()) : toEnv;
      await transporter.sendMail({
        from: process.env.ESUNDHED_FROM_EMAIL || process.env.EMAIL_USER,
        to,
        subject: 'New eSundhed Report Available',
        text: `A new report is available: ${url}`,
        attachments: [{
          filename: 'esundhed-latest.xlsx',
          content: buffer
        }]
      });
      console.log(`[📧] Email sent for new eSundhed report: ${url}`);
    }
  } catch (err) {
    console.error('[email] notifyNewEsundhedReport failed (logged, not thrown):', err && err.message ? err.message : err);
  }
}

async function checkEsundhedUpdate() {
  try {
    console.log('[🔍] Checking eSundhed for updated report...');
    const result = await fetchLatestEsundhedReport();
    if (!result) {
      console.log('[❌] Could not locate download link.');
      return { filename: null };
    }

    const { fileName, fullUrl } = result;
    const buffer = await getFileBuffer(fullUrl);
    const hash = getHash(buffer);

    console.log(`[🧮] Computed hash: ${hash}`);

    if (!hash || hash.length !== 64) {
      console.error('[‼️] Invalid hash generated. Skipping save and notification.');
      return { filename: null };
    }

    const lastRecord = await getLastEsundhedRecord();
    const isNew = !lastRecord || hash !== lastRecord.hash;

    console.log('[🧪 Final Check]', {
      fileName,
      hash,
      lastHash: lastRecord?.hash,
      isNew
    });

    if (isNew) {
      console.log(`[✅] New report detected or updated contents: ${fileName}`);

      // Persist to DB first, then notify. If save fails, log and skip notification.
      const saved = await saveEsundhedRecord(fileName, hash);
      if (!saved) {
        console.warn('[⚠️] Failed to save record to Supabase; skipping email notification.');
      } else {
        await retry(() => notifyNewEsundhedReport(fullUrl, buffer));
      }
    } else {
      console.log(`[🟰] Report already recorded. Skipping. Same hash: ${lastRecord.hash}`);
    }

    return {
      filename: fileName,
      hash,
      updated: isNew,
      timestamp: new Date().toISOString()
    };
  } catch (err) {
    console.log(`[🔥] Error checking eSundhed update: ${err}`);
    return { filename: null };
  }
}

module.exports = {
  checkEsundhedUpdate,
  fetchLatestEsundhedReport
};