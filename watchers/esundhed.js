// watchers/esundhed.js
require('dotenv').config();
const fetch = require('node-fetch');
const cheerio = require('cheerio');
const nodemailer = require('nodemailer');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const BASE_URL = 'https://www.esundhed.dk/Emner/Laegemidler/Laegemidlermodovervaegt';
const FILE_DOWNLOAD = './esundhed-latest.xlsx';
const FILE_TABLE = 'esundhed_report';

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

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

  const linkTag = $('h3:contains("Månedsopgørelser")')
    .closest('div')
    .find('a[href$=".ashx"]')
    .first();

  const relativeHref = linkTag.attr('href');
  if (!relativeHref) return null;

  const fullUrl = new URL(relativeHref, BASE_URL).href;
  const fileName = path.basename(relativeHref);

  return { fileName, fullUrl };
}

async function getLastEsundhedFilename() {
  const { data, error } = await supabase.from(FILE_TABLE).select('filename').eq('id', 1).single();
  return error ? null : data.filename;
}

async function saveEsundhedFilename(filename) {
  await supabase.from(FILE_TABLE).upsert({ id: 1, filename });
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

async function notifyNewEsundhedReport(url) {
  const buffer = await getFileBuffer(url);
  await transporter.sendMail({
    from: process.env.ESUNDHED_FROM_EMAIL || process.env.EMAIL_USER,
    to: process.env.ESUNDHED_TO_EMAIL || process.env.EMAIL_TO,
    subject: 'New eSundhed Report Available',
    text: `A new report is available: ${url}`,
    attachments: [{
      filename: 'esundhed-latest.xlsx',
      content: buffer
    }]
  });
  console.log(`[📧] Email sent for new eSundhed report: ${url}`);
}


async function checkEsundhedUpdate() {
  try {
    console.log('[🔍] Checking eSundhed for updated report...');
    const result = await fetchLatestEsundhedReport();
    if (!result) return console.log('[❌] Could not locate download link.');

    const { fileName, fullUrl } = result;
    const lastFileName = await getLastEsundhedFilename();

    if (fileName !== lastFileName) {
      console.log(`[✅] New report detected: ${fileName}`);
      await retry(() => notifyNewEsundhedReport(fullUrl));
      await saveEsundhedFilename(fileName);
    } else {
      console.log(`[🟰] Report already recorded: ${fileName}`);
    }
  } catch (err) {
    console.log(`[🔥] Error checking eSundhed update: ${err}`);
  }
}


module.exports = { checkEsundhedUpdate };
