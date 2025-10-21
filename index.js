require('dotenv').config();
// Debug Supabase connectivity when DEBUG_SUPABASE=1 is set (safe for debug deploys)
if (process.env.DEBUG_SUPABASE === '1') {
  try {
    require('./debug/supabase-check');
  } catch (e) {
    console.warn('[debug] supabase-check not found:', e.message);
  }
}
const express = require('express');
const cron = require('node-cron');
const { runWatcher: checkVA } = require('./watchers/va');
const { checkEsundhedUpdate: checkEsundhed } = require('./watchers/esundhed');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const PORT = process.env.PORT || 3000;

// Prefer server-only service role key for backend writes (fallback to anon key)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY
);

// Status memory
let lastVA = { time: null, month: null };
let lastEsundhed = { time: null, filename: null, updated_at: null };

// Patch watchers to update status
async function updateVA() {
  console.log(`[⏰] ${new Date().toISOString()} — Cron triggered: checkVA()`);
  const result = await checkVA();
  if (result?.month) {
    lastVA = { time: new Date(), month: result.month };
  }
}

async function updateEsundhed() {
  console.log(`[⏰] ${new Date().toISOString()} — Cron triggered: checkEsundhed()`);
  const result = await checkEsundhed();

  if (result?.filename) {
    const { data, error } = await supabase
      .from('esundhed_report')
      .select('updated_at')
      .eq('id', 1)
      .single();

    const updatedAt = data?.updated_at
      ? new Date(data.updated_at)
      : null;

    lastEsundhed = {
      time: new Date(),
      filename: result.filename,
      updated_at: updatedAt
    };
  }
}

// Cron Jobs: DK local hours (UTC+2)
cron.schedule('0 4,8,12,16,20 * * *', updateVA); // 06, 10, 14, 18, 22 DK
cron.schedule('0 4,8,12,16,20 * * *', updateEsundhed);

// Endpoints
app.get('/', async (_, res) => {
  function toDK(date) {
    // Accept Date, ISO string, or timestamp. Return '—' when missing or invalid.
    if (!date) return '—';
    const parsed = (typeof date === 'string' || typeof date === 'number') ? new Date(date) : date;
    if (!(parsed instanceof Date) || Number.isNaN(parsed.getTime())) return '—';
    return new Date(parsed.getTime() + 2 * 60 * 60 * 1000).toLocaleString('da-DK');
  }

  // If in-memory values are empty (for example after a restart), read latest persisted rows
  try {
    if (!lastEsundhed.filename) {
      const { data: latest, error } = await supabase
        .from('esundhed_report')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (!error && latest) lastEsundhed = { time: latest.created_at, filename: latest.filename, updated_at: latest.updated_at };
    }

    if (!lastVA.time) {
      const { data: latestVa, error } = await supabase
        .from('va_report')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (!error && latestVa) lastVA = { time: latestVa.created_at, month: latestVa.month };
    }
  } catch (e) {
    console.error('[ui] fallback DB read failed:', e && e.message ? e.message : e);
  }

  res.send(`
    <html lang="en">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Status Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
      </head>
      <body class="bg-gray-100 text-gray-800 font-sans p-6">
        <div class="max-w-2xl mx-auto bg-white rounded-xl shadow-md p-6">
          <h1 class="text-2xl font-bold mb-4">✅ Universal Watcher Status</h1>
          <p class="mb-2">Service is <span class="font-semibold text-green-600">LIVE</span> and actively monitoring both VA & eSundhed reports.</p>

          <div class="mt-6">
            <h2 class="text-xl font-semibold mb-2">VA Report</h2>
            <p><strong>Last Check:</strong> ${toDK(lastVA.time)}</p>
            <p><strong>Latest Month:</strong> ${lastVA.month || '—'}</p>
          </div>

          <div class="mt-6">
            <h2 class="text-xl font-semibold mb-2">eSundhed Report</h2>
            <p><strong>Last Check:</strong> ${toDK(lastEsundhed.time)}</p>
            <p><strong>Latest File:</strong> ${lastEsundhed.filename || '—'}</p>
            <p><strong>Last Reported:</strong> ${toDK(lastEsundhed.updated_at)}</p>
          </div>

          <div class="mt-6 text-sm text-gray-500">Last refreshed at ${toDK(new Date())}</div>
        </div>
      </body>
    </html>
  `);
});

app.get('/ping', (_, res) => res.send('pong'));

app.get('/scrape/va', async (_, res) => {
  await updateVA();
  res.send('VA scrape complete!');
});

app.get('/scrape/esundhed', async (_, res) => {
  await updateEsundhed();
  res.send('eSundhed scrape complete!');
});

// Temporary endpoint to test email sending. Call with POST /test-email?to=you@domain.tld
app.post('/test-email', async (req, res) => {
  const nodemailer = require('nodemailer');
  const user = process.env.EMAIL_USER;
  const pass = process.env.EMAIL_PASS;
  const to = req.query.to || process.env.EMAIL_TO || user;

  if (!user || !pass) {
    console.error('[email-test] Missing EMAIL_USER or EMAIL_PASS');
    return res.status(400).send('Missing EMAIL_USER or EMAIL_PASS environment vars');
  }

  // Try SMTPS (465) first, then fallback to STARTTLS (587) if 465 times out
  const trySend = async (options) => {
    const t = nodemailer.createTransport(options);
    await t.verify();
    return t.sendMail({ from: user, to, subject: 'va-report-watcher test email', text: 'This is a test email sent from va-report-watcher' });
  };

  // Option 1: SMTPS on 465
  const opts465 = { host: 'smtp.gmail.com', port: 465, secure: true, auth: { user, pass } };
  // Option 2: STARTTLS on 587
  const opts587 = { host: 'smtp.gmail.com', port: 587, secure: false, requireTLS: true, auth: { user, pass } };

  try {
    const info = await trySend(opts465);
    console.log('[email-test] sent (465)', info && info.messageId ? info.messageId : info);
    return res.send('Email sent (465)');
  } catch (err465) {
    console.warn('[email-test] 465 failed:', err465 && err465.message ? err465.message : String(err465));
    // If 465 timed out or failed, try 587
    try {
      const info = await trySend(opts587);
      console.log('[email-test] sent (587)', info && info.messageId ? info.messageId : info);
      return res.send('Email sent (587)');
    } catch (err587) {
      console.error('[email-test] 587 failed:', err587 && err587.message ? err587.message : String(err587));
      // Return both errors so you can diagnose in the response/logs
      return res.status(500).send('465 error: ' + (err465 && err465.message ? err465.message : String(err465)) + ' | 587 error: ' + (err587 && err587.message ? err587.message : String(err587)));
    }
  }
});

app.listen(PORT, () => {
  console.log(`🌐 Server running on port ${PORT}`);
  updateVA();
  updateEsundhed();
});
