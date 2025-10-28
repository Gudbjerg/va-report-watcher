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
let lastVA = { time: null, month: null, updated_at: null };
let lastEsundhed = { time: null, filename: null, updated_at: null };

// Patch watchers to update status
async function updateVA() {
  console.log(`[⏰] ${new Date().toISOString()} — Cron triggered: checkVA()`);
  const result = await checkVA();
  if (result?.month) {
    // Read persisted updated_at from Supabase (same pattern as eSundhed)
    try {
      const { data, error } = await supabase
        .from('va_report')
        .select('updated_at')
        .eq('id', 1)
        .single();

      const updatedAt = data?.updated_at ? new Date(data.updated_at) : null;

      lastVA = { time: new Date(), month: result.month, updated_at: updatedAt };
    } catch (e) {
      console.error('[ui] fallback VA DB read failed:', e && e.message ? e.message : e);
      // Fallback to in-memory values if DB read fails
      lastVA = { time: new Date(), month: result.month, updated_at: null };
    }
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
            <p><strong>Last Reported:</strong> ${toDK(lastVA.updated_at)}</p>
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
  const { sendMail } = require('./lib/sendEmail');
  const toQuery = req.query.to;

  // Global guard to prevent accidental sends from production while migrating providers
  if (process.env.DISABLE_EMAIL === 'true') {
    console.log('[email-test] DISABLE_EMAIL=true — rejecting test send');
    return res.status(503).send('Email sending is disabled (DISABLE_EMAIL=true)');
  }

  try {
    const to = toQuery || process.env.TO_EMAIL || process.env.EMAIL_USER;
    await sendMail({
      to,
      from: process.env.FROM_EMAIL || process.env.EMAIL_USER,
      subject: 'va-report-watcher test email',
      text: 'This is a test email sent from va-report-watcher'
    });
    return res.send('Email sent (queued)');
  } catch (err) {
    console.error('[email-test] failed:', err && err.message ? err.message : err);
    return res.status(500).send('Test email failed: ' + (err && err.message ? err.message : String(err)));
  }
});

app.listen(PORT, () => {
  console.log(`🌐 Server running on port ${PORT}`);
  updateVA();
  updateEsundhed();
});
