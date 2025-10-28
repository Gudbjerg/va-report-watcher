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

// Cron Jobs: run at 06:00,10:00,14:00,18:00,22:00 in Europe/Copenhagen timezone
// Use timezone-aware scheduling so times don't drift when the server is in UTC.
cron.schedule('0 6,10,14,18,22 * * *', updateVA, { timezone: 'Europe/Copenhagen' });
cron.schedule('0 6,10,14,18,22 * * *', updateEsundhed, { timezone: 'Europe/Copenhagen' });

// Endpoints
// Helper: render the dashboard HTML for a given project name
async function renderDashboard(project = 'Universal') {
  function toDK(date) {
    if (!date) return '—';
    const parsed = (typeof date === 'string' || typeof date === 'number') ? new Date(date) : date;
    if (!(parsed instanceof Date) || Number.isNaN(parsed.getTime())) return '—';
    try {
      return parsed.toLocaleString('da-DK', { timeZone: 'Europe/Copenhagen', timeZoneName: 'short' });
    } catch (e) {
      return new Date(parsed.getTime() + 2 * 60 * 60 * 1000).toLocaleString('da-DK');
    }
  }

  // Ensure in-memory state is populated from DB if empty
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
      if (!error && latestVa) lastVA = { time: latestVa.created_at, month: latestVa.month, updated_at: latestVa.updated_at };
    }
  } catch (e) {
    console.error('[ui] fallback DB read failed:', e && e.message ? e.message : e);
  }

  return `
    <html lang="en">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>${project} — Watcher Status</title>
        <script src="https://cdn.tailwindcss.com"></script>
      </head>
      <body class="bg-gray-50 text-gray-800 font-sans">
        <header class="bg-white shadow">
          <div class="max-w-4xl mx-auto px-6 py-4 flex items-center justify-between">
            <div class="flex items-center space-x-4">
              <div class="text-2xl font-bold text-slate-800">va-report-watcher</div>
              <nav class="hidden md:flex items-center space-x-3 text-sm text-slate-600">
                <a href="/" class="hover:text-slate-900">Home</a>
                <div class="relative">
                  <button id="reportsBtn" class="hover:text-slate-900">Reports ▾</button>
                  <div id="reportsMenu" class="hidden absolute mt-2 bg-white border rounded shadow-md py-1">
                    <a class="block px-4 py-2 text-sm hover:bg-gray-100" href="/project/va">VA</a>
                    <a class="block px-4 py-2 text-sm hover:bg-gray-100" href="/project/esundhed">eSundhed</a>
                    <a class="block px-4 py-2 text-sm hover:bg-gray-100" href="/project/other">Other</a>
                  </div>
                </div>
                <a href="/about" class="hover:text-slate-900">About</a>
              </nav>
            </div>
            <div class="flex items-center space-x-4">
              <a href="https://www.linkedin.com/" target="_blank" rel="noopener noreferrer" class="text-sm text-slate-600 hover:text-blue-600">LinkedIn</a>
              <button id="mobileMenuBtn" class="md:hidden text-slate-600">☰</button>
            </div>
          </div>
        </header>

        <main class="max-w-4xl mx-auto p-6">
          <div class="bg-white rounded-xl shadow p-6">
            <h1 class="text-2xl font-bold mb-2">✅ ${project} Status</h1>
            <p class="mb-4">Service is <span class="font-semibold text-green-600">LIVE</span> and actively monitoring reports.</p>

            <div class="grid md:grid-cols-2 gap-6">
              <div>
                <h2 class="text-lg font-semibold">VA Report</h2>
                <p class="mt-2"><strong>Last Check:</strong> ${toDK(lastVA.time)}</p>
                <p><strong>Latest Month:</strong> ${lastVA.month || '—'}</p>
                <p><strong>Last Reported:</strong> ${toDK(lastVA.updated_at)}</p>
              </div>

              <div>
                <h2 class="text-lg font-semibold">eSundhed Report</h2>
                <p class="mt-2"><strong>Last Check:</strong> ${toDK(lastEsundhed.time)}</p>
                <p><strong>Latest File:</strong> ${lastEsundhed.filename || '—'}</p>
                <p><strong>Last Reported:</strong> ${toDK(lastEsundhed.updated_at)}</p>
              </div>
            </div>

            <div class="mt-6 text-sm text-gray-500">Last refreshed at ${toDK(new Date())}</div>
          </div>
        </main>

        <script>
          // simple dropdown toggle
          document.addEventListener('DOMContentLoaded', function() {
            const btn = document.getElementById('reportsBtn');
            const menu = document.getElementById('reportsMenu');
            if (btn && menu) {
              btn.addEventListener('click', () => menu.classList.toggle('hidden'));
            }
            const mobile = document.getElementById('mobileMenuBtn');
            if (mobile) {
              mobile.addEventListener('click', () => alert('Mobile menu — coming soon'));
            }
          });
        </script>
      </body>
    </html>
  `;
}

// Root & project routes
app.get('/', async (_, res) => {
  res.send(await renderDashboard('Universal'));
});

app.get('/project/:name', async (req, res) => {
  const name = req.params.name || 'Project';
  res.send(await renderDashboard(name.charAt(0).toUpperCase() + name.slice(1)));
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
