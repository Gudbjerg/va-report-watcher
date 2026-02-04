require('dotenv').config();
// Debug Supabase connectivity when DEBUG_SUPABASE=1 is set (safe for debug deploys)
if (process.env.DEBUG_SUPABASE === '1') {
  try {
    require('./debug/supabase-check');
  } catch (e) {
    console.warn('[debug] supabase-check not found:', e && e.message ? e.message : e);
  }
}

const { execFile } = require("child_process");

// Minimal runtime setup: create Express app, expose assets, and provide safe
// fallbacks for globals that other modules expect when running locally.
const express = require('express');
const fs = require('fs');
const path = require('path');
const PORT = process.env.PORT || 3000;
const app = express();
app.use(express.json());
app.use('/assets', express.static(path.join(__dirname, 'assets')));

// Simple in-memory run guard to prevent concurrent duplicate runs
const activeRuns = new Map(); // key: indexId, value: boolean
const lastRunTimes = new Map(); // key: indexId, value: timestamp ms
// Track active child processes to allow listing and cancellation
const activeProcesses = new Map(); // key: indexId, value: { cp, startedAt, args }
// Simple daily usage tracker (tokens/runs) with configurable limit
const FACTSET_DAILY_LIMIT = Number(process.env.FACTSET_DAILY_LIMIT || 40);
let usageState = { date: new Date().toISOString().slice(0, 10), total: 0, byIndex: new Map() };
function _resetUsageIfNewDay() {
  const today = new Date().toISOString().slice(0, 10);
  if (usageState.date !== today) { usageState = { date: today, total: 0, byIndex: new Map() }; }
}
function _incUsage(indexId) {
  _resetUsageIfNewDay();
  const id = String(indexId || '').toUpperCase() || 'KAXCAP';
  const prev = usageState.byIndex.get(id) || 0;
  usageState.byIndex.set(id, prev + 1);
  usageState.total += 1;
}
function _getUsage() {
  _resetUsageIfNewDay();
  const byIdx = {}; for (const [k, v] of usageState.byIndex.entries()) byIdx[k] = v;
  const remaining = Math.max(0, FACTSET_DAILY_LIMIT - usageState.total);
  return { date: usageState.date, limit: FACTSET_DAILY_LIMIT, usedTotal: usageState.total, remaining, byIndex: byIdx };
}
app.get('/api/usage', (req, res) => { try { return res.json(_getUsage()); } catch (e) { return res.status(500).json({ error: String(e && e.message || e) }); } });
// Month-to-date usage log (migrated to Supabase with file fallback)
const USAGE_LOG_FILE = path.join(__dirname, 'logs', 'api_usage.log');
function _logUsageRunFile(indexId) {
  try {
    const dir = path.dirname(USAGE_LOG_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const entry = JSON.stringify({ ts: new Date().toISOString(), indexId: String(indexId || '').toUpperCase() }) + "\n";
    fs.appendFileSync(USAGE_LOG_FILE, entry, 'utf8');
  } catch (e) { /* ignore */ }
}
async function _logUsageRunSupabase(indexId) {
  try {
    if (!supabase || typeof supabase.from !== 'function') return;
    const row = { index_id: String(indexId || '').toUpperCase(), created_at: new Date().toISOString() };
    // Ignore errors — this is best-effort logging
    await supabase.from('api_usage_log').insert([row]);
  } catch (e) { /* ignore */ }
}
function _getMonthUsageFile() {
  try {
    const limit = Number(process.env.FACTSET_MONTHLY_LIMIT || 1000);
    const monthKey = new Date().toISOString().slice(0, 7); // YYYY-MM
    if (!fs.existsSync(USAGE_LOG_FILE)) return { month: monthKey, limit, used: 0, remaining: limit };
    const txt = fs.readFileSync(USAGE_LOG_FILE, 'utf8');
    const lines = txt.split(/\r?\n/).filter(Boolean);
    let used = 0;
    for (const line of lines) {
      try { const j = JSON.parse(line); if (j && typeof j.ts === 'string' && j.ts.slice(0, 7) === monthKey) used += 1; } catch { }
    }
    const remaining = Math.max(0, limit - used);
    return { month: monthKey, limit, used, remaining };
  } catch (e) {
    return { month: new Date().toISOString().slice(0, 7), limit: Number(process.env.FACTSET_MONTHLY_LIMIT || 1000), used: null, remaining: null, error: String(e && e.message || e) };
  }
}
async function _getMonthUsageSupabase() {
  const limit = Number(process.env.FACTSET_MONTHLY_LIMIT || 1000);
  const now = new Date();
  const monthKey = now.toISOString().slice(0, 7);
  const monthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1, 0, 0, 0));
  const nextMonthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1, 0, 0, 0));
  try {
    if (!supabase || typeof supabase.from !== 'function') throw new Error('supabase not configured');
    // Count rows in current month window
    const { count, error } = await supabase
      .from('api_usage_log')
      .select('id', { count: 'exact', head: true })
      .gte('created_at', monthStart.toISOString())
      .lt('created_at', nextMonthStart.toISOString());
    if (error) throw error;
    const used = Number(count || 0);
    const remaining = Math.max(0, limit - used);
    return { month: monthKey, limit, used, remaining, source: 'supabase' };
  } catch (e) {
    const f = _getMonthUsageFile();
    return { ...f, source: 'file', error: (f.error || (e && e.message ? e.message : String(e))) };
  }
}
app.get('/api/usage/month', async (req, res) => { try { return res.json(await _getMonthUsageSupabase()); } catch (e) { return res.status(500).json({ error: String(e && e.message || e) }); } });

// Best-effort backfill: copy current-month entries from file log to Supabase
async function _backfillMonthUsageToSupabase() {
  try {
    if (!supabase || typeof supabase.from !== 'function') return;
    if (!fs.existsSync(USAGE_LOG_FILE)) return;
    const now = new Date();
    const monthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1, 0, 0, 0));
    const nextMonthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1, 0, 0, 0));
    const { count } = await supabase
      .from('api_usage_log')
      .select('id', { count: 'exact', head: true })
      .gte('created_at', monthStart.toISOString())
      .lt('created_at', nextMonthStart.toISOString());
    if (Number(count || 0) > 0) return; // already populated for this month
    const txt = fs.readFileSync(USAGE_LOG_FILE, 'utf8');
    const lines = txt.split(/\r?\n/).filter(Boolean).slice(-5000); // safety cap
    const rows = [];
    for (const line of lines) {
      try {
        const j = JSON.parse(line);
        if (!j || !j.ts) continue;
        const ts = new Date(j.ts);
        if (ts >= monthStart && ts < nextMonthStart) {
          rows.push({ index_id: String(j.indexId || '').toUpperCase(), created_at: new Date(j.ts).toISOString() });
        }
      } catch { }
    }
    if (rows.length > 0) {
      await supabase.from('api_usage_log').insert(rows);
      console.log('[usage] backfilled', rows.length, 'rows to Supabase for current month');
    }
  } catch (e) {
    console.warn('[usage] backfill skipped:', e && e.message ? e.message : e);
  }
}

// Scheduler log helper: ensure logs directory and append entries
function writeSchedulerLog(line) {
  try {
    const logsDir = path.join(__dirname, 'logs');
    if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir, { recursive: true });
    const file = path.join(logsDir, 'scheduler.log');
    const entry = `[${new Date().toISOString()}] ${line}\n`;
    fs.appendFileSync(file, entry, 'utf8');
  } catch (e) {
    // If logging fails, still print to console
    console.error('[scheduler] writeSchedulerLog failed:', e && e.message ? e.message : e);
  }
}
// (removed stray client-side DOM injection)


const { getLatestKaxcapStatus } = require('./projects/kaxcap-index/kaxcapStatus');

// Safe defaults so the server can start even if optional services (Supabase,
// scheduled watchers) are not configured in this environment.
let lastVA = { time: null, month: null, updated_at: null };
let lastEsundhed = { time: null, filename: null, updated_at: null };

// Use shared Supabase client (with stub fallback inside the module)
const supabase = require('./lib/supabaseClient');

// Discovered runtime functions (set by discoverWatchers)
let checkVAFn = null;
// (removed stray client-side DOM injection)
let checkEsundhedFn = null;
// Lightweight registry of discovered watchers for the dashboard
let discoveredWatchers = [];

// --- Issuer key + class helpers (server-side, used by grouping endpoints) ---
function _parseTicker(tk) {
  const t = String(tk || '').toUpperCase();
  if (!t) return { base: '', cls: null, exch: null };
  const preDash = t.split('-')[0];
  const m = preDash.match(/^(.*?)(?:\.([A-Z]))?$/);
  const base = (m && m[1]) ? m[1].replace(/\./g, '') : preDash.replace(/\./g, '');
  const cls = (m && m[2]) ? m[2] : null; // A..Z class
  const exch = (t.includes('-') ? t.split('-').slice(1).join('-') : null);
  return { base, cls, exch };
}
function _stripClassWords(s) {
  return String(s || '')
    .replace(/\b(?:CLASS|CLA|SER\.?|SERIES)\s+[A-Z](?![A-Z])/gi, '')
    // Remove patterns like "A share" / "B share"
    .replace(/\b([A-Z])\s*SHARE\b/gi, '')
    // Remove trailing single-letter class tokens like " Oyj A" or " (B)"
    .replace(/\s*\(([A-Z])\)\s*$/g, '')
    .replace(/\s+[A-Z]$/g, '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}
function _issuerKeyFromName(name) {
  let s = String(name || '').toUpperCase();
  s = s.replace(/\b(?:CLASS|CLA|SER\.?|SERIES)\s+[A-Z](?![A-Z])/g, '');
  s = s.replace(/\b(A\/S|AB|PLC|OYJ|OY|ASA|AS|SA|SE)\b/g, '');
  s = s.replace(/\s{2,}/g, ' ').trim();
  return s;
}
function _issuerKeyFromRow(r) {
  const nmRaw = r && (r.issuer || r.name);
  if (nmRaw) {
    const nm = _stripClassWords(nmRaw);
    return _issuerKeyFromName(nm).toUpperCase();
  }
  if (r && r.ticker) {
    const p = _parseTicker(r.ticker);
    if (p.base) return p.base.toUpperCase();
  }
  return '';
}
function _classFromRow(r) {
  const t = r && r.ticker ? String(r.ticker) : '';
  const p = _parseTicker(t);
  if (p.cls) return p.cls;
  const n = String(r && (r.name || r.issuer) || '').toUpperCase();
  const m = n.match(/\b(?:CLASS|CLA|SER\.?|SERIES)\s+([A-Z])(?![A-Z])/);
  return m && m[1] ? m[1] : null;
}

function discoverWatchers() {
  // Try project-first locations
  try {
    const vaPath = path.join(__dirname, 'projects', 'analyst-scraper', 'watchers', 'va.js');
    if (fs.existsSync(vaPath)) {
      const mod = require(vaPath);
      checkVAFn = mod.runWatcher || mod;
      console.log('[loader] loaded VA watcher from', vaPath);
      discoveredWatchers.push({ key: 'va', name: 'VA', path: vaPath, route: '/scrape/va' });
    }
  } catch (e) {
    console.warn('[loader] could not load project VA watcher:', e && e.message ? e.message : e);
  }

  try {
    // Try a couple of reasonable filenames in the project watcher folder so renames are smooth
    const watcherDir = path.join(__dirname, 'projects', 'analyst-scraper', 'watchers');
    const candidates = ['sundhedsdatabank.js', 'esundhed.js'];
    for (const fn of candidates) {
      const p = path.join(watcherDir, fn);
      if (fs.existsSync(p)) {
        const mod = require(p);
        checkEsundhedFn = mod.checkEsundhedUpdate || mod;
        console.log('[loader] loaded Sundhedsdatabank watcher from', p);
        discoveredWatchers.push({ key: 'sundhedsdatabank', name: 'Sundhedsdatabank', path: p, route: '/scrape/sundhedsdatabank' });
        break;
      }
    }
  } catch (e) {
    console.warn('[loader] could not load project Sundhedsdatabank watcher:', e && e.message ? e.message : e);
  }

  // Fallback to legacy shims if project files missing
  if (!checkVAFn) {
    try {
      const vaLegacy = require('./watchers/va');
      checkVAFn = vaLegacy.runWatcher || vaLegacy;
      console.log('[loader] falling back to ./watchers/va');
      discoveredWatchers.push({ key: 'va', name: 'VA (legacy)', path: path.join(__dirname, 'watchers', 'va.js'), route: '/scrape/va' });
    } catch (e) {
      console.warn('[loader] no VA legacy watcher available:', e && e.message ? e.message : e);
    }
  }

  // No legacy fallback for Sundhedsdatabank; use project watcher only
}
// Map index_id to per-index table name; fallback to shared table
function tableForIndex(indexId) {
  const id = String(indexId || '').trim().toUpperCase();
  if (id === 'KAXCAP') return 'index_constituents_kaxcap';
  if (id === (process.env.HEL_INDEX_ID || 'HELXCAP')) return 'index_constituents_helxcap';
  if (id === (process.env.STO_INDEX_ID || 'OMXSALLS')) return 'index_constituents_omxsalls';
  return 'index_constituents';
}
// Map index_id to per-index quarterly table name
function quarterlyTableForIndex(indexId) {
  const id = String(indexId || '').trim().toUpperCase();
  if (id === 'KAXCAP') return 'index_quarterly_kaxcap';
  if (id === (process.env.HEL_INDEX_ID || 'HELXCAP')) return 'index_quarterly_helxcap';
  if (id === (process.env.STO_INDEX_ID || 'OMXSALLS')) return 'index_quarterly_omxsalls';
  return null;
}

// Map index_id to per-index issuers table name
function issuersTableForIndex(indexId) {
  const id = String(indexId || '').trim().toUpperCase();
  if (id === 'KAXCAP') return 'index_issuers_kaxcap';
  if (id === (process.env.HEL_INDEX_ID || 'HELXCAP')) return 'index_issuers_helxcap';
  if (id === (process.env.STO_INDEX_ID || 'OMXSALLS')) return 'index_issuers_omxsalls';
  return null;
}

// Run discovery at startup so scheduled jobs have their handlers available
discoverWatchers();

// Patch watchers to update status (wrappers remain to keep routes/ui unchanged)
async function updateVA() {
  console.log(`[⏰] ${new Date().toISOString()} — Cron triggered: checkVA()`);
  if (!checkVAFn) {
    console.warn('[updateVA] no VA watcher function available');
    return;
  }
  const result = await checkVAFn();
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
  if (!checkEsundhedFn) {
    console.warn('[updateEsundhed] no eSundhed watcher function available');
    return;
  }
  const result = await checkEsundhedFn();

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
      updated_at: updatedAt,
      lastRun: { status: result.updated ? 'new' : 'no-change', message: result.updated ? 'New report saved' : 'No-change', time: new Date() }
    };
  } else {
    // No file found or error — record a last run outcome for visibility
    lastEsundhed = Object.assign(lastEsundhed || {}, { time: new Date(), lastRun: { status: 'no-link', message: 'No XLSX link found or error', time: new Date() } });
  }
}

// discoveredWatchers is populated by discoverWatchers()

// Shared head and header for consistent site layout
function renderHead(title) {
  return `
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>${title}</title>
        <link rel="icon" type="image/png" href="/assets/favicon-32.png" />
        <script src="https://cdn.tailwindcss.com"></script>
        <script>
          // If Tailwind CDN fails (network/CSP), load a minimal local fallback
          (function(){
            function tailwindMissing(){
              try { return !(window.tailwind && window.tailwind.config); } catch (e) { return true; }
            }
            if (tailwindMissing()) {
              var link = document.createElement('link');
              link.rel = 'stylesheet';
              link.href = '/assets/tailwind-fallback.css';
              document.head.appendChild(link);
            }
          })();
          // Mobile menu toggle (applies across pages using shared header)
          document.addEventListener('DOMContentLoaded', function(){
            try {
              var btn = document.getElementById('mobileMenuBtn');
              var menu = document.getElementById('mobileMenu');
              if (btn && menu) {
                btn.addEventListener('click', function(){
                  var hidden = menu.classList.contains('hidden');
                  menu.classList.toggle('hidden', !hidden);
                  btn.setAttribute('aria-expanded', hidden ? 'true' : 'false');
                });
              }
            } catch(e) {}
          });
        </script>
      `;
}

function renderHeader() {
  return `
        <header class="bg-white border-b">
          <div class="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
      <a href="/" class="flex items-center gap-3">
                <img src="/assets/MarketBuddyLogo.png" alt="MarketBuddy" class="h-12 md:h-16 lg:h-20 w-auto object-contain">
      </a>
            <nav class="hidden md:flex gap-8 items-center text-sm text-slate-600">
              <a href="/indexes" class="hover:text-slate-900">Indexes</a>
              <a href="/watchers" class="hover:text-slate-900">Watchers</a>
              <a href="/product/ai-analyst" class="hover:text-slate-900">AI Analyst</a>
                      <a href="/about" class="hover:text-slate-900">About</a>
                      <a href="https://www.linkedin.com/in/tobias-gudbjerg-59b893249/" target="_blank" rel="noopener" class="hover:text-slate-900 flex items-center gap-2">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512" class="w-4 h-4" fill="currentColor" aria-hidden="true"><path d="M100.28 448H7.4V148.9h92.88zm-46.44-340a53.66 53.66 0 1 1 53.66-53.66 53.66 53.66 0 0 1-53.66 53.66zM447.9 448h-92.68V302.4c0-34.7-.7-79.4-48.4-79.4-48.4 0-55.8 37.8-55.8 76.8V448h-92.7V148.9h89V196h1.3c12.4-23.5 42.6-48.4 87.7-48.4 93.8 0 111.2 61.8 111.2 142.3V448z"/></svg>
                        <span class="sr-only">LinkedIn</span>
                      </a>
              <a href="/contact" class="px-3 py-2 rounded bg-yellow-400 text-slate-900 font-semibold">Login</a>
            </nav>
            <button id="mobileMenuBtn" class="md:hidden text-slate-600" aria-expanded="false" aria-controls="mobileMenu" aria-label="Open menu">☰</button>
          </div>
          <div id="mobileMenu" class="md:hidden hidden border-t">
            <div class="max-w-6xl mx-auto px-6 py-3 grid gap-2 text-sm">
              <a href="/indexes" class="py-2">Indexes</a>
              <a href="/watchers" class="py-2">Watchers</a>
              <a href="/product/ai-analyst" class="py-2">AI Analyst</a>
              <a href="/about" class="py-2">About</a>
              <a href="/contact" class="py-2">Login</a>
            </div>
          </div>
        </header>
      `;
}

function renderFooter() {
  return `
      <footer class="bg-white border-t mt-12">
        <div class="max-w-6xl mx-auto px-6 py-4 text-sm text-slate-600 flex items-center justify-between">
          <div>MarketBuddy — internal ABG tool</div>
          <div class="text-xs text-slate-500">Not an official ABG Sundal Collier product — <a href="/legal" class="text-blue-600">legal</a></div>
          <div class="flex items-center gap-3">
            <a href="https://www.linkedin.com/in/tobias-gudbjerg-59b893249/" target="_blank" rel="noopener" class="text-slate-600 flex items-center gap-2">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512" class="w-4 h-4" fill="currentColor" aria-hidden="true"><path d="M100.28 448H7.4V148.9h92.88zm-46.44-340a53.66 53.66 0 1 1 53.66-53.66 53.66 53.66 0 0 1-53.66 53.66zM447.9 448h-92.68V302.4c0-34.7-.7-79.4-48.4-79.4-48.4 0-55.8 37.8-55.8 76.8V448h-92.7V148.9h89V196h1.3c12.4-23.5 42.6-48.4 87.7-48.4 93.8 0 111.2 61.8 111.2 142.3V448z"/></svg>
              <span>tobias-gudbjerg</span>
            </a>
          </div>
        </div>
      </footer>
    `;
}
// Server-side helpers and routes for index pages
// Helper: fetch latest snapshot rows for an index
async function fetchLatestIndexRows(indexId) {
  const table = tableForIndex(indexId);
  const sel = supabase.from(table).select('as_of').order('as_of', { ascending: false }).limit(1);
  const { data: dates, error: err1 } = await sel;
  if (err1) throw err1;
  const asOf = dates && dates[0] ? dates[0].as_of : null;
  if (!asOf) return [];
  const { data, error: err2 } = await supabase
    .from(table)
    .select('*')
    .eq('as_of', asOf)
    .order('capped_weight', { ascending: false });
  if (err2) throw err2;
  return Array.isArray(data) ? data : [];
}

// Helper: fetch index metadata (currency, aum) dynamically; use safe fallbacks
async function getIndexMeta(indexId) {
  const fallback = (id) => {
    const up = String(id || '').toUpperCase();
    if (up === 'KAXCAP') return { currency: 'DKK', aum: 110000000000 };
    if (up === (process.env.HEL_INDEX_ID || 'HELXCAP')) return { currency: 'EUR', aum: 22000000000 };
    if (up === (process.env.STO_INDEX_ID || 'OMXSALLS')) return { currency: 'SEK', aum: 450000000000 };
    return { currency: '', aum: null };
  };
  try {
    const { data, error } = await supabase
      .from('indexes')
      .select('name, currency, aum')
      .eq('name', String(indexId))
      .limit(1)
      .maybeSingle();
    if (error || !data) return fallback(indexId);
    return { currency: data.currency || fallback(indexId).currency, aum: (data.aum != null ? Number(data.aum) : fallback(indexId).aum) };
  } catch (e) {
    return fallback(indexId);
  }
}
function renderIndexTable(title, rows, columns) {
  const headers = columns.map((c) => '<th class="px-3 py-2 text-left text-xs font-semibold text-slate-600">' + c.label + '</th>').join('');
  const body = rows.map((r) => {
    const tds = columns.map((c) => {
      const raw = r[c.key];
      const val = c.format ? c.format(raw, r) : (raw != null ? raw : '—');
      return '<td class="px-3 py-2 text-sm text-slate-700">' + val + '</td>';
    }).join('');
    return '<tr class="border-t odd:bg-slate-50 hover:bg-slate-100">' + tds + '</tr>';
  }).join('');
  return [
    '<section class="max-w-6xl mx-auto px-6 py-8">',
    '<h2 class="text-xl font-bold mb-3">' + title + '</h2>',
    '<div class="bg-white rounded-xl shadow overflow-x-auto">',
    '<table class="min-w-full">',
    '<thead class="bg-slate-50"><tr>' + headers + '</tr></thead>',
    '<tbody>' + body + '</tbody>',
    '</table>',
    '</div>',
    '</section>'
  ].join('');
}

async function fetchIndexRows(indexId) {
  const table = tableForIndex(indexId);
  const { data, error } = await supabase
    .from(table)
    .select('*')
    .order('as_of', { ascending: false })
    .limit(5000);
  if (error) throw error;
  return data || [];
}

// Helper: fetch rows for an index at a specific as_of (exact match)
async function fetchIndexRowsByAsOf(indexId, asOf) {
  const table = tableForIndex(indexId);
  const { data, error } = await supabase
    .from(table)
    .select('*')
    .eq('as_of', asOf)
    .limit(5000);
  if (error) throw error;
  return data || [];
}

// Deduplicate rows by a key, keeping the row with the latest updated_at
function dedupeLatestBy(rows, key) {
  try {
    const map = new Map();
    for (const r of (rows || [])) {
      const k = String(r[key] ?? '').toUpperCase();
      if (!k) continue;
      const u = r.updated_at ? new Date(r.updated_at).getTime() : 0;
      const prev = map.get(k);
      if (!prev || u > prev.__u) {
        const copy = Object.assign({}, r);
        copy.__u = u;
        map.set(k, copy);
      }
    }
    return Array.from(map.values()).map(({ __u, ...rest }) => rest);
  } catch (e) {
    return rows || [];
  }
}

function rankBy(rows, key, desc = true) {
  return [...rows].sort((a, b) => {
    const av = Number(a[key] || 0);
    const bv = Number(b[key] || 0);
    return desc ? (bv - av) : (av - bv);
  });
}

app.get('/kaxcap', async (req, res) => { return res.redirect(302, '/index?idx=KAXCAP'); });

app.get('/hel', async (req, res) => { const idxId = process.env.HEL_INDEX_ID || 'HELXCAP'; return res.redirect(302, '/index?idx=' + encodeURIComponent(idxId)); });

app.get('/sto', async (req, res) => { const idxId = process.env.STO_INDEX_ID || 'OMXSALLS'; return res.redirect(302, '/index?idx=' + encodeURIComponent(idxId)); });

// Alias route for Indexes overview
app.get('/indexes', async (req, res) => { return res.redirect(302, '/index'); });

// Per-index Quarterly pages mirroring Excel-style proforma
function renderQuarterlyRows(rows, region) {
  const aumByRegion = { CPH: 110000000000, HEL: 22000000000, STO: 450000000000 };
  const aum = aumByRegion[region];
  return rows.map(r => {
    const mcapBn = (r.mcap_uncapped != null ? Number(r.mcap_uncapped) / 1e9 : (r.mcap != null ? Number(r.mcap) / 1e9 : null));
    const currU = (r.curr_weight_uncapped != null ? Number(r.curr_weight_uncapped) : (r.old_weight != null ? Number(r.old_weight) : null));
    const currC = (r.curr_weight_capped != null ? Number(r.curr_weight_capped) : null);
    const tgt = (r.weight != null ? Number(r.weight) : (r.new_weight != null ? Number(r.new_weight) : null));
    const deltaFrac = (r.delta_pct != null ? Number(r.delta_pct) : (currC != null && tgt != null ? (tgt - currC) : null));
    const deltaAmt = (aum && deltaFrac != null ? aum * deltaFrac : null);
    return Object.assign({}, r, {
      name_view: r.name || r.issuer || '',
      mcap_bn: mcapBn,
      curr_uncapped_pct: currU,
      curr_capped_pct: currC,
      target_pct: tgt,
      delta_pct_view: deltaFrac,
      delta_amt: deltaAmt,
    });
  });
}

function quarterlyColumns(ccy) {
  const fmtPct = v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '');
  return [
    { key: 'name_view', label: 'Name' },
    { key: 'mcap_bn', label: 'Mcap (bn)', format: v => (v != null ? Number(v).toFixed(2) : '') },
    { key: 'curr_uncapped_pct', label: 'Current (uncapped)', format: fmtPct },
    { key: 'curr_capped_pct', label: 'Current (capped)', format: fmtPct },
    { key: 'target_pct', label: 'Target', format: fmtPct },
    { key: 'delta_pct_view', label: 'Delta, %', format: v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '') },
    { key: 'delta_amt', label: 'Delta, ' + ccy, format: v => (v != null ? Math.round(Number(v)).toLocaleString('en-DK') : '') },
    { key: 'delta_vol', label: 'Delta Vol (shrs)', format: v => (v != null ? Math.round(Number(v)).toLocaleString('en-DK') : '') },
    { key: 'days_to_cover', label: 'Days to Cover', format: v => (v != null ? Number(v).toFixed(2) : '') },
    { key: 'flags', label: 'Flags' },
  ];
}

async function fetchQuarterlyLatest(indexId) {
  const tableName = quarterlyTableForIndex(indexId);
  if (!tableName) return { asOf: null, rows: [] };
  const { data: dates, error: err1 } = await supabase.from(tableName).select('as_of').order('as_of', { ascending: false }).limit(1);
  if (err1) throw err1;
  const asOf = dates && dates[0] ? dates[0].as_of : null;
  let rows = [];
  if (asOf) {
    const { data, error: err2 } = await supabase.from(tableName).select('*').eq('as_of', asOf).order('mcap_uncapped', { ascending: false });
    if (err2) throw err2;
    rows = Array.isArray(data) ? data : [];
  }
  return { asOf, rows };
}

app.get('/kaxcap/quarterly', async (req, res) => { return res.redirect(302, '/index?idx=KAXCAP'); });

app.get('/hel/quarterly', async (req, res) => { const idxId = process.env.HEL_INDEX_ID || 'HELXCAP'; return res.redirect(302, '/index?idx=' + encodeURIComponent(idxId)); });

app.get('/sto/quarterly', async (req, res) => { const idxId = process.env.STO_INDEX_ID || 'OMXSALLS'; return res.redirect(302, '/index?idx=' + encodeURIComponent(idxId)); });

// (removed duplicate early '/product/rebalancer' route; canonical route kept later)
// Helper: render the dashboard HTML for a given project name
async function renderDashboard(project = 'Universal') {
  // Ensure in-memory state is populated from DB if empty
  try {
    if (!lastEsundhed.filename) {
      const { data: latest, error } = await supabase
        .from('esundhed_report')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (!error && latest) lastEsundhed = { time: latest.created_at ? new Date(latest.created_at) : null, filename: latest.filename, updated_at: latest.updated_at ? new Date(latest.updated_at) : null };
    }

    if (!lastVA.time) {
      const { data: latestVa, error } = await supabase
        .from('va_report')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (!error && latestVa) lastVA = { time: latestVa.created_at ? new Date(latestVa.created_at) : null, month: latestVa.month, updated_at: latestVa.updated_at ? new Date(latestVa.updated_at) : null };
    }
  } catch (e) {
    console.error('[ui] fallback DB read failed:', e && e.message ? e.message : e);
  }

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

  // Render a single coherent HTML template. Client-side JS (below) will fetch and render memes dynamically
  return `<!doctype html>
  <html lang="en">
    <head>${renderHead('MarketBuddy — ' + project)}</head>
    <body class="bg-gray-50 text-gray-800 font-sans">
      ${renderHeader()}

  
        <div class="max-w-6xl mx-auto px-6 py-16">
          <div>
            <div class="mb-4 text-sm text-yellow-600 uppercase tracking-wide">Powered by ABG Sundal Collier</div>
            <h1 class="text-4xl md:text-5xl font-extrabold leading-tight mb-4 text-gray-900">Internal Index Analytics & Watchers</h1>
            <p class="text-gray-700 max-w-2xl mb-6">MarketBuddy is an internal ABG tool for equity index analytics and operational monitoring. It provides issuer-level grouping across share classes, quarterly proforma calculations with cap enforcement, and daily status tracking with deltas, flows, and days-to-cover. Designed for reliability and clarity for ABG sales and trading.</p>
            <div class="text-sm text-gray-600">Use the top navigation to open product pages.</div>
          </div>
        </div>

  <main class="max-w-4xl mx-auto p-6">
        <div class="bg-white rounded-xl shadow p-6">
          <h1 class="text-2xl font-bold mb-2">MarketBuddy — ABG internal</h1>
          <p class="mb-4 text-gray-600">Internal tool for ABG sales and trading: real-time watchers, index rebalancer proposals, and an AI analyst that provides fast, plain-language comments on rebalances and earnings deviations for quick distribution to the sales desk.</p>
          <div class="mt-4 flex items-center gap-4">
            <a href="/watchers" class="inline-block text-blue-600">Open Watchers →</a>
            <a href="/indexes" class="inline-block text-blue-600">Indexes →</a>
            <a href="/product/ai-analyst" class="inline-block text-blue-600">AI Analyst →</a>
          </div>
        </div>
      </main>

      <script>
        // Convert any elements with data-iso to Europe/Copenhagen timezone
        // and include a short timezone name (CET/CEST depending on date)
        document.querySelectorAll('[data-iso]').forEach(el => {
          const iso = el.getAttribute('data-iso');
          if (!iso) return;
          try {
            const d = new Date(iso);
            el.textContent = d.toLocaleString('da-DK', { timeZone: 'Europe/Copenhagen', timeZoneName: 'short' });
          } catch (e) {
            // leave server-rendered fallback
          }
        });

        // (meme section removed)
      </script>
    ${renderFooter()}
    </body>
  </html>`;
}

// end of cleaned renderDashboard

// Root & project routes
app.get('/', async (_, res) => {
  res.send(await renderDashboard('Universal'));
});

app.get('/project/:name', async (req, res) => {
  const name = req.params.name || 'Project';
  res.send(await renderDashboard(name.charAt(0).toUpperCase() + name.slice(1)));
});

app.get('/ping', (_, res) => res.send('pong'));

// Make the overview discoverable at root
app.get('/', (req, res) => res.redirect(302, '/index'));

app.get('/scrape/va', async (_, res) => {
  await updateVA();
  res.send('VA scrape complete!');
});

// New canonical route name
app.get('/scrape/sundhedsdatabank', async (_, res) => {
  await updateEsundhed();
  res.send('Sundhedsdatabank scrape complete!');
});
// Soft fallback: keep old route but redirect for compatibility
app.get('/scrape/esundhed', (req, res) => res.redirect(302, '/scrape/sundhedsdatabank'));

app.get('/api/kaxcap/status', getLatestKaxcapStatus);

// Trigger the KAXCAP FactSet Python worker manually
app.post('/api/kaxcap/run', (req, res) => {
  try {
    const scriptPath = path.join(__dirname, 'workers', 'indexes', 'main.py');
    let pythonCmd = process.env.PYTHON || 'python3';
    try {
      const venvPy = path.join(__dirname, '.venv', 'bin', process.platform === 'win32' ? 'python.exe' : 'python');
      if (fs.existsSync(venvPy)) pythonCmd = venvPy;
    } catch { }
    const args = [scriptPath];
    const { region, indexId, asOf, quarterly } = Object.assign({}, req.query, req.body);
    const idxKey = String(indexId || '').toUpperCase() || 'KAXCAP';
    const nowTs = Date.now();
    const lastTs = lastRunTimes.get(idxKey) || 0;
    // Throttle repeated triggers (60s) and prevent concurrent runs
    if (activeRuns.get(idxKey)) {
      return res.status(429).json({ ok: false, error: 'Run already in progress for ' + idxKey });
    }
    if (nowTs - lastTs < 60_000) {
      return res.status(429).json({ ok: false, error: 'Run throttled (try again in ' + Math.ceil((60_000 - (nowTs - lastTs)) / 1000) + 's)' });
    }
    // Quota check (tokens/runs) — block when no remaining
    const usage = _getUsage();
    if (usage.remaining <= 0) {
      return res.status(429).json({ ok: false, error: 'Quota exceeded — no tokens left today', usage });
    }
    if (region) { args.push('--region', String(region).toUpperCase()); }
    if (indexId) { args.push('--index-id', String(indexId)); }
    if (asOf) { args.push('--as-of', String(asOf)); }
    // Only run quarterly when explicitly requested via query/body
    if (String(quarterly).toLowerCase() === 'true' || String(quarterly) === '1') {
      args.push('--quarterly');
    }
    activeRuns.set(idxKey, true);
    lastRunTimes.set(idxKey, nowTs);
    _incUsage(idxKey);
    _logUsageRunFile(idxKey);
    _logUsageRunSupabase(idxKey); // fire-and-forget
    const childProc = execFile(pythonCmd, args, { env: process.env }, (error, stdout, stderr) => {
      activeRuns.delete(idxKey);
      // Try to read last saved rate snapshot from worker
      let rateSnap = null;
      try {
        const rf = path.join(__dirname, 'logs', 'api_rate.json');
        if (fs.existsSync(rf)) {
          const txt = fs.readFileSync(rf, 'utf8');
          rateSnap = JSON.parse(txt || '{}');
        }
      } catch (e) { /* ignore */ }
      if (error) {
        console.error('[kaxcap-run] error:', error);
        if (stderr) console.error('[kaxcap-run] stderr:', stderr);
        return res.status(500).json({ ok: false, error: String(error), stderr, usage: _getUsage(), rate: rateSnap });
      }
      if (stderr) console.warn('[kaxcap-run] stderr:', stderr);
      console.log('[kaxcap-run] stdout:', stdout);
      return res.json({ ok: true, stdout, startedAt: new Date(nowTs).toISOString(), usage: _getUsage(), rate: rateSnap });
    });
    // Track child process for listing/cancellation
    try {
      activeProcesses.set(idxKey, { cp: childProc, startedAt: new Date(nowTs).toISOString(), args });
      childProc.on('exit', (code, signal) => {
        activeProcesses.delete(idxKey);
        console.log('[kaxcap-run] process exit', { indexId: idxKey, code, signal });
      });
    } catch (e) {
      console.warn('[kaxcap-run] failed to track child process:', e && e.message ? e.message : e);
    }
  } catch (e) {
    return res.status(500).json({ ok: false, error: e && e.message ? e.message : String(e) });
  }
});

// API: list active runs and their process info
app.get('/api/runs/active', (req, res) => {
  try {
    const items = [];
    for (const [idx, info] of activeProcesses.entries()) {
      items.push({
        indexId: idx,
        pid: info && info.cp && info.cp.pid ? info.cp.pid : null,
        startedAt: info && info.startedAt ? info.startedAt : null,
        args: info && Array.isArray(info.args) ? info.args : []
      });
    }
    return res.json({ active: items, count: items.length });
  } catch (e) {
    return res.status(500).json({ error: e && e.message ? e.message : String(e) });
  }
});

// API: cancel a running worker for the given indexId (best-effort SIGTERM)
app.delete('/api/kaxcap/run', (req, res) => {
  try {
    const { indexId } = Object.assign({}, req.query, req.body);
    const idxKey = String(indexId || '').toUpperCase();
    if (!idxKey) return res.status(400).json({ ok: false, error: 'indexId required' });
    const info = activeProcesses.get(idxKey);
    if (!info || !info.cp) {
      return res.status(404).json({ ok: false, error: 'No active run for ' + idxKey });
    }
    const pid = info.cp.pid || null;
    let terminated = false;
    try {
      terminated = info.cp.kill('SIGTERM');
    } catch (e) {
      console.warn('[cancel] SIGTERM failed for', idxKey, e && e.message ? e.message : e);
    }
    // Fallback: force kill after 5s if still present
    setTimeout(() => {
      try {
        if (activeProcesses.has(idxKey)) {
          const inf = activeProcesses.get(idxKey);
          if (inf && inf.cp) {
            inf.cp.kill('SIGKILL');
          }
          activeProcesses.delete(idxKey);
          activeRuns.delete(idxKey);
          console.log('[cancel] SIGKILL applied for', idxKey);
        }
      } catch (e) {
        console.warn('[cancel] SIGKILL failed for', idxKey, e && e.message ? e.message : e);
      }
    }, 5000);
    // mark as not active immediately for UI responsiveness
    activeRuns.delete(idxKey);
    return res.json({ ok: true, indexId: idxKey, pid, terminatedRequested: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e && e.message ? e.message : String(e) });
  }
});

// API: index meta (currency, AUM) for overview page
app.get('/api/index/:indexId/meta', async (req, res) => {
  try {
    const indexId = String(req.params.indexId || '').toUpperCase();
    const meta = await getIndexMeta(indexId);
    return res.json({ indexId, currency: meta.currency || null, aum: meta.aum || null });
  } catch (e) {
    return res.status(500).json({ error: e && e.message ? e.message : String(e) });
  }
});

// API: latest constituents for a given index id
app.get('/api/index/:indexId/constituents', async (req, res) => {
  try {
    const indexId = req.params.indexId;
    const tableName = tableForIndex(indexId);
    // latest as_of (per-index tables do not have index_id)
    const sel = supabase.from(tableName).select('as_of').order('as_of', { ascending: false }).limit(1);
    const { data: dates, error: err1 } = await sel;
    if (err1) throw err1;
    const asOf = dates && dates[0] ? dates[0].as_of : null;
    let rows = [];
    if (asOf) {
      const { data, error: err2 } = await supabase
        .from(tableName)
        .select('*')
        .eq('as_of', asOf)
        .order('capped_weight', { ascending: false });
      if (err2) throw err2;
      const raw = Array.isArray(data) ? data : [];
      const lastUpdated = raw.reduce((mx, r) => {
        const t = r && r.updated_at ? new Date(r.updated_at).getTime() : 0;
        return t > mx ? t : mx;
      }, 0);
      // Dedupe by ticker, keep latest updated_at
      rows = dedupeLatestBy(raw, 'ticker');
      // Totals (uncapped from mcap; capped when available)
      const totals = (() => {
        try {
          const unc = (rows || []).reduce((s, r) => s + (Number(r.mcap || 0) || 0), 0);
          const cap = (rows || []).reduce((s, r) => s + (Number(r.mcap_capped || 0) || 0), 0);
          return { mcap_uncapped: unc, mcap_capped: (cap > 0 ? cap : null) };
        } catch { return { mcap_uncapped: null, mcap_capped: null }; }
      })();
      return res.json({ asOf, lastUpdated: (lastUpdated ? new Date(lastUpdated).toISOString() : null), rows, totals });
    }
    res.json({ asOf, lastUpdated: null, rows: [], totals: { mcap_uncapped: null, mcap_capped: null } });
  } catch (e) {
    res.status(500).json({ error: e && e.message ? e.message : String(e) });
  }
});

// API: grouped constituents by issuer with class children (server-side aggregation)
app.get('/api/index/:indexId/constituents_grouped', async (req, res) => {
  try {
    const indexId = req.params.indexId;
    const tableName = tableForIndex(indexId);
    const { data: dates, error: err1 } = await supabase
      .from(tableName)
      .select('as_of')
      .order('as_of', { ascending: false })
      .limit(1);
    if (err1) throw err1;
    const asOf = dates && dates[0] ? dates[0].as_of : null;
    if (!asOf) return res.json({ asOf: null, lastUpdated: null, rows: [] });
    const { data, error: err2 } = await supabase
      .from(tableName)
      .select('*')
      .eq('as_of', asOf);
    if (err2) throw err2;
    const raw = Array.isArray(data) ? data : [];
    // latest updated_at across the snapshot
    const lastUpdated = raw.reduce((mx, r) => {
      const t = r && r.updated_at ? new Date(r.updated_at).getTime() : 0;
      return t > mx ? t : mx;
    }, 0);
    // group by issuer key
    const groups = new Map();
    for (const r of raw) {
      const key = _issuerKeyFromRow(r);
      const g = groups.get(key) || { __key: key, rows: [] };
      g.rows.push(r);
      groups.set(key, g);
    }
    const rows = [];
    for (const g of groups.values()) {
      const ch = g.rows;
      // aggregate issuer-level sums per methodology (weights already reflect issuer distribution)
      const agg = ch.reduce((acc, r) => {
        acc.weight += Number(r.weight || 0);
        acc.capped_weight += Number(r.capped_weight || 0);
        // Prefer explicit current capped if present; else derive from proposed and delta
        const currCap = (typeof r.curr_weight_capped === 'number')
          ? Number(r.curr_weight_capped)
          : ((typeof r.capped_weight === 'number' && typeof r.delta_pct === 'number')
            ? (Number(r.capped_weight) - Number(r.delta_pct))
            : 0);
        acc.curr_capped += currCap;
        acc.curr_uncapped += Number(r.curr_weight_uncapped || 0);
        acc.mcap += Number(r.mcap || r.market_cap || 0);
        acc.delta_pct += Number(typeof r.delta_pct === 'number' ? r.delta_pct : 0);
        if (r.flags) acc.flags.push(String(r.flags));
        return acc;
      }, { weight: 0, capped_weight: 0, curr_capped: 0, curr_uncapped: 0, mcap: 0, delta_pct: 0, flags: [] });
      const first = ch[0] || {};
      const children = ch.map(r => ({ ...r, __class: _classFromRow(r) }));
      rows.push({
        ...first,
        name: _stripClassWords(first.name || first.issuer || first.ticker || ''),
        issuer: _stripClassWords(first.issuer || first.name || ''),
        weight: agg.weight,
        capped_weight: agg.capped_weight,
        curr_weight_capped: agg.curr_capped,
        curr_weight_uncapped: agg.curr_uncapped,
        mcap: agg.mcap,
        delta_pct: agg.delta_pct,
        flags: Array.from(new Set(agg.flags.filter(Boolean))).join('; '),
        __multi: children.length > 1,
        __children: children,
      });
    }
    // order by capped_weight desc for consistency
    rows.sort((a, b) => Number(b.capped_weight || 0) - Number(a.capped_weight || 0));
    // Totals across raw rows for this as_of
    const totals = (() => {
      try {
        const unc = (raw || []).reduce((s, r) => s + (Number(r.mcap || 0) || 0), 0);
        const cap = (raw || []).reduce((s, r) => s + (Number(r.mcap_capped || 0) || 0), 0);
        return { mcap_uncapped: unc, mcap_capped: (cap > 0 ? cap : null) };
      } catch { return { mcap_uncapped: null, mcap_capped: null }; }
    })();
    return res.json({ asOf, lastUpdated: (lastUpdated ? new Date(lastUpdated).toISOString() : null), rows, totals });
  } catch (e) {
    res.status(500).json({ error: e && e.message ? e.message : String(e) });
  }
});

// API: latest issuer-level snapshot for a given index id
app.get('/api/index/:indexId/issuers', async (req, res) => {
  try {
    const indexId = req.params.indexId;
    const tableName = issuersTableForIndex(indexId);
    if (!tableName) return res.json({ asOf: null, rows: [] });
    const { data: dates, error: err1 } = await supabase
      .from(tableName)
      .select('as_of')
      .order('as_of', { ascending: false })
      .limit(1);
    if (err1) throw err1;
    const asOf = dates && dates[0] ? dates[0].as_of : null;
    let rows = [];
    if (asOf) {
      const { data, error: err2 } = await supabase
        .from(tableName)
        .select('*')
        .eq('as_of', asOf)
        .order('mcap_uncapped', { ascending: false });
      if (err2) throw err2;
      const raw = Array.isArray(data) ? data : [];
      const lastUpdated = raw.reduce((mx, r) => {
        const t = r && r.updated_at ? new Date(r.updated_at).getTime() : 0;
        return t > mx ? t : mx;
      }, 0);
      rows = dedupeLatestBy(raw, 'issuer');
      return res.json({ asOf, lastUpdated: (lastUpdated ? new Date(lastUpdated).toISOString() : null), rows });
    }
    res.json({ asOf, lastUpdated: null, rows: [] });
  } catch (e) {
    res.status(500).json({ error: e && e.message ? e.message : String(e) });
  }
});

// API: latest quarterly proforma for a given index id
// Reads from a quarterly table if present; returns empty if not found.
app.get('/api/index/:indexId/quarterly', async (req, res) => {
  try {
    const indexId = req.params.indexId;
    const tableName = quarterlyTableForIndex(indexId);
    if (!tableName) return res.json({ asOf: null, rows: [] });
    const { data: dates, error: err1 } = await supabase
      .from(tableName)
      .select('as_of')
      .order('as_of', { ascending: false })
      .limit(1);
    if (err1) throw err1;
    const asOf = dates && dates[0] ? dates[0].as_of : null;
    let rows = [];
    if (asOf) {
      const { data, error: err2 } = await supabase
        .from(tableName)
        .select('*')
        .eq('as_of', asOf)
        // Order by uncapped mcap as per user spec for quarterly preview
        .order('mcap_uncapped', { ascending: false });
      if (err2) throw err2;
      // Map to API shape expected by UI without forcing DB schema changes
      const raw = (Array.isArray(data) ? data : []).map(r => ({
        ...r,
        old_weight: (r.curr_weight_capped ?? r.curr_weight_uncapped ?? null),
        new_weight: (typeof r.weight !== 'undefined' ? r.weight : null)
      }));
      const lastUpdated = raw.reduce((mx, r) => {
        const t = r && r.updated_at ? new Date(r.updated_at).getTime() : 0;
        return t > mx ? t : mx;
      }, 0);
      rows = dedupeLatestBy(raw, 'ticker');
      const totals = (() => {
        try {
          const unc = (rows || []).reduce((s, r) => s + (Number(r.mcap_uncapped || 0) || 0), 0);
          const cap = (rows || []).reduce((s, r) => s + (Number(r.mcap_capped || 0) || 0), 0);
          return { mcap_uncapped: unc, mcap_capped: (cap > 0 ? cap : null) };
        } catch { return { mcap_uncapped: null, mcap_capped: null }; }
      })();
      return res.json({ asOf, lastUpdated: (lastUpdated ? new Date(lastUpdated).toISOString() : null), rows, totals });
    }
    res.json({ asOf, lastUpdated: null, rows: [], totals: { mcap_uncapped: null, mcap_capped: null } });
  } catch (e) {
    res.status(500).json({ error: e && e.message ? e.message : String(e) });
  }
});

// API: issuer-level quarterly aggregation with class children
app.get('/api/index/:indexId/quarterly_grouped', async (req, res) => {
  try {
    const indexId = req.params.indexId;
    const tableName = quarterlyTableForIndex(indexId);
    if (!tableName) return res.json({ asOf: null, lastUpdated: null, rows: [] });

    // Get latest as_of (date) for quarterly
    const { data: dates, error: err1 } = await supabase
      .from(tableName)
      .select('as_of')
      .order('as_of', { ascending: false })
      .limit(1);
    if (err1) throw err1;
    const asOf = dates && dates[0] ? dates[0].as_of : null;
    if (!asOf) return res.json({ asOf: null, lastUpdated: null, rows: [] });

    // Determine a cutoff timestamp based on latest updated_at within this as_of (purely defensive)
    let qCutoff = null;
    try {
      const { data: qmax } = await supabase
        .from(tableName)
        .select('updated_at')
        .eq('as_of', asOf)
        .order('updated_at', { ascending: false })
        .limit(1);
      qCutoff = qmax && qmax[0] ? qmax[0].updated_at : null;
    } catch { }

    // Load quarterly rows (limit to as_of and <= cutoff if available)
    let qQuery = supabase.from(tableName).select('*').eq('as_of', asOf);
    if (qCutoff) qQuery = qQuery.lte('updated_at', qCutoff);
    const { data: qRowsRaw, error: err2 } = await qQuery;
    if (err2) throw err2;
    const qRows = Array.isArray(qRowsRaw) ? qRowsRaw : [];

    // Normalize quarterly rows: keep distinct current capped/uncapped and target
    const qNorm = qRows.map(r => ({
      ...r,
      curr_weight_uncapped: (typeof r.curr_weight_uncapped === 'number') ? Number(r.curr_weight_uncapped) : null,
      curr_weight_capped: (typeof r.curr_weight_capped === 'number') ? Number(r.curr_weight_capped) : (typeof r.old_weight === 'number' ? Number(r.old_weight) : null),
      new_weight: (typeof r.new_weight === 'number') ? Number(r.new_weight) : (typeof r.weight === 'number' ? Number(r.weight) : null),
      mcap_use: (typeof r.mcap_uncapped === 'number') ? Number(r.mcap_uncapped) : (typeof r.mcap === 'number' ? Number(r.mcap) : 0),
    }));

    // Try to enrich from Daily snapshot with the same as_of and not newer than quarterly cutoff
    const dailyTable = tableForIndex(indexId);
    let dailyRows = [];
    let enrichmentDailyAsOf = null;
    let enrichmentDailyUpdatedAt = null;
    let enrichmentMixed = false;
    try {
      let dQuery = supabase.from(dailyTable).select('*').eq('as_of', asOf);
      if (qCutoff) dQuery = dQuery.lte('updated_at', qCutoff);
      const resp = await dQuery;
      if (!resp.error && Array.isArray(resp.data) && resp.data.length > 0) {
        dailyRows = resp.data;
        enrichmentDailyAsOf = asOf;
        enrichmentDailyUpdatedAt = dailyRows.reduce((mx, r) => {
          const t = r && r.updated_at ? new Date(r.updated_at).getTime() : 0; return t > mx ? t : mx;
        }, 0);
      } else {
        // Fallback: latest Daily snapshot regardless of as_of (mark as mixed)
        const { data: dDates } = await supabase
          .from(dailyTable)
          .select('as_of')
          .order('as_of', { ascending: false })
          .limit(1);
        const dAsOf = dDates && dDates[0] ? dDates[0].as_of : null;
        if (dAsOf) {
          const { data: dRowsRaw } = await supabase.from(dailyTable).select('*').eq('as_of', dAsOf);
          dailyRows = Array.isArray(dRowsRaw) ? dRowsRaw : [];
          enrichmentDailyAsOf = dAsOf;
          enrichmentMixed = (asOf && dAsOf && dAsOf !== asOf);
          enrichmentDailyUpdatedAt = dailyRows.reduce((mx, r) => {
            const t = r && r.updated_at ? new Date(r.updated_at).getTime() : 0; return t > mx ? t : mx;
          }, 0);
        }
      }
    } catch { }

    // Build basic lookups from the chosen Daily slice
    const priceByTicker = new Map();
    const advByTicker = new Map(); // shares/day
    for (const dr of (dailyRows || [])) {
      const tk = String(dr.ticker || '').toUpperCase(); if (!tk) continue;
      const price = (dr.price != null) ? Number(dr.price) : (dr.FG_PRICE_NOW != null ? Number(dr.FG_PRICE_NOW) : (dr.FG_PRICE != null ? Number(dr.FG_PRICE) : null));
      const adv = (dr.avg_daily_volume != null) ? Number(dr.avg_daily_volume) : null;
      priceByTicker.set(tk, (price != null && !Number.isNaN(price) ? price : null));
      advByTicker.set(tk, (adv != null && !Number.isNaN(adv) ? adv : null));
    }

    // Attach price/adv to quarterly rows; do NOT compute flows here (client has AUM)
    const qEnriched = qNorm.map(r => {
      const tk = String(r.ticker || '').toUpperCase();
      return Object.assign({}, r, {
        price: (priceByTicker.get(tk) ?? null),
        avg_daily_volume: (advByTicker.get(tk) ?? null)
      });
    });

    // lastUpdated across quarterly slice
    const lastUpdated = qEnriched.reduce((mx, r) => {
      const t = r && r.updated_at ? new Date(r.updated_at).getTime() : 0; return t > mx ? t : mx;
    }, 0);

    // Group to issuer-level with children
    const groups = new Map();
    for (const r of qEnriched) {
      const key = _issuerKeyFromRow(r);
      const g = groups.get(key) || { __key: key, rows: [] };
      g.rows.push(r);
      groups.set(key, g);
    }
    const rows = [];
    for (const g of groups.values()) {
      const ch = g.rows;
      const agg = ch.reduce((acc, r) => {
        acc.curr_capped += Number(r.curr_weight_capped || 0);
        acc.curr_uncapped += Number(r.curr_weight_uncapped || 0);
        acc.proposed += Number(r.new_weight || 0);
        acc.mcap += Number(r.mcap_use || 0);
        if (r.flags) acc.flags.push(String(r.flags));
        return acc;
      }, { curr_capped: 0, curr_uncapped: 0, proposed: 0, mcap: 0, flags: [] });
      const delta_pct = (agg.proposed != null && agg.curr_capped != null) ? (agg.proposed - agg.curr_capped) : null;
      const first = ch[0] || {};
      const children = ch.map(r => ({ ...r, __class: _classFromRow(r) }));
      const multi = children.length > 1;
      rows.push({
        ...first,
        name: _stripClassWords(first.name || first.issuer || first.ticker || ''),
        issuer: _stripClassWords(first.issuer || first.name || ''),
        mcap_uncapped: agg.mcap,
        old_weight: (agg.curr_capped != null ? agg.curr_capped : null),
        curr_weight_uncapped: (agg.curr_uncapped != null ? agg.curr_uncapped : null),
        new_weight: (agg.proposed != null ? agg.proposed : null),
        delta_pct,
        flags: Array.from(new Set(agg.flags.filter(Boolean))).join('; '),
        __multi: multi,
        __children: children,
      });
    }
    rows.sort((a, b) => Number(b.mcap_uncapped || 0) - Number(a.mcap_uncapped || 0));

    // Totals for quarterly
    const totals = (() => {
      try {
        const unc = (qEnriched || []).reduce((s, r) => s + (Number(r.mcap_uncapped || r.mcap || 0) || 0), 0);
        const cap = (qEnriched || []).reduce((s, r) => s + (Number(r.mcap_capped || 0) || 0), 0);
        return { mcap_uncapped: unc, mcap_capped: (cap > 0 ? cap : null) };
      } catch { return { mcap_uncapped: null, mcap_capped: null }; }
    })();

    return res.json({
      asOf,
      lastUpdated: (lastUpdated ? new Date(lastUpdated).toISOString() : null),
      rows,
      enrichmentDailyAsOf,
      enrichmentDailyUpdatedAt: (enrichmentDailyUpdatedAt ? new Date(enrichmentDailyUpdatedAt).toISOString() : null),
      enrichmentMixed,
      quarterlyCutoff: qCutoff || null,
      totals
    });
  } catch (e) {
    res.status(500).json({ error: e && e.message ? e.message : String(e) });
  }
});
// Index page: tabs for KAXCAP/HEL/STO with two tables and refresh controls
app.get('/index', async (req, res) => {
  try {
    res.send(`
      <html>
        <head>${renderHead('Indexes')}</head>
        <body class="bg-gray-50 p-6">
          ${renderHeader()}
          <main class="max-w-6xl mx-auto p-6">
            <div class="bg-white rounded-xl shadow p-6">
              <h1 class="text-3xl font-bold mb-2">Index Overview</h1>
              <p class="text-sm text-slate-600 mb-3">Quarterly uses uncapped ranking to propose proforma weights; Daily shows current capped ranking and flags any 10%/40% rule breaches.</p>
              <div class="flex gap-3 mb-3 sticky top-0 bg-white/95 backdrop-blur supports-[backdrop-filter]:bg-white/80 z-10 py-2">
                <button class="px-3 py-2 border rounded" data-idx="KAXCAP">KAXCAP (CPH)</button>
                <button class="px-3 py-2 border rounded" data-idx="${process.env.HEL_INDEX_ID || 'HELXCAP'}">Helsinki</button>
                <button class="px-3 py-2 border rounded opacity-50 cursor-not-allowed" title="Paused">Stockholm (paused)</button>
                <span class="ml-auto"></span>
                <div class="flex items-center gap-3">
                  <span id="refreshTicker" class="text-xs text-slate-500" aria-live="polite">auto-refresh in 20:00</span>
                  <span id="quotaLeft" class="text-xs text-slate-500" aria-live="polite"></span>
                  <span id="factsetLeft" class="text-xs text-slate-500" aria-live="polite"></span>
                  <span id="monthLeft" class="text-xs text-slate-500" aria-live="polite"></span>
                  <button id="rateDetailsBtn" class="text-xs underline text-slate-600">Headers</button>
                  <a href="/status" class="text-xs underline text-slate-600">Status</a>
                  <label class="text-xs text-slate-600 flex items-center gap-1"><input id="pauseRefresh" type="checkbox" class="align-middle"> Pause</label>
                  
                  <button id="refreshBtn" class="px-3 py-2 bg-yellow-400 text-slate-900 rounded">Refresh Selected</button>
                </div>
              </div>
              <div id="summaryBar" class="flex flex-wrap items-center gap-2 mb-3 text-sm" aria-label="Index summary"></div>
              <div id="alertBar" class="hidden mb-3 p-3 rounded border border-red-200 bg-red-50 text-red-800 text-sm"></div>
              <div id="meta" class="text-sm text-slate-600 mb-2">Select an index to load data.</div>
              <div id="rateDetailsPanel" class="hidden mb-3 p-3 rounded border border-slate-200 bg-slate-50 text-xs text-slate-700"></div>

              <section class="mb-6">
                <h2 class="text-xl md:text-2xl font-bold mb-1">Quarterly Proforma</h2>
                <p class="text-xs text-slate-500 mb-2">Uncapped ranking → assign exception caps and 4.5% cap; deltas vs current capped, with AUM-derived flow and DTC.</p>
                <div id="quarterlyMeta" class="text-xs text-slate-500 mb-2"></div>
                <div id="quarterlyTable"></div>
              </section>

              <section>
                <h2 class="text-xl md:text-2xl font-bold mb-1">Daily Status <span id="dailyBadge" class="ml-2 inline-block px-2 py-0.5 rounded-full text-xs bg-slate-100 text-slate-700" aria-label="Warnings count">0</span></h2>
                <p class="text-xs text-slate-500 mb-2">Current capped ranking and daily rule tracking. Flags 10% exception breaches and 40% aggregate (>5%) with cut candidate at 4.5%.</p>
                <div id="dailyMeta" class="text-xs text-slate-500 mb-2"></div>
                <div id="dailyWarnings" class="mb-2"></div>
                <div id="dailyTable"></div>
              </section>

              <div class="mt-6"><a href="/" class="text-blue-600">← Back</a></div>
            </div>
          </main>
          ${renderFooter()}
          <script>
            let selected = (() => {
              try {
                const qp = new URLSearchParams(location.search);
                const raw = (qp.get('idx') || 'KAXCAP').toString();
                const up = raw.toUpperCase();
                const hel = '${process.env.HEL_INDEX_ID || 'HELXCAP'}'.toUpperCase();
                const sto = '${process.env.STO_INDEX_ID || 'OMXSALLS'}'.toUpperCase();
                const allowed = new Set(['KAXCAP', hel, sto]);
                return allowed.has(up) ? up : 'KAXCAP';
              } catch (e) { return 'KAXCAP'; }
            })();
            const btns = Array.from(document.querySelectorAll('button[data-idx]'));
            const refreshBtn = document.getElementById('refreshBtn');
            const pauseCb = document.getElementById('pauseRefresh');
            const refreshTicker = document.getElementById('refreshTicker');
            const currencyByRegion = { CPH: 'DKK', HEL: 'EUR', STO: 'SEK' };
            const aumByRegion = { CPH: 110000000000, HEL: 22000000000, STO: 450000000000 };
            let selectedMeta = { ccy: '', aum: null };
            let dailyRowsCache = [];
            let quarterlyRowsCache = [];
            let dailyLimit = 25;
            let quarterlyLimit = 15;
            let dailySort = { key: 'capped_weight', dir: 'desc' };
            let quarterlySort = { key: 'mcap', dir: 'desc' };
            let nextRefreshSec = 1200;
            // Make 'Headers' toggle work immediately; content will fill after loadUsage()
            (function(){
              const btn = document.getElementById('rateDetailsBtn');
              const panel = document.getElementById('rateDetailsPanel');
              if (btn && panel) {
                btn.addEventListener('click', () => {
                  const hidden = panel.classList.contains('hidden');
                  panel.classList.toggle('hidden', !hidden);
                  if (hidden && !panel.innerHTML.trim()) {
                    panel.innerHTML = '<div class="text-slate-600 mb-1">Rate headers snapshot</div><pre class="whitespace-pre-wrap">(no headers yet)</pre>';
                  }
                  // Best-effort: persist latest snapshot to Supabase when toggled
                  try { fetch('/api/rate/log', { method: 'POST' }); } catch(e) {}
                });
              }
            })();

            // Persist Pause preference to localStorage (default: unpaused)
            (function(){
              try {
                const saved = localStorage.getItem('pauseRefresh');
                if (pauseCb) {
                  pauseCb.checked = (saved === '1');
                  pauseCb.addEventListener('change', () => {
                    try { localStorage.setItem('pauseRefresh', pauseCb.checked ? '1' : '0'); } catch(e) {}
                  });
                }
              } catch(e) {}
            })();
            function regionFor(indexId){
              const id = String(indexId||'').toUpperCase();
              if(id==='KAXCAP' || id==='OMXCAPPGI') return 'CPH';
              if(id==='$HEL_INDEX$' || id==='HELXCAP') return 'HEL';
              if(id==='$STO_INDEX$' || id==='OMXSALLS') return 'STO';
              return 'CPH';
            }
            function sanitizeId(s){
              return String(s||'').replace(/[^a-z0-9]+/gi,'-').toLowerCase();
            }
            function badge(txt, color){
              const base = 'inline-block px-2 py-0.5 rounded-full text-xs';
              const c = color==='green' ? 'bg-green-100 text-green-800' : color==='red' ? 'bg-red-100 text-red-800' : 'bg-slate-100 text-slate-700';
              return '<span class="'+base+' '+c+'">'+txt+'</span>';
            }
            // --- Company-level grouping helpers (combine share classes A/B/etc.) ---
            function extractClasses(name){
              const s = String(name||'');
              const out = new Set();
              // Detect share-class markers like "Class A", "Cla B", "Series B", or "B share"
              const regexes = [/(?:^|\s)(?:class|cla|ser\.?|series)\s*([A-Z])(?![a-z])/ig, /\b([A-Z])\s*share\b/ig];
              for (const re of regexes){
                let m; while((m=re.exec(s))){ out.add(String(m[1]||'').toUpperCase()); }
              }
              return Array.from(out);
            }
            function stripClassDesignators(name){
              return String(name||'')
                .replace(/\b(?:class|cla|ser\.?|series)\s+[A-Z](?![a-z])/ig, '')
                .replace(/\s{2,}/g,' ').trim();
            }
            function companyKeyFrom(row){
              const nmName = row.name || row.issuer || '';
              if (nmName) {
                return stripClassDesignators(nmName).toLowerCase();
              }
              const tk = String(row.ticker || '');
              if (tk) {
                // Derive issuer key from ticker like MAERSK.A-CSE or NOVO.B-CSE → 'maersk' / 'novo'
                const preDash = tk.split('-')[0];
                const noClass = preDash.replace(/\.[A-Z](?=-|$)/, '').replace(/\.[A-Z]$/, '');
                return noClass.replace(/\./g,'').toLowerCase();
              }
              return stripClassDesignators(String(row.ticker || '')).toLowerCase();
            }
            function groupByCompanyDaily(rows){
              const map = new Map();
              for (const r of rows){
                const key = companyKeyFrom(r);
                const entry = map.get(key) || { __key:key, __rows:[], name:'', issuer:'', weight:0, capped_weight:0, mcap:0, market_cap:0, delta_pct:0, price:null, avg_daily_volume:null, flagsSet:new Set(), __classes:new Set(), ticker:null };
                entry.__rows.push(r);
                const nm = r.name || r.issuer || r.ticker || '';
                if (!entry.name) entry.name = stripClassDesignators(nm);
                if (!entry.issuer) entry.issuer = stripClassDesignators(r.issuer || nm);
                entry.weight += (Number(r.weight)||0);
                entry.capped_weight += (Number(r.capped_weight)||0);
                entry.mcap += (Number(r.mcap || r.market_cap || 0) || 0);
                entry.market_cap = entry.mcap;
                entry.delta_pct += (typeof r.delta_pct==='number'? Number(r.delta_pct):0);
                // Keep price/volume only if single-class; will blank later if multi
                if (entry.__rows.length===1){ entry.price = (r.price!=null? Number(r.price):null); entry.avg_daily_volume = (r.avg_daily_volume!=null? Number(r.avg_daily_volume):null); }
                if (r.flags) entry.flagsSet.add(String(r.flags));
                extractClasses(nm).forEach(c=>entry.__classes.add(c));
                // Also infer class from ticker patterns like MAERSK.A-CSE
                const tk = String(r.ticker||'');
                const m1 = tk.match(/\.([A-Z])(?=-|$)/);
                if (m1 && m1[1]) entry.__classes.add(String(m1[1]).toUpperCase());
                if (!entry.ticker) entry.ticker = r.ticker || null;
                map.set(key, entry);
              }
              const out = [];
              for (const e of map.values()){
                const multi = e.__rows.length>1 && e.__classes.size>0;
                const classes = Array.from(e.__classes).sort();
                const label = multi && classes.length>1 ? (e.name + ' ' + classes.join(' + ')) : e.name;
                out.push({
                  ...e.__rows[0],
                  name: label,
                  issuer: e.issuer,
                  weight: e.weight,
                  capped_weight: e.capped_weight,
                  mcap: e.mcap,
                  market_cap: e.market_cap,
                  delta_pct: e.delta_pct,
                  // DTC inputs blanked if multi-class
                  price: multi ? null : e.price,
                  avg_daily_volume: multi ? null : e.avg_daily_volume,
                  flags: (()=>{
                    const f = Array.from(e.flagsSet).filter(Boolean).join('; ');
                    // Derive 10% breach if aggregate exceeds 10%
                    const ten = (e.capped_weight>0.10) ? '10% breach' : '';
                    return [f, ten].filter(Boolean).join('; ');
                  })(),
                  __multi: multi
                });
              }
              return out;
            }
            function groupByCompanyQuarterly(rows){
              const map = new Map();
              for (const r of rows){
                const key = companyKeyFrom(r);
                const entry = map.get(key) || { __key:key, __rows:[], name:'', issuer:'', mcap_uncapped:null, mcap:null, old_weight:0, new_weight:0, delta_pct:0, delta_vol:null, days_to_cover:null, flagsSet:new Set(), __classes:new Set() };
                entry.__rows.push(r);
                const nm = r.name || r.issuer || r.ticker || '';
                if (!entry.name) entry.name = stripClassDesignators(nm);
                if (!entry.issuer) entry.issuer = stripClassDesignators(r.issuer || nm);
                // add mcap values only when present to avoid misleading 0.00
                if (r.mcap_uncapped != null && !Number.isNaN(Number(r.mcap_uncapped))) {
                  entry.mcap_uncapped = (entry.mcap_uncapped == null ? 0 : entry.mcap_uncapped) + Number(r.mcap_uncapped);
                } else if (r.mcap != null && !Number.isNaN(Number(r.mcap))) {
                  entry.mcap = (entry.mcap == null ? 0 : entry.mcap) + Number(r.mcap);
                }
                entry.old_weight += (typeof r.old_weight==='number'? Number(r.old_weight): (typeof r.curr_weight_capped==='number'? Number(r.curr_weight_capped): (typeof r.curr_weight_uncapped==='number'? Number(r.curr_weight_uncapped):0)));
                entry.new_weight += (typeof r.new_weight==='number'? Number(r.new_weight): (typeof r.weight==='number'? Number(r.weight):0));
                entry.delta_pct += (typeof r.delta_pct==='number'? Number(r.delta_pct):0);
                if (r.flags) entry.flagsSet.add(String(r.flags));
                extractClasses(nm).forEach(c=>entry.__classes.add(c));
                const tk = String(r.ticker||'');
                const m1 = tk.match(/\.([A-Z])(?=-|$)/);
                if (m1 && m1[1]) entry.__classes.add(String(m1[1]).toUpperCase());
                map.set(key, entry);
              }
              const out = [];
              for (const e of map.values()){
                const multi = e.__rows.length>1 && e.__classes.size>0;
                const classes = Array.from(e.__classes).sort();
                const label = multi && classes.length>1 ? (e.name + ' ' + classes.join(' + ')) : e.name;
                out.push({
                  ...e.__rows[0],
                  name: label,
                  issuer: e.issuer,
                  mcap_uncapped: e.mcap_uncapped,
                  mcap: e.mcap,
                  old_weight: e.old_weight,
                  new_weight: e.new_weight,
                  delta_pct: e.delta_pct,
                  delta_vol: multi ? null : (e.__rows[0].delta_vol!=null? Number(e.__rows[0].delta_vol): null),
                  days_to_cover: multi ? null : (e.__rows[0].days_to_cover!=null? Number(e.__rows[0].days_to_cover): null),
                  flags: Array.from(e.flagsSet).filter(Boolean).join('; '),
                  __multi: multi
                });
              }
              return out;
            }
            function fmtNum(n, decimals){
              return (n!=null && !Number.isNaN(n)) ? Number(n).toFixed(decimals) : '';
            }
            function sortRows(rows, getVal, dir){
              const out = rows.slice();
              out.sort((a,b)=>{
                const va = getVal(a), vb = getVal(b);
                if (va==null && vb==null) return 0;
                if (va==null) return 1;
                if (vb==null) return -1;
                if (va<vb) return dir==='asc'? -1: 1;
                if (va>vb) return dir==='asc'? 1: -1;
                return 0;
              });
              return out;
            }
            // Select index via buttons and highlight active
            function setActive(){
              btns.forEach(b=>{
                if (b.getAttribute('data-idx') === selected) {
                  b.classList.add('bg-blue-600','text-white');
                  b.classList.remove('bg-white');
                } else {
                  b.classList.remove('bg-blue-600','text-white');
                }
              });
            }
            btns.forEach(btn => btn.addEventListener('click', () => { selected = btn.getAttribute('data-idx'); setActive(); loadAll(); }));
            setActive();
            // Trigger Python worker refresh for selected
            refreshBtn?.addEventListener('click', async () => {
              try {
                const idx = selected;
                const region = regionFor(idx);
                // Always refresh both daily and quarterly on click
                refreshBtn.disabled = true; refreshBtn.textContent = 'Refreshing…';
                const r = await fetch('/api/kaxcap/run?region=' + encodeURIComponent(region) + '&indexId=' + encodeURIComponent(idx) + '&quarterly=1', { method: 'POST' });
                const json = await r.json();
                if (r.status === 429) {
                  const msg = (json && json.error) ? String(json.error) : 'Quota exceeded';
                  const ab = document.getElementById('alertBar'); if (ab) { ab.textContent = msg; ab.classList.remove('hidden'); }
                  document.getElementById('meta').textContent = 'Refresh failed: ' + msg;
                  await loadUsage();
                } else if (json.ok) {
                  document.getElementById('meta').textContent = 'Refresh started for ' + selected + ' at ' + (json.startedAt ? new Date(json.startedAt).toLocaleString('da-DK') : 'now') + ' — loading…';
                  const ab = document.getElementById('alertBar'); if (ab) ab.classList.add('hidden');
                } else {
                  document.getElementById('meta').textContent = 'Refresh failed: ' + (json.error || 'unknown');
                }
                setTimeout(async ()=>{ await loadAll(); refreshBtn.disabled=false; refreshBtn.textContent='Refresh Selected'; }, 1500);
              } catch (e) {
                document.getElementById('meta').textContent = 'Refresh failed: ' + (e && e.message ? e.message : e);
                refreshBtn.disabled=false; refreshBtn.textContent='Refresh Selected';
              }
            });

            async function loadAll() {
              document.getElementById('meta').textContent = 'Loading ' + selected + '…';
              await loadMeta();
              // Load Daily first so quarterly can reuse price/ADV/mcap lookups
              await loadDaily();
              await loadQuarterly();
              await loadUsage();
              document.getElementById('meta').textContent = 'Loaded ' + selected;
              nextRefreshSec = 1200;
            }
            async function loadUsage(){
              try {
                const r = await fetch('/api/usage'); const j = await r.json();
                if (j && typeof j.remaining !== 'undefined') {
                  const el = document.getElementById('quotaLeft'); if (el) el.textContent = 'quota left today: ' + j.remaining;
                }
              } catch(e) {}
              try {
                const r2 = await fetch('/api/rate'); const j2 = await r2.json();
                const snap = j2 && j2.snapshot; if (snap) {
                  const lim = (snap.limit!=null? Number(snap.limit): null);
                  const rem = (snap.remaining!=null? Number(snap.remaining): null);
                  const rst = snap.reset || '';
                  const el2 = document.getElementById('factsetLeft'); if (el2) {
                    el2.textContent = (rem!=null && lim!=null) ? ('FactSet remaining: ' + rem + '/' + lim) : 'FactSet remaining: n/a';
                  }
                  // Populate raw header details panel
                  const panel = document.getElementById('rateDetailsPanel');
                  if (panel) {
                    const meta = [];
                    if (j2 && j2.fileMtime) meta.push('file_mtime: ' + j2.fileMtime);
                    if (snap.window) meta.push('window: ' + snap.window);
                    if (snap.reset) meta.push('reset: ' + snap.reset);
                    const headerText = JSON.stringify(snap, null, 2);
                    panel.innerHTML = '<div class="mb-2 text-slate-600">' + (meta.join(' · ') || 'Rate headers snapshot') + '</div>' + '<pre class="whitespace-pre-wrap">' + headerText.replace(/[<&]/g, c=>({"<":"&lt;","&":"&amp;"}[c])) + '</pre>';
                  }
                }
              } catch(e) {}
              try {
                const r3 = await fetch('/api/usage/month'); const j3 = await r3.json();
                if (j3 && typeof j3.remaining !== 'undefined') {
                  const el3 = document.getElementById('monthLeft'); if (el3) el3.textContent = 'month left: ' + j3.remaining + ' / ' + j3.limit;
                }
              } catch(e) {}
            }

            async function loadMeta(){
              try {
                const r = await fetch('/api/index/' + selected + '/meta');
                const j = await r.json();
                if (j && (j.currency || j.aum)) {
                  selectedMeta = { ccy: j.currency || '', aum: (typeof j.aum === 'number' ? j.aum : null) };
                  return;
                }
              } catch(e) {}
              // Fallback to region defaults if API/meta not available
              const region = regionFor(selected);
              selectedMeta = { ccy: currencyByRegion[region] || '', aum: aumByRegion[region] || null };
            }

            async function loadQuarterly() {
              try {
                const r = await fetch('/api/index/' + selected + '/quarterly_grouped');
                const json = await r.json();
                const rows = json.rows || [];
                const region = regionFor(selected);
                const aum = (selectedMeta.aum != null ? selectedMeta.aum : (aumByRegion[region] || null));
                const ccy = selectedMeta.ccy || currencyByRegion[region] || '';
                const lastUpdQ = (json.lastUpdated ? new Date(json.lastUpdated).toLocaleString('da-DK') : 'unknown');
                const enrichNote = (json.enrichmentMixed && json.enrichmentDailyAsOf && json.asOf && json.enrichmentDailyAsOf !== json.asOf)
                  ? (' · Enriched with Daily as_of: ' + json.enrichmentDailyAsOf)
                  : '';
                const dailyUpdNote = (json.enrichmentDailyUpdatedAt ? (' · Daily updated: ' + new Date(json.enrichmentDailyUpdatedAt).toLocaleString('da-DK')) : '');
                const totUncBnQ = (json.totals && typeof json.totals.mcap_uncapped === 'number') ? (Number(json.totals.mcap_uncapped)/1e9) : null;
                const totCapBnQ = (json.totals && typeof json.totals.mcap_capped === 'number') ? (Number(json.totals.mcap_capped)/1e9) : null;
                document.getElementById('quarterlyMeta').textContent = 'As of: ' + (json.asOf || 'unknown') + ' · Updated: ' + lastUpdQ + ' · Rows: ' + rows.length + ' · AUM (' + (ccy || 'CCY') + '): ' + (aum ? aum.toLocaleString('en-DK') : 'n/a') + enrichNote + dailyUpdNote + ' · ' + '<a class="text-blue-600" href="/api/index/' + selected + '/quarterly_grouped">JSON</a>';
                // Append totals as a badge line below
                try {
                  const qMetaEl = document.getElementById('quarterlyMeta');
                  if (qMetaEl) {
                    const totTxt = ' · Total MCAP uncapped: ' + (totUncBnQ!=null ? totUncBnQ.toFixed(2)+' bn' : 'n/a') + ' · capped: ' + (totCapBnQ!=null ? totCapBnQ.toFixed(2)+' bn' : 'n/a');
                    qMetaEl.textContent = qMetaEl.textContent + totTxt;
                  }
                } catch(e) {}
                // If enrichmentMixed, show a small alert as well to avoid mistaken conclusions
                if (json.enrichmentMixed) {
                  const ab = document.getElementById('alertBar'); if (ab) {
                    ab.textContent = 'Note: Quarterly enrichment used Daily snapshot ' + (json.enrichmentDailyAsOf || '') + ' which differs from Quarterly as_of ' + (json.asOf || '') + '.';
                    ab.classList.remove('hidden');
                  }
                }
                quarterlyRowsCache = rows.slice();
                // Build lookups from daily cache
                // - ticker -> display name
                // - ticker -> ADV (millions)
                // - ticker -> Price (spot)
                // - companyKey -> Market Cap (bn) aggregated
                const nameByTicker = new Map();
                const advByTickerMillions = new Map();
                const priceByTicker = new Map();
                const mcapBnByCompanyKey = new Map();
                try {
                  (dailyRowsCache || []).forEach(dr => {
                    const tk = (dr.ticker || '').toString().toUpperCase();
                    if (!tk) return;
                    const nm = (dr.name || dr.issuer || dr.ticker || '').toString();
                    nameByTicker.set(tk, stripClassDesignators(nm));
                    const adv = (dr.avg_daily_volume != null ? Number(dr.avg_daily_volume) : null);
                    advByTickerMillions.set(tk, (adv != null ? (adv/1e6) : null));
                    if (dr.price != null && !Number.isNaN(Number(dr.price))) {
                      priceByTicker.set(tk, Number(dr.price));
                    }
                    const ck = companyKeyFrom(dr);
                    const mcapRaw = (dr.market_cap != null ? Number(dr.market_cap) : (dr.mcap != null ? Number(dr.mcap) : null));
                    if (mcapRaw != null && !Number.isNaN(mcapRaw)) {
                      mcapBnByCompanyKey.set(ck, (mcapRaw/1e9));
                    }
                  });
                } catch (e) {}
                function getQuarterlyVal(row){
                  const issuer = (row.issuer || row.ticker || '').toLowerCase();
                  const mcap = (row.mcap_uncapped!=null && Number(row.mcap_uncapped)>0 ? Number(row.mcap_uncapped)
                                : (row.mcap!=null && Number(row.mcap)>0 ? Number(row.mcap)
                                : (row.mcap_bn!=null && Number(row.mcap_bn)>0 ? Number(row.mcap_bn)*1e9 : null)));
                  const currW = (row.old_weight != null) ? Number(row.old_weight) : null;
                  const newW = (row.new_weight != null) ? Number(row.new_weight) : null;
                  const deltaFrac = (currW != null && newW != null) ? (newW - currW) : null;
                  const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
                  const tkUp = (row.ticker || '').toString().toUpperCase();
                  const priceVal = (!row.__multi ? (priceByTicker.get(tkUp) ?? (row.price!=null? Number(row.price): null)) : null);
                  if (quarterlySort.key==='issuer') return issuer;
                  if (quarterlySort.key==='mcap') return mcap;
                  if (quarterlySort.key==='price') return priceVal;
                  if (quarterlySort.key==='curr') return currW;
                  if (quarterlySort.key==='new') return newW;
                  if (quarterlySort.key==='delta_pct') return deltaFrac;
                  if (quarterlySort.key==='delta_amt') return deltaAmt;
                  if (quarterlySort.key==='delta_vol') return (row.delta_vol!=null? Number(row.delta_vol): null);
                  if (quarterlySort.key==='dtc') return (row.days_to_cover!=null? Number(row.days_to_cover): null);
                  return mcap;
                }
                const sorted = sortRows(quarterlyRowsCache, getQuarterlyVal, quarterlySort.dir);
                const top = sorted.slice(0, quarterlyLimit);
                const trs = top.map(r => {
                  // Prefer human-readable company name from Daily cache when quarterly payload lacks it
                  const tkUp = (r.ticker || '').toString().toUpperCase();
                  const mappedName = nameByTicker.get(tkUp);
                  const issuer = mappedName || (r.name || r.issuer || r.ticker || '');
                  const cKey = companyKeyFrom(r);
                  // Derive Market Cap (bn) robustly from quarterly payload; fallback to Daily map
                  const mcapBn = (() => {
                    const raw = (r.mcap_uncapped != null && Number(r.mcap_uncapped) > 0)
                      ? Number(r.mcap_uncapped)
                      : (r.mcap != null && Number(r.mcap) > 0 ? Number(r.mcap) : null);
                    if (raw != null) return raw / 1e9;
                    const fromMap = mcapBnByCompanyKey.get(cKey);
                    return fromMap != null ? Number(fromMap) : null;
                  })();
                  const price = (!r.__multi ? (priceByTicker.get(tkUp) ?? (r.price!=null? Number(r.price): null)) : null);
                  const currW = (typeof r.old_weight==='number') ? Number(r.old_weight) : null; // decimal fraction
                  const newW = (typeof r.new_weight==='number') ? Number(r.new_weight) : null; // decimal fraction
                  const deltaFrac = (currW != null && newW != null) ? (newW - currW) : null;
                  const currWPct = (currW != null) ? (currW * 100) : null;
                  const newWPct = (newW != null) ? (newW * 100) : null;
                  const deltaPct = (deltaFrac != null) ? (deltaFrac * 100) : null;
                  const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
                  const deltaVol = (r.__multi ? null : (r.delta_vol != null ? Number(r.delta_vol) : null));
                  // Recompute DTC with Daily ADV (millions) when available for better accuracy
                  const advM = advByTickerMillions.get(tkUp);
                  const dtc = (r.__multi ? null : (
                    (deltaVol != null && advM != null && advM > 0) ? (Math.abs(deltaVol) / advM) : (r.days_to_cover != null ? Number(r.days_to_cover) : null)
                  ));
                  const deltaClass = (deltaPct!=null && deltaPct>0) ? 'text-green-700' : (deltaPct!=null && deltaPct<0 ? 'text-red-700' : '');
                  const rowId = 'q-' + sanitizeId(issuer);
                  const toggle = (r.__multi ? '<button class="toggle-btn mr-2 text-xs px-1.5 py-0.5 border rounded bg-slate-50 hover:bg-slate-100 text-slate-600 border-slate-300" aria-expanded="false" data-toggle="'+rowId+'">▶</button>' : '');
                  // Correct the <tr> opening tag (was missing '>')
                  let html = '<tr class="border-b">'
                    + '<td class="px-3 py-2 text-sm sticky left-0 bg-white">' + toggle + issuer + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (mcapBn != null && mcapBn>0 ? mcapBn.toFixed(2) : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (price != null ? price.toFixed(2) : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right" title="' + (currWPct!=null? currWPct.toFixed(2)+'%':'') + '">' + (currWPct != null ? currWPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right" title="' + (newWPct!=null? newWPct.toFixed(2)+'%':'') + '">' + (newWPct != null ? newWPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right ' + deltaClass + '">' + (deltaPct != null ? deltaPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (deltaAmt != null ? Math.round(deltaAmt).toLocaleString('en-DK') : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (deltaVol != null ? deltaVol.toFixed(2) : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (dtc != null ? dtc.toFixed(2) : '') + '</td>'
                    + '</tr>';
                  if (r.__multi && Array.isArray(r.__children)) {
                    const childRows = r.__children.map(ch => {
                      const cTk = (ch.ticker || '').toString().toUpperCase();
                      const cName = (nameByTicker.get(cTk) || ch.name || ch.issuer || ch.ticker || '');
                      const cClass = ch.__class ? (' (' + ch.__class + ')') : '';
                      const cMcapBn = (() => {
                        const raw = (ch.mcap_uncapped != null && Number(ch.mcap_uncapped) > 0)
                          ? Number(ch.mcap_uncapped)
                          : (ch.mcap != null && Number(ch.mcap) > 0 ? Number(ch.mcap) : null);
                        return raw != null ? (raw / 1e9) : null;
                      })();
                      const cPrice = (priceByTicker.get(cTk) ?? (ch.price != null ? Number(ch.price) : null));
                      const cCurr = (typeof ch.curr_weight_capped==='number' ? Number(ch.curr_weight_capped) : (typeof ch.curr_weight_uncapped==='number' ? Number(ch.curr_weight_uncapped) : (typeof ch.old_weight==='number' ? Number(ch.old_weight) : null)));
                      const cNew = (typeof ch.weight==='number' ? Number(ch.weight) : (typeof ch.new_weight==='number' ? Number(ch.new_weight) : null));
                      const cDelta = (typeof ch.delta_pct==='number' ? Number(ch.delta_pct) : (cCurr!=null && cNew!=null ? (cNew - cCurr) : null));
                      const cDeltaAmt = (aum && cDelta != null ? aum * cDelta : null);
                      const cAdvM = advByTickerMillions.get(cTk);
                      const cVol = (ch.delta_vol != null ? Number(ch.delta_vol) : ((cDeltaAmt != null && cPrice != null) ? (cDeltaAmt/Number(cPrice)/1e6) : null));
                      const cDtc = (ch.days_to_cover != null ? Number(ch.days_to_cover) : ((cVol != null && cAdvM != null && cAdvM > 0) ? (Math.abs(cVol)/cAdvM) : null));
                      const cCurrPct = (cCurr!=null ? (cCurr*100).toFixed(2)+'%' : '');
                      const cNewPct = (cNew!=null ? (cNew*100).toFixed(2)+'%' : '');
                      const cDeltaPct = (cDelta!=null ? (cDelta*100).toFixed(2)+'%' : '');
                      // Correct the <tr> opening tag for child rows (was missing '>')
                      return '<tr class="border-b hidden child-row bg-slate-50 child-of-'+rowId+'">'
                        + '<td class="px-3 py-2 text-xs pl-8 border-l-2 border-slate-200">' + cName + cClass + '</td>'
                        + '<td class="px-3 py-2 text-xs text-right">' + (cMcapBn!=null ? cMcapBn.toFixed(2) : '') + '</td>'
                        + '<td class="px-3 py-2 text-xs text-right">' + (cPrice!=null ? Number(cPrice).toFixed(2) : '') + '</td>'
                        + '<td class="px-3 py-2 text-xs text-right">' + cCurrPct + '</td>'
                        + '<td class="px-3 py-2 text-xs text-right">' + cNewPct + '</td>'
                        + '<td class="px-3 py-2 text-xs text-right">' + cDeltaPct + '</td>'
                        + '<td class="px-3 py-2 text-xs text-right">' + (cDeltaAmt!=null ? Math.round(cDeltaAmt).toLocaleString('en-DK') : '') + '</td>'
                        + '<td class="px-3 py-2 text-xs text-right">' + (cVol!=null ? Number(cVol).toFixed(2) : '') + '</td>'
                        + '<td class="px-3 py-2 text-xs text-right">' + (cDtc!=null ? Number(cDtc).toFixed(2) : '') + '</td>'
                      + '</tr>';
                    }).join('');
                    html += childRows;
                  }
                  return html;
                }).join('');
                const header = '<tr>'
                  + '<th class="px-3 py-2 sticky left-0 bg-gray-100 cursor-pointer" data-qsort="issuer">Company Name</th>'
                  + '<th class="px-3 py-2 text-right cursor-pointer" data-qsort="mcap">Market Cap, bn</th>'
                  + '<th class="px-3 py-2 text-right cursor-pointer" data-qsort="price">Price</th>'
                  + '<th class="px-3 py-2 text-right cursor-pointer" data-qsort="curr">Current</th>'
                  + '<th class="px-3 py-2 text-right cursor-pointer" data-qsort="new">Proforma</th>'
                  + '<th class="px-3 py-2 text-right cursor-pointer" data-qsort="delta_pct">Delta, %</th>'
                  + '<th class="px-3 py-2 text-right cursor-pointer" data-qsort="delta_amt">Delta, ' + (ccy || 'Amt') + '</th>'
                  + '<th class="px-3 py-2 text-right cursor-pointer" data-qsort="delta_vol">Delta, Vol (millions)</th>'
                  + '<th class="px-3 py-2 text-right cursor-pointer" data-qsort="dtc">Days to Cover</th>'
                + '</tr>';
                const qControls = '<div class="flex items-center gap-2 mb-2">'
                  + '<button id="qTop15" class="px-2 py-1 border rounded text-xs">Top 15</button>'
                  + '<button id="qTop50" class="px-2 py-1 border rounded text-xs">Show 50</button>'
                  + '<button id="quarterlyCsv" class="px-2 py-1 border rounded text-xs">Export CSV</button>'
                  + '</div>';
                document.getElementById('quarterlyTable').innerHTML = qControls + '<div class="overflow-auto"><table class="w-full text-left"><thead class="bg-gray-100">' + header + '</thead><tbody>' + (trs || '<tr><td class="px-3 py-4 text-sm text-slate-500" colspan="9">No data.</td></tr>') + '</tbody></table></div>';
                // Quarterly control handlers
                document.getElementById('qTop15').onclick = ()=>{ quarterlyLimit=15; loadQuarterly(); };
                document.getElementById('qTop50').onclick = ()=>{ quarterlyLimit=50; loadQuarterly(); };
                document.getElementById('quarterlyCsv').onclick = ()=>{
                  try {
                    const header = ['issuer','mcap_bn','price','curr_weight_pct','proforma_weight_pct','delta_pct','delta_amt','delta_vol','dtc'];
                    const csv = [header.join(',')].concat(top.map(r=>{
                      const tkUp = (r.ticker || '').toString().toUpperCase();
                      const issuer = (nameByTicker.get(tkUp) || r.name || r.issuer || r.ticker || '');
                      const mcapBn = (r.mcap_bn != null) ? Number(r.mcap_bn) : (r.mcap != null ? (Number(r.mcap) / 1e9) : (r.mcap_uncapped != null ? (Number(r.mcap_uncapped) / 1e9) : ''));
                      const price = (!r.__multi ? (priceByTicker.get(tkUp) ?? (r.price!=null? Number(r.price): '')) : '');
                      const currW = (r.old_weight != null) ? Number(r.old_weight) : null;
                      const newW = (r.new_weight != null) ? Number(r.new_weight) : null;
                      const deltaFrac = (currW != null && newW != null) ? (newW - currW) : null;
                      const currWPct = (currW != null) ? (currW * 100).toFixed(2) : '';
                      const newWPct = (newW != null) ? (newW * 100).toFixed(2) : '';
                      const deltaPct = (deltaFrac != null) ? (deltaFrac * 100).toFixed(2) : '';
                      const deltaAmt = (aum && deltaFrac != null) ? Math.round(aum * deltaFrac) : '';
                      const deltaVol = (r.delta_vol != null) ? Number(r.delta_vol).toFixed(2) : '';
                      const advM = advByTickerMillions.get(tkUp);
                      const dtc = (advM!=null && advM>0 && r.delta_vol!=null) ? (Math.abs(Number(r.delta_vol))/advM).toFixed(2) : ((r.days_to_cover != null) ? Number(r.days_to_cover).toFixed(2) : '');
                      return [JSON.stringify(issuer), mcapBn, price, currWPct, newWPct, deltaPct, deltaAmt, deltaVol, dtc].join(',');
                    })).join('\\n');
                    const blob = new Blob([csv], {type:'text/csv'});
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a'); a.href=url; a.download=selected+'_quarterly.csv'; a.click(); URL.revokeObjectURL(url);
                  } catch(e){}
                };
                // Expand/collapse handlers (Quarterly)
                document.querySelectorAll('#quarterlyTable [data-toggle]').forEach(btn => {
                  btn.addEventListener('click', () => {
                    const id = btn.getAttribute('data-toggle');
                    const rows = document.querySelectorAll('.child-of-' + id);
                    const hidden = rows.length && rows[0].classList.contains('hidden');
                    rows.forEach(tr => tr.classList.toggle('hidden', !hidden));
                    btn.textContent = hidden ? '▼' : '▶';
                    btn.setAttribute('aria-expanded', hidden ? 'true' : 'false');
                  });
                });
                // Header click sorting (Quarterly)
                document.querySelectorAll('#quarterlyTable th[data-qsort]').forEach(th => {
                  th.addEventListener('click', () => {
                    const key = th.getAttribute('data-qsort');
                    if (quarterlySort.key === key) {
                      quarterlySort.dir = (quarterlySort.dir === 'asc' ? 'desc' : 'asc');
                    } else {
                      quarterlySort.key = key; quarterlySort.dir = 'desc';
                    }
                    loadQuarterly();
                  });
                });
              } catch (e) {
                document.getElementById('quarterlyMeta').textContent = 'Quarterly load failed';
                document.getElementById('quarterlyTable').innerHTML = '';
              }
            }

            async function loadDaily() {
              try {
                const r = await fetch('/api/index/' + selected + '/constituents_grouped');
                const json = await r.json();
                const rows = json.rows || [];
                const totalW = rows.reduce((s, rr) => s + (Number(rr.weight) || 0), 0);
                // Aggregate of current capped weights over 5% to monitor 40% rule proximity
                const sumOver5 = rows.reduce((acc, rr) => {
                  const currCap = (typeof rr.curr_weight_capped === 'number') ? Number(rr.curr_weight_capped)
                    : ((typeof rr.capped_weight === 'number' && typeof rr.delta_pct === 'number') ? (Number(rr.capped_weight) - Number(rr.delta_pct))
                      : (typeof rr.capped_weight === 'number' ? Number(rr.capped_weight)
                        : (typeof rr.weight === 'number' ? Number(rr.weight) : null)));
                  return (currCap != null && currCap > 0.05) ? (acc + currCap) : acc;
                }, 0);
                const headroom = (0.40 - (sumOver5 || 0));
                const lastUpdD = (json.lastUpdated ? new Date(json.lastUpdated).toLocaleString('da-DK') : 'unknown');
                const totUncBn = (json.totals && typeof json.totals.mcap_uncapped === 'number') ? (Number(json.totals.mcap_uncapped)/1e9) : null;
                const totCapBn = (json.totals && typeof json.totals.mcap_capped === 'number') ? (Number(json.totals.mcap_capped)/1e9) : null;
                document.getElementById('dailyMeta').textContent = 'As of: ' + (json.asOf || 'unknown')
                  + ' · Updated: ' + lastUpdD
                  + ' · Rows: ' + rows.length
                  + ' · Sum(weight): ' + (totalW ? totalW.toFixed(6) : 'n/a')
                  + ' · >5% sum: ' + ((sumOver5 || 0) * 100).toFixed(2) + '%'
                  + ' · Headroom to 40%: ' + (headroom * 100).toFixed(2) + '%'
                  + ' · Total MCAP uncapped: ' + (totUncBn!=null ? totUncBn.toFixed(2)+' bn' : 'n/a')
                  + ' · capped: ' + (totCapBn!=null ? totCapBn.toFixed(2)+' bn' : 'n/a')
                  + ' · ' + '<a class="text-blue-600" href="/api/index/' + selected + '/constituents_grouped">JSON</a>';
                const top = rows.slice(0, 25);
                const region = regionFor(selected);
                const aum = (selectedMeta.aum != null ? selectedMeta.aum : (aumByRegion[region] || null));
                const ccy = selectedMeta.ccy || currencyByRegion[region] || '';
                dailyRowsCache = rows.slice();
                const sorted = sortRows(dailyRowsCache, (r)=> {
                  const curr = (typeof r.curr_weight_capped === 'number') ? Number(r.curr_weight_capped)
                    : ((typeof r.capped_weight === 'number' && typeof r.delta_pct === 'number') ? (Number(r.capped_weight) - Number(r.delta_pct)) : (typeof r.capped_weight === 'number' ? Number(r.capped_weight) : (typeof r.weight === 'number' ? Number(r.weight) : 0)));
                  return curr;
                }, dailySort.dir);
                const topN = sorted.slice(0, dailyLimit);
                const trs = topN.map(r => {
                  const hasCapDiff = (typeof r.capped_weight === 'number' && typeof r.weight === 'number' && Math.abs(r.capped_weight - r.weight) > 1e-9);
                  const flags = (r.flags && String(r.flags).trim()) || '';
                  const rowClass = flags.includes('40% breach') ? 'bg-red-50' : (flags.includes('10% breach') ? 'bg-yellow-50' : '');
                  const mcapRaw = (r.market_cap != null ? Number(r.market_cap) : (r.mcap != null ? Number(r.mcap) : null));
                  const mcapBn = (mcapRaw != null) ? (mcapRaw / 1e9) : null;
                  // Current capped weight from row; Target = current + delta (only when flagged)
                  const currCap = (typeof r.curr_weight_capped === 'number') ? Number(r.curr_weight_capped)
                    : ((typeof r.capped_weight === 'number' && typeof r.delta_pct === 'number') ? (Number(r.capped_weight) - Number(r.delta_pct))
                        : (typeof r.capped_weight === 'number' ? Number(r.capped_weight)
                          : (typeof r.weight === 'number' ? Number(r.weight) : null)));
                  const targetCap = (currCap != null && typeof r.delta_pct === 'number') ? (currCap + Number(r.delta_pct)) : null;
                  const wPct = (currCap != null) ? (currCap * 100) : null; // display current capped
                  const cwPct = (targetCap != null) ? (targetCap * 100) : null; // display target only when flagged
                  const deltaFrac = (typeof r.delta_pct === 'number') ? Number(r.delta_pct) : null;
                  const deltaPct = (deltaFrac != null) ? (deltaFrac * 100) : null;
                  const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
                  const deltaVolShares = (!r.__multi && deltaAmt != null && r.price != null) ? (deltaAmt / Number(r.price)) : null;
                  const adv = (!r.__multi && r.avg_daily_volume != null ? Number(r.avg_daily_volume) : null);
                  const dtcRaw = (!r.__multi && deltaVolShares != null && adv != null && adv > 0) ? (Math.abs(deltaVolShares) / adv) : null;
                  const displayName = (r.name || r.issuer || r.ticker || '');
                  const id = 'row-' + sanitizeId(displayName);
                  const deltaClass = (deltaPct!=null && deltaPct>0) ? 'text-green-700' : (deltaPct!=null && deltaPct<0 ? 'text-red-700' : '');
                  const cutPill = flags.includes('40% breach') ? '<span class="ml-2 inline-block px-2 py-0.5 rounded-full text-xs bg-red-100 text-red-800">cut candidate</span>' : '';
                  const showCalcs = Boolean(flags);
                  const toggle = (r.__multi ? '<button class="toggle-btn mr-2 text-xs px-1.5 py-0.5 border rounded bg-slate-50 hover:bg-slate-100 text-slate-600 border-slate-300" aria-expanded="false" data-toggle="'+id+'">▶</button>' : '');
                  let html = (
                    '<tr id="' + id + '" class="border-b ' + rowClass + '">'
                    + '<td class="px-3 py-2 text-sm sticky left-0 bg-white">' + toggle + displayName + cutPill + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (mcapBn != null ? mcapBn.toFixed(2) : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (!r.__multi && r.price != null ? Number(r.price).toFixed(2) : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right" title="' + (wPct!=null? wPct.toFixed(2)+'%':'') + '">' + (wPct != null ? wPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right" title="' + (cwPct!=null? cwPct.toFixed(2)+'%':'') + '">' + (showCalcs && cwPct != null ? cwPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right ' + deltaClass + '">' + (showCalcs && deltaPct != null ? deltaPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (showCalcs && deltaAmt != null ? Math.round(deltaAmt).toLocaleString('en-DK') : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (showCalcs && dtcRaw != null ? dtcRaw.toFixed(2) : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm">' + (flags ? '<span class="text-red-700 font-semibold" title="' + flags + '">' + flags + '</span>' : '') + '</td>'
                    + '</tr>'
                  );
                  if (r.__multi && Array.isArray(r.__children)){
                    const childRows = r.__children.map(ch => {
                      const nm = (ch.name || ch.issuer || ch.ticker || '');
                      const cls = ch.__class ? (' ('+ch.__class+')') : '';
                      const cmcap = (ch.market_cap != null ? Number(ch.market_cap) : (ch.mcap != null ? Number(ch.mcap) : null));
                      const cmcapBn = (cmcap != null ? cmcap/1e9 : null);
                      const cW = (ch.curr_weight_capped != null ? Number(ch.curr_weight_capped) : (ch.capped_weight != null ? Number(ch.capped_weight) : (ch.weight != null ? Number(ch.weight) : null)));
                      // Only show Target/Delta when flagged at parent or child level
                      const childFlags = (ch.flags && String(ch.flags).trim()) || '';
                      const childShowCalcs = Boolean(childFlags || flags);
                      const cT = (childShowCalcs && typeof ch.delta_pct==='number' && cW!=null ? (cW + Number(ch.delta_pct)) : null);
                      const cWP = (cW!=null ? (cW*100).toFixed(2)+'%' : '');
                      const cTP = (cT!=null ? (cT*100).toFixed(2)+'%' : '');
                      const cDP = (childShowCalcs && typeof ch.delta_pct==='number' ? (Number(ch.delta_pct)*100).toFixed(2)+'%' : '');
                      const cDA = (childShowCalcs && typeof ch.delta_pct==='number' && aum ? Math.round(aum*Number(ch.delta_pct)).toLocaleString('en-DK') : '');
                      const cVol = (childShowCalcs && typeof ch.delta_pct==='number' && ch.price!=null ? (aum*Number(ch.delta_pct)/Number(ch.price)) : null);
                      const cDtc = (childShowCalcs && cVol!=null && ch.avg_daily_volume!=null && Number(ch.avg_daily_volume)>0 ? (Math.abs(cVol)/Number(ch.avg_daily_volume)).toFixed(2) : '');
                      return '<tr class="border-b hidden child-row bg-slate-50 child-of-'+id+'">'
                        + '<td class="px-3 py-2 text-xs pl-8 border-l-2 border-slate-200">'+nm+cls+'</td>'
                        + '<td class="px-3 py-2 text-xs text-right">'+(cmcapBn!=null? cmcapBn.toFixed(2): '')+'</td>'
                        + '<td class="px-3 py-2 text-xs text-right">'+(ch.price!=null? Number(ch.price).toFixed(2): '')+'</td>'
                        + '<td class="px-3 py-2 text-xs text-right">'+cWP+'</td>'
                        + '<td class="px-3 py-2 text-xs text-right">'+cTP+'</td>'
                        + '<td class="px-3 py-2 text-xs text-right">'+cDP+'</td>'
                        + '<td class="px-3 py-2 text-xs text-right">'+cDA+'</td>'
                        + '<td class="px-3 py-2 text-xs text-right">'+(cVol!=null? Number(cVol).toFixed(2): '')+'</td>'
                        + '<td class="px-3 py-2 text-xs text-right">'+cDtc+'</td>'
                      + '</tr>';
                    }).join('');
                    html += childRows;
                  }
                  return html;
                }).join('');
                // Warnings panel and badge
                try {
                  const flagged = rows.filter(r => r.flags && String(r.flags).trim());
                  document.getElementById('dailyBadge').textContent = (flagged.length || 0);
                  if (flagged.length > 0) {
                    const items = flagged.slice(0, 8).map(r => {
                      const name = (r.name || r.issuer || r.ticker || '');
                      const id = 'row-' + sanitizeId(name);
                      return '<li class="text-sm"><a href="#' + id + '" class="hover:underline"><span class="inline-block px-2 py-0.5 rounded bg-red-100 text-red-800 mr-2" aria-label="flag">' + (r.flags || '') + '</span>' + name + '</a></li>';
                    }).join('');
                    document.getElementById('dailyWarnings').innerHTML = '<div class="p-3 border border-red-200 rounded bg-red-50" aria-label="Warnings"><div class="text-sm font-semibold text-red-800 mb-1">Warnings: ' + flagged.length + ' breach' + (flagged.length !== 1 ? 'es' : '') + ' detected</div><ul class="space-y-1">' + items + '</ul></div>';
                  } else {
                    document.getElementById('dailyWarnings').innerHTML = '';
                  }
                } catch (e) {
                  document.getElementById('dailyWarnings').innerHTML = '';
                }
                // Summary bar
                try {
                  const summary = [];
                  summary.push(badge('As of: ' + (json.asOf || 'unknown')));
                  summary.push(badge('Updated: ' + (json.lastUpdated ? new Date(json.lastUpdated).toLocaleString('da-DK') : 'unknown')));
                  summary.push(badge('AUM ' + (ccy || '') + ': ' + (aum ? aum.toLocaleString('en-DK') : 'n/a')));
                  summary.push(badge('Rows: ' + rows.length));
                  document.getElementById('summaryBar').innerHTML = summary.join(' ');
                } catch (e) {}
                // Table with controls
                const controls = '<div class="flex items-center gap-2 mb-2"><button id="dailyTop25" class="px-2 py-1 border rounded text-xs">Top 25</button><button id="dailyTop100" class="px-2 py-1 border rounded text-xs">Show 100</button><button id="dailyCsv" class="px-2 py-1 border rounded text-xs">Export CSV</button></div>';
                document.getElementById('dailyTable').innerHTML = controls + '<div class="overflow-auto"><table class="w-full text-left"><thead class="bg-gray-100"><tr><th class="px-3 py-2 sticky left-0 bg-gray-100 cursor-pointer" data-dsort="issuer">Company Name</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="mcap">Market Cap, bn</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="price">Price</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="weight">Current (capped)</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="capped_weight">Target (capped)</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="delta_pct">Delta, %</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="delta_amt">Delta, ' + (ccy || 'Amt') + '</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="dtc">Days to Cover</th><th class="px-3 py-2">Flags</th></tr></thead><tbody>' + (trs || '<tr><td class="px-3 py-4 text-sm text-slate-500" colspan="9">No data.</td></tr>') + '</tbody></table></div>';
                // Expand/collapse handlers (Daily)
                document.querySelectorAll('#dailyTable [data-toggle]').forEach(btn => {
                  btn.addEventListener('click', () => {
                    const id = btn.getAttribute('data-toggle');
                    const rows = document.querySelectorAll('.child-of-' + id);
                    const hidden = rows.length && rows[0].classList.contains('hidden');
                    rows.forEach(tr => tr.classList.toggle('hidden', !hidden));
                    btn.textContent = hidden ? '▼' : '▶';
                    btn.setAttribute('aria-expanded', hidden ? 'true' : 'false');
                  });
                });
                // Header click sorting (Daily)
                function getDailyVal(row){
                  const issuer = (row.name || row.issuer || row.ticker || '').toLowerCase();
                  const mcap = (row.market_cap != null ? Number(row.market_cap) : (row.mcap != null ? Number(row.mcap) : null));
                  const price = (!row.__multi && row.price != null ? Number(row.price) : null);
                  const weight = (row.weight != null ? Number(row.weight) : null);
                  const cweight = (row.curr_weight_capped != null ? Number(row.curr_weight_capped) : (row.capped_weight != null ? Number(row.capped_weight) : null));
                  const deltaFrac = (typeof row.delta_pct === 'number') ? Number(row.delta_pct) : null;
                  const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
                  const dtcVal = (() => {
                    const deltaVolShares = (deltaAmt != null && row.price != null) ? (deltaAmt / Number(row.price)) : null;
                    return (deltaVolShares != null && row.avg_daily_volume != null) ? (Math.abs(deltaVolShares) / Number(row.avg_daily_volume)) : null;
                  })();
                  if (dailySort.key==='issuer') return issuer;
                  if (dailySort.key==='mcap') return mcap;
                  if (dailySort.key==='price') return price;
                  if (dailySort.key==='weight') return weight;
                  if (dailySort.key==='capped_weight') return cweight;
                  if (dailySort.key==='delta_pct') return deltaFrac;
                  if (dailySort.key==='delta_amt') return deltaAmt;
                  if (dailySort.key==='dtc') return dtcVal;
                  return cweight ?? weight ?? mcap;
                }
                document.querySelectorAll('#dailyTable th[data-dsort]').forEach(th => {
                  th.addEventListener('click', () => {
                    const key = th.getAttribute('data-dsort');
                    if (dailySort.key === key) {
                      dailySort.dir = (dailySort.dir === 'asc' ? 'desc' : 'asc');
                    } else {
                      dailySort.key = key; dailySort.dir = 'desc';
                    }
                    // re-sort and rebuild body only
                    const sorted = sortRows(dailyRowsCache, getDailyVal, dailySort.dir);
                    const topN2 = sorted.slice(0, dailyLimit);
                    const tbodyHtml = topN2.map(r => {
                      const hasCapDiff = (typeof r.capped_weight === 'number' && typeof r.weight === 'number' && Math.abs(r.capped_weight - r.weight) > 1e-9);
                      const flags = (r.flags && String(r.flags).trim()) || (hasCapDiff ? 'capped' : '');
                      const rowClass = flags.includes('40% breach') ? 'bg-red-50' : (flags.includes('10% breach') ? 'bg-yellow-50' : '');
                      const mcapRaw = (r.market_cap != null ? Number(r.market_cap) : (r.mcap != null ? Number(r.mcap) : null));
                      const mcapBn = (mcapRaw != null) ? (mcapRaw / 1e9) : null;
                      const currCap = (typeof r.curr_weight_capped === 'number') ? Number(r.curr_weight_capped)
                        : ((typeof r.capped_weight === 'number' && typeof r.delta_pct === 'number') ? (Number(r.capped_weight) - Number(r.delta_pct))
                            : (typeof r.capped_weight === 'number' ? Number(r.capped_weight) : (typeof r.weight === 'number' ? Number(r.weight) : null)));
                      const targetCap = (currCap != null && typeof r.delta_pct === 'number') ? (currCap + Number(r.delta_pct)) : null;
                      const wPct = (currCap != null) ? (currCap * 100) : null; // current capped
                      const cwPct = (targetCap != null) ? (targetCap * 100) : null; // target only when flagged
                      const deltaFrac = (typeof r.delta_pct === 'number') ? Number(r.delta_pct) : null;
                      const deltaPct = (deltaFrac != null) ? (deltaFrac * 100) : null;
                      const deltaAmt2 = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
                      const deltaVolShares = (!r.__multi && deltaAmt2 != null && r.price != null) ? (deltaAmt2 / Number(r.price)) : null;
                      const adv2 = (!r.__multi && r.avg_daily_volume != null ? Number(r.avg_daily_volume) : null);
                      const dtc2 = (!r.__multi && deltaVolShares != null && adv2 != null && adv2 > 0) ? (Math.abs(deltaVolShares) / adv2) : null;
                      const name = (r.name || r.issuer || r.ticker || '');
                      const id = 'row-' + sanitizeId(name);
                      const deltaClass = (deltaPct!=null && deltaPct>0) ? 'text-green-700' : (deltaPct!=null && deltaPct<0 ? 'text-red-700' : '');
                      const cutPill = flags.includes('40% breach') ? '<span class="ml-2 inline-block px-2 py-0.5 rounded-full text-xs bg-red-100 text-red-800">cut candidate</span>' : '';
                      const showCalcs = Boolean(flags);
                      return (
                        '<tr id="' + id + '" class="border-b ' + rowClass + '">'
                        + '<td class="px-3 py-2 text-sm sticky left-0 bg-white">' + (r.name || r.issuer || '') + cutPill + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right">' + (mcapBn != null ? mcapBn.toFixed(2) : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right">' + (!r.__multi && r.price != null ? Number(r.price).toFixed(2) : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right" title="' + (wPct!=null? wPct.toFixed(2)+'%':'') + '">' + (wPct != null ? wPct.toFixed(2) + '%' : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right" title="' + (cwPct!=null? cwPct.toFixed(2)+'%':'') + '">' + (showCalcs && cwPct != null ? cwPct.toFixed(2) + '%' : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right ' + deltaClass + '">' + (showCalcs && deltaPct != null ? deltaPct.toFixed(2) + '%' : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right">' + (showCalcs && deltaAmt2 != null ? Math.round(deltaAmt2).toLocaleString('en-DK') : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right">' + (showCalcs && dtc2 != null ? dtc2.toFixed(2) : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm">' + (flags ? '<span class="text-red-700 font-semibold" title="' + flags + '">' + flags + '</span>' : '') + '</td>'
                        + '</tr>'
                      );
                    }).join('');
                    const tbody = document.querySelector('#dailyTable table tbody');
                    if (tbody) tbody.innerHTML = tbodyHtml;
                  });
                });
                // Control handlers
                document.getElementById('dailyTop25').onclick = ()=>{ dailyLimit=25; loadDaily(); };
                document.getElementById('dailyTop100').onclick = ()=>{ dailyLimit=100; loadDaily(); };
                document.getElementById('dailyCsv').onclick = ()=>{
                  try {
                    const header = ['company_name','mcap_bn','weight_pct','capped_weight_pct','delta_pct','delta_amt','dtc'];
                    const csv = [header.join(',')].concat(topN.map(r=>{
                      const mcapBn = (r.mcap!=null? Number(r.mcap)/1e9 : (r.mcap_uncapped!=null? Number(r.mcap_uncapped)/1e9: ''));
                      const w = (r.weight!=null? (Number(r.weight)*100).toFixed(2): '');
                      const cw = (r.capped_weight!=null? (Number(r.capped_weight)*100).toFixed(2): '');
                      const d = (typeof r.delta_pct==='number'? (Number(r.delta_pct)*100).toFixed(2): '');
                      const da = (typeof r.delta_pct==='number' && aum? Math.round(aum*Number(r.delta_pct)).toString(): '');
                      const deltaVolShares = (!r.__multi && typeof r.delta_pct==='number' && r.price!=null? (aum*Number(r.delta_pct)/Number(r.price)): null);
                      const adv = (!r.__multi && r.avg_daily_volume!=null? Number(r.avg_daily_volume): null);
                      const dtcV = (!r.__multi && deltaVolShares!=null && adv!=null && adv>0? (Math.abs(deltaVolShares)/adv).toFixed(2): '');
                      return [JSON.stringify(r.name||r.issuer||r.ticker||''), mcapBn, w, cw, d, da, dtcV].join(',');
                    })).join('\\n');
                    const blob = new Blob([csv], {type:'text/csv'});
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a'); a.href=url; a.download=selected+'_daily.csv'; a.click(); URL.revokeObjectURL(url);
                  } catch(e){}
                };
              } catch (e) {
                document.getElementById('dailyMeta').textContent = 'Daily load failed';
                document.getElementById('dailyTable').innerHTML = '';
              }
            }

            // Only auto-refresh when market open for selected index
            function isMarketOpen(idx){
              const reg = regionFor(idx);
              const tz = reg==='HEL' ? 'Europe/Helsinki' : (reg==='STO' ? 'Europe/Stockholm' : 'Europe/Copenhagen');
              try {
                const now = new Date();
                const d = new Intl.DateTimeFormat('en-GB', { timeZone: tz, hour: '2-digit', minute: '2-digit', weekday: 'short' }).formatToParts(now);
                const parts = Object.fromEntries(d.map(p=>[p.type,p.value]));
                const hr = parseInt(parts.hour,10);
                const mn = parseInt(parts.minute,10);
                const wk = (parts.weekday||'').toLowerCase();
                const isWk = !['sat','sun'].includes(wk.slice(0,3));
                // Market windows per region (local time)
                const windows = { CPH: { openH: 8, openM: 0, closeH: 17, closeM: 30 }, HEL: { openH: 8, openM: 0, closeH: 17, closeM: 30 }, STO: { openH: 8, openM: 0, closeH: 17, closeM: 30 } };
                const w = windows[reg] || windows.CPH;
                const afterOpen = (hr>w.openH) || (hr===w.openH && mn>=w.openM);
                const beforeClose = (hr<w.closeH) || (hr===w.closeH && mn<w.closeM);
                return isWk && afterOpen && beforeClose;
              } catch { return true; }
            }
            setInterval(async () => {
              if (!isMarketOpen(selected)) { refreshTicker.textContent = 'market closed — no auto-refresh'; return; }
              if (pauseCb && pauseCb.checked) { refreshTicker.textContent = 'auto-refresh paused'; return; }
              nextRefreshSec = Math.max(0, nextRefreshSec - 1);
              const m = String(Math.floor(nextRefreshSec/60)).padStart(2,'0');
              const s = String(nextRefreshSec%60).padStart(2,'0');
              refreshTicker.textContent = 'auto-refresh in ' + m + ':' + s;
              if (nextRefreshSec === 0) {
                try {
                  const idx = selected; const region = regionFor(idx);
                  const r = await fetch('/api/kaxcap/run?region=' + encodeURIComponent(region) + '&indexId=' + encodeURIComponent(idx) + '&quarterly=1', { method: 'POST' });
                  const json = await r.json();
                  if (r.status === 429) {
                    const msg = (json && json.error) ? String(json.error) : 'Quota exceeded';
                    const ab = document.getElementById('alertBar'); if (ab) { ab.textContent = msg; ab.classList.remove('hidden'); }
                  } else if (json && json.ok) {
                    const ab = document.getElementById('alertBar'); if (ab) ab.classList.add('hidden');
                  }
                } catch (e) { /* ignore auto error */ }
                await loadAll(); nextRefreshSec = 1200;
              }
            }, 1000);

            loadAll();
          </script>
        </body>
      </html>
    `);
  } catch (e) {
    console.error('[ui] /index page failed:', e && e.message ? e.message : e);
    res.status(500).send('Failed to load index page');
  }
});

// Helper: render an index snapshot page for a given index_id
async function renderIndexSnapshot(res, indexId, pageTitle) {
  try {
    const tableName = tableForIndex(indexId);
    const { data: dates, error: err1 } = await supabase
      .from(tableName)
      .select('as_of')
      .order('as_of', { ascending: false })
      .limit(1);
    if (err1) throw err1;
    const asOf = dates && dates[0] ? dates[0].as_of : null;
    let rows = [];
    if (asOf) {
      const { data, error: err2 } = await supabase
        .from(tableName)
        .select('*')
        .eq('as_of', asOf)
        .order('capped_weight', { ascending: false });
      if (err2) throw err2;
      rows = Array.isArray(data) ? data : [];
    }

    const totalW = rows.reduce((s, r) => s + (Number(r.weight) || 0), 0);
    const tableRowsHtml = rows.slice(0, 1000).map(r => {
      const mcapRaw = (r.market_cap != null ? Number(r.market_cap) : (r.mcap != null ? Number(r.mcap) : null));
      const mcapBn = (mcapRaw != null) ? (mcapRaw / 1e9) : null;
      const wPct = (r.weight != null) ? (Number(r.weight) * 100) : null;
      const cwPct = (r.capped_weight != null) ? (Number(r.capped_weight) * 100) : null;
      const flags = (typeof r.capped_weight === 'number' && typeof r.weight === 'number' && Math.abs(r.capped_weight - r.weight) > 1e-9) ? 'capped' : '';
      return (
        '<tr class="border-b">'
        + '<td class="px-3 py-2 text-sm">' + (r.issuer || '') + '</td>'
        + '<td class="px-3 py-2 text-sm">' + (mcapBn != null ? mcapBn.toFixed(2) : '') + '</td>'
        + '<td class="px-3 py-2 text-sm">' + (wPct != null ? wPct.toFixed(2) + '%' : '') + '</td>'
        + '<td class="px-3 py-2 text-sm">' + (cwPct != null ? cwPct.toFixed(2) + '%' : '') + '</td>'
        + '<td class="px-3 py-2 text-sm">' + flags + '</td>'
        + '</tr>'
      );
    }).join('');

    res.send(`
<!doctype html>
<html>
  <head>${renderHead(pageTitle + ' — Latest Snapshot')}</head>
  <body class="bg-gray-50 p-6">
    ${renderHeader()}
    <main class="max-w-6xl mx-auto p-6">
      <div class="bg-white rounded-xl shadow p-6">
        <h1 class="text-2xl font-bold mb-2">${pageTitle} — Latest Snapshot</h1>
        <div class="text-sm text-slate-600 mb-4">
          As of: ${asOf || 'unknown'} · Rows: ${rows.length} · Sum(weight): ${totalW ? totalW.toFixed(6) : 'n/a'}
          · <a class="text-blue-600" href="/api/index/${indexId}/constituents">JSON</a>
        </div>
        <div class="overflow-auto border rounded">
          <table class="w-full text-left">
            <thead class="bg-gray-100">
              <tr>
                <th class="px-3 py-2">Issuer</th>
                <th class="px-3 py-2">Market Cap, bn</th>
                <th class="px-3 py-2">Weight</th>
                <th class="px-3 py-2">Capped Weight</th>
                <th class="px-3 py-2">Flags</th>
              </tr>
            </thead>
            <tbody>${tableRowsHtml || '<tr><td class="px-3 py-4 text-sm text-slate-500" colspan="5">No data yet.</td></tr>'}</tbody>
          </table>
        </div>
        <div class="mt-6"><a href="/product/rebalancer" class="text-blue-600">Open Rebalancer</a></div>
      </div>
    </main>
    ${renderFooter()}
  </body>
</html>`);
  } catch (e) {
    console.error('[ui] renderIndexSnapshot failed:', e && e.message ? e.message : e);
    res.status(500).send('Failed to load index page');
  }
}

// Region snapshot routes removed to avoid overriding daily Top 25 pages

// Provide a product URL alias that points to the same KAXCAP page
app.get('/product/kaxcap', (req, res) => res.redirect(302, '/index?idx=KAXCAP'));

// Watchers index: lists discovered watchers and links to per-watcher pages
app.get('/watchers', async (req, res) => {
  function toDK(date) {
    if (!date) return '—';
    const parsed = (typeof date === 'string' || typeof date === 'number') ? new Date(date) : date;
    if (!(parsed instanceof Date) || Number.isNaN(parsed.getTime())) return '—';
    try { return parsed.toLocaleString('da-DK', { timeZone: 'Europe/Copenhagen', timeZoneName: 'short' }); } catch (e) { return parsed.toString(); }
  }

  // Small helper to render a human-friendly relative time (server-side).
  // Outputs strings like "just now", "5m ago", "3h ago", "2d ago" or '—'.
  function formatRelative(date) {
    if (!date) return '—';
    const parsed = (typeof date === 'string' || typeof date === 'number') ? new Date(date) : date;
    if (!(parsed instanceof Date) || Number.isNaN(parsed.getTime())) return '—';
    const now = new Date();
    const diff = Math.floor((now.getTime() - parsed.getTime()) / 1000); // seconds
    if (diff < 0) return 'just now';
    if (diff < 60) return 'just now';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
    if (diff < 7 * 86400) return Math.floor(diff / 86400) + 'd ago';
    // older than a week: show localized short date
    try { return parsed.toLocaleDateString('da-DK', { timeZone: 'Europe/Copenhagen' }); } catch (e) { return parsed.toDateString(); }
  }

  const rows = discoveredWatchers.map(w => {
    const last_iso = (w.key === 'va') ? (lastVA.time ? lastVA.time.toISOString() : '') : (lastEsundhed.time ? lastEsundhed.time.toISOString() : '');
    const reported_iso = (w.key === 'va') ? (lastVA.updated_at ? lastVA.updated_at.toISOString() : '') : (lastEsundhed.updated_at ? lastEsundhed.updated_at.toISOString() : '');
    // derive a simple status: 'New report' when reported_iso > last_iso
    let status = 'Up to date';
    let statusClass = 'bg-slate-100 text-slate-700';
    try {
      if (reported_iso && last_iso && reported_iso > last_iso) {
        status = 'New report';
        statusClass = 'bg-yellow-100 text-yellow-800';
      } else if (!last_iso) {
        status = 'No check';
        statusClass = 'bg-gray-100 text-slate-500';
      }
    } catch (e) { }

    const last_run_message = (w.key === 'va') ? (lastVA.lastRun ? lastVA.lastRun.message : '') : (lastEsundhed.lastRun ? lastEsundhed.lastRun.message : '');

    return {
      key: w.key,
      name: w.name,
      route: w.route,
      path: w.path,
      last_iso,
      reported_iso,
      last_check: toDK(last_iso),
      last_check_relative: formatRelative(last_iso),
      last_reported: toDK(reported_iso),
      last_reported_relative: formatRelative(reported_iso),
      last_run_message,
      status,
      statusClass
    };
  });

  res.send(`
  <html><head>${renderHead('Watchers')}</head>
    <body class="bg-gray-50 p-6">
      ${renderHeader()}
      <main class="max-w-6xl mx-auto p-6">
        <div class="bg-white rounded-xl shadow p-6">
          <h1 class="text-3xl md:text-4xl font-bold mb-4">Watchers</h1>
          <p class="text-sm text-gray-600 mb-2">List of discovered watchers. Click a watcher to view details or trigger a run.</p>
          <p class="text-sm text-gray-600 mb-4">Watchers run approximately every 3 hours. Times shown are Europe/Copenhagen (CET/CEST depending on daylight saving). To be added to the mailing list for a specific scraper or to request scraping of a website or dataset, please contact Tobias via <a href="https://www.linkedin.com/in/tobias-gudbjerg-59b893249/" target="_blank" rel="noopener" class="text-blue-600">LinkedIn</a>.</p>
          <div class="grid grid-cols-1 gap-4">
            ${rows.map(r => `
              <div class="p-3 border rounded flex items-center justify-between gap-4">
                <div class="flex items-center gap-4">
                  <div class="w-10 h-10 bg-slate-100 rounded flex items-center justify-center text-sm font-semibold text-slate-700">${r.name.split(' ').slice(0, 2).map(s => s[0] || '').join('')}</div>
                  <div>
                    <div class="flex items-center gap-3">
                      <a class="text-blue-600 font-semibold text-lg" href="/watcher/${r.key}">${r.name}</a>
                      <span class="px-2 py-0.5 rounded-full text-xs font-medium ${r.statusClass}">${r.status}</span>
                    </div>
                    <div class="text-xs text-slate-500 mt-1">
                      <div>Last check: <span title="${r.last_check}" data-iso="${r.last_iso || ''}">${r.last_check_relative || r.last_check}</span></div>
                      <div>Last reported: <span class="text-sm font-semibold text-slate-800">${r.last_reported_relative || r.last_reported}</span>
                        <span class="text-xs text-slate-400 ml-2" data-iso="${r.reported_iso || ''}">${r.last_reported}</span>
                      </div>
                      <div class="text-xs text-slate-400 mt-1">Last run: <span class="text-slate-700 font-medium">${r.last_run_message || '—'}</span></div>
                    </div>
                  </div>
                </div>
                <div class="mt-0">
                  <a class="inline-block bg-yellow-400 hover:bg-yellow-500 text-slate-900 px-4 py-2 rounded shadow-sm w-28 text-center" href="${r.route}">Run now</a>
                </div>
              </div>
            `).join('')}
          </div>
          <div class="mt-6"><a href="/" class="text-blue-600">← Back</a></div>
        </div>
      </main>

      <script>
        // Convert any elements with data-iso to Europe/Copenhagen timezone for consistency
        document.querySelectorAll('[data-iso]').forEach(el => {
          const iso = el.getAttribute('data-iso');
          if (!iso) return;
          try {
            const d = new Date(iso);
            el.textContent = d.toLocaleString('da-DK', { timeZone: 'Europe/Copenhagen', timeZoneName: 'short' });
          } catch (e) {
            // leave server-rendered fallback
          }
        });
      </script>

    </body></html>
  `);
});

// Per-watcher details and manual trigger
app.get('/watcher/:key', async (req, res) => {
  const key = req.params.key;
  const watcher = discoveredWatchers.find(w => w.key === key);
  if (!watcher) return res.status(404).send('Watcher not found');
  // compute ISO timestamps for client formatting
  const last_iso = (key === 'va') ? (lastVA.time ? lastVA.time.toISOString() : '') : (lastEsundhed.time ? lastEsundhed.time.toISOString() : '');
  const reported_iso = (key === 'va') ? (lastVA.updated_at ? lastVA.updated_at.toISOString() : '') : (lastEsundhed.updated_at ? lastEsundhed.updated_at.toISOString() : '');

  function formatDK(date) {
    if (!date) return '—';
    const parsed = (typeof date === 'string' || typeof date === 'number') ? new Date(date) : date;
    if (!(parsed instanceof Date) || Number.isNaN(parsed.getTime())) return '—';
    try { return parsed.toLocaleString('da-DK', { timeZone: 'Europe/Copenhagen', timeZoneName: 'short' }); } catch (e) { return '—'; }
  }

  res.send(`
  <html><head>${renderHead(watcher.name)}</head>
      <body class="bg-gray-50 p-6">
        ${renderHeader()}
        <main class="max-w-4xl mx-auto p-6">
          <div class="bg-white rounded-xl shadow p-6">
            <h1 class="text-3xl font-bold mb-2">${watcher.name}</h1>
            <p class="text-sm text-slate-600 mb-4">Path: <code class="bg-gray-100 px-2 py-1 rounded">${watcher.path}</code></p>
            <p class="mb-2"><strong>Last check:</strong> <span class="text-base font-medium text-slate-800" data-iso="${last_iso}">${formatDK(last_iso)}</span></p>
            <p class="mb-4"><strong>Last reported:</strong> <span class="text-base font-medium text-slate-800" data-iso="${reported_iso}">${formatDK(reported_iso)}</span></p>
            <p class="text-xs text-slate-500 mb-4">Times shown in Europe/Copenhagen (CET/CEST depending on date).</p>
            <div class="flex gap-3">
              <a class="inline-block bg-yellow-400 hover:bg-yellow-500 text-slate-900 px-4 py-2 rounded" href="${watcher.route}">Run now</a>
              <a class="inline-block border px-4 py-2 rounded" href="/watchers">Back to watchers</a>
            </div>
          </div>
        </main>

        <script>
          // convert data-iso timestamps on this page to Europe/Copenhagen for consistency
          document.querySelectorAll('[data-iso]').forEach(el => {
            const iso = el.getAttribute('data-iso');
            if (!iso) return;
            try {
              const d = new Date(iso);
              el.textContent = d.toLocaleString('da-DK', { timeZone: 'Europe/Copenhagen', timeZoneName: 'short' });
            } catch (e) {}
          });
        </script>

    </body></html>
  `);
});

// About page: simple bio / project description with same UI tone as Watchers
app.get('/about', async (req, res) => {
  const linkedin = 'https://www.linkedin.com/in/tobias-gudbjerg-59b893249/';
  res.send(`
    <html><head>${renderHead('About')}</head>
      <body class="bg-gray-50 p-6">
        ${renderHeader()}
        <main class="max-w-6xl mx-auto p-6">
          <div class="bg-white rounded-xl shadow p-6">
            <h1 class="text-2xl font-bold mb-4">About MarketBuddy</h1>
            <p class="text-sm text-gray-600 mb-4">MarketBuddy is an internal ABG tool that centralises real-time watchers, rebalancer proposals and quick AI analyst commentary. It's built to help ABG sales and trading teams spot and communicate index changes and earnings surprises faster.</p>
            <div class="mb-4 p-4 border-l-4 border-yellow-400 bg-yellow-50">
              <strong>Important:</strong>
              <p class="text-sm text-gray-700 mt-2">This repository and the software it contains are internal tooling and experimental work. They are <strong>not</strong> an official product or offering of ABG Sundal Collier. For the full legal disclaimer, see <a href="/legal" class="text-blue-600">the legal page</a>.</p>
            </div>
            <p class="text-sm text-gray-600 mb-4">Maintained by Tobias Gudbjerg. For access or questions, reach out to Tobias internally or via the LinkedIn profile linked below.</p>
            <div class="mt-4">
              <a href="/" class="ml-4 text-blue-600">← Back</a>
              <a href="${linkedin}" target="_blank" rel="noopener" class="inline-block bg-blue-600 text-white px-4 py-2 rounded">Contact (LinkedIn) →</a>
            </div>
          </div>
        </main>
      </body></html>
    `);
});

// Legal page: render LEGAL.md content simply for web reference
app.get('/legal', async (req, res) => {
  try {
    const txt = fs.readFileSync(path.join(__dirname, 'LEGAL.md'), 'utf8');
    // simple preformatted rendering so content is exact and visible
    res.send(`
      <html><head>${renderHead('Legal')}</head>
        <body class="bg-gray-50 p-6">
          ${renderHeader()}
          <main class="max-w-6xl mx-auto p-6">
            <div class="bg-white rounded-xl shadow p-6">
              <h1 class="text-2xl font-bold mb-4">Legal disclaimer</h1>
              <pre class="whitespace-pre-wrap text-sm text-slate-700 p-3 bg-gray-50 rounded">${txt.replace(/</g, '&lt;')}</pre>
              <div class="mt-4"><a href="/" class="text-blue-600">← Back</a></div>
            </div>
          </main>
        </body></html>
    `);
  } catch (e) {
    console.error('[legal] failed to read LEGAL.md:', e && e.message ? e.message : e);
    res.status(500).send('Legal page not available');
  }
});

// API: list persisted rebalancer proposals
app.get('/api/rebalancer/proposals', async (_, res) => {
  try {
    const { data, error } = await supabase
      .from('index_proposals')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(100);
    if (error) return res.status(500).json({ error: error.message || error });
    return res.json({ proposals: data });
  } catch (e) {
    console.error('[api] rebalancer proposals failed:', e && e.message ? e.message : e);
    return res.status(500).json({ error: String(e) });
  }
});

// API: compute a rebalancer proposal from provided data or from `index_constituents` table
app.post('/api/rebalancer/compute', async (req, res) => {
  try {
    const body = req.body || {};
    const indexId = body.indexId || body.index_id || 'OMXCAPPGI';
    const options = body.options || {};
    let data = body.data;

    // If no data provided, try to load from index_constituents table
    if ((!Array.isArray(data) || data.length === 0) && supabase) {
      try {
        const tableName = tableForIndex(indexId);
        // Pull only the latest snapshot and avoid duplicates
        const { data: dates, error: err1 } = await supabase
          .from(tableName)
          .select('as_of')
          .order('as_of', { ascending: false })
          .limit(1);
        if (err1) throw err1;
        const asOf = dates && dates[0] ? dates[0].as_of : null;
        const { data: rows, error } = await supabase
          .from(tableName)
          .select('*')
          .eq('as_of', asOf)
          .order('capped_weight', { ascending: false });
        if (!error && Array.isArray(rows) && rows.length > 0) {
          data = rows.map(r => ({
            ticker: r.ticker,
            issuer: r.name || r.issuer || r.ticker,
            price: r.price,
            mcap: Number(r.mcap || r.market_cap || 0),
            avg_30d_volume: r.avg_daily_volume || 0,
            currentWeight: Number(r.capped_weight || r.weight || 0)
          }));
        }
      } catch (e) {
        console.error('[api] failed to load index_constituents for compute:', e && e.message ? e.message : e);
      }
    }

    // If still no data, require it from caller
    if (!Array.isArray(data) || data.length === 0) return res.status(400).json({ error: 'No constituent data available. Provide `data` in request body or populate index_constituents table.' });

    // Use the rebalancer compute module
    const { computeProposal } = require('./projects/kaxcap-index/rebalancer');
    const result = computeProposal(indexId, data, options);
    return res.json({ proposal: result });
  } catch (e) {
    console.error('[api] compute rebalancer failed:', e && e.message ? e.message : e);
    return res.status(500).json({ error: String(e) });
  }
});

app.post('/api/rebalancer/proposals', async (req, res) => {
  try {
    const payload = req.body || {};

    // Simple API key guard for now (environment variable REBALANCER_API_KEY)
    const apiKey = process.env.REBALANCER_API_KEY || '';
    if (apiKey) {
      const provided = req.get('x-rebalancer-key') || req.query.key || '';
      if (!provided || provided !== apiKey) return res.status(401).json({ error: 'missing or invalid API key' });
    }

    // minimal validation
    if (!payload.indexId || !payload.proposed) return res.status(400).json({ error: 'indexId and proposed array required' });

    // Prefer atomic insert via Postgres RPC if available
    try {
      if (supabase && typeof supabase.rpc === 'function') {
        const { data, error } = await supabase.rpc('insert_proposal_with_constituents', { payload: payload });
        if (error) {
          console.error('[api] rpc insert failed:', error);
          // fall through to legacy behavior
        } else {
          return res.status(201).json({ proposal: data });
        }
      }
    } catch (rpcErr) {
      console.error('[api] rpc call failed:', rpcErr && rpcErr.message ? rpcErr.message : rpcErr);
      // continue to fallback
    }

    // Fallback: previous behavior (insert proposal then constituents)
    const row = {
      index_id: payload.indexId,
      name: payload.name || null,
      status: payload.status || 'proposed',
      payload: payload
    };
    const { data, error } = await supabase.from('index_proposals').insert([row]).select();
    if (error) return res.status(500).json({ error: error.message || error });
    const created = data && data[0];

    // Persist proposal constituents (if provided in payload.proposed)
    try {
      const proposalId = created && created.id;
      if (proposalId && Array.isArray(payload.proposed) && payload.proposed.length > 0) {
        const rows = payload.proposed.map(p => ({
          proposal_id: proposalId,
          index_id: payload.indexId,
          issuer: p.issuer || p.ticker || null,
          ticker: String(p.ticker || p.symbol || '').toUpperCase(),
          name: p.name || p.ticker || p.symbol || '',
          price: typeof p.price !== 'undefined' ? Number(p.price) : null,
          shares: typeof p.shares !== 'undefined' ? p.shares : null,
          mcap: typeof p.mcap !== 'undefined' ? Number(p.mcap) : null,
          old_weight: typeof p.oldWeight !== 'undefined' ? Number(p.oldWeight) : (typeof p.old_weight !== 'undefined' ? Number(p.old_weight) : null),
          new_weight: typeof p.newWeight !== 'undefined' ? Number(p.newWeight) : (typeof p.new_weight !== 'undefined' ? Number(p.new_weight) : null),
          created_at: new Date()
        }));

        const { data: pcData, error: pcError } = await supabase.from('proposal_constituents').insert(rows).select();
        if (pcError) {
          // Roll back created proposal to avoid half-created state
          try {
            await supabase.from('index_proposals').delete().eq('id', proposalId);
          } catch (delErr) {
            console.error('[api] failed to rollback proposal after constituents insert error:', delErr && delErr.message ? delErr.message : delErr);
          }
          console.error('[api] insert proposal_constituents failed:', pcError && pcError.message ? pcError.message : pcError);
          return res.status(500).json({ error: pcError.message || pcError });
        }
      }
    } catch (innerErr) {
      console.error('[api] persist constituents failed:', innerErr && innerErr.message ? innerErr.message : innerErr);
      return res.status(500).json({ error: String(innerErr) });
    }

    return res.status(201).json({ proposal: created });
  } catch (e) {
    console.error('[api] post rebalancer proposal failed:', e && e.message ? e.message : e);
    return res.status(500).json({ error: String(e) });
  }
});

// Simple memes store (file-backed). This keeps things lightweight and works without DB setup.
const MEME_STORE = path.join(__dirname, 'data', 'memes.json');
function readMemes() {
  try {
    if (!fs.existsSync(MEME_STORE)) return [];
    const raw = fs.readFileSync(MEME_STORE, 'utf8');
    return JSON.parse(raw || '[]');
  } catch (e) {
    console.error('[memes] read failed:', e && e.message ? e.message : e);
    return [];
  }
}
function writeMemes(arr) {
  try {
    const dir = path.dirname(MEME_STORE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(MEME_STORE, JSON.stringify(arr, null, 2), 'utf8');
    return true;
  } catch (e) {
    console.error('[memes] write failed:', e && e.message ? e.message : e);
    return false;
  }
}

app.get('/api/memes', async (req, res) => {
  const memes = readMemes();
  return res.json({ memes });
});

app.post('/api/memes', async (req, res) => {
  try {
    const { title = '', image = '', caption = '' } = req.body || {};
    if (!image && !caption) return res.status(400).json({ error: 'Provide at least an image URL or a caption' });
    const memes = readMemes();
    const entry = { id: Date.now().toString(), title: String(title).slice(0, 200), image: String(image).slice(0, 2000), caption: String(caption).slice(0, 1000), created_at: new Date().toISOString() };
    memes.unshift(entry); // latest first
    const ok = writeMemes(memes);
    if (!ok) return res.status(500).json({ error: 'Failed to persist meme' });
    return res.status(201).json({ meme: entry });
  } catch (e) {
    console.error('[api] post meme failed:', e && e.message ? e.message : e);
    return res.status(500).json({ error: String(e) });
  }
});

// Minimal debug endpoint to verify Supabase env presence (no secrets leaked)
app.get('/api/debug/env', (req, res) => {
  const supUrl = process.env.SUPABASE_URL;
  const supKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;
  const python = process.env.PYTHON || 'python3';
  res.json({
    ok: true,
    supabase_configured: Boolean(supUrl && supKey),
    supabase_url_present: Boolean(supUrl),
    supabase_key_present: Boolean(supKey),
    python_cmd: python
  });
});

// API: expose last captured FactSet rate-limit snapshot from Python worker
app.get('/api/rate', async (req, res) => {
  try {
    const file = path.join(__dirname, 'logs', 'api_rate.json');
    if (!fs.existsSync(file)) return res.json({ ok: true, snapshot: null });
    const txt = fs.readFileSync(file, 'utf8');
    const json = JSON.parse(txt || '{}');
    // Ensure any sensitive header values are redacted before returning
    try {
      if (json && json.headers && typeof json.headers === 'object') {
        const redactKeys = new Set(['set-cookie','cookie','authorization','x-datadirect-request-key','x-factset-api-request-key','x-api-key','api-key']);
        const safe = {};
        for (const [k,v] of Object.entries(json.headers)) {
          safe[k] = redactKeys.has(String(k).toLowerCase()) ? '[redacted]' : v;
        }
        json.headers = safe;
      }
    } catch {}
    let fileMtime = null;
    try { const st = fs.statSync(file); fileMtime = st.mtime ? new Date(st.mtime).toISOString() : null; } catch { }
    return res.json({ ok: true, snapshot: json, fileMtime });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e && e.message ? e.message : String(e) });
  }
});

// API: persist last rate-limit snapshot into Supabase (optional, no-op if not configured)
app.post('/api/rate/log', async (req, res) => {
  try {
    const table = (process.env.SUPABASE_HEADERS_TABLE || 'api_headers_log');
    if (!supabase || typeof supabase.from !== 'function') return res.json({ ok: true, persisted: false, reason: 'supabase not configured' });
    const file = path.join(__dirname, 'logs', 'api_rate.json');
    if (!fs.existsSync(file)) return res.json({ ok: true, persisted: false, reason: 'no snapshot' });
    const snap = JSON.parse(fs.readFileSync(file, 'utf8') || '{}');
    const row = {
      created_at: (snap.ts || new Date().toISOString()),
      limit: snap.limit ?? null,
      remaining: snap.remaining ?? null,
      reset: snap.reset ?? null,
      limit_second: snap.limit_second ?? null,
      remaining_second: snap.remaining_second ?? null,
      limit_day: snap.limit_day ?? null,
      remaining_day: snap.remaining_day ?? null,
      retry_after: snap.retry_after ?? null,
      headers: snap.headers || null
    };
    try { await supabase.from(table).insert([row]); } catch(e) { /* ignore insert errors */ }
    return res.json({ ok: true, persisted: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e && e.message ? e.message : String(e) });
  }
});

// API: lightweight status across markets — counts, latest as_of, totals
app.get('/api/status', async (req, res) => {
  async function getIndexStatus(idx) {
    try {
      const dailyTable = tableForIndex(idx);
      const issuersTable = issuersTableForIndex(idx);
      const quarterlyTable = quarterlyTableForIndex(idx);
      // latest as_of dates
      const { data: dDates } = await supabase.from(dailyTable).select('as_of').order('as_of', { ascending: false }).limit(1);
      const asOfDaily = dDates && dDates[0] ? dDates[0].as_of : null;
      const { data: qDates } = quarterlyTable ? await supabase.from(quarterlyTable).select('as_of').order('as_of', { ascending: false }).limit(1) : { data: [] };
      const asOfQuarterly = qDates && qDates[0] ? qDates[0].as_of : null;
      // counts
      let dailyCount = 0, issuersCount = 0, quarterlyCount = 0;
      if (asOfDaily) {
        const { count: dCount } = await supabase.from(dailyTable).select('ticker', { count: 'exact', head: true }).eq('as_of', asOfDaily);
        dailyCount = Number(dCount || 0);
      }
      if (issuersTable && asOfDaily) {
        const { count: iCount } = await supabase.from(issuersTable).select('issuer', { count: 'exact', head: true }).eq('as_of', asOfDaily);
        issuersCount = Number(iCount || 0);
      }
      if (quarterlyTable && asOfQuarterly) {
        const { count: qCount } = await supabase.from(quarterlyTable).select('ticker', { count: 'exact', head: true }).eq('as_of', asOfQuarterly);
        quarterlyCount = Number(qCount || 0);
      }
      // totals
      async function sumTotals(tableName, asOf) {
        if (!tableName || !asOf) return { mcap_uncapped: null, mcap_capped: null };
        const { data } = await supabase.from(tableName).select('mcap,mcap_capped').eq('as_of', asOf).limit(5000);
        const rows = Array.isArray(data) ? data : [];
        const unc = rows.reduce((s, r) => s + (Number(r.mcap || 0) || 0), 0);
        const cap = rows.reduce((s, r) => s + (Number(r.mcap_capped || 0) || 0), 0);
        return { mcap_uncapped: unc, mcap_capped: (cap > 0 ? cap : null) };
      }
      const totalsDaily = await sumTotals(dailyTable, asOfDaily);
      const totalsQuarterly = await sumTotals(quarterlyTable, asOfQuarterly);
      return { indexId: idx, asOfDaily, asOfQuarterly, dailyCount, issuersCount, quarterlyCount, totalsDaily, totalsQuarterly };
    } catch (e) {
      return { indexId: idx, error: e && e.message ? e.message : String(e) };
    }
  }
  try {
    const hel = (process.env.HEL_INDEX_ID || 'HELXCAP');
    const sto = (process.env.STO_INDEX_ID || 'OMXSALLS');
    const results = [];
    for (const idx of ['KAXCAP', hel]) { results.push(await getIndexStatus(idx)); }
    res.json({ ok: true, status: results });
  } catch (e) {
    res.status(500).json({ ok: false, error: e && e.message ? e.message : String(e) });
  }
});

// Status page: simple dashboard view
app.get('/status', async (req, res) => {
  try {
    const r = await (await fetch('http://localhost:' + PORT + '/api/status')).json().catch(()=>({ ok:false }));
    const items = (r && r.status) ? r.status : [];
    res.send(`
      <html>
        <head>${renderHead('Status')}</head>
        <body class="bg-gray-50 p-6">
          ${renderHeader()}
          <main class="max-w-6xl mx-auto p-6">
            <div class="bg-white rounded-xl shadow p-6">
              <h1 class="text-2xl font-bold mb-4">System Status</h1>
              <div class="grid md:grid-cols-2 gap-4">
                ${items.map(it=>`
                  <div class="border rounded p-3">
                    <div class="font-semibold mb-1">${it.indexId}</div>
                    <div class="text-sm text-slate-600">Daily as_of: ${it.asOfDaily||'n/a'} · Quarterly as_of: ${it.asOfQuarterly||'n/a'}</div>
                    <div class="text-sm">Rows — daily: ${it.dailyCount} · issuers: ${it.issuersCount} · quarterly: ${it.quarterlyCount}</div>
                    <div class="text-sm">Totals — daily uncapped: ${it.totalsDaily?.mcap_uncapped?.toLocaleString?.('en-DK')||'n/a'} · capped: ${it.totalsDaily?.mcap_capped?.toLocaleString?.('en-DK')||'n/a'}</div>
                    <div class="text-sm">Totals — quarterly uncapped: ${it.totalsQuarterly?.mcap_uncapped?.toLocaleString?.('en-DK')||'n/a'} · capped: ${it.totalsQuarterly?.mcap_capped?.toLocaleString?.('en-DK')||'n/a'}</div>
                  </div>
                `).join('')}
              </div>
              <div class="mt-4 text-sm"><a href="/index" class="text-blue-600">← Back to Index Overview</a></div>
            </div>
          </main>
          ${renderFooter()}
        </body>
      </html>
    `);
  } catch (e) {
    res.status(500).send('status error: ' + (e && e.message ? e.message : String(e)));
  }
});

// Product page: Rebalancer dashboard
app.get('/product/rebalancer', async (req, res) => { return res.redirect(302, '/index'); });

// AI Analyst product page: minimal UI consistent with Watchers and Rebalancer
app.get('/product/ai-analyst', async (req, res) => {
  try {
    res.send(`
      <html>
        <head>${renderHead('AI Analyst')}</head>
        <body class="bg-gray-50 p-6">
          ${renderHeader()}
          <main class="max-w-6xl mx-auto p-6">
            <div class="bg-white rounded-xl shadow p-6">
              <h1 class="text-3xl font-bold mb-4">AI Analyst</h1>
              <p class="text-sm text-gray-600 mb-4">AI Analyst ingests reports and proposals, generates concise summaries, and answers natural-language queries over stored documents.</p>
              <div class="grid md:grid-cols-2 gap-6">
                <div class="p-4 border rounded bg-gray-50">
                  <h2 class="font-semibold mb-2">Ingest</h2>
                  <p class="text-sm text-gray-700 mb-3">Upload or point to documents to add them to the analysis pipeline.</p>
                  <button class="px-4 py-2 bg-yellow-400 text-slate-900 rounded">Open Ingest</button>
                </div>
                <div class="p-4 border rounded bg-gray-50">
                  <h2 class="font-semibold mb-2">Query</h2>
                  <p class="text-sm text-gray-700 mb-3">Ask a question about ingested reports or proposals.</p>
                  <form id="aiQueryForm" class="space-y-3">
                    <input name="q" placeholder="Ask something like: 'Summarize recent rebalancer proposals'" class="w-full px-3 py-2 rounded border" />
                    <div class="flex gap-2"><button type="submit" class="px-4 py-2 bg-blue-600 text-white rounded">Run Query</button></div>
                  </form>
                  <div id="aiResult" class="mt-4 text-sm text-gray-700"></div>
                </div>
              </div>
              <div class="mt-6"><a href="/" class="text-blue-600">← Back</a></div>
            </div>
          </main>
        ${renderFooter()}
        <script>
          document.getElementById('aiQueryForm')?.addEventListener('submit', async (ev) => {
            ev.preventDefault();
            const q = ev.target.q.value;
            const resEl = document.getElementById('aiResult');
            resEl.textContent = 'Thinking...';
            try {
              const r = await fetch('/api/ai/query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ query: q }) });
              const json = await r.json();
              resEl.textContent = json.answer || JSON.stringify(json);
            } catch (e) {
              resEl.textContent = 'Query failed: ' + (e && e.message ? e.message : e);
            }
          });
        </script>
        </body>
      </html>
    `);
  } catch (err) {
    console.error('[ui] ai-analyst page failed:', err && err.message ? err.message : err);
    res.status(500).send('AI Analyst page failed');
  }
});

// API: simple health check for Supabase connectivity and latest snapshot counts
app.get('/api/health', async (_, res) => {
  try {
    const indices = ['KAXCAP', 'HELXCAP', 'OMXSALLS'];
    const results = [];
    for (const idx of indices) {
      const tableName = tableForIndex(idx);
      let latest = null;
      let count = null;
      try {
        const { data: dates, error: e1 } = await supabase
          .from(tableName)
          .select('as_of')
          .order('as_of', { ascending: false })
          .limit(1);
        if (!e1 && dates && dates[0] && dates[0].as_of) {
          latest = dates[0].as_of;
          const { data: rows, error: e2 } = await supabase
            .from(tableName)
            .select('*')
            .eq('as_of', latest);
          if (!e2 && Array.isArray(rows)) count = rows.length;
        }
      } catch (e) { }
      results.push({ index: idx, table: tableName, latest, count });
    }
    return res.json({ ok: true, results });
  } catch (e) {
    console.error('[api] health failed:', e && e.message ? e.message : e);
    return res.status(500).json({ ok: false, error: e && e.message ? e.message : e });
  }
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
      subject: 'MarketBuddy test email',
      text: 'This is a test email sent from MarketBuddy'
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
  // One-time backfill so month counter reflects earlier runs
  _backfillMonthUsageToSupabase();
  // Run FactSet worker batch once on startup to populate tables
  (async () => {
    try {
      const scriptPath = path.join(__dirname, 'workers', 'indexes', 'main.py');
      const venvPy = (() => {
        try { const p = path.join(__dirname, '.venv', 'bin', process.platform === 'win32' ? 'python.exe' : 'python'); return fs.existsSync(p) ? p : null; } catch { return null; }
      })();
      const candidates = [
        process.env.PYTHON || '',
        venvPy || '',
        'python3',
        'python',
        '/usr/bin/python3',
        '/usr/local/bin/python3'
      ].filter(Boolean);
      const runs = [
        { region: 'CPH', indexId: process.env.CPH_INDEX_ID || 'KAXCAP' },
        { region: 'HEL', indexId: process.env.HEL_INDEX_ID || 'HELXCAP' }
        // Stockholm paused
      ];
      console.log('[startup] Beginning FactSet batch for', runs.map(r => r.region + ':' + r.indexId).join(', '));
      writeSchedulerLog('startup FactSet batch starting');
      for (const r of runs) {
        let launched = false;
        let lastErr = null;
        for (const py of candidates) {
          console.log('[startup] trying interpreter:', py, 'region:', r.region, 'index:', r.indexId, 'args: --quarterly');
          const args = [scriptPath, '--region', r.region, '--index-id', r.indexId, '--quarterly'];
          // eslint-disable-next-line no-await-in-loop
          await new Promise((resolve) => {
            execFile(py, args, { env: process.env }, (error, stdout, stderr) => {
              if (error) {
                lastErr = error;
                const enoent = error && (error.code === 'ENOENT' || /not found|ENOENT/i.test(String(error.message || error)));
                console.error('[startup] interpreter failed:', py, 'error:', error && error.message ? error.message : String(error));
                if (stderr) console.error('[startup] stderr:', stderr);
                // Try next candidate on ENOENT; otherwise stop for this run
                if (enoent) {
                  resolve();
                } else {
                  writeSchedulerLog(`startup FactSet error region=${r.region} index=${r.indexId}: ${error && error.message ? error.message : String(error)}`);
                  resolve();
                }
              } else {
                launched = true;
                if (stderr) console.warn('[startup] stderr:', stderr);
                console.log('[startup] FactSet run ok', r);
                if (stdout) console.log('[startup] stdout:', stdout);
                writeSchedulerLog(`startup FactSet ok region=${r.region} index=${r.indexId}`);
                _logUsageRunSupabase(r.indexId);
                resolve();
              }
            });
          });
          if (launched) break;
        }
        if (!launched) {
          console.error('[startup] all interpreter candidates failed for', r, 'last error:', lastErr && lastErr.message ? lastErr.message : String(lastErr));
          writeSchedulerLog(`startup FactSet failed all interpreters region=${r.region} index=${r.indexId}: ${lastErr && lastErr.message ? lastErr.message : String(lastErr)}`);
        }
      }
      writeSchedulerLog('startup FactSet batch complete');
    } catch (e) {
      console.error('[startup] FactSet batch failed', e && e.message ? e.message : e);
      writeSchedulerLog(`startup FactSet batch failed: ${e && e.message ? e.message : String(e)}`);
    }
  })();
});

// Schedule watchers to run periodically using node-cron if available.
try {
  const cron = require('node-cron');
  // Run every 4 hours (minute 0) -> 6x/day. Use Europe/Copenhagen timezone.
  cron.schedule('0 */4 * * *', async () => {
    const startTs = new Date();
    console.log('[scheduler] scheduled run starting', startTs.toISOString());
    writeSchedulerLog('scheduled run starting');

    // VA run
    try {
      const vaResult = await updateVA();
      const summary = vaResult ? `VA result: ${JSON.stringify(vaResult)}` : 'VA result: no-change or null';
      console.log('[scheduler] VA:', summary);
      writeSchedulerLog(summary);
    } catch (e) {
      console.error('[scheduler] updateVA failed', e && e.message ? e.message : e);
      writeSchedulerLog(`VA error: ${e && e.message ? e.message : String(e)}`);
    }

    // eSundhed run
    try {
      const esResult = await updateEsundhed();
      const summary = esResult ? `eSundhed result: ${JSON.stringify(esResult)}` : 'eSundhed result: no-change or null';
      console.log('[scheduler] eSundhed:', summary);
      writeSchedulerLog(summary);
    } catch (e) {
      console.error('[scheduler] updateEsundhed failed', e && e.message ? e.message : e);
      writeSchedulerLog(`eSundhed error: ${e && e.message ? e.message : String(e)}`);
    }

    // Trigger FactSet worker for all markets (daily + quarterly)
    try {
      let pythonCmd = process.env.PYTHON || 'python3';
      try {
        const venvPy = path.join(__dirname, '.venv', 'bin', process.platform === 'win32' ? 'python.exe' : 'python');
        if (fs.existsSync(venvPy)) pythonCmd = venvPy;
      } catch { }
      const scriptPath = path.join(__dirname, 'workers', 'indexes', 'main.py');
      const runs = [
        { region: 'CPH', indexId: process.env.CPH_INDEX_ID || 'KAXCAP' },
        { region: 'HEL', indexId: process.env.HEL_INDEX_ID || 'HELXCAP' }
        // Stockholm paused
      ];
      for (const r of runs) {
        await new Promise((resolve) => {
          // Use --quarterly so the single run refreshes both Daily and Quarterly tables
          execFile(pythonCmd, [scriptPath, '--region', r.region, '--index-id', r.indexId, '--quarterly'], { env: process.env }, (error, stdout, stderr) => {
            if (error) {
              console.error('[scheduler] FactSet run error', r, error);
              writeSchedulerLog(`FactSet run error region=${r.region} index=${r.indexId}: ${error && error.message ? error.message : String(error)}`);
            } else {
              console.log('[scheduler] FactSet run ok', r, stdout);
              writeSchedulerLog(`FactSet run ok region=${r.region} index=${r.indexId}`);
              _logUsageRunSupabase(r.indexId);
            }
            resolve();
          });
        });
      }
    } catch (e) {
      console.error('[scheduler] FactSet batch failed', e && e.message ? e.message : e);
      writeSchedulerLog(`FactSet batch failed: ${e && e.message ? e.message : String(e)}`);
    }

    const endTs = new Date();
    console.log('[scheduler] scheduled run complete', endTs.toISOString());
    writeSchedulerLog(`scheduled run complete (duration_ms=${endTs - startTs})`);
  }, { timezone: 'Europe/Copenhagen' });
  console.log('[scheduler] cron scheduled: every 4 hours (Europe/Copenhagen)');
} catch (e) {
  console.log('[scheduler] node-cron not available or failed to load — scheduled runs disabled');
}

// end of file
