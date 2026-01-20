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
              <a href="/index" class="hover:text-slate-900">Indexes</a>
              <a href="/watchers" class="hover:text-slate-900">Watchers</a>
              <a href="/product/ai-analyst" class="hover:text-slate-900">AI Analyst</a>
                      <a href="/about" class="hover:text-slate-900">About</a>
                      <a href="https://www.linkedin.com/in/tobias-gudbjerg-59b893249/" target="_blank" rel="noopener" class="hover:text-slate-900 flex items-center gap-2">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512" class="w-4 h-4" fill="currentColor" aria-hidden="true"><path d="M100.28 448H7.4V148.9h92.88zm-46.44-340a53.66 53.66 0 1 1 53.66-53.66 53.66 53.66 0 0 1-53.66 53.66zM447.9 448h-92.68V302.4c0-34.7-.7-79.4-48.4-79.4-48.4 0-55.8 37.8-55.8 76.8V448h-92.7V148.9h89V196h1.3c12.4-23.5 42.6-48.4 87.7-48.4 93.8 0 111.2 61.8 111.2 142.3V448z"/></svg>
                        <span class="sr-only">LinkedIn</span>
                      </a>
              <a href="/contact" class="px-3 py-2 rounded bg-yellow-400 text-slate-900 font-semibold">Login/Sign Up</a>
            </nav>
            <button id="mobileMenuBtn" class="md:hidden text-slate-600">☰</button>
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

function rankBy(rows, key, desc = true) {
  return [...rows].sort((a, b) => {
    const av = Number(a[key] || 0);
    const bv = Number(b[key] || 0);
    return desc ? (bv - av) : (av - bv);
  });
}

app.get('/kaxcap', async (req, res) => {
  try {
    const rows = (await fetchLatestIndexRows('KAXCAP')).filter(r => Number(r.capped_weight || 0) > 0);
    // Daily must be capped ranking — show Top 25 by capped_weight
    const ranked = rankBy(rows, 'capped_weight', true).slice(0, 25);
    const meta = await getIndexMeta('KAXCAP');
    const aum = meta.aum;
    const viewRows = ranked.map(r => {
      const mcapRaw = (r.market_cap != null ? Number(r.market_cap) : (r.mcap != null ? Number(r.mcap) : null));
      const mcapBn = (mcapRaw != null) ? (mcapRaw / 1e9) : null;
      const deltaFrac = (typeof r.delta_pct === 'number') ? Number(r.delta_pct) : null;
      const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
      const deltaVolShares = (deltaAmt != null && r.price != null) ? (deltaAmt / Number(r.price)) : null;
      const adv = (r.avg_daily_volume != null ? Number(r.avg_daily_volume) : null);
      const dtc = (deltaVolShares != null && adv != null && adv > 0) ? (Math.abs(deltaVolShares) / adv) : null;
      return Object.assign({}, r, {
        name_view: r.name || r.issuer || '',
        mcap_bn: mcapBn,
        weight_pct: (r.weight != null ? Number(r.weight) : null),
        capped_weight_pct: (r.capped_weight != null ? Number(r.capped_weight) : null),
        delta_pct_view: (deltaFrac != null ? deltaFrac : null),
        delta_amt: (deltaAmt != null ? deltaAmt : null),
        dtc_view: (dtc != null ? dtc : null)
      });
    });
    const cols = [
      { key: 'name_view', label: 'Name' },
      { key: 'mcap_bn', label: 'Market Cap, bn', format: v => (v != null ? Number(v).toFixed(2) : '') },
      { key: 'weight_pct', label: 'Weight', format: v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '') },
      { key: 'capped_weight_pct', label: 'Capped Weight', format: v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '') },
      { key: 'delta_pct_view', label: 'Delta, %', format: v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '') },
      { key: 'delta_amt', label: 'Delta, ' + (meta.currency || 'CCY'), format: v => (v != null ? Math.round(Number(v)).toLocaleString('en-DK') : '') },
      { key: 'dtc_view', label: 'Days to Cover', format: v => (v != null ? Number(v).toFixed(2) : '') },
      { key: 'flags', label: 'Flags' },
    ];
    const controls = '<div class="max-w-6xl mx-auto px-6 flex items-center gap-4"><a href="/kaxcap/quarterly" class="text-blue-600">Quarterly preview →</a><button id="refreshKAX" class="px-3 py-2 border rounded">Refresh Now</button><span id="refreshKAXMeta" class="text-sm text-slate-600"></span></div>';
    const html = ['<!doctype html><html><head>', renderHead('KAXCAP — Daily Status (Top 25 by Capped Weight)'), '</head><body class="bg-gray-50">', renderHeader(), renderIndexTable('KAXCAP — Daily Status (Top 25 by Capped Weight)', viewRows, cols), controls, '<script>document.getElementById("refreshKAX")?.addEventListener("click", async ()=>{try{const r=await fetch("/api/kaxcap/run?region=CPH&indexId=KAXCAP",{method:"POST"});const j=await r.json();document.getElementById("refreshKAXMeta").textContent=j.ok?"Updated — reloading…":"Error: "+(j.error||"unknown");setTimeout(()=>location.reload(),1200);}catch(e){document.getElementById("refreshKAXMeta").textContent="Error: "+(e&&e.message?e.message:e);}});</script>', renderFooter(), '</body></html>'].join('');
    res.send(html);
  } catch (e) {
    res.status(500).send(String(e && e.message ? e.message : e));
  }
});

app.get('/hel', async (req, res) => {
  try {
    const rows = (await fetchLatestIndexRows(process.env.HEL_INDEX_ID || 'HELXCAP')).filter(r => Number(r.capped_weight || 0) > 0);
    const ranked = rankBy(rows, 'capped_weight', true).slice(0, 25);
    const idxId = process.env.HEL_INDEX_ID || 'HELXCAP';
    const meta = await getIndexMeta(idxId);
    const aum = meta.aum;
    const viewRows = ranked.map(r => {
      const mcapRaw = (r.market_cap != null ? Number(r.market_cap) : (r.mcap != null ? Number(r.mcap) : null));
      const mcapBn = (mcapRaw != null) ? (mcapRaw / 1e9) : null;
      const deltaFrac = (typeof r.delta_pct === 'number') ? Number(r.delta_pct) : null;
      const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
      const deltaVolShares = (deltaAmt != null && r.price != null) ? (deltaAmt / Number(r.price)) : null;
      const adv = (r.avg_daily_volume != null ? Number(r.avg_daily_volume) : null);
      const dtc = (deltaVolShares != null && adv != null && adv > 0) ? (Math.abs(deltaVolShares) / adv) : null;
      return Object.assign({}, r, {
        name_view: r.name || r.issuer || '',
        mcap_bn: mcapBn,
        weight_pct: (r.weight != null ? Number(r.weight) : null),
        capped_weight_pct: (r.capped_weight != null ? Number(r.capped_weight) : null),
        delta_pct_view: (deltaFrac != null ? deltaFrac : null),
        delta_amt: (deltaAmt != null ? deltaAmt : null),
        dtc_view: (dtc != null ? dtc : null)
      });
    });
    const cols = [
      { key: 'name_view', label: 'Name' },
      { key: 'mcap_bn', label: 'Market Cap, bn', format: v => (v != null ? Number(v).toFixed(2) : '') },
      { key: 'weight_pct', label: 'Weight', format: v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '') },
      { key: 'capped_weight_pct', label: 'Capped Weight', format: v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '') },
      { key: 'delta_pct_view', label: 'Delta, %', format: v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '') },
      { key: 'delta_amt', label: 'Delta, ' + (meta.currency || 'CCY'), format: v => (v != null ? Math.round(Number(v)).toLocaleString('en-DK') : '') },
      { key: 'dtc_view', label: 'Days to Cover', format: v => (v != null ? Number(v).toFixed(2) : '') },
      { key: 'flags', label: 'Flags' },
    ];
    const controls = '<div class="max-w-6xl mx-auto px-6 flex items-center gap-4"><a href="/hel/quarterly" class="text-blue-600">Quarterly preview →</a><button id="refreshHEL" class="px-3 py-2 border rounded">Refresh Now</button><span id="refreshHELMeta" class="text-sm text-slate-600"></span></div>';
    const html = ['<!doctype html><html><head>', renderHead('HEL — Daily Status (Top 25 by Capped Weight)'), '</head><body class="bg-gray-50">', renderHeader(), renderIndexTable('HEL — Daily Status (Top 25 by Capped Weight)', viewRows, cols), controls, '<script>document.getElementById("refreshHEL")?.addEventListener("click", async ()=>{try{const r=await fetch("/api/kaxcap/run?region=HEL&indexId=' + idxId + '",{method:"POST"});const j=await r.json();document.getElementById("refreshHELMeta").textContent=j.ok?"Updated — reloading…":"Error: "+(j.error||"unknown");setTimeout(()=>location.reload(),1200);}catch(e){document.getElementById("refreshHELMeta").textContent="Error: "+(e&&e.message?e.message:e);}});</script>', renderFooter(), '</body></html>'].join('');
    res.send(html);
  } catch (e) {
    res.status(500).send(String(e && e.message ? e.message : e));
  }
});

app.get('/sto', async (req, res) => {
  try {
    const rows = (await fetchLatestIndexRows(process.env.STO_INDEX_ID || 'OMXSALLS')).filter(r => Number(r.capped_weight || 0) > 0);
    const ranked = rankBy(rows, 'capped_weight', true).slice(0, 25);
    const idxId = process.env.STO_INDEX_ID || 'OMXSALLS';
    const meta = await getIndexMeta(idxId);
    const aum = meta.aum;
    const viewRows = ranked.map(r => {
      const mcapRaw = (r.market_cap != null ? Number(r.market_cap) : (r.mcap != null ? Number(r.mcap) : null));
      const mcapBn = (mcapRaw != null) ? (mcapRaw / 1e9) : null;
      const deltaFrac = (typeof r.delta_pct === 'number') ? Number(r.delta_pct) : null;
      const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
      const deltaVolShares = (deltaAmt != null && r.price != null) ? (deltaAmt / Number(r.price)) : null;
      const adv = (r.avg_daily_volume != null ? Number(r.avg_daily_volume) : null);
      const dtc = (deltaVolShares != null && adv != null && adv > 0) ? (Math.abs(deltaVolShares) / adv) : null;
      return Object.assign({}, r, {
        name_view: r.name || r.issuer || '',
        mcap_bn: mcapBn,
        weight_pct: (r.weight != null ? Number(r.weight) : null),
        capped_weight_pct: (r.capped_weight != null ? Number(r.capped_weight) : null),
        delta_pct_view: (deltaFrac != null ? deltaFrac : null),
        delta_amt: (deltaAmt != null ? deltaAmt : null),
        dtc_view: (dtc != null ? dtc : null)
      });
    });
    const cols = [
      { key: 'name_view', label: 'Name' },
      { key: 'mcap_bn', label: 'Market Cap, bn', format: v => (v != null ? Number(v).toFixed(2) : '') },
      { key: 'weight_pct', label: 'Weight', format: v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '') },
      { key: 'capped_weight_pct', label: 'Capped Weight', format: v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '') },
      { key: 'delta_pct_view', label: 'Delta, %', format: v => (v != null ? ((Number(v) > 1 ? Number(v) : Number(v) * 100).toFixed(2) + '%') : '') },
      { key: 'delta_amt', label: 'Delta, ' + (meta.currency || 'CCY'), format: v => (v != null ? Math.round(Number(v)).toLocaleString('en-DK') : '') },
      { key: 'dtc_view', label: 'Days to Cover', format: v => (v != null ? Number(v).toFixed(2) : '') },
      { key: 'flags', label: 'Flags' },
    ];
    const controls = '<div class="max-w-6xl mx-auto px-6 flex items-center gap-4"><a href="/sto/quarterly" class="text-blue-600">Quarterly preview →</a><button id="refreshSTO" class="px-3 py-2 border rounded">Refresh Now</button><span id="refreshSTOMeta" class="text-sm text-slate-600"></span></div>';
    const html = ['<!doctype html><html><head>', renderHead('STO — Daily Status (Top 25 by Capped Weight)'), '</head><body class="bg-gray-50">', renderHeader(), renderIndexTable('STO — Daily Status (Top 25 by Capped Weight)', viewRows, cols), controls, '<script>document.getElementById("refreshSTO")?.addEventListener("click", async ()=>{try{const r=await fetch("/api/kaxcap/run?region=STO&indexId=' + idxId + '",{method:"POST"});const j=await r.json();document.getElementById("refreshSTOMeta").textContent=j.ok?"Updated — reloading…":"Error: "+(j.error||"unknown");setTimeout(()=>location.reload(),1200);}catch(e){document.getElementById("refreshSTOMeta").textContent="Error: "+(e&&e.message?e.message:e);}});</script>', renderFooter(), '</body></html>'].join('');
    res.send(html);
  } catch (e) {
    res.status(500).send(String(e && e.message ? e.message : e));
  }
});

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

app.get('/kaxcap/quarterly', async (req, res) => {
  try {
    const { asOf, rows } = await fetchQuarterlyLatest('KAXCAP');
    const idxMeta = await getIndexMeta('KAXCAP');
    const region = 'CPH';
    const ccy = idxMeta.currency || 'DKK';
    const viewRows = renderQuarterlyRows(rows, region);
    const cols = quarterlyColumns(ccy);
    const pageMeta = '<div class="max-w-6xl mx-auto px-6 text-sm text-slate-600">As of: ' + (asOf || 'unknown') + ' · Rows: ' + rows.length + '</div>';
    const html = ['<!doctype html><html><head>', renderHead('KAXCAP — Quarterly Proforma'), '</head><body class="bg-gray-50">', renderHeader(), pageMeta, renderIndexTable('KAXCAP — Quarterly Proforma', viewRows, cols), renderFooter(), '</body></html>'].join('');
    res.send(html);
  } catch (e) {
    res.status(500).send(String(e && e.message ? e.message : e));
  }
});

app.get('/hel/quarterly', async (req, res) => {
  try {
    const idxId = process.env.HEL_INDEX_ID || 'HELXCAP';
    const { asOf, rows } = await fetchQuarterlyLatest(idxId);
    const idxMeta = await getIndexMeta(idxId);
    const region = 'HEL';
    const ccy = idxMeta.currency || 'EUR';
    const viewRows = renderQuarterlyRows(rows, region);
    const cols = quarterlyColumns(ccy);
    const pageMeta = '<div class="max-w-6xl mx-auto px-6 text-sm text-slate-600">As of: ' + (asOf || 'unknown') + ' · Rows: ' + rows.length + '</div>';
    const html = ['<!doctype html><html><head>', renderHead('HEL — Quarterly Proforma'), '</head><body class="bg-gray-50">', renderHeader(), pageMeta, renderIndexTable('HEL — Quarterly Proforma', viewRows, cols), renderFooter(), '</body></html>'].join('');
    res.send(html);
  } catch (e) {
    res.status(500).send(String(e && e.message ? e.message : e));
  }
});

app.get('/sto/quarterly', async (req, res) => {
  try {
    const idxId = process.env.STO_INDEX_ID || 'OMXSALLS';
    const { asOf, rows } = await fetchQuarterlyLatest(idxId);
    const idxMeta = await getIndexMeta(idxId);
    const region = 'STO';
    const ccy = idxMeta.currency || 'SEK';
    const viewRows = renderQuarterlyRows(rows, region);
    const cols = quarterlyColumns(ccy);
    const pageMeta = '<div class="max-w-6xl mx-auto px-6 text-sm text-slate-600">As of: ' + (asOf || 'unknown') + ' · Rows: ' + rows.length + '</div>';
    const html = ['<!doctype html><html><head>', renderHead('STO — Quarterly Proforma'), '</head><body class="bg-gray-50">', renderHeader(), pageMeta, renderIndexTable('STO — Quarterly Proforma', viewRows, cols), renderFooter(), '</body></html>'].join('');
    res.send(html);
  } catch (e) {
    res.status(500).send(String(e && e.message ? e.message : e));
  }
});

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
    <head>${renderHead('MarketBuddy — ' + project)}
      <style>/* small helper to make posted meme images scale */
        .meme-img{width:100%;height:auto;object-fit:cover;border-radius:8px}
        .meme-img{max-height:400px;}
        .meme-collapsed .meme-area, .meme-collapsed #memeFormWrapper { display: none; }
        .recent-thumb{width:56px;height:56px;object-fit:cover;border-radius:6px;border:1px solid rgba(255,255,255,0.08)}
      </style>
    </head>
    <body class="bg-gray-50 text-gray-800 font-sans">
      ${renderHeader()}

  
        <div class="max-w-6xl mx-auto px-6 py-16 grid grid-cols-1 md:grid-cols-3 gap-8 items-center">
          <div class="md:col-span-2">
            <div class="mb-4 text-sm text-yellow-600 uppercase tracking-wide">Powered by ABG Sundal Collier</div>
            <h1 class="text-4xl md:text-5xl font-extrabold leading-tight mb-4 text-gray-900">Your Intelligent Edge in Equity Sales</h1>
            <p class="text-gray-700 max-w-xl mb-6">Real-time scraping, index analytics, and an AI that converts rebalances and earnings deviations into concise, actionable sales commentary for ABG traders and sales teams.</p>
            <div class="text-sm text-gray-600">Use the top navigation to open product pages.</div>
          </div>

          <div class="md:block">
            <div id="memeCard" class="bg-white/5 rounded-lg p-4 shadow-lg">
              <div id="memeArea" class="meme-area">
                <img id="topMeme" src="/assets/tscMeme.jpg" alt="hero" class="meme-img mb-3" loading="lazy">
                <div class="text-sm text-gray-800 font-medium">Meme of the Moment</div>
                <div id="memeMeta" class="mt-2 bg-white p-3 rounded border flex items-start gap-3">
                  <div class="text-sm text-gray-700">
                    <div id="memeTitle" class="font-semibold text-gray-900">Internal meme</div>
                    <div id="memeCaption" class="text-gray-600 text-xs mt-1">For internal ABG use — post brief comments or images for the team</div>
                  </div>
                </div>
              </div>

              <div class="mt-3 flex items-center justify-between">
                <div class="text-xs text-gray-600">Tip: collapse to hide the image (session only)</div>
                <div class="flex items-center gap-3">
                  <button id="toggleMemeBtn" class="text-xs text-blue-700 underline">Collapse</button>
                </div>
              </div>

              <div id="recentMemes" class="mt-3 flex gap-2"></div>

              <div id="memeFormWrapper" class="mt-3 hidden">
                <form id="memeForm" class="space-y-2">
                  <input name="title" placeholder="Title" class="w-full px-3 py-2 rounded border bg-white text-gray-800 text-sm" />
                  <input name="image" placeholder="Image URL" class="w-full px-3 py-2 rounded border bg-white text-gray-800 text-sm" />
                  <textarea name="caption" placeholder="Short caption" class="w-full px-3 py-2 rounded border bg-white text-gray-800 text-sm"></textarea>
                  <div class="flex gap-2">
                    <button type="submit" class="px-3 py-2 bg-yellow-400 text-slate-900 rounded">Post</button>
                    <button type="button" id="cancelMeme" class="px-3 py-2 border rounded">Cancel</button>
                  </div>
                </form>
              </div>
            </div>
          </div>
        </div>

  <main class="max-w-4xl mx-auto p-6">
        <div class="bg-white rounded-xl shadow p-6">
          <h1 class="text-2xl font-bold mb-2">MarketBuddy — ABG internal</h1>
          <p class="mb-4 text-gray-600">Internal tool for ABG sales and trading: real-time watchers, index rebalancer proposals, and an AI analyst that provides fast, plain-language comments on rebalances and earnings deviations for quick distribution to the sales desk.</p>
          <div class="mt-4 flex items-center gap-4">
            <a href="/watchers" class="inline-block text-blue-600">Open Watchers →</a>
            <a href="/kaxcap" class="inline-block text-blue-600">KAXCAP Index →</a>
            <a href="/product/rebalancer" class="inline-block text-blue-600">Index Rebalancer →</a>
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

        // Meme collapse state is session-scoped so it's dynamic per browser session
        const toggleBtn = document.getElementById('toggleMemeBtn');
        const memeCard = document.getElementById('memeCard');
        const memeFormWrapper = document.getElementById('memeFormWrapper');

        function isCollapsed() {
          return sessionStorage.getItem('mb:meme:collapsed') === '1';
        }
        function setCollapsed(v) {
          sessionStorage.setItem('mb:meme:collapsed', v ? '1' : '0');
          if (v) memeCard.classList.add('meme-collapsed'); else memeCard.classList.remove('meme-collapsed');
          toggleBtn.textContent = v ? 'Expand' : 'Collapse';
        }

        if (toggleBtn) toggleBtn.addEventListener('click', () => setCollapsed(!isCollapsed()));
        // initialize collapsed state (apply class, don't remove the card entirely)
        setCollapsed(isCollapsed());

        // Fetch memes from the server (simple file-backed store) and render prominently
        async function loadMemes() {
          try {
            const res = await fetch('/api/memes');
            if (!res.ok) return;
            const json = await res.json();
            const memes = (json && json.memes) || [];
            if (memes.length === 0) return;
            const top = memes[0];
            const topImg = document.getElementById('topMeme');
            const titleEl = document.getElementById('memeTitle');
            const captionEl = document.getElementById('memeCaption');
            if (top.image) topImg.src = top.image;
            if (top.title) titleEl.textContent = top.title;
            if (top.caption) captionEl.textContent = top.caption;
            // render up to 3 recent thumbnails
            const recentEl = document.getElementById('recentMemes');
            if (recentEl) {
              recentEl.innerHTML = '';
              memes.slice(0, 3).forEach(m => {
                const img = document.createElement('img');
                img.className = 'recent-thumb';
                img.src = m.image || '/assets/tscMeme.jpg';
                img.title = m.title || '';
                img.addEventListener('click', () => {
                  if (m.image) topImg.src = m.image;
                  titleEl.textContent = m.title || '';
                  captionEl.textContent = m.caption || '';
                });
                recentEl.appendChild(img);
              });
            }
          } catch (e) {
            console.warn('Failed to load memes', e);
          }
        }

        loadMemes();

        // Meme posting UI
        const memeForm = document.getElementById('memeForm');
        const cancelBtn = document.getElementById('cancelMeme');
        if (memeForm) {
          // show form when user clicks title area
          document.getElementById('memeMeta').addEventListener('click', () => {
            memeFormWrapper.classList.toggle('hidden');
          });

          memeForm.addEventListener('submit', async (ev) => {
            ev.preventDefault();
            const fd = new FormData(memeForm);
            const payload = { title: fd.get('title') || '', image: fd.get('image') || '', caption: fd.get('caption') || '' };
            try {
              const r = await fetch('/api/memes', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
              if (!r.ok) throw new Error('Failed to post');
              memeForm.reset();
              memeFormWrapper.classList.add('hidden');
              await loadMemes();
              alert('Meme posted for this session');
            } catch (e) {
              console.error('Post meme failed', e);
              alert('Posting meme failed');
            }
          });

          if (cancelBtn) cancelBtn.addEventListener('click', () => memeFormWrapper.classList.add('hidden'));
        }
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
    if (region) { args.push('--region', String(region).toUpperCase()); }
    if (indexId) { args.push('--index-id', String(indexId)); }
    if (asOf) { args.push('--as-of', String(asOf)); }
    if (String(quarterly).toLowerCase() === 'true') { args.push('--quarterly'); }
    execFile(pythonCmd, args, { env: process.env }, (error, stdout, stderr) => {
      if (error) {
        console.error('[kaxcap-run] error:', error);
        if (stderr) console.error('[kaxcap-run] stderr:', stderr);
        return res.status(500).json({ ok: false, error: String(error), stderr });
      }
      if (stderr) console.warn('[kaxcap-run] stderr:', stderr);
      console.log('[kaxcap-run] stdout:', stdout);
      return res.json({ ok: true, stdout });
    });
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
      rows = Array.isArray(data) ? data : [];
    }
    res.json({ asOf, rows });
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
      rows = (Array.isArray(data) ? data : []).map(r => ({
        ...r,
        old_weight: (r.curr_weight_capped ?? r.curr_weight_uncapped ?? null),
        new_weight: (typeof r.weight !== 'undefined' ? r.weight : null)
      }));
    }
    res.json({ asOf, rows });
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
                  <span id="refreshTicker" class="text-xs text-slate-500" aria-live="polite">auto-refresh in 05:00</span>
                  <label class="text-xs text-slate-600 flex items-center gap-1"><input id="pauseRefresh" type="checkbox" class="align-middle"> Pause</label>
                  <label class="text-xs text-slate-600 flex items-center gap-1"><input id="runQuarterly" type="checkbox" class="align-middle"> Quarterly</label>
                  <button id="refreshBtn" class="px-3 py-2 bg-yellow-400 text-slate-900 rounded">Refresh Selected</button>
                </div>
              </div>
              <div id="summaryBar" class="flex flex-wrap items-center gap-2 mb-3 text-sm" aria-label="Index summary"></div>
              <div id="meta" class="text-sm text-slate-600 mb-2">Select an index to load data.</div>

              <section class="mb-6">
                <h2 class="font-bold mb-1">Quarterly Proforma</h2>
                <p class="text-xs text-slate-500 mb-2">Uncapped ranking → assign exception caps and 4.5% cap; deltas vs current capped, with AUM-derived flow and DTC.</p>
                <div id="quarterlyMeta" class="text-xs text-slate-500 mb-2"></div>
                <div id="quarterlyTable"></div>
              </section>

              <section>
                <h2 class="font-bold mb-1">Daily Status <span id="dailyBadge" class="ml-2 inline-block px-2 py-0.5 rounded-full text-xs bg-slate-100 text-slate-700" aria-label="Warnings count">0</span></h2>
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
            let selected = 'KAXCAP';
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
            let nextRefreshSec = 300;
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
                const q = document.getElementById('runQuarterly');
                const quarterlyParam = (q && q.checked) ? '&quarterly=true' : '';
                const r = await fetch('/api/kaxcap/run?region=' + encodeURIComponent(region) + '&indexId=' + encodeURIComponent(idx) + quarterlyParam, { method: 'POST' });
                const json = await r.json();
                document.getElementById('meta').textContent = 'Refresh triggered for ' + selected + (json.ok ? ' — OK' : (' — Error: ' + (json.error || 'unknown')));
                setTimeout(loadAll, 1200);
              } catch (e) {
                document.getElementById('meta').textContent = 'Refresh failed: ' + (e && e.message ? e.message : e);
              }
            });

            async function loadAll() {
              document.getElementById('meta').textContent = 'Loading ' + selected + '…';
              await loadMeta();
              await Promise.all([loadQuarterly(), loadDaily()]);
              document.getElementById('meta').textContent = 'Loaded ' + selected;
              nextRefreshSec = 300;
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
                const r = await fetch('/api/index/' + selected + '/quarterly');
                const json = await r.json();
                const rows = json.rows || [];
                const region = regionFor(selected);
                const aum = (selectedMeta.aum != null ? selectedMeta.aum : (aumByRegion[region] || null));
                const ccy = selectedMeta.ccy || currencyByRegion[region] || '';
                document.getElementById('quarterlyMeta').textContent = 'As of: ' + (json.asOf || 'unknown') + ' · Rows: ' + rows.length + ' · AUM (' + (ccy || 'CCY') + '): ' + (aum ? aum.toLocaleString('en-DK') : 'n/a') + ' · ' + '<a class="text-blue-600" href="/api/index/' + selected + '/quarterly">JSON</a>';
                quarterlyRowsCache = rows.slice();
                function getQuarterlyVal(row){
                  const issuer = (row.issuer || row.ticker || '').toLowerCase();
                  const mcap = (row.mcap_uncapped!=null? Number(row.mcap_uncapped): (row.mcap!=null? Number(row.mcap): (row.mcap_bn!=null? Number(row.mcap_bn)*1e9: null)));
                  const currW = (row.old_weight != null) ? Number(row.old_weight) : null;
                  const newW = (row.new_weight != null) ? Number(row.new_weight) : null;
                  const deltaFrac = (currW != null && newW != null) ? (newW - currW) : null;
                  const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
                  if (quarterlySort.key==='issuer') return issuer;
                  if (quarterlySort.key==='mcap') return mcap;
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
                  const issuer = (r.issuer || r.ticker || '');
                  const mcapBn = (r.mcap_bn != null) ? Number(r.mcap_bn) : (
                    r.mcap != null ? (Number(r.mcap) / 1e9) : (
                      r.mcap_uncapped != null ? (Number(r.mcap_uncapped) / 1e9) : null
                    )
                  );
                  const currW = (r.old_weight != null) ? Number(r.old_weight) : null; // decimal fraction
                  const newW = (r.new_weight != null) ? Number(r.new_weight) : null; // decimal fraction
                  const deltaFrac = (currW != null && newW != null) ? (newW - currW) : null;
                  const currWPct = (currW != null) ? (currW * 100) : null;
                  const newWPct = (newW != null) ? (newW * 100) : null;
                  const deltaPct = (deltaFrac != null) ? (deltaFrac * 100) : null;
                  const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
                  const deltaVol = (r.delta_vol != null) ? Number(r.delta_vol) : null;
                  const dtc = (r.days_to_cover != null) ? Number(r.days_to_cover) : null;
                  const deltaClass = (deltaPct!=null && deltaPct>0) ? 'text-green-700' : (deltaPct!=null && deltaPct<0 ? 'text-red-700' : '');
                  return '<tr class="border-b">'
                    + '<td class="px-3 py-2 text-sm sticky left-0 bg-white">' + issuer + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (mcapBn != null ? mcapBn.toFixed(2) : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right" title="' + (currWPct!=null? currWPct.toFixed(2)+'%':'') + '">' + (currWPct != null ? currWPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right" title="' + (newWPct!=null? newWPct.toFixed(2)+'%':'') + '">' + (newWPct != null ? newWPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right ' + deltaClass + '">' + (deltaPct != null ? deltaPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (deltaAmt != null ? Math.round(deltaAmt).toLocaleString('en-DK') : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (deltaVol != null ? deltaVol.toFixed(2) : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (dtc != null ? dtc.toFixed(2) : '') + '</td>'
                    + '</tr>';
                }).join('');
                const header = '<tr>'
                  + '<th class="px-3 py-2 sticky left-0 bg-gray-100 cursor-pointer" data-qsort="issuer">Company Name</th>'
                  + '<th class="px-3 py-2 text-right cursor-pointer" data-qsort="mcap">Market Cap, bn</th>'
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
                document.getElementById('quarterlyTable').innerHTML = qControls + '<div class="overflow-auto"><table class="w-full text-left"><thead class="bg-gray-100">' + header + '</thead><tbody>' + (trs || '<tr><td class="px-3 py-4 text-sm text-slate-500" colspan="8">No data.</td></tr>') + '</tbody></table></div>';
                // Quarterly control handlers
                document.getElementById('qTop15').onclick = ()=>{ quarterlyLimit=15; loadQuarterly(); };
                document.getElementById('qTop50').onclick = ()=>{ quarterlyLimit=50; loadQuarterly(); };
                document.getElementById('quarterlyCsv').onclick = ()=>{
                  try {
                    const header = ['issuer','mcap_bn','curr_weight_pct','proforma_weight_pct','delta_pct','delta_amt','delta_vol','dtc'];
                    const csv = [header.join(',')].concat(top.map(r=>{
                      const issuer = (r.issuer || r.ticker || '');
                      const mcapBn = (r.mcap_bn != null) ? Number(r.mcap_bn) : (r.mcap != null ? (Number(r.mcap) / 1e9) : (r.mcap_uncapped != null ? (Number(r.mcap_uncapped) / 1e9) : ''));
                      const currW = (r.old_weight != null) ? Number(r.old_weight) : null;
                      const newW = (r.new_weight != null) ? Number(r.new_weight) : null;
                      const deltaFrac = (currW != null && newW != null) ? (newW - currW) : null;
                      const currWPct = (currW != null) ? (currW * 100).toFixed(2) : '';
                      const newWPct = (newW != null) ? (newW * 100).toFixed(2) : '';
                      const deltaPct = (deltaFrac != null) ? (deltaFrac * 100).toFixed(2) : '';
                      const deltaAmt = (aum && deltaFrac != null) ? Math.round(aum * deltaFrac) : '';
                      const deltaVol = (r.delta_vol != null) ? Number(r.delta_vol).toFixed(2) : '';
                      const dtc = (r.days_to_cover != null) ? Number(r.days_to_cover).toFixed(2) : '';
                      return [JSON.stringify(issuer), mcapBn, currWPct, newWPct, deltaPct, deltaAmt, deltaVol, dtc].join(',');
                    })).join('\\n');
                    const blob = new Blob([csv], {type:'text/csv'});
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a'); a.href=url; a.download=selected+'_quarterly.csv'; a.click(); URL.revokeObjectURL(url);
                  } catch(e){}
                };
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
                const r = await fetch('/api/index/' + selected + '/constituents');
                const json = await r.json();
                const rows = json.rows || [];
                const totalW = rows.reduce((s, r) => s + (Number(r.weight) || 0), 0);
                document.getElementById('dailyMeta').textContent = 'As of: ' + (json.asOf || 'unknown') + ' · Rows: ' + rows.length + ' · Sum(weight): ' + (totalW ? totalW.toFixed(6) : 'n/a') + ' · ' + '<a class="text-blue-600" href="/api/index/' + selected + '/constituents">JSON</a>';
                const top = rows.slice(0, 25);
                const region = regionFor(selected);
                const aum = (selectedMeta.aum != null ? selectedMeta.aum : (aumByRegion[region] || null));
                const ccy = selectedMeta.ccy || currencyByRegion[region] || '';
                dailyRowsCache = rows.slice();
                const sorted = sortRows(dailyRowsCache, (r)=> Number(r.capped_weight ?? r.weight ?? 0), dailySort.dir);
                const topN = sorted.slice(0, dailyLimit);
                const trs = topN.map(r => {
                  const hasCapDiff = (typeof r.capped_weight === 'number' && typeof r.weight === 'number' && Math.abs(r.capped_weight - r.weight) > 1e-9);
                  const flags = (r.flags && String(r.flags).trim()) || (hasCapDiff ? 'capped' : '');
                  const rowClass = flags.includes('40% breach') ? 'bg-red-50' : (flags.includes('10% breach') ? 'bg-yellow-50' : '');
                  const mcapRaw = (r.market_cap != null ? Number(r.market_cap) : (r.mcap != null ? Number(r.mcap) : null));
                  const mcapBn = (mcapRaw != null) ? (mcapRaw / 1e9) : null;
                  const wPct = (r.weight != null) ? (Number(r.weight) * 100) : null;
                  const cwPct = (r.capped_weight != null) ? (Number(r.capped_weight) * 100) : null;
                  const deltaFrac = (typeof r.delta_pct === 'number') ? Number(r.delta_pct) : null;
                  const deltaPct = (deltaFrac != null) ? (deltaFrac * 100) : null;
                  const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
                  const deltaVolShares = (deltaAmt != null && r.price != null) ? (deltaAmt / Number(r.price)) : null;
                  const adv = (r.avg_daily_volume != null ? Number(r.avg_daily_volume) : null);
                  const dtc = (deltaVolShares != null && adv != null && adv > 0) ? (Math.abs(deltaVolShares) / adv) : null;
                  const displayName = (r.name || r.issuer || r.ticker || '');
                  const id = 'row-' + sanitizeId(displayName);
                  const deltaClass = (deltaPct!=null && deltaPct>0) ? 'text-green-700' : (deltaPct!=null && deltaPct<0 ? 'text-red-700' : '');
                  const cutPill = flags.includes('40% breach') ? '<span class="ml-2 inline-block px-2 py-0.5 rounded-full text-xs bg-red-100 text-red-800">cut candidate</span>' : '';
                  return (
                    '<tr id="' + id + '" class="border-b ' + rowClass + '">'
                    + '<td class="px-3 py-2 text-sm sticky left-0 bg-white">' + displayName + cutPill + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (mcapBn != null ? mcapBn.toFixed(2) : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right" title="' + (wPct!=null? wPct.toFixed(2)+'%':'') + '">' + (wPct != null ? wPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right" title="' + (cwPct!=null? cwPct.toFixed(2)+'%':'') + '">' + (cwPct != null ? cwPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right ' + deltaClass + '">' + (deltaPct != null ? deltaPct.toFixed(2) + '%' : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (deltaAmt != null ? Math.round(deltaAmt).toLocaleString('en-DK') : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm text-right">' + (dtc != null ? dtc.toFixed(2) : '') + '</td>'
                    + '<td class="px-3 py-2 text-sm">' + (flags ? '<span class="text-red-700 font-semibold" title="' + flags + '">' + flags + '</span>' : '') + '</td>'
                    + '</tr>'
                  );
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
                  summary.push(badge('AUM ' + (ccy || '') + ': ' + (aum ? aum.toLocaleString('en-DK') : 'n/a')));
                  summary.push(badge('Rows: ' + rows.length));
                  document.getElementById('summaryBar').innerHTML = summary.join(' ');
                } catch (e) {}
                // Table with controls
                const controls = '<div class="flex items-center gap-2 mb-2"><button id="dailyTop25" class="px-2 py-1 border rounded text-xs">Top 25</button><button id="dailyTop100" class="px-2 py-1 border rounded text-xs">Show 100</button><button id="dailyCsv" class="px-2 py-1 border rounded text-xs">Export CSV</button></div>';
                document.getElementById('dailyTable').innerHTML = controls + '<div class="overflow-auto"><table class="w-full text-left"><thead class="bg-gray-100"><tr><th class="px-3 py-2 sticky left-0 bg-gray-100 cursor-pointer" data-dsort="issuer">Company Name</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="mcap">Market Cap, bn</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="weight">Weight</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="capped_weight">Capped Weight</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="delta_pct">Delta, %</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="delta_amt">Delta, ' + (ccy || 'Amt') + '</th><th class="px-3 py-2 text-right cursor-pointer" data-dsort="dtc">Days to Cover</th><th class="px-3 py-2">Flags</th></tr></thead><tbody>' + (trs || '<tr><td class="px-3 py-4 text-sm text-slate-500" colspan="8">No data.</td></tr>') + '</tbody></table></div>';
                // Header click sorting (Daily)
                function getDailyVal(row){
                  const issuer = (row.name || row.issuer || row.ticker || '').toLowerCase();
                  const mcap = (row.market_cap != null ? Number(row.market_cap) : (row.mcap != null ? Number(row.mcap) : null));
                  const weight = (row.weight != null ? Number(row.weight) : null);
                  const cweight = (row.capped_weight != null ? Number(row.capped_weight) : null);
                  const deltaFrac = (typeof row.delta_pct === 'number') ? Number(row.delta_pct) : null;
                  const deltaAmt = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
                  const dtcVal = (() => {
                    const deltaVolShares = (deltaAmt != null && row.price != null) ? (deltaAmt / Number(row.price)) : null;
                    return (deltaVolShares != null && row.avg_daily_volume != null) ? (Math.abs(deltaVolShares) / Number(row.avg_daily_volume)) : null;
                  })();
                  if (dailySort.key==='issuer') return issuer;
                  if (dailySort.key==='mcap') return mcap;
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
                      const wPct = (r.weight != null) ? (Number(r.weight) * 100) : null;
                      const cwPct = (r.capped_weight != null) ? (Number(r.capped_weight) * 100) : null;
                      const deltaFrac = (typeof r.delta_pct === 'number') ? Number(r.delta_pct) : null;
                      const deltaPct = (deltaFrac != null) ? (deltaFrac * 100) : null;
                      const deltaAmt2 = (aum && deltaFrac != null) ? (aum * deltaFrac) : null;
                      const deltaVolShares = (deltaAmt2 != null && r.price != null) ? (deltaAmt2 / Number(r.price)) : null;
                      const adv2 = (r.avg_daily_volume != null ? Number(r.avg_daily_volume) : null);
                      const dtc2 = (deltaVolShares != null && adv2 != null && adv2 > 0) ? (Math.abs(deltaVolShares) / adv2) : null;
                      const name = (r.name || r.issuer || r.ticker || '');
                      const id = 'row-' + sanitizeId(name);
                      const deltaClass = (deltaPct!=null && deltaPct>0) ? 'text-green-700' : (deltaPct!=null && deltaPct<0 ? 'text-red-700' : '');
                      const cutPill = flags.includes('40% breach') ? '<span class="ml-2 inline-block px-2 py-0.5 rounded-full text-xs bg-red-100 text-red-800">cut candidate</span>' : '';
                      return (
                        '<tr id="' + id + '" class="border-b ' + rowClass + '">'
                        + '<td class="px-3 py-2 text-sm sticky left-0 bg-white">' + (r.issuer || '') + cutPill + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right">' + (mcapBn != null ? mcapBn.toFixed(2) : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right" title="' + (wPct!=null? wPct.toFixed(2)+'%':'') + '">' + (wPct != null ? wPct.toFixed(2) + '%' : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right" title="' + (cwPct!=null? cwPct.toFixed(2)+'%':'') + '">' + (cwPct != null ? cwPct.toFixed(2) + '%' : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right ' + deltaClass + '">' + (deltaPct != null ? deltaPct.toFixed(2) + '%' : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right">' + (deltaAmt2 != null ? Math.round(deltaAmt2).toLocaleString('en-DK') : '') + '</td>'
                        + '<td class="px-3 py-2 text-sm text-right">' + (dtc2 != null ? dtc2.toFixed(2) : '') + '</td>'
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
                      const deltaVolShares = (typeof r.delta_pct==='number' && r.price!=null? (aum*Number(r.delta_pct)/Number(r.price)): null);
                      const adv = (r.avg_daily_volume!=null? Number(r.avg_daily_volume): null);
                      const dtcV = (deltaVolShares!=null && adv!=null && adv>0? (Math.abs(deltaVolShares)/adv).toFixed(2): '');
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

            // Auto-refresh countdown and control
            setInterval(() => {
              if (pauseCb && pauseCb.checked) { refreshTicker.textContent = 'auto-refresh paused'; return; }
              nextRefreshSec = Math.max(0, nextRefreshSec - 1);
              const m = String(Math.floor(nextRefreshSec/60)).padStart(2,'0');
              const s = String(nextRefreshSec%60).padStart(2,'0');
              refreshTicker.textContent = 'auto-refresh in ' + m + ':' + s;
              if (nextRefreshSec === 0) { loadAll(); }
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
app.get('/product/kaxcap', (req, res) => res.redirect(302, '/kaxcap'));

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
              <a href="${linkedin}" target="_blank" rel="noopener" class="inline-block bg-blue-600 text-white px-4 py-2 rounded">Contact (LinkedIn) →</a>
              <a href="/" class="ml-4 text-blue-600">← Back</a>
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

// Product page: Rebalancer dashboard
app.get('/product/rebalancer', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('index_proposals')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(50);

    const rows = (error || !data) ? [] : data;
    const listItems = rows.map(r => {
      const payload = r.payload || {};
      const indexId = payload.indexId || payload.index_id || 'unknown';
      const created = r.created_at ? new Date(r.created_at).toISOString() : '';
      return `<li class="mb-3 border rounded p-3 bg-gray-50"><strong>${indexId}</strong> — ${created} <pre class="mt-2 text-xs">${JSON.stringify(payload, null, 2)}</pre></li>`;
    }).join('');

    res.send(`
  <html>
    <head>${renderHead('Index Rebalancer — Proposals')}</head>
    <body class="bg-gray-50 p-6">
      ${renderHeader()}
      <main class="max-w-6xl mx-auto p-6">
        <div class="bg-white rounded-xl shadow p-6">
          <h1 class="text-3xl font-bold mb-4">Index Rebalancer — Proposals</h1>
          <p class="text-sm text-gray-600 mb-4">This page shows persisted rebalancing proposals (from Supabase) and lets you run live or quarterly previews using the compute engine.</p>

          <section class="mb-6 p-4 border rounded bg-gray-50">
            <h2 class="font-semibold mb-2">Live / Preview Controls</h2>
            <div class="grid md:grid-cols-3 gap-3 items-end">
              <div>
                <label class="text-xs text-slate-600">Market</label>
                <select id="region" class="w-full px-3 py-2 rounded border">
                  <option value="CPH">Copenhagen (CPH)</option>
                  <option value="HEL">Helsinki (HEL)</option>
                  <option value="STO">Stockholm (STO)</option>
                </select>
              </div>
              <div>
                <label class="text-xs text-slate-600">Index</label>
                <input id="indexId" class="w-full px-3 py-2 rounded border bg-gray-100" readonly />
                <div class="text-xs text-slate-500 mt-1">Auto-selected from market</div>
              </div>
              <div>
                <label class="text-xs text-slate-600">Quarterly preview</label>
                <div class="flex items-center gap-2">
                  <input type="checkbox" id="quarterly" />
                  <label for="quarterly" class="text-sm">Enable quarterly exceptions</label>
                </div>
              </div>
            </div>
            <div class="mt-3 flex gap-3">
              <button id="computeBtn" class="px-4 py-2 bg-blue-600 text-white rounded">Compute Preview</button>
              <button id="showPersisted" class="px-4 py-2 border rounded">Show Persisted Proposals</button>
            </div>
          </section>

          <section id="results" class="mb-6 p-4 border rounded bg-white">
            <h2 class="font-semibold mb-2">Preview Results</h2>
            <div id="resultsMeta" class="text-sm text-slate-600 mb-3">No preview computed yet.</div>
            <div id="resultsTable"></div>
          </section>

          <section class="mt-6">
            <h2 class="font-semibold mb-2">Recent Persisted Proposals</h2>
            <ul>${listItems || '<li class="text-sm text-gray-500">No proposals found</li>'}</ul>
          </section>

          <div class="mt-6"><a href="/watchers" class="text-blue-600">← Back to Watchers</a></div>
        </div>
      </main>
    ${renderFooter()}

    <script>
      async function renderProposal(proposal) {
        const meta = proposal.meta || {};
        document.getElementById('resultsMeta').textContent =
          'Generated: ' + (meta.generated_at || '') + ' — Method: ' + (meta.method || '');
        const rows = proposal.proposed || [];
        // Group by issuer to avoid double-counting multiple tickers/classes
        const grouped = {};
        for (const r of rows) {
          const key = String(r.issuer || '').trim();
          if (!grouped[key]) grouped[key] = { issuer: key, oldWeight: 0, newWeight: 0, capped: false };
          grouped[key].oldWeight += Number(r.oldWeight || 0);
          grouped[key].newWeight += Number(r.newWeight || 0);
          grouped[key].capped = grouped[key].capped || !!r.capped;
        }
        const agg = Object.values(grouped);
        agg.sort((a,b) => (b.newWeight || 0) - (a.newWeight || 0));
        const top = agg.slice(0, 200);

        // Build without template literals to avoid nested backticks
        const tableRows = top.map(function(r) {
          return '<tr class="border-b">'
            + '<td class="px-3 py-2 text-sm">' + (r.issuer || '') + '</td>'
            + '<td class="px-3 py-2 text-sm">' + ((r.oldWeight || 0) * 100).toFixed(2) + '%</td>'
            + '<td class="px-3 py-2 text-sm">' + ((r.newWeight || 0) * 100).toFixed(2) + '%</td>'
            + '<td class="px-3 py-2 text-sm">' + (r.capped ? '<span class="text-red-600 font-semibold">capped</span>' : '') + '</td>'
            + '</tr>';
        }).join('');

        document.getElementById('resultsTable').innerHTML =
          '<div class="overflow-auto">'
          + '<table class="w-full text-left">'
          + '<thead><tr class="bg-gray-100"><th class="px-3 py-2">Issuer</th><th class="px-3 py-2">Old W</th><th class="px-3 py-2">New W</th><th class="px-3 py-2">Flags</th></tr></thead>'
          + '<tbody>' + tableRows + '</tbody>'
          + '</table>'
          + '</div>';
      }

      const helId = '${process.env.HEL_INDEX_ID || 'HELXCAP'}';
      const stoId = '${process.env.STO_INDEX_ID || 'OMXSALLS'}';
      const cphId = 'KAXCAP';
      const regionSelect = document.getElementById('region');
      const indexInput = document.getElementById('indexId');
      function updateIndexId(){
        const r = regionSelect.value;
        indexInput.value = r === 'HEL' ? helId : (r === 'STO' ? stoId : cphId);
      }
      updateIndexId();
      regionSelect.addEventListener('change', updateIndexId);

      document.getElementById('computeBtn')?.addEventListener('click', async () => {
        const indexId = indexInput.value || cphId;
        const region = regionSelect.value || 'CPH';
        const quarterly = document.getElementById('quarterly').checked;
        document.getElementById('resultsMeta').textContent = 'Computing…';
        try {
          const res = await fetch('/api/rebalancer/compute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ indexId, options: { region, quarterly } })
          });
          const json = await res.json();
          if (json.error) {
            document.getElementById('resultsMeta').textContent = 'Error: ' + json.error;
            document.getElementById('resultsTable').innerHTML = '';
            return;
          }
          renderProposal(json.proposal);
        } catch (e) {
          document.getElementById('resultsMeta').textContent = 'Compute failed: ' + (e && e.message ? e.message : e);
        }
      });

      document.getElementById('showPersisted')?.addEventListener('click', () => {
        window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
      });
    </script>

    </body>
  </html>
    `);
  } catch (e) {
    console.error('[ui] rebalancer page failed:', e && e.message ? e.message : e);
    res.status(500).send('Failed to load rebalancer proposals');
  }
});

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
  // Run FactSet worker batch once on startup to populate tables
  (async () => {
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
          execFile(pythonCmd, [scriptPath, '--region', r.region, '--index-id', r.indexId], { env: process.env }, (error, stdout, stderr) => {
            if (error) {
              console.error('[startup] FactSet run error', r, error);
              writeSchedulerLog(`startup FactSet error region=${r.region} index=${r.indexId}: ${error && error.message ? error.message : String(error)}`);
            } else {
              console.log('[startup] FactSet run ok', r, stdout);
              writeSchedulerLog(`startup FactSet ok region=${r.region} index=${r.indexId}`);
            }
            resolve();
          });
        });
      }
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

    // Trigger FactSet worker for all markets (daily)
    try {
      const pythonCmd = process.env.PYTHON || 'python3';
      const scriptPath = path.join(__dirname, 'workers', 'indexes', 'main.py');
      const runs = [
        { region: 'CPH', indexId: process.env.CPH_INDEX_ID || 'KAXCAP' },
        { region: 'HEL', indexId: process.env.HEL_INDEX_ID || 'HELXCAP' }
        // Stockholm paused
      ];
      for (const r of runs) {
        await new Promise((resolve) => {
          execFile(pythonCmd, [scriptPath, '--region', r.region, '--index-id', r.indexId], { env: process.env }, (error, stdout, stderr) => {
            if (error) {
              console.error('[scheduler] FactSet run error', r, error);
              writeSchedulerLog(`FactSet run error region=${r.region} index=${r.indexId}: ${error && error.message ? error.message : String(error)}`);
            } else {
              console.log('[scheduler] FactSet run ok', r, stdout);
              writeSchedulerLog(`FactSet run ok region=${r.region} index=${r.indexId}`);
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
