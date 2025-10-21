// debug/supabase-check.js
// Quick connectivity probe for Render (run only when DEBUG_SUPABASE=1)
// This file intentionally logs only connectivity info and does not perform DB writes.

const fetch = require('node-fetch');

const url = (process.env.SUPABASE_URL || '').trim();
const key = (process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY || '').trim();

console.log('[debug] SUPABASE_URL present:', !!url, 'length:', url.length ? url.length : 0);
console.log('[debug] SUPABASE_KEY present:', !!key, 'length:', key.length ? key.length : 0);

if (!url || !key) {
    console.error('[debug] Missing SUPABASE_URL or SUPABASE_KEY in env.');
    return;
}

(async () => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);

    try {
        const probeUrl = url.replace(/\/$/, '') + '/rest/v1';
        console.log('[debug] Probing URL:', probeUrl);
        const resp = await fetch(probeUrl, {
            method: 'GET',
            headers: { apikey: key, Authorization: `Bearer ${key}` },
            signal: controller.signal
        });
        clearTimeout(timeout);
        console.log('[debug] fetch status:', resp.status, resp.statusText);
        const text = await resp.text().catch(() => '<no body>');
        console.log('[debug] body preview:', (text || '').slice(0, 200));
    } catch (err) {
        if (err.name === 'AbortError') {
            console.error('[debug] Connection timed out (fetch aborted)');
        } else {
            console.error('[debug] Fetch error:', err && err.message ? err.message : err);
        }
    }
})();

