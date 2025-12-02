// lib/supabaseClient.js
const { createClient } = require('@supabase/supabase-js');

function _stubQuery(result = { data: [], error: null }) {
  const q = {
    select: async () => result,
    order() { return q; },
    limit() { return q; },
    maybeSingle: async () => ({ data: null, error: null }),
    eq() { return q; }
  };
  return q;
}

const url = (process.env.SUPABASE_URL || '').trim();
const key = (
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_KEY ||
  ''
).trim();

let supabase;

if (url && key) {
  supabase = createClient(url, key);
} else {
  console.warn('[supabase] missing SUPABASE_URL or key – using stub client');
  supabase = { from: () => _stubQuery() };
}

module.exports = supabase;
