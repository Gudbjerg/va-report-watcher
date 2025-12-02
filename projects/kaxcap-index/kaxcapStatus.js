// projects/kaxcap-index/kaxcapStatus.js
const supabase = require('../../lib/supabaseClient');

async function getLatestKaxcapStatus(req, res) {
  try {
    // 1) latest as_of for KAXCAP
    const { data: dates, error: err1 } = await supabase
      .from('index_constituents')
      .select('as_of')
      .eq('index_id', 'KAXCAP')
      .order('as_of', { ascending: false })
      .limit(1);

    if (err1) return res.status(500).json({ error: err1.message });
    if (!dates || !dates.length) return res.json({ asOf: null, rows: [] });

    const asOf = dates[0].as_of;

    // 2) all rows for that as_of, ordered by weight
    const { data: rows, error: err2 } = await supabase
      .from('index_constituents')
      .select('*')
      .eq('index_id', 'KAXCAP')
      .eq('as_of', asOf)
      .order('weight', { ascending: false });

    if (err2) return res.status(500).json({ error: err2.message });

    return res.json({ asOf, rows });
  } catch (e) {
    console.error('[api] kaxcap status failed:', e && e.message ? e.message : e);
    return res.status(500).json({ error: String(e) });
  }
}

module.exports = { getLatestKaxcapStatus };
