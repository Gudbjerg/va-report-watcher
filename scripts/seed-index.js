#!/usr/bin/env node
require('dotenv').config();
const supabase = require('../lib/supabaseClient');

(async () => {
    try {
        const today = new Date().toISOString().slice(0, 10);

        const indices = [
            { id: 'KAXCAP', region: 'CPH' },
            { id: process.env.HEL_INDEX_ID || 'HELXCAP', region: 'HEL' },
            { id: process.env.STO_INDEX_ID || 'OMXSALLS', region: 'STO' },
        ];

        // Seed daily (index_constituents)
        const dailyRows = [];
        for (const idx of indices) {
            dailyRows.push(
                { index_id: idx.id, ticker: 'AAA', issuer: 'AAA CORP', name: 'AAA CORP', price: 100, shares: 1000000, mcap: 100000000, weight: 0.10, avg_vol_30d: 10000, as_of: today, region: idx.region },
                { index_id: idx.id, ticker: 'BBB', issuer: 'BBB LTD', name: 'BBB LTD', price: 80, shares: 1500000, mcap: 120000000, weight: 0.12, avg_vol_30d: 15000, as_of: today, region: idx.region },
                { index_id: idx.id, ticker: 'CCC', issuer: 'CCC PLC', name: 'CCC PLC', price: 50, shares: 3000000, mcap: 150000000, weight: 0.15, avg_vol_30d: 20000, as_of: today, region: idx.region }
            );
        }

        // Delete existing rows for today
        for (const idx of indices) {
            await supabase.from('index_constituents').delete().eq('index_id', idx.id).eq('as_of', today);
        }

        const { error: dailyErr } = await supabase.from('index_constituents').insert(dailyRows);
        if (dailyErr) {
            console.error('[seed] daily insert error:', dailyErr.message || dailyErr);
        } else {
            console.log('[seed] daily inserted:', dailyRows.length);
        }

        // Seed quarterly (try index_quarterly_proforma, fallback to index_quarterly)
        const qTableCandidates = ['index_quarterly_proforma', 'index_quarterly_status', 'index_quarterly'];
        let qTable = null;
        for (const t of qTableCandidates) {
            try {
                const { data, error } = await supabase.from(t).select('as_of').limit(1);
                if (!error) { qTable = t; break; }
            } catch (e) { }
        }
        if (!qTable) {
            // Try to create a logical default: index_quarterly_proforma may not exist; we will attempt insert and log result
            qTable = 'index_quarterly_proforma';
        }

        const quarterlyRows = [];
        for (const idx of indices) {
            quarterlyRows.push(
                { index_id: idx.id, ticker: 'AAA', issuer: 'AAA CORP', old_weight: 0.095, new_weight: 0.070, flags: 'capped', as_of: today },
                { index_id: idx.id, ticker: 'BBB', issuer: 'BBB LTD', old_weight: 0.110, new_weight: 0.070, flags: 'capped', as_of: today },
                { index_id: idx.id, ticker: 'CCC', issuer: 'CCC PLC', old_weight: 0.060, new_weight: 0.060, flags: '', as_of: today }
            );
        }

        // Delete today's proforma rows if possible
        for (const idx of indices) {
            try { await supabase.from(qTable).delete().eq('index_id', idx.id).eq('as_of', today); } catch (e) { }
        }

        const { error: qErr } = await supabase.from(qTable).insert(quarterlyRows);
        if (qErr) {
            console.warn(`[seed] quarterly insert error on ${qTable}:`, qErr.message || qErr);
            console.warn('[seed] Note: quarterly table may not exist; skip for now.');
        } else {
            console.log('[seed] quarterly inserted:', quarterlyRows.length, 'into', qTable);
        }

        console.log('[seed] complete. Open /index to view tables.');
    } catch (e) {
        console.error('[seed] failed:', e && e.message ? e.message : e);
        process.exit(1);
    }
})();
