/**
 * workers/rebalancer.js
 * Background worker skeleton for index rebalancing.
 *
 * Responsibilities:
 * - Fetch index data from FactSet (via lib/factset.js)
 * - Compute a rebalancing proposal (delta changes)
 * - Persist proposals to Supabase (table: index_proposals)
 * - Expose a small API surface for manual triggering
 */
const { getIndexData } = require('../lib/factset');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY
);

async function computeProposal(indexId, opts = {}) {
    // Fetch index constituents (FactSet or mock)
    const constituents = await getIndexData(indexId, opts);

    // VERY simple placeholder rebalancing logic: suggest scaling weights
    // to round percentages and produce a proposal object
    const proposal = constituents.map(c => ({
        ticker: c.ticker,
        currentWeight: c.weight,
        targetWeight: Math.round(c.weight * 100) / 100 // placeholder
    }));

    return { indexId, created_at: new Date().toISOString(), proposal };
}

async function persistProposal(record) {
    const { data, error } = await supabase.from('index_proposals').insert([{ payload: record }]);
    if (error) throw error;
    return data;
}

async function runRebalance(indexId, opts = {}) {
    const proposal = await computeProposal(indexId, opts);
    // Persist proposal if requested
    if (opts.persist !== false) {
        await persistProposal(proposal);
    }
    return proposal;
}

module.exports = { runRebalance, computeProposal, persistProposal };
