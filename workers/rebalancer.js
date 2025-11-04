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
    // Normalize top-level columns: index_id and status for efficient queries
    const indexId = record.indexId || record.index_id || (record.payload && record.payload.indexId) || null;
    const status = record.status || 'pending';

    const insertRow = {
        index_id: indexId,
        payload: record,
        status
    };

    const { data, error } = await supabase.from('index_proposals').insert([insertRow]);
    if (error) throw error;
    const inserted = Array.isArray(data) && data.length ? data[0] : null;

    const proposalItems = record.proposal || (record.payload && record.payload.proposal) || [];
    if (inserted && Array.isArray(proposalItems) && proposalItems.length) {
        const rows = proposalItems.map(p => ({
            proposal_id: inserted.id,
            name: p.name || p.company || null,
            ticker: p.ticker || p.symbol || null,
            price: p.price != null ? p.price : (p.px || null),
            shares: p.shares != null ? p.shares : (p.raw_shares || null),
            shares_capped: p.shares_capped != null ? p.shares_capped : (p.capped_shares || null),
            mcap: p.mcap != null ? p.mcap : (p.market_cap || null),
            mcap_capped: p.mcap_capped != null ? p.mcap_capped : (p.capped_mcap || null),
            avg_30d_volume: p.avg_30d_volume != null ? p.avg_30d_volume : (p.avg30 || null),
            weight: p.weight != null ? p.weight : (p.currentWeight != null ? p.currentWeight : null),
            weight_capped: p.weight_capped != null ? p.weight_capped : (p.targetWeight != null ? p.targetWeight : null)
        }));

        const { error: cError } = await supabase.from('proposal_constituents').insert(rows);
        if (cError) console.error('[persistProposal] failed to insert constituents:', cError);
    }

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
