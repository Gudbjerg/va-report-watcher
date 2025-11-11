/**
 * Rebalancer for OMX All-Share Capped Indexes implementing Nasdaq methodology
 * - Supports uncapped (market cap) and capped indexes (Stockholm/ Copenhagen/ Helsinki variants)
 * - Implements the repeated two-stage daily weight adjustment described in the methodology
 *
 * Input: array of constituents with fields { ticker, issuer, price, mcap, avg_30d_volume, currentWeight }
 * - `issuer` is required for correct issuer-level caps; if missing ticker is used as issuer fallback
 *
 * Output: { indexId, meta, proposed: [{ ticker, issuer, price, mcap, oldWeight, newWeight, capped }] }
 */

function sum(arr, key) {
    return arr.reduce((s, x) => s + (Number(x[key]) || 0), 0);
}

function groupByIssuer(items) {
    const byIssuer = new Map();
    for (const it of items) {
        const issuer = String(it.issuer || it.ticker || '').toUpperCase();
        if (!byIssuer.has(issuer)) byIssuer.set(issuer, []);
        byIssuer.get(issuer).push(it);
    }
    return byIssuer;
}

function computeIssuerMcaps(items) {
    // items: normalized list with numeric mcap
    const byIssuer = groupByIssuer(items);
    const issuers = [];
    let total = 0;
    for (const [issuer, list] of byIssuer.entries()) {
        const mcap = list.reduce((s, x) => s + (Number(x.mcap) || 0), 0);
        issuers.push({ issuer, mcap, constituents: list });
        total += mcap;
    }
    // compute initial issuer weights
    for (const u of issuers) u.initWeight = total ? (u.mcap / total) : 0;
    // sort descending by mcap (useful for quarterly exceptions)
    issuers.sort((a, b) => b.mcap - a.mcap);
    return { issuers, total };
}

function applyQuarterlyExceptions(issuers, params) {
    // params: { cap: 0.045, exceptionCap: 0.07 (or 0.09), exceptionAggregateLimit: 0.36 }
    const { cap, exceptionCap, exceptionAggregateLimit } = params;
    // Determine maximum number of exception issuers by aggregate limit
    const maxExceptions = Math.floor(exceptionAggregateLimit / exceptionCap);
    const fixed = new Map();
    // select top N issuers by mcap as exceptions (they may have weights up to exceptionCap)
    for (let i = 0; i < issuers.length && i < maxExceptions; i++) {
        fixed.set(issuers[i].issuer, exceptionCap);
    }
    // Now rescale the rest proportionally to sum to (1 - sum(fixed))
    const sumFixed = Array.from(fixed.values()).reduce((s, x) => s + x, 0);
    const remaining = Math.max(0, 1 - sumFixed);
    const freeTotal = issuers.filter(u => !fixed.has(u.issuer)).reduce((s, u) => s + u.initWeight, 0) || 1;
    const final = new Map();
    for (const u of issuers) {
        if (fixed.has(u.issuer)) final.set(u.issuer, fixed.get(u.issuer));
        else final.set(u.issuer, (u.initWeight / freeTotal) * remaining);
    }
    return final;
}

function applyDailyCapping(issuers, params) {
    // Implements the repeated two-stage weight adjustment described in the methodology
    // params: { cap: 0.045, exceptionCap: 0.07|0.09 }
    const { cap, exceptionCap } = params;
    // copy initial weights
    const init = issuers.map(u => ({ issuer: u.issuer, w: u.initWeight }));

    // Fixed map holds issuer => fixedWeight that cannot be changed further (e.g., exceptionCap or 0.045)
    const fixed = new Map();

    // helper to compute tentative weights given fixed map
    function computeTentative() {
        const sumFixed = Array.from(fixed.values()).reduce((s, x) => s + x, 0);
        const remaining = Math.max(0, 1 - sumFixed);
        const freeTotal = init.filter(u => !fixed.has(u.issuer)).reduce((s, u) => s + u.w, 0) || 1;
        const tentative = new Map();
        for (const u of init) {
            if (fixed.has(u.issuer)) tentative.set(u.issuer, fixed.get(u.issuer));
            else tentative.set(u.issuer, (u.w / freeTotal) * remaining);
        }
        return tentative;
    }

    // Iteratively apply Stage1 and Stage2
    while (true) {
        // Stage1: any issuer with original weight > 10% is fixed to exceptionCap
        let added = false;
        for (const u of init) {
            if (!fixed.has(u.issuer) && u.w > 0.10) {
                fixed.set(u.issuer, exceptionCap);
                added = true;
            }
        }

        // Compute tentative after stage1
        const tentative = computeTentative();
        // Compute aggregate of issuers with tentative > 5%
        let agg = 0;
        const over5 = [];
        for (const [issuer, wt] of tentative.entries()) {
            if (wt > 0.05) {
                agg += wt;
                over5.push({ issuer, wt });
            }
        }

        if (agg <= 0.40 && !added) {
            // no adjustments needed beyond stage1 and no new additions -> finalize
            return tentative;
        }

        if (agg <= 0.40 && added) {
            // stage1 additions done and aggregate now OK -> finalize
            return tentative;
        }

        // Stage2: need to reduce some issuer(s): pick the issuer among those with tentative > 5% with the lowest tentative weight
        over5.sort((a, b) => a.w - b.w);
        // find the first issuer not already fixed to exceptionCap
        let chosen = null;
        for (const o of over5) {
            const curFixed = fixed.get(o.issuer);
            if (typeof curFixed === 'undefined' || curFixed !== exceptionCap) {
                chosen = o.issuer;
                break;
            }
        }
        if (!chosen) {
            // all over5 are already exception capped — return tentative to avoid infinite loop
            return tentative;
        }
        // fix chosen to cap (4.5%) and repeat
        fixed.set(chosen, cap);
        // loop continues until agg <= 0.40
    }
}

function distributeIssuerWeightsToConstituents(issuers, finalIssuerWeights) {
    // For each issuer, distribute final issuer weight to its constituents proportionally to their mcap
    const proposed = [];
    for (const u of issuers) {
        const finalIssWt = finalIssuerWeights.get(u.issuer) || 0;
        const totalMcap = u.constituents.reduce((s, c) => s + (Number(c.mcap) || 0), 0) || 1;
        for (const c of u.constituents) {
            const proportion = (Number(c.mcap) || 0) / totalMcap;
            const newWeight = finalIssWt * proportion;
            proposed.push(Object.assign({}, c, { newWeight }));
        }
    }
    return proposed;
}

function normalizeInput(data) {
    return data.map(d => ({
        ticker: String(d.ticker || '').toUpperCase(),
        issuer: String(d.issuer || d.ticker || '').toUpperCase(),
        price: typeof d.price !== 'undefined' ? Number(d.price) : 0,
        mcap: typeof d.mcap !== 'undefined' ? Number(d.mcap) : 0,
        avg_30d_volume: typeof d.avg_30d_volume !== 'undefined' ? Number(d.avg_30d_volume) : 0,
        currentWeight: typeof d.currentWeight !== 'undefined' ? Number(d.currentWeight) : 0
    }));
}

function computeProposal(indexId, data = [], options = {}) {
    // indexId: string like 'OMXCAPPGI' (Copenhagen capped). options.mode: 'capped'|'uncapped', options.region: 'CPH'|'HEL'|'STO'
    const mode = options.mode || (String(indexId || '').toLowerCase().includes('cap') ? 'capped' : 'uncapped');
    const region = (options.region || '').toUpperCase();

    const items = normalizeInput(data);
    const { issuers } = computeIssuerMcaps(items);

    // default parameters per region
    const paramsByRegion = {
        CPH: { cap: 0.045, exceptionCap: 0.07, exceptionAggregateLimit: 0.36 },
        HEL: { cap: 0.045, exceptionCap: 0.07, exceptionAggregateLimit: 0.36 },
        STO: { cap: 0.045, exceptionCap: 0.09, exceptionAggregateLimit: 0.36 }
    };
    const params = paramsByRegion[region] || { cap: 0.045, exceptionCap: 0.07, exceptionAggregateLimit: 0.36 };

    let finalIssuerWeightsMap;
    if (mode === 'uncapped') {
        finalIssuerWeightsMap = new Map(issuers.map(u => [u.issuer, u.initWeight]));
    } else {
        // For capped indexes, apply quarterly-style exceptions if options.quarterly === true, else daily
        if (options.quarterly) {
            finalIssuerWeightsMap = applyQuarterlyExceptions(issuers, params);
        } else {
            finalIssuerWeightsMap = applyDailyCapping(issuers, params);
        }
    }

    const proposed = distributeIssuerWeightsToConstituents(issuers, finalIssuerWeightsMap).map(p => ({
        ticker: p.ticker,
        issuer: p.issuer,
        price: p.price,
        mcap: p.mcap,
        oldWeight: p.currentWeight || 0,
        newWeight: p.newWeight || 0,
        capped: (p.newWeight || 0) < (p.currentWeight || 0)
    }));

    return {
        indexId: indexId || 'OMXCAPPGI',
        meta: { generated_at: new Date().toISOString(), method: mode === 'uncapped' ? 'market-cap' : 'capped-nasdaq-daily', region, params },
        proposed
    };
}

module.exports = { computeProposal };
