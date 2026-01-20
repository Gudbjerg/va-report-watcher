// projects/kaxcap-index/rebalancer.js
// Compute module to support /api/rebalancer/compute and scripts/test-rebalancer.js

function normalize(arr) {
    const s = arr.reduce((t, v) => t + (v > 0 ? v : 0), 0);
    if (s <= 0) return arr.map(() => 0);
    return arr.map(v => (v > 0 ? v / s : 0));
}

function capPerConstituent(base, cap) {
    const n = base.length;
    let w = base.slice();
    for (let iter = 0; iter < 1000; iter++) {
        const overIdx = [];
        for (let i = 0; i < n; i++) if (w[i] > cap) overIdx.push(i);
        if (overIdx.length === 0) break;
        let excess = 0;
        for (const i of overIdx) {
            excess += (w[i] - cap);
            w[i] = cap;
        }
        const underIdx = [];
        for (let i = 0; i < n; i++) if (!overIdx.includes(i) && w[i] > 0) underIdx.push(i);
        if (underIdx.length === 0) {
            w = normalize(w);
            break;
        }
        let underSum = 0;
        for (const i of underIdx) underSum += w[i];
        if (underSum <= 0) break;
        for (const i of underIdx) w[i] += excess * (w[i] / underSum);
    }
    return normalize(w);
}

function capPerIssuer(current, issuers, issuerCap) {
    if (!issuerCap || !Array.isArray(issuers) || issuers.length !== current.length) return current;
    let w = current.slice();
    const n = w.length;
    const maxIter = 1000;
    for (let iter = 0; iter < maxIter; iter++) {
        // compute group sums
        const groupSum = new Map();
        for (let i = 0; i < n; i++) {
            const g = issuers[i] || '';
            groupSum.set(g, (groupSum.get(g) || 0) + (w[i] || 0));
        }
        let changed = false;
        // scale down any group over cap
        for (const [g, sum] of groupSum.entries()) {
            if (sum > issuerCap + 1e-12) {
                const scale = issuerCap / sum;
                for (let i = 0; i < n; i++) if ((issuers[i] || '') === g && w[i] > 0) {
                    w[i] *= scale;
                    changed = true;
                }
            }
        }
        w = normalize(w);
        if (!changed) break;
    }
    return w;
}

function capWithConstraints(base, { capConstituent, issuers, capIssuer }) {
    let w = base.slice();
    const maxIter = 1000;
    for (let iter = 0; iter < maxIter; iter++) {
        const before = w.slice();
        if (typeof capConstituent === 'number') {
            w = capPerConstituent(w, capConstituent);
        }
        if (typeof capIssuer === 'number' && issuers) {
            w = capPerIssuer(w, issuers, capIssuer);
        }
        // check convergence
        let delta = 0;
        for (let i = 0; i < w.length; i++) delta += Math.abs(w[i] - before[i]);
        if (delta < 1e-12) break;
    }
    return normalize(w);
}

function computeProposal(indexId, data, options = {}) {
    const quarterly = !!options.quarterly;
    const maxWeight = typeof options.maxWeight === 'number' ? options.maxWeight : 0.10;
    const cap = maxWeight + (quarterly ? 0.02 : 0);
    const issuerCap = typeof options.issuerCap === 'number' ? options.issuerCap : undefined;

    const mcaps = data.map(r => Number(r.mcap || r.market_cap || 0));
    const base = normalize(mcaps);
    const issuers = data.map(r => r.issuer || r.name || r.ticker || '');
    const final = capWithConstraints(base, { capConstituent: cap, issuers, capIssuer: issuerCap });

    const rows = data.map((r, i) => ({
        ticker: String(r.ticker || r.symbol || '').toUpperCase(),
        issuer: r.issuer || r.name || r.ticker || '',
        oldWeight: Number(r.currentWeight || r.weight || 0),
        newWeight: Number(final[i] || 0),
        capped: (final[i] || 0) < (base[i] || 0) && (base[i] || 0) > 0
    }));

    rows.sort((a, b) => b.newWeight - a.newWeight);

    const proposal = {
        meta: {
            generated_at: new Date().toISOString(),
            method: `mcap_norm_capC_${cap.toFixed(2)}${issuerCap ? `_capI_${issuerCap.toFixed(2)}` : ''}`,
            indexId,
            quarterly
        },
        proposed: rows
    };
    return proposal;
}

module.exports = { computeProposal };
