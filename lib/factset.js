/**
 * lib/factset.js
 * Minimal FactSet client stub for local development.
 *
 * - Reads FACTSET_API_KEY and FACTSET_BASE_URL from env
 * - If FACTSET_MOCK === 'true' returns deterministic mock data
 * - Exposes getIndexData(indexId) that returns an array of constituents
 */
const fetch = require('node-fetch');

const DEFAULT_MOCK = [
    { id: '1', ticker: 'AAPL', weight: 0.15, shares: 1000, price: 150, currency: 'USD' },
    { id: '2', ticker: 'MSFT', weight: 0.12, shares: 800, price: 300, currency: 'USD' },
    { id: '3', ticker: 'GOOG', weight: 0.10, shares: 500, price: 2800, currency: 'USD' }
];

async function getIndexData(indexId, opts = {}) {
    const mock = (process.env.FACTSET_MOCK || 'false') === 'true' || opts.mock;
    if (mock) {
        return DEFAULT_MOCK;
    }

    const apiKey = process.env.FACTSET_API_KEY;
    const base = process.env.FACTSET_BASE_URL || 'https://api.factset.com';
    if (!apiKey) {
        throw new Error('FACTSET_API_KEY not set (or enable FACTSET_MOCK=true for local dev)');
    }

    // Example endpoint (placeholder) — adapt to your FactSet contract
    const url = `${base}/indices/${encodeURIComponent(indexId)}/constituents`;

    const res = await fetch(url, {
        headers: {
            Authorization: `Bearer ${apiKey}`,
            Accept: 'application/json'
        },
        timeout: 15000
    });

    if (!res.ok) {
        const body = await res.text().catch(() => '');
        const err = new Error(`FactSet request failed: ${res.status} ${res.statusText}`);
        err.body = body;
        throw err;
    }

    const json = await res.json();
    // Map to normalized shape: { id, ticker, weight, shares, price, currency }
    return json.map(item => ({
        id: item.id || item.securityId || null,
        ticker: item.ticker || item.symbol || item.primaryTicker || null,
        weight: item.weight || item.targetWeight || 0,
        shares: item.shares || item.quantity || 0,
        price: item.price || item.lastPrice || 0,
        currency: item.currency || 'USD'
    }));
}

module.exports = { getIndexData };
