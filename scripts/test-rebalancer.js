const { computeProposal } = require('../projects/kaxcap-index/rebalancer');

// Sample data with two securities per issuer to exercise issuer-level capping
const sample = [
    { ticker: 'AAA', issuer: 'AAA', price: 100, mcap: 1_000_000, avg_30d_volume: 10000, currentWeight: 0.05 },
    { ticker: 'AAB', issuer: 'AAA', price: 50, mcap: 500_000, avg_30d_volume: 8000, currentWeight: 0.02 },
    { ticker: 'BBB', issuer: 'BBB', price: 50, mcap: 3_000_000, avg_30d_volume: 20000, currentWeight: 0.10 },
    { ticker: 'CCC', issuer: 'CCC', price: 200, mcap: 700_000, avg_30d_volume: 5000, currentWeight: 0.02 },
    { ticker: 'DDD', issuer: 'DDD', price: 80, mcap: 2_500_000, avg_30d_volume: 12000, currentWeight: 0.08 }
];

// Compute a daily capped Copenhagen proposal
const proposalDaily = computeProposal('OMXCCAP-TEST', sample, { mode: 'capped', region: 'CPH', quarterly: false });
console.log('Daily Capped Proposal (CPH):', JSON.stringify(proposalDaily, null, 2));

// Compute a quarterly capped Copenhagen proposal
const proposalQuarterly = computeProposal('OMXCCAP-TEST', sample, { mode: 'capped', region: 'CPH', quarterly: true });
console.log('Quarterly Capped Proposal (CPH):', JSON.stringify(proposalQuarterly, null, 2));
