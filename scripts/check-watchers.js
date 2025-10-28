const path = require('path');

function tryResolve(pRelativeFromRoot) {
    try {
        const absolute = path.resolve(__dirname, '..', pRelativeFromRoot);
        const res = require.resolve(absolute);
        console.log(`${pRelativeFromRoot} -> ${res}`);
    } catch (e) {
        console.error(`${pRelativeFromRoot} -> MISSING: ${e.message}`);
    }
}

tryResolve('projects/analyst-scraper/watchers/va.js');
tryResolve('projects/analyst-scraper/watchers/esundhed.js');
tryResolve('projects/ai-analyst/watchers/dummy.js');
tryResolve('projects/kaxcap-index/watchers/dummy.js');
console.log('check-watchers script complete');
