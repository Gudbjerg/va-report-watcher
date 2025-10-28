// DEPRECATED shim for legacy path watchers/va.js
// Prefer: projects/analyst-scraper/watchers/va.js
module.exports = {
  runWatcher: async function deprecatedVA() {
    console.warn('[deprecated] legacy shim watchers/va.js invoked — use projects/analyst-scraper/watchers/va.js instead');
    return { month: null };
  }
};
