// DEPRECATED shim for legacy path watchers/esundhed.js
// Prefer: projects/analyst-scraper/watchers/esundhed.js
module.exports = {
  checkEsundhedUpdate: async function deprecatedEsundhed() {
    console.warn('[deprecated] legacy shim watchers/esundhed.js invoked — use projects/analyst-scraper/watchers/esundhed.js instead');
    return { filename: null };
  }
};