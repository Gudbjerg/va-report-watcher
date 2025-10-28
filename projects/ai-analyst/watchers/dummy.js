// Dummy watcher for AI Analyst project — placeholder to keep scheduler stable
module.exports = {
    runWatcher: async function runDummy() {
        console.log('[ai-analyst] dummy watcher heartbeat — no-op');
        // Return a shaped object similar to other watchers (no new report)
        return { ok: true };
    }
};
