// Dummy watcher for KAXCAP Index project — placeholder to keep scheduler stable
module.exports = {
    runWatcher: async function runDummy() {
        console.log('[kaxcap-index] dummy watcher heartbeat — no-op');
        return { ok: true };
    }
};
