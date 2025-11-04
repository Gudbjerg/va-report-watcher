## Projects / Watchers - HOWTO

This repository uses a small project-first layout: each project should live under `projects/<project-name>/watchers/*.js`.

Watcher contract (minimal):

- Export a function that runs a single check and returns an object describing what it found.
- Recommended function name: `runWatcher()` or export the function as the module default.
- For eSundhed-style watchers you may export named helpers like `fetchLatestEsundhedReport()` plus a public `checkEsundhedUpdate()` for the runtime.

Return shape (recommended):

- For VA watcher: `{ month: 'YYYY-MM' }` when there is a result, else `{}`.
- For eSundhed watcher: `{ filename: '...', hash: '...', url: '...' }` when a file was found, else `{}`.

Examples and best-practices

- Use the central `lib/sendEmail.js` to send attachments and respect the `DISABLE_EMAIL=true` guard.
- Keep side-effects (DB writes) minimal inside the watcher function — prefer returning found metadata and let the caller/updater perform persistence.
- Expose small, testable helper functions (e.g. `fetchLatestEsundhedReport()`) and keep a thin runtime wrapper that calls them.

Testing

- Place developer test scripts in the `tests/` directory. The test scripts can `require('../projects/<project>/watchers/<file>')` to load helpers.

Adding a new project

1. Create `projects/<project-name>/watchers/mywatcher.js` following the contract above.
2. Add any environment variables (document them in this README or the project's own README).
3. Run the tests locally from `npm run smoke` (or run the individual scripts in `tests/`).

Notes

- The server autodiscovers watchers at startup and will log which watchers it found. The dashboard will list discovered watchers and provide links to manually trigger them (`/scrape/<key>`).
- Legacy watchers may still exist in the old `watchers/` folder; they are kept only for compatibility until the project watchers are verified.
# Projects directory

This folder lists the three top-level projects the site will present:

- Analyst Scraper (VA & eSundhed watchers)
- AI Analyst (analysis and summarization pipeline)
- KAXCAP Index (index tracking and rebalancer)

Planned structure:

- projects/
  - analyst-scraper/  -> watcher code, reporters, historical data
  - ai-analyst/       -> analysis pipelines, LLM integration
  - kaxcap-index/     -> index calculation, rebalancer logic

For now these are placeholders; as we scale we can move each project's code into its own subfolder and expose API endpoints under `/api/{project}`.
