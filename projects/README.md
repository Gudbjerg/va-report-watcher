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
