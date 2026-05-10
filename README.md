# AutoTrend Atlas

AutoTrend Atlas is a zero-paid-infra intelligence engine that ingests public RSS feeds, scores stories locally, stores its own rolling memory in the repo, and publishes a world-class static dashboard on GitHub Pages.

## What it does

- pulls a wide RSS/query mesh across geopolitics, cyber, AI, macro, consumer tech, climate, and APAC
- deduplicates and ranks stories with local heuristics only
- tracks rolling memory in `output/cache.json`
- stores daily snapshots in `output/history/*.json`
- computes momentum, rising themes, and region heat from local history
- generates a fully static, animated dashboard plus RSS and JSON artifacts
- deploys automatically with GitHub Actions + GitHub Pages

## Stack

- Python 3.11
- `feedparser`, `requests`, `beautifulsoup4`, `jinja2`, `lxml`, `vaderSentiment`
- GitHub Actions for scheduled runs
- GitHub Pages for static hosting
- No AI APIs, no external memory APIs, no database, no server

## Run locally

```bash
pip install -r requirements.txt
python app.py
```

The generated site and data land in `output/`:

- `output/index.html`
- `output/trends.json`
- `output/trending.rss`
- `output/cache.json`
- `output/history/*.json`

## How memory works

This project is intentionally "git as memory":

- short-term memory: `output/cache.json`
- expiry control: `output/expiry.json`
- long-term memory: `output/history/*.json`
- permanent audit trail: git commits made by Actions

That means the dashboard can evolve over time without any hosted memory layer.

## Automation

The workflow in `.github/workflows/autotrend.yml`:

- runs every 20 minutes
- rebuilds the dashboard
- commits updated output/history back into the repo
- deploys the latest static output to GitHub Pages

## Notes

- This is optimized for free-tier stability, so it scales through a broad RSS/query mesh rather than expensive full-page crawling.
- The rolling memory window can accumulate thousands of deduplicated stories across runs while staying GitHub Actions friendly.
