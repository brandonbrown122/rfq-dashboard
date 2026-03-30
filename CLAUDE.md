# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Flask-based dashboard for monitoring Kalshi RFQ trading positions. Displays exposure breakdowns, fill velocity, PnL, and a cascade parlay visualizer. Deployed on Render.com.

## Commands

```bash
# Run locally
python server.py

# Production (as deployed on Render)
gunicorn server:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120

# Refresh positions from API
python refresh_positions.py --source api

# Refresh positions from local fills file
python refresh_positions.py

# Install dependencies
pip install -r requirements.txt
```

No test suite or linter is configured.

## Architecture

Four Python modules with clear separation:

- **server.py** — Flask app with 12+ JSON API endpoints and HTML serving. Runs a background auto-refresh thread on 5-minute intervals. Has an in-memory balance cache (30s TTL).
- **data_engine.py** — Pure data logic: parses fill blocks from `unified_rfq_fills.txt` via regex, aggregates exposure by sport/bet_type/creator/legs, computes per-leg exposure. Sport classification uses ticker suffixes, hardcoded team lists, and keyword matching.
- **refresh_positions.py** — Syncs positions from either the local fills file or Kalshi API. Parallel market info fetching (8 workers). Writes to `positions_cache.json` and `market_info_cache.json`.
- **kalshi_api.py** — Thin wrapper around `kalshi_python` SDK. Handles auth (PEM file locally, env var in cloud), fill pagination, market settlement checks with retry/backoff.

**Frontend** (`static/`): Vanilla JS with no framework. `index.html` is the positions dashboard; `cascade.html` is the cascade parlay visualizer. Both fetch from `/api/*` endpoints.

## Data Flow

1. `refresh_positions.py` pulls fills from API or local file → enriches with market info → writes `positions_cache.json`
2. `server.py` reads cache on API requests → `data_engine.py` parses/aggregates → JSON response
3. Frontend fetches JSON endpoints and renders tables/charts

## Key Caches

- `positions_cache.json` — All position data (open + settled). Primary data source for the dashboard.
- `market_info_cache.json` — Market titles, leg descriptions, settlement status. Prevents redundant API calls.

## Environment Variables

- `BILLY2_KALSHI_API_KEY_ID` — Kalshi API key ID
- `KALSHI_PRIVATE_KEY` — PEM key content (cloud deployment)
- Local dev uses `billy2_private_key.pem` file in parent directory

## Sport Classification

Multiple strategies in priority order: ticker suffix matching, hardcoded NBA/NHL/MLB team name lists, keyword/league matching. This is a frequent source of bugs — changes here require checking all classification paths in both `data_engine.py` and `refresh_positions.py`.
