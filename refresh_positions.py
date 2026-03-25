#!/usr/bin/env python3
"""
Refresh open positions cache.

Two modes:
1. Local (default): Parses fills from unified_rfq_fills.txt (rich leg data)
2. API-only (--source api): Fetches fills from Kalshi API (for cloud deploy)

Usage:
    python refresh_positions.py [--days N]                # local mode, 7 days
    python refresh_positions.py --source api --days 7     # API-only mode
    python refresh_positions.py --days 0                  # all history
"""
import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from kalshi_api import get_client, get_market_settlement, get_fills, get_market_info
from data_engine import (
    parse_fill_blocks, save_cache, FILLS_FILE, CACHE_FILE,
    load_market_info_cache, save_market_info_cache,
)


NBA_TEAMS = {
    "ATLANTA", "HAWKS", "BOSTON", "CELTICS", "BROOKLYN", "NETS", "CHARLOTTE", "HORNETS",
    "CHICAGO", "BULLS", "CLEVELAND", "CAVALIERS", "DALLAS", "MAVERICKS", "DENVER", "NUGGETS",
    "DETROIT", "PISTONS", "GOLDEN STATE", "WARRIORS", "HOUSTON", "ROCKETS", "INDIANA", "PACERS",
    "LOS ANGELES L", "LAKERS", "LOS ANGELES C", "CLIPPERS", "MEMPHIS", "GRIZZLIES",
    "MIAMI", "HEAT", "MILWAUKEE", "BUCKS", "MINNESOTA", "TIMBERWOLVES", "NEW ORLEANS", "PELICANS",
    "NEW YORK", "KNICKS", "OKLAHOMA CITY", "THUNDER", "ORLANDO", "MAGIC", "PHILADELPHIA", "76ERS",
    "PHOENIX", "SUNS", "PORTLAND", "TRAIL BLAZERS", "BLAZERS", "SACRAMENTO", "KINGS",
    "SAN ANTONIO", "SPURS", "TORONTO", "RAPTORS", "UTAH", "JAZZ", "WASHINGTON", "WIZARDS",
}

NHL_TEAMS = {
    "BRUINS", "SABRES", "RED WINGS", "PANTHERS", "CANADIENS", "SENATORS", "LIGHTNING",
    "MAPLE LEAFS", "HURRICANES", "BLUE JACKETS", "DEVILS", "ISLANDERS", "RANGERS",
    "FLYERS", "PENGUINS", "CAPITALS", "BLACKHAWKS", "AVALANCHE", "STARS", "WILD",
    "PREDATORS", "BLUES", "JETS", "FLAMES", "OILERS", "CANUCKS", "DUCKS",
    "COYOTES", "KRAKEN", "SHARKS", "GOLDEN KNIGHTS", "KNIGHTS",
}


def _classify_sport_from_leg(leg_str):
    """Classify sport from a leg description."""
    upper = leg_str.upper()

    # Check ticker suffix if present
    if "KXMLB" in upper or "KXMVEMLB" in upper:
        return "mlb"
    if "KXNBA" in upper or "KXMVENBA" in upper:
        return "nba"
    if "KXNCAAMB" in upper or "KXNCAAB" in upper or "KXMVENCAAMB" in upper:
        return "ncaab"
    if "KXNHL" in upper or "KXMVENHL" in upper:
        return "nhl"
    if any(t in upper for t in ("KXEPL", "KXLALIGA", "KXSERIEA", "KXBUNDESLIGA", "KXLIGUE1", "KXUCL", "KXSOCCER")):
        return "soccer"

    # Bet type keywords for sport detection
    if any(k in upper for k in ("TOTAL RUNS", "HITS", "STRIKEOUTS", "RBI", "INNINGS", "HOME RUN", "EARNED RUN")):
        return "mlb"
    if any(k in upper for k in ("PTS", "REB", "AST", "3PM", "BLK", "STL", "POINTS SCORED")):
        return "nba"
    if "WINS BY OVER" in upper and "POINTS" in upper:
        return "nba"
    if any(k in upper for k in ("GOALS", "BTTS", "BOTH TEAMS TO SCORE", "CLEAN SHEET")):
        return "soccer"
    if any(k in upper for k in ("SAVES", "SHOTS ON GOAL")):
        return "nhl"

    # Strip "yes " / "no " prefix and check team names
    clean = upper
    if clean.startswith("YES "):
        clean = clean[4:]
    elif clean.startswith("NO "):
        clean = clean[3:]
    clean = clean.strip()
    if any(team in clean for team in NHL_TEAMS):
        return "nhl"
    if any(team in clean for team in NBA_TEAMS):
        return "nba"

    return "other"


def _classify_sport_from_ticker(ticker):
    """Classify sport from a Kalshi SGP ticker."""
    upper = (ticker or "").upper()
    if "MLB" in upper:
        return "mlb"
    if "NBA" in upper:
        return "nba"
    if "NCAAMB" in upper or "NCAAB" in upper:
        return "ncaab"
    if "NHL" in upper:
        return "nhl"
    if any(t in upper for t in ("EPL", "LALIGA", "SERIEA", "BUNDESLIGA", "LIGUE1", "UCL", "SOCCER")):
        return "soccer"
    # CROSSCATEGORY and SPORTSMULTIGAME tickers are multi-sport parlays
    if "CROSSCATEGORY" in upper or "SPORTSMULTIGAME" in upper:
        return "ncaab"  # default to ncaab since it's mostly college
    return "other"


def _extract_game_from_event(event_ticker):
    """Extract game matchup from event ticker.
    e.g. KXNBAGAME-26MAR13CLEDAL → CLE vs DAL
         KXEPLGAME-26MAR14BURBOU → BUR vs BOU
    """
    if not event_ticker:
        return ""
    # Pattern: ...-YYMMMDDXXXYYY where XXX and YYY are 3-letter team codes
    m = re.search(r'-\d{2}[A-Z]{3}\d{2}([A-Z]{3})([A-Z]{3})$', event_ticker)
    if m:
        return f"{m.group(1)} vs {m.group(2)}"
    # Try longer codes (some have 4+ chars)
    m = re.search(r'-\d{2}[A-Z]{3}\d{2}(.+)$', event_ticker)
    if m:
        code = m.group(1)
        if len(code) >= 6:
            half = len(code) // 2
            return f"{code[:half]} vs {code[half:]}"
    return ""


def _parse_legs_from_title(title):
    """Parse leg descriptions from a Kalshi market title.
    SGP titles use comma-separated legs like:
    'yes Team A,no Team B wins by over 6.5 Points,yes Over 159.5 points scored'
    """
    if not title:
        return []

    # Try comma-separated first (most SGP markets)
    parts = [p.strip() for p in title.split(",") if p.strip()]
    if len(parts) > 1:
        return parts

    # Try AND-separated
    parts = re.split(r'\s+AND\s+|\s+&\s+', title, flags=re.IGNORECASE)
    if len(parts) > 1:
        return [p.strip() for p in parts if p.strip()]

    return [title.strip()]


def fetch_fills_from_file(days):
    """Parse fills from the local text file."""
    print(f"[refresh] Reading fills from {FILLS_FILE.name}...")
    if not FILLS_FILE.exists():
        print("[refresh] No fills file found!")
        return []

    text = FILLS_FILE.read_text(errors="replace")
    all_fills = parse_fill_blocks(text)
    print(f"[refresh] Parsed {len(all_fills)} total fills")

    if days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        filtered = []
        for f in all_fills:
            try:
                ts = datetime.strptime(f["timestamp"], "%Y-%m-%d %H:%M:%S %Z").replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    filtered.append(f)
            except Exception:
                filtered.append(f)
        all_fills = filtered
        print(f"[refresh] After {days}-day filter: {len(all_fills)} fills")

    return all_fills


def fetch_fills_from_api(client, days):
    """Fetch fills from Kalshi API and enrich with market info."""
    print(f"[refresh] Fetching fills from Kalshi API (last {days} days)...")
    raw_fills = get_fills(client, days=days if days > 0 else None)
    print(f"[refresh] Got {len(raw_fills)} raw fills from API")

    # Group by ticker — we only care about NO-side fills (that's what the bot does)
    ticker_fills = {}
    for f in raw_fills:
        ticker = f.get("ticker", "")
        if not ticker:
            continue
        side = f.get("side", "")
        # Keep the latest fill per ticker
        if ticker not in ticker_fills:
            ticker_fills[ticker] = f
        else:
            # Keep the one with the latest timestamp
            if f.get("created_time", "") > ticker_fills[ticker].get("created_time", ""):
                ticker_fills[ticker] = f

    print(f"[refresh] {len(ticker_fills)} unique tickers")

    # Fetch market info for titles/legs (with persistent cache)
    market_info_cache = load_market_info_cache()
    market_info = {}
    tickers_to_fetch = []

    for t in ticker_fills:
        if t in market_info_cache:
            market_info[t] = market_info_cache[t]
        else:
            tickers_to_fetch.append(t)

    print(f"[refresh] Market info: {len(market_info)} cached, {len(tickers_to_fetch)} to fetch")

    if tickers_to_fetch:
        fetched = 0
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(get_market_info, client, t): t for t in tickers_to_fetch}
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    info = future.result()
                    if info:
                        # Store only the fields we need to keep cache small
                        cached = {
                            "title": info.get("title", ""),
                            "mve_selected_legs": info.get("mve_selected_legs", []),
                            "status": info.get("status", ""),
                            "result": info.get("result", ""),
                        }
                        market_info[ticker] = cached
                        market_info_cache[ticker] = cached
                except Exception:
                    pass
                fetched += 1
                if fetched % 100 == 0:
                    print(f"  Fetched market info: {fetched}/{len(tickers_to_fetch)}...")

        # Save updated cache
        save_market_info_cache(market_info_cache)
        print(f"[refresh] Fetched {fetched} new, total cache: {len(market_info_cache)}")

    # Build fill dicts matching the local-file format
    fills = []
    for ticker, raw in ticker_fills.items():
        info = market_info.get(ticker, {})
        title = info.get("title", "") or info.get("subtitle", "") or ""
        mve_legs = info.get("mve_selected_legs", [])

        # Parse basic fill data — API uses dollars, not cents
        no_price_dollars = float(raw.get("no_price_dollars", 0) or 0)
        yes_price_dollars = float(raw.get("yes_price_dollars", 0) or 0)
        no_cents = round(no_price_dollars * 100)
        yes_cents = round(yes_price_dollars * 100)

        # count_fp is a string like "467.00"
        count_raw = raw.get("count_fp", raw.get("count", 0))
        contracts = int(float(count_raw)) if count_raw else 0

        side = raw.get("side", "no")

        # Collateral = no_price * contracts (in dollars)
        collateral = no_price_dollars * contracts
        size = contracts  # each contract is $1 notional

        # Parse timestamp
        created = raw.get("created_time", "")
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            ts_str = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            ts_str = created

        # Build legs from mve_selected_legs (has sport/game info) + title parts
        title_parts = _parse_legs_from_title(title)

        legs = []
        leg_sports = []
        if mve_legs:
            for i, mleg in enumerate(mve_legs):
                evt = mleg.get("event_ticker", "")
                mkt = mleg.get("market_ticker", "")
                leg_side = mleg.get("side", "")

                # Classify sport from the leg's market ticker (e.g. KXNBAGAME, KXEPLGAME)
                sport = _classify_sport_from_ticker(mkt) or _classify_sport_from_ticker(evt)
                if sport == "other":
                    sport = _classify_sport_from_ticker(ticker)

                # Extract game from event ticker: KXNBAGAME-26MAR13CLEDAL → CLE vs DAL
                game = _extract_game_from_event(evt)

                # Build leg description: combine title part with game context
                if i < len(title_parts):
                    leg_desc = title_parts[i]
                else:
                    leg_desc = f"{leg_side} {mkt}"

                # Append game context and market ticker for filtering
                if game:
                    leg_desc = f"{leg_desc} ({game})"
                leg_desc = f"{leg_desc} | {mkt}"

                legs.append(leg_desc)
                leg_sports.append(sport)
        else:
            # Fallback: use title-based parsing
            legs = title_parts
            # Prefer parent ticker classification over leg-text heuristics
            ticker_sport = _classify_sport_from_ticker(ticker)
            if ticker_sport != "other":
                leg_sports = [ticker_sport] * len(legs)
            else:
                leg_sports = [_classify_sport_from_leg(leg) for leg in legs]

        sports = list(set(leg_sports)) if leg_sports else ["other"]

        fills.append({
            "timestamp": ts_str,
            "ticker": ticker,
            "contracts": contracts,
            "size": size,
            "collateral": round(collateral, 2),
            "yes_cents": yes_cents,
            "no_cents": no_cents,
            "legs": legs,
            "leg_sports": leg_sports,
            "sports": sports,
            "num_legs": len(legs),
            "title": title,
            "creator_id": "",
            "quote_id": "",
            "rfq_id": "",
            "book_prices": {},
            "latency": 0.0,
        })

    return fills


def check_settlements(all_fills, client):
    """Check settlement status for all fills. Returns positions list."""
    # Deduplicate by ticker
    unique_tickers = {}
    for f in all_fills:
        t = f.get("ticker", "")
        if t:
            unique_tickers[t] = f

    print(f"[refresh] {len(unique_tickers)} unique tickers to check settlement")

    settlement_map = {}

    # Load existing cache to skip already-settled
    existing_cache = {}
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE) as cf:
                old = json.load(cf)
            for pos in old.get("positions", []):
                if pos.get("settled"):
                    existing_cache[pos["ticker"]] = {
                        "status": "settled",
                        "result": pos.get("result", ""),
                        "settled": True,
                        "revenue_cents": pos.get("revenue_cents", 0),
                        "no_total_cost_dollars": pos.get("no_total_cost_dollars", "0"),
                    }
        except Exception:
            pass

    tickers_to_check = []
    for t in unique_tickers:
        if t in existing_cache:
            settlement_map[t] = existing_cache[t]
        else:
            tickers_to_check.append(t)

    print(f"[refresh] {len(existing_cache)} already settled (cached), {len(tickers_to_check)} to check via API")

    checked = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(get_market_settlement, client, t): t for t in tickers_to_check}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
                settlement_map[ticker] = result
                checked += 1
                if checked % 50 == 0:
                    print(f"  Checked {checked}/{len(tickers_to_check)}...")
            except Exception:
                settlement_map[ticker] = {"status": "error", "result": "", "settled": False}
                errors += 1

    print(f"[refresh] Settlement check complete: {checked} checked, {errors} errors")

    # Build positions list with P&L
    positions = []
    for fill in all_fills:
        ticker = fill.get("ticker", "")
        settlement = settlement_map.get(ticker, {"status": "unknown", "result": "", "settled": False})

        pnl = None
        outcome = "pending"
        if settlement["settled"]:
            result = settlement["result"]
            no_cents = fill.get("no_cents", 0)
            contracts = fill.get("contracts", 0)

            if result == "scalar":
                revenue_cents = settlement.get("revenue_cents", 0)
                cost_str = settlement.get("no_total_cost_dollars", "0")
                cost_dollars = float(cost_str)
                revenue_dollars = revenue_cents / 100.0
                pnl = revenue_dollars - cost_dollars
                outcome = "win" if pnl >= 0 else "loss"
            elif result == "no":
                pnl = ((100 - no_cents) * contracts) / 100
                outcome = "win"
            elif result == "yes":
                pnl = -(no_cents * contracts) / 100
                outcome = "loss"

        positions.append({
            **fill,
            "settled": settlement["settled"],
            "result": settlement.get("result", ""),
            "market_status": settlement.get("status", ""),
            "pnl": pnl,
            "outcome": outcome,
        })

    return positions


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="Days of fill history to scan (0=all)")
    parser.add_argument("--source", choices=["file", "api"], default=None,
                        help="Data source: 'file' (local txt) or 'api' (Kalshi API). Auto-detects if omitted.")
    args = parser.parse_args()

    client = get_client()

    # Auto-detect source: use file if it exists, otherwise API
    source = args.source
    if source is None:
        source = "file" if FILLS_FILE.exists() else "api"

    if source == "file":
        all_fills = fetch_fills_from_file(args.days)
    else:
        all_fills = fetch_fills_from_api(client, args.days)

    if not all_fills:
        print("[refresh] No fills found!")
        return

    positions = check_settlements(all_fills, client)

    open_count = sum(1 for p in positions if not p["settled"])
    settled_count = sum(1 for p in positions if p["settled"])
    wins = sum(1 for p in positions if p["outcome"] == "win")
    losses = sum(1 for p in positions if p["outcome"] == "loss")

    cache_data = {
        "positions": positions,
        "last_refresh": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total": len(positions),
            "open": open_count,
            "settled": settled_count,
            "wins": wins,
            "losses": losses,
        },
    }
    save_cache(cache_data)
    print(f"[refresh] Saved {len(positions)} positions to cache")
    print(f"  Open: {open_count} | Settled: {settled_count} (W:{wins} L:{losses})")


if __name__ == "__main__":
    main()
