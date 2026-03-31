"""
Data engine — parses fills, checks settlement, computes open exposure.

Two data sources:
1. Kalshi API fills (authoritative, but slow to fetch + check settlement)
2. Local fills txt file (fast, has leg details the API doesn't)

Strategy:
- refresh_positions.py runs periodically → fetches fills from API, checks
  settlement for each unique ticker, saves to positions_cache.json
- Between refreshes, we parse new fills from the txt file (anything after
  the last cached timestamp) and assume they're open
- Dashboard reads from the combined view
"""
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent.parent
CACHE_FILE = Path(__file__).parent / "positions_cache.json"
MARKET_INFO_CACHE_FILE = Path(__file__).parent / "market_info_cache.json"
FILLS_FILE = BASE_DIR / "unified_rfq_fills.txt"


# ─── Fill block parser (from unified_rfq_fills.txt) ───

FILL_BLOCK_RE = re.compile(
    r"={80}\nFILL @ (.+?)\n={80}\n(.*?)(?=\n={80}\n|\Z)",
    re.DOTALL,
)

def parse_fill_blocks(text):
    """Parse fill blocks from the fills txt file into structured dicts."""
    fills = []
    blocks = text.split("=" * 80)

    i = 0
    while i < len(blocks):
        block = blocks[i].strip()
        if block.startswith("FILL @"):
            # The fill header and body are split across two blocks
            header = block
            body = blocks[i + 1].strip() if i + 1 < len(blocks) else ""
            fill = _parse_single_fill(header, body)
            if fill:
                fills.append(fill)
            i += 2
        else:
            i += 1
    return fills


def _parse_single_fill(header, body):
    """Parse a single fill block."""
    combined = header + "\n" + body

    # Timestamp
    ts_match = re.search(r"FILL @ (.+)", header)
    if not ts_match:
        return None
    timestamp_str = ts_match.group(1).strip()

    # Fields
    def extract(pattern, text=combined):
        m = re.search(pattern, text)
        return m.group(1).strip() if m else ""

    quote_id = extract(r"Quote ID:\s+(.+)")
    rfq_id = extract(r"RFQ ID:\s+(.+)")
    creator_id = extract(r"Creator ID:\s+(.+)")
    ticker = extract(r"Ticker:\s+(.+)")

    # Pricing
    contracts_match = re.search(r"Contracts:\s*(\d+)", combined)
    contracts = int(contracts_match.group(1)) if contracts_match else 0

    size_match = re.search(r"Size:\s*\$?([\d.]+)", combined)
    size = float(size_match.group(1)) if size_match else 0.0

    collateral_match = re.search(r"Collateral:\s*\$?([\d.]+)", combined)
    collateral = float(collateral_match.group(1)) if collateral_match else 0.0

    # Our quoted price
    we_quoted_match = re.search(r"We Quoted:\s*.+?\((\d+)c YES / (\d+)c NO\)", combined)
    yes_cents = int(we_quoted_match.group(1)) if we_quoted_match else 0
    no_cents = int(we_quoted_match.group(2)) if we_quoted_match else 0

    # Sportsbook prices
    book_prices = {}
    for bm in re.finditer(r"^\s+(Fanduel|Novig|Draftkings)\s+([+-]\d+)\s+\(([\d.]+)c\)", combined, re.MULTILINE):
        book_prices[bm.group(1).lower()] = {
            "odds": int(bm.group(2)),
            "implied_cents": float(bm.group(3)),
        }

    # Legs
    legs = []
    leg_section = re.search(r"LEGS \((\d+)\):\n(.*?)(?:\nFill Latency|\Z)", combined, re.DOTALL)
    if leg_section:
        for lm in re.finditer(r"\d+\.\s+(.+)", leg_section.group(2)):
            legs.append(lm.group(1).strip())

    # Classify sport per leg
    leg_sports = []
    for leg in legs:
        leg_sports.append(_classify_leg_sport(leg))

    sports = list(set(leg_sports)) if leg_sports else []

    # Fill latency
    latency_match = re.search(r"Fill Latency:\s*([\d.]+)s", combined)
    latency = float(latency_match.group(1)) if latency_match else 0.0

    return {
        "timestamp": timestamp_str,
        "quote_id": quote_id,
        "rfq_id": rfq_id,
        "creator_id": creator_id,
        "ticker": ticker,
        "contracts": contracts,
        "size": size,
        "collateral": collateral,
        "yes_cents": yes_cents,
        "no_cents": no_cents,
        "book_prices": book_prices,
        "legs": legs,
        "leg_sports": leg_sports,
        "sports": sports,
        "num_legs": len(legs),
        "latency": latency,
    }


def _classify_leg_sport(leg_str):
    """Classify a leg's sport from its ticker suffix."""
    upper = leg_str.upper()
    if "KXMLB" in upper or "KXMVEMLB" in upper:
        return "mlb"
    if "KXNBA" in upper or "KXMVENBA" in upper:
        return "nba"
    if "KXNCAAMB" in upper or "KXNCAAB" in upper or "KXMVENCAAMB" in upper:
        return "ncaab"
    if "KXNHL" in upper or "KXMVENHL" in upper:
        return "nhl"
    if any(tag in upper for tag in ("KXEPL", "KXLALIGA", "KXSERIEA", "KXBUNDESLIGA", "KXLIGUE1", "KXUCL", "KXSOCCER")):
        return "soccer"
    return "other"


def _classify_leg_bet_type(leg_str):
    """Classify a leg's bet type from its ticker and description.

    Uses ticker suffixes for precise classification (especially player props),
    falling back to description-based heuristics.
    """
    upper = leg_str.upper()

    # Extract ticker from leg string (format: "description | KXTICKER-...")
    ticker_part = ""
    if "| " in leg_str:
        ticker_part = leg_str.split("| ")[-1].upper()

    # ── Ticker-based classification (most reliable) ──

    if ticker_part:
        # MLB props — check HRR before HR to avoid false match
        if "KXMLB" in ticker_part or "KXMVEMLB" in ticker_part:
            if "-HRR-" in ticker_part or ticker_part.startswith("KXMLBHRR"):
                return "hits+runs+rbis"
            if "-HR-" in ticker_part or ticker_part.startswith("KXMLBHR"):
                return "home_runs"
            if "-KS-" in ticker_part or ticker_part.startswith("KXMLBKS"):
                return "strikeouts"
            if "-HIT-" in ticker_part or ticker_part.startswith("KXMLBHIT"):
                return "hits"
            if "-TB-" in ticker_part or ticker_part.startswith("KXMLBTB"):
                return "total_bases"
            if "-RFI-" in ticker_part or ticker_part.startswith("KXMLBRFI"):
                return "first_inning_run"
            if "SPREAD" in ticker_part:
                return "spread"
            if "TOTAL" in ticker_part:
                return "total"
            if "GAME" in ticker_part:
                return "moneyline"

        # NBA props
        if "KXNBA" in ticker_part or "KXMVENBA" in ticker_part:
            if "PTS-" in ticker_part or ticker_part.startswith("KXNBAPTS"):
                return "points"
            if "AST-" in ticker_part or ticker_part.startswith("KXNBAAST"):
                return "assists"
            if "REB-" in ticker_part or ticker_part.startswith("KXNBAREB"):
                return "rebounds"
            if "3PM-" in ticker_part or ticker_part.startswith("KXNBA3PM"):
                return "threes"
            if "BLK-" in ticker_part or ticker_part.startswith("KXNBABLK"):
                return "blocks"
            if "STL-" in ticker_part or ticker_part.startswith("KXNBASTL"):
                return "steals"
            if "SPREAD" in ticker_part:
                return "spread"
            if "TOTAL" in ticker_part:
                return "total"
            if "GAME" in ticker_part:
                return "moneyline"

        # NHL props
        if "KXNHL" in ticker_part or "KXMVENHL" in ticker_part:
            if "SPREAD" in ticker_part:
                return "spread"
            if "TOTAL" in ticker_part:
                return "total"
            if "GAME" in ticker_part:
                return "moneyline"

        # NCAAB
        if "KXNCAAMB" in ticker_part or "KXNCAAB" in ticker_part or "KXMVENCAAMB" in ticker_part:
            if "SPREAD" in ticker_part:
                return "spread"
            if "TOTAL" in ticker_part:
                return "total"
            if "GAME" in ticker_part:
                return "moneyline"

    # ── Description-based fallback ──

    # Spread
    if "SPREAD" in upper or "POINT SPREAD" in upper or "WINS BY" in upper.replace("OVER ", ""):
        return "spread"
    if "-SP" in ticker_part:
        return "spread"

    # Total
    if "POINTS SCORED" in upper or "TOTAL-" in ticker_part:
        return "total"
    if ("OVER " in upper or "UNDER " in upper) and ("GOALS" in upper or "POINTS" in upper):
        return "total"

    # BTTS
    if "BTTS" in upper or "BOTH TEAMS" in upper:
        return "btts"

    # Generic player prop fallback (description keywords)
    if any(k in upper for k in ("STRIKEOUT", "STRIKEOUTS")):
        return "strikeouts"
    if "HOME RUN" in upper:
        return "home_runs"
    if "HITS + RUNS + RBIS" in upper or "HITS+RUNS+RBIS" in upper:
        return "hits+runs+rbis"
    if " HIT" in upper and "TOTAL BASE" not in upper:
        return "hits"
    if "TOTAL BASE" in upper:
        return "total_bases"
    if re.search(r'\b\d+\+?\s*(PTS|POINTS)\b', upper):
        return "points"
    if re.search(r'\b\d+\+?\s*(REB|REBOUNDS)\b', upper):
        return "rebounds"
    if re.search(r'\b\d+\+?\s*(AST|ASSISTS)\b', upper):
        return "assists"
    if re.search(r'\b\d+\+?\s*(3PM|THREE)\b', upper):
        return "threes"
    if re.search(r'\b\d+\+?\s*(BLK|BLOCKS)\b', upper):
        return "blocks"
    if re.search(r'\b\d+\+?\s*(STL|STEALS)\b', upper):
        return "steals"
    # Pattern like "Player Name: N+"
    if re.search(r':\s*\d+\+', leg_str):
        return "player_prop"

    # Moneyline
    if "GAME-" in ticker_part or "GAME-" in upper:
        return "moneyline"
    clean = upper
    if clean.startswith("[YES] ") or clean.startswith("[NO] "):
        clean = clean.split("] ", 1)[1] if "] " in clean else clean
    if clean.startswith("YES ") or clean.startswith("NO "):
        clean = clean[4:] if clean.startswith("YES ") else clean[3:]
    if len(clean.split()) <= 3 and not any(c.isdigit() for c in clean):
        return "moneyline"

    if "DRAW" in upper:
        return "draw"
    return "other"


# ─── Positions cache ───

def load_cache():
    """Load cached positions data."""
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"positions": [], "last_refresh": None, "last_fill_timestamp": None}


def save_cache(data):
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_market_info_cache():
    """Load cached market info (titles, legs, sport) to avoid re-fetching."""
    if MARKET_INFO_CACHE_FILE.exists():
        try:
            with open(MARKET_INFO_CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_market_info_cache(data):
    with open(MARKET_INFO_CACHE_FILE, "w") as f:
        json.dump(data, f)


# ─── Combined view: cached positions + fresh fills ───

def get_open_positions():
    """Return list of open (unsettled) positions with full metadata.
    Combines cached settlement data with fresh fills from the txt file.
    """
    cache = load_cache()
    positions = cache.get("positions", [])

    # Filter to open only
    open_positions = [p for p in positions if not p.get("settled", False)]
    return open_positions


def get_all_positions():
    """Return all positions (open + settled) from cache."""
    cache = load_cache()
    return cache.get("positions", [])


# ─── Aggregation helpers ───

def aggregate_exposure(positions, group_by="sport"):
    """Aggregate exposure across positions.
    group_by: 'sport', 'bet_type', 'num_legs', 'creator', 'leg'
    """
    groups = defaultdict(lambda: {"collateral": 0.0, "size": 0.0, "count": 0, "positions": []})

    for pos in positions:
        if group_by == "sport":
            # A position can span multiple sports
            sports = pos.get("sports", ["other"])
            if not sports:
                sports = ["other"]
            for sport in sports:
                key = sport
                groups[key]["collateral"] += pos.get("collateral", 0) / len(sports)
                groups[key]["size"] += pos.get("size", 0) / len(sports)
                groups[key]["count"] += 1
                groups[key]["positions"].append(pos)

        elif group_by == "bet_type":
            bet_types = set()
            for leg in pos.get("legs", []):
                bet_types.add(_classify_leg_bet_type(leg))
            if not bet_types:
                bet_types = {"other"}
            for bt in bet_types:
                groups[bt]["collateral"] += pos.get("collateral", 0) / len(bet_types)
                groups[bt]["size"] += pos.get("size", 0) / len(bet_types)
                groups[bt]["count"] += 1
                groups[bt]["positions"].append(pos)

        elif group_by == "num_legs":
            key = str(pos.get("num_legs", 0))
            groups[key]["collateral"] += pos.get("collateral", 0)
            groups[key]["size"] += pos.get("size", 0)
            groups[key]["count"] += 1

        elif group_by == "creator":
            key = pos.get("creator_id", "unknown")
            groups[key]["collateral"] += pos.get("collateral", 0)
            groups[key]["size"] += pos.get("size", 0)
            groups[key]["count"] += 1

        elif group_by == "leg":
            for leg in pos.get("legs", []):
                # Normalize: strip the ticker suffix for grouping
                leg_key = re.sub(r'\s*\|\s*KX\S+$', '', leg).strip()
                groups[leg_key]["collateral"] += pos.get("collateral", 0)
                groups[leg_key]["size"] += pos.get("size", 0)
                groups[leg_key]["count"] += 1

    # Convert to sorted list
    result = []
    for key, data in groups.items():
        result.append({
            "key": key,
            "collateral": round(data["collateral"], 2),
            "size": round(data["size"], 2),
            "count": data["count"],
        })
    result.sort(key=lambda x: x["collateral"], reverse=True)
    return result


def get_top_risk_positions(positions, n=20):
    """Get the N positions with highest collateral (most at risk)."""
    sorted_pos = sorted(positions, key=lambda p: p.get("collateral", 0), reverse=True)
    return sorted_pos[:n]


def get_creator_summary(positions):
    """Summarize creator activity for open positions."""
    creators = defaultdict(lambda: {"count": 0, "collateral": 0.0, "size": 0.0, "tickers": []})
    for pos in positions:
        cid = pos.get("creator_id", "unknown")
        creators[cid]["count"] += 1
        creators[cid]["collateral"] += pos.get("collateral", 0)
        creators[cid]["size"] += pos.get("size", 0)
        creators[cid]["tickers"].append(pos.get("ticker", ""))

    result = []
    for cid, data in creators.items():
        result.append({
            "creator_id": cid,
            "count": data["count"],
            "collateral": round(data["collateral"], 2),
            "size": round(data["size"], 2),
        })
    result.sort(key=lambda x: x["collateral"], reverse=True)
    return result


def compute_leg_exposure(positions):
    """Compute per-leg exposure across all open positions.
    Returns list of {leg, sport, bet_type, collateral, size, count}.
    """
    legs = defaultdict(lambda: {"collateral": 0.0, "size": 0.0, "count": 0, "sport": "", "bet_type": ""})

    for pos in positions:
        pos_legs = pos.get("legs", [])
        pos_leg_sports = pos.get("leg_sports", [])
        pos_collateral = pos.get("collateral", 0)
        pos_size = pos.get("size", 0)

        for i, leg_str in enumerate(pos_legs):
            leg_key = re.sub(r'\s*\|\s*KX\S+$', '', leg_str).strip()
            legs[leg_key]["collateral"] += pos_collateral
            legs[leg_key]["size"] += pos_size
            legs[leg_key]["count"] += 1
            # Use pre-computed leg sport from refresh if available
            if i < len(pos_leg_sports) and pos_leg_sports[i] != "other":
                legs[leg_key]["sport"] = pos_leg_sports[i]
            else:
                legs[leg_key]["sport"] = _classify_leg_sport(leg_str)
            legs[leg_key]["bet_type"] = _classify_leg_bet_type(leg_str)

    result = []
    for key, data in legs.items():
        result.append({
            "leg": key,
            "sport": data["sport"],
            "bet_type": data["bet_type"],
            "collateral": round(data["collateral"], 2),
            "size": round(data["size"], 2),
            "count": data["count"],
        })
    result.sort(key=lambda x: x["collateral"], reverse=True)
    return result
