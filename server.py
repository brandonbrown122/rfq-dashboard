#!/usr/bin/env python3
"""
RFQ Bot Dashboard Server

Endpoints:
    GET /                     — Dashboard UI
    GET /api/balance          — Kalshi portfolio balance
    GET /api/positions        — All cached positions (open + settled)
    GET /api/open             — Open positions only
    GET /api/exposure/sport   — Exposure grouped by sport
    GET /api/exposure/bet_type — Exposure grouped by bet type
    GET /api/exposure/legs    — Exposure grouped by number of legs
    GET /api/exposure/leg     — Per-leg exposure
    GET /api/creators         — Creator summary
    GET /api/top_risk         — Highest-risk open positions
    GET /api/summary          — Quick summary stats
    POST /api/refresh         — Trigger a position refresh (async)
"""
import json
import os
import sys
import subprocess
import threading
import time
from pathlib import Path
from datetime import datetime, timezone

from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from kalshi_api import get_balance, get_client
from data_engine import (
    load_cache, get_open_positions, get_all_positions,
    aggregate_exposure, get_top_risk_positions,
    get_creator_summary, compute_leg_exposure,
    FILLS_FILE, parse_fill_blocks,
)

app = Flask(__name__, static_folder="static")
CORS(app)

# ─── In-memory caches with TTL ───
_balance_cache = {"data": None, "ts": 0}
BALANCE_TTL = 30  # seconds

_refresh_lock = threading.Lock()
_refresh_running = False
REFRESH_INTERVAL = 300  # 5 minutes


def _run_refresh(days=7):
    """Run refresh_positions.py as a subprocess."""
    global _refresh_running
    if _refresh_running:
        return
    _refresh_running = True
    try:
        script = str(Path(__file__).parent / "refresh_positions.py")
        subprocess.run(
            [sys.executable, script, "--days", str(days)],
            cwd=str(Path(__file__).parent),
            capture_output=True, text=True, timeout=600,
        )
    finally:
        _refresh_running = False


def _auto_refresh_loop():
    """Sleep until the next 5-minute wall-clock mark, then refresh forever."""
    while True:
        now = time.time()
        # Next multiple of REFRESH_INTERVAL (e.g. :00, :05, :10, ...)
        next_tick = (now // REFRESH_INTERVAL + 1) * REFRESH_INTERVAL
        time.sleep(next_tick - now)
        print(f"[auto-refresh] Triggered at {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
        _run_refresh(days=7)


# ─── API Routes ───

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/balance")
def api_balance():
    global _balance_cache
    now = time.time()
    if _balance_cache["data"] and now - _balance_cache["ts"] < BALANCE_TTL:
        return jsonify(_balance_cache["data"])

    try:
        raw = get_balance()
        balance_cents = raw.get("balance", 0)
        portfolio_cents = raw.get("portfolio_value", 0)
        data = {
            "balance": balance_cents / 100,
            "portfolio_value": portfolio_cents / 100,
            "total": (balance_cents + portfolio_cents) / 100,
            "raw": raw,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        _balance_cache = {"data": data, "ts": now}
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/summary")
def api_summary():
    cache = load_cache()
    positions = cache.get("positions", [])
    open_pos = [p for p in positions if not p.get("settled")]
    settled_pos = [p for p in positions if p.get("settled")]

    total_open_collateral = sum(p.get("collateral", 0) for p in open_pos)
    total_open_size = sum(p.get("size", 0) for p in open_pos)
    wins = sum(1 for p in settled_pos if p.get("outcome") == "win")
    losses = sum(1 for p in settled_pos if p.get("outcome") == "loss")
    total_pnl = sum(p.get("pnl", 0) or 0 for p in settled_pos)

    # Today stats
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    today_settled = [p for p in settled_pos if p.get("timestamp", "")[:10] == today_str]
    today_wins = sum(1 for p in today_settled if p.get("outcome") == "win")
    today_losses = sum(1 for p in today_settled if p.get("outcome") == "loss")
    today_pnl = sum(p.get("pnl", 0) or 0 for p in today_settled)
    today_open = [p for p in open_pos if p.get("timestamp", "")[:10] == today_str]

    # Per-day breakdown
    from collections import defaultdict
    daily = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0, "fills": 0})
    for p in settled_pos:
        d = p.get("timestamp", "")[:10]
        daily[d]["fills"] += 1
        daily[d]["pnl"] += p.get("pnl", 0) or 0
        if p.get("outcome") == "win":
            daily[d]["wins"] += 1
        elif p.get("outcome") == "loss":
            daily[d]["losses"] += 1
    daily_list = []
    for d in sorted(daily.keys()):
        dd = daily[d]
        wr = round(dd["wins"] / max(dd["wins"] + dd["losses"], 1) * 100, 1)
        daily_list.append({"date": d, "wins": dd["wins"], "losses": dd["losses"],
                           "pnl": round(dd["pnl"], 2), "fills": dd["fills"], "win_rate": wr})

    return jsonify({
        "total_positions": len(positions),
        "open_positions": len(open_pos),
        "settled_positions": len(settled_pos),
        "open_collateral": round(total_open_collateral, 2),
        "open_size": round(total_open_size, 2),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / max(wins + losses, 1) * 100, 1),
        "total_pnl": round(total_pnl, 2),
        "today": {
            "date": today_str,
            "open": len(today_open),
            "settled": len(today_settled),
            "wins": today_wins,
            "losses": today_losses,
            "pnl": round(today_pnl, 2),
            "win_rate": round(today_wins / max(today_wins + today_losses, 1) * 100, 1),
        },
        "daily": daily_list,
        "last_refresh": cache.get("last_refresh"),
    })


@app.route("/api/fill_velocity")
def api_fill_velocity():
    """Count fills in the last 10 min, 1 hour, and 12 hours."""
    cache = load_cache()
    positions = cache.get("positions", [])
    now = datetime.now(timezone.utc)

    windows = {"10m": 600, "1h": 3600, "12h": 43200}
    counts = {}
    for label, seconds in windows.items():
        count = 0
        for p in positions:
            ts_str = p.get("timestamp", "")
            if not ts_str:
                continue
            try:
                ts = datetime.strptime(ts_str.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if (now - ts).total_seconds() <= seconds:
                    count += 1
            except ValueError:
                continue
        counts[label] = count

    return jsonify(counts)


@app.route("/api/positions")
def api_positions():
    positions = get_all_positions()
    return jsonify({"count": len(positions), "positions": positions})


@app.route("/api/open")
def api_open():
    """Open positions with optional filters.
    Query params: sport, bet_type, min_legs, max_legs, sort (collateral|size|timestamp)
    """
    positions = get_open_positions()

    # Filters
    sport = request.args.get("sport")
    if sport:
        positions = [p for p in positions if sport.lower() in [s.lower() for s in p.get("sports", [])]]

    min_legs = request.args.get("min_legs", type=int)
    if min_legs:
        positions = [p for p in positions if p.get("num_legs", 0) >= min_legs]

    max_legs = request.args.get("max_legs", type=int)
    if max_legs:
        positions = [p for p in positions if p.get("num_legs", 0) <= max_legs]

    # Sort
    sort_by = request.args.get("sort", "collateral")
    reverse = True
    if sort_by == "timestamp":
        positions.sort(key=lambda p: p.get("timestamp", ""), reverse=True)
    elif sort_by == "size":
        positions.sort(key=lambda p: p.get("size", 0), reverse=True)
    else:
        positions.sort(key=lambda p: p.get("collateral", 0), reverse=True)

    total_collateral = sum(p.get("collateral", 0) for p in positions)
    total_size = sum(p.get("size", 0) for p in positions)

    return jsonify({
        "count": len(positions),
        "total_collateral": round(total_collateral, 2),
        "total_size": round(total_size, 2),
        "positions": positions,
    })


@app.route("/api/exposure/<group_by>")
def api_exposure(group_by):
    """Aggregate open exposure. group_by: sport, bet_type, num_legs, creator, leg"""
    if group_by not in ("sport", "bet_type", "num_legs", "creator", "leg"):
        return jsonify({"error": f"Invalid group_by: {group_by}"}), 400

    positions = get_open_positions()
    groups = aggregate_exposure(positions, group_by=group_by)

    total_collateral = sum(g["collateral"] for g in groups)
    return jsonify({
        "group_by": group_by,
        "total_collateral": round(total_collateral, 2),
        "groups": groups,
    })


@app.route("/api/leg_exposure")
def api_leg_exposure():
    """Per-leg exposure across open positions.
    Query params: sport, bet_type, limit (default 50)
    """
    positions = get_open_positions()
    legs = compute_leg_exposure(positions)

    sport = request.args.get("sport")
    if sport:
        legs = [l for l in legs if l["sport"] == sport.lower()]

    bet_type = request.args.get("bet_type")
    if bet_type:
        legs = [l for l in legs if l["bet_type"] == bet_type.lower()]

    limit = request.args.get("limit", 50, type=int)
    legs = legs[:limit]

    return jsonify({"count": len(legs), "legs": legs})


@app.route("/api/creators")
def api_creators():
    positions = get_open_positions()
    creators = get_creator_summary(positions)
    return jsonify({"count": len(creators), "creators": creators})


@app.route("/api/top_risk")
def api_top_risk():
    n = request.args.get("n", 20, type=int)
    positions = get_open_positions()
    top = get_top_risk_positions(positions, n=n)
    total = sum(p.get("collateral", 0) for p in top)
    return jsonify({"count": len(top), "total_collateral": round(total, 2), "positions": top})


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Trigger a background position refresh."""
    if _refresh_running:
        return jsonify({"status": "already_running"})

    days = request.json.get("days", 7) if request.is_json else 7
    threading.Thread(target=_run_refresh, args=(days,), daemon=True).start()
    return jsonify({"status": "started", "days": days})


@app.route("/api/refresh_status")
def api_refresh_status():
    cache = load_cache()
    return jsonify({
        "running": _refresh_running,
        "last_refresh": cache.get("last_refresh"),
        "total_cached": len(cache.get("positions", [])),
    })


# ─── Auto-refresh on startup (works with both gunicorn and direct run) ───

_auto_refresh_started = False

def start_auto_refresh():
    global _auto_refresh_started
    if _auto_refresh_started:
        return
    _auto_refresh_started = True

    # Run an initial refresh on boot if no cache exists
    cache = load_cache()
    if not cache.get("positions"):
        print("[startup] No cache found, running initial refresh...")
        threading.Thread(target=_run_refresh, args=(7,), daemon=True).start()
    else:
        open_count = sum(1 for p in cache["positions"] if not p.get("settled"))
        print(f"[startup] Cache loaded: {len(cache['positions'])} positions ({open_count} open)")

    # Start the auto-refresh loop
    threading.Thread(target=_auto_refresh_loop, daemon=True).start()
    print("[startup] Auto-refresh: every 5 min on the clock (:00, :05, :10, ...)")


# Start auto-refresh when module is loaded (for gunicorn)
start_auto_refresh()


# ─── Main ───

if __name__ == "__main__":
    print("=" * 60)
    print("RFQ BOT DASHBOARD")
    print("=" * 60)
    port = int(os.getenv("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "0") == "1")
