"""
Kalshi API client for dashboard — balance, settlement checks, fills.

Supports two auth modes:
1. Local: reads PEM file from disk (BILLY2_KALSHI_PRIVATE_KEY_PATH or default path)
2. Cloud: reads PEM content from KALSHI_PRIVATE_KEY env var (for Render/cloud deploy)
"""
import os
import sys
import json
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from kalshi_python import Configuration, KalshiClient
from dotenv import load_dotenv

# Load env — try local .env files
_base = Path(__file__).parent.parent
for p in [_base / '.env', _base.parent / '.env', Path(__file__).parent / '.env']:
    if p.exists():
        load_dotenv(p)
        break

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
API_KEY_ID = os.getenv("BILLY2_KALSHI_API_KEY_ID") or os.getenv("KALSHI_API_KEY_ID", "")


def _get_private_key_path():
    """Resolve private key: env var content > env var path > default file."""
    # Cloud mode: PEM content in env var
    pem_content = os.getenv("KALSHI_PRIVATE_KEY")
    if pem_content:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
        tmp.write(pem_content)
        tmp.close()
        return tmp.name

    # Local mode: file path
    path = os.getenv("BILLY2_KALSHI_PRIVATE_KEY_PATH", str(_base / "billy2_private_key.pem"))
    return path


def get_client():
    config = Configuration(host=BASE_URL)
    client = KalshiClient(config)
    client.set_kalshi_auth(API_KEY_ID, _get_private_key_path())
    return client


def get_balance(client=None):
    """Fetch portfolio balance. Returns dict with balance, portfolio_value (in cents)."""
    if client is None:
        client = get_client()
    resp = client.call_api("GET", f"{BASE_URL}/portfolio/balance")
    if resp.status == 200:
        return json.loads(resp.read())
    raise Exception(f"Balance API error: {resp.status}")


def get_market_settlement(client, ticker, retries=3):
    """Check a single market's settlement status with retry on rate limit.
    Returns dict with status, result, settled bool, and revenue/cost for scalar markets.
    """
    import time as _time
    for attempt in range(retries):
        try:
            resp = client.call_api("GET", f"{BASE_URL}/markets/{ticker}")
            if resp.status == 200:
                market = json.loads(resp.read()).get("market", {})
                status = market.get("status", "")
                result = market.get("result", "")
                # Settled if closed/finalized with yes/no result, OR scalar (parlay settlement)
                settled = status in ("closed", "finalized") and result in ("yes", "no", "scalar")
                out = {"status": status, "result": result, "settled": settled}

                # For scalar results (parlays), fetch settlement details for P&L
                if settled and result == "scalar":
                    try:
                        sresp = client.call_api("GET", f"{BASE_URL}/portfolio/settlements?ticker={ticker}&limit=1")
                        if sresp.status == 200:
                            sdata = json.loads(sresp.read())
                            settlements = sdata.get("settlements", [])
                            if settlements:
                                s = settlements[0]
                                out["revenue_cents"] = s.get("revenue", 0)
                                out["no_total_cost_dollars"] = s.get("no_total_cost_dollars", "0")
                                out["settlement_value"] = s.get("value", 0)
                    except Exception:
                        pass
                return out
            elif resp.status == 404:
                return {"status": "not_found", "result": "", "settled": False}
            elif resp.status == 429:
                _time.sleep(1.0 + attempt * 2.0)
                continue
            else:
                if attempt < retries - 1:
                    _time.sleep(0.5)
                    continue
                return {"status": "error", "result": "", "settled": False}
        except Exception:
            if attempt < retries - 1:
                _time.sleep(0.5)
                continue
            return {"status": "error", "result": "", "settled": False}
    return {"status": "error", "result": "", "settled": False}


def get_market_info(client, ticker, retries=3):
    """Fetch market info (title, subtitle, status, result). Used to enrich API fills."""
    import time as _time
    for attempt in range(retries):
        try:
            resp = client.call_api("GET", f"{BASE_URL}/markets/{ticker}")
            if resp.status == 200:
                market = json.loads(resp.read()).get("market", {})
                return market
            elif resp.status == 429:
                _time.sleep(1.0 + attempt * 2.0)
                continue
            elif resp.status == 404:
                return None
            else:
                if attempt < retries - 1:
                    _time.sleep(0.5)
                    continue
                return None
        except Exception:
            if attempt < retries - 1:
                _time.sleep(0.5)
                continue
            return None
    return None


def get_fills(client, days=None, min_ts=None):
    """Paginate through /portfolio/fills. Returns list of fill dicts."""
    from datetime import datetime, timedelta, timezone
    all_fills = []
    cursor = None

    if days and not min_ts:
        min_ts = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())

    while True:
        url = f"{BASE_URL}/portfolio/fills?limit=1000"
        if min_ts:
            url += f"&min_ts={min_ts}"
        if cursor:
            url += f"&cursor={cursor}"
        resp = client.call_api("GET", url)
        if resp.status != 200:
            break
        data = json.loads(resp.read())
        fills = data.get("fills", [])
        if not fills:
            break
        all_fills.extend(fills)
        cursor = data.get("cursor")
        if not cursor:
            break
    return all_fills
