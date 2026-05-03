"""Futu Stock API Client - wraps stockapi.loadingtechnology.app REST API.
Replaces direct Futu SDK calls with simple HTTP requests.
"""
import requests
import time
import os
from typing import Optional, List, Dict

API_BASE = "https://stockapi.loadingtechnology.app/api/v1"
API_KEY = os.getenv("STOCK_API_KEY", "test-api-key-12345")
HEADERS = {"X-API-Key": API_KEY, "Content-Type": "application/json"}


def _get(path: str, params: dict = None, timeout: int = 30) -> dict:
    """GET request to stock API."""
    url = f"{API_BASE}{path}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _post(path: str, body: dict = None, timeout: int = 30) -> dict:
    """POST request to stock API."""
    url = f"{API_BASE}{path}"
    resp = requests.post(url, headers=HEADERS, json=body, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def get_quotes_batch(symbols: List[str]) -> Dict[str, dict]:
    """Get realtime quotes for multiple stocks.
    
    Returns {symbol: {last_price, volume, turnover, pe_ratio, ...}}
    """
    data = _post("/quote/batch", {"symbols": symbols})
    quotes = {}
    for q in data.get("quotes", []):
        quotes[q["code"]] = {
            "last_price": q.get("last_price", 0),
            "open_price": q.get("open_price", 0),
            "high_price": q.get("high_price", 0),
            "low_price": q.get("low_price", 0),
            "prev_close": q.get("prev_close_price", 0),
            "volume": q.get("volume", 0),
            "turnover": q.get("turnover", 0),
            "turnover_rate": q.get("turnover_rate", 0),
            "pe_ratio": q.get("pe_ratio"),
            "amplitude": q.get("amplitude"),
        }
    return quotes


def get_option_chain(stock: str, expiry: str = None,
                     option_type: str = None,
                     delta_min: float = 0.05,
                     delta_max: float = 0.92) -> List[dict]:
    """Get option chain for a stock with Greeks.
    
    Returns list of option dicts with: code, strike_price, last_price,
    implied_volatility, delta, gamma, theta, vega, open_interest, volume
    """
    params = {
        "delta_min": delta_min,
        "delta_max": delta_max,
    }
    if expiry:
        params["expiry"] = expiry
    if option_type:
        params["option_type"] = option_type
    
    try:
        data = _get(f"/option/chain/{stock}", params=params)
        return data.get("data", [])
    except Exception:
        return []


def get_macd(symbol: str) -> Optional[dict]:
    """Get MACD indicator for a stock.
    
    Returns {macd, signal, histogram}
    """
    try:
        return _get(f"/indicator/macd/{symbol}").get("data")
    except Exception:
        return None


def get_rsi(symbol: str) -> Optional[dict]:
    """Get RSI indicator."""
    try:
        return _get(f"/indicator/rsi/{symbol}").get("data")
    except Exception:
        return None


def get_capital_flow(symbol: str) -> dict:
    """Get capital flow data for a stock."""
    try:
        return _get(f"/capital-flow/{symbol}")
    except Exception:
        return {}


def get_us_financials(symbol: str) -> dict:
    """Get US stock financial data (overview, estimates)."""
    try:
        clean = symbol.replace("US.", "")
        overview = _get(f"/us/overview/{clean}")
        return overview
    except Exception:
        return {}


def get_market_status(market: str = "US") -> dict:
    """Check if market is open."""
    try:
        return _get(f"/market/status/{market}")
    except Exception:
        return {}


def get_intraday_kline(symbol: str, period_minutes: int = 5) -> List[dict]:
    """Get intraday k-line data for volume analysis."""
    try:
        data = _get(f"/kline/{symbol}/intraday", params={"period": period_minutes})
        return data.get("kline_list", []) or data.get("data", [])
    except Exception:
        return []

def get_all_us_stocks(cache_path: str = "/tmp/us_stocks_cache.json", force_refresh: bool = False) -> List[str]:
    """Get all US stocks from Stock API sector plates (cached 24h).
    
    Fetches all sector plates for US market, then collects all stocks.
    Results cached to avoid repeated API calls.
    
    Returns list of stock codes like ['US.AAPL', 'US.NVDA', ...]
    """
    import json as _json
    import os as _os
    
    # Check cache
    if not force_refresh and _os.path.exists(cache_path):
        try:
            mtime = _os.path.getmtime(cache_path)
            if time.time() - mtime < 86400:  # 24 hours
                with open(cache_path) as f:
                    return _json.load(f).get("stocks", [])
        except Exception:
            pass
    
    # Fetch plates list
    plates = []
    try:
        plates_data = _get("/market/plate/list/US", timeout=15)
        plates = plates_data.get("plates", [])
    except Exception:
        pass
    
    if not plates:
        return []
    
    all_stocks = set()
    for i, plate in enumerate(plates):
        pc = plate.get("plate_code", "")
        if not pc:
            continue
        try:
            stock_data = _get(f"/market/plate/stock/{pc}", timeout=10)
            stocks = stock_data.get("stocks", [])
            all_stocks.update(stocks)
        except Exception:
            continue
        # Small delay between calls
        if i % 20 == 0:
            time.sleep(0.5)
        else:
            time.sleep(0.15)
    
    stock_list = sorted(all_stocks)
    
    # Cache
    try:
        _os.makedirs(_os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w') as f:
            _json.dump({"updated": time.strftime("%Y-%m-%d %H:%M"), "count": len(stock_list), "stocks": stock_list}, f)
    except Exception:
        pass
    
    return stock_list
