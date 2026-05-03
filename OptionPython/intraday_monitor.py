"""Intraday Options Monitor - polls Stock API during US market hours for real-time alerts.

Uses stockapi.loadingtechnology.app (NOT Futu SDK directly).
Detects: turnover bursts, price moves during market hours.
Writes alerts to Notion Alerts database.
"""
import os, sys, time
from datetime import datetime, time as dtime
from typing import Callable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from stock_api_client import get_quotes_batch
from notion_client import (
    add_page, title_val, rich_text_val, number_val, date_val, select_val,
)

# Notion Alerts DB ID
ALERTS_DB_ID = "3551f5d1-7d2f-8131-aa04-eb9101e044d0"

# Alert thresholds
TURNOVER_BURST_RATIO = 3.0
PRICE_MOVE_PCT = 3.0


def is_market_hours() -> bool:
    """Check if US market is currently open (in HKT)."""
    now = datetime.now()
    h = now.hour
    m = now.minute
    # US market in HKT: ~21:30-04:00 (summer), ~22:30-05:00 (winter)
    # Simple check: between 21:00 and 05:00
    return h >= 21 or h < 5


def fetch_baseline(watchlist: list) -> dict:
    """Get baseline data for watchlist stocks."""
    try:
        quotes = get_quotes_batch(watchlist)
        baseline = {}
        for s, q in quotes.items():
            baseline[s] = {
                'last_price': q.get('last_price', 0) or 0,
                'turnover': q.get('turnover', 0) or 0,
            }
        return baseline
    except Exception:
        return {}


def scan_for_alerts(watchlist: list, baseline: dict, log_fn: Callable = print) -> list:
    """Scan watchlist for intraday anomalies using Stock API."""
    alerts = []
    
    try:
        current = get_quotes_batch(watchlist)
    except Exception:
        return alerts
    
    for code in watchlist:
        cur = current.get(code, {})
        bl = baseline.get(code, {})
        if not cur or not bl:
            continue
        
        cur_price = cur.get('last_price', 0) or 0
        cur_turnover = cur.get('turnover', 0) or 0
        bl_price = bl.get('last_price', 0) or 0
        bl_turnover = bl.get('turnover', 0) or 0
        
        # Check turnover burst
        if bl_turnover > 0 and cur_turnover / bl_turnover > TURNOVER_BURST_RATIO:
            alerts.append({
                'stock': code,
                'type': '🔥 Turnover Burst',
                'severity': '🔴 High',
                'message': f'{code} turnover {cur_turnover/bl_turnover:.1f}x baseline',
                'value': round(cur_turnover / bl_turnover, 1),
                'price': cur_price,
            })
        
        # Check price move
        if bl_price > 0:
            pct = (cur_price - bl_price) / bl_price * 100
            if abs(pct) > PRICE_MOVE_PCT:
                direction = '📈' if pct > 0 else '📉'
                alerts.append({
                    'stock': code,
                    'type': f'{direction} Price Move',
                    'severity': '🟡 Medium' if abs(pct) < 5 else '🔴 High',
                    'message': f'{code} {pct:+.1f}%',
                    'value': round(pct, 1),
                    'price': cur_price,
                })
    
    return alerts


def write_alerts(alerts: list, db_id: str = None) -> int:
    """Write alerts to Notion Alerts database."""
    if not db_id:
        db_id = ALERTS_DB_ID
    if not alerts or not db_id:
        return 0
    
    count = 0
    for alert in alerts[:10]:  # Max 10 to avoid flooding
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            add_page(db_id, {
                'Alert': title_val(f'{timestamp} - {alert["stock"]}'),
                'Date': date_val(datetime.now().strftime('%Y-%m-%d')),
                'Stock': rich_text_val(alert['stock']),
                'Type': select_val(alert['type']),
                'Severity': select_val(alert['severity']),
                'Message': rich_text_val(alert['message']),
                'Value': number_val(alert.get('value', 0)),
                'Price': number_val(alert.get('price', 0)),
            })
            count += 1
        except Exception:
            pass
    
    return count


def quick_scan(watchlist: list, log_fn: Callable = print) -> int:
    """Single scan: fetch quotes, detect alerts, write to Notion.
    
    Returns number of alerts written.
    """
    baseline = fetch_baseline(watchlist)
    if not baseline:
        log_fn("Intraday: no baseline data")
        return 0
    
    alerts = scan_for_alerts(watchlist, baseline, log_fn)
    if alerts:
        log_fn(f"Intraday: {len(alerts)} alerts found")
        written = write_alerts(alerts)
        log_fn(f"Intraday: {written} written to Notion")
        return written
    return 0
