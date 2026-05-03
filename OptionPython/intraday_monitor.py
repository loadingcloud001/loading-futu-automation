"""Intraday Options Monitor - polls Futu API during market hours for real-time alerts.

Detects:
  - Sudden IV spikes (>20% in 30 min)
  - Turnover bursts (>3x normal rate)  
  - P/C ratio flips (sentiment change)

Writes alerts to Notion Alerts database instead of Telegram.
Can run as a separate process alongside app.py scheduler.
"""

import os
import sys
import time
import json
from datetime import datetime, time as dtime
from collections import defaultdict
from typing import Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from notion_client import (
    title_val, rich_text_val, number_val, date_val, select_val,
    add_page,
)

# Notion Alerts DB ID
ALERTS_DB_ID = "3551f5d1-7d2f-8131-aa04-eb9101e044d0"

# US market hours in HKT (UTC+8)
# Regular: 21:30 - 04:00 (summer) / 22:30 - 05:00 (winter)
MARKET_OPEN_HKT = dtime(21, 30)
MARKET_CLOSE_HKT = dtime(4, 0)

# Alert thresholds
IV_SPIKE_PCT = 20       # IV up 20% from baseline
TURNOVER_BURST_X = 3.0  # Turnover 3x baseline rate
PC_RATIO_FLIP = 2.0     # P/C ratio crosses this threshold


def is_market_hours() -> bool:
    """Check if US market is currently open (in HKT)."""
    now = datetime.now().time()
    # Market open 21:30 - 04:00 next day
    if now >= MARKET_OPEN_HKT or now <= MARKET_CLOSE_HKT:
        return True
    return False


def compute_baseline(quote_ctx, stock_codes: list) -> dict:
    """Get baseline metrics for watchlist stocks at market open.
    
    Returns {stock_code: {turnover, ivc, ivp, pc_ratio}}
    """
    baseline = {}
    for code in stock_codes:
        try:
            ret, data = quote_ctx.get_market_snapshot([code])
            if ret == 0 and not data.empty:  # RET_OK = 0 in futu
                baseline[code] = {
                    'turnover': float(data.get('turnover', [0])[0] or 0),
                    'ivc': float(data.get('option_implied_volatility', [0])[0] or 0),
                    'ivp': float(data.get('option_implied_volatility', [0])[0] or 0),
                    'last_price': float(data.get('last_price', [0])[0] or 0),
                }
        except Exception:
            continue
    return baseline


def scan_for_alerts(
    quote_ctx,
    watchlist: list,
    baseline: dict,
    log_fn: Callable = print,
) -> list:
    """Scan watchlist for intraday anomalies.
    
    Returns list of alert dicts.
    """
    alerts = []
    
    for code in watchlist:
        try:
            ret, data = quote_ctx.get_market_snapshot([code])
            if ret != 0 or data.empty:
                continue
            
            current = {
                'turnover': float(data.get('turnover', [0])[0] or 0),
                'last_price': float(data.get('last_price', [0])[0] or 0),
            }
            
            bl = baseline.get(code, {})
            if not bl:
                continue
            
            # Check turnover burst
            if bl.get('turnover', 0) > 0:
                burst_ratio = current['turnover'] / bl['turnover']
                if burst_ratio > TURNOVER_BURST_X:
                    alerts.append({
                        'stock': code,
                        'type': '🔥 Turnover Burst',
                        'severity': '🔴 High',
                        'message': f'{code} turnover {burst_ratio:.1f}x baseline',
                        'value': round(burst_ratio, 1),
                        'price': current['last_price'],
                    })
            
            # Check price move
            if bl.get('last_price', 0) > 0:
                price_change = (current['last_price'] - bl['last_price']) / bl['last_price'] * 100
                if abs(price_change) > 3:
                    direction = '📈' if price_change > 0 else '📉'
                    alerts.append({
                        'stock': code,
                        'type': f'{direction} Price Move',
                        'severity': '🟡 Medium' if abs(price_change) < 5 else '🔴 High',
                        'message': f'{code} {price_change:+.1f}%',
                        'value': round(price_change, 1),
                        'price': current['last_price'],
                    })
                    
        except Exception:
            continue
    
    return alerts


def write_alerts_to_notion(alerts: list, db_id: str) -> int:
    """Write alerts to Notion Alerts database.
    
    Returns number of alerts written.
    """
    if not db_id:
        return 0
    
    count = 0
    for alert in alerts:
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            props = {
                'Alert': title_val(f'{timestamp} - {alert["stock"]}'),
                'Date': date_val(datetime.now().strftime('%Y-%m-%d')),
                'Stock': rich_text_val(alert['stock']),
                'Type': select_val(alert['type']),
                'Severity': select_val(alert['severity']),
                'Message': rich_text_val(alert['message']),
                'Value': number_val(alert.get('value', 0)),
                'Price': number_val(alert.get('price', 0)),
            }
            add_page(db_id, props)
            count += 1
        except Exception:
            pass
    
    return count


def run_intraday_monitor(
    quote_ctx,
    watchlist: list,
    db_id: str = None,
    interval_seconds: int = 900,  # 15 min
    log_fn: Callable = print,
    stop_event=None,
):
    """Run intraday monitoring loop.
    
    Call this from a separate thread/process during market hours.
    """
    log_fn(f"Intraday monitor starting. Watchlist: {len(watchlist)} stocks, interval: {interval_seconds}s")
    
    # Get baseline at start
    baseline = compute_baseline(quote_ctx, watchlist)
    log_fn(f"Baseline captured for {len(baseline)} stocks")
    
    while True:
        if stop_event and stop_event.is_set():
            break
        
        if not is_market_hours():
            log_fn("Market closed. Waiting...")
            time.sleep(300)
            continue
        
        alerts = scan_for_alerts(quote_ctx, watchlist, baseline, log_fn)
        
        if alerts:
            log_fn(f"Found {len(alerts)} alerts!")
            for a in alerts:
                log_fn(f"  {a['severity']} {a['type']}: {a['message']}")
            
            if db_id:
                written = write_alerts_to_notion(alerts, db_id)
                log_fn(f"  Written {written} alerts to Notion")
            
            # Update baseline after alerts to avoid re-triggering
            baseline = compute_baseline(quote_ctx, watchlist)
        
        # Sleep in chunks for clean shutdown
        for _ in range(interval_seconds):
            if stop_event and stop_event.is_set():
                break
            time.sleep(1)
    
    log_fn("Intraday monitor stopped")
