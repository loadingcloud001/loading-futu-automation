"""Multi-Day Trend Analyzer - computes 5/10/20 day rolling metrics.

Queries Notion Historical Archive DB to build trend data.
Replaces simple day-over-day comparison with multi-day moving averages.

Metrics computed:
  - IV 5d/10d/20d moving average (trend direction)
  - Turnover 5d/10d/20d moving average (volume momentum)
  - Anomaly streak (consecutive days flagged 🔴)
  - IV percentile (current IV vs 20-day range)
"""

import os
import sys
import math
from datetime import date, timedelta
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from notion_client import (
    HISTORICAL_ARCHIVE_DB_ID,
    query_database,
)


def fetch_historical_data(days_back: int = 20) -> dict:
    """Fetch historical data from Notion Historical Archive.
    
    Returns:
        {date_str: {stock_code: {tc, tp, ivc, ivp, total, pc}}}
    """
    pages = query_database(HISTORICAL_ARCHIVE_DB_ID)
    
    history = defaultdict(dict)
    
    for page in pages:
        props = page.get('properties', {})
        
        date_p = props.get('Date', {}).get('date', {})
        date_str = (date_p.get('start', '') or '')[:10]
        if not date_str:
            continue
        
        stock_p = props.get('Stock', {}).get('rich_text', [])
        stock = stock_p[0]['plain_text'] if stock_p else ''
        if not stock:
            # Try Entry field (format: "YYYY-MM-DD US.XXXX")
            entry_p = props.get('Entry', {}).get('title', [])
            if entry_p:
                entry_text = entry_p[0]['plain_text']
                parts = entry_text.split(' ', 1)
                if len(parts) == 2:
                    stock = parts[1]
        
        tc = props.get('CALL Turnover', {}).get('number', 0) or 0
        tp = props.get('PUT Turnover', {}).get('number', 0) or 0
        ivc = props.get('Call IV', {}).get('number', 0) or 0
        ivp = props.get('Put IV', {}).get('number', 0) or 0
        
        history[date_str][stock] = {
            'tc': float(tc),
            'tp': float(tp),
            'total': float(tc) + float(tp),
            'ivc': float(ivc),
            'ivp': float(ivp),
            'pc': float(tp) / float(tc) if tc > 0 else 999,
        }
    
    return dict(history)


def compute_rolling_metrics(
    today_data: dict,
    history: dict,
    windows: list = [5, 10, 20],
) -> dict:
    """Compute rolling metrics for each stock.
    
    Args:
        today_data: {stock_code: {tc, tp, ivc, ivp, total}}
        history: {date: {stock: {tc, tp, ivc, ivp, total}}}
        windows: list of window sizes
    
    Returns:
        {stock_code: {iv_ma5, iv_ma10, iv_ma20, turnover_ma5, ...,
                       iv_rank, anomaly_streak, trend_score}}
    """
    # Sort dates descending
    dates = sorted(history.keys(), reverse=True)
    if not dates:
        return {}
    
    results = {}
    all_stocks = set(today_data.keys())
    for d in dates:
        all_stocks.update(history[d].keys())
    
    for stock in all_stocks:
        # Build time series for this stock
        ivc_series = []
        ivp_series = []
        turnover_series = []
        anomaly_flags = []
        dates_with_data = []
        
        for d in dates:
            if stock in history[d]:
                hd = history[d][stock]
                ivc_series.append(hd['ivc'])
                ivp_series.append(hd['ivp'])
                turnover_series.append(hd['total'])
                dates_with_data.append(d)
                
                # Approximate anomaly detection (turnover > 2x 20d avg)
                if len(turnover_series) >= 20:
                    avg20_so_far = sum(turnover_series[-20:]) / 20
                    if hd['total'] > avg20_so_far * 1.5:
                        anomaly_flags.append(1)
                    else:
                        anomaly_flags.append(0)
                else:
                    anomaly_flags.append(0)
        
        if not ivc_series:
            continue
        
        today = today_data.get(stock, {})
        today_ivc = today.get('ivc', 0)
        today_turnover = today.get('total', 0)
        
        metrics = {'dates_available': len(dates_with_data)}
        
        # Rolling MAs
        for w in windows:
            if len(ivc_series) >= w:
                metrics[f'ivc_ma{w}'] = round(sum(ivc_series[:w]) / w, 4)
                metrics[f'ivp_ma{w}'] = round(sum(ivp_series[:w]) / w, 4) if len(ivp_series) >= w else 0
                metrics[f'turnover_ma{w}'] = round(sum(turnover_series[:w]) / w, 0)
            else:
                metrics[f'ivc_ma{w}'] = today_ivc
                metrics[f'ivp_ma{w}'] = today.get('ivp', 0)
                metrics[f'turnover_ma{w}'] = today_turnover
        
        # IV Rank (where current IV sits in 20-day range)
        if len(ivc_series) >= 5:
            iv_range = max(ivc_series[:min(20, len(ivc_series))]) - min(ivc_series[:min(20, len(ivc_series))])
            if iv_range > 0:
                iv_rank = (today_ivc - min(ivc_series[:20])) / iv_range * 100
                metrics['iv_rank'] = round(min(100, max(0, iv_rank)), 1)
            else:
                metrics['iv_rank'] = 50
        else:
            metrics['iv_rank'] = 50
        
        # Anomaly streak (consecutive days of elevated turnover)
        streak = 0
        for flag in anomaly_flags:
            if flag == 1:
                streak += 1
            else:
                break
        metrics['anomaly_streak'] = streak
        
        # Trend score (-100 to +100): positive = IV rising (good for option sellers, bad for buyers)
        if len(ivc_series) >= 10:
            short_ma = sum(ivc_series[:5]) / min(5, len(ivc_series))
            long_ma = sum(ivc_series[:min(20, len(ivc_series))]) / min(20, len(ivc_series))
            if long_ma > 0:
                trend = (short_ma / long_ma - 1) * 100
                metrics['iv_trend_score'] = round(trend, 1)
            else:
                metrics['iv_trend_score'] = 0
        else:
            metrics['iv_trend_score'] = 0
        
        # Turnover momentum (acceleration)
        if len(turnover_series) >= 5:
            short_t = sum(turnover_series[:5]) / 5
            if len(turnover_series) >= 10:
                long_t = sum(turnover_series[:10]) / 10
            else:
                long_t = short_t
            if long_t > 0:
                metrics['volume_momentum'] = round((short_t / long_t - 1) * 100, 1)
            else:
                metrics['volume_momentum'] = 0
        else:
            metrics['volume_momentum'] = 0
        
        results[stock] = metrics
    
    return results


def get_trend_summary(metrics: dict) -> str:
    """Generate human-readable trend summary."""
    parts = []
    
    iv_rank = metrics.get('iv_rank', 50)
    if iv_rank > 80:
        parts.append(f'IV高位({iv_rank:.0f}%)')
    elif iv_rank < 20:
        parts.append(f'IV低位({iv_rank:.0f}%)')
    
    trend = metrics.get('iv_trend_score', 0)
    if trend > 5:
        parts.append(f'IV上升({trend:+.1f}%)')
    elif trend < -5:
        parts.append(f'IV下降({trend:+.1f}%)')
    
    momentum = metrics.get('volume_momentum', 0)
    if momentum > 20:
        parts.append(f'量加速({momentum:+.0f}%)')
    elif momentum < -20:
        parts.append(f'量減速({momentum:+.0f}%)')
    
    streak = metrics.get('anomaly_streak', 0)
    if streak >= 3:
        parts.append(f'連續{streak}日異常')
    
    return ' | '.join(parts) if parts else '穩定'
