"""Options Flow Scanner - detects unusual single-strike options activity.

Goes beyond aggregate CALL/PUT turnover to identify specific strikes with:
  - Volume > 2x Open Interest (fresh positions)
  - Turnover > 5x stock's median contract turnover
  - Large premium sweeps (>$1M in a single strike)

Writes findings to Notion Flow Alerts database.
"""

import os
import sys
import time
from collections import defaultdict
from typing import Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from notion_client import (
    title_val, rich_text_val, number_val, date_val, select_val,
    add_page,
)

# Notion Flow Alerts DB ID
FLOW_DB_ID = "3551f5d1-7d2f-81f7-ba27-d1a75ad51717"


def scan_option_flow(
    quote_ctx,
    stock_code: str,
    log_fn: Callable = print,
    vol_min: int = 50,
    delta_range: tuple = (0.01, 0.99),
    volume_oi_ratio_threshold: float = 2.0,
    premium_threshold: float = 500000,  # $500K
) -> list:
    """Scan unusual options flow for a single stock.
    
    Returns list of alert dicts for unusual strikes.
    """
    alerts = []
    
    try:
        from futu import OptionDataFilter, OptionType, RET_OK
        
        # Fetch CALL chain
        call_filter = OptionDataFilter(
            delta_min=delta_range[0], delta_max=delta_range[1], vol_min=vol_min
        )
        put_filter = OptionDataFilter(
            delta_min=-delta_range[1], delta_max=-delta_range[0], vol_min=vol_min
        )
        
        for opt_type, filt in [(OptionType.CALL, 'Call'), (OptionType.PUT, 'Put')]:
            ret, chain = quote_ctx.get_option_chain(
                code=stock_code, data_filter=call_filter if filt == 'Call' else put_filter,
                option_type=opt_type
            )
            if ret != 0 or chain.empty:
                continue
            
            codes = chain['code'].tolist()
            if not codes:
                continue
            
            # Get market snapshot for these option contracts
            ret_snap, snap = quote_ctx.get_market_snapshot(codes)
            if ret_snap != 0 or snap.empty:
                continue
            
            # Compute median turnover for this stock's options
            turnovers = snap['turnover'].dropna().tolist()
            if not turnovers:
                continue
            
            median_turnover = sorted(turnovers)[len(turnovers) // 2] if turnovers else 0
            
            for _, row in snap.iterrows():
                volume = float(row.get('volume', 0) or 0)
                turnover = float(row.get('turnover', 0) or 0)
                oi = float(row.get('option_open_interest', 0) or 0)
                strike = row.get('strike_price', 0)
                expiry = str(row.get('strike_time', ''))[:10]
                code = row.get('code', '')
                
                # Volume/Open Interest ratio
                vo_ratio = volume / oi if oi > 0 else 0
                
                # Turnover anomaly
                turnover_ratio = turnover / median_turnover if median_turnover > 0 else 0
                
                signals = []
                if vo_ratio > volume_oi_ratio_threshold:
                    signals.append(f'Vol/OI={vo_ratio:.1f}x')
                if turnover_ratio > 5:
                    signals.append(f'Turnover={turnover_ratio:.0f}x median')
                if abs(turnover) > premium_threshold:
                    signals.append(f'Premium=${turnover:,.0f}')
                
                if signals:
                    strike_price = float(row.get('strike_price', 0))
                    alerts.append({
                        'stock': stock_code,
                        'option_code': code,
                        'opt_type': filt,
                        'strike': strike_price,
                        'expiry': expiry,
                        'volume': int(volume),
                        'open_interest': int(oi),
                        'vo_ratio': round(vo_ratio, 1),
                        'turnover': round(turnover, 0),
                        'turnover_ratio': round(turnover_ratio, 1),
                        'signals': ' | '.join(signals),
                        'direction': '📈 看多' if filt == 'Call' else '📉 看空',
                    })
            
            time.sleep(1)  # Rate limit
        
    except Exception as e:
        log_fn(f"Flow scan error for {stock_code}: {e}")
    
    return alerts


def batch_scan_flow(
    quote_ctx,
    stock_list: list,
    top_n: int = 50,
    log_fn: Callable = print,
) -> list:
    """Scan flow for top N stocks (by aggregate turnover) + watchlist.
    
    Args:
        stock_list: list of stock codes to scan (should be pre-sorted by turnover)
        top_n: number of top stocks to scan
        log_fn: logging function
    
    Returns list of all flow alerts found.
    """
    top_stocks = stock_list[:top_n]
    all_alerts = []
    
    log_fn(f"Scanning options flow for {len(top_stocks)} stocks...")
    
    for i, code in enumerate(top_stocks):
        alerts = scan_option_flow(quote_ctx, code, log_fn=log_fn)
        all_alerts.extend(alerts)
        
        if alerts:
            log_fn(f"  [{i+1}/{len(top_stocks)}] {code}: {len(alerts)} unusual strikes")
        
        time.sleep(0.5)  # Rate limit
    
    # Sort by turnover descending (most significant first)
    all_alerts.sort(key=lambda x: x['turnover'], reverse=True)
    return all_alerts


def write_flow_alerts_to_notion(alerts: list, db_id: str) -> int:
    """Write flow alerts to Notion Flow Alerts database."""
    if not db_id:
        return 0
    
    count = 0
    for alert in alerts:
        try:
            props = {
                'Alert': title_val(f'{alert["stock"]} {alert["opt_type"]} {alert["strike"]}'),
                'Date': date_val(time.strftime('%Y-%m-%d')),
                'Stock': rich_text_val(alert['stock']),
                'Type': select_val(f'{alert["direction"]}'),
                'Severity': select_val('🔴 High' if alert['turnover'] > 1000000 else '🟡 Medium'),
                'Message': rich_text_val(
                    f'{alert["opt_type"]} {alert["strike"]} Exp:{alert["expiry"]} | '
                    f'Vol:{alert["volume"]} OI:{alert["open_interest"]} | '
                    f'{alert["signals"]}'
                ),
                'Value': number_val(alert.get('turnover', 0)),
                'Price': number_val(alert.get('strike', 0)),
            }
            add_page(db_id, props)
            count += 1
        except Exception:
            pass
    
    return count
