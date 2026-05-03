"""Options Flow Scanner - detects unusual single-strike options activity using Stock API.

Flags:
  - Volume > 2x Open Interest (fresh positions being opened)
  - Turnover > 5x stock's median contract turnover
  - Premium > $500K in a single strike (large directional bet)

Used at end of daily run to scan top stocks for unusual flow.
"""
import time, sys, os
from typing import Callable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from stock_api_client import get_option_chain
from notion_client import (
    add_page, title_val, rich_text_val, number_val, date_val, select_val,
    FLOW_DB_ID,
)


def scan_stock_flow(stock_code: str, vol_oi_threshold: float = 2.0,
                    premium_threshold: float = 500000) -> list:
    """Scan a single stock for unusual options flow.
    
    Returns list of alert dicts for flagged strikes.
    """
    alerts = []
    
    try:
        for opt_type in ['CALL', 'PUT']:
            chain = get_option_chain(stock_code, option_type=opt_type)
            if not chain:
                continue
            
            # Compute median turnover for baseline
            turnovers = []
            for opt in chain:
                vol = opt.get('volume', 0) or 0
                price = opt.get('last_price', 0) or 0
                if vol > 0 and price > 0:
                    turnovers.append(vol * price * 100)
            
            if not turnovers:
                continue
            
            median_turnover = sorted(turnovers)[len(turnovers)//2]
            
            for opt in chain:
                volume = opt.get('volume', 0) or 0
                if volume < 10:  # Skip low volume
                    continue
                
                price = opt.get('last_price', 0) or 0
                oi = opt.get('open_interest', 0) or 0
                strike = opt.get('strike_price', 0) or 0
                expiry = str(opt.get('strike_time', ''))[:10]
                delta = opt.get('delta', 0) or 0
                
                turnover = volume * price * 100
                vo_ratio = volume / oi if oi > 0 else 0
                
                signals = []
                if vo_ratio > vol_oi_threshold:
                    signals.append(f'Vol/OI={vo_ratio:.1f}x')
                if median_turnover > 0 and turnover > median_turnover * 5:
                    signals.append(f'Turnover={turnover/median_turnover:.0f}x median')
                if turnover > premium_threshold:
                    signals.append(f'Premium=\${turnover:,.0f}')
                
                if signals:
                    direction = '📈 看多' if opt_type == 'CALL' else '📉 看空'
                    alerts.append({
                        'stock': stock_code,
                        'opt_type': opt_type,
                        'strike': strike,
                        'expiry': expiry,
                        'volume': int(volume),
                        'open_interest': int(oi),
                        'vo_ratio': round(vo_ratio, 1),
                        'turnover': round(turnover, 0),
                        'delta': round(delta, 3),
                        'signals': ' | '.join(signals),
                        'direction': direction,
                    })
                
    except Exception as e:
        pass
    
    return alerts


def batch_scan(top_stocks: list, log_fn: Callable = print) -> list:
    """Scan top N stocks for unusual flow."""
    all_alerts = []
    log_fn(f"Flow scanner: scanning {len(top_stocks)} stocks...")
    
    for i, code in enumerate(top_stocks):
        alerts = scan_stock_flow(code)
        all_alerts.extend(alerts)
        if alerts:
            log_fn(f"  [{i+1}/{len(top_stocks)}] {code}: {len(alerts)} unusual strikes")
        time.sleep(0.3)
    
    all_alerts.sort(key=lambda x: x['turnover'], reverse=True)
    log_fn(f"Flow scanner: {len(all_alerts)} total alerts")
    return all_alerts


def write_flow_alerts(alerts: list, log_fn: Callable = print) -> int:
    """Write flow alerts to Notion Flow Alerts database."""
    if not FLOW_DB_ID or not alerts:
        return 0
    
    count = 0
    for alert in alerts[:20]:  # Max 20 alerts to avoid flooding
        try:
            props = {
                'Alert': title_val(f'{alert["stock"]} {alert["opt_type"]} {alert["strike"]}'),
                'Date': date_val(time.strftime('%Y-%m-%d')),
                'Stock': rich_text_val(alert['stock']),
                'Type': select_val(alert['direction']),
                'Severity': select_val('🔴 High' if alert['turnover'] > 1000000 else '🟡 Medium'),
                'Message': rich_text_val(
                    f'{alert["opt_type"]} {alert["strike"]} Exp:{alert["expiry"]} | '
                    f'Vol:{alert["volume"]} OI:{alert["open_interest"]} Δ:{alert["delta"]} | '
                    f'{alert["signals"]}'
                ),
                'Value': number_val(alert['turnover']),
                'Price': number_val(alert['strike']),
            }
            add_page(FLOW_DB_ID, props)
            count += 1
        except Exception:
            pass
    
    return count
