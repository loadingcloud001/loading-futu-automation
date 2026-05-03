"""Earnings Calendar - fetches upcoming earnings dates from Yahoo Finance.

Adds IV Crush risk warnings to Daily Snapshot.
Flags stocks with earnings within 7 days → high risk for long option positions.
"""

import requests
import json
import time
from datetime import datetime, date, timedelta
from typing import Optional, Dict
from collections import defaultdict


# Yahoo Finance session with cookie-based auth
_SESSION = None
_CRUMB = None


def _get_session():
    global _SESSION, _CRUMB
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        try:
            _SESSION.get('https://fc.yahoo.com/', timeout=10)
            r = _SESSION.get('https://query2.finance.yahoo.com/v1/test/getcrumb', timeout=5)
            _CRUMB = r.text.strip()
        except Exception:
            _CRUMB = ''
    return _SESSION, _CRUMB


def fetch_earnings(symbol: str) -> Optional[dict]:
    """Fetch earnings data for a single stock.
    
    Returns dict with: next_earnings_date, earnings_call_date, is_estimated,
                       earnings_avg, earnings_low, earnings_high
    """
    session, crumb = _get_session()
    url = f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=calendarEvents'
    if crumb:
        url += f'&crumb={crumb}'
    
    try:
        r = session.get(url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        events = (data.get('quoteSummary', {})
                  .get('result', [{}])[0]
                  .get('calendarEvents', {}))
        earnings = events.get('earnings', {})
        
        if not earnings:
            return None
        
        earnings_dates = earnings.get('earningsDate', [])
        call_dates = earnings.get('earningsCallDate', [])
        
        next_date = None
        if earnings_dates:
            ts = earnings_dates[0].get('raw') or earnings_dates[0].get('fmt')
            if ts:
                try:
                    next_date = datetime.fromtimestamp(int(ts))
                except (ValueError, TypeError, OSError):
                    try:
                        next_date = datetime.strptime(str(ts)[:10], '%Y-%m-%d')
                    except ValueError:
                        pass
        
        return {
            'next_earnings_date': next_date,
            'is_estimated': earnings.get('isEarningsDateEstimate', True),
            'earnings_avg': earnings.get('earningsAverage', {}).get('raw'),
            'earnings_low': earnings.get('earningsLow', {}).get('raw'),
            'earnings_high': earnings.get('earningsHigh', {}).get('raw'),
        }
    except Exception:
        return None


def get_earnings_flag(days_to_earnings: int) -> tuple:
    """Get warning flag based on days until earnings.
    
    Returns (flag_emoji, flag_text, risk_level)
    """
    if days_to_earnings < 0:
        return ('📅', 'Past', 'none')
    elif days_to_earnings == 0:
        return ('🚫', 'TODAY - Avoid!', 'critical')
    elif days_to_earnings == 1:
        return ('🔴', 'Tomorrow - IV Crush Risk', 'critical')
    elif days_to_earnings <= 3:
        return ('🟠', f'{days_to_earnings}d - High Risk', 'high')
    elif days_to_earnings <= 7:
        return ('🟡', f'{days_to_earnings}d - Caution', 'medium')
    else:
        return ('🟢', 'Safe', 'low')


def batch_fetch_earnings(symbols: list, delay: float = 0.5) -> Dict[str, dict]:
    """Fetch earnings for multiple symbols.
    
    Args:
        symbols: list of ticker symbols (e.g. ['AAPL', 'NVDA'])
        delay: seconds between requests to avoid rate limiting
    
    Returns {symbol: earnings_dict}
    """
    results = {}
    for i, sym in enumerate(symbols):
        # Strip US. prefix if present
        clean = sym.replace('US.', '') if sym.startswith('US.') else sym
        try:
            data = fetch_earnings(clean)
            if data:
                results[sym] = data
            time.sleep(delay)
        except Exception:
            pass
    return results


def get_iv_crush_warning(stock_code: str, earnings_data: dict = None) -> dict:
    """Generate IV crush warning for a stock.
    
    Returns dict with: flag, days_to_earnings, warning_text, should_skip_long_option
    """
    if not earnings_data or not earnings_data.get('next_earnings_date'):
        return {
            'flag': '🟢',
            'days_to_earnings': 999,
            'warning_text': '',
            'should_skip_long_option': False,
        }
    
    next_date = earnings_data['next_earnings_date']
    today = date.today()
    
    if isinstance(next_date, datetime):
        days = (next_date.date() - today).days
    else:
        days = 999
    
    flag_emoji, flag_text, risk = get_earnings_flag(days)
    
    should_skip = risk in ('critical', 'high')
    
    return {
        'flag': flag_emoji,
        'days_to_earnings': days,
        'warning_text': f'{flag_emoji} Earnings {flag_text}' if flag_text else '',
        'should_skip_long_option': should_skip,
        'next_date': next_date.strftime('%Y-%m-%d') if next_date else '',
    }


def enrich_daily_snapshot(stocks: list, earnings_cache: dict = None) -> list:
    """Add earnings warnings to stock data list.
    
    Args:
        stocks: list of dicts with at least 'stock' key
        earnings_cache: pre-fetched earnings data
    
    Returns stocks list with added earnings fields.
    """
    if earnings_cache is None:
        return stocks
    
    for s in stocks:
        code = s.get('stock', '')
        ed = earnings_cache.get(code)
        warning = get_iv_crush_warning(code, ed)
        s['earnings_flag'] = warning['flag']
        s['days_to_earnings'] = warning['days_to_earnings']
        s['earnings_warning'] = warning['warning_text']
        s['skip_long_option'] = warning['should_skip_long_option']
        s['next_earnings_date'] = warning.get('next_date', '')
    
    return stocks
