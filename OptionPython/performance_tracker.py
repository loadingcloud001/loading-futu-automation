"""Performance Tracker - replicates Excel 美股期權交易策略 A2 win rate formula.

Excel formula:
  A2: =(Countif(3:3,"Profit")+Countif(64:64,"Profit")) /
       (Countif(3:3,"Loss")+Countif(3:3,"Profit")+Countif(64:64,"Loss")+Countif(64:64,"Profit"))

Computes: win rate, profit factor, drawdown, monthly P&L, expectancy from Notion Trade Journal.
"""

import os
import sys
import math
from datetime import datetime, date
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from notion_client import (
        TRADE_JOURNAL_DB_ID,
        query_database,
        rich_text_val,
        number_val,
        date_val,
        title_val,
    )
except ImportError:
    TRADE_JOURNAL_DB_ID = "3541f5d17d2f8118bc8ff4bf9996cd51"


def fetch_trades_from_notion() -> list:
    """Fetch all completed trades from Notion Trade Journal.
    
    Returns list of dicts with: date, stock, direction, result, pnl, risk
    """
    pages = query_database(TRADE_JOURNAL_DB_ID)
    trades = []
    
    for page in pages:
        props = page.get('properties', {})
        
        # Date
        date_p = props.get('Date', {}).get('date', {})
        date_str = (date_p.get('start', '') or '')[:10]
        
        # Stock
        stock_p = props.get('Stock', {}).get('rich_text', [])
        stock = stock_p[0]['plain_text'] if stock_p else ''
        
        # Result
        result_p = props.get('Result', {}).get('select', {})
        result = result_p.get('name', '') if result_p else ''
        
        # P&L
        pnl = props.get('P&L (HKD)', {}).get('number')
        
        # Direction
        dir_p = props.get('Direction', {}).get('select', {})
        direction = dir_p.get('name', '') if dir_p else ''
        
        # Risk
        risk = props.get('Risk (HKD)', {}).get('number')
        
        trades.append({
            'date': date_str,
            'stock': stock,
            'direction': direction,
            'result': result,
            'pnl': float(pnl) if pnl is not None else 0,
            'risk': float(risk) if risk is not None else 0,
        })
    
    return trades


def compute_performance(trades: list) -> dict:
    """Compute performance metrics from trade list.
    
    Returns dict with all stats matching Excel formulas.
    """
    # Filter completed trades (those with P&L)
    completed = [t for t in trades if t.get('result') in ('✅ Profit', '❌ Loss')]
    
    if not completed:
        return {
            'total_trades': 0, 'win_rate': 0, 'profit_factor': 0,
            'total_pnl': 0, 'expectancy': 0, 'max_drawdown': 0,
            'monthly_pnl': {}, 'avg_win': 0, 'avg_loss': 0,
        }
    
    profits = [t for t in completed if t['result'] == '✅ Profit']
    losses = [t for t in completed if t['result'] == '❌ Loss']
    
    total_pnl = sum(t['pnl'] for t in completed)
    total_trades = len(completed)
    win_count = len(profits)
    loss_count = len(losses)
    
    # Win rate (matches Excel A2)
    win_rate = round(win_count / total_trades * 100, 1) if total_trades > 0 else 0
    
    # Average win/loss
    avg_win = round(sum(t['pnl'] for t in profits) / len(profits), 2) if profits else 0
    avg_loss = round(abs(sum(t['pnl'] for t in losses)) / len(losses), 2) if losses else 0
    
    # Profit factor
    gross_profit = sum(t['pnl'] for t in profits)
    gross_loss = abs(sum(t['pnl'] for t in losses))
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else 0
    
    # Expectancy per trade
    loss_rate = loss_count / total_trades if total_trades > 0 else 0
    win_rate_decimal = win_count / total_trades if total_trades > 0 else 0
    expectancy = round((win_rate_decimal * avg_win) - (loss_rate * avg_loss), 2)
    
    # Monthly P&L
    monthly = defaultdict(float)
    monthly_count = defaultdict(int)
    for t in completed:
        try:
            m = t['date'][:7]  # YYYY-MM
            monthly[m] += t['pnl']
            monthly_count[m] += 1
        except:
            pass
    
    # Max drawdown
    sorted_trades = sorted(completed, key=lambda x: x['date'])
    peak = 0
    cum = 0
    max_dd = 0
    for t in sorted_trades:
        cum += t['pnl']
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
    
    # Direction breakdown
    call_trades = [t for t in completed if 'Call' in t.get('direction', '')]
    put_trades = [t for t in completed if 'Put' in t.get('direction', '')]
    call_win = len([t for t in call_trades if t['result'] == '✅ Profit'])
    put_win = len([t for t in put_trades if t['result'] == '✅ Profit'])
    
    # Streaks
    streaks = []
    curr = 0
    for t in sorted_trades:
        if t['result'] == '✅ Profit':
            if curr > 0: curr += 1
            else: streaks.append(curr); curr = 1
        else:
            if curr < 0: curr -= 1
            else: streaks.append(curr); curr = -1
    streaks.append(curr)
    
    return {
        'total_trades': total_trades,
        'win_count': win_count,
        'loss_count': loss_count,
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_factor': profit_factor,
        'expectancy': expectancy,
        'total_pnl': round(total_pnl, 2),
        'max_drawdown': round(max_dd, 2),
        'peak_equity': round(peak, 2),
        'monthly_pnl': dict(sorted(monthly.items())),
        'monthly_count': dict(sorted(monthly_count.items())),
        'call_trades': len(call_trades),
        'call_win_rate': round(call_win / len(call_trades) * 100, 1) if call_trades else 0,
        'put_trades': len(put_trades),
        'put_win_rate': round(put_win / len(put_trades) * 100, 1) if put_trades else 0,
        'max_win_streak': max(s for s in streaks if s > 0) if any(s > 0 for s in streaks) else 0,
        'max_loss_streak': abs(min(s for s in streaks if s < 0)) if any(s < 0 for s in streaks) else 0,
    }


def format_performance_summary(stats: dict) -> str:
    """Format performance stats into human-readable summary."""
    if stats['total_trades'] == 0:
        return "No completed trades yet."
    
    lines = [
        f"📊 Performance Summary ({stats['total_trades']} trades)",
        f"Win Rate: {stats['win_rate']}% ({stats['win_count']}W / {stats['loss_count']}L)",
        f"Profit Factor: {stats['profit_factor']}",
        f"Total P&L: ${stats['total_pnl']:,.2f}",
        f"Avg Win: ${stats['avg_win']:,.2f} | Avg Loss: ${stats['avg_loss']:,.2f}",
        f"Expectancy: ${stats['expectancy']:,.2f}/trade",
        f"Max Drawdown: ${stats['max_drawdown']:,.2f}",
        f"Call Win Rate: {stats['call_win_rate']}% ({stats['call_trades']} trades)",
        f"Put Win Rate: {stats['put_win_rate']}% ({stats['put_trades']} trades)",
        f"Max Win Streak: {stats['max_win_streak']} | Max Loss Streak: {stats['max_loss_streak']}",
        "",
        "Monthly P&L:",
    ]
    for month, pnl in stats['monthly_pnl'].items():
        emoji = '✅' if pnl > 0 else '❌'
        lines.append(f"  {month}: {emoji} ${pnl:,.2f}")
    
    return '\n'.join(lines)
