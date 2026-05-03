"""Volume Analyzer - replicates Excel Calculation Sheet logic.

Takes option turnover data and ranks 5-minute time slots by average trading volume.
Used by 投資計算 to determine best/worst trading times per stock.

Excel formula replicated:
  C1: =sumif(csv!M:M, B1, csv!J:J) / COUNTIF(csv!M:M, B1)  (avg turnover per slot)
  D1: =C1/sum(C:C)  (turnover weight %)
  A1: =rank(D1, D:D)  (rank by weight)
"""

from collections import defaultdict
from typing import List, Tuple


def analyze_time_slots(
    trades: List[dict],
    slot_minutes: int = 5
) -> dict:
    """Analyze volume distribution across time slots.
    
    Args:
        trades: list of dicts with keys: time (HH:MM), turnover (float)
        slot_minutes: size of each time slot in minutes (default 5)
    
    Returns dict with:
        slots: list of (time, avg_turnover, weight_pct, rank) sorted by rank
        peak_times: top 3 highest volume times
        low_times: bottom 3 lowest volume times
    """
    if not trades:
        return {'slots': [], 'peak_times': [], 'low_times': []}
    
    # Group by time slot
    slot_turnover = defaultdict(list)
    for t in trades:
        time_str = t.get('time', '')
        turnover = float(t.get('turnover', 0))
        if time_str and turnover > 0:
            # Round to nearest slot
            try:
                h, m = time_str.split(':')[:2]
                h, m = int(h), int(m)
                m_rounded = (m // slot_minutes) * slot_minutes
                slot_key = f'{h:02d}:{m_rounded:02d}'
                slot_turnover[slot_key].append(turnover)
            except (ValueError, IndexError):
                continue
    
    if not slot_turnover:
        return {'slots': [], 'peak_times': [], 'low_times': []}
    
    # Compute averages and weights
    slot_stats = []
    total_avg = 0
    for slot_key, turnovers in slot_turnover.items():
        avg = sum(turnovers) / len(turnovers)
        slot_stats.append({
            'time': slot_key,
            'avg_turnover': avg,
            'count': len(turnovers),
        })
        total_avg += avg
    
    # Compute weight percentages and rank
    for s in slot_stats:
        s['weight_pct'] = round(s['avg_turnover'] / total_avg * 100, 2) if total_avg > 0 else 0
    
    slot_stats.sort(key=lambda x: x['avg_turnover'], reverse=True)
    
    # Assign ranks
    slots = []
    for i, s in enumerate(slot_stats):
        slots.append({
            'rank': i + 1,
            'time': s['time'],
            'avg_turnover': round(s['avg_turnover'], 2),
            'weight_pct': s['weight_pct'],
            'count': s['count'],
        })
    
    return {
        'slots': slots,
        'peak_times': [s['time'] for s in slots[:3]],
        'low_times': [s['time'] for s in slots[-3:]] if len(slots) >= 3 else [],
        'total_slots': len(slots),
    }


def analyze_stock_option_volume(
    all_option_data: List[dict]
) -> dict:
    """Analyze option trading volume by time slot across all stocks.
    
    Args:
        all_option_data: list of dicts with keys: 
            stock (str), time (str HH:MM), turnover (float), option_type (Call/Put)
    
    Returns dict with:
        per_stock: {stock_code: volume_analysis_result}
        market_wide: volume analysis across all stocks
    """
    # Market-wide analysis
    market = analyze_time_slots(all_option_data)
    
    # Per-stock analysis
    per_stock = {}
    stock_groups = defaultdict(list)
    for t in all_option_data:
        stock_groups[t.get('stock', '')].append(t)
    
    for stock, trades in stock_groups.items():
        result = analyze_time_slots(trades)
        per_stock[stock] = result
    
    return {
        'market_wide': market,
        'per_stock': per_stock,
    }


def get_recommended_trade_times(
    stock_code: str,
    analysis_result: dict,
    prefer_high: bool = True
) -> Tuple[str, str, str]:
    """Get recommended trade times for a stock.
    
    Args:
        stock_code: stock ticker
        analysis_result: output from analyze_stock_option_volume()
        prefer_high: True = recommend high-volume times (for entry/exit)
                     False = recommend low-volume times (to avoid)
    
    Returns:
        (best_time, second_best, third_best) or ('', '', '') if no data
    """
    per_stock = analysis_result.get('per_stock', {})
    stock_data = per_stock.get(stock_code, {})
    times = stock_data.get('peak_times' if prefer_high else 'low_times', [])
    
    return (
        times[0] if len(times) > 0 else '',
        times[1] if len(times) > 1 else '',
        times[2] if len(times) > 2 else '',
    )


# ── Integration with existing peak trade time data ──

# These are manually maintained from 美股數據分析 col D-I
# They represent KNOWN peak trading times per stock (from historical analysis)
MANUAL_PEAK_TIMES = {}  # Will be populated from Notion/Excel migration

def get_peak_times(stock_code: str, computed: dict = None) -> dict:
    """Get peak trading times for a stock, preferring computed over manual.
    
    Returns dict with: peak_time, second_time, low_time
    """
    # Try computed data first
    if computed:
        per_stock = computed.get('per_stock', {})
        sd = per_stock.get(stock_code, {})
        peak = sd.get('peak_times', [])
        low = sd.get('low_times', [])
        if peak:
            return {
                'peak_time': peak[0] if len(peak) > 0 else '',
                'second_time': peak[1] if len(peak) > 1 else '',
                'low_time': low[0] if len(low) > 0 else '',
            }
    
    # Fall back to manual data (migrated from Excel)
    return {
        'peak_time': '',
        'second_time': '',
        'low_time': '',
    }
