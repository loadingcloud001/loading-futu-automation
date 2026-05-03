"""Notion sync module for futu automation.
Called by app.py after collecting daily option data to push results to Notion.

Architecture:
- Daily Snapshot DB: cleared + repopulated with TODAY ONLY (369 stocks)
- Historical Archive DB: append-only, keeps all past records
- Trade Journal DB: manually managed by user, not touched here
"""
import sys
import os
import math
import time
from datetime import datetime, date
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from notion_client import (
        DAILY_SNAPSHOT_DB_ID,
        HISTORICAL_ARCHIVE_DB_ID,
        clear_database,
        add_page,
        add_pages_batch,
        query_database,
        title_val,
        number_val,
        date_val,
        select_val,
        rich_text_val,
    )
except ImportError:
    # Standalone usage: import directly
    from notion_client import (
        DAILY_SNAPSHOT_DB_ID,
        HISTORICAL_ARCHIVE_DB_ID,
        clear_database,
        add_page,
        add_pages_batch,
        query_database,
        title_val,
        number_val,
        date_val,
        select_val,
        rich_text_val,
    )


def _safe_float(v, default=0.0) -> float:
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (TypeError, ValueError):
        return default


def compute_metrics(today_data: dict, yesterday_data: dict,
                    trend_data: dict = None, earnings_data: dict = None) -> list:
    """Compute all derived metrics for each stock.
    
    Args:
        today_data: {stock_code: (tc, tp, ivc, ivp, stock_price)} today's raw data
        yesterday_data: {stock_code: (tc, tp, ivc, ivp)} yesterday's raw data
        trend_data: {stock_code: {iv_ma5, iv_rank, ...}} from trend_analyzer
        earnings_data: {stock_code: {next_earnings_date, flag, ...}} from earnings_calendar
    
    Returns:
        List of dicts with all computed metrics ready for Notion upload
    """
    today_str = date.today().strftime("%Y-%m-%d")
    results = []

    for stock, values in today_data.items():
        if len(values) >= 5: tc, tp, ivc, ivp, stock_price = values
        else: tc, tp, ivc, ivp = values; stock_price = 0
        tc = int(_safe_float(tc))
        tp = int(_safe_float(tp))
        ivc = _safe_float(ivc) / 100.0  # Convert from raw to decimal
        ivp = _safe_float(ivp) / 100.0

        total = tc + tp
        pc_ratio = round(tp / tc, 2) if tc > 0 else (999 if tp > 0 else 0)

        # Yesterday comparison
        y_tc, y_tp, y_ivc, y_ivp = yesterday_data.get(stock, (0, 0, 0, 0))
        y_tc = int(_safe_float(y_tc))
        y_tp = int(_safe_float(y_tp))
        y_ivc = _safe_float(y_ivc) / 100.0
        y_ivp = _safe_float(y_ivp) / 100.0
        y_total = y_tc + y_tp

        t_delta = round((total - y_total) / y_total, 4) if y_total > 0 else 0
        ivc_delta = round((ivc - y_ivc) / y_ivc, 4) if y_ivc > 0 else 0
        iv_spread = round(ivc - ivp, 4)

        # Anomaly detection
        is_anomaly = abs(t_delta) > 0.2 or abs(ivc_delta) > 0.2
        if is_anomaly:
            anomaly = "🔴 異常"
        elif abs(t_delta) > 0.1:
            anomaly = "🟡 關注"
        else:
            anomaly = "🟢 正常"

        # Signal text
        signals = []
        if tp > 0 and tc / tp > 2:
            signals.append(f"CALL主導({tc / tp:.1f}x)")
        elif tc > 0 and tp / tc > 2:
            signals.append(f"PUT主導({tp / tc:.1f}x)")
        if abs(t_delta) > 0.2:
            signals.append(f"量變{t_delta:+.0%}")
        if abs(ivc_delta) > 0.2:
            signals.append(f"IV變{ivc_delta:+.0%}")
        if iv_spread > 0.05:
            signals.append("Call IV溢價")
        elif iv_spread < -0.05:
            signals.append("Put IV溢價")

        signal_text = " | ".join(signals) if signals else ""

        # Trade planning using Black-Scholes (if stock_price available)
        call_plan = {}
        put_plan = {}
        stock_price = today_data.get(stock, (0, 0, 0, 0, 0))[4] if len(today_data.get(stock, (0,))) > 4 else 0
        
        if stock_price > 0:
            try:
                from trade_planner import plan_trade
                # Generate Call plan
                if ivc > 0:
                    cp = plan_trade(stock, stock_price, round(stock_price, -1) if stock_price > 50 else round(stock_price, 0),
                                    iv_call=ivc, iv_put=ivp, is_call=True, risk_amount=182.41)
                    call_plan = cp
                # Generate Put plan
                if ivp > 0:
                    pp = plan_trade(stock, stock_price, round(stock_price, -1) if stock_price > 50 else round(stock_price, 0),
                                    iv_call=ivc, iv_put=ivp, is_call=False, risk_amount=182.41)
                    put_plan = pp
            except Exception:
                pass

        # Compute peak trade times from volume analyzer
        peak_time = ""
        second_time = ""
        try:
            from volume_analyzer import get_peak_times
            times = get_peak_times(stock)
            peak_time = times.get('peak_time', '')
            second_time = times.get('second_time', '')
        except Exception:
            pass

        results.append({
            "stock": stock,
            "date": today_str,
            "anomaly": anomaly,
            "total": total,
            "tc": tc,
            "tp": tp,
            "pc": pc_ratio,
            "t_delta": t_delta,
            "ivc": round(ivc, 4),
            "ivp": round(ivp, 4),
            "iv_spread": iv_spread,
            "ivc_delta": ivc_delta,
            "signal": signal_text,
            "industry": "",
            "stock_price": stock_price,
            "call_plan": call_plan,
            "put_plan": put_plan,
            "peak_time": peak_time,
            "second_time": second_time,
            # Trend data
            "iv_rank": round(trend_data.get(stock, {}).get('iv_rank', 50), 1) if trend_data else 50,
            "iv_trend_score": trend_data.get(stock, {}).get('iv_trend_score', 0) if trend_data else 0,
            "volume_momentum": trend_data.get(stock, {}).get('volume_momentum', 0) if trend_data else 0,
            "anomaly_streak": trend_data.get(stock, {}).get('anomaly_streak', 0) if trend_data else 0,
            "trend_summary": trend_data.get(stock, {}).get('trend_summary', '') if trend_data else '',
            # Earnings data
            "earnings_flag": earnings_data.get(stock, {}).get('flag', '') if earnings_data else '',
            "days_to_earnings": earnings_data.get(stock, {}).get('days_to_earnings', 999) if earnings_data else 999,
            "next_earnings_date": earnings_data.get(stock, {}).get('next_date', '') if earnings_data else '',
            "earnings_warning": earnings_data.get(stock, {}).get('warning_text', '') if earnings_data else '',
            "skip_long_option": earnings_data.get(stock, {}).get('should_skip_long_option', False) if earnings_data else False,
        })

    return results


def sync_daily_snapshot(metrics: list, log_fn=None) -> int:
    """Clear + repopulate Daily Snapshot with today's data.
    
    Returns number of entries written.
    """
    log = log_fn or print

    log("Clearing old Daily Snapshot entries...")
    clear_database(DAILY_SNAPSHOT_DB_ID)
    time.sleep(1)

    items = []
    for m in metrics:
        cp = m.get("call_plan", {}) or {}
        pp = m.get("put_plan", {}) or {}
        props = {
            "Stock": title_val(m["stock"]),
            "Date": date_val(m["date"]),
            "Stock Price": number_val(m.get("stock_price", 0)),
            "Total Turnover": number_val(m["total"]),
            "CALL Turnover": number_val(m["tc"]),
            "PUT Turnover": number_val(m["tp"]),
            "P/C Ratio": number_val(m["pc"]),
            "Turnover Δ%": number_val(m["t_delta"]),
            "Call IV": number_val(m["ivc"]),
            "Put IV": number_val(m["ivp"]),
            "IV Spread": number_val(m["iv_spread"]),
            "IVc Change": number_val(m["ivc_delta"]),
            "Signal": rich_text_val(m["signal"]),
            # Trade plan columns
            "Call Strike": number_val(cp.get("strike", 0)),
            "Call Buy Price": number_val(cp.get("buy_option_price", 0)),
            "Call Target Price": number_val(cp.get("profit_option_price", 0)),
            "Call Stop Price": number_val(cp.get("stop_option_price", 0)),
            "Call Contracts": number_val(cp.get("contracts", 0)),
            "Call R:R": number_val(cp.get("risk_reward_ratio", 0)),
            "Put Strike": number_val(pp.get("strike", 0)),
            "Put Buy Price": number_val(pp.get("buy_option_price", 0)),
            "Put Target Price": number_val(pp.get("profit_option_price", 0)),
            "Put Stop Price": number_val(pp.get("stop_option_price", 0)),
            "Put Contracts": number_val(pp.get("contracts", 0)),
            "Put R:R": number_val(pp.get("risk_reward_ratio", 0)),
            # Peak trade times
            "Best Trade Time": rich_text_val(m.get("peak_time", "")),
            "2nd Trade Time": rich_text_val(m.get("second_time", "")),
            # Trend metrics
            "IV Rank (20d)": number_val(m.get("iv_rank", 50) / 100),
            "IV Trend Score": number_val(m.get("iv_trend_score", 0)),
            "Volume Momentum": number_val(m.get("volume_momentum", 0) / 100),
            "Anomaly Streak": number_val(m.get("anomaly_streak", 0)),
            "Trend Summary": rich_text_val(m.get("trend_summary", "")),
            # Earnings
            "Earnings Flag": select_val(m.get("earnings_flag", "")) if m.get("earnings_flag") else {},
            "Days to Earnings": number_val(m.get("days_to_earnings", 999)),
            "Next Earnings": date_val(m["next_earnings_date"]) if m.get("next_earnings_date") else {},
            # Anomaly & signals
            "Anomaly": select_val(m["anomaly"]),
            "Direction Signal": select_val(m.get("direction_signal", "⚖️ 平衡")),
            "My Decision": select_val("⏳ 待分析"),
            "Notes": rich_text_val(""),
        }
        items.append(props)

    log(f"Writing {len(items)} stocks to Daily Snapshot...")
    count = add_pages_batch(DAILY_SNAPSHOT_DB_ID, items, delay=0.35)
    log(f"Daily Snapshot: {count}/{len(items)} written")
    return count


def append_historical(metrics: list, log_fn=None) -> int:
    """Append today's data to Historical Archive (keep all history).
    
    Returns number of entries written.
    """
    log = log_fn or print

    items = []
    for m in metrics:
        entry_title = f"{m['date']} {m['stock']}"
        props = {
            "Entry": title_val(entry_title),
            "Date": date_val(m["date"]),
            "Stock": rich_text_val(m["stock"]),
            "Anomaly": select_val(m["anomaly"]),
            "Total Turnover": number_val(m["total"]),
            "CALL Turnover": number_val(m["tc"]),
            "PUT Turnover": number_val(m["tp"]),
            "P/C Ratio": number_val(m["pc"]),
            "Turnover Δ%": number_val(m["t_delta"]),
            "Call IV": number_val(m["ivc"]),
            "Put IV": number_val(m["ivp"]),
            "IV Spread": number_val(m["iv_spread"]),
            "IVc Change": number_val(m["ivc_delta"]),
            "Signal": rich_text_val(m["signal"]),
            "Industry": rich_text_val(m["industry"]),
        }
        items.append(props)

    log(f"Writing {len(items)} entries to Historical Archive...")
    count = add_pages_batch("3551f5d1-7d2f-817a-bf45-de95e46791a1", items, delay=0.35)
    log(f"Historical Archive: {count}/{len(items)} written")
    return count


def get_yesterday_data() -> dict:
    """Read yesterday's stock data from Notion Historical Archive.
    Returns {stock_code: (tc, tp, ivc, ivp)} dict.
    """
    yesterday_str = date.today().strftime("%Y-%m-%d")
    # Query yesterday's entries from Historical Archive
    # This is a simplified approach - in production, query the Historical DB
    # for entries matching yesterday's date
    try:
        # For now, return empty (will be populated after first historical write)
        pages = query_database(
            "3551f5d1-7d2f-817a-bf45-de95e46791a1",
            {"property": "Date", "date": {"equals": yesterday_str}},
        )
        # ... parse pages to extract data
    except Exception:
        pass

    return {}


def full_sync(today_data: dict, yesterday_data: Optional[dict] = None, log_fn=None) -> dict:
    """Run complete sync: compute metrics, update Daily Snapshot, append Historical.
    
    Args:
        today_data: {stock_code: (tc, tp, ivc, ivp)}
        yesterday_data: optional yesterday's data for comparison
    
    Returns:
        {"daily_count": int, "historical_count": int}
    """
    log = log_fn or print

    if yesterday_data is None:
        yesterday_data = get_yesterday_data()

    # Fetch trend data from Historical Archive
    trend_data = {}
    try:
        from trend_analyzer import fetch_historical_data, compute_rolling_metrics
        history = fetch_historical_data(days_back=20)
        trend_data = compute_rolling_metrics(today_data, history)
        log(f"Trend analysis: {len(trend_data)} stocks processed")
    except Exception as e:
        log(f"Trend analysis skipped: {e}")

    # Fetch earnings data (cached, only if stock prices available)
    earnings_data = {}
    try:
        stocks_with_price = [k for k, v in today_data.items() if len(v) >= 5 and v[4] > 0]
        if stocks_with_price:
            from earnings_calendar import batch_fetch_earnings, enrich_daily_snapshot
            earnings_data = batch_fetch_earnings(stocks_with_price[:50], delay=0.3)  # Top 50 to avoid rate limits
            log(f"Earnings data: {len(earnings_data)} stocks with upcoming earnings")
    except Exception as e:
        log(f"Earnings fetch skipped: {e}")

    # Compute peak trading times from capital flow data (top 30 stocks)
    peak_times = {}
    try:
        top_stocks = sorted(today_data.keys(), key=lambda k: today_data[k][0]+today_data[k][1], reverse=True)[:30]
        from stock_api_client import batch_get_peak_times
        peak_times = batch_get_peak_times(top_stocks, delay=0.3)
        log(f"Peak trade times: {len(peak_times)} stocks computed")
    except Exception as e:
        log(f"Peak times skipped: {e}")

    log(f"Computing metrics for {len(today_data)} stocks...")
    metrics = compute_metrics(today_data, yesterday_data, trend_data, earnings_data)
    
    # Enrich metrics with computed peak times
    for m in metrics:
        stock = m['stock']
        if stock in peak_times and peak_times[stock].get('peak_time'):
            m['peak_time'] = peak_times[stock]['peak_time']
            m['second_time'] = peak_times[stock].get('second_time', '')

    # Count anomalies
    anomaly_count = sum(1 for m in metrics if m["anomaly"] == "🔴 異常")
    attention_count = sum(1 for m in metrics if m["anomaly"] == "🟡 關注")
    log(f"Anomalies: {anomaly_count} 🔴, {attention_count} 🟡")

    daily_count = sync_daily_snapshot(metrics, log_fn=log)
    historical_count = append_historical(metrics, log_fn=log)

    return {
        "daily_count": daily_count,
        "historical_count": historical_count,
        "anomaly_count": anomaly_count,
        "attention_count": attention_count,
    }
