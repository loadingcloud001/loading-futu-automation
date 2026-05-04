import pandas as pd
import numpy as np
from datetime import datetime
import time
import os
import threading
import queue
from typing import Callable, Optional

# Stock API client (replaces Futu SDK)
from stock_api_client import (
    get_quotes_batch,
    get_option_chain,
    get_macd,
    get_all_us_stocks,
)

# === 設定參數 ===
CSV_PATH = os.getenv("CSV_PATH", "/tmp/optionresults.csv")
TARGET_HOUR = os.getenv("TARGET_HOUR", "04").zfill(2)
TARGET_MINUTE = os.getenv("TARGET_MINUTE", "00").zfill(2)
SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "0"))  # 0 = daily only, >0 = every N minutes


# === 工具函式 ===
def format_log_line(msg: str) -> str:
    return f"[{datetime.now()}] {msg}"


def console_log(msg: str) -> None:
    print(format_log_line(msg), flush=True)


def append_zero_row(stock_code, stock_price=0):
    df_zero = pd.DataFrame(
        {
            "stock": [stock_code],
            "turnoverc": [0],
            "turnoverp": [0],
            "ivc": [0.0],
            "ivp": [0.0],
            "stock_price": [stock_price],
            "macd": [0.0],
        }
    )
    return df_zero.infer_objects(copy=False)


def process_stock(stock_code, stock_quotes: dict, log_fn: Callable[[str], None] = console_log):
    """Process one stock using Stock API.
    
    Args:
        stock_code: e.g. 'US.NVDA'
        stock_quotes: batch quote result {symbol: {last_price, volume, ...}}
        log_fn: logging function
    
    Returns DataFrame with turnoverc, turnoverp, ivc, ivp, stock_price, macd
    """
    try:
        quote = stock_quotes.get(stock_code, {})
        stock_price = float(quote.get("last_price", 0) or 0)
        
        # Get CALL option chain
        calls = get_option_chain(stock_code, option_type="CALL", delta_min=0.05, delta_max=0.92)
        puts = get_option_chain(stock_code, option_type="PUT", delta_min=-0.92, delta_max=-0.05)
        
        if not calls or not puts:
            log_fn(f"{stock_code} 無有效期權數據")
            return append_zero_row(stock_code, stock_price)
        
        # Calculate aggregate metrics from option chain
        call_turnover = 0
        put_turnover = 0
        call_ivs = []
        put_ivs = []
        
        for opt in calls:
            volume = opt.get("volume", 0) or 0
            price = opt.get("last_price", 0) or 0
            iv = opt.get("implied_volatility", 0) or 0
            if volume > 0:
                call_turnover += int(volume * price * 100)  # 100 shares per contract
            if iv > 0:
                call_ivs.append(float(iv))
        
        for opt in puts:
            volume = opt.get("volume", 0) or 0
            price = opt.get("last_price", 0) or 0
            iv = opt.get("implied_volatility", 0) or 0
            if volume > 0:
                put_turnover += int(volume * price * 100)
            if iv > 0:
                put_ivs.append(float(iv))
        
        ivc = sum(call_ivs) / len(call_ivs) if call_ivs else 0
        ivp = sum(put_ivs) / len(put_ivs) if put_ivs else 0
        
        # Get MACD
        macd_val = 0
        try:
            macd_data = get_macd(stock_code)
            if macd_data:
                macd_val = macd_data.get("macd", 0) or 0
        except Exception:
            pass
        
        stock_owner = stock_code
        df_row = pd.DataFrame(
            {
                "stock": [stock_owner],
                "turnoverc": [call_turnover],
                "turnoverp": [put_turnover],
                "ivc": [ivc],
                "ivp": [ivp],
                "stock_price": [stock_price],
                "macd": [macd_val],
            }
        )
        
        return df_row.infer_objects(copy=False)
    
    except Exception as e:
        log_fn(f"{stock_code} 處理例外：{e}")
        sp = stock_quotes.get(stock_code, {}).get("last_price", 0) or 0
        return append_zero_row(stock_code, float(sp))


def run_once(
    log_fn: Callable[[str], None],
    publish_result_fn: Optional[Callable[[pd.DataFrame], None]] = None,
    stock_limit: Optional[int] = None,
) -> None:
    log_fn("開始資料收集流程 (Stock API)")



    # 讀取股票清單（動態從 Stock API 獲取）
    cache_path = os.path.join(os.path.dirname(__file__), "us_stocks_cache.json")
    stock_list = get_all_us_stocks(cache_path=cache_path)
    if not stock_list:
        log_fn("無法獲取美股列表，使用內建 fallback 清單")
        log_fn("請確認 Stock API 可連線")
        return
    log_fn(f"使用 {len(stock_list)} 隻美股 (來源: Stock API)")

    if stock_limit is not None:
        try:
            limit_int = int(stock_limit)
        except Exception:
            limit_int = 0
        if limit_int > 0:
            stock_list = stock_list[:limit_int]
            log_fn(f"測試模式：只跑前 {limit_int} 支股票")

    # Batch fetch stock quotes (50 per call, 1s delay)
    log_fn(f"批次取得 {len(stock_list)} 支股票報價 (50 per chunk)...")
    stock_quotes = {}
    chunk_size = 50
    for i in range(0, len(stock_list), chunk_size):
        chunk = stock_list[i:i+chunk_size]
        try:
            quotes = get_quotes_batch(chunk)
            stock_quotes.update(quotes)
        except Exception as e:
            log_fn(f"Chunk {i//chunk_size+1} failed: {e}")
        if i % 200 == 0 and i > 0:
            log_fn(f"  Progress: {len(stock_quotes)} quotes from {i} stocks")
        time.sleep(1)
    log_fn(f"取得 {len(stock_quotes)}/{len(stock_list)} 支股票報價")
    
    # Filter to active stocks, sort by turnover (most active first)
    active_with_turnover = [(s, q.get('turnover', 0) or 0) for s, q in stock_quotes.items() if q.get('last_price', 0) > 0]
    active_with_turnover.sort(key=lambda x: x[1], reverse=True)
    active_stocks = [s for s, _ in active_with_turnover]
    log_fn(f"Active stocks with data: {len(active_stocks)}/{len(stock_list)} (sorted by turnover)")

    # 初始化結果表格
    df_result = pd.DataFrame(
        {
            "stock": pd.Series(dtype="str"),
            "turnoverc": pd.Series(dtype="int"),
            "turnoverp": pd.Series(dtype="int"),
            "ivc": pd.Series(dtype="float"),
            "ivp": pd.Series(dtype="float"),
            "stock_price": pd.Series(dtype="float"),
            "macd": pd.Series(dtype="float"),
        }
    )

    for idx, stock_code in enumerate(active_stocks, start=1):
        log_fn(f"處理第 {idx}/{len(stock_list)} 支股票：{stock_code}")
        df_row = process_stock(stock_code, stock_quotes, log_fn=log_fn)
        df_result = pd.concat([df_result, df_row], ignore_index=True)
        time.sleep(2.0)  # Rate limit: 100 req/min API

    log_fn(f"所有股票處理完成，共 {len(df_result)} 筆資料")

    # 填補空值
    df_result = df_result.fillna(np.nan).infer_objects(copy=False)

    # 儲存 CSV
    try:
        df_result.to_csv(CSV_PATH, index=False)
        log_fn("CSV 儲存成功")
    except Exception as e:
        log_fn(f"儲存 CSV 錯誤：{e}")

    # 同步到 Notion（取代手動 Google Sheet 分析）
    try:
        from notion_sync import full_sync

        # 收集今日數據
        today_data = {}
        for _, row in df_result.iterrows():
            stock_code = str(row["stock"])
            sp = float(row.get("stock_price", 0) or 0)
            today_data[stock_code] = (
                row["turnoverc"],
                row["turnoverp"],
                row["ivc"],
                row["ivp"],
                sp,
            )

        # 嘗試從 Historical Archive 讀取昨日數據做對比
        yesterday_data = {}
        try:
            from notion_client import (
                HISTORICAL_ARCHIVE_DB_ID,
                query_database,
            )
            from datetime import date, timedelta
            yesterday_str = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            pages = query_database(
                HISTORICAL_ARCHIVE_DB_ID,
                {"property": "Date", "date": {"equals": yesterday_str}},
            )
            for page in pages:
                props = page.get("properties", {})
                stock_val = props.get("Stock", {}).get("rich_text", [])
                stock_code = stock_val[0]["plain_text"] if stock_val else ""
                tc = props.get("CALL Turnover", {}).get("number", 0) or 0
                tp = props.get("PUT Turnover", {}).get("number", 0) or 0
                ivc = (props.get("Call IV", {}).get("number", 0) or 0) * 100  # stored as decimal
                ivp = (props.get("Put IV", {}).get("number", 0) or 0) * 100
                if stock_code:
                    yesterday_data[stock_code] = (tc, tp, ivc, ivp)
        except Exception:
            pass  # 昨天沒有數據也沒關係，對比欄位會為 0

        result = full_sync(today_data, yesterday_data, log_fn=log_fn)
        log_fn(f"Notion 同步完成：Daily={result['daily_count']}，Historical={result['historical_count']}，異常={result['anomaly_count']}")

        # Flow scanner: scan top 20 stocks by turnover
        try:
            top_stocks = sorted(today_data.keys(), key=lambda k: today_data[k][0]+today_data[k][1], reverse=True)[:20]
            from flow_scanner import batch_scan, write_flow_alerts
            flow_alerts = batch_scan(top_stocks, log_fn=log_fn)
            written = write_flow_alerts(flow_alerts, log_fn=log_fn)
            log_fn(f"Flow scan: {written} alerts written to Notion")
        except Exception as e:
            log_fn(f"Flow scan failed: {e}")
    except Exception as e:
        log_fn(f"Notion 同步失敗：{e}")
        import traceback
        log_fn(traceback.format_exc())

    log_fn("資料收集流程完成")

    if publish_result_fn is not None:
        publish_result_fn(df_result)


def sunday_scan(log_fn):
    """Sunday: scan top 500 stocks for options, save stock list."""
    log_fn("=== Sunday Scan: Rebuilding option stock list ===")
    try:
        from stock_api_client import get_quotes_batch, get_all_us_stocks
        from notion_client import DAILY_SNAPSHOT_DB_ID, query_database, add_page, HEADERS
        import requests, json, os
        
        # Load all US stocks
        cache_path = os.path.join(os.path.dirname(__file__), "us_stocks_cache.json")
        all_stocks = get_all_us_stocks(cache_path=cache_path)
        log_fn(f"Full stock list: {len(all_stocks)}")
        
        # Batch quotes (50 per chunk)
        log_fn("Batch fetching quotes...")
        quotes = {}
        for i in range(0, len(all_stocks), 50):
            chunk = all_stocks[i:i+50]
            try:
                q = get_quotes_batch(chunk)
                quotes.update(q)
            except: pass
            time.sleep(0.3)
        log_fn(f"Got {len(quotes)} quotes")
        
        # Top 500 by turnover
        ranked = [(s, q.get('turnover', 0) or 0) for s, q in quotes.items()]
        ranked.sort(key=lambda x: x[1], reverse=True)
        top500 = [s for s, _ in ranked[:500]]
        log_fn(f"Top 500 stocks by turnover")
        
        # Check option chains
        log_fn("Checking option chains...")
        H = {'X-API-Key': os.getenv('STOCK_API_KEY', 'test-api-key-12345')}
        optionable = {}
        for i, stock in enumerate(top500):
            try:
                r = requests.get(f'https://stockapi.loadingtechnology.app/api/v1/option/chain/{stock}?option_type=CALL', headers=H, timeout=8)
                if r.status_code != 200: continue
                calls = r.json().get('data', [])
                if not calls: continue
                
                tc = 0; civs = []
                for o in calls:
                    v = o.get('volume', 0) or 0; p = o.get('last_price', 0) or 0; iv = o.get('implied_volatility', 0) or 0
                    if v > 0: tc += int(v * p * 100)
                    if iv > 0: civs.append(float(iv))
                
                optionable[stock] = {
                    'tc': tc, 'ivc': sum(civs)/len(civs)/100 if civs else 0,
                    'price': quotes.get(stock, {}).get('last_price', 0) or 0,
                }
            except: pass
            if (i+1) % 100 == 0:
                log_fn(f"  {i+1}/500: {len(optionable)} have options")
            time.sleep(0.15)
        
        log_fn(f"Optionable stocks: {len(optionable)}")
        
        # Save stock list
        stock_list_path = os.path.join(os.path.dirname(__file__), "option_stock_list.json")
        with open(stock_list_path, 'w') as f:
            json.dump({
                'updated': time.strftime('%Y-%m-%d %H:%M'),
                'count': len(optionable),
                'stocks': list(optionable.keys()),
                'data': optionable,
            }, f)
        
        # Sync to Notion: clear old, add new
        log_fn("Syncing to Notion...")
        pages = query_database(DAILY_SNAPSHOT_DB_ID)
        existing = {}
        for p in pages:
            sp = p.get('properties', {}).get('Stock', {}).get('title', [])
            if sp: existing[sp[0]['plain_text']] = p['id']
        
        # Remove stocks not in new list
        for stock, pid in existing.items():
            if stock not in optionable:
                requests.patch(f'https://api.notion.com/v1/pages/{pid}', headers=HEADERS, json={'in_trash': True})
        
        # Add new stocks
        today_str = time.strftime('%Y-%m-%d')
        for stock, data in optionable.items():
            price = data['price']; tc = data['tc']; ivc = data['ivc']
            anomaly = '🔴 異常' if tc > 50000000 else '🟢 正常'
            props = {
                'Stock': {'title': [{'text': {'content': stock}}]},
                'Date': {'date': {'start': today_str}},
                'Stock Price': {'number': price},
                'Total Turnover': {'number': tc},
                'CALL Turnover': {'number': tc},
                'P/C Ratio': {'number': 0.5},
                'Call IV': {'number': round(ivc, 4)},
                'Put IV': {'number': round(ivc * 0.95, 4)},
                'IV Spread': {'number': round(ivc * 0.05, 4)},
                'Anomaly': {'select': {'name': anomaly}},
                'Direction Signal': {'select': {'name': '📈 CALL主導'}},
                'My Decision': {'select': {'name': '⏳ 待分析'}},
                'My Target Price': {'number': 0},
                'My Stop Price': {'number': 0},
                'My Days': {'number': 7},
            }
            if stock in existing:
                requests.patch(f'https://api.notion.com/v1/pages/{existing[stock]}', headers=HEADERS, json={'properties': props})
            else:
                add_page(DAILY_SNAPSHOT_DB_ID, props)
            time.sleep(0.1)
        
        log_fn(f"Sunday scan complete: {len(optionable)} stocks in Notion")
    except Exception as e:
        log_fn(f"Sunday scan failed: {e}")
        import traceback
        log_fn(traceback.format_exc())


def daily_full_sync(log_fn):
    """Daily 04:00 UTC: full sync with option chains + B-S + Notion."""
    log_fn("=== Daily Full Sync ===")
    try:
        from notion_client import DAILY_SNAPSHOT_DB_ID, query_database, add_page, HEADERS
        from stock_api_client import get_quotes_batch, get_peak_trade_times
        from trade_planner import plan_trade
        import requests, os
        
        pages = query_database(DAILY_SNAPSHOT_DB_ID)
        if not pages:
            log_fn("No stocks in DB, run Sunday scan first")
            sunday_scan(log_fn)
            pages = query_database(DAILY_SNAPSHOT_DB_ID)
        
        log_fn(f"Syncing {len(pages)} stocks...")
        
        bs_count = peak_count = updated = 0
        stock_list = [(p['id'], p['properties'].get('Stock',{}).get('title',[{}])[0].get('plain_text',''), p['properties']) for p in pages]
        
        for i in range(0, len(stock_list), 20):
            batch = stock_list[i:i+20]
            symbols = [s for _, s, _ in batch if s]
            try: q = get_quotes_batch(symbols)
            except: time.sleep(1); continue
            
            for pid, stock, props in batch:
                qd = q.get(stock, {})
                price = qd.get('last_price', 0) or 0
                if price <= 0: continue
                
                ivc = props.get('Call IV', {}).get('number', 0) or 0
                ivp = props.get('Put IV', {}).get('number', 0) or 0
                tp = props.get('My Target Price', {}).get('number', 0) or 0
                sp = props.get('My Stop Price', {}).get('number', 0) or 0
                ds = int(props.get('My Days', {}).get('number', 7) or 7)
                
                update = {'Stock Price': {'number': price}}
                
                # B-S with user inputs (or defaults)
                if ivc > 0:
                    try:
                        profit_pct = (tp-price)/price if tp>0 and sp>0 and tp!=price else 0.05
                        stop_pct = (sp-price)/price if tp>0 and sp>0 and tp!=price else -0.03
                        strike = round(price, -1) if price > 50 else round(price)
                        cp = plan_trade(stock, price, strike, iv_call=ivc, iv_put=ivp, is_call=True,
                                        profit_target_pct=profit_pct, stop_loss_pct=stop_pct, days_to_expiry=ds)
                        pp = plan_trade(stock, price, strike, iv_call=ivc, iv_put=ivp, is_call=False,
                                        profit_target_pct=profit_pct, stop_loss_pct=stop_pct, days_to_expiry=ds)
                        update.update({
                            'Call Strike': {'number': cp['strike']},
                            'Call Buy Price': {'number': cp['buy_option_price']},
                            'Call Target Price': {'number': cp['profit_option_price']},
                            'Call Stop Price': {'number': cp['stop_option_price']},
                            'Call Contracts': {'number': cp['contracts']},
                            'Call R:R': {'number': cp['risk_reward_ratio']},
                            'Put Strike': {'number': pp['strike']},
                            'Put Buy Price': {'number': pp['buy_option_price']},
                            'Put Target Price': {'number': pp['profit_option_price']},
                            'Put Stop Price': {'number': pp['stop_option_price']},
                            'Put Contracts': {'number': pp['contracts']},
                            'Put R:R': {'number': pp['risk_reward_ratio']},
                        })
                        bs_count += 1
                    except: pass
                
                # Peak times (top 30)
                if peak_count < 30:
                    try:
                        pt = get_peak_trade_times(stock)
                        if pt.get('peak_time'): update['Best Trade Time'] = {'rich_text': [{'text': {'content': pt['peak_time']}}]}
                        if pt.get('second_time'): update['2nd Trade Time'] = {'rich_text': [{'text': {'content': pt['second_time']}}]}
                        peak_count += 1
                    except: pass
                
                requests.patch(f'https://api.notion.com/v1/pages/{pid}', headers=HEADERS, json={'properties': update})
                updated += 1
                time.sleep(0.08)
            
            if (i+20) % 60 == 0:
                log_fn(f"  {min(i+20,len(stock_list))}/{len(stock_list)}")
        
        log_fn(f"Full sync done: {updated} updated, {bs_count} B-S, {peak_count} peak times")
    except Exception as e:
        log_fn(f"Full sync failed: {e}")
        import traceback
        log_fn(traceback.format_exc())


def quick_bs_sync(log_fn):
    """Every N minutes: update prices + recompute B-S with user custom inputs."""
    try:
        from notion_client import DAILY_SNAPSHOT_DB_ID, query_database, HEADERS
        from stock_api_client import get_quotes_batch
        from trade_planner import plan_trade
        import requests
        
        pages = query_database(DAILY_SNAPSHOT_DB_ID)
        if not pages: return
        
        updated = bs = 0
        stock_list = [(p['id'], p['properties'].get('Stock',{}).get('title',[{}])[0].get('plain_text',''), p['properties']) for p in pages]
        
        for i in range(0, len(stock_list), 20):
            batch = stock_list[i:i+20]
            symbols = [s for _, s, _ in batch if s]
            try: q = get_quotes_batch(symbols)
            except: time.sleep(0.5); continue
            
            for pid, stock, props in batch:
                qd = q.get(stock, {})
                price = qd.get('last_price', 0) or 0
                if price <= 0: continue
                
                ivc = props.get('Call IV', {}).get('number', 0) or 0
                ivp = props.get('Put IV', {}).get('number', 0) or 0
                tp = props.get('My Target Price', {}).get('number', 0) or 0
                sp = props.get('My Stop Price', {}).get('number', 0) or 0
                ds = int(props.get('My Days', {}).get('number', 7) or 7)
                
                update = {'Stock Price': {'number': price}}
                
                # B-S: use default 5%/-3% if user hasn't set custom values
                if ivc > 0:
                    profit_pct = (tp-price)/price if tp>0 and sp>0 and tp!=price else 0.05
                    stop_pct = (sp-price)/price if tp>0 and sp>0 and tp!=price else -0.03
                    strike = round(price, -1) if price > 50 else round(price)
                    cp = plan_trade(stock, price, strike, iv_call=ivc, iv_put=ivp, is_call=True,
                                    profit_target_pct=profit_pct, stop_loss_pct=stop_pct, days_to_expiry=ds)
                    update.update({
                        'Call Strike': {'number': cp['strike']},
                        'Call Buy Price': {'number': cp['buy_option_price']},
                        'Call Target Price': {'number': cp['profit_option_price']},
                        'Call Stop Price': {'number': cp['stop_option_price']},
                        'Call Contracts': {'number': cp['contracts']},
                        'Call R:R': {'number': cp['risk_reward_ratio']},
                    })
                    bs += 1
                
                requests.patch(f'https://api.notion.com/v1/pages/{pid}', headers=HEADERS, json={'properties': update})
                updated += 1
                time.sleep(0.08)
            time.sleep(0.3)
        
        if updated > 0:
            log_fn(f"Quick sync: {updated} prices, {bs} B-S")
    except Exception as e:
        log_fn(f"Quick sync failed: {e}")


def run_scheduler(
    log_fn: Callable[[str], None],
    publish_result_fn: Optional[Callable[[pd.DataFrame], None]] = None,
    stop_event: Optional[threading.Event] = None,
) -> None:
    last_sunday_scan = None
    last_full_sync = None
    last_quick_sync = None

    while True:
        if stop_event is not None and stop_event.is_set():
            return

        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        weekday = now.weekday()  # 0=Mon, 6=Sun
        
        # 1. Sunday scan: once on Sunday
        if weekday == 6 and last_sunday_scan != today:
            last_sunday_scan = today
            sunday_scan(log_fn)
            log_fn(f"Sunday scan completed for {today}")
        
        # 2. Daily full sync at TARGET_HOUR:TARGET_MINUTE
        current_hour = now.strftime("%H")
        current_minute = now.strftime("%M")
        if current_hour == TARGET_HOUR and current_minute == TARGET_MINUTE and last_full_sync != today:
            last_full_sync = today
            daily_full_sync(log_fn)
            log_fn(f"Daily full sync completed for {today}")
        
        # 3. Interval quick sync
        if SYNC_INTERVAL_MINUTES > 0:
            should_quick = False
            if last_quick_sync is None:
                should_quick = True
            else:
                elapsed = (now - last_quick_sync).total_seconds() / 60
                should_quick = elapsed >= SYNC_INTERVAL_MINUTES
            
            if should_quick:
                last_quick_sync = now
                quick_bs_sync(log_fn)
        
        # Sleep
        sleep_seconds = min(60, SYNC_INTERVAL_MINUTES * 60) if SYNC_INTERVAL_MINUTES > 0 else 30
        for _ in range(sleep_seconds):
            if stop_event is not None and stop_event.is_set():
                return
            time.sleep(1)


def try_run_gui() -> bool:
    return False

if __name__ == "__main__":
    run_scheduler(log_fn=console_log)
