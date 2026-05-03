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
CSV_PATH = os.getenv("CSV_PATH", "/app/20241205optionresults.csv")
TARGET_HOUR = os.getenv("TARGET_HOUR", "04").zfill(2)
TARGET_MINUTE = os.getenv("TARGET_MINUTE", "00").zfill(2)


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
        log_fn("無法獲取美股列表，使用 fallback CSV")
        try:
            stock_data = pd.read_csv("/app/20241205stocklist.csv")
            stock_list = stock_data["stock"].tolist()
        except Exception:
            log_fn("Fallback CSV 也失敗，中止")
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

    # Batch fetch stock quotes (1 API call for ALL stocks)
    log_fn(f"批次取得 {len(stock_list)} 支股票報價...")
    stock_quotes = {}
    try:
        stock_quotes = get_quotes_batch(stock_list)
        log_fn(f"取得 {len(stock_quotes)} 支股票報價")
    except Exception as e:
        log_fn(f"批次報價失敗：{e}")
        return

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

    for idx, stock_code in enumerate(stock_list, start=1):
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


def run_scheduler(
    log_fn: Callable[[str], None],
    publish_result_fn: Optional[Callable[[pd.DataFrame], None]] = None,
    stop_event: Optional[threading.Event] = None,
) -> None:
    last_trigger_key: Optional[str] = None

    while True:
        if stop_event is not None and stop_event.is_set():
            return

        now = datetime.now()
        current_hour = now.strftime("%H")
        current_minute = now.strftime("%M")

        trigger_key = now.strftime("%Y-%m-%d %H:%M")
        should_trigger = (
            current_hour == TARGET_HOUR
            and current_minute == TARGET_MINUTE
            and trigger_key != last_trigger_key
        )

        if should_trigger:
            last_trigger_key = trigger_key
            run_once(log_fn=log_fn, publish_result_fn=publish_result_fn)


        else:
            log_fn(
                f"等待目標時間 {TARGET_HOUR}:{TARGET_MINUTE}，目前時間：{current_hour}:{current_minute}"
            )

        # Check if market is open and do quick anomaly scan
        try:
            from datetime import datetime as dt
            now_hkt = dt.now()
            # US market: 21:30-04:00 HKT (summer), 22:30-05:00 (winter)
            is_market = (now_hkt.hour >= 21 or now_hkt.hour < 5)
            is_minute_15 = now_hkt.minute % 15 == 0
            
            if is_market and is_minute_15 and not getattr(run_scheduler, '_last_monitor_minute', -1) == now_hkt.minute:
                run_scheduler._last_monitor_minute = now_hkt.minute
                try:
                    from stock_api_client import get_quotes_batch
                    from notion_client import add_page, title_val, rich_text_val, number_val, date_val, select_val, ALERTS_DB_ID
                    # Quick scan of top 10 anomaly stocks
                    top10 = ['US.NVDA','US.TSLA','US.AAPL','US.AMD','US.MSFT','US.AMZN','US.META','US.GOOGL','US.AVGO','US.INTC']
                    quotes = get_quotes_batch(top10)
                    for s, q in quotes.items():
                        turnover = q.get('turnover', 0) or 0
                        if turnover > 1000000000:  # > B turnover
                            add_page(ALERTS_DB_ID, {
                                'Alert': title_val(f'{dt.now().strftime("%H:%M")} - {s}'),
                                'Date': date_val(dt.now().strftime('%Y-%m-%d')),
                                'Stock': rich_text_val(s),
                                'Type': select_val('🔥 Turnover Burst'),
                                'Severity': select_val('🔴 High'),
                                'Message': rich_text_val(f'{s} turnover ${turnover:,.0f} during market'),
                                'Value': number_val(turnover),
                                'Price': number_val(q.get('last_price', 0)),
                            })
                except Exception:
                    pass
        except Exception:
            pass

        # Sleep in small chunks so we can exit quickly when stop_event is set.
        for _ in range(30):
            if stop_event is not None and stop_event.is_set():
                return
            time.sleep(1)


def try_run_gui() -> bool:
    """Run the Tkinter window if available. Returns True if GUI started."""
    if os.getenv("FUTU_GUI", "0") not in ("1", "true", "TRUE", "yes", "YES"):
        return False

    try:
        import tkinter as tk
        from tkinter.scrolledtext import ScrolledText
    except Exception as e:
        console_log(f"GUI 啟動失敗（tkinter 不可用）：{e}")
        return False

    if os.name != "nt" and not os.getenv("DISPLAY"):
        console_log("GUI 未啟動：找不到 DISPLAY（可能在 Docker/無視窗環境）")
        return False

    event_queue: "queue.Queue[tuple[str, str]]" = queue.Queue()
    stop_event = threading.Event()

    def gui_log(msg: str) -> None:
        event_queue.put(("log", format_log_line(msg)))

    def gui_publish_result(df: pd.DataFrame) -> None:
        preview = df.to_string(index=False)
        event_queue.put(("result", preview))

    root = tk.Tk()
    root.title("Futu Status")
    root.geometry("1100x650")

    paned = tk.PanedWindow(root, orient=tk.HORIZONTAL)
    paned.pack(fill=tk.BOTH, expand=True)

    left = tk.Frame(paned)
    right = tk.Frame(paned)
    paned.add(left, stretch="always")
    paned.add(right, stretch="always")

    left_label = tk.Label(left, text="Run Print Time")
    left_label.pack(anchor="w")
    log_text = ScrolledText(left, wrap=tk.WORD)
    log_text.pack(fill=tk.BOTH, expand=True)

    right_label = tk.Label(right, text="Futu Result (when target time met)")
    right_label.pack(anchor="w")
    result_text = ScrolledText(right, wrap=tk.NONE)
    result_text.pack(fill=tk.BOTH, expand=True)
    result_text.insert(tk.END, f"Waiting for {TARGET_HOUR}:{TARGET_MINUTE}...\n")

    def poll_events() -> None:
        try:
            while True:
                kind, payload = event_queue.get_nowait()
                if kind == "log":
                    log_text.insert(tk.END, payload + "\n")
                    log_text.see(tk.END)
                elif kind == "result":
                    result_text.delete("1.0", tk.END)
                    result_text.insert(tk.END, payload + "\n")
                    result_text.see("1.0")
        except queue.Empty:
            pass

        root.after(200, poll_events)

    def on_close() -> None:
        stop_event.set()
        root.after(300, root.destroy)

    root.protocol("WM_DELETE_WINDOW", on_close)

    worker = threading.Thread(
        target=run_scheduler,
        kwargs={
            "log_fn": gui_log,
            "publish_result_fn": gui_publish_result,
            "stop_event": stop_event,
        },
        daemon=True,
    )
    worker.start()

    poll_events()
    root.mainloop()
    return True


if __name__ == "__main__":
    # If GUI is enabled and available, it will run the scheduler in a background thread.
    if try_run_gui():
        raise SystemExit(0)

    # Fallback: original console behavior
    run_scheduler(log_fn=console_log)
