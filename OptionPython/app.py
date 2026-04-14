import pygsheets
import pandas as pd
import numpy as np
from futu import *
from datetime import datetime
import time
import os
import threading
import queue
from typing import Callable, Optional

# === 設定參數 ===
FUTU_HOST = os.getenv('FUTU_OPEND_IP', 'futu-opend')
FUTU_PORT = int(os.getenv('FUTU_OPEND_PORT', '11111'))
SERVICE_FILE = os.getenv('SERVICE_FILE', '/app/service_account.json')
SHEET_NAME = os.getenv('SHEET_NAME', 'LID Risk Management')
WORKSHEET_TITLE = os.getenv('WORKSHEET_TITLE', 'Today')
EXCEL_PATH = os.getenv('EXCEL_PATH', '/app/20241205stocklist.xlsx')
CSV_PATH = os.getenv('CSV_PATH', '/app/20241205optionresults.csv')
TARGET_HOUR = os.getenv('TARGET_HOUR', '04').zfill(2)
TARGET_MINUTE = os.getenv('TARGET_MINUTE', '00').zfill(2)
FUTU_RSA_FILE_PATH = os.getenv('FUTU_RSA_FILE_PATH', '').strip()

# === 工具函式 ===
def format_log_line(msg: str) -> str:
    return f"[{datetime.now()}] {msg}"


def console_log(msg: str) -> None:
    print(format_log_line(msg), flush=True)


def init_futu_encryption(log_fn: Callable[[str], None]) -> None:
    if not FUTU_RSA_FILE_PATH:
        return
    try:
        SysConfig.set_init_rsa_file(FUTU_RSA_FILE_PATH)
        SysConfig.enable_proto_encrypt(True)
        log_fn(f"已啟用 Futu 協議加密 (RSA): {FUTU_RSA_FILE_PATH}")
    except Exception as e:
        log_fn(f"啟用 RSA 失敗：{e}")

def append_zero_row(stock_code):
    df_zero = pd.DataFrame({
        'stock': [stock_code],
        'turnoverc': [0],
        'turnoverp': [0],
        'ivc': [0.0],
        'ivp': [0.0]
    })
    return df_zero.infer_objects(copy=False)

def process_stock(quote_ctx, stock_code, log_fn: Callable[[str], None] = console_log):
    try:
        ret1, data1 = quote_ctx.get_option_expiration_date(code=stock_code)
        if ret1 != RET_OK:
            log_fn(f"{stock_code} 無法取得期權到期日")
            return append_zero_row(stock_code)

        filter_call = OptionDataFilter(delta_min=0.05, delta_max=0.92, vol_min=1)
        filter_put = OptionDataFilter(delta_min=-0.92, delta_max=-0.05, vol_min=1)

        ret2, data2 = quote_ctx.get_option_chain(code=stock_code, data_filter=filter_call, option_type=OptionType.CALL)
        ret3, data3 = quote_ctx.get_option_chain(code=stock_code, data_filter=filter_put, option_type=OptionType.PUT)

        cc = data2['code'].tolist() if ret2 == RET_OK and not data2.empty else []
        pp = data3['code'].tolist() if ret3 == RET_OK and not data3.empty else []

        if not cc or not pp:
            log_fn(f"{stock_code} 無有效期權代碼")
            return append_zero_row(stock_code)

        time.sleep(10)  # 避免 API 過載

        retc, datac = quote_ctx.get_market_snapshot(cc)
        retp, datap = quote_ctx.get_market_snapshot(pp)

        if retc != RET_OK or retp != RET_OK:
            log_fn(f"{stock_code} 市場快照取得失敗")
            return append_zero_row(stock_code)

        aaac = datac[['stock_owner', 'code', 'turnover', 'option_implied_volatility', 'option_type']]
        aaap = datap[['stock_owner', 'code', 'turnover', 'option_implied_volatility', 'option_type']]

        stock_owner = aaac['stock_owner'].iloc[0]
        turnoverc = aaac['turnover'].iloc[1:].astype(int).sum()
        turnoverp = aaap['turnover'].iloc[1:].astype(int).sum()
        ivc = aaac['option_implied_volatility'].iloc[1:].astype(float).mean()
        ivp = aaap['option_implied_volatility'].iloc[1:].astype(float).mean()

        df_row = pd.DataFrame({
            'stock': [stock_owner],
            'turnoverc': [turnoverc],
            'turnoverp': [turnoverp],
            'ivc': [ivc],
            'ivp': [ivp]
        })

        return df_row.infer_objects(copy=False)

    except Exception as e:
        log_fn(f"{stock_code} 處理例外：{e}")
        return append_zero_row(stock_code)


def run_once(
    log_fn: Callable[[str], None],
    publish_result_fn: Optional[Callable[[pd.DataFrame], None]] = None,
    stock_limit: Optional[int] = None,
    update_sheets: bool = True,
) -> None:
    log_fn("開始資料收集流程")

    init_futu_encryption(log_fn)

    quote_ctx = OpenQuoteContext(host=FUTU_HOST, port=FUTU_PORT)

    wks = None
    if update_sheets:
        # Google Sheets 授權
        try:
            gc = pygsheets.authorize(service_file=SERVICE_FILE)
            sh = gc.open(SHEET_NAME)
            wks = sh.worksheet('title', WORKSHEET_TITLE)
        except Exception as e:
            log_fn(f"Google Sheets 授權失敗：{e}")
            quote_ctx.close()
            return

    # 讀取股票清單
    try:
        stock_data = pd.read_csv('/app/20241205stocklist.csv')
        stock_list = stock_data['stock'].tolist()
    except Exception as e:
        log_fn(f"讀取 CSV 錯誤：{e}")
        quote_ctx.close()
        return

    if stock_limit is not None:
        try:
            limit_int = int(stock_limit)
        except Exception:
            limit_int = 0
        if limit_int > 0:
            stock_list = stock_list[:limit_int]
            log_fn(f"測試模式：只跑前 {limit_int} 支股票")

    # 初始化結果表格，強制指定欄位型別
    df_result = pd.DataFrame({
        'stock': pd.Series(dtype='str'),
        'turnoverc': pd.Series(dtype='int'),
        'turnoverp': pd.Series(dtype='int'),
        'ivc': pd.Series(dtype='float'),
        'ivp': pd.Series(dtype='float')
    })

    for idx, stock_code in enumerate(stock_list, start=1):
        log_fn(f"處理第 {idx}/{len(stock_list)} 支股票：{stock_code}")
        df_row = process_stock(quote_ctx, stock_code, log_fn=log_fn)
        df_result = pd.concat([df_result, df_row], ignore_index=True)
        time.sleep(1)

    log_fn(f"所有股票處理完成，共 {len(df_result)} 筆資料")

    # 填補空值並推斷型別（避免 FutureWarning）
    df_result = df_result.fillna(np.nan).infer_objects(copy=False)

    # 更新 Google Sheet
    if update_sheets and wks is not None:
        try:
            wks.set_dataframe(df_result, (1, 1))
            log_fn("Google Sheet 更新成功")
        except Exception as e:
            log_fn(f"更新 Google Sheet 錯誤：{e}")

    # 儲存 CSV
    try:
        df_result.to_csv(CSV_PATH, index=False)
        log_fn("CSV 儲存成功")
    except Exception as e:
        log_fn(f"儲存 CSV 錯誤：{e}")

    quote_ctx.close()
    log_fn("Futu Quote Context 已關閉")

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

        # Sleep in small chunks so we can exit quickly when stop_event is set.
        for _ in range(30):
            if stop_event is not None and stop_event.is_set():
                return
            time.sleep(1)


def try_run_gui() -> bool:
    """Run the Tkinter window if available. Returns True if GUI started."""
    if os.getenv('FUTU_GUI', '0') not in ('1', 'true', 'TRUE', 'yes', 'YES'):
        return False

    try:
        import tkinter as tk
        from tkinter.scrolledtext import ScrolledText
    except Exception as e:
        console_log(f"GUI 啟動失敗（tkinter 不可用）：{e}")
        return False

    if os.name != 'nt' and not os.getenv('DISPLAY'):
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
        kwargs={"log_fn": gui_log, "publish_result_fn": gui_publish_result, "stop_event": stop_event},
        daemon=True,
    )
    worker.start()

    poll_events()
    root.mainloop()
    return True


if __name__ == '__main__':
    # If GUI is enabled and available, it will run the scheduler in a background thread.
    if try_run_gui():
        raise SystemExit(0)

    # Fallback: original console behavior
    run_scheduler(log_fn=console_log)
