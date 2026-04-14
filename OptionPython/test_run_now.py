import os
from typing import Optional

import pandas as pd

from app import console_log, run_once


def _int_env(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None or val == "":
        return default
    try:
        return int(val)
    except Exception:
        return default


def main() -> None:
    # Run immediately (no TARGET_HOUR/MINUTE wait) and only process the first N stocks.
    stock_limit = _int_env("TEST_STOCK_LIMIT", 10)

    # By default, test mode skips Google Sheets updates.
    update_sheets = os.getenv("TEST_UPDATE_SHEETS", "0") in ("1", "true", "TRUE", "yes", "YES")

    console_log(f"TEST run-now starting. limit={stock_limit}, update_sheets={update_sheets}")

    # Collect + (still) write CSV via app.py behavior.
    run_once(
        log_fn=console_log,
        publish_result_fn=None,
        stock_limit=stock_limit,
        update_sheets=update_sheets,
    )

    console_log("TEST run-now finished")


if __name__ == "__main__":
    main()
