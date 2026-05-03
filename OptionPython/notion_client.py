"""Notion API client for futu automation data pipeline."""
import os
import time
import requests
from typing import Optional

NOTION_KEY = os.getenv("NOTION_API_KEY", "ntn_l8419114287aA45IOyliwxOSk6QtQ3HbMx9Pxa42hSzcwz")
NOTION_VERSION = "2022-06-28"
NOTION_BASE = "https://api.notion.com/v1"

HEADERS = {
    "Authorization": f"Bearer {NOTION_KEY}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# ── Page IDs ──────────────────────────────────────────────
US_OPTIONS_PAGE_ID = "3531f5d17d2f80bab97ff884088b1146"
HK_OPTIONS_PAGE_ID = "3541f5d17d2f80b68a3efa9b8cd1d30f"
NET_WORTH_PAGE_ID = "3541f5d17d2f80239e7cf139516b1c99"
SAVINGS_PAGE_ID = "3541f5d17d2f80a1898dfafb4971c6ed"
CALCULATOR_PAGE_ID = "3541f5d17d2f80e489d5e2b210e7e6b7"
LIFE_PLANNING_PAGE_ID = "3541f5d17d2f80c791e0f3b960ee49ea"

# ── US Options Database IDs ───────────────────────────────
DAILY_SNAPSHOT_DB_ID = "3551f5d1-7d2f-81c7-a117-cf4c04d83e81"
HISTORICAL_ARCHIVE_DB_ID = "3551f5d1-7d2f-817a-bf45-de95e46791a1"
TRADE_JOURNAL_DB_ID = "3551f5d1-7d2f-8138-b9a8-d6cf01ca0a43"
# US Options
ACCOUNTS_DB_ID = "3541f5d17d2f81578027e9b6b58f8c2b"
# Alerts & Flow
ALERTS_DB_ID = "3551f5d1-7d2f-8131-aa04-eb9101e044d0"
FLOW_DB_ID = "3551f5d1-7d2f-81f7-ba27-d1a75ad51717"


def _request(method: str, path: str, data: Optional[dict] = None) -> dict:
    url = f"{NOTION_BASE}{path}"
    resp = requests.request(method, url, headers=HEADERS, json=data)
    resp.raise_for_status()
    return resp.json()


def create_database(parent_page_id: str, title: str, properties: dict) -> str:
    """Create a Notion database under a page. Returns database_id."""
    payload = {
        "parent": {"type": "page_id", "page_id": parent_page_id},
        "title": [{"type": "text", "text": {"content": title}}],
        "properties": properties,
    }
    result = _request("POST", "/databases", payload)
    db_id = result["id"]
    print(f"  ✓ Created database: {title} ({db_id})")
    return db_id


def add_page(database_id: str, properties: dict) -> str:
    """Add a row to a database. Returns page_id."""
    payload = {
        "parent": {"database_id": database_id},
        "properties": properties,
    }
    result = _request("POST", "/pages", payload)
    return result["id"]


def add_pages_batch(database_id: str, items: list, delay: float = 0.34) -> int:
    """Add multiple rows to a database with rate-limit delay. Returns count added."""
    count = 0
    for item in items:
        try:
            add_page(database_id, item)
            count += 1
            time.sleep(delay)
        except Exception as e:
            print(f"  ✗ Failed: {item.get('Name', item)}: {e}")
    return count


def clear_database(database_id: str):
    """Archive all pages in a database (use with caution)."""
    has_more = True
    cursor = None
    while has_more:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        result = _request("POST", f"/databases/{database_id}/query", payload)
        for page in result.get("results", []):
            _request("PATCH", f"/pages/{page['id']}", {"archived": True, "in_trash": True})
        has_more = result.get("has_more", False)
        cursor = result.get("next_cursor")
        if has_more:
            time.sleep(0.3)


def query_database(database_id: str, filter_obj: Optional[dict] = None) -> list:
    """Query all pages from a database. Returns list of page objects."""
    results = []
    has_more = True
    cursor = None
    while has_more:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        if filter_obj:
            payload["filter"] = filter_obj
        result = _request("POST", f"/databases/{database_id}/query", payload)
        results.extend(result.get("results", []))
        has_more = result.get("has_more", False)
        cursor = result.get("next_cursor")
    return results


def page_exists(database_id: str, title_text: str) -> bool:
    """Check if a page with given title already exists in database."""
    try:
        result = _request(
            "POST",
            f"/databases/{database_id}/query",
            {
                "page_size": 1,
                "filter": {
                    "property": "title",
                    "title": {"equals": title_text},
                },
            },
        )
        return len(result.get("results", [])) > 0
    except Exception:
        # If title property not found, try common alternatives
        for prop_name in ["Trade", "Entry", "Alert"]:
            try:
                result = _request(
                    "POST",
                    f"/databases/{database_id}/query",
                    {
                        "page_size": 1,
                        "filter": {
                            "property": prop_name,
                            "rich_text": {"equals": title_text},
                        },
                    },
                )
                if len(result.get("results", [])) > 0:
                    return True
            except Exception:
                continue
        
        # Also check Title property
        try:
            result = _request(
                "POST",
                f"/databases/{database_id}/query",
                {
                    "page_size": 1,
                    "filter": {
                        "property": "Trade",
                        "title": {"equals": title_text},
                    },
                },
            )
            return len(result.get("results", [])) > 0
        except Exception:
            return False


def add_page_unique(database_id: str, properties: dict, title_text: str) -> Optional[str]:
    """Add a page only if a page with same title doesn't already exist.
    
    Returns page_id if created, None if skipped (duplicate).
    """
    if page_exists(database_id, title_text):
        return None
    return add_page(database_id, properties)


# ── Property builders ────────────────────────────────────

def title_prop() -> dict:
    return {"title": {}}


def number_prop(fmt: str = "number") -> dict:
    return {"number": {"format": fmt}}


def date_prop() -> dict:
    return {"date": {}}


def select_prop(options: list) -> dict:
    return {"select": {"options": [{"name": o, "color": "default"} for o in options]}}


def rich_text_prop() -> dict:
    return {"rich_text": {}}


def checkbox_prop() -> dict:
    return {"checkbox": {}}


def url_prop() -> dict:
    return {"url": {}}


# ── Value builders for adding pages ───────────────────────

def title_val(text: str) -> dict:
    return {"title": [{"text": {"content": text}}]}


def number_val(n: float) -> dict:
    return {"number": n}


def date_val(date_str: str) -> dict:
    return {"date": {"start": date_str}}


def select_val(name: str) -> dict:
    return {"select": {"name": name}} if name else {}


def rich_text_val(text: str) -> dict:
    return {"rich_text": [{"text": {"content": text}}]} if text else {"rich_text": []}


def checkbox_val(v: bool) -> dict:
    return {"checkbox": v}
