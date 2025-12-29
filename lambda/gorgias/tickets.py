import os
from datetime import datetime, timezone
from typing import Any, Dict

from common import (
    STREAM_NAME,
    make_session,
    safe_get,
    s3_put_json,
    enqueue_next,
    extract_job,
    parse_iso_utc,
    should_stop_by_cutoff,
    time_budget_ok,
    S3_PREFIX_BASE,
    PAGE_SIZE,
)

ENDPOINT = "/tickets"
ORDER_BY_FIELD = "updated_datetime"  # as you requested
DIRECTION_DEFAULT = "asc"

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    job = extract_job(event)

    cursor = job.get("cursor")
    page_start = int(job.get("page_start", 1))
    run_id = job.get("run_id") or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    max_pages = int(job.get("max_pages", 60))
    direction = job.get("direction", DIRECTION_DEFAULT)

    cutoff = job.get("cutoff")
    cutoff_dt = parse_iso_utc(cutoff) if cutoff else None

    session = make_session()

    pages_written = 0
    next_cursor = cursor

    while pages_written < max_pages and time_budget_ok(context):
        params = {"limit": PAGE_SIZE, "order_by": f"{ORDER_BY_FIELD}:{direction}"}
        if next_cursor:
            params["cursor"] = next_cursor

        payload = safe_get(session, ENDPOINT, params=params)
        items = payload.get("data", [])
        if not items:
            next_cursor = None
            break

        page_no = page_start + pages_written
        s3_key = f"{S3_PREFIX_BASE}/tickets/{run_id}/page_{page_no}.json"
        s3_put_json(s3_key, payload)

        pages_written += 1
        next_cursor = (payload.get("meta") or {}).get("next_cursor")

        # stop if cutoff reached (ASC)
        if should_stop_by_cutoff(items, ORDER_BY_FIELD, direction, cutoff_dt):
            next_cursor = None
            break

        if not next_cursor:
            break

    # enqueue continuation if needed
    if next_cursor:
        enqueue_next({
            "cursor": next_cursor,
            "page_start": page_start + pages_written,
            "run_id": run_id,
            "max_pages": max_pages,
            "cutoff": cutoff,
            "direction": direction,
        })

    return {
        "stream": "tickets",
        "run_id": run_id,
        "pages_written": pages_written,
        "continued": bool(next_cursor),
    }
