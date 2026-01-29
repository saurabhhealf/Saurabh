"""
messages.py — Gorgias /search extractor (Hourly + Daily + Backfill)

MAJOR UPDATE:
- Uses /api/search endpoint instead of /api/messages.
- Enables efficient time-based filtering (Gap Fills take seconds, not hours).
- Solves the "Oldest First" sorting trap.
"""

import os
import json
import time
import random
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
import urllib.parse

import boto3
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -------------------- AWS clients --------------------
_ddb = boto3.resource("dynamodb")
_s3 = boto3.client("s3")
_sm = boto3.client("secretsmanager")

# -------------------- Env --------------------
GORGIAS_BASE_URL = os.environ.get("GORGIAS_BASE_URL", "https://healf-uk.gorgias.com/api").rstrip("/")
STATE_TABLE = os.environ["STATE_TABLE"]
S3_BUCKET = os.environ.get("S3_BUCKET", "sources-data")
S3_PREFIX_BASE = os.environ.get("S3_PREFIX_BASE", "gorgias").strip("/")

STREAM_NAME = "messages"
ENDPOINT = "/search"  # <--- CHANGED TO SEARCH

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "30")) # Search API max is often lower/different, safe default
PAGES_PER_INVOCATION = int(os.environ.get("PAGES_PER_INVOCATION", "50"))

# Rate limit + retries
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "8"))
BACKOFF_BASE_SECONDS = float(os.environ.get("BACKOFF_BASE_SECONDS", "1.0"))
BACKOFF_MAX_SECONDS = float(os.environ.get("BACKOFF_MAX_SECONDS", "30.0"))
MIN_SUCCESS_DELAY_SECONDS = float(os.environ.get("MIN_SUCCESS_DELAY_SECONDS", "0.2"))

# Lambda time handling
REQUEST_TIMEOUT = (10, 60)
TIME_LEFT_BUFFER_MS = int(os.environ.get("TIME_LEFT_BUFFER_MS", "15000"))

# Lease lock
LEASE_SECONDS = int(os.environ.get("LEASE_SECONDS", "180"))
LEASE_RENEW_EVERY_PAGES = int(os.environ.get("LEASE_RENEW_EVERY_PAGES", "5"))
CHECKPOINT_EVERY_PAGES = int(os.environ.get("CHECKPOINT_EVERY_PAGES", "5"))

# Secrets
GORGIAS_EMAIL_SECRET = os.environ.get("GORGIAS_EMAIL_SECRET", "gorgias_email")
GORGIAS_API_KEY_SECRET = os.environ.get("GORGIAS_API_KEY_SECRET", "gorgias_api_key")

TABLE = _ddb.Table(STATE_TABLE)


# -------------------- Helpers --------------------
def _utc_now_ts() -> int:
    return int(time.time())

def _utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _safe_json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception:
        return None

def _get_secret_value(secret_id: str) -> str:
    resp = _sm.get_secret_value(SecretId=secret_id)
    secret = (resp.get("SecretString") or "").strip()
    parsed = _safe_json_loads(secret)
    if isinstance(parsed, dict):
        for k in ["GORGIAS_EMAIL", "GORGIAS_API_KEY", "value", "secret", "token", "api_key"]:
            if k in parsed and isinstance(parsed[k], str) and parsed[k].strip():
                return parsed[k].strip().strip('"').strip("'")
    return secret.strip().strip('"').strip("'")

def _gorgias_auth() -> Tuple[str, str]:
    email = _get_secret_value(GORGIAS_EMAIL_SECRET)
    api_key = _get_secret_value(GORGIAS_API_KEY_SECRET)
    if not email or not api_key:
        raise ValueError("Missing Gorgias credentials")
    return email, api_key

def _time_left_ok(context, buffer_ms: int = TIME_LEFT_BUFFER_MS) -> bool:
    if context is None:
        return True
    try:
        return context.get_remaining_time_in_millis() > buffer_ms
    except Exception:
        return True

def _write_jsonl_to_s3(stream: str, job_start_id: str, page: int, records: List[Dict[str, Any]]) -> str:
    if not records:
        return ""
    dt = _utc_today_str()
    key = f"{S3_PREFIX_BASE}/{stream}/dt={dt}/job={job_start_id}/page={page:06d}.json"
    body = "".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n" for r in records)
    _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
    logger.info(f"[{stream}] wrote s3://{S3_BUCKET}/{key} rows={len(records)}")
    return key

# -------------------- Date / Query Logic --------------------
def _build_search_query(target_date_str: str) -> str:
    """
    Constructs the Gorgias Lucene query for specific time windows.
    Format: type:message AND created_datetime:>=2026-01-29 ...
    """
    base_query = "type:message"
    
    if not target_date_str:
        # No date = Full Backfill (Be careful, Search API limits might apply, but this starts from everything)
        return base_query

    # Case 1: Daily -> "2026-01-29"
    if len(target_date_str) == 10:
        return f"{base_query} AND created_datetime:>={target_date_str} AND created_datetime:<{target_date_str}T23:59:59"

    # Case 2: Hourly -> "2026-01-29T14"
    # We construct strict ISO timestamps for that hour
    try:
        dt = datetime.strptime(target_date_str, "%Y-%m-%dT%H")
        start_ts = dt.strftime("%Y-%m-%dT%H:00:00") # Search API prefers simple ISO or YYYY-MM-DD
        end_ts = (dt + timedelta(minutes=59, seconds=59)).strftime("%Y-%m-%dT%H:%M:%S")
        return f"{base_query} AND created_datetime:>={start_ts} AND created_datetime:<={end_ts}"
    except ValueError:
        logger.warning(f"Could not parse target_date: {target_date_str}, falling back to daily assumption")
        return f"{base_query} AND created_datetime:>={target_date_str}"

# -------------------- DDB State --------------------
def _ddb_get(job_start_id: str) -> Optional[Dict[str, Any]]:
    resp = TABLE.get_item(Key={"job_start_id": job_start_id})
    return resp.get("Item")

def _ddb_acquire_lease(job_start_id: str, request_id: str) -> bool:
    now = _utc_now_ts()
    lease_until = now + LEASE_SECONDS
    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            UpdateExpression="SET lease_until=:lu, lease_owner=:lo, in_flight=:t, updated_at=:now",
            ConditionExpression="attribute_not_exists(lease_until) OR lease_until <= :now OR lease_owner = :lo",
            ExpressionAttributeValues={":lu": lease_until, ":lo": request_id, ":t": True, ":now": now},
        )
        return True
    except ClientError as e:
        if e.response["Error"].get("Code") == "ConditionalCheckFailedException":
            return False
        raise

def _ddb_renew_lease(job_start_id: str, request_id: str) -> None:
    now = _utc_now_ts()
    lease_until = now + LEASE_SECONDS
    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            UpdateExpression="SET lease_until=:lu, updated_at=:now",
            ConditionExpression="lease_owner = :lo",
            ExpressionAttributeValues={":lu": lease_until, ":now": now, ":lo": request_id},
        )
    except ClientError:
        pass

def _ddb_checkpoint(job_start_id: str, status: str, page: int, cursor: Optional[str], note: str = "") -> None:
    now = _utc_now_ts()
    expr = "SET #status=:s, #page=:p, #updated_at=:now, in_flight=:f"
    names = {"#status": "status", "#page": "page", "#updated_at": "updated_at"}
    vals = {":s": status, ":p": page, ":now": now, ":f": False}

    if cursor:
        expr += ", #cursor=:c"
        names["#cursor"] = "cursor"
        vals[":c"] = cursor
    if note:
        expr += ", #note=:n"
        names["#note"] = "note"
        vals[":n"] = note[:2000]
    if not cursor:
        expr += " REMOVE #cursor"
        names["#cursor"] = "cursor"

    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals,
    )

def _ddb_done(job_start_id: str, note: str = "") -> None:
    _ddb_checkpoint(job_start_id, "DONE", page=0, cursor=None, note=note)

def _ddb_error(job_start_id: str, page: int, cursor: Optional[str], err: str) -> None:
    # Error state but keep cursor to retry
    now = _utc_now_ts()
    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression="SET #status=:s, last_error=:e, in_flight=:f, updated_at=:now",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":s": "ERROR", ":e": err[:2000], ":f": False, ":now": now}
    )

# -------------------- API Fetcher --------------------
def _request_with_backoff(session: requests.Session, method: str, url: str, params: Dict[str, Any], auth: Tuple[str, str], bearer_key: str) -> requests.Response:
    backoff = BACKOFF_BASE_SECONDS
    last_resp = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.request(method, url, params=params, auth=auth, timeout=REQUEST_TIMEOUT)
            last_resp = r
            
            if r.status_code == 401: # Auth retry
                r = session.request(method, url, params=params, headers={"Authorization": f"Bearer {bearer_key}"}, timeout=REQUEST_TIMEOUT)
                last_resp = r

            if r.status_code == 429: # Rate Limit
                retry_after = r.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else backoff + random.random()
                logger.warning(f"[{STREAM_NAME}] 429. sleeping {sleep_s:.2f}s")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                continue
            
            if r.status_code >= 500: # Server Error
                time.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                continue
            
            if r.status_code >= 400: # Client Error - Crash Immediately
                logger.error(f"[{STREAM_NAME}] 4xx Error: {r.text}")
                r.raise_for_status()

            return r

        except requests.exceptions.RequestException as e:
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)

    raise RuntimeError(f"Exceeded max retries. Status: {last_resp.status_code if last_resp else 'None'}")

def _fetch_page(session, cursor, target_date_str):
    email, api_key = _gorgias_auth()
    url = f"{GORGIAS_BASE_URL}{ENDPOINT}"
    
    # BUILD SEARCH QUERY
    query = _build_search_query(target_date_str)
    
    params = {
        "query": query,
        "limit": PAGE_SIZE,
        "order_by": "created_datetime:desc" # Search supports desc!
    }
    if cursor:
        params["cursor"] = cursor

    logger.info(f"[{STREAM_NAME}] Searching: {query} | Cursor: {'Yes' if cursor else 'No'}")

    r = _request_with_backoff(session, "GET", url, params, (email, api_key), api_key)
    
    j = r.json()
    items = j.get("data") or []
    meta = j.get("meta") or {}
    next_cursor = meta.get("cursor") or meta.get("next_cursor") # Search uses 'cursor' in meta usually

    return items, next_cursor

# -------------------- Main Handler --------------------
def handler(event, context):
    request_id = getattr(context, "aws_request_id", "no_context")
    
    # Parse input
    record = (event.get("Records") or [None])[0] if isinstance(event, dict) else None
    body = {}
    if record and isinstance(record, dict) and record.get("body"):
        try: body = json.loads(record["body"])
        except: body = {"raw_body": record["body"]}
    elif isinstance(event, dict):
        body = event

    job_start_id = body.get("job_start_id")
    target_date_str = body.get("target_date")

    if not job_start_id:
        return {"ok": False, "reason": "missing_job_start_id"}

    # Check State
    state = _ddb_get(job_start_id)
    if not state:
        logger.warning(f"[{STREAM_NAME}] Missing state for {job_start_id}")
        return {"ok": False, "reason": "missing_state"}
    
    if state.get("status") != "RUNNING":
        return {"ok": True, "skipped": True, "status": state.get("status")}

    if not _ddb_acquire_lease(job_start_id, request_id):
        return {"ok": True, "skipped": True, "reason": "lease_busy"}

    cursor = body.get("cursor") or state.get("cursor")
    if cursor == "": cursor = None
    page = int(body.get("page") or state.get("page") or 1)
    
    processed = 0
    total_written = 0
    session = requests.Session()
    
    try:
        while processed < PAGES_PER_INVOCATION and _time_left_ok(context):
            if processed > 0 and processed % LEASE_RENEW_EVERY_PAGES == 0:
                _ddb_renew_lease(job_start_id, request_id)

            try:
                items, next_cursor = _fetch_page(session, cursor, target_date_str)
            except RuntimeError as e:
                _ddb_checkpoint(job_start_id, "RUNNING", page, cursor, note=f"paused: {e}")
                return {"ok": True, "done": False, "reason": "transient_error"}

            if items:
                _write_jsonl_to_s3(STREAM_NAME, job_start_id, page, items)
                total_written += len(items)

            processed += 1
            cursor = next_cursor
            page += 1

            if not cursor:
                _ddb_done(job_start_id, f"completed (wrote {total_written} items)")
                return {"ok": True, "done": True}

            if processed % CHECKPOINT_EVERY_PAGES == 0:
                _ddb_checkpoint(job_start_id, "RUNNING", page, cursor, note=f"page {page}")

        _ddb_checkpoint(job_start_id, "RUNNING", page, cursor, note=f"paused at page {page}")
        return {"ok": True, "done": False}

    except Exception as e:
        logger.exception(f"[{STREAM_NAME}] Fatal Error")
        _ddb_error(job_start_id, page, cursor, str(e))
        raise