"""
tickets.py — Gorgias /tickets extractor (Backfill + Daily) → S3 JSONL

Goals:
- Supports Backfill (fetch all) AND Daily (incremental window).
- Robust Rate Limiting: Backoff + Jitter on 429s.
- Concurrency Safety: Uses DDB Leases to prevent overlapping runs.
- Checkpointing: Resumes exactly where it left off.
"""

import os
import json
import time
import random
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

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

STREAM_NAME = "tickets"
ENDPOINT = "/tickets"

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))
PAGES_PER_INVOCATION = int(os.environ.get("PAGES_PER_INVOCATION", "50"))

# Rate limit + retries
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "8"))
BACKOFF_BASE_SECONDS = float(os.environ.get("BACKOFF_BASE_SECONDS", "1.0"))
BACKOFF_MAX_SECONDS = float(os.environ.get("BACKOFF_MAX_SECONDS", "30.0"))
MIN_SUCCESS_DELAY_SECONDS = float(os.environ.get("MIN_SUCCESS_DELAY_SECONDS", "0.2"))
THROTTLE_RATIO = float(os.environ.get("THROTTLE_RATIO", "0.70"))
THROTTLE_MAX_SLEEP_SECONDS = float(os.environ.get("THROTTLE_MAX_SLEEP_SECONDS", "2.0"))

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

def _parse_limit_header(headers: Dict[str, str]) -> Optional[Tuple[int, int]]:
    h = headers.get("X-Gorgias-Account-Api-Call-Limit") or headers.get("x-gorgias-account-api-call-limit")
    if not h:
        return None
    try:
        used_s, limit_s = h.split("/", 1)
        used = int(used_s.strip())
        limit = int(limit_s.strip())
        if limit <= 0:
            return None
        return used, limit
    except Exception:
        return None

def _throttle_on_success(headers: Dict[str, str]) -> None:
    parsed = _parse_limit_header(headers)
    sleep_s = MIN_SUCCESS_DELAY_SECONDS if MIN_SUCCESS_DELAY_SECONDS > 0 else 0.0
    if parsed:
        used, limit = parsed
        ratio = used / limit
        if ratio >= THROTTLE_RATIO:
            ramp = (ratio - THROTTLE_RATIO) / max(1e-9, (1.0 - THROTTLE_RATIO))
            sleep_s = max(sleep_s, min(THROTTLE_MAX_SLEEP_SECONDS, 0.2 + ramp * THROTTLE_MAX_SLEEP_SECONDS))
    if sleep_s > 0:
        time.sleep(sleep_s)

def _write_jsonl_to_s3(stream: str, job_start_id: str, page: int, records: List[Dict[str, Any]]) -> str:
    if not records:
        return ""
    dt = _utc_today_str()
    key = f"{S3_PREFIX_BASE}/{stream}/dt={dt}/job={job_start_id}/page={page:06d}.json"
    body = "".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n" for r in records)
    _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
    logger.info(f"[{stream}] wrote s3://{S3_BUCKET}/{key} rows={len(records)}")
    return key

# -------------------- DDB State / Lease --------------------
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
            ExpressionAttributeValues={
                ":lu": lease_until,
                ":lo": request_id,
                ":t": True,
                ":now": now,
            },
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
    except ClientError as e:
        if e.response["Error"].get("Code") == "ConditionalCheckFailedException":
            return
        raise

def _ddb_checkpoint(job_start_id: str, status: str, page: int, cursor: Optional[str], note: str = "", last_error: str = "") -> None:
    now = _utc_now_ts()
    expr = "SET #status=:s, #page=:p, #updated_at=:now, in_flight=:f"
    names = {"#status": "status", "#page": "page", "#updated_at": "updated_at"}
    vals = {":s": status, ":p": page, ":now": now, ":f": False}

    if cursor:
        expr += ", #cursor=:c"
        names["#cursor"] = "cursor"
        vals[":c"] = cursor
    else:
        expr += " REMOVE #cursor"
        names["#cursor"] = "cursor"

    if note:
        expr += ", #note=:n"
        names["#note"] = "note"
        vals[":n"] = note[:2000]

    if last_error:
        expr += ", #last_error=:e"
        names["#last_error"] = "last_error"
        vals[":e"] = last_error[:2000]

    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals,
    )

def _ddb_done(job_start_id: str, note: str = "") -> None:
    _ddb_checkpoint(job_start_id, "DONE", page=0, cursor=None, note=note, last_error="")

def _ddb_error(job_start_id: str, page: int, cursor: Optional[str], err: str) -> None:
    _ddb_checkpoint(job_start_id, "ERROR", page=page, cursor=cursor, note="error", last_error=err)

# -------------------- API Fetcher --------------------
def _request_with_backoff(session: requests.Session, method: str, url: str, params: Dict[str, Any], auth: Tuple[str, str], bearer_key: str) -> requests.Response:
    backoff = BACKOFF_BASE_SECONDS
    last_resp = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.request(method, url, params=params, auth=auth, timeout=REQUEST_TIMEOUT)
            last_resp = r
            
            if r.status_code == 401:
                r = session.request(method, url, params=params, headers={"Authorization": f"Bearer {bearer_key}"}, timeout=REQUEST_TIMEOUT)
                last_resp = r

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except:
                        sleep_s = backoff
                else:
                    sleep_s = backoff + random.random()
                
                logger.warning(f"[{STREAM_NAME}] 429 rate limited. attempt={attempt}. sleeping {sleep_s:.2f}s")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                continue
            
            if r.status_code >= 500:
                sleep_s = backoff + random.random()
                logger.warning(f"[{STREAM_NAME}] {r.status_code} server error. attempt={attempt}. sleeping {sleep_s:.2f}s")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                continue
                
            return r
        except requests.exceptions.RequestException as e:
            logger.warning(f"[{STREAM_NAME}] Network error: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)

    raise RuntimeError(f"Exceeded max retries. Last status: {last_resp.status_code if last_resp else 'None'}")

def _fetch_page(session: requests.Session, cursor: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str], Dict[str, str]]:
    email, api_key = _gorgias_auth()
    url = f"{GORGIAS_BASE_URL}{ENDPOINT}"
    
    # Gorgias Tickets Sort Order
    params = {
        "limit": PAGE_SIZE,
        "order_by": "updated_datetime:desc"
    }
    if cursor:
        params["cursor"] = cursor

    r = _request_with_backoff(session, "GET", url, params, (email, api_key), api_key)
    
    if r.status_code >= 400:
        logger.error(f"[{STREAM_NAME}] HTTP {r.status_code} body={r.text[:500]}")
        r.raise_for_status()

    headers = dict(r.headers)
    _throttle_on_success(headers)
    
    j = r.json()
    items = []
    if isinstance(j, dict):
        for k in ["data", "items", "results", "tickets"]:
            if k in j and isinstance(j[k], list):
                items = j[k]
                break
    
    next_cursor = None
    if isinstance(j, dict):
        meta = j.get("meta") or {}
        next_cursor = meta.get("next_cursor") or meta.get("nextCursor")

    return items, next_cursor, headers

# -------------------- Main Handler --------------------
def handler(event, context):
    request_id = getattr(context, "aws_request_id", "no_context")
    
    # Parse input
    record = (event.get("Records") or [None])[0] if isinstance(event, dict) else None
    body = {}
    if record and isinstance(record, dict) and record.get("body"):
        try:
            body = json.loads(record["body"])
        except:
            body = {"raw_body": record["body"]}
    elif isinstance(event, dict):
        body = event

    job_start_id = body.get("job_start_id")
    if not job_start_id:
        logger.error(f"[{STREAM_NAME}] Missing job_start_id. event={json.dumps(event)[:1000]}")
        return {"ok": False, "reason": "missing_job_start_id"}

    # --- Date Window Logic ---
    target_date_str = body.get("target_date")
    now_dt = datetime.now(timezone.utc)
    
    # If no date provided (Backfill), or explicit date provided (Daily)
    # Daily logic: default lookback 2 days if not specified but implies daily context
    is_daily = target_date_str is not None
    
    if is_daily:
        try:
            start_of_window = datetime.strptime(target_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"Invalid target_date format: {target_date_str}")
            return {"ok": False, "reason": "invalid_date"}
    else:
        start_of_window = None

    # Check State
    state = _ddb_get(job_start_id)
    if not state:
        logger.warning(f"[{STREAM_NAME}] Missing state for {job_start_id}")
        return {"ok": False, "reason": "missing_state"}
    
    if state.get("status") != "RUNNING":
        return {"ok": True, "skipped": True, "status": state.get("status")}

    # Acquire Lease
    if not _ddb_acquire_lease(job_start_id, request_id):
        logger.info(f"[{STREAM_NAME}] Lease busy for {job_start_id}")
        return {"ok": True, "skipped": True, "reason": "lease_busy"}

    # Prepare for Loop
    cursor = body.get("cursor") or state.get("cursor")
    if cursor == "": cursor = None
    page = int(body.get("page") or state.get("page") or 1)
    
    processed = 0
    session = requests.Session()
    
    try:
        while processed < PAGES_PER_INVOCATION and _time_left_ok(context):
            # Renew lease periodically
            if processed > 0 and processed % LEASE_RENEW_EVERY_PAGES == 0:
                _ddb_renew_lease(job_start_id, request_id)

            try:
                items, next_cursor, headers = _fetch_page(session, cursor)
            except RuntimeError as e:
                msg = str(e)
                logger.warning(f"[{STREAM_NAME}] Transient failure: {msg}")
                _ddb_checkpoint(job_start_id, "RUNNING", page, cursor, note=f"paused: {msg}")
                return {"ok": True, "done": False, "reason": "transient_error"}

            if not items:
                _ddb_done(job_start_id, "completed (no items)")
                return {"ok": True, "done": True}

            # Filter items by date window (Daily only)
            valid_items = []
            reached_end_of_window = False
            
            if is_daily:
                for item in items:
                    upd = item.get("updated_datetime")
                    if not upd: continue
                    try:
                        upd_dt = datetime.fromisoformat(upd.replace("Z", "+00:00"))
                    except:
                        continue
                    
                    if upd_dt >= start_of_window:
                        valid_items.append(item)
                    else:
                        reached_end_of_window = True
                        break
            else:
                # Backfill: keep all items
                valid_items = items
            
            # Write valid items
            if valid_items:
                _write_jsonl_to_s3(STREAM_NAME, job_start_id, page, valid_items)

            processed += 1
            cursor = next_cursor
            page += 1

            # Stop condition 1: Data is too old (Daily only)
            if is_daily and reached_end_of_window:
                logger.info(f"[{STREAM_NAME}] Reached data older than {target_date_str}. Done.")
                _ddb_done(job_start_id, f"completed window {target_date_str}")
                return {"ok": True, "done": True}

            # Stop condition 2: No more pages from API
            if not cursor:
                _ddb_done(job_start_id, "completed (end of stream)")
                return {"ok": True, "done": True}

            # Checkpoint
            if processed % CHECKPOINT_EVERY_PAGES == 0:
                _ddb_checkpoint(job_start_id, "RUNNING", page, cursor, note=f"processing page {page}")

        # Loop finished (time/page limit) -> Checkpoint and pause
        _ddb_checkpoint(job_start_id, "RUNNING", page, cursor, note=f"paused at page {page}")
        return {"ok": True, "done": False}

    except Exception as e:
        logger.exception(f"[{STREAM_NAME}] Fatal Error")
        _ddb_error(job_start_id, page, cursor, str(e))
        raise