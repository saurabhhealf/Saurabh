import os
import json
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------- AWS clients ----------
_ddb = boto3.resource("dynamodb")
_s3 = boto3.client("s3")
_sm = boto3.client("secretsmanager")

# ---------- Env ----------
GORGIAS_BASE_URL = os.environ.get("GORGIAS_BASE_URL", "https://healf-uk.gorgias.com/api").rstrip("/")
STATE_TABLE = os.environ["STATE_TABLE"]
S3_BUCKET = os.environ.get("S3_BUCKET", "sources-data")
S3_PREFIX_BASE = os.environ.get("S3_PREFIX_BASE", "gorgias").strip("/")

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))
# Use 100 to process more data per run (~10k records)
PAGES_PER_INVOCATION = int(os.environ.get("PAGES_PER_INVOCATION", "100"))
REQUEST_TIMEOUT = (10, 60)

GORGIAS_EMAIL_SECRET = os.environ.get("GORGIAS_EMAIL_SECRET", "gorgias_email")
GORGIAS_API_KEY_SECRET = os.environ.get("GORGIAS_API_KEY_SECRET", "gorgias_api_key")

STREAM_NAME = "messages"
ENDPOINT = "/messages"

TABLE = _ddb.Table(STATE_TABLE)

# ---------- Helpers ----------
def _utc_now_ts() -> int: return int(time.time())
def _safe_json_loads(s: str) -> Any:
    try: return json.loads(s)
    except: return None

def _get_secret_value(secret_id: str) -> str:
    resp = _sm.get_secret_value(SecretId=secret_id)
    secret = (resp.get("SecretString") or "").strip()
    parsed = _safe_json_loads(secret)
    if isinstance(parsed, dict):
        for k in ["GORGIAS_EMAIL", "GORGIAS_API_KEY", "value", "secret", "token", "api_key", "apiKey", "key", "password", "email", "username"]:
            if k in parsed and isinstance(parsed[k], str) and parsed[k].strip():
                return parsed[k].strip().strip('"').strip("'")
    return secret.strip().strip('"').strip("'")

def _gorgias_auth() -> Tuple[str, str]:
    email = _get_secret_value(GORGIAS_EMAIL_SECRET)
    api_key = _get_secret_value(GORGIAS_API_KEY_SECRET)
    if not email or not api_key: raise ValueError("Missing Gorgias credentials")
    return email, api_key

def _time_left_ok(context, buffer_ms: int = 12_000) -> bool:
    if context is None: return True
    try: return context.get_remaining_time_in_millis() > buffer_ms
    except: return True

def _maybe_adaptive_sleep(headers: Dict[str, str]) -> None:
    limit_hdr = headers.get("X-Gorgias-Account-Api-Call-Limit") or headers.get("x-gorgias-account-api-call-limit")
    if not limit_hdr: return
    try:
        used_s, limit_s = limit_hdr.split("/", 1)
        if int(limit_s.strip()) > 0 and int(used_s.strip()) / int(limit_s.strip()) >= 0.9:
            time.sleep(2.0)
    except: pass

def make_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=0)
    session.mount("https://", adapter)
    return session

def safe_get(session: requests.Session, path: str, params: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    url = f"{GORGIAS_BASE_URL}{path}"
    email, api_key = _gorgias_auth()
    auth = (email, api_key)

    while True:
        r = session.get(url, params=params, auth=auth, timeout=REQUEST_TIMEOUT)

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 10))
            logger.warning(f"[{STREAM_NAME}] 429 rate limited. sleeping {retry_after}s")
            time.sleep(retry_after + 1)
            continue

        if r.status_code >= 400:
            logger.error(f"[{STREAM_NAME}] ERROR url={r.url} status={r.status_code} body={r.text[:1200]}")
        r.raise_for_status()

        headers = {k: v for k, v in r.headers.items()}
        _maybe_adaptive_sleep(headers)
        return r.json(), headers

def _write_jsonl_to_s3(stream: str, job_start_id: str, page: int, records: List[Dict[str, Any]]) -> str:
    if not records: return ""
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # Deterministic naming (page=000001.json)
    key = f"{S3_PREFIX_BASE}/{stream}/dt={dt}/job={job_start_id}/page={page:06d}.json"
    body = "".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n" for r in records)
    _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
    logger.info(f"[{stream}] wrote s3://{S3_BUCKET}/{key} rows={len(records)}")
    return key

# ---------- DDB Helpers (FIXED: UNCONDITIONAL UPDATE) ----------
def _ddb_get(job_start_id: str) -> Optional[Dict[str, Any]]: 
    return TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")

def _ddb_update_running(job_start_id: str, next_page: int, next_cursor: Optional[str], note: str = "") -> None:
    now = _utc_now_ts()
    # Unconditional update to fix the loop
    expr = "SET #status=:running, #in_flight=:false, #lease_until=:zero, #page=:p, #updated_at=:now"
    names = {"#status": "status", "#in_flight": "in_flight", "#lease_until": "lease_until", "#page": "page", "#updated_at": "updated_at"}
    vals = {":running": "RUNNING", ":false": False, ":zero": 0, ":p": next_page, ":now": now}
    if next_cursor:
        expr += ", #cursor=:c"; names["#cursor"] = "cursor"; vals[":c"] = next_cursor
    else:
        expr += " REMOVE #cursor"; names["#cursor"] = "cursor"
    if note: expr += ", #note=:n"; names["#note"] = "note"; vals[":n"] = note[:2000]
    
    TABLE.update_item(Key={"job_start_id": job_start_id}, UpdateExpression=expr, ExpressionAttributeNames=names, ExpressionAttributeValues=vals)

def _ddb_done(job_start_id: str, note: str = "") -> None:
    now = _utc_now_ts()
    expr = "SET #status=:done, #in_flight=:false, #lease_until=:zero, #updated_at=:now"
    names = {"#status": "status", "#in_flight": "in_flight", "#lease_until": "lease_until", "#updated_at": "updated_at"}
    vals = {":done": "DONE", ":false": False, ":zero": 0, ":now": now}
    if note: expr += ", #note=:n"; names["#note"] = "note"; vals[":n"] = note[:2000]
    TABLE.update_item(Key={"job_start_id": job_start_id}, UpdateExpression=expr, ExpressionAttributeNames=names, ExpressionAttributeValues=vals)

def _ddb_error(job_start_id: str, err: str) -> None:
    now = _utc_now_ts()
    TABLE.update_item(Key={"job_start_id": job_start_id}, UpdateExpression="SET #status=:err, #in_flight=:false, #lease_until=:zero, #updated_at=:now, #last_error=:e", ExpressionAttributeNames={"#status": "status", "#in_flight": "in_flight", "#lease_until": "lease_until", "#updated_at": "updated_at", "#last_error": "last_error"}, ExpressionAttributeValues={":err": "ERROR", ":false": False, ":zero": 0, ":now": now, ":e": (err or "")[:2000]})

# ---------- Handler ----------
def _release_lease(job_start_id: str) -> None:
    # Helper for finally block
    now = int(time.time())
    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression="SET #in_flight = :false, #lease_until = :zero, #updated_at = :now",
        ExpressionAttributeNames={"#in_flight": "in_flight", "#lease_until": "lease_until", "#updated_at": "updated_at"},
        ExpressionAttributeValues={":false": False, ":zero": 0, ":now": now}
    )

def handler(event, context):
    try:
        record = (event.get("Records") or [None])[0]
        body = json.loads(record["body"]) if record and record.get("body") else (event or {})
        job_start_id = body.get("job_start_id")
        if not job_start_id: raise ValueError("Missing job_start_id")

        state = _ddb_get(job_start_id)
        if not state: return {"ok": False, "reason": "missing_state"}
        if state.get("status") != "RUNNING": return {"ok": True, "skipped": True}

        # Page for filename only
        page = int(body.get("page") or state.get("page") or 1)
        cursor = body.get("cursor") or state.get("cursor")
        if cursor == "": cursor = None

        processed = 0
        session = make_session()

        while processed < PAGES_PER_INVOCATION and _time_left_ok(context):
            # 2. Fetch Params (Cursor Only - No 'page' or 'order_by' for messages)
            params = {"limit": PAGE_SIZE}
            if cursor: params["cursor"] = cursor

            payload, headers = safe_get(session, ENDPOINT, params)
            
            # Robust parsing for list
            items = []
            if isinstance(payload, dict):
                for k in ["data", "items", "results", STREAM_NAME]:
                    if k in payload and isinstance(payload[k], list):
                        items = payload[k]; break
            
            if not items:
                _ddb_done(job_start_id, "no items")
                return {"ok": True, "done": True}
                
            _write_jsonl_to_s3(STREAM_NAME, job_start_id, page, items)
            processed += 1
            
            # 3. Stop Conditions
            meta = payload.get("meta", {}) if isinstance(payload, dict) else {}
            next_cursor = meta.get("next_cursor") or meta.get("nextCursor")

            if not next_cursor:
                _ddb_done(job_start_id, "completed")
                return {"ok": True, "done": True}
                
            cursor = next_cursor
            page += 1

        _ddb_update_running(job_start_id, page, cursor, f"processed {processed}")
        return {"ok": True, "done": False}

    except Exception as e:
        logger.exception(f"[{STREAM_NAME}] error")
        try:
            if 'job_start_id' in locals(): _ddb_error(job_start_id, str(e))
        except: pass
        raise
    finally:
        # Safety: if we never wrote a page (so no progress/done update released the lease),
        # release it so orchestrator can retry quickly.
        if 'processed' in locals() and processed == 0 and 'job_start_id' in locals():
            _release_lease(job_start_id)