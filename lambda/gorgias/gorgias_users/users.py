import os
import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests

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

STREAM_NAME = "users"
ENDPOINT = "/users"

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))
PAGES_PER_INVOCATION = int(os.environ.get("PAGES_PER_INVOCATION", "50"))

# Rate limit + retries
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
BACKOFF_BASE_SECONDS = 1.0
REQUEST_TIMEOUT = (10, 60)
TIME_LEFT_BUFFER_MS = 12_000

# Secrets
GORGIAS_EMAIL_SECRET = os.environ.get("GORGIAS_EMAIL_SECRET", "gorgias_email")
GORGIAS_API_KEY_SECRET = os.environ.get("GORGIAS_API_KEY_SECRET", "gorgias_api_key")

TABLE = _ddb.Table(STATE_TABLE)

# -------------------- Helpers --------------------
def _utc_now_ts() -> int:
    return int(time.time())

def _safe_json_loads(s: str) -> Any:
    try: return json.loads(s)
    except: return None

def _get_secret_value(secret_id: str) -> str:
    resp = _sm.get_secret_value(SecretId=secret_id)
    secret = (resp.get("SecretString") or "").strip()
    parsed = _safe_json_loads(secret)
    if isinstance(parsed, dict):
        for k in ["value", "secret", "token", "api_key", "apiKey", "password", "email"]:
            if k in parsed and isinstance(parsed[k], str) and parsed[k].strip():
                return parsed[k].strip().strip('"').strip("'")
    return secret.strip().strip('"').strip("'")

def _gorgias_auth() -> Tuple[str, str]:
    email = _get_secret_value(GORGIAS_EMAIL_SECRET)
    api_key = _get_secret_value(GORGIAS_API_KEY_SECRET)
    if not email or not api_key: raise ValueError("Missing Gorgias credentials")
    return email, api_key

def _time_left_ok(context) -> bool:
    if context is None: return True
    return context.get_remaining_time_in_millis() > TIME_LEFT_BUFFER_MS

def _write_jsonl_to_s3(stream: str, job_start_id: str, page: int, records: List[Dict[str, Any]]) -> str:
    if not records: return ""
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{S3_PREFIX_BASE}/{stream}/dt={dt}/job={job_start_id}/page={page:06d}-{int(time.time())}-{uuid.uuid4().hex}.json"
    body = "".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n" for r in records)
    _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
    logger.info(f"[{stream}] wrote s3://{S3_BUCKET}/{key} rows={len(records)}")
    return key

# -------------------- DDB State --------------------
def _ddb_get(job_start_id: str) -> Optional[Dict[str, Any]]:
    return TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")

def _ddb_update_running(job_start_id: str, next_page: int, next_cursor: Optional[str], note: str = "") -> None:
    now = _utc_now_ts()
    expr = "SET #status=:r, #in_flight=:f, #lease_until=:z, #page=:p, #updated_at=:n"
    names = {"#status": "status", "#in_flight": "in_flight", "#lease_until": "lease_until", "#page": "page", "#updated_at": "updated_at"}
    vals = {":r": "RUNNING", ":f": False, ":z": 0, ":p": next_page, ":n": now}
    
    if next_cursor:
        expr += ", #cursor=:c"
        names["#cursor"] = "cursor"
        vals[":c"] = next_cursor
    else:
        expr += " REMOVE #cursor"
        names["#cursor"] = "cursor"

    if note:
        expr += ", #note=:nt"
        names["#note"] = "note"
        vals[":nt"] = note[:2000]

    TABLE.update_item(Key={"job_start_id": job_start_id}, UpdateExpression=expr, ExpressionAttributeNames=names, ExpressionAttributeValues=vals)

def _ddb_done(job_start_id: str, note: str = "") -> None:
    now = _utc_now_ts()
    expr = "SET #status=:d, #in_flight=:f, #lease_until=:z, #updated_at=:n"
    names = {"#status": "status", "#in_flight": "in_flight", "#lease_until": "lease_until", "#updated_at": "updated_at"}
    vals = {":d": "DONE", ":f": False, ":z": 0, ":n": now}
    if note:
        expr += ", #note=:nt"
        names["#note"] = "note"
        vals[":nt"] = note[:2000]
    TABLE.update_item(Key={"job_start_id": job_start_id}, UpdateExpression=expr, ExpressionAttributeNames=names, ExpressionAttributeValues=vals)

def _ddb_error(job_start_id: str, err: str) -> None:
    now = _utc_now_ts()
    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression="SET #status=:e, #in_flight=:f, #lease_until=:z, #updated_at=:n, #last_error=:err",
        ExpressionAttributeNames={"#status": "status", "#in_flight": "in_flight", "#lease_until": "lease_until", "#updated_at": "updated_at", "#last_error": "last_error"},
        ExpressionAttributeValues={":e": "ERROR", ":f": False, ":z": 0, ":n": now, ":err": str(err)[:2000]}
    )

# -------------------- API Logic --------------------
def _fallback_fetch_me(session: requests.Session) -> List[Dict[str, Any]]:
    """Fetch only the current user if full list is denied."""
    logger.info("Access denied for full list. Fetching '/api/me' instead.")
    url = f"{GORGIAS_BASE_URL}/me"
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    if r.status_code == 200:
        return [r.json()]
    logger.warning(f"Could not fetch /api/me either: {r.status_code}")
    return []

def handler(event, context):
    try:
        record = (event.get("Records") or [None])[0]
        body = json.loads(record["body"]) if record and record.get("body") else (event or {})
        job_start_id = body.get("job_start_id")
        if not job_start_id: raise ValueError("Missing job_start_id")

        state = _ddb_get(job_start_id)
        if not state:
            logger.warning(f"[{STREAM_NAME}] missing ddb state job={job_start_id}")
            return {"ok": False, "reason": "missing_state"}

        if state.get("status") != "RUNNING":
            logger.info(f"[{STREAM_NAME}] job not RUNNING (status={state.get('status')}); skipping")
            return {"ok": True, "skipped": True}

        page = int(body.get("page") or state.get("page") or 1)
        cursor = body.get("cursor") or state.get("cursor")
        
        email, api_key = _gorgias_auth()
        session = requests.Session()
        session.auth = (email, api_key)

        processed_pages = 0
        total_rows = 0

        while processed_pages < PAGES_PER_INVOCATION and _time_left_ok(context):
            url = f"{GORGIAS_BASE_URL}{ENDPOINT}"
            params = {"limit": PAGE_SIZE, "order_by": "created_datetime", "direction": "asc", "page": page}
            if cursor: params["cursor"] = cursor

            try:
                r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                
                # --- SAFE MODE: HANDLE 401 WITHOUT CRASHING ---
                if r.status_code == 401:
                    logger.warning(f"401 Unauthorized for {url}. Attempting fallback to /me.")
                    
                    # If we are on page 1, we can at least save the 'me' user
                    if page == 1:
                        items = _fallback_fetch_me(session)
                        if items:
                            _write_jsonl_to_s3(STREAM_NAME, job_start_id, page, items)
                            total_rows += len(items)
                    
                    # Mark job as DONE so it doesn't retry forever
                    _ddb_done(job_start_id, note="completed (fallback mode - 401 access restricted)")
                    return {"ok": True, "done": True, "rows": total_rows, "fallback": True}

                r.raise_for_status()
                data = r.json()
                items = data.get("data") or data.get("users") or []
                
                if not items and isinstance(data, list): items = data
                
            except Exception as e:
                # If it's a hard network error, we might want to fail, 
                # but if we already handled 401 above, this catches other things.
                logger.error(f"Request failed: {e}")
                raise e

            if not items:
                _ddb_done(job_start_id, note=f"completed page={page}")
                return {"ok": True, "done": True, "rows": total_rows}

            _write_jsonl_to_s3(STREAM_NAME, job_start_id, page, items)
            processed_pages += 1
            total_rows += len(items)

            # Pagination Logic
            meta = data.get("meta") or {}
            next_cursor = meta.get("next_cursor") or meta.get("cursor_next")
            
            if next_cursor:
                cursor = next_cursor
                page += 1
            else:
                if len(items) < PAGE_SIZE:
                    _ddb_done(job_start_id, note="completed")
                    return {"ok": True, "done": True, "rows": total_rows}
                page += 1

            _ddb_update_running(job_start_id, page, cursor, f"continuing rows={total_rows}")

        return {"ok": True, "done": False, "rows": total_rows, "next_page": page}

    except Exception as e:
        logger.exception("Fatal Error")
        try:
            if 'job_start_id' in locals() and job_start_id: _ddb_error(job_start_id, str(e))
        except: pass
        raise