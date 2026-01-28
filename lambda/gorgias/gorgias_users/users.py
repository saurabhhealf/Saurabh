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

STREAM_NAME = "users"
ENDPOINT = "/users"

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
TIME_LEFT_BUFFER_MS = 12_000

# Secrets
GORGIAS_EMAIL_SECRET = os.environ.get("GORGIAS_EMAIL_SECRET", "gorgias_email")
GORGIAS_API_KEY_SECRET = os.environ.get("GORGIAS_API_KEY_SECRET", "gorgias_api_key")

TABLE = _ddb.Table(STATE_TABLE)


# -------------------- Helpers --------------------
def _utc_now_ts() -> int:
    return int(time.time())

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
        for k in ["value", "secret", "token", "api_key", "apiKey", "key", "password", "email", "username"]:
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
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = (
        f"{S3_PREFIX_BASE}/{stream}/dt={dt}/job={job_start_id}/"
        f"page={page:06d}-{int(time.time())}-{uuid.uuid4().hex}.json"
    )
    body = "".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n" for r in records)
    _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
    logger.info(f"[{stream}] wrote s3://{S3_BUCKET}/{key} rows={len(records)}")
    return key

# -------------------- DDB State --------------------
def _ddb_get(job_start_id: str) -> Optional[Dict[str, Any]]:
    return TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")

def _ddb_update_running(job_start_id: str, next_page: int, next_cursor: Optional[str], note: str = "") -> None:
    now = _utc_now_ts()
    # 1. Build SET clause
    expr = "SET #status=:running, #in_flight=:false, #lease_until=:zero, #page=:p, #updated_at=:now"
    names = {
        "#status": "status",
        "#in_flight": "in_flight",
        "#lease_until": "lease_until",
        "#page": "page",
        "#updated_at": "updated_at",
    }
    vals: Dict[str, Any] = {
        ":running": "RUNNING",
        ":false": False,
        ":zero": 0,
        ":p": next_page,
        ":now": now,
    }

    if next_cursor:
        expr += ", #cursor=:c"
        names["#cursor"] = "cursor"
        vals[":c"] = next_cursor
    
    if note:
        expr += ", #note=:n"
        names["#note"] = "note"
        vals[":n"] = note[:2000]

    # 2. Append REMOVE clause LAST
    if not next_cursor:
        expr += " REMOVE #cursor"
        names["#cursor"] = "cursor"

    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals,
    )

def _ddb_done(job_start_id: str, note: str = "") -> None:
    now = _utc_now_ts()
    expr = "SET #status=:done, #in_flight=:false, #lease_until=:zero, #updated_at=:now"
    names = {
        "#status": "status",
        "#in_flight": "in_flight",
        "#lease_until": "lease_until",
        "#updated_at": "updated_at",
    }
    vals: Dict[str, Any] = {":done": "DONE", ":false": False, ":zero": 0, ":now": now}
    
    if note:
        expr += ", #note=:n"
        names["#note"] = "note"
        vals[":n"] = note[:2000]
        
    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals,
    )

def _ddb_error(job_start_id: str, err: str) -> None:
    now = _utc_now_ts()
    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression="SET #status=:err, #in_flight=:false, #lease_until=:zero, #updated_at=:now, #last_error=:e",
        ExpressionAttributeNames={
            "#status": "status",
            "#in_flight": "in_flight",
            "#lease_until": "lease_until",
            "#updated_at": "updated_at",
            "#last_error": "last_error",
        },
        ExpressionAttributeValues={
            ":err": "ERROR",
            ":false": False,
            ":zero": 0,
            ":now": now,
            ":e": (err or "")[:2000],
        },
    )

# -------------------- API Fetcher --------------------
def _request_with_backoff(session: requests.Session, method: str, url: str, params: Dict[str, Any], auth: Tuple[str, str], bearer_key: str) -> requests.Response:
    backoff = BACKOFF_BASE_SECONDS
    last_resp = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.request(method, url, params=params, auth=auth, timeout=REQUEST_TIMEOUT)
            last_resp = r
            
            # 401 fallback
            if r.status_code == 401:
                # DEBUG LOG
                logger.info(f"[DEBUG] 401 on attempt {attempt}. Retrying with Bearer token.")
                r = session.request(method, url, params=params, headers={"Authorization": f"Bearer {bearer_key}"}, timeout=REQUEST_TIMEOUT)
                last_resp = r

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                sleep_s = backoff
                if retry_after:
                    try: sleep_s = float(retry_after)
                    except: pass
                else:
                    sleep_s = backoff + random.random()
                
                logger.warning(f"[{STREAM_NAME}] 429 rate limited. sleeping {sleep_s:.2f}s")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                continue
            
            if r.status_code >= 500:
                sleep_s = backoff + random.random()
                logger.warning(f"[{STREAM_NAME}] {r.status_code} server error. sleeping {sleep_s:.2f}s")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                continue
                
            return r
        except requests.exceptions.RequestException as e:
            logger.warning(f"[{STREAM_NAME}] Network error: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)

    if last_resp:
        last_resp.raise_for_status()
    raise RuntimeError(f"Exceeded max retries for {url}")

def _parse_items(resp_json: Any) -> List[Dict[str, Any]]:
    if isinstance(resp_json, dict):
        # DEBUG LOG: See keys
        logger.info(f"[DEBUG] Response Keys: {list(resp_json.keys())}")
        
        for k in ["data", "items", "results", STREAM_NAME]:
            v = resp_json.get(k)
            if isinstance(v, list):
                return v
        for v in resp_json.values():
            if isinstance(v, list):
                return v
    if isinstance(resp_json, list):
        logger.info(f"[DEBUG] Response is a LIST of length {len(resp_json)}")
        return resp_json
        
    logger.warning(f"[DEBUG] Could not parse items. Body: {str(resp_json)[:200]}")
    return []

def _parse_next_cursor(resp_json: Any) -> Optional[str]:
    if isinstance(resp_json, dict):
        meta = resp_json.get("meta")
        if isinstance(meta, dict):
            for k in ["next_cursor", "nextCursor", "cursor_next", "cursorNext"]:
                v = meta.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
    return None

def _fetch_page(session: requests.Session, page: int, cursor: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    email, api_key = _gorgias_auth()
    url = f"{GORGIAS_BASE_URL}{ENDPOINT}"

    params: Dict[str, Any] = {
        "limit": PAGE_SIZE,
        "per_page": PAGE_SIZE,
        "page": page,
        "order_by": "created_datetime",
        "order_direction": "asc",
        "sort": "created_datetime",
        "direction": "asc",
    }
    if cursor:
        params["cursor"] = cursor

    logger.info(f"[DEBUG] Fetching URL: {url} Params: {params}")

    r = _request_with_backoff(session, "GET", url, params, (email, api_key), api_key)
    
    headers = dict(r.headers)
    _throttle_on_success(headers)

    j = r.json()
    items = _parse_items(j)
    logger.info(f"[DEBUG] Fetched {len(items)} items.")
    return items, _parse_next_cursor(j)

# -------------------- Main Handler --------------------
def handler(event, context):
    try:
        record = (event.get("Records") or [None])[0]
        body = json.loads(record["body"]) if record and record.get("body") else (event or {})
        job_start_id = body.get("job_start_id")
        if not job_start_id:
            raise ValueError("Missing job_start_id")

        state = _ddb_get(job_start_id)
        if not state:
            logger.warning(f"[{STREAM_NAME}] missing ddb state job={job_start_id}")
            return {"ok": False, "reason": "missing_state"}

        if state.get("status") != "RUNNING":
            logger.info(f"[{STREAM_NAME}] job not RUNNING (status={state.get('status')}); skipping")
            return {"ok": True, "skipped": True, "status": state.get("status")}

        page = int(body.get("page") or state.get("page") or 1)
        cursor = body.get("cursor") or state.get("cursor")

        processed_pages = 0
        total_rows = 0
        session = requests.Session()

        while processed_pages < PAGES_PER_INVOCATION and _time_left_ok(context):
            items, next_cursor = _fetch_page(session, page=page, cursor=cursor)

            if not items:
                logger.info(f"[DEBUG] No items found. Marking DONE. Page={page} Cursor={cursor}")
                _ddb_done(job_start_id, note=f"no_items page={page} cursor_present={bool(cursor)}")
                return {"ok": True, "done": True, "rows": total_rows}

            _write_jsonl_to_s3(STREAM_NAME, job_start_id, page, items)

            processed_pages += 1
            total_rows += len(items)

            if next_cursor:
                cursor = next_cursor
                page += 1
            else:
                if len(items) < PAGE_SIZE:
                    _ddb_done(job_start_id, note=f"completed page={page} (short page)")
                    return {"ok": True, "done": True, "rows": total_rows}
                page += 1

            _ddb_update_running(
                job_start_id,
                next_page=page,
                next_cursor=cursor if cursor else None,
                note=f"continuing pages_processed={processed_pages} rows={total_rows}",
            )

        return {"ok": True, "done": False, "rows": total_rows, "next_page": page, "cursor_present": bool(cursor)}

    except Exception as e:
        logger.exception(f"[{STREAM_NAME}] error")
        try:
            if 'job_start_id' in locals():
                _ddb_error(job_start_id, str(e))
        except:
            pass
        raise