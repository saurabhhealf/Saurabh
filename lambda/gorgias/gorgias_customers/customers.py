import json
import os
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
_s3 = boto3.client("s3")
_sqs = boto3.client("sqs")
_secrets = boto3.client("secretsmanager")

# Config (same as before)
GORGIAS_BASE_URL = "https://healf-uk.gorgias.com/api"
S3_BUCKET = "sources-data"
S3_PREFIX_BASE = "gorgias"
PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 60)

# Secrets Manager names (same as before)
SECRET_GORGIAS_EMAIL = "gorgias_email"
SECRET_GORGIAS_API_KEY = "gorgias_api_key"

# Per-lambda env (same as before)
BACKFILL_QUEUE_URL = os.environ["BACKFILL_QUEUE_URL"]

# Stream config
STREAM_NAME = "customers"
ENDPOINT = "/customers"
ORDER_BY_FIELD = "created_datetime"
DIRECTION_DEFAULT = "asc"

# New behavior: fixed pages per invocation
PAGES_PER_INVOCATION = 1

_cached_email: Optional[str] = None
_cached_key: Optional[str] = None


def get_secret_string(secret_name: str) -> str:
    r = _secrets.get_secret_value(SecretId=secret_name)
    if "SecretString" in r and r["SecretString"]:
        return r["SecretString"]
    return r["SecretBinary"].decode("utf-8")


def get_gorgias_auth() -> tuple[str, str]:
    global _cached_email, _cached_key
    if _cached_email and _cached_key:
        return (_cached_email, _cached_key)

    email_raw = get_secret_string(SECRET_GORGIAS_EMAIL).strip()
    key_raw = get_secret_string(SECRET_GORGIAS_API_KEY).strip()

    # Handle possible JSON-wrapped secret values
    try:
        email_obj = json.loads(email_raw)
        if isinstance(email_obj, dict):
            email_raw = (email_obj.get("value") or email_obj.get("email") or email_raw).strip()
    except Exception:
        pass

    try:
        key_obj = json.loads(key_raw)
        if isinstance(key_obj, dict):
            key_raw = (key_obj.get("value") or key_obj.get("api_key") or key_obj.get("key") or key_raw).strip()
    except Exception:
        pass

    if not email_raw or not key_raw:
        raise RuntimeError("Gorgias email/api key secrets are empty")

    _cached_email, _cached_key = email_raw, key_raw
    return (_cached_email, _cached_key)


def parse_iso_utc(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def make_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=0)
    session.mount("https://", adapter)
    return session


def safe_get(session: requests.Session, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{GORGIAS_BASE_URL}{path}"
    auth = get_gorgias_auth()

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
        return r.json()


def s3_put_json(key: str, payload: Dict[str, Any]) -> None:
    _s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )


def enqueue_next(body: Dict[str, Any]) -> None:
    _sqs.send_message(
        QueueUrl=BACKFILL_QUEUE_URL,
        MessageBody=json.dumps(body),
        MessageGroupId=STREAM_NAME,  # keep same behavior (FIFO assumption)
    )


def extract_job(event: Dict[str, Any]) -> Dict[str, Any]:
    if "Records" in event and event["Records"]:
        body = event["Records"][0].get("body", "{}")
        return json.loads(body) if isinstance(body, str) else body
    return event


def time_budget_ok(context: Any, buffer_ms: int = 70_000) -> bool:
    if context is None:
        return True
    return context.get_remaining_time_in_millis() > buffer_ms


def should_stop_by_cutoff(items: List[Dict[str, Any]], field: str, direction: str, cutoff_dt: Optional[datetime]) -> bool:
    if not cutoff_dt:
        return False

    dts: List[datetime] = []
    for it in items:
        v = it.get(field)
        if not v:
            continue
        try:
            dts.append(parse_iso_utc(v))
        except Exception:
            continue

    if not dts:
        return False

    direction = (direction or "").lower()
    if direction == "desc":
        return min(dts) <= cutoff_dt
    # default asc
    return max(dts) >= cutoff_dt


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    job = extract_job(event)

    cursor = job.get("cursor")
    page_start = int(job.get("page_start", 1))
    run_id = job.get("run_id") or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    direction = job.get("direction", DIRECTION_DEFAULT)

    cutoff = job.get("cutoff")
    cutoff_dt = parse_iso_utc(cutoff) if cutoff else None

    session = make_session()
    pages_written = 0
    next_cursor = cursor

    while pages_written < PAGES_PER_INVOCATION and time_budget_ok(context):
        params: Dict[str, Any] = {
            "limit": PAGE_SIZE,
            "order_by": f"{ORDER_BY_FIELD}:{direction}",
        }
        if next_cursor:
            params["cursor"] = next_cursor

        payload = safe_get(session, ENDPOINT, params=params)
        items = payload.get("data", [])
        if not items:
            next_cursor = None
            break

        page_no = page_start + pages_written
        s3_key = f"{S3_PREFIX_BASE}/customers/{run_id}/page_{page_no}.json"
        s3_put_json(s3_key, payload)

        pages_written += 1
        next_cursor = (payload.get("meta") or {}).get("next_cursor")

        if should_stop_by_cutoff(items, ORDER_BY_FIELD, direction, cutoff_dt):
            next_cursor = None
            break

        if not next_cursor:
            break

    if next_cursor:
        enqueue_next(
            {
                "cursor": next_cursor,
                "page_start": page_start + pages_written,
                "run_id": run_id,
                "cutoff": cutoff,
                "direction": direction,
            }
        )

    return {"stream": STREAM_NAME, "run_id": run_id, "pages_written": pages_written, "continued": bool(next_cursor)}
