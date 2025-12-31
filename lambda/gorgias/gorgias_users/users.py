import json
import os
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_s3 = boto3.client("s3")
_sqs = boto3.client("sqs")
_secrets = boto3.client("secretsmanager")

GORGIAS_BASE_URL = "https://healf-uk.gorgias.com/api"
S3_BUCKET = "sources-data"
S3_PREFIX_BASE = "gorgias"
PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 60)

SECRET_GORGIAS_EMAIL = "gorgias_email"
SECRET_GORGIAS_API_KEY = "gorgias_api_key"

BACKFILL_QUEUE_URL = os.environ["BACKFILL_QUEUE_URL"]

STREAM_NAME = "users"
ENDPOINT = "/users"
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

    try:
        obj = json.loads(email_raw)
        if isinstance(obj, dict):
            email_raw = obj.get("GORGIAS_EMAIL") or obj.get("email") or obj.get("value") or email_raw
    except Exception:
        pass

    try:
        obj = json.loads(key_raw)
        if isinstance(obj, dict):
            key_raw = obj.get("GORGIAS_API_KEY") or obj.get("api_key") or obj.get("key") or obj.get("value") or key_raw
    except Exception:
        pass

    email_raw = str(email_raw).strip().strip('"').strip("'")
    key_raw = str(key_raw).strip().strip('"').strip("'")

    if not email_raw or not key_raw:
        raise RuntimeError("Gorgias email/api key secrets are empty after parsing")

    _cached_email, _cached_key = email_raw, key_raw
    return (_cached_email, _cached_key)


def make_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=0)
    session.mount("https://", adapter)
    return session


def safe_get(session: requests.Session, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{GORGIAS_BASE_URL}{path}"
    auth = get_gorgias_auth()
    logger.info(f"[{STREAM_NAME}] auth email_present={bool(auth[0])} api_key_len={len(auth[1])}")

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
        MessageGroupId=STREAM_NAME,
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


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    job = extract_job(event)

    cursor = job.get("cursor")
    page_start = int(job.get("page_start", 1))

    session = make_session()
    pages_written = 0
    next_cursor = cursor
    finish_reason: Optional[str] = None

    while pages_written < PAGES_PER_INVOCATION and time_budget_ok(context):
        params: Dict[str, Any] = {"limit": PAGE_SIZE}
        if next_cursor:
            params["cursor"] = next_cursor

        payload = safe_get(session, ENDPOINT, params=params)
        items = payload.get("data", []) or []

        if not items:
            next_cursor = None
            finish_reason = "empty_page"
            logger.info(f"[{STREAM_NAME}] FINISHED reason={finish_reason} page_start={page_start} cursor_present={bool(cursor)}")
            break

        page_no = page_start + pages_written
        s3_key = f"{S3_PREFIX_BASE}/{STREAM_NAME}/page_{page_no}.json"
        s3_put_json(s3_key, payload)

        api_next_cursor = (payload.get("meta") or {}).get("next_cursor")
        logger.info(
            f"[{STREAM_NAME}] WROTE page={page_no} items={len(items)} "
            f"next_cursor_present={bool(api_next_cursor)} s3_key={s3_key}"
        )

        pages_written += 1
        next_cursor = api_next_cursor

        if not next_cursor:
            finish_reason = "no_next_cursor"
            logger.info(f"[{STREAM_NAME}] FINISHED reason={finish_reason} last_page={page_no}")
            break

    if next_cursor:
        next_body = {
            "cursor": next_cursor,
            "page_start": page_start + pages_written,
        }
        logger.info(f"[{STREAM_NAME}] ENQUEUE {json.dumps(next_body)}")
        enqueue_next(next_body)

    result = {
        "stream": STREAM_NAME,
        "pages_written": pages_written,
        "continued": bool(next_cursor),
        "finish_reason": finish_reason,
        "next_page_start": (page_start + pages_written) if next_cursor else None,
    }
    logger.info(f"[{STREAM_NAME}] RESULT {json.dumps(result)}")
    return result
