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

# ✅ Hardcode non-secret config
GORGIAS_BASE_URL = "https://healf-uk.gorgias.com/api"
S3_BUCKET = "sources-data"
S3_PREFIX_BASE = "gorgias"
PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 60)

# ✅ Secret names (hardcoded; values come from Secrets Manager)
SECRET_GORGIAS_EMAIL = "gorgias_email"
SECRET_GORGIAS_API_KEY = "gorgias_api_key"

# ✅ Still need these per-lambda because they’re not secrets
BACKFILL_QUEUE_URL = os.environ["BACKFILL_QUEUE_URL"]
STREAM_NAME = os.environ.get("STREAM_NAME", "unknown")

# Cached secrets (reuse across warm invocations)
_cached_email: Optional[str] = None
_cached_key: Optional[str] = None

def get_secret_string(secret_name: str) -> str:
    r = _secrets.get_secret_value(SecretId=secret_name)
    if "SecretString" in r and r["SecretString"]:
        return r["SecretString"]
    # if stored as binary, decode
    return r["SecretBinary"].decode("utf-8")

def get_gorgias_auth() -> tuple[str, str]:
    global _cached_email, _cached_key
    if _cached_email and _cached_key:
        return (_cached_email, _cached_key)

    email_raw = get_secret_string(SECRET_GORGIAS_EMAIL).strip()
    key_raw = get_secret_string(SECRET_GORGIAS_API_KEY).strip()

    # If you stored JSON like {"value":"..."} handle it:
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
