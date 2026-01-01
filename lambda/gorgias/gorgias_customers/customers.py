import json
import os
import time
import logging
import gzip
from typing import Any, Dict, Optional, List

import boto3
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
_s3 = boto3.client("s3")
_secrets = boto3.client("secretsmanager")
_ddb = boto3.resource("dynamodb")

# Config
GORGIAS_BASE_URL = "https://healf-uk.gorgias.com/api"
S3_BUCKET = os.environ.get("S3_BUCKET", "sources-data")
S3_PREFIX_BASE = os.environ.get("S3_PREFIX_BASE", "gorgias")

STATE_TABLE = os.environ["STATE_TABLE"]
TABLE = _ddb.Table(STATE_TABLE)

STREAM_NAME = "customers"
ENDPOINT = "/customers"
ORDER_BY_FIELD = "created_datetime"
DIRECTION_DEFAULT = "asc"

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))
REQUEST_TIMEOUT = (10, 60)

# Do more than 1 page per invocation if you want faster + fewer scheduler ticks
PAGES_PER_INVOCATION = int(os.environ.get("PAGES_PER_INVOCATION", "5"))
SLEEP_BETWEEN_REQUESTS_SEC = float(os.environ.get("SLEEP_BETWEEN_REQUESTS_SEC", "0.0"))

# Secrets Manager
SECRET_GORGIAS_EMAIL = "gorgias_email"
SECRET_GORGIAS_API_KEY = "gorgias_api_key"

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

    # email secret can be plaintext OR JSON key/value
    try:
        obj = json.loads(email_raw)
        if isinstance(obj, dict):
            email_raw = obj.get("GORGIAS_EMAIL") or obj.get("email") or obj.get("value") or email_raw
    except Exception:
        pass

    # api key secret can be plaintext OR JSON key/value
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


def s3_put_json_gz(key: str, payload: Dict[str, Any]) -> None:
    raw = json.dumps(payload).encode("utf-8")
    gz = gzip.compress(raw)
    _s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=gz,
        ContentType="application/json",
        ContentEncoding="gzip",
    )


def time_budget_ok(context: Any, buffer_ms: int = 70_000) -> bool:
    if context is None:
        return True
    return context.get_remaining_time_in_millis() > buffer_ms


def _release_lease(job_start_id: str) -> None:
    now = int(time.time())
    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression="SET #in_flight = :false, #lease_until = :zero, #updated_at = :now",
        ExpressionAttributeNames={
            "#in_flight": "in_flight",
            "#lease_until": "lease_until",
            "#updated_at": "updated_at",
        },
        ExpressionAttributeValues={
            ":false": False,
            ":zero": 0,
            ":now": now,
        },
    )


def _ddb_mark_done(job_start_id: str, expected_cursor: Optional[str], expected_page: int) -> None:
    now = int(time.time())
    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            ConditionExpression=(
                "#status = :running AND #page = :expected_page AND #in_flight = :true AND "
                "((#cursor = :expected_cursor) OR (attribute_not_exists(#cursor) AND :cursor_is_null = :true))"
            ),
            UpdateExpression="SET #status = :done, #in_flight = :false, #lease_until = :zero, #updated_at = :now REMOVE #cursor",
            ExpressionAttributeNames={
                "#status": "status",
                "#page": "page",
                "#cursor": "cursor",
                "#in_flight": "in_flight",
                "#lease_until": "lease_until",
                "#updated_at": "updated_at",
            },
            ExpressionAttributeValues={
                ":running": "RUNNING",
                ":done": "DONE",
                ":expected_page": expected_page,
                ":expected_cursor": expected_cursor or "__MISSING__",
                ":cursor_is_null": expected_cursor is None,
                ":true": True,
                ":false": False,
                ":zero": 0,
                ":now": now,
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning(
                f"[{STREAM_NAME}] DDB conditional failed (DONE). Likely duplicate/out-of-order. job={job_start_id}"
            )
            # Still release lease to avoid stuck RUNNING/in_flight
            _release_lease(job_start_id)
            return
        raise


def _ddb_update_progress(job_start_id: str, expected_cursor: Optional[str], expected_page: int, next_cursor: str, next_page: int) -> None:
    now = int(time.time())
    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            ConditionExpression=(
                "#status = :running AND #page = :expected_page AND #in_flight = :true AND "
                "((#cursor = :expected_cursor) OR (attribute_not_exists(#cursor) AND :cursor_is_null = :true))"
            ),
            UpdateExpression="SET #cursor = :next_cursor, #page = :next_page, #in_flight = :false, #lease_until = :zero, #updated_at = :now REMOVE #last_error",
            ExpressionAttributeNames={
                "#status": "status",
                "#page": "page",
                "#cursor": "cursor",
                "#in_flight": "in_flight",
                "#lease_until": "lease_until",
                "#updated_at": "updated_at",
                "#last_error": "last_error",
            },
            ExpressionAttributeValues={
                ":running": "RUNNING",
                ":expected_page": expected_page,
                ":expected_cursor": expected_cursor or "__MISSING__",
                ":cursor_is_null": expected_cursor is None,
                ":true": True,
                ":false": False,
                ":next_cursor": next_cursor,
                ":next_page": next_page,
                ":zero": 0,
                ":now": now,
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning(
                f"[{STREAM_NAME}] DDB conditional failed (PROGRESS). Likely duplicate/out-of-order. job={job_start_id}"
            )
            # Release lease so orchestrator can re-drive safely
            _release_lease(job_start_id)
            return
        raise


def _ddb_record_error(job_start_id: str, err: str) -> None:
    now = int(time.time())
    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression="SET #last_error = :e, #in_flight = :false, #lease_until = :zero, #updated_at = :now",
        ExpressionAttributeNames={
            "#last_error": "last_error",
            "#in_flight": "in_flight",
            "#lease_until": "lease_until",
            "#updated_at": "updated_at",
        },
        ExpressionAttributeValues={
            ":e": err[:2000],
            ":false": False,
            ":zero": 0,
            ":now": now,
        },
    )


def _parse_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    body = rec.get("body", "{}")
    return json.loads(body) if isinstance(body, str) else body


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    records = event.get("Records", [])
    logger.info(f"[{STREAM_NAME}] received_records={len(records)}")

    results: List[Dict[str, Any]] = []
    for rec in records:
        job = _parse_record(rec)

        job_start_id = job["job_start_id"]

        # IMPORTANT: msg_* are the expected values in Dynamo for this SQS message.
        # We NEVER mutate these, so conditional updates stay correct.
        msg_cursor: Optional[str] = job.get("cursor")
        msg_page: int = int(job.get("page", 1))

        # Local loop variables (we can mutate these freely)
        cursor = msg_cursor
        page = msg_page

        session = make_session()
        pages_written = 0

        try:
            while pages_written < PAGES_PER_INVOCATION and time_budget_ok(context):
                params: Dict[str, Any] = {
                    "limit": PAGE_SIZE,
                    "order_by": f"{ORDER_BY_FIELD}:{job.get('direction', DIRECTION_DEFAULT)}",
                }
                if cursor:
                    params["cursor"] = cursor

                payload = safe_get(session, ENDPOINT, params=params)
                items = payload.get("data", []) or []
                api_next_cursor = (payload.get("meta") or {}).get("next_cursor")

                if not items:
                    logger.info(f"[{STREAM_NAME}] empty page -> DONE job={job_start_id} page={page}")
                    _ddb_mark_done(job_start_id, expected_cursor=msg_cursor, expected_page=msg_page)
                    break

                s3_key = f"{S3_PREFIX_BASE}/{STREAM_NAME}/job={job_start_id}/page={page:06d}.json.gz"
                s3_put_json_gz(s3_key, payload)

                logger.info(
                    f"[{STREAM_NAME}] WROTE job={job_start_id} page={page} items={len(items)} "
                    f"next_cursor_present={bool(api_next_cursor)} s3_key={s3_key}"
                )

                pages_written += 1

                if not api_next_cursor:
                    logger.info(f"[{STREAM_NAME}] no next_cursor -> DONE job={job_start_id} last_page={page}")
                    _ddb_mark_done(job_start_id, expected_cursor=msg_cursor, expected_page=msg_page)
                    break

                # Update state ONCE per page using the ORIGINAL expected msg_* values.
                # This is safe even with multi-page-per-invocation because orchestrator only enqueues 1 message at a time.
                _ddb_update_progress(
                    job_start_id=job_start_id,
                    expected_cursor=msg_cursor,
                    expected_page=msg_page,
                    next_cursor=api_next_cursor,
                    next_page=page + 1,
                )

                # After successful state update, our "message expected state" becomes the new state
                msg_cursor = api_next_cursor
                msg_page = page + 1

                # Advance local loop vars too
                cursor = api_next_cursor
                page = page + 1

                if SLEEP_BETWEEN_REQUESTS_SEC > 0:
                    time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

        except Exception as e:
            logger.exception(f"[{STREAM_NAME}] ERROR job={job_start_id}")
            _ddb_record_error(job_start_id, str(e))
            raise

        results.append({"job_start_id": job_start_id, "pages_written": pages_written})

    return {"results": results}
