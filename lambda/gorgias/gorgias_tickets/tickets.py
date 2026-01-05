
import json
import os
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List, Tuple

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
GORGIAS_BASE_URL = os.environ.get("GORGIAS_BASE_URL", "https://healf-uk.gorgias.com/api")

S3_BUCKET = os.environ.get("S3_BUCKET", "sources-data")
S3_PREFIX_BASE = os.environ.get("S3_PREFIX_BASE", "gorgias")

STATE_TABLE = os.environ["STATE_TABLE"]
TABLE = _ddb.Table(STATE_TABLE)

STREAM_NAME = 'tickets'
ENDPOINT = '/tickets'

# Ordering / cutoff
USE_ORDER_BY = os.environ.get("USE_ORDER_BY", "true").lower() in ("1", "true", "yes", "y")
ORDER_BY_FIELD = 'updated_datetime'  # only used if USE_ORDER_BY is true
DIRECTION_DEFAULT = "desc"           # managers want "latest -> go back"
CUTOFF_DAYS = int(os.environ.get("CUTOFF_DAYS", "33"))
CUTOFF_FIELD = 'updated_datetime'      # datetime field inside items used for cutoff/filter
FILTER_TO_CUTOFF = os.environ.get("FILTER_TO_CUTOFF", "true").lower() in ("1", "true", "yes", "y")

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))
REQUEST_TIMEOUT = (10, 60)

PAGES_PER_INVOCATION = int(os.environ.get("PAGES_PER_INVOCATION", "5"))
SLEEP_BETWEEN_REQUESTS_SEC = float(os.environ.get("SLEEP_BETWEEN_REQUESTS_SEC", "0.0"))

# Secrets Manager
SECRET_GORGIAS_EMAIL = "gorgias_email"
SECRET_GORGIAS_API_KEY = "gorgias_api_key"

_cached_email: Optional[str] = None
_cached_key: Optional[str] = None


def get_secret_string(secret_name: str) -> str:
    r = _secrets.get_secret_value(SecretId=secret_name)
    if r.get("SecretString"):
        return r["SecretString"]
    return r["SecretBinary"].decode("utf-8")


def get_gorgias_auth() -> Tuple[str, str]:
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


def parse_iso_utc(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def compute_cutoff_dt(now_utc: Optional[datetime] = None) -> datetime:
    now_utc = now_utc or datetime.now(timezone.utc)
    return now_utc - timedelta(days=CUTOFF_DAYS)


def make_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=0)
    session.mount("https://", adapter)
    return session


def _maybe_adaptive_sleep(headers: Dict[str, str]) -> None:
    # If the API gives us X-Gorgias-Account-Api-Call-Limit like "10/40",
    # gently back off when we're close to the ceiling to reduce 429s.
    limit_hdr = headers.get("X-Gorgias-Account-Api-Call-Limit") or headers.get("x-gorgias-account-api-call-limit")
    if not limit_hdr:
        return
    try:
        used_s, limit_s = limit_hdr.split("/", 1)
        used = int(used_s.strip())
        limit = int(limit_s.strip())
        if limit > 0 and used / limit >= 0.9:
            time.sleep(2.0)
    except Exception:
        return


def safe_get(session: requests.Session, path: str, params: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    url = f"{GORGIAS_BASE_URL}{path}"
    auth = get_gorgias_auth()

    while True:
        r = session.get(url, params=params, auth=auth, timeout=REQUEST_TIMEOUT)

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 10))
            logger.warning(f"[{STREAM_NAME}] 429 rate limited. sleeping {retry_after}s")
            time.sleep(retry_after + 1)
            continue

        # Optional: if order_by is not supported for this endpoint (or this account),
        # fall back once without it.
        if r.status_code == 400 and USE_ORDER_BY and "order_by" in params:
            logger.warning(f"[{STREAM_NAME}] 400 with order_by, retrying once without order_by. url={r.url}")
            params = dict(params)
            params.pop("order_by", None)
            r = session.get(url, params=params, auth=auth, timeout=REQUEST_TIMEOUT)

        if r.status_code >= 400:
            logger.error(f"[{STREAM_NAME}] ERROR url={r.url} status={r.status_code} body={r.text[:1200]}")
        r.raise_for_status()

        headers = {k: v for k, v in r.headers.items()}
        _maybe_adaptive_sleep(headers)
        return r.json(), headers


def s3_put_json(key: str, payload: Dict[str, Any]) -> None:
    raw = json.dumps(payload).encode("utf-8")
    _s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=raw,
        ContentType="application/json",
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

    if expected_cursor is None:
        cursor_condition = "attribute_not_exists(#cursor)"
        eav = {
            ":running": "RUNNING",
            ":done": "DONE",
            ":expected_page": expected_page,
            ":true": True,
            ":false": False,
            ":zero": 0,
            ":now": now,
        }
    else:
        cursor_condition = "#cursor = :expected_cursor"
        eav = {
            ":running": "RUNNING",
            ":done": "DONE",
            ":expected_page": expected_page,
            ":expected_cursor": expected_cursor,
            ":true": True,
            ":false": False,
            ":zero": 0,
            ":now": now,
        }

    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            ConditionExpression=(
                "#status = :running AND #page = :expected_page AND #in_flight = :true AND "
                f"({cursor_condition})"
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
            ExpressionAttributeValues=eav,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning(f"[{STREAM_NAME}] DDB conditional failed (DONE). Likely duplicate/out-of-order. job={job_start_id}")
            _release_lease(job_start_id)
            return
        raise


def _ddb_update_progress(
    job_start_id: str,
    expected_cursor: Optional[str],
    expected_page: int,
    next_cursor: str,
    next_page: int,
) -> None:
    now = int(time.time())

    if expected_cursor is None:
        cursor_condition = "attribute_not_exists(#cursor)"
        eav = {
            ":running": "RUNNING",
            ":expected_page": expected_page,
            ":true": True,
            ":false": False,
            ":next_cursor": next_cursor,
            ":next_page": next_page,
            ":zero": 0,
            ":now": now,
        }
    else:
        cursor_condition = "#cursor = :expected_cursor"
        eav = {
            ":running": "RUNNING",
            ":expected_page": expected_page,
            ":expected_cursor": expected_cursor,
            ":true": True,
            ":false": False,
            ":next_cursor": next_cursor,
            ":next_page": next_page,
            ":zero": 0,
            ":now": now,
        }

    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            ConditionExpression=(
                "#status = :running AND #page = :expected_page AND #in_flight = :true AND "
                f"({cursor_condition})"
            ),
            UpdateExpression=(
                "SET #cursor = :next_cursor, #page = :next_page, #in_flight = :false, "
                "#lease_until = :zero, #updated_at = :now REMOVE #last_error"
            ),
            ExpressionAttributeNames={
                "#status": "status",
                "#page": "page",
                "#cursor": "cursor",
                "#in_flight": "in_flight",
                "#lease_until": "lease_until",
                "#updated_at": "updated_at",
                "#last_error": "last_error",
            },
            ExpressionAttributeValues=eav,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning(f"[{STREAM_NAME}] DDB conditional failed (PROGRESS). Likely duplicate/out-of-order. job={job_start_id}")
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


def _apply_cutoff(
    items: List[Dict[str, Any]],
    cutoff_dt: datetime,
) -> Tuple[List[Dict[str, Any]], bool, Optional[datetime], Optional[datetime]]:
    # Returns (filtered_items, reached_cutoff, min_dt, max_dt).
    dts: List[datetime] = []
    for it in items:
        dt = parse_iso_utc(it.get(CUTOFF_FIELD))
        if dt:
            dts.append(dt)

    if not dts:
        # If the field isn't present, we can't safely cutoff; keep all.
        return items, False, None, None

    min_dt = min(dts)
    max_dt = max(dts)

    reached_cutoff = min_dt < cutoff_dt

    if not FILTER_TO_CUTOFF:
        return items, reached_cutoff, min_dt, max_dt

    filtered: List[Dict[str, Any]] = []
    for it in items:
        dt = parse_iso_utc(it.get(CUTOFF_FIELD))
        if not dt:
            filtered.append(it)
            continue
        if dt >= cutoff_dt:
            filtered.append(it)

    return filtered, reached_cutoff, min_dt, max_dt


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    records = event.get("Records", [])
    logger.info(f"[{STREAM_NAME}] received_records={len(records)}")

    results: List[Dict[str, Any]] = []
    cutoff_dt = compute_cutoff_dt()

    for rec in records:
        job = _parse_record(rec)
        job_start_id = job["job_start_id"]

        # Expected state from the SQS message (used for conditional updates)
        msg_cursor: Optional[str] = job.get("cursor")
        msg_page: int = int(job.get("page", 1))

        # Local loop vars (mutable)
        cursor = msg_cursor
        page = msg_page

        direction = (job.get("direction") or DIRECTION_DEFAULT).lower()

        session = make_session()
        pages_written = 0

        try:
            while pages_written < PAGES_PER_INVOCATION and time_budget_ok(context):
                params: Dict[str, Any] = {"limit": PAGE_SIZE}
                if USE_ORDER_BY and ORDER_BY_FIELD:
                    params["order_by"] = f"{ORDER_BY_FIELD}:{direction}"
                if cursor:
                    params["cursor"] = cursor

                payload, headers = safe_get(session, ENDPOINT, params=params)
                items = payload.get("data", []) or []
                api_next_cursor = (payload.get("meta") or {}).get("next_cursor")

                if not items:
                    logger.info(f"[{STREAM_NAME}] empty page -> DONE job={job_start_id} page={page}")
                    _ddb_mark_done(job_start_id, expected_cursor=msg_cursor, expected_page=msg_page)
                    break

                # Cutoff handling (33 days)
                filtered_items, reached_cutoff, min_dt, max_dt = _apply_cutoff(items, cutoff_dt)

                if FILTER_TO_CUTOFF:
                    payload = dict(payload)
                    payload["data"] = filtered_items
                    payload["_healf_cutoff"] = {
                        "cutoff_days": CUTOFF_DAYS,
                        "cutoff_utc": cutoff_dt.isoformat(),
                        "field": CUTOFF_FIELD,
                        "direction": direction,
                        "reached_cutoff": reached_cutoff,
                        "min_dt": min_dt.isoformat() if min_dt else None,
                        "max_dt": max_dt.isoformat() if max_dt else None,
                        "original_count": len(items),
                        "kept_count": len(filtered_items),
                    }

                # If everything is older than cutoff, we're done and shouldn't write.
                if FILTER_TO_CUTOFF and len(filtered_items) == 0 and reached_cutoff:
                    logger.info(f"[{STREAM_NAME}] cutoff passed and no items kept -> DONE job={job_start_id} page={page}")
                    _ddb_mark_done(job_start_id, expected_cursor=msg_cursor, expected_page=msg_page)
                    break

                # Write to S3 (same layout as customers)
                s3_key = f"{S3_PREFIX_BASE}/{STREAM_NAME}/job={job_start_id}/page={page:06d}.json"
                s3_put_json(s3_key, payload)

                limit_hdr = headers.get("X-Gorgias-Account-Api-Call-Limit") or headers.get("x-gorgias-account-api-call-limit")
                logger.info(
                    f"[{STREAM_NAME}] WROTE job={job_start_id} page={page} items={len(payload.get('data') or [])} "
                    f"next_cursor_present={bool(api_next_cursor)} cutoff_reached={reached_cutoff} "
                    f"rate_limit={limit_hdr!r} s3_key={s3_key}"
                )

                pages_written += 1

                # Stop at cutoff boundary (we already wrote the last in-range page)
                if reached_cutoff:
                    logger.info(f"[{STREAM_NAME}] reached cutoff -> DONE job={job_start_id} last_page={page}")
                    _ddb_mark_done(job_start_id, expected_cursor=msg_cursor, expected_page=msg_page)
                    break

                if not api_next_cursor:
                    logger.info(f"[{STREAM_NAME}] no next_cursor -> DONE job={job_start_id} last_page={page}")
                    _ddb_mark_done(job_start_id, expected_cursor=msg_cursor, expected_page=msg_page)
                    break

                # Update DDB using the expected msg_* values
                _ddb_update_progress(
                    job_start_id=job_start_id,
                    expected_cursor=msg_cursor,
                    expected_page=msg_page,
                    next_cursor=api_next_cursor,
                    next_page=page + 1,
                )

                # After successful state update, expected state becomes the new state
                msg_cursor = api_next_cursor
                msg_page = page + 1

                # Advance local vars
                cursor = api_next_cursor
                page = page + 1

                if SLEEP_BETWEEN_REQUESTS_SEC > 0:
                    time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

        except Exception as e:
            logger.exception(f"[{STREAM_NAME}] ERROR job={job_start_id}")
            _ddb_record_error(job_start_id, str(e))
            raise
        finally:
            # Safety: if we never wrote a page (so no progress/done update released the lease),
            # release it so orchestrator can retry quickly.
            if pages_written == 0:
                _release_lease(job_start_id)

        results.append({"job_start_id": job_start_id, "pages_written": pages_written})

    return {"results": results}
