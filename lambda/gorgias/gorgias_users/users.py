import os
import json
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_ddb = boto3.resource("dynamodb")
_s3 = boto3.client("s3")
_sm = boto3.client("secretsmanager")

GORGIAS_BASE_URL = os.environ.get("GORGIAS_BASE_URL", "https://healf-uk.gorgias.com/api").rstrip("/")
STATE_TABLE = os.environ["STATE_TABLE"]
S3_BUCKET = os.environ.get("S3_BUCKET", "sources-data")
S3_PREFIX_BASE = os.environ.get("S3_PREFIX_BASE", "gorgias").strip("/")

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))
PAGES_PER_INVOCATION = int(os.environ.get("PAGES_PER_INVOCATION", "20"))

GORGIAS_EMAIL_SECRET = os.environ.get("GORGIAS_EMAIL_SECRET", "gorgias_email")
GORGIAS_API_KEY_SECRET = os.environ.get("GORGIAS_API_KEY_SECRET", "gorgias_api_key")

STREAM_NAME = "users"
ENDPOINT = "/users"

TABLE = _ddb.Table(STATE_TABLE)


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
                return parsed[k].strip()
    return secret


def _gorgias_auth() -> Tuple[str, str]:
    email = _get_secret_value(GORGIAS_EMAIL_SECRET)
    api_key = _get_secret_value(GORGIAS_API_KEY_SECRET)
    if not email or not api_key:
        raise ValueError("Missing Gorgias credentials")
    return email, api_key


def _time_left_ok(context, buffer_ms: int = 12_000) -> bool:
    if context is None:
        return True
    try:
        return context.get_remaining_time_in_millis() > buffer_ms
    except Exception:
        return True


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


def _ddb_get(job_start_id: str) -> Optional[Dict[str, Any]]:
    return TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")


def _ddb_update_running(job_start_id: str, next_page: int, next_cursor: Optional[str], note: str = "") -> None:
    now = _utc_now_ts()
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
    else:
        expr += " REMOVE #cursor"
        names["#cursor"] = "cursor"
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


def _parse_items(resp_json: Any) -> List[Dict[str, Any]]:
    if isinstance(resp_json, dict):
        for k in ["data", "items", "results", STREAM_NAME]:
            v = resp_json.get(k)
            if isinstance(v, list):
                return v
        for v in resp_json.values():
            if isinstance(v, list):
                return v
    if isinstance(resp_json, list):
        return resp_json
    return []


def _parse_next_cursor(resp_json: Any) -> Optional[str]:
    if isinstance(resp_json, dict):
        meta = resp_json.get("meta")
        if isinstance(meta, dict):
            for k in ["next_cursor", "nextCursor", "cursor_next", "cursorNext"]:
                v = meta.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
        for k in ["next_cursor", "nextCursor"]:
            v = resp_json.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def _fetch_page(page: int, cursor: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    email, api_key = _gorgias_auth()
    url = f"{GORGIAS_BASE_URL}{ENDPOINT}"

    params: Dict[str, Any] = {
        "limit": PAGE_SIZE,
        "per_page": PAGE_SIZE,
        "page": page,
        # ASC from the beginning
        "order_by": "created_datetime",
        "order_direction": "asc",
        "sort": "created_datetime",
        "direction": "asc",
    }
    if cursor:
        params["cursor"] = cursor

    r = requests.get(url, params=params, auth=(email, api_key), timeout=60)
    r.raise_for_status()
    j = r.json()
    return _parse_items(j), _parse_next_cursor(j)


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

        while processed_pages < PAGES_PER_INVOCATION and _time_left_ok(context):
            items, next_cursor = _fetch_page(page=page, cursor=cursor)

            if not items:
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
            job_start_id = None
            if event and event.get("Records"):
                body = json.loads(event["Records"][0]["body"])
                job_start_id = body.get("job_start_id")
            if job_start_id:
                _ddb_error(job_start_id, str(e))
        except Exception:
            pass
        raise
