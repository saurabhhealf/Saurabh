import base64
import json
import os
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import boto3
import requests

API_REVISION = "2025-10-15"
PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 60)  # (connect_timeout, read_timeout)
CUSTOM_PROFILE_PROPERTIES = {
    "DATE_OF_BIRTH": "Date_of_Birth",
    "LAST_FEMALE_CYCLE_STATUS": "Last_Female_Cycle_Status",
}
SECRET_NAME = "klaviyo"
SECRET_KEY_NAME = "API_KEY"

_secrets_client = boto3.client("secretsmanager")
_s3_client = boto3.client("s3")


def get_api_key(secret_name: str = SECRET_NAME, key_name: str = SECRET_KEY_NAME) -> str:
    """Fetch the Klaviyo API key from AWS Secrets Manager."""
    resp = _secrets_client.get_secret_value(SecretId=secret_name)
    secret = resp.get("SecretString")
    if secret is None:
        binary_secret = resp.get("SecretBinary")
        if binary_secret is None:
            raise ValueError(f"Secret {secret_name} has no payload")
        secret = base64.b64decode(binary_secret).decode("utf-8")

    try:
        parsed = json.loads(secret)
        if isinstance(parsed, dict):
            api_key = (
                parsed.get(key_name)
                or parsed.get("api_key")
                or parsed.get("KLAVIYO_API_KEY")
            )
            if api_key:
                return api_key
    except json.JSONDecodeError:
        pass

    return secret


def make_session(api_key: str, pool_maxsize: int = 20) -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=pool_maxsize,
        pool_maxsize=pool_maxsize,
        max_retries=0,
    )
    session.mount("https://", adapter)
    session.headers.update({
        "Authorization": f"Klaviyo-API-Key {api_key}",
        "accept": "application/json",
        "revision": API_REVISION,
        "Connection": "keep-alive",
    })
    return session


def safe_get(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 4,
    timeout: tuple = (10, 60),
) -> requests.Response:
    """GET with basic retry & backoff. Used for ONE page per Lambda."""
    attempt = 0
    while True:
        try:
            response = session.get(url, params=params, timeout=timeout)
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
        ) as exc:
            attempt += 1
            if attempt > max_retries:
                raise
            sleep_seconds = min(2 ** attempt, 30) + random.uniform(0, 1)
            print(f"[WARN] Network issue: {exc}. Retrying in {sleep_seconds:.1f}s")
            time.sleep(sleep_seconds)
            continue

        if response.status_code in (429, 500, 502, 503, 504):
            attempt += 1
            if attempt > max_retries:
                response.raise_for_status()
            sleep_seconds = min(2 ** attempt, 30) + random.uniform(0, 1)
            print(f"[WARN] HTTP {response.status_code}. Retrying in {sleep_seconds:.1f}s")
            time.sleep(sleep_seconds)
            continue

        if response.status_code == 400:
            print(f"[ERROR] 400 Bad Request. Response body: {response.text}")
            response.raise_for_status()

        response.raise_for_status()
        return response


def save_payload(bucket_name: str, prefix: str, page_index: int, payload: Dict[str, Any]) -> str:
    key = f"{prefix}/page_{page_index}.json"
    body = json.dumps(payload).encode("utf-8")
    _s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
    return key


def normalize_date_range(
    date_str: Optional[str],
    start_dt: Optional[str],
    end_dt: Optional[str],
    default_date: Optional[datetime] = None,
) -> Tuple[str, str]:
    """
    Resolve the date range we should pull.
    If date_str (YYYY-MM-DD) is provided, we use that full day in UTC.
    Otherwise start_dt and end_dt must both be present.
    """
    if date_str:
        start = datetime.strptime(date_str, "%Y-%m-%d")
        end = start + timedelta(days=1)
        return (
            f"{start:%Y-%m-%dT%H:%M:%S}Z",
            f"{end:%Y-%m-%dT%H:%M:%S}Z",
        )

    if start_dt and end_dt:
        return start_dt, end_dt

    base = default_date or (datetime.utcnow() - timedelta(days=1))
    base = datetime(base.year, base.month, base.day)
    next_day = base + timedelta(days=1)
    return (
        f"{base:%Y-%m-%dT%H:%M:%S}Z",
        f"{next_day:%Y-%m-%dT%H:%M:%S}Z",
    )


def build_updated_filter(start_datetime: str, end_datetime: str) -> str:
    """
    Build a Klaviyo API filter that limits profiles by updated timestamp.
    """
    return (
        f'and(greater-or-equal(updated,"{start_datetime}"),'
        f'less-than(updated,"{end_datetime}"))'
    )


def add_custom_columns(payload: Dict[str, Any]) -> None:
    """
    Surface select Klaviyo profile properties as first-class columns.
    """
    records = payload.get("data")
    if not isinstance(records, list):
        return

    for record in records:
        if not isinstance(record, dict):
            continue

        attributes = record.get("attributes") or {}
        properties = attributes.get("properties") or {}
        if not isinstance(properties, dict):
            properties = {}

        for column_name, property_key in CUSTOM_PROFILE_PROPERTIES.items():
            record[column_name] = properties.get(property_key)


def handler(event: Optional[Dict[str, Any]] = None, _context: Any = None) -> Dict[str, Any]:
    """
    Fetch a single day's worth of profiles (or a custom date range) and write
    every page from that slice to S3 in one Lambda invocation.
    """
    event = event or {}
    start_time = time.time()

    bucket_name = os.environ.get("KLAVIYO_PROFILES_BUCKET")
    if not bucket_name:
        raise ValueError("Missing KLAVIYO_PROFILES_BUCKET environment variable")

    default_day = datetime.utcnow() - timedelta(days=1)
    target_date = event.get("date") or event.get("run_date") or default_day.strftime("%Y-%m-%d")
    start_dt, end_dt = normalize_date_range(
        event.get("date") or event.get("run_date"),
        event.get("start_datetime"),
        event.get("end_datetime"),
        default_day,
    )

    prefix = f"profiles/{target_date}"

    secret_name = event.get("secret_name") or SECRET_NAME
    secret_key = event.get("secret_key") or SECRET_KEY_NAME

    api_key = get_api_key(secret_name, secret_key).strip()
    if not api_key:
        raise ValueError(f"Secret {secret_name} did not return an API key")
    session = make_session(api_key)

    url = "https://a.klaviyo.com/api/profiles/"
    params: Dict[str, Any] = {
        "page[size]": str(PAGE_SIZE),
        "sort": "updated",
        "additional-fields[profile]": "subscriptions",
        "filter": build_updated_filter(start_dt, end_dt),
    }

    pages_saved = 0
    page_index = 1

    while True:
        response = safe_get(session, url, params=params, timeout=REQUEST_TIMEOUT)
        payload = response.json()
        add_custom_columns(payload)

        data = payload.get("data") or []
        if not data:
            print("[INFO] No more profiles in this window; finishing.")
            break

        key = save_payload(bucket_name, prefix, page_index, payload)
        print(f"[INFO] Saved page {page_index} ({len(data)} profiles) to s3://{bucket_name}/{key}")
        pages_saved += 1

        next_url = (payload.get("links") or {}).get("next")
        if not next_url:
            break

        url, params = next_url, None
        page_index += 1

    elapsed = time.time() - start_time
    print(f"[INFO] Finished {target_date} window in {elapsed:.1f}s ({pages_saved} pages).")

    return {
        "pages_saved": pages_saved,
        "run_date": target_date,
        "start_datetime": start_dt,
        "end_datetime": end_dt,
        "s3_prefix": f"s3://{bucket_name}/{prefix}/",
    }


if __name__ == "__main__":
    result = handler()
    print(
        f"[DONE] Saved {result['pages_saved']} pages to {result['s3_prefix']}, "
        f"page_index={result['page_index']}, done={result['done']}"
    )
