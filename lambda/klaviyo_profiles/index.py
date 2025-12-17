import base64
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import boto3
import requests

# --- Configuration ---
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
    """GET with basic retry & backoff."""
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
            print(f"[ERROR] 400 Bad Request. URL: {url} Params: {params}")
            print(f"[ERROR] Response body: {response.text}")
            response.raise_for_status()

        response.raise_for_status()
        return response


def save_payload(bucket_name: str, prefix: str, page_index: int, payload: Dict[str, Any]) -> str:
    key = f"{prefix}/page_{page_index}.json"
    body = json.dumps(payload).encode("utf-8")
    _s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
    return key


def get_target_window(date_str: Optional[str]) -> Tuple[datetime, datetime, str]:
    """
    Returns (start_dt, end_dt, date_string) in UTC.
    If no date provided, defaults to Yesterday.
    """
    if date_str:
        # User provided specific date (e.g., from SQS or Test Event)
        start = datetime.strptime(date_str, "%Y-%m-%d")
    else:
        # Default to Yesterday
        base = datetime.utcnow() - timedelta(days=1)
        start = datetime(base.year, base.month, base.day)
        date_str = start.strftime("%Y-%m-%d")

    # Make start aware (UTC)
    start = start.replace(tzinfo=timezone.utc)
    # End is exactly 24 hours later
    end = start + timedelta(days=1)
    
    return start, end, date_str


def parse_klaviyo_date(date_str: str) -> datetime:
    """Helper to parse ISO strings like '2025-12-16T12:00:00+00:00' or '...Z'"""
    # Fix 'Z' to '+00:00' for standard fromisoformat compatibility
    if date_str.endswith('Z'):
        date_str = date_str[:-1] + '+00:00'
    
    dt = datetime.fromisoformat(date_str)
    # Ensure UTC
    return dt.astimezone(timezone.utc)


def add_custom_columns(payload: Dict[str, Any]) -> None:
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
    Fetches profiles updated on a specific 'date'.
    Strategy: Sort by -updated (newest first).
      1. Skip records newer than (date + 1 day).
      2. Save records within (date).
      3. STOP when we hit records older than (date).
    """
    event = event or {}
    start_time = time.time()

    bucket_name = os.environ.get("KLAVIYO_PROFILES_BUCKET")
    if not bucket_name:
        raise ValueError("Missing KLAVIYO_PROFILES_BUCKET environment variable")

    # 1. Determine the exact 24-hour window we want to scrape
    start_ts, end_ts, target_date_str = get_target_window(event.get("date") or event.get("run_date"))

    prefix = f"profiles/{target_date_str}"
    print(f"[INFO] Target Window: {start_ts} to {end_ts} (Target Date: {target_date_str})")

    secret_name = event.get("secret_name") or SECRET_NAME
    secret_key = event.get("secret_key") or SECRET_KEY_NAME

    api_key = get_api_key(secret_name, secret_key).strip()
    session = make_session(api_key)

    url = "https://a.klaviyo.com/api/profiles/"
    
    # 2. KEY CHANGE: Sort by newest first, NO filter parameter
    params: Dict[str, Any] = {
        "page[size]": str(PAGE_SIZE),
        "sort": "-updated",
        "additional-fields[profile]": "subscriptions",
    }

    pages_saved = 0
    page_index = 1
    total_profiles = 0
    stop_fetching = False

    while True:
        response = safe_get(session, url, params=params, timeout=REQUEST_TIMEOUT)
        payload = response.json()
        
        add_custom_columns(payload)
        raw_data = payload.get("data") or []

        if not raw_data:
            print("[INFO] No more profiles available from API.")
            break

        valid_records = []
        
        # 3. Client-Side Filter Loop
        for record in raw_data:
            updated_str = record.get("attributes", {}).get("updated")
            if not updated_str:
                continue

            try:
                record_dt = parse_klaviyo_date(updated_str)
            except ValueError:
                continue

            # Case A: Record is NEWER than our target day (e.g. from Today, but we want Yesterday)
            # We skip this record, but we must CONTINUE fetching pages to find older ones.
            if record_dt >= end_ts:
                continue
            
            # Case B: Record is OLDER than our target day.
            # Since we are sorting by newest, this means ALL future records are too old.
            # We STOP immediately.
            if record_dt < start_ts:
                stop_fetching = True
                break
            
            # Case C: Record is INSIDE our target day. Keep it.
            valid_records.append(record)

        # 4. Save if we found any valid records on this page
        if valid_records:
            payload["data"] = valid_records
            # Clean up links to avoid confusion
            if "links" in payload:
                del payload["links"]
            
            key = save_payload(bucket_name, prefix, page_index, payload)
            print(f"[INFO] Saved page {page_index} ({len(valid_records)} profiles) to s3://{bucket_name}/{key}")
            pages_saved += 1
            total_profiles += len(valid_records)
        else:
            # If valid_records is empty but stop_fetching is False, it means
            # the entire page was "Too New". We just proceed to the next page.
            pass

        if stop_fetching:
            print(f"[INFO] Reached profiles updated before {start_ts}. Stopping sync.")
            break

        # Pagination
        next_url = (response.json().get("links") or {}).get("next")
        if not next_url:
            break

        url, params = next_url, None
        page_index += 1

    elapsed = time.time() - start_time
    print(f"[INFO] Finished {target_date_str} window in {elapsed:.2f}s. Saved {total_profiles} profiles across {pages_saved} pages.")

    return {
        "pages_saved": pages_saved,
        "total_profiles": total_profiles,
        "run_date": target_date_str,
        "elapsed_seconds": elapsed,
        "s3_prefix": f"s3://{bucket_name}/{prefix}/",
    }

if __name__ == "__main__":
    # Local Testing
    # os.environ["KLAVIYO_PROFILES_BUCKET"] = "my-test-bucket"
    # handler({"date": "2024-01-01"})
    pass