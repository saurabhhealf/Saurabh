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
# Safety Limit: 50 pages = 5,000 profiles/minute. 
# This is plenty for "last 1 minute" unless you have massive spikes.
MAX_PAGES_SAFETY_LIMIT = 50 
REQUEST_TIMEOUT = (10, 60)

# Custom fields to flatten
CUSTOM_PROFILE_PROPERTIES = {
    "DATE_OF_BIRTH": "Date_of_Birth",
    "LAST_FEMALE_CYCLE_STATUS": "Last_Female_Cycle_Status",
}

# Constants
SECRET_NAME = "klaviyo"
SECRET_KEY_NAME = "API_KEY"
BUCKET_NAME = os.environ.get("KLAVIYO_PROFILES_BUCKET")
BACKFILL_QUEUE_URL = os.environ.get("KLAVIYO_PROFILES_BACKFILL_QUEUE_URL")
BACKFILL_GROUP_ID = "profiles-backfill"

_secrets_client = boto3.client("secretsmanager")
_s3_client = boto3.client("s3")
_sqs_client = boto3.client("sqs")


def get_api_key(secret_name: str = SECRET_NAME, key_name: str = SECRET_KEY_NAME) -> str:
    """Fetch API Key from AWS Secrets Manager."""
    r = _secrets_client.get_secret_value(SecretId=secret_name)
    secret = r.get("SecretString")
    if secret is None:
        binary_secret = r.get("SecretBinary")
        if binary_secret is None:
            raise ValueError(f"Secret {secret_name} is empty.")
        secret = base64.b64decode(binary_secret).decode("utf-8")

    try:
        parsed = json.loads(secret)
        if isinstance(parsed, dict):
            return parsed.get(key_name) or parsed.get("api_key")
    except json.JSONDecodeError:
        pass
    return secret


def make_session(api_key: str) -> requests.Session:
    """Create a re-useable HTTP session with standard headers."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=0)
    session.mount("https://", adapter)
    session.headers.update({
        "Authorization": f"Klaviyo-API-Key {api_key}",
        "accept": "application/json",
        "revision": API_REVISION,
    })
    return session


def safe_get(session: requests.Session, url: str, params: Optional[Dict] = None) -> requests.Response:
    """
    Robust GET with logic to handle Klaviyo's specific 429 Rate Limits.
    """
    attempt = 0
    max_retries = 5
    
    while True:
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except Exception as e:
            attempt += 1
            if attempt > max_retries: raise
            sleep_time = min(2 ** attempt, 30) + random.uniform(0, 1)
            print(f"[WARN] Network error: {e}. Retrying in {sleep_time:.2f}s...")
            time.sleep(sleep_time)
            continue

        # Handle Rate Limits (429) specifically using the Retry-After header
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 10))
            print(f"[WARN] Rate Limit Hit (429). Sleeping for {retry_after} seconds...")
            time.sleep(retry_after + 1) # Add 1s buffer
            continue

        # Handle other temporary server errors
        if r.status_code in [500, 502, 503, 504]:
            attempt += 1
            if attempt > max_retries: r.raise_for_status()
            sleep_time = min(2 ** attempt, 30)
            print(f"[WARN] Server Error {r.status_code}. Retrying in {sleep_time}s...")
            time.sleep(sleep_time)
            continue
        
        r.raise_for_status()
        return r


def add_custom_columns(payload: Dict[str, Any]) -> None:
    """Helper: Pulls specific nested properties to the top level."""
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


def save_payload(bucket: str, key: str, data: Dict) -> None:
    _s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data).encode("utf-8"))


def parse_klaviyo_date(date_str: str) -> datetime:
    """Helper to parse ISO strings like '2025-12-16T12:00:00+00:00' or '...Z' to UTC"""
    if date_str.endswith('Z'):
        date_str = date_str[:-1] + '+00:00'
    dt = datetime.fromisoformat(date_str)
    return dt.astimezone(timezone.utc)


def parse_iso_to_utc(date_str: str) -> datetime:
    """Parse generic ISO string to UTC datetime."""
    if date_str.endswith("Z"):
        date_str = date_str[:-1] + "+00:00"
    return datetime.fromisoformat(date_str).astimezone(timezone.utc)


def get_trigger_time(event: Dict[str, Any]) -> datetime:
    """Return the event trigger time in UTC, falling back to now."""
    event_time = event.get("time")
    if isinstance(event_time, str):
        try:
            if event_time.endswith("Z"):
                event_time = event_time[:-1] + "+00:00"
            return datetime.fromisoformat(event_time).astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def extract_backfill_window(event: Dict[str, Any]) -> Optional[Tuple[datetime, datetime]]:
    """
    If invoked via SQS backfill, return an explicit (start, end) window.
    The queue body should be JSON with a `start` ISO timestamp.
    """
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        return None

    record = records[0]
    if record.get("eventSource") != "aws:sqs":
        return None

    body = record.get("body")
    if not isinstance(body, str):
        return None

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None

    start_raw = payload.get("start")
    if not isinstance(start_raw, str):
        return None

    try:
        start_ts = parse_iso_to_utc(start_raw)
    except ValueError:
        return None

    end_ts = start_ts + timedelta(hours=1)
    return start_ts, end_ts


def enqueue_next_window(next_start: datetime) -> None:
    """Send the next hour to the backfill queue if we have room to run."""
    if not BACKFILL_QUEUE_URL:
        print("[INFO] Backfill queue URL not configured; skipping enqueue.")
        return

    latest_full_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    if next_start >= latest_full_hour:
        print(f"[INFO] Reached latest full hour ({latest_full_hour}); stopping backfill enqueue.")
        return

    body = json.dumps({"start": next_start.isoformat()})
    _sqs_client.send_message(
        QueueUrl=BACKFILL_QUEUE_URL,
        MessageBody=body,
        MessageGroupId=BACKFILL_GROUP_ID,
        MessageDeduplicationId=body,  # deterministic de-dupe for the same window
    )
    print(f"[ENQUEUE] Scheduled next backfill window starting at {next_start}")


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # 1. Define Window: either backfill window from SQS or the previous full hour
    backfill_window = extract_backfill_window(event)
    if backfill_window:
        start_ts, end_ts = backfill_window
        print(f"[INFO] Backfill window requested via SQS: {start_ts} -> {end_ts}")
    else:
        trigger_time = get_trigger_time(event)
        end_ts = trigger_time.replace(minute=0, second=0, microsecond=0)
        start_ts = end_ts - timedelta(hours=1)
    
    print(f"[START] Fetching profiles updated between {start_ts} and {end_ts}")

    # 2. Setup
    if not BUCKET_NAME:
        raise ValueError("Environment variable KLAVIYO_PROFILES_BUCKET is missing.")
        
    api_key = get_api_key()
    session = make_session(api_key)

    url = "https://a.klaviyo.com/api/profiles/"
    params = {
        "page[size]": str(PAGE_SIZE),
        "sort": "-updated", # Newest first
        "additional-fields[profile]": "subscriptions"
    }

    page_index = 1
    total_profiles_saved = 0
    stop_fetching = False
    
    # Increase safety limit for large windows (e.g. backfill)
    MAX_PAGES_SAFETY_LIMIT = 1000 

    # 3. Processing Loop
    print('enter our processing loop')
    while True:
        if page_index > MAX_PAGES_SAFETY_LIMIT:
            print(f"[LIMIT] Hit safety limit of {MAX_PAGES_SAFETY_LIMIT} pages. Stopping.")
            break

        response = safe_get(session, url, params=params)
        payload = response.json()
        
        add_custom_columns(payload)
        raw_data = payload.get("data") or []

        if not raw_data:
            print("[FINISH] No more data returned by API.")
            break

        valid_records = []

        print('filter loop')
        for record in raw_data:
            updated_str = record.get("attributes", {}).get("updated")
            if not updated_str:
                continue

            try:
                record_dt = parse_klaviyo_date(updated_str)
            except ValueError:
                continue

            if record_dt > end_ts:
                continue

            # Stop once we drop below the window start
            if record_dt < start_ts:
                stop_fetching = True
                break
            
            valid_records.append(record)

        # 5. Save Valid Records
        if valid_records:
            payload["data"] = valid_records
            if "links" in payload: del payload["links"] 
            
            s3_key = f"profiles/{end_ts.strftime("%Y-%m-%d_%H-%M-%S")}/page_{page_index}.json"
            save_payload(BUCKET_NAME, s3_key, payload)
            
            count = len(valid_records)
            total_profiles_saved += count
            print(f"[SAVE] Saved {count} profiles to {s3_key}")
        else:
            print(f"[INFO] No valid profiles found on page {page_index}.")
        
        if stop_fetching:
            print(f"[INFO] Reached the end of our time window ({start_ts}). Stopping.")
            break

        # 6. Pagination
        next_link = (response.json().get("links") or {}).get("next")
        if not next_link:
            break
        
        url = next_link
        params = None 
        page_index += 1
        
        time.sleep(0.1)

    if backfill_window:
        enqueue_next_window(end_ts)

    return {
        "status": "completed", 
        "profiles_saved": total_profiles_saved, 
    }
