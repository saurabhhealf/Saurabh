import base64
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import boto3
import requests

# --- Configuration ---
API_REVISION = "2025-10-15"
PAGE_SIZE = 100
# We only want 1 minute of data, so we likely won't need many pages. 
# But we keep a safety limit just in case.
MAX_PAGES_SAFETY_LIMIT = 20  
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

_secrets_client = boto3.client("secretsmanager")
_s3_client = boto3.client("s3")


def get_api_key(secret_name: str = SECRET_NAME, key_name: str = SECRET_KEY_NAME) -> str:
    """Fetch API Key from AWS Secrets Manager."""
    resp = _secrets_client.get_secret_value(SecretId=secret_name)
    secret = resp.get("SecretString")
    if secret is None:
        binary_secret = resp.get("SecretBinary")
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
    """Create a reuseable HTTP session with standard headers."""
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
    """Get with retry logic for network blips."""
    attempt = 0
    while True:
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except Exception as e:
            attempt += 1
            if attempt > 4: raise
            print(f"[WARN] Network error: {e}. Retrying...")
            time.sleep(2 ** attempt)
            continue

        if resp.status_code in [429, 500, 502, 503, 504]:
            attempt += 1
            if attempt > 4: resp.raise_for_status()
            print(f"[WARN] HTTP {resp.status_code}. Retrying...")
            time.sleep(2 ** attempt)
            continue
        
        resp.raise_for_status()
        return resp


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


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # 1. Define "Last Minute" Window
    # End = Now (UTC)
    # Start = Now - 1 Minute
    end_ts = datetime.now(timezone.utc)
    start_ts = end_ts - timedelta(minutes=1)
    
    # Run ID is simply the current timestamp
    run_id = end_ts.strftime("%Y-%m-%d_%H-%M-%S")
    
    print(f"[START] Fetching profiles updated between {start_ts} and {end_ts}")
    print(f"[INFO] Run ID: {run_id}")

    # 2. Setup
    if not BUCKET_NAME:
        raise ValueError("Environment variable KLAVIYO_PROFILES_BUCKET is missing.")
        
    api_key = get_api_key()
    session = make_session(api_key)

    url = "https://a.klaviyo.com/api/profiles/"
    params = {
        "page[size]": str(PAGE_SIZE),
        "sort": "-updated", # Newest first is critical for this strategy
        "additional-fields[profile]": "subscriptions"
    }

    page_index = 1
    total_profiles_saved = 0
    stop_fetching = False

    # 3. Processing Loop
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

        # 4. Filter Loop
        for record in raw_data:
            updated_str = record.get("attributes", {}).get("updated")
            if not updated_str:
                continue

            try:
                record_dt = parse_klaviyo_date(updated_str)
            except ValueError:
                continue

            # If record is newer than our "end_ts" (technically impossible if end_ts is NOW, 
            # but good safety if clock skew exists), we skip it but keep scanning.
            if record_dt > end_ts:
                continue

            # CRITICAL CHECK:
            # If we see a record older than 1 minute ago, we STOP completely.
            # Because the list is sorted by newest, everyone after this is also too old.
            if record_dt < start_ts:
                stop_fetching = True
                break
            
            # If we are here, the record is within the last minute
            valid_records.append(record)

        # 5. Save Valid Records
        if valid_records:
            # Only save the filtered list
            payload["data"] = valid_records
            if "links" in payload: del payload["links"] # Clean up metadata
            
            s3_key = f"profiles/{run_id}/page_{page_index}.json"
            save_payload(BUCKET_NAME, s3_key, payload)
            
            count = len(valid_records)
            total_profiles_saved += count
            print(f"[SAVE] Saved {count} profiles to {s3_key}")
        
        if stop_fetching:
            print(f"[INFO] Reached profiles older than 1 minute ({start_ts}). Stopping.")
            break

        # 6. Pagination
        next_link = (response.json().get("links") or {}).get("next")
        if not next_link:
            break
        
        url = next_link
        params = None 
        page_index += 1

    return {
        "status": "completed", 
        "profiles_saved": total_profiles_saved, 
        "run_id": run_id
    }