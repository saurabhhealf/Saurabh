import base64
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import boto3
import requests

# --- Configuration ---
# API revision for the 2025 stability
API_REVISION = "2025-10-15"
PAGE_SIZE = 100
# Safety Limit: 70 pages = 7,000 profiles per Lambda execution to prevent timeout.
MAX_PAGES_SAFETY_LIMIT = 70 
REQUEST_TIMEOUT = (10, 60)

# Custom fields to flatten into the top level of the JSON records
CUSTOM_PROFILE_PROPERTIES = {
    "DATE_OF_BIRTH": "Date_of_Birth",
    "LAST_FEMALE_CYCLE_STATUS": "Last_Female_Cycle_Status",
}

# AWS Environment Variables & Constants
SECRET_NAME = "klaviyo"
SECRET_KEY_NAME = "API_KEY"
BUCKET_NAME = os.environ.get("KLAVIYO_PROFILES_BUCKET")
BACKFILL_QUEUE_URL = os.environ.get("KLAVIYO_PROFILES_BACKFILL_QUEUE_URL")
BACKFILL_GROUP_ID = "profiles-backfill"

# Global Clients for Connection Reuse
_s3_client = boto3.client("s3")
_sqs_client = boto3.client("sqs")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Helper Functions ---

def get_api_key(secret_name: str = SECRET_NAME, key_name: str = SECRET_KEY_NAME) -> str:
    """Fetch API Key from AWS Secrets Manager."""
    secrets_client = boto3.client("secretsmanager")
    try:
        r = secrets_client.get_secret_value(SecretId=secret_name)
        secret = r.get("SecretString")
        parsed = json.loads(secret)
        # Check both potential key names in the secret dictionary
        return parsed.get(key_name) or parsed.get("api_key")
    except Exception as e:
        logger.error(f"Error fetching secret: {e}")
        return secret

def make_klaviyo_session(api_key: str) -> requests.Session:
    """Create a re-useable HTTP session with standard Klaviyo headers."""
    session = requests.Session()
    # Pool connections for performance within the Lambda
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=0)
    session.mount("https://", adapter)
    session.headers.update({
        "Authorization": f"Klaviyo-API-Key {api_key}",
        "accept": "application/json",
        "revision": API_REVISION,
    })
    return session

def safe_get(session: requests.Session, url: str, params: Optional[Dict] = None) -> requests.Response:
    """Robust GET with exponential backoff and Klaviyo 429 Rate Limit handling."""
    attempt = 0
    max_retries = 5
    while True:
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            
            # Handle Klaviyo Rate Limits (429) specifically
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", 10))
                logger.warning(f"Rate Limit (429). Waiting {retry_after}s...")
                time.sleep(retry_after + 1)
                continue
                
            r.raise_for_status()
            return r
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(f"Max retries reached for URL {url}")
                raise
            sleep_time = min(2 ** attempt, 30) + random.uniform(0, 1)
            logger.warning(f"Network error: {e}. Retrying in {sleep_time:.2f}s...")
            time.sleep(sleep_time)

def add_custom_columns(payload: Dict[str, Any]) -> None:
    """Extracts nested custom properties to the top level of each record."""
    records = payload.get("data")
    if not isinstance(records, list):
        return
    for record in records:
        attributes = record.get("attributes") or {}
        properties = attributes.get("properties") or {}
        for column_name, property_key in CUSTOM_PROFILE_PROPERTIES.items():
            record[column_name] = properties.get(property_key)

def save_payload(bucket: str, key: str, data: Dict) -> None:
    """Uploads a dictionary as a JSON file to S3."""
    _s3_client.put_object(
        Bucket=bucket, 
        Key=key, 
        Body=json.dumps(data).encode("utf-8")
    )

def parse_iso_to_utc(date_str: str) -> datetime:
    """Standardizes ISO timestamps (including 'Z' format) to UTC datetimes."""
    if date_str.endswith("Z"):
        date_str = date_str[:-1] + "+00:00"
    return datetime.fromisoformat(date_str).astimezone(timezone.utc)

def get_trigger_time(event: Dict[str, Any]) -> datetime:
    """Extracts the trigger time from the event or defaults to now."""
    event_time = event.get("time")
    if isinstance(event_time, str):
        try:
            return parse_iso_to_utc(event_time)
        except ValueError:
            pass
    return datetime.now(timezone.utc)

def extract_backfill_window(event: Dict[str, Any]) -> Optional[Tuple[datetime, datetime]]:
    """Determines if the Lambda was triggered via SQS for a backfill window."""
    try:
        record = event.get("Records")[0]
        if record.get("eventSource") != "aws:sqs":
            return None
        payload = json.loads(record["body"])
        start_raw = payload.get("start")
        if not start_raw:
            return None
        start_ts = parse_iso_to_utc(start_raw)
        # Returns exactly 1 hour window: [start, start + 1 hour)
        return start_ts, start_ts + timedelta(hours=1)
    except (TypeError, IndexError, KeyError, AttributeError, json.JSONDecodeError):
        return None

def enqueue_next_window(next_start: datetime) -> None:
    """Enqueues the next hour for backfilling until the current time is reached."""
    if not BACKFILL_QUEUE_URL:
        logger.info("No BACKFILL_QUEUE_URL found; skipping enqueue.")
        return
        
    # Boundary: Don't schedule an hour that hasn't finished yet
    latest_full_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    if next_start >= latest_full_hour:
        logger.info(f"Backfill reached current time boundary ({latest_full_hour}). Stopping.")
        return
    
    start_str = next_start.strftime('%Y-%m-%dT%H:%M:%SZ')
    _sqs_client.send_message(
        QueueUrl=BACKFILL_QUEUE_URL,
        MessageBody=json.dumps({"start": start_str}),
        MessageGroupId=BACKFILL_GROUP_ID,
        # Unique ID prevents duplicate processing of the same hour window
        MessageDeduplicationId=start_str.replace(":", "-")
    )
    logger.info(f"[ENQUEUE] Scheduled next hour: {start_str}")

# --- Main Lambda Handler ---

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # 1. Determine the Time Window (Strict 1 Hour)
    backfill_win = extract_backfill_window(event)
    if backfill_win:
        start_ts, end_ts = backfill_win
    else:
        # Standard Trigger: Process the previous full clock hour
        trigger_time = get_trigger_time(event)
        end_ts = trigger_time.replace(minute=0, second=0, microsecond=0)
        start_ts = end_ts - timedelta(hours=1)
    
    start_str = start_ts.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str = end_ts.strftime('%Y-%m-%dT%H:%M:%SZ')
    logger.info(f"[START] Processing window: {start_str} to {end_str}")

    api_key = get_api_key()
    session = make_klaviyo_session(api_key)
    
    # Base endpoint for profiles
    url = "https://a.klaviyo.com/api/profiles/"
    
    # 2. Initial parameters for the first page
    # Using 'and()' operator for strict bounding
    current_params = {
        "page[size]": str(PAGE_SIZE),
        "sort": "-updated", 
        "filter": f"and(greater-or-equal(updated,{start_str}),less-than(updated,{end_str}))",
        "additional-fields[profile]": "subscriptions"
    }

    page_index = 1
    total_saved = 0

    # 3. Processing Loop with Cursor-based Pagination
    while True:
        # Prevent infinite loops or excessive execution time
        if page_index > MAX_PAGES_SAFETY_LIMIT:
            logger.warning(f"Hit safety limit of {MAX_PAGES_SAFETY_LIMIT} pages. Stopping.")
            break 

        # Call API
        # IMPORTANT: current_params is passed on page 1 only to avoid 400 Bad Request.
        # Pagination 'next' links already contain the filter and sort query.
        response = safe_get(session, url, params=current_params)
        payload = response.json()
        
        add_custom_columns(payload)
        records = payload.get("data") or []
        
        if not records:
            logger.info("No records found for this time window.")
            break

        # 4. Save to S3 (Partitioned by Date/Hour for easy querying)
        folder = start_ts.strftime('%Y-%m-%d/%H')
        s3_key = f"profiles/{folder}/page_{page_index}.json"
        save_payload(BUCKET_NAME, s3_key, payload)
        
        total_saved += len(records)
        logger.info(f"Page {page_index}: Saved {len(records)} records to {s3_key}")

        # 5. Determine if there is a Next Page
        next_link = (payload.get("links") or {}).get("next")
        if not next_link:
            logger.info("No more pages in this window.")
            break
        
        # Prepare for the next loop iteration
        url = next_link
        current_params = None  # Crucial fix for 400 error: reset params for next_link
        page_index += 1

    # 6. Chain the next hour if this was a backfill task
    if backfill_win:
        enqueue_next_window(end_ts)

    return {
        "status": "completed", 
        "processed_window": f"{start_str} to {end_str}",
        "profiles_saved": total_saved,
        "s3_path": f"s3://{BUCKET_NAME}/profiles/{start_ts.strftime('%Y-%m-%d/%H')}/"
    }