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
API_REVISION = "2025-10-15"
PAGE_SIZE = 100
MAX_PAGES_SAFETY_LIMIT = 70
REQUEST_TIMEOUT = (10, 60)

CUSTOM_PROFILE_PROPERTIES = {
    "DATE_OF_BIRTH": "Date_of_Birth",
    "LAST_FEMALE_CYCLE_STATUS": "Last_Female_Cycle_Status",
}

SECRET_NAME = "klaviyo"
SECRET_KEY_NAME = "API_KEY"
BUCKET_NAME = os.environ.get("KLAVIYO_PROFILES_BUCKET")

_s3_client = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Helper Functions ---
def get_api_key() -> str:
    secrets_client = boto3.client("secretsmanager")
    try:
        r = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret_str = r["SecretString"]
        parsed = json.loads(secret_str)
        api_key = parsed.get(SECRET_KEY_NAME) or parsed.get("api_key")
        if not api_key:
            raise ValueError("API key not found in secret")
        return api_key
    except Exception as e:
        logger.error(f"Failed to get API key: {e}")
        raise

def make_klaviyo_session(api_key: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Klaviyo-API-Key {api_key}",
        "accept": "application/json",
        "revision": API_REVISION,
    })
    return session

def safe_get(session: requests.Session, url: str, params: Optional[Dict] = None) -> requests.Response:
    attempt = 0
    max_retries = 5
    while True:
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", 10))
                logger.warning(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after + 1)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                logger.error(f"Request failed after {max_retries} retries: {e}")
                raise
            sleep_time = min(2 ** attempt, 30) + random.uniform(0, 1)
            logger.warning(f"Request error, retry {attempt} in {sleep_time:.1f}s: {e}")
            time.sleep(sleep_time)

def parse_iso_to_utc(date_str: str) -> datetime:
    if date_str.endswith("Z"):
        date_str = date_str[:-1] + "+00:00"
    return datetime.fromisoformat(date_str).astimezone(timezone.utc)

def extract_backfill_window(event: Dict[str, Any]) -> Optional[Tuple[datetime, datetime]]:
    """Extract EXACT 1-hour window from event. Returns None if no start time."""
    try:
        start_str = None
        
        # If triggered via SQS
        if "Records" in event:
            record = event["Records"][0]
            body_str = record.get("body")
            body = json.loads(body_str) if isinstance(body_str, str) else body_str
            start_str = body.get("start")
        else:
            # Direct invocation
            start_str = event.get("start")

        if not start_str:
            logger.warning("No 'start' parameter found in event")
            return None

        # Parse and enforce exact hour boundaries
        start_ts = parse_iso_to_utc(start_str)
        start_ts = start_ts.replace(minute=0, second=0, microsecond=0)
        end_ts = start_ts + timedelta(hours=1)
        
        return start_ts, end_ts
    except Exception as e:
        logger.error(f"Failed to extract window: {e}")
        return None

def add_custom_columns(payload: Dict[str, Any]) -> None:
    records = payload.get("data") or []
    for record in records:
        props = (record.get("attributes") or {}).get("properties") or {}
        for col, key in CUSTOM_PROFILE_PROPERTIES.items():
            record[col] = props.get(key)

def save_payload(key: str, data: Dict) -> None:
    if not BUCKET_NAME:
        raise ValueError("KLAVIYO_PROFILES_BUCKET environment variable not set")
    _s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
    )

# --- Main Handler ---
def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Fetch profiles for ONE SPECIFIC HOUR and stop."""
    logger.info(f"Event received: {json.dumps(event)}")
    
    # 1. Get the EXACT 1-hour window
    window = extract_backfill_window(event)
    if not window:
        # If no window specified, return error - don't run
        error_msg = "No 'start' parameter provided. Example: {'start': '2025-06-21T15:00:00+00:00'}"
        logger.error(error_msg)
        return {
            "status": "error",
            "message": error_msg,
            "profiles_saved": 0
        }
    
    start_ts, end_ts = window
    logger.info(f"[EXACT WINDOW] Processing: {start_ts} to {end_ts}")
    
    # 2. Setup API parameters for THIS HOUR ONLY
    filter_lower = (start_ts - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    filter_upper = end_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    api_key = get_api_key()
    session = make_klaviyo_session(api_key)
    
    url = "https://a.klaviyo.com/api/profiles/"
    current_params = {
        "page[size]": str(PAGE_SIZE),
        "sort": "-updated",
        "filter": f"less-than(updated,{filter_upper}),greater-than(updated,{filter_lower})",
        "additional-fields[profile]": "subscriptions",
    }
    
    page_index = 1
    total_saved = 0
    folder = start_ts.strftime("%Y-%m-%d_%H-%M-%S")
    
    # 3. Paginate through ALL profiles in this hour
    while url and page_index <= MAX_PAGES_SAFETY_LIMIT:
        response = safe_get(session, url, params=current_params)
        payload = response.json()
        
        add_custom_columns(payload)
        records = payload.get("data") or []
        
        if not records:
            logger.info(f"No more records found for {start_ts}")
            break
        
        # Save to S3
        s3_key = f"profiles/{folder}/page_{page_index}.json"
        save_payload(s3_key, payload)
        total_saved += len(records)
        logger.info(f"Saved {len(records)} records to {s3_key}")
        
        # Get next page link
        next_link = (payload.get("links") or {}).get("next")
        if not next_link:
            logger.info(f"Reached last page for {start_ts}")
            break
        
        # Move to next page
        url = next_link
        current_params = None  # Next link includes params
        page_index += 1
    
    if page_index > MAX_PAGES_SAFETY_LIMIT:
        logger.warning(f"Stopped at safety limit of {MAX_PAGES_SAFETY_LIMIT} pages")
    
    # 4. STOP COMPLETELY - No SQS calls, no recursion
    logger.info(f"[COMPLETE] Processed {total_saved} profiles for {start_ts}. Function stopping.")
    
    return {
        "status": "completed",
        "window": f"{start_ts.isoformat()}/{end_ts.isoformat()}",
        "profiles_saved": total_saved,
        "s3_folder": folder,
        "note": "Function stopped - no recursion"
    }