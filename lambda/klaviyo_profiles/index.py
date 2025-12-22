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
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Helper Functions ---

def get_api_key() -> str:
    """Fetch Klaviyo API key from Secrets Manager."""
    secrets_client = boto3.client("secretsmanager")
    try:
        r = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret_str = r.get("SecretString")
        parsed = json.loads(secret_str)
        return parsed.get(SECRET_KEY_NAME) or parsed.get("api_key")
    except Exception as e:
        logger.error(f"Error fetching Klaviyo secret: {e}")
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
    while True:
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 10)) + 1
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            attempt += 1
            if attempt > 5: raise
            time.sleep(min(2 ** attempt, 30) + random.uniform(0, 1))

def parse_iso_to_utc(date_str: str) -> datetime:
    if date_str.endswith("Z"):
        date_str = date_str[:-1] + "+00:00"
    return datetime.fromisoformat(date_str).astimezone(timezone.utc)

def extract_backfill_window(event: Dict[str, Any]) -> Optional[Tuple[datetime, datetime]]:
    start_str = event.get("start")
    if not start_str and "Records" in event:
        body = json.loads(event["Records"][0]["body"]) if isinstance(event["Records"][0]["body"], str) else event["Records"][0]["body"]
        start_str = body.get("start")
    
    if not start_str: return None

    start_ts = parse_iso_to_utc(start_str).replace(minute=0, second=0, microsecond=0)
    return start_ts, start_ts + timedelta(hours=1)

def add_custom_columns(payload: Dict[str, Any]) -> None:
    records = payload.get("data") or []
    for record in records:
        props = (record.get("attributes") or {}).get("properties") or {}
        for col, key in CUSTOM_PROFILE_PROPERTIES.items():
            record[col] = props.get(key)

def save_payload(bucket: str, key: str, data: Dict) -> None:
    _s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
    )

# --- Main Handler ---

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # 1. Determine exact 1-hour window
    window = extract_backfill_window(event)
    if window:
        start_ts, end_ts = window
    else:
        # Default fallback to previous hour
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        end_ts = now
        start_ts = now - timedelta(hours=1)

    filter_lower = (start_ts - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    filter_upper = end_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    logger.info(f"[START] Window: {start_ts} to {end_ts}")

    # 2. Setup Session and Initial Parameters
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

    # 3. Pagination Loop
    while True:
        if page_index > MAX_PAGES_SAFETY_LIMIT:
            logger.warning("Safety limit hit.")
            break

        response = safe_get(session, url, params=current_params)
        payload = response.json()
        
        add_custom_columns(payload)
        records = payload.get("data") or []
        
        if not records:
            break

        # Save to S3
        folder = start_ts.strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"profiles/{folder}/page_{page_index}.json"
        save_payload(BUCKET_NAME, s3_key, payload)

        total_saved += len(records)
        logger.info(f"Page {page_index}: Saved {len(records)} records.")

        # Check for next page
        next_link = (payload.get("links") or {}).get("next")
        if not next_link:
            break

        # Prepare for next iteration
        url = next_link
        current_params = None  # Klaviyo next links include the params already
        page_index += 1

    # 4. Final Return (No SQS/Recursion)
    return {
        "status": "completed",
        "window": f"{start_ts.isoformat()}/{end_ts.isoformat()}",
        "profiles_saved": total_saved,
    }