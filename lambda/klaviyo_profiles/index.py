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
BACKFILL_QUEUE_URL = os.environ.get("KLAVIYO_PROFILES_BACKFILL_QUEUE_URL")
BACKFILL_GROUP_ID = "profiles-backfill"

_s3_client = boto3.client("s3")
_sqs_client = boto3.client("sqs")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# --- Helper Functions ---

def get_api_key(secret_name: str = SECRET_NAME, key_name: str = SECRET_KEY_NAME) -> str:
    """Fetch Klaviyo API key from Secrets Manager."""
    secrets_client = boto3.client("secretsmanager")
    try:
        r = secrets_client.get_secret_value(SecretId=secret_name)
        secret_str = r.get("SecretString")
        parsed = json.loads(secret_str)
        api_key = parsed.get(key_name) or parsed.get("api_key")
        if not api_key:
            raise RuntimeError("API key not found in secret")
        return api_key
    except Exception as e:
        logger.error(f"Error fetching Klaviyo secret '{secret_name}': {e}")
        raise


def make_klaviyo_session(api_key: str) -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=5, pool_maxsize=5, max_retries=0
    )
    session.mount("https://", adapter)
    session.headers.update(
        {
            "Authorization": f"Klaviyo-API-Key {api_key}",
            "accept": "application/json",
            "revision": API_REVISION,
        }
    )
    return session


def safe_get(session: requests.Session, url: str, params: Optional[Dict] = None) -> requests.Response:
    """Robust GET with Klaviyo-specific error logging for 400 errors."""
    attempt = 0
    max_retries = 5

    while True:
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", 10))
                logger.warning(f"Rate Limit (429). Waiting {retry_after}s...")
                time.sleep(retry_after + 1)
                continue

            if r.status_code == 400:
                logger.error(f"Klaviyo 400 Error Body: {r.text}")
                r.raise_for_status()

            r.raise_for_status()
            return r

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                # Permanent error, don't retry
                raise

            attempt += 1
            if attempt > max_retries:
                raise

            sleep_time = min(2 ** attempt, 30) + random.uniform(0, 1)
            logger.warning(f"HTTPError, retrying in {sleep_time:.2f}s (attempt {attempt})")
            time.sleep(sleep_time)

        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise

            sleep_time = min(2 ** attempt, 30) + random.uniform(0, 1)
            logger.warning(f"Request error {e}, retrying in {sleep_time:.2f}s (attempt {attempt})")
            time.sleep(sleep_time)


def parse_iso_to_utc(date_str: str) -> datetime:
    """
    Accepts BOTH ...Z and ...+00:00 etc, returns aware UTC datetime.
    """
    if date_str.endswith("Z"):
        date_str = date_str[:-1] + "+00:00"
    return datetime.fromisoformat(date_str).astimezone(timezone.utc)


def get_trigger_time(event: Dict[str, Any]) -> datetime:
    event_time = event.get("time")
    if isinstance(event_time, str):
        try:
            return parse_iso_to_utc(event_time)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def extract_backfill_window(event: Dict[str, Any]) -> Optional[Tuple[datetime, datetime]]:
    """
    Extract start/end for a 1-hour backfill window.

    Supports:
      - Direct invocation: { "start": "2025-02-22T15:00:00+00:00" }
      - SQS event: { "Records": [ { "body": "{\"start\":\"...\"}" } ] }
    """
    try:
        start_str = None

        # SQS shape
        if "Records" in event:
            record = event["Records"][0]
            body = record.get("body")
            if isinstance(body, str):
                payload = json.loads(body)
            elif isinstance(body, dict):
                payload = body
            else:
                payload = {}
            start_str = payload.get("start")
        else:
            # Direct Lambda invoke
            start_str = event.get("start")

        if not start_str:
            return None

        start_ts = parse_iso_to_utc(start_str)

        # Enforce exact 1-hour window aligned on the provided hour
        start_ts = start_ts.replace(minute=0, second=0, microsecond=0)
        end_ts = start_ts + timedelta(hours=1)
        return start_ts, end_ts
    except Exception as e:
        logger.warning(f"extract_backfill_window failed: {e}")
        return None


def enqueue_next_window(next_start: datetime) -> None:
    if not BACKFILL_QUEUE_URL:
        return

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    if next_start >= now:
        # Don't schedule beyond "current" hour
        return

    start_str = next_start.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    _sqs_client.send_message(
        QueueUrl=BACKFILL_QUEUE_URL,
        MessageBody=json.dumps({"start": start_str}),
        MessageGroupId=BACKFILL_GROUP_ID,
        MessageDeduplicationId=start_str.replace(":", "-").replace("+", "P"),
    )


def add_custom_columns(payload: Dict[str, Any]) -> None:
    records = payload.get("data")
    if not isinstance(records, list):
        return

    for record in records:
        attr = record.get("attributes") or {}
        props = attr.get("properties") or {}
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
    """
    Fetch all profiles updated in a strict 1-hour window.

    - If event has a 'start' (direct or via SQS), we use that exact hour.
    - Otherwise, we default to the *previous* full hour.

    Window is [H:00:00, H+1:00:00), implemented as:
      greater-than(updated, H:00:00 - 1s)
      AND
      less-than(updated, H+1:00:00)
    """

    # 1. Determine hour window
    backfill_win = extract_backfill_window(event)
    if backfill_win:
        start_ts, end_ts = backfill_win
    else:
        trigger_time = get_trigger_time(event)
        end_ts = trigger_time.replace(minute=0, second=0, microsecond=0)
        start_ts = end_ts - timedelta(hours=1)

    # Inclusive lower bound hack: > (start - 1 second)
    filter_lower_dt = start_ts - timedelta(seconds=1)
    filter_upper_dt = end_ts

    # Use Z in filter (matches Klaviyo examples)
    filter_lower = filter_lower_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    filter_upper = filter_upper_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # For logging / response
    window_start_str = start_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    window_end_str = end_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(f"[START] Window (logical): {window_start_str} to {window_end_str}")
    logger.info(
        f"[FILTER RAW] less-than(updated,{filter_upper}),"
        f"greater-than(updated,{filter_lower})"
    )

    api_key = get_api_key()
    session = make_klaviyo_session(api_key)
    url = "https://a.klaviyo.com/api/profiles/"

    # 2. Initial page params (1 Lambda = 1 hour)
    current_params = {
        "page[size]": str(PAGE_SIZE),
        "sort": "-updated",
        # less-than first, greater-than second, no quotes
        "filter": (
            f"less-than(updated,{filter_upper}),"
            f"greater-than(updated,{filter_lower})"
        ),
        "additional-fields[profile]": "subscriptions",
    }

    page_index = 1
    total_saved = 0

    # 3. Processing loop
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

        # S3 folder naming: 2025-06-21_15-00-00/
        folder = start_ts.strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"profiles/{folder}/page_{page_index}.json"

        save_payload(BUCKET_NAME, s3_key, payload)

        total_saved += len(records)
        logger.info(f"Page {page_index}: Saved {len(records)} records to {s3_key}.")

        next_link = (payload.get("links") or {}).get("next")
        if not next_link:
            break

        url = next_link
        current_params = None
        page_index += 1

    # 4. Enqueue next backfill hour if applicable
    #    (DISABLED: we only want one hour per Lambda invocation)
    # if backfill_win:
    #     enqueue_next_window(end_ts)

    return {
        "status": "completed",
        "window": f"{window_start_str}/{window_end_str}",
        "profiles_saved": total_saved,
    }
