import base64
import json
import os
import random
import time
from datetime import datetime
from typing import Any, Dict, Optional

import boto3
import requests

# --- Configuration ---
API_REVISION = "2025-10-15"
PAGE_SIZE = 100
MAX_PAGES_PER_RUN = 50  # Stop after 50 pages and restart to prevent timeout
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
_lambda_client = boto3.client("lambda")


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


def invoke_next_lambda(function_name: str, next_url: str, next_page_index: int, run_id: str):
    """
    Trigger the SAME Lambda asynchronously to continue the job.
    """
    payload = {
        "next_url": next_url,
        "page_index": next_page_index,
        "run_id": run_id
    }
    print(f"[RECURSION] Invoking self for page {next_page_index}...")
    _lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event',  # Asynchronous - fire and forget
        Payload=json.dumps(payload)
    )


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    # 1. Setup Context
    run_id = event.get("run_id")
    if not run_id:
        # UPDATED: Name is just UTC timestamp now (e.g. "2025-12-17_14-30-00")
        run_id = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        print(f"[START] Starting NEW full profile dump. Run ID: {run_id}")

    # 2. Determine Start Position
    url = event.get("next_url") or "https://a.klaviyo.com/api/profiles/"
    page_index = event.get("page_index") or 1
    
    # Params are only needed for the very first call.
    params = None
    if not event.get("next_url"):
        params = {
            "page[size]": str(PAGE_SIZE),
            "sort": "-updated", 
            "additional-fields[profile]": "subscriptions"
        }

    # 3. Initialize Connections
    if not BUCKET_NAME:
        raise ValueError("Environment variable KLAVIYO_PROFILES_BUCKET is missing.")
        
    api_key = get_api_key()
    session = make_session(api_key)
    pages_processed_this_run = 0

    print(f"[INFO] Processing Run '{run_id}' | Starting at Page {page_index}")

    # 4. Processing Loop
    while True:
        # Check Limits
        if pages_processed_this_run >= MAX_PAGES_PER_RUN:
            print(f"[LIMIT] Reached {MAX_PAGES_PER_RUN} pages. Recursing...")
            invoke_next_lambda(context.function_name, url, page_index, run_id)
            return {"status": "continued", "next_page": page_index}

        # Fetch
        response = safe_get(session, url, params=params)
        payload = response.json()
        
        # Flatten
        add_custom_columns(payload)
        data = payload.get("data") or []

        if not data:
            print("[FINISH] No more data returned. Job complete.")
            break

        # Save
        s3_key = f"profiles/{run_id}/page_{page_index}.json"
        save_payload(BUCKET_NAME, s3_key, payload)
        print(f"[SAVE] Saved {len(data)} profiles to {s3_key}")

        # Advance
        pages_processed_this_run += 1
        page_index += 1
        
        # Next Link
        next_link = (payload.get("links") or {}).get("next")
        if not next_link:
            print("[FINISH] No 'next' link provided. Job complete.")
            break
        
        url = next_link
        params = None 

    return {"status": "completed", "total_pages": page_index, "run_id": run_id}