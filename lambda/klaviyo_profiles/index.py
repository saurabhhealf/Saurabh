import base64
import json
import os
import random
import time
from typing import Any, Dict, Optional

import boto3
import requests

API_REVISION = "2025-10-15"
PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 60)  # (connect_timeout, read_timeout)
CHAIN_MAX_RETRIES = 3
CUSTOM_PROFILE_PROPERTIES = {
    "DATE_OF_BIRTH": "Date_of_Birth",
    "LAST_FEMALE_CYCLE_STATUS": "Last_Female_Cycle_Status",
}
SECRET_NAME = "klaviyo"
SECRET_KEY_NAME = "API_KEY"

_secrets_client = boto3.client("secretsmanager")
_s3_client = boto3.client("s3")
_lambda_client = boto3.client("lambda")


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


def enqueue_next_invocation(
    *,
    next_url: str,
    page_index: int,
    run_date: str,
    secret_name: str,
    secret_key: str,
) -> bool:
    """
    Fire-and-forget invoke so the next page runs in a fresh Lambda.
    Returns True once the invoke API accepts the payload.
    """
    target_name = (
        os.environ.get("KLAVIYO_PROFILES_FUNCTION_NAME")
        or os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
    )
    if not target_name:
        print("[INFO] AWS_LAMBDA_FUNCTION_NAME is not set; skipping automatic chaining.")
        return False

    payload = {
        "next_url": next_url,
        "page_index": page_index,
        "run_date": run_date,
        "secret_name": secret_name,
        "secret_key": secret_key,
    }

    attempt = 0
    while True:
        try:
            _lambda_client.invoke(
                FunctionName=target_name,
                InvocationType="Event",
                Payload=json.dumps(payload).encode("utf-8"),
            )
            print(f"[INFO] Triggered next invocation for page {page_index} via {target_name}.")
            return True
        except Exception as exc:
            attempt += 1
            if attempt > CHAIN_MAX_RETRIES:
                raise RuntimeError(
                    f"Failed to enqueue page {page_index} after {attempt} attempts"
                ) from exc
            sleep_seconds = min(2 ** attempt, 30) + random.uniform(0, 1)
            print(
                f"[WARN] Unable to enqueue page {page_index}: {exc}. Retrying in {sleep_seconds:.1f}s"
            )
            time.sleep(sleep_seconds)


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
    One Lambda invocation = ONE Klaviyo page.

    Input (event):
      - next_url (optional): Klaviyo 'next' link from previous page
      - page_index (optional): which page number to write (default 1)
      - run_date (optional): snapshot date; kept constant across the whole run
      - secret_name/secret_key (optional): override the default secret lookup

    Output:
      - done: bool (True when there is NO next page)
      - next_url: str | None (pass this into the next invocation)
      - page_index: int (the next page index to use)
      - run_date: str (YYYY-MM-DD)
      - pages_saved: int (always 1 unless there was no data)
      - s3_prefix: str
      - next_invocation_triggered: bool (True when we queued the subsequent Lambda)
    """
    event = event or {}

    bucket_name = os.environ.get("KLAVIYO_PROFILES_BUCKET")
    if not bucket_name:
        raise ValueError("Missing KLAVIYO_PROFILES_BUCKET environment variable")

    # One export run == one date prefix
    run_date = event.get("run_date") or time.strftime("%Y-%m-%d")
    prefix = f"profiles/{run_date}"

    secret_name = event.get("secret_name") or SECRET_NAME
    secret_key = event.get("secret_key") or SECRET_KEY_NAME

    api_key = get_api_key(secret_name, secret_key).strip()
    if not api_key:
        raise ValueError(f"Secret {secret_name} did not return an API key")
    session = make_session(api_key)

    # Decide which URL to hit
    url_from_event = event.get("next_url")
    if url_from_event:
        # Continue from previous page
        url = url_from_event
        params: Optional[Dict[str, Any]] = None  # full URL already includes query string
    else:
        # First page
        url = "https://a.klaviyo.com/api/profiles/"
        params = {
            "page[size]": str(PAGE_SIZE),
            "sort": "updated",
            "additional-fields[profile]": "subscriptions",
        }

    page_index = int(event.get("page_index", 1))

    # EXACTLY ONE REQUEST = ONE PAGE
    response = safe_get(session, url, params=params, timeout=REQUEST_TIMEOUT)
    payload = response.json()
    add_custom_columns(payload)

    data = payload.get("data") or []
    if not data:
        print("[INFO] No profiles returned on this page; finishing.")
        return {
            "done": True,
            "next_url": None,
            "page_index": page_index,  # unchanged
            "pages_saved": 0,
            "run_date": run_date,
            "s3_prefix": f"s3://{bucket_name}/{prefix}/",
            "next_invocation_triggered": False,
        }

    key = save_payload(bucket_name, prefix, page_index, payload)
    print(f"[INFO] Saved page {page_index} ({len(data)} profiles) to s3://{bucket_name}/{key}")

    next_url = (payload.get("links") or {}).get("next")
    next_invocation_triggered = False

    if next_url:
        next_invocation_triggered = enqueue_next_invocation(
            next_url=next_url,
            page_index=page_index + 1,
            run_date=run_date,
            secret_name=secret_name,
            secret_key=secret_key,
        )

    # This output is shaped so the **next** invocation can just receive it as input.
    return {
        "done": not bool(next_url),
        "next_url": next_url,
        "page_index": page_index + 1,
        "pages_saved": 1,
        "run_date": run_date,
        "s3_prefix": f"s3://{bucket_name}/{prefix}/",
        "next_invocation_triggered": next_invocation_triggered,
    }


if __name__ == "__main__":
    result = handler()
    print(
        f"[DONE] Saved {result['pages_saved']} pages to {result['s3_prefix']}, "
        f"page_index={result['page_index']}, done={result['done']}"
    )
