import base64
import json
import os
import random
import time
from typing import Any, Dict, Optional

import boto3
import requests

API_REVISION = "2025-10-15"
PAGE_SIZE = 200
REQUEST_TIMEOUT = (10, 180)

_secrets_client = boto3.client("secretsmanager")
_s3_client = boto3.client("s3")


def get_api_key(secret_name: str) -> str:
    """Pull the Klaviyo API key from AWS Secrets Manager."""
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
            # Handle both {"api_key": "..."} and {"KLAVIYO_API_KEY": "..."}
            return parsed.get("api_key") or parsed.get("KLAVIYO_API_KEY") or secret
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


def safe_get(session: requests.Session,
             url: str,
             params: Optional[Dict[str, Any]] = None,
             max_retries: int = 7,
             timeout: tuple = (10, 150)) -> requests.Response:
    attempt = 0
    while True:
        try:
            response = session.get(url, params=params, timeout=timeout)
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError) as exc:
            attempt += 1
            if attempt > max_retries:
                raise
            sleep_seconds = min(2 ** attempt, 60) + random.uniform(0, 1)
            print(f"[WARN] Network issue: {exc}. Retrying in {sleep_seconds:.1f}s")
            time.sleep(sleep_seconds)
            continue

        if response.status_code in (429, 500, 502, 503, 504):
            attempt += 1
            if attempt > max_retries:
                response.raise_for_status()
            sleep_seconds = min(2 ** attempt, 60) + random.uniform(0, 1)
            print(f"[WARN] HTTP {response.status_code}. Retrying in {sleep_seconds:.1f}s")
            time.sleep(sleep_seconds)
            continue

        if response.status_code == 400:
            print(f"[ERROR] 400 Bad Request. Response body: {response.text}")
            response.raise_for_status()

        response.raise_for_status()
        return response


def build_filter(start_datetime: Optional[str], end_datetime: Optional[str]) -> Optional[str]:
    filter_parts = []

    if start_datetime:
        filter_parts.append(f"greater-or-equal(datetime,{start_datetime})")

    if end_datetime:
        filter_parts.append(f"less-than(datetime,{end_datetime})")

    if len(filter_parts) == 2:
        return f"and({filter_parts[0]},{filter_parts[1]})"
    if filter_parts:
        return filter_parts[0]
    return None


def save_payload(bucket_name: str, date_prefix: str, page_index: int, payload: Dict[str, Any]) -> None:
    key = f"{date_prefix}/page_{page_index}.json"
    body = json.dumps(payload).encode("utf-8")
    _s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)


def extract_event_value(event: Dict[str, Any], key: str) -> Optional[str]:
    if key in event:
        return event[key]
    detail = event.get("detail")
    if isinstance(detail, dict):
        return detail.get(key)
    return None


def handler(event: Dict[str, Any], _context) -> Dict[str, Any]:
    bucket_name = os.environ.get("KLAVIYO_EVENTS_BUCKET")
    if not bucket_name:
        raise ValueError("Missing KLAVIYO_EVENTS_BUCKET environment variable")

    secret_name = os.environ.get("KLAVIYO_API_SECRET_NAME")
    if not secret_name:
        raise ValueError("Missing KLAVIYO_API_SECRET_NAME environment variable")

    start_datetime = extract_event_value(event, "start_datetime")
    if not start_datetime:
        raise ValueError("Event payload must include 'start_datetime'")

    end_datetime = extract_event_value(event, "end_datetime")
    date_prefix = start_datetime.split("T")[0]

    api_key = get_api_key(secret_name).strip()
    if not api_key:
        raise ValueError(f"Secret {secret_name} did not return an API key")

    session = make_session(api_key)
    filter_str = build_filter(start_datetime, end_datetime)

    url = "https://a.klaviyo.com/api/events"
    params: Dict[str, Any] = {
        "page[size]": str(PAGE_SIZE),
        "sort": "-datetime",
        "include": "metric,profile,attributions",
    }
    if filter_str:
        params["filter"] = filter_str

    pages_saved = 0
    page_index = 1

    while True:
        response = safe_get(session, url, params=params, timeout=REQUEST_TIMEOUT)
        payload = response.json()

        data = payload.get("data") or []
        if not data:
            print("[INFO] No more events; finishing pagination.")
            break

        save_payload(bucket_name, date_prefix, page_index, payload)
        print(f"[INFO] Saved page {page_index} ({len(data)} events) to s3://{bucket_name}/{date_prefix}/page_{page_index}.json")
        pages_saved += 1

        next_url = (payload.get("links") or {}).get("next")
        if not next_url:
            break

        url, params = next_url, None
        page_index += 1

    return {
        "pages_saved": pages_saved,
        "s3_prefix": f"s3://{bucket_name}/{date_prefix}/"
    }
