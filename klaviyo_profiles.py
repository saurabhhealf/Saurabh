#!/usr/bin/env python3
"""
Fetch Klaviyo profiles and write each page to S3.

- No Snowflake writes; raw payload per page.
- No incremental window or deduping; downstream tooling handles that.
- Relies on env vars (e.g., GitHub Secrets) for configuration:
    * KLAVIYO_API_KEY
    * KLAVIYO_PROFILES_BUCKET
    * KLAVIYO_PROFILES_PREFIX (optional; defaults to profiles/YYYY-MM-DD)
"""

import json
import os
import random
import time
from typing import Any, Dict, Optional

import boto3
import requests

API_REVISION = "2025-10-15"
PAGE_SIZE = 100
REQUEST_TIMEOUT = (10, 180)
CUSTOM_PROFILE_PROPERTIES = {
    "DATE_OF_BIRTH": "Date_of_Birth",
    "LAST_FEMALE_CYCLE_STATUS": "Last_Female_Cycle_Status",
}

_s3_client = boto3.client("s3")


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


def save_payload(bucket_name: str, prefix: str, page_index: int, payload: Dict[str, Any]) -> str:
    key = f"{prefix}/page_{page_index}.json"
    body = json.dumps(payload).encode("utf-8")
    _s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
    return key


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


def fetch_profiles_to_s3(bucket_name: str, prefix: str) -> Dict[str, Any]:
    api_key = os.environ.get("KLAVIYO_API_KEY")
    if not api_key:
        raise ValueError("Missing KLAVIYO_API_KEY environment variable")

    session = make_session(api_key.strip())
    url = "https://a.klaviyo.com/api/profiles/"
    params: Dict[str, Any] = {
        "page[size]": str(PAGE_SIZE),
        "sort": "updated",
        "fields[profile]": "email,external_id,properties,updated,created,subscriptions",
    }

    pages_saved = 0
    page_index = 1

    while True:
        response = safe_get(session, url, params=params, timeout=REQUEST_TIMEOUT)
        payload = response.json()
        add_custom_columns(payload)

        data = payload.get("data") or []
        if not data:
            print("[INFO] No more profiles; finishing pagination.")
            break

        key = save_payload(bucket_name, prefix, page_index, payload)
        print(f"[INFO] Saved page {page_index} ({len(data)} profiles) to s3://{bucket_name}/{key}")
        pages_saved += 1

        next_url = (payload.get("links") or {}).get("next")
        if not next_url:
            break

        url, params = next_url, None
        page_index += 1

    return {
        "pages_saved": pages_saved,
        "s3_prefix": f"s3://{bucket_name}/{prefix}/"
    }


def handler(event: Optional[Dict[str, Any]] = None, _context: Any = None) -> Dict[str, Any]:
    event = event or {}

    bucket_name = event.get("bucket") or os.environ.get("KLAVIYO_PROFILES_BUCKET")
    if not bucket_name:
        raise ValueError("Missing KLAVIYO_PROFILES_BUCKET environment variable")

    prefix = (
        event.get("prefix")
        or os.environ.get("KLAVIYO_PROFILES_PREFIX")
        or f"profiles/{time.strftime('%Y-%m-%d')}"
    )

    return fetch_profiles_to_s3(bucket_name, prefix)


if __name__ == "__main__":
    result = handler()
    print(f"[DONE] Saved {result['pages_saved']} pages to {result['s3_prefix']}")
