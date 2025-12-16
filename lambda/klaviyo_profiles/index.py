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
REQUEST_TIMEOUT = (10, 180)

_secrets_client = boto3.client("secretsmanager")
_s3_client = boto3.client("s3")


def get_api_key(secret_name: str) -> str:
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


def save_payload(bucket_name: str, prefix: str, page_index: int, payload: Dict[str, Any]) -> str:
    key = f"{prefix}/page_{page_index}.json"
    body = json.dumps(payload).encode("utf-8")
    _s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
    return key


def handler(event: Optional[Dict[str, Any]] = None, _context: Any = None) -> Dict[str, Any]:
    event = event or {}

    bucket_name = event.get("bucket") or os.environ.get("KLAVIYO_PROFILES_BUCKET")
    if not bucket_name:
        raise ValueError("Missing KLAVIYO_PROFILES_BUCKET environment variable")

    secret_name = event.get("secret_name") or os.environ.get("KLAVIYO_API_SECRET_NAME")
    if not secret_name:
        raise ValueError("Missing KLAVIYO_API_SECRET_NAME environment variable")

    prefix = (
        event.get("prefix")
        or os.environ.get("KLAVIYO_PROFILES_PREFIX")
        or f"profiles/{time.strftime('%Y-%m-%d')}"
    )

    api_key = get_api_key(secret_name).strip()
    if not api_key:
        raise ValueError(f"Secret {secret_name} did not return an API key")

    session = make_session(api_key)
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
        "s3_prefix": f"s3://{bucket_name}/{prefix}/",
    }


def main():
    result = handler({}, None)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
