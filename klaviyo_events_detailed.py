#!/usr/bin/env python3
"""
Klaviyo events (detailed) pull → Snowflake in batches.
Fetches events with related metric/profile/attribution data and upserts into Snowflake.
Deduped via MERGE on ID.
Supports configurable START/END datetimes via variables.
"""

import os
import sys
import json
import time
import random
import base64
from typing import Dict, List, Tuple

import boto3
import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv()

# --------------------------
# CONFIG
# --------------------------
SECRET_NAME = "klaviyo"
SECRET_KEY_NAME = "API_KEY"

_secrets_client = boto3.client("secretsmanager")


def get_klaviyo_api_key(secret_name: str = SECRET_NAME, key_name: str = SECRET_KEY_NAME) -> str:
    """Load the Klaviyo API key from AWS Secrets Manager."""
    try:
        resp = _secrets_client.get_secret_value(SecretId=secret_name)
    except _secrets_client.exceptions.ResourceNotFoundException as exc:
        raise ValueError(f"Secret {secret_name} not found") from exc

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


API_KEY = get_klaviyo_api_key().strip()
if not API_KEY:
    print(f"[ERROR] Secret {SECRET_NAME} did not return an API key")
    sys.exit(1)

API_REVISION = "2025-10-15"  # Klaviyo API revision

SNOWFLAKE_DB = "HEALF"
SNOWFLAKE_SCHEMA = "HEALF_BI"
SNOWFLAKE_TABLE = "KLAVIYO_EVENTS_DETAILED"

BATCH_SIZE = 2000  # how many events to accumulate before writing

# --------------------------
# DATE WINDOW (EDIT THESE)
# --------------------------
# Use RFC3339 format, e.g. "2024-01-01T00:00:00Z"
START_DATETIME = "2025-01-01T00:00:00Z"   # REQUIRED
END_DATETIME   = None                     # OPTIONAL, e.g. "2025-12-31T23:59:59Z"


# --------------------------
# SNOWFLAKE
# --------------------------
def _get_private_key_bytes() -> bytes:
    key_base64 = os.environ["SNOWFLAKE_PRIVATE_KEY"]
    key_bytes = base64.b64decode(key_base64)
    p_key = serialization.load_pem_private_key(key_bytes, password=None)
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
        private_key=_get_private_key_bytes(),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def write_events_batch_to_snowflake(conn, events: List[Dict]) -> int:
    """
    Upsert events into Snowflake using MERGE (no duplicates on ID).
    """
    if not events:
        return 0

    cursor = conn.cursor()

    try:
        def esc(v):
            if v is None:
                return "NULL"
            return "'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'"

        def esc_json(v):
            j = json.dumps(v or {}, ensure_ascii=False).replace("\\", "\\\\").replace("'", "''")
            return f"'{j}'"

        chunk_size = 100
        total = 0

        for i in range(0, len(events), chunk_size):
            chunk = events[i:i + chunk_size]

            selects = []
            for e in chunk:
                selects.append(f"""
                    SELECT
                        {esc(e.get("ID"))}                  AS ID,
                        {esc(e.get("TYPE"))}                AS TYPE,
                        {esc(e.get("DATETIME"))}            AS DATETIME,
                        {esc(e.get("TIMESTAMP_NUM"))}       AS TIMESTAMP_NUM,
                        {esc(e.get("UUID"))}                AS UUID,
                        {esc(e.get("METRIC_ID"))}           AS METRIC_ID,
                        {esc(e.get("PROFILE_ID"))}          AS PROFILE_ID,
                        PARSE_JSON({esc_json(e.get("EVENT_PROPERTIES"))}) AS EVENT_PROPERTIES,
                        PARSE_JSON({esc_json(e.get("ATTRIBUTES"))})       AS ATTRIBUTES,
                        PARSE_JSON({esc_json(e.get("RELATIONSHIPS"))})    AS RELATIONSHIPS,
                        PARSE_JSON({esc_json(e.get("LINKS"))})            AS LINKS,
                        PARSE_JSON({esc_json(e.get("INCLUDED"))})         AS INCLUDED,
                        CURRENT_TIMESTAMP()                 AS LOAD_TS
                """)

            using_sql = " UNION ALL ".join(selects)

            merge_sql = f"""
                MERGE INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} t
                USING (
                    {using_sql}
                ) s
                ON t.ID = s.ID
                WHEN MATCHED THEN UPDATE SET
                    TYPE             = s.TYPE,
                    DATETIME         = s.DATETIME,
                    TIMESTAMP_NUM    = s.TIMESTAMP_NUM,
                    UUID             = s.UUID,
                    METRIC_ID        = s.METRIC_ID,
                    PROFILE_ID       = s.PROFILE_ID,
                    EVENT_PROPERTIES = s.EVENT_PROPERTIES,
                    ATTRIBUTES       = s.ATTRIBUTES,
                    RELATIONSHIPS    = s.RELATIONSHIPS,
                    LINKS            = s.LINKS,
                    INCLUDED         = s.INCLUDED,
                    LOAD_TS          = s.LOAD_TS
                WHEN NOT MATCHED THEN INSERT (
                    ID, TYPE, DATETIME, TIMESTAMP_NUM, UUID,
                    METRIC_ID, PROFILE_ID,
                    EVENT_PROPERTIES, ATTRIBUTES, RELATIONSHIPS, LINKS, INCLUDED, LOAD_TS
                ) VALUES (
                    s.ID, s.TYPE, s.DATETIME, s.TIMESTAMP_NUM, s.UUID,
                    s.METRIC_ID, s.PROFILE_ID,
                    s.EVENT_PROPERTIES, s.ATTRIBUTES, s.RELATIONSHIPS, s.LINKS, s.INCLUDED, s.LOAD_TS
                );
            """

            cursor.execute(merge_sql)
            total += len(chunk)

        return total

    finally:
        cursor.close()


# --------------------------
# HTTP HELPERS
# --------------------------
def make_session(pool_maxsize: int = 20) -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=pool_maxsize,
        pool_maxsize=pool_maxsize,
        max_retries=0,
    )
    s.mount("https://", adapter)
    s.headers.update({
        "Authorization": f"Klaviyo-API-Key {API_KEY}",
        "accept": "application/json",
        "revision": API_REVISION,
        "Connection": "keep-alive",
    })
    return s


def safe_get(session, url, params=None, max_retries=7, timeout=(10, 150)):
    attempt = 0
    while True:
        try:
            resp = session.get(url, params=params, timeout=timeout)
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError) as e:
            attempt += 1
            if attempt > max_retries:
                raise
            sleep_s = min(2 ** attempt, 60) + random.uniform(0, 1)
            print(f"[WARN] Network issue: {e}. Retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue

        if resp.status_code in (429, 500, 502, 503, 504):
            attempt += 1
            if attempt > max_retries:
                resp.raise_for_status()
            sleep_s = min(2 ** attempt, 60) + random.uniform(0, 1)
            print(f"[WARN] HTTP {resp.status_code}. Retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue

        # Handle 400 errors with more info
        if resp.status_code == 400:
            print(f"[ERROR] 400 Bad Request. Response body: {resp.text}")
            resp.raise_for_status()

        resp.raise_for_status()
        return resp


# --------------------------
# NORMALIZE EVENTS
# --------------------------
def build_included_index(included_list: List[Dict]) -> Dict[Tuple[str, str], Dict]:
    """
    Build index on (type, id) from top-level 'included' array.
    """
    idx = {}
    for item in included_list or []:
        t = item.get("type")
        i = item.get("id")
        if t and i:
            idx[(t, i)] = item
    return idx


def normalize_event(record: Dict, included_index: Dict[Tuple[str, str], Dict]) -> Dict:
    """
    Flatten an event into a detailed row.
    """
    attributes = record.get("attributes", {}) or {}
    relationships = record.get("relationships", {}) or {}
    links = record.get("links", {}) or {}

    # Basic fields
    event_id = record.get("id")
    event_type = record.get("type")

    # Attributes: datetime, timestamp, uuid, event_properties
    datetime_val = attributes.get("datetime")
    timestamp_num = attributes.get("timestamp")
    uuid = attributes.get("uuid")
    event_properties = attributes.get("event_properties") or attributes.get("properties") or {}

    # Relationships → metric/profile/attributions
    profile_rel = relationships.get("profile") or {}
    metric_rel = relationships.get("metric") or {}
    attributions_rel = relationships.get("attributions") or {}

    profile_data = (profile_rel.get("data") or {}) or {}
    metric_data = (metric_rel.get("data") or {}) or {}
    attributions_data = attributions_rel.get("data") or []

    profile_id = profile_data.get("id")
    metric_id = metric_data.get("id")

    # Pull included objects for more detail
    included_profile = included_index.get(("profile", profile_id)) if profile_id else None
    included_metric = included_index.get(("metric", metric_id)) if metric_id else None

    included_attributions = []
    for a in attributions_data:
        t = a.get("type")
        i = a.get("id")
        if t and i:
            full = included_index.get((t, i))
            included_attributions.append(full or a)

    # Package included bundle
    included_bundle = {
        "profile": included_profile,
        "metric": included_metric,
        "attributions": included_attributions,
    }

    return {
        "ID": event_id,
        "TYPE": event_type,
        "DATETIME": datetime_val,
        "TIMESTAMP_NUM": timestamp_num,
        "UUID": uuid,
        "METRIC_ID": metric_id,
        "PROFILE_ID": profile_id,
        "EVENT_PROPERTIES": event_properties,
        "ATTRIBUTES": attributes,
        "RELATIONSHIPS": relationships,
        "LINKS": links,
        "INCLUDED": included_bundle,
    }


# --------------------------
# MAIN
# --------------------------
def main():
    PAGE_SIZE = 200  # max per docs for Get Events

    conn = get_snowflake_connection()
    session = make_session()

    # ----- Build filter string -----
    # Klaviyo filter syntax: greater-or-equal(datetime,2024-01-01T00:00:00Z)
    # NO quotes around the datetime value!
    filter_parts = []

    if START_DATETIME:
        filter_parts.append(f"greater-or-equal(datetime,{START_DATETIME})")

    if END_DATETIME:
        filter_parts.append(f"less-than(datetime,{END_DATETIME})")

    if len(filter_parts) == 2:
        # Combine with and()
        filter_str = f"and({filter_parts[0]},{filter_parts[1]})"
    elif len(filter_parts) == 1:
        filter_str = filter_parts[0]
    else:
        filter_str = None

    if filter_str:
        print(f"[INFO] Using filter: {filter_str}")
    else:
        print("[INFO] No datetime filter applied; full range.")

    url = "https://a.klaviyo.com/api/events"

    params = {
        "page[size]": str(PAGE_SIZE),
        "sort": "-datetime",                      # newest first
        "include": "metric,profile,attributions", # get all related objects
    }
    if filter_str:
        params["filter"] = filter_str

    buffer: List[Dict] = []
    seen_ids: set = set()
    total_upserted = 0

    try:
        while True:
            resp = safe_get(session, url, params=params, timeout=(10, 180))
            payload = resp.json()

            data = payload.get("data", []) or []
            included = payload.get("included", []) or []
            included_index = build_included_index(included)

            if not data:
                print("[INFO] No more events; finishing.")
                break

            for item in data:
                event_id = item.get("id")
                if not event_id or event_id in seen_ids:
                    continue

                seen_ids.add(event_id)
                rec = normalize_event(item, included_index)
                buffer.append(rec)

                if len(buffer) >= BATCH_SIZE:
                    upserted = write_events_batch_to_snowflake(conn, buffer)
                    total_upserted += upserted
                    print(f"[INFO] Batch upserted {upserted} events (total={total_upserted})")
                    buffer.clear()

            next_url = (payload.get("links") or {}).get("next")
            if not next_url:
                break
            # 'next' already encodes cursor & params; on subsequent calls, don't send old params
            url, params = next_url, None

        if buffer:
            upserted = write_events_batch_to_snowflake(conn, buffer)
            total_upserted += upserted
            print(f"[INFO] Final batch upserted {upserted} events")

        print(f"[DONE] Upserted total {total_upserted} events")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
