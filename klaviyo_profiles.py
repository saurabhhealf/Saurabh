#!/usr/bin/env python3
"""
Klaviyo profiles pull → Snowflake in batches.
Uses executemany with PARSE_JSON for proper VARIANT insertion.
"""

import os
import sys
import json
import time
import random
import base64
from typing import Dict, List

import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv()

# --------------------------
# CONFIG
# --------------------------
API_KEY = os.environ.get("KLAVIYO_API_KEY")
if not API_KEY:
    print("[ERROR] KLAVIYO_API_KEY env var is missing")
    sys.exit(1)

API_REVISION = "2025-10-15"

SNOWFLAKE_DB = "HEALF"
SNOWFLAKE_SCHEMA = "HEALF_BI"
SNOWFLAKE_TABLE = "KLAVIYO_PROFILES"

BATCH_SIZE = 2000


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


def write_profiles_batch_to_snowflake(conn, profiles: List[Dict]) -> int:
    """
    Upsert profiles into Snowflake using MERGE (no duplicates on ID).
    """
    if not profiles:
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

        for i in range(0, len(profiles), chunk_size):
            chunk = profiles[i:i + chunk_size]

            selects = []
            for p in chunk:
                selects.append(f"""
                    SELECT
                        {esc(p.get("ID"))} AS ID,
                        {esc(p.get("TYPE"))} AS TYPE,
                        {esc(p.get("EMAIL"))} AS EMAIL,
                        {esc(p.get("EXTERNAL_ID"))} AS EXTERNAL_ID,
                        {esc(p.get("UPDATED"))} AS UPDATED,
                        {esc(p.get("DATE_OF_BIRTH"))} AS DATE_OF_BIRTH,
                        {esc(p.get("LAST_FEMALE_CYCLE_STATUS"))} AS LAST_FEMALE_CYCLE_STATUS,
                        PARSE_JSON({esc_json(p.get("ATTRIBUTES"))}) AS ATTRIBUTES,
                        PARSE_JSON({esc_json(p.get("PROPERTIES"))}) AS PROPERTIES,
                        PARSE_JSON({esc_json(p.get("RELATIONSHIPS"))}) AS RELATIONSHIPS,
                        PARSE_JSON({esc_json(p.get("LINKS"))}) AS LINKS,
                        PARSE_JSON({esc_json(p.get("SEGMENTS"))}) AS SEGMENTS
                """)

            using_sql = " UNION ALL ".join(selects)

            merge_sql = f"""
                MERGE INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} t
                USING (
                    {using_sql}
                ) s
                ON t.ID = s.ID
                WHEN MATCHED THEN UPDATE SET
                    TYPE = s.TYPE,
                    EMAIL = s.EMAIL,
                    EXTERNAL_ID = s.EXTERNAL_ID,
                    UPDATED = s.UPDATED,
                    DATE_OF_BIRTH = s.DATE_OF_BIRTH,
                    LAST_FEMALE_CYCLE_STATUS = s.LAST_FEMALE_CYCLE_STATUS,
                    ATTRIBUTES = s.ATTRIBUTES,
                    PROPERTIES = s.PROPERTIES,
                    RELATIONSHIPS = s.RELATIONSHIPS,
                    LINKS = s.LINKS,
                    SEGMENTS = s.SEGMENTS
                WHEN NOT MATCHED THEN INSERT (
                    ID, TYPE, EMAIL, EXTERNAL_ID, UPDATED, DATE_OF_BIRTH, LAST_FEMALE_CYCLE_STATUS,
                    ATTRIBUTES, PROPERTIES, RELATIONSHIPS, LINKS, SEGMENTS
                ) VALUES (
                    s.ID, s.TYPE, s.EMAIL, s.EXTERNAL_ID, s.UPDATED, s.DATE_OF_BIRTH, s.LAST_FEMALE_CYCLE_STATUS,
                    s.ATTRIBUTES, s.PROPERTIES, s.RELATIONSHIPS, s.LINKS, s.SEGMENTS
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

        resp.raise_for_status()
        return resp


# --------------------------
# NORMALIZE
# --------------------------
def normalize_profile(record: Dict) -> Dict:
    attributes = record.get("attributes", {}) or {}
    relationships = record.get("relationships", {}) or {}
    properties = attributes.get("properties", {}) or {}
    return {
        "ID": record.get("id"),
        "TYPE": record.get("type"),
        "EMAIL": attributes.get("email"),
        "EXTERNAL_ID": attributes.get("external_id"),
        "UPDATED": attributes.get("updated"),
        "DATE_OF_BIRTH": properties.get("Date_of_Birth"),
        "LAST_FEMALE_CYCLE_STATUS": properties.get("Last_Female_Cycle_Status"),
        "ATTRIBUTES": attributes,
        "PROPERTIES": properties,
        "RELATIONSHIPS": relationships,
        "LINKS": record.get("links", {}) or {},
        "SEGMENTS": relationships.get("segments", {}) or {},
    }


# --------------------------
# MAIN
# --------------------------
def main():
    PAGE_SIZE = 100

    session = make_session()
    conn = get_snowflake_connection()

    url = "https://a.klaviyo.com/api/profiles/"
    params = {
        "page[size]": str(PAGE_SIZE),
        "sort": "updated",
        "fields[profile]": "email,external_id,properties,updated,created,subscriptions",
    }

    buffer: List[Dict] = []
    seen_ids: set = set()
    total_inserted = 0

    try:
        while True:
            resp = safe_get(session, url, params=params, timeout=(10, 180))
            payload = resp.json()
            data = payload.get("data", []) or []

            if not data:
                print("[INFO] No more profiles; finishing.")
                break

            for item in data:
                rec = normalize_profile(item)
                pid = rec.get("ID")
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)
                buffer.append(rec)

                if len(buffer) >= BATCH_SIZE:
                    inserted = write_profiles_batch_to_snowflake(conn, buffer)
                    total_inserted += inserted
                    print(f"[INFO] Batch inserted {inserted} (total={total_inserted})")
                    buffer.clear()
                    

            next_url = (payload.get("links") or {}).get("next")
            if not next_url:
                break
            url, params = next_url, None

        if buffer:
            inserted = write_profiles_batch_to_snowflake(conn, buffer)
            total_inserted += inserted
            print(f"[INFO] Final batch inserted {inserted}")

        print(f"[DONE] Inserted total {total_inserted} rows")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
