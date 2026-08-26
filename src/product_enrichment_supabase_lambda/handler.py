"""
Product enrichment export Lambda (Supabase -> S3).

Every run takes a FULL snapshot of each configured table via the Supabase
REST API (PostgREST) and writes it as CSV to S3 under a date/time folder pair:

    s3://{bucket}/{prefix}/{YYYY-MM-DD}/{YYYY-MM-DD_HH:MM:SS}/{table}.csv

e.g. s3://shopify-products-metadata/supabase/2026-08-26/2026-08-26_06:00:00/products.csv
     s3://shopify-products-metadata/supabase/2026-08-26/2026-08-26_18:00:00/products.csv

The date folder is the UTC date of the run and the time folder is the UTC
timestamp of the actual invocation, so scheduled runs and manual test
invocations both land in their own time folder under the same day.
S3 has no real directories -- writing the key creates the whole hierarchy,
so "create the folder if it does not exist" happens for free.

Columns are never hardcoded. Every request is select=* and the CSV header is
built from the keys the API actually returns, so adding a column to a table
shows up in the next run with no code change and no redeploy.

Rows are paged out of PostgREST and written straight to a file on /tmp, then
multipart-uploaded, so memory stays flat regardless of table size.

No third-party dependencies: stdlib only, plus the boto3 that ships with the
Lambda runtime.
"""

import os
import csv
import json
import time
import logging
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DATE_FOLDER_FORMAT = "%Y-%m-%d"
# Same timestamp convention as the existing product-enrichment pipeline.
TIME_FOLDER_FORMAT = "%Y-%m-%d_%H:%M:%S"

REQUEST_TIMEOUT = 60      # seconds per HTTP request
MAX_ATTEMPTS = 4          # per request, with backoff


# ---------------------------------------------------------------------------
# JSON -> CSV value coercion
#
# Everything lands in the CSV as text. The rules below exist so that no value
# picks up an artefact on the way out:
#
#   null            -> empty field       (not "None", not "null")
#   true / false    -> true / false      (not Python's "True" / "False")
#   numbers         -> the exact digits Supabase sent. json.loads builds
#                      _RawNumber instead of int/float, so 12.50 stays
#                      "12.50" and 0.00001 never turns into "1e-05" via a
#                      float round-trip. Nested numbers survive too.
#   json / arrays   -> compact JSON      (not a Python repr with ' quotes)
#   text            -> unchanged
#
# The csv module then applies RFC4180 quoting, which is the one unavoidable
# transformation: a field containing a comma, quote or newline is wrapped in
# quotes and its own quotes doubled. Every CSV reader undoes that, so what a
# consumer reads back is identical to what Supabase returned.
# ---------------------------------------------------------------------------
class _RawNumber(str):
    """A JSON number held as its exact source text.

    json.loads is told to build these instead of int/float, which is what
    stops 12.50 collapsing to 12.5. They subclass str so a top-level numeric
    column needs no special handling, but _dump_json has to emit them
    unquoted or a nested {"n": 1} would come back out as {"n": "1"}.
    """


def _dump_json(value):
    """Compact JSON that preserves _RawNumber values as bare numbers.

    json.dumps cannot do this: it would quote them, since they are strings.
    """
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, _RawNumber):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ",".join(_dump_json(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ",".join(
            json.dumps(str(k), ensure_ascii=False) + ":" + _dump_json(v)
            for k, v in value.items()
        ) + "}"
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _to_text(value):
    if value is None:
        return ""
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):      # covers _RawNumber -- exact digits, as sent
        return value
    # dict / list -> compact JSON, no padding spaces, unicode preserved.
    return _dump_json(value)


class SupabaseRest:
    """Minimal PostgREST client over urllib."""

    def __init__(self, base_url, service_key, schema):
        self.base_url = base_url.rstrip("/") + "/rest/v1"
        self.headers = {
            "apikey": service_key,
            "Authorization": "Bearer " + service_key,
            "Accept": "application/json",
            # Required to read a schema other than `public`. The schema must
            # also be listed under Project Settings > API > Exposed schemas.
            "Accept-Profile": schema,
        }

    def get_columns_from_openapi(self, table):
        """Column names for a table straight from the API schema.

        Only used when a table is empty, where there is no row to read the
        column names off. PostgREST publishes them at the API root."""
        body, _ = self._request(self.base_url + "/", dict(self.headers))
        spec = json.loads(body)
        # PostgREST <=11 emits Swagger 2.0, 12+ emits OpenAPI 3.
        schemas = spec.get("definitions") or spec.get("components", {}).get("schemas", {})
        return list(schemas.get(table, {}).get("properties", {}).keys())

    def get_page(self, table, params, offset, limit, want_count=False):
        """Return (rows, total_or_None) for one page."""
        query = urllib.parse.urlencode(params, safe=".,*")
        url = "{}/{}?{}".format(self.base_url, urllib.parse.quote(table), query)

        headers = dict(self.headers)
        headers["Range-Unit"] = "items"
        headers["Range"] = "{}-{}".format(offset, offset + limit - 1)
        if want_count:
            headers["Prefer"] = "count=exact"

        body, content_range = self._request(url, headers)
        rows = json.loads(body, parse_float=_RawNumber, parse_int=_RawNumber)

        total = None
        if content_range and "/" in content_range:
            tail = content_range.rsplit("/", 1)[1]
            if tail.isdigit():
                total = int(tail)
        return rows, total

    def _request(self, url, headers):
        last_error = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            req = urllib.request.Request(url, headers=headers, method="GET")
            try:
                with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                    return resp.read().decode("utf-8"), resp.headers.get("Content-Range")
            except urllib.error.HTTPError as exc:
                # 416 means the offset is past the end of the table, which is
                # simply the end of pagination rather than a failure.
                if exc.code == 416:
                    return "[]", exc.headers.get("Content-Range")
                detail = exc.read().decode("utf-8", "replace")[:500]
                # 4xx other than 429 will not improve by retrying.
                if exc.code < 500 and exc.code != 429:
                    raise RuntimeError(
                        "HTTP {} from Supabase: {}".format(exc.code, detail)
                    ) from exc
                last_error = RuntimeError(
                    "HTTP {} from Supabase: {}".format(exc.code, detail)
                )
            except (urllib.error.URLError, TimeoutError) as exc:
                last_error = exc

            if attempt < MAX_ATTEMPTS:
                backoff = 2 ** attempt
                logger.warning(
                    "Request failed (attempt %d/%d): %s -- retrying in %ds",
                    attempt, MAX_ATTEMPTS, last_error, backoff,
                )
                time.sleep(backoff)

        raise RuntimeError(
            "Supabase request failed after {} attempts: {}".format(MAX_ATTEMPTS, last_error)
        )


def _probe_columns(client, table):
    """Paging without an explicit sort can repeat or skip rows between pages,
    so pick a stable column to order by: `id` when the table has one,
    otherwise its first column. Also returns the column list, which is the
    header fallback for an empty table."""
    rows, _ = client.get_page(table, {"select": "*"}, offset=0, limit=1)
    if not rows:
        # Empty table: no row to read column names off, so ask the API schema
        # instead. Keeps the header-only CSV a real header rather than a blank
        # line. Never fatal -- an empty header is better than a failed run.
        try:
            return None, client.get_columns_from_openapi(table)
        except Exception:
            logger.warning("Could not read columns for empty table %s", table, exc_info=True)
            return None, []
    columns = list(rows[0].keys())
    order_column = "id" if "id" in columns else columns[0]
    return order_column, columns


class _Utf8Reader:
    """Adapts a text-mode file to the binary read() interface boto3 wants."""

    def __init__(self, text_file):
        self._f = text_file

    def read(self, size=-1):
        chunk = self._f.read(size if size and size > 0 else -1)
        return chunk.encode("utf-8")


def _export_table(client, s3_client, bucket, key, table, page_size):
    order_column, probe_columns = _probe_columns(client, table)

    with tempfile.TemporaryFile(mode="w+", encoding="utf-8", newline="") as tmp:
        writer = None
        header = None
        total_written = 0
        reported_total = None
        offset = 0
        page_cap = None

        while True:
            params = {"select": "*"}
            if order_column:
                params["order"] = order_column + ".asc"

            rows, total = client.get_page(
                table, params, offset, page_size, want_count=(offset == 0)
            )
            if total is not None and reported_total is None:
                reported_total = total

            if not rows:
                break

            if writer is None:
                # Header comes from the data, so new columns need no code change.
                header = list(rows[0].keys())
                writer = csv.writer(tmp, lineterminator="\n")
                writer.writerow(header)

            for row in rows:
                unexpected = [k for k in row if k not in header]
                if unexpected:
                    logger.warning(
                        "Row in %s has columns absent from the header %s -- dropped. "
                        "Re-run to pick up a schema change.", table, unexpected,
                    )
                writer.writerow([_to_text(row.get(col)) for col in header])

            total_written += len(rows)

            # The server caps page size (Supabase defaults to 1000 rows), so
            # the real page size is whatever the first response returned.
            if page_cap is None:
                page_cap = len(rows)

            offset += len(rows)
            if reported_total is not None and offset >= reported_total:
                break
            if len(rows) < page_cap:
                break

        if writer is None:
            # Empty table. Fall back to the probe's column list so the run still
            # produces a header-only CSV and the folder layout stays identical
            # for every run instead of silently missing a file.
            logger.warning("Table %s returned no rows -- uploading header-only CSV", table)
            writer = csv.writer(tmp, lineterminator="\n")
            writer.writerow(probe_columns)

        if reported_total is not None and total_written != reported_total:
            raise RuntimeError(
                "Incomplete export of {}: wrote {} rows but the API reported {}".format(
                    table, total_written, reported_total
                )
            )

        tmp.flush()
        size = tmp.tell()
        tmp.seek(0)
        s3_client.upload_fileobj(
            _Utf8Reader(tmp),
            bucket,
            key,
            ExtraArgs={"ContentType": "text/csv"},
        )

    logger.info(
        "Uploaded %d rows (%d chars) from %s to s3://%s/%s",
        total_written, size, table, bucket, key,
    )
    return total_written


def main(event, context):
    utc_now = datetime.now(timezone.utc)
    date_folder = utc_now.strftime(DATE_FOLDER_FORMAT)
    time_folder = utc_now.strftime(TIME_FOLDER_FORMAT)

    bucket = os.environ["S3_BUCKET_NAME"]
    prefix = os.environ["S3_PREFIX"].strip("/")
    schema = os.environ["SUPABASE_SCHEMA"]
    tables = [t.strip() for t in os.environ["SUPABASE_TABLES"].split(",") if t.strip()]
    page_size = int(os.environ.get("PAGE_SIZE", "1000"))

    logger.info(
        "Run started at %s UTC -- exporting %s from schema '%s' to s3://%s/%s/%s/%s/",
        time_folder, tables, schema, bucket, prefix, date_folder, time_folder,
    )

    client = SupabaseRest(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_KEY"],
        schema,
    )
    s3_client = boto3.client("s3")

    results = {}
    failures = {}
    for table in tables:
        key = "{}/{}/{}/{}.csv".format(prefix, date_folder, time_folder, table)
        try:
            results[table] = _export_table(
                client, s3_client, bucket, key, table, page_size
            )
        except Exception as exc:
            # Keep going so one bad table does not block the others, then fail
            # the invocation at the end so the error is visible in CloudWatch
            # and EventBridge metrics.
            logger.exception("Failed exporting table %s", table)
            failures[table] = str(exc)

    summary = ", ".join("{}={}".format(t, n) for t, n in results.items())
    if failures:
        detail = ", ".join("{}: {}".format(t, e) for t, e in failures.items())
        raise RuntimeError(
            "Export at {} failed for {} table(s) -- {}. Succeeded: {}".format(
                time_folder, len(failures), detail, summary or "none"
            )
        )

    logger.info("Run complete: %s", summary)
    return {
        "statusCode": 200,
        "run_timestamp_utc": time_folder,
        "s3_folder": "s3://{}/{}/{}/{}/".format(bucket, prefix, date_folder, time_folder),
        "rows": results,
    }
