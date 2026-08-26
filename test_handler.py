"""Integration test: run the real handler against a fake PostgREST server.

Rows are held as RAW JSON TEXT, not Python objects, because that is what
PostgREST actually puts on the wire -- Postgres serialises numeric straight
to its own digits, so `12.50` stays `12.50`. Round-tripping through Python
floats in the fake server would corrupt the values before the handler ever
sees them and would be testing the wrong thing.
"""
import io, os, sys, json, csv, threading, http.server, urllib.parse
from unittest import mock

sys.path.insert(0, r"d:\shopify\Supabase\src\product_enrichment_supabase_lambda")

SERVER_PAGE_CAP = 3          # force multi-page paging (real Supabase caps at 1000)
SERVICE_KEY = "test-service-role-key"
SCHEMA = "product_enrichment"

# (id, raw JSON text) -- exactly the bytes PostgREST would emit.
PRODUCTS = [
    (1, r'{"id":1,"name":"Plain","price":12.50,"live":true,"meta":null,"tags":["a","b"]}'),
    (2, r'{"id":2,"name":"He said \"hi\"","price":0.00001,"live":false,"meta":{"k":"v"},"tags":[]}'),
    (3, r'{"id":3,"name":"comma, inside","price":100000000000000000000,"live":null,"meta":null,"tags":null}'),
    (4, r'{"id":4,"name":"line\nbreak","price":0.00,"live":true,"meta":{"n":1},"tags":["x"]}'),
    (5, r'{"id":5,"name":"unicode \u00fcn\u00ef","price":9.90,"live":false,"meta":null,"tags":[]}'),
    (6, r'{"id":6,"name":"","price":null,"live":true,"meta":null,"tags":[]}'),
    (7, r'{"id":7,"name":"trailing space  ","price":3.00,"live":false,"meta":null,"tags":["z"]}'),
]
INGREDIENTS = []             # empty table -> exercises the OpenAPI header path
INGREDIENT_COLUMNS = ["id", "ingredient_name", "allergen"]

TABLES = {"products": PRODUCTS, "ingredients": INGREDIENTS}
OPENAPI = {
    "swagger": "2.0",
    "definitions": {
        "products":    {"properties": {c: {} for c in
                                       ["id", "name", "price", "live", "meta", "tags"]}},
        "ingredients": {"properties": {c: {} for c in INGREDIENT_COLUMNS}},
    },
}
requests_seen = []


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body_bytes, extra_headers=()):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        for k, v in extra_headers:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body_bytes)))
        self.end_headers()
        self.wfile.write(body_bytes)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)

        assert self.headers.get("apikey") == SERVICE_KEY, "missing/bad apikey header"
        assert self.headers.get("Authorization") == "Bearer " + SERVICE_KEY
        assert self.headers.get("Accept-Profile") == SCHEMA, "missing Accept-Profile"

        # API root -> OpenAPI/Swagger spec
        if parsed.path.rstrip("/").endswith("/rest/v1"):
            requests_seen.append(("<openapi>", None, None, None))
            self._send(200, json.dumps(OPENAPI).encode("utf-8"))
            return

        table = parsed.path.rsplit("/", 1)[-1]
        rng = self.headers.get("Range", "0-999")
        start, end = (int(x) for x in rng.split("-"))
        requests_seen.append((table, rng, qs.get("order", [None])[0],
                              self.headers.get("Prefer")))

        rows = TABLES.get(table)
        if rows is None:
            self.send_error(404); return

        ordered = sorted(rows, key=lambda r: r[0])
        total = len(ordered)

        if total and start >= total:                      # past the end
            self.send_response(416)
            self.send_header("Content-Range", "*/%d" % total)
            self.end_headers(); return

        page = ordered[start:min(end + 1, start + SERVER_PAGE_CAP)]
        body = ("[" + ",".join(text for _, text in page) + "]").encode("utf-8")

        if self.headers.get("Prefer") == "count=exact":
            cr = "%d-%d/%d" % (start, start + len(page) - 1, total)
        else:
            cr = "%d-%d/*" % (start, start + len(page) - 1)
        self._send(200 if not page else 206, body, [("Content-Range", cr)])


srv = http.server.HTTPServer(("127.0.0.1", 0), Handler)
port = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()

os.environ.update({
    "SUPABASE_URL": "http://127.0.0.1:%d" % port,
    "SUPABASE_SERVICE_KEY": SERVICE_KEY,
    "SUPABASE_SCHEMA": SCHEMA,
    "SUPABASE_TABLES": "products,ingredients",
    "PAGE_SIZE": "1000",
    "S3_BUCKET_NAME": "shopify-products-metadata",
    "S3_PREFIX": "supabase",
})

import handler

uploads = {}


class FakeS3:
    def upload_fileobj(self, fh, bucket, key, ExtraArgs=None):
        uploads[key] = fh.read().decode("utf-8")


import datetime as _dt


class Frozen(_dt.datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 8, 26, 6, 0, 0, tzinfo=tz)


with mock.patch.object(handler.boto3, "client", return_value=FakeS3()), \
     mock.patch.object(handler, "datetime", Frozen):
    result = handler.main({}, None)

print("=" * 72)
print("RESULT:", json.dumps(result, indent=2))
print("=" * 72)
print("HTTP requests (table, range, order, prefer):")
for r in requests_seen:
    print("   ", r)
print("=" * 72)
for key, body in uploads.items():
    print("KEY: %s" % key)
    print("-" * 72)
    sys.stdout.buffer.write(body.encode("utf-8", "replace"))
    print("-" * 72)

pk = "supabase/2026-08-26/2026-08-26_06:00:00/products.csv"
ik = "supabase/2026-08-26/2026-08-26_06:00:00/ingredients.csv"
assert set(uploads) == {pk, ik}, uploads.keys()

rows = list(csv.DictReader(io.StringIO(uploads[pk])))
assert len(rows) == 7, "expected all 7 rows across pages, got %d" % len(rows)
assert [r["id"] for r in rows] == [str(i) for i in range(1, 8)], "row order/dupes"

checks = {
    "12.50 keeps its trailing zero":        rows[0]["price"] == "12.50",
    "0.00001 not turned into 1e-05":        rows[1]["price"] == "0.00001",
    "big int keeps full precision":         rows[2]["price"] == "100000000000000000000",
    "0.00 not collapsed to 0.0":            rows[3]["price"] == "0.00",
    "9.90 keeps its trailing zero":         rows[4]["price"] == "9.90",
    "bool true is lowercase":               rows[0]["live"] == "true",
    "bool false is lowercase":              rows[1]["live"] == "false",
    "null bool -> empty field":             rows[2]["live"] == "",
    "null scalar -> empty field":           rows[5]["price"] == "",
    "null json -> empty field":             rows[0]["meta"] == "",
    "array -> compact JSON, no py repr":    rows[0]["tags"] == '["a","b"]',
    "object -> compact JSON":               rows[1]["meta"] == '{"k":"v"}',
    "nested number keeps digits":           rows[3]["meta"] == '{"n":1}',
    "empty array preserved":                rows[1]["tags"] == "[]",
    "embedded quotes round-trip":           rows[1]["name"] == 'He said "hi"',
    "embedded comma round-trip":            rows[2]["name"] == "comma, inside",
    "embedded newline round-trip":          rows[3]["name"] == "line\nbreak",
    "unicode preserved":                    rows[4]["name"] == "unicode \u00fcn\u00ef",
    "empty string stays empty":             rows[5]["name"] == "",
    "trailing spaces preserved":            rows[6]["name"] == "trailing space  ",
}
print("=" * 72)
bad = 0
for label, ok in sorted(checks.items()):
    print("  %s  %s" % ("PASS" if ok else "FAIL", label))
    bad += 0 if ok else 1

ing_lines = uploads[ik].strip().split("\n")
ing_ok = ing_lines == [",".join(INGREDIENT_COLUMNS)]
print("  %s  empty table -> real header row (got %r)" % ("PASS" if ing_ok else "FAIL", ing_lines))
bad += 0 if ing_ok else 1

paged_ok = sum(1 for r in requests_seen if r[0] == "products") > 3
print("  %s  paged through the table in multiple requests" % ("PASS" if paged_ok else "FAIL"))
bad += 0 if paged_ok else 1

ordered_ok = all(r[2] == "id.asc" for r in requests_seen if r[0] == "products" and r[2])
print("  %s  paging requests carry a stable order=id.asc" % ("PASS" if ordered_ok else "FAIL"))
bad += 0 if ordered_ok else 1

print("=" * 72)
print("FAILURES: %d" % bad)
srv.shutdown()
sys.exit(1 if bad else 0)
