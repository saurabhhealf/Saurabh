# Product Enrichment (Supabase) → S3

Twice-daily full snapshot of the Supabase `product_enrichment` schema into S3.

| | |
|---|---|
| Lambda | `product-enrichment-supabase` |
| Schedule (EventBridge rule) | `product-enrichment-supabase-schedule` — `cron(0 6,18 * * ? *)` |
| Runs at | **06:00 UTC** and **18:00 UTC**, every day |
| Source | Supabase project `product-database`, schema `product_enrichment`, tables `products` and `ingredients` |
| Destination | `s3://sources-data/supabase/` |
| Region | `eu-west-2` |
| Logs | CloudWatch → `/aws/lambda/product-enrichment-supabase` |

Each run takes a **full snapshot** — the entire table every time, no incremental logic.

> Names are deliberately distinct from the existing `product-enrichment`
> (Snowflake) pipeline on the `product-enrichment-pipeline` branch. Its IAM
> role, Lambda and EventBridge rule live in the same AWS account; reusing those
> names would have failed on create and silently retargeted its schedule.

---

## Output layout

```
s3://sources-data/supabase/
└── 2026-08-26/                       <- UTC date of the run
    ├── 2026-08-26_06:00:00/          <- UTC timestamp of the invocation
    │   ├── products.csv
    │   └── ingredients.csv
    └── 2026-08-26_18:00:00/
        ├── products.csv
        └── ingredients.csv
```

Both runs on a given day land under the same date folder, each in its own time
folder. S3 has no real directories, so "create the folder if it isn't there"
happens automatically when the object key is written — nothing to check first.

**Manual invocations behave the same way.** The time folder comes from the
actual invocation time, not from the schedule, so a manual test at 11:23:07 UTC
creates `2026-08-26/2026-08-26_11:23:07/`. It never overwrites a scheduled run.

The time folder uses `%Y-%m-%d_%H:%M:%S`, the same timestamp convention as the
existing `product-enrichment` (Snowflake) pipeline. The extra date folder above
it is new to this pipeline, so both of a day's runs group together.

---

## Columns are not hardcoded

Every request is `select=*`, and the CSV header is built from the keys the API
actually returns. **Add a column to a table and it appears in the next run** —
no code change, no redeploy, no column list to maintain anywhere.

Everything is written as text, on the assumption consumers will cast types
themselves. The conversion is deliberately lossless — these are the rules:

| Postgres / JSON value | CSV field | Note |
|---|---|---|
| `null` | *(empty)* | never `None` or the word `null` |
| `12.50` | `12.50` | trailing zeros kept — no float round-trip |
| `0.00001` | `0.00001` | never `1e-05` |
| very large integers | full digits | no precision loss |
| `true` / `false` | `true` / `false` | lowercase, not Python's `True` |
| `json` / `jsonb` / arrays | `{"k":"v"}` / `["a","b"]` | compact JSON, not a Python repr |
| text with unicode | unchanged | UTF-8 throughout |
| empty string | *(empty)* | |
| leading/trailing spaces | preserved | |

The one transformation that *is* applied is standard CSV quoting: a field
containing a comma, a double quote, or a newline gets wrapped in quotes and its
own quotes doubled (`He said "hi"` → `"He said ""hi"""`). That is RFC 4180 and
is mandatory — every CSV reader undoes it automatically, so the value read back
is identical to what Supabase sent. Pandas, Excel, Athena and Spark all handle
it without configuration.

All of the above is covered by an integration test that runs the handler
against a fake PostgREST server.

---

## Supabase credentials

This pipeline uses a **secret API key** over Supabase's REST API — the
`sb_secret_...` key under **Secret keys** in newer Supabase projects (older
projects call the equivalent key `service_role`, a JWT). It does **not** need
the database password, and it does not need the account password either.

> Note it cannot use the direct Postgres host. `db.<project-ref>.supabase.co`
> resolves to an IPv6 address only, and a Lambda outside a VPC has no IPv6
> egress — it would hang until timeout.

### 1. Get the key

**Project Settings → API keys → Secret keys** → the `default` row. Copy it.

This key bypasses row-level security, so treat it like a password: never
commit it, always set it with `--secret`.

### 2. Expose the schema

`product_enrichment` is not the default `public` schema, so PostgREST will not
serve it until it is exposed:

**Project Settings → API → Exposed schemas** → add `product_enrichment` → Save.

Without this every request comes back `404` / `406`. The Lambda sends an
`Accept-Profile: product_enrichment` header to select it.

---

## Deploy

There is **no build step** — the Lambda has no third-party dependencies, so
nothing needs vendoring before `pulumi up`.

```bash
# 1. Python deps for Pulumi itself
python -m venv venv
./venv/Scripts/pip install -r requirements.txt      # Windows
# source venv/bin/activate && pip install -r requirements.txt   # macOS/Linux

# 2. Select the stack (org is healfz-org)
pulumi stack select healfz-org/product-enrichment-supabase/dev
#   first time:  pulumi stack init healfz-org/product-enrichment-supabase/dev

# 3. The one secret. supabaseUrl is already set in Pulumi.dev.yaml.
pulumi config set --secret supabaseServiceKey '<secret API key>'

# 4. Ship it
pulumi up
```

`supabaseServiceKey` is stored encrypted in the Pulumi stack config, and the
Lambda's environment variables are encrypted at rest by AWS. If you'd prefer
the key never to sit in a Lambda env var at all, the next step up is Secrets
Manager with a `secretsmanager:GetSecretValue` grant on the role.

### Destination bucket

Pulumi does **not** create or manage `sources-data` — it only grants the Lambda
permission to write into it, the same way the `crm-klaviyo` pipeline treats
`engineering-s3-data-share`. The bucket and prefix are set at the top of
[__main__.py](__main__.py) if they ever need to move.

---

## Test it without waiting for the schedule

```bash
aws lambda invoke \
  --function-name product-enrichment-supabase \
  --region eu-west-2 \
  /dev/stdout

aws s3 ls "s3://sources-data/supabase/$(date -u +%F)/" --recursive
```

A successful run returns the row count per table and the exact S3 folder it
wrote to.

---

## How it works

[handler.py](src/product_enrichment_supabase_lambda/handler.py) pages each
table out of PostgREST and writes rows straight to a file on `/tmp`, then
multipart-uploads it — memory stays flat regardless of table size.

Notes on behaviour:

- **Paging is ordered.** Requests carry `order=id.asc` (or the table's first
  column if there is no `id`). Without a stable sort, offset paging can repeat
  or skip rows between pages.
- **The page size is discovered, not assumed.** Supabase caps responses at
  `max-rows` (1000 by default). The handler reads the real page size off the
  first response rather than trusting its own request size.
- **Short reads are caught.** The first request asks for an exact row count and
  the run fails if the rows written don't match it, rather than quietly
  uploading a truncated CSV.
- **Empty tables still produce a file** — a header-only CSV, with column names
  read from the API's OpenAPI schema, so the folder layout is identical for
  every run rather than silently missing a file.
- **One bad table doesn't block the other.** Both are attempted; the invocation
  then fails so the error surfaces in CloudWatch and EventBridge metrics.
- Transient 5xx and 429 responses are retried with backoff; 4xx fail fast.

## Layout

```
__main__.py                            Pulumi program (IAM, Lambda, EventBridge)
Pulumi.yaml / Pulumi.dev.yaml          Project + stack config
src/product_enrichment_supabase_lambda/
    handler.py                         The export logic (stdlib only)
    requirements.txt                   Documents that there are no deps
```
