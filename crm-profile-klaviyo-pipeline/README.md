# crm-profile-klaviyo-pipeline

Standalone Pulumi project. Daily snapshot of the CRM profile mart from Snowflake
into S3, then replicated cross-account to the engineering team's AWS account.

```
Snowflake                         AWS data acct (051826722213)              AWS eng acct (908519936890)
healf.healf_bi                    Lambda: crm-profile-klaviyo-export         (replication target bucket)
  .mart_crm_profile_klaviyo  ──▶  EventBridge: 08:00 UTC daily          ──▶  S3 replication (see REPLICATION.md)
                                  S3: engineering-s3-data-share/
                                      crm-customers/<YYYY-MM-DD>/file_<N>.csv
```

## What the Lambda does

1. Connects to Snowflake (key-pair auth, same mechanism as `snowflake_fetch_example.py`).
2. `ALTER SESSION SET TIMEZONE = 'UTC'` — every timestamp/date is emitted in UTC.
3. `SELECT * FROM healf.healf_bi.mart_crm_profile_klaviyo ORDER BY CUSTOMER_ID ASC`.
4. Streams the result in **100,000-row batches** (`fetchmany`), writing each batch
   to its own object. Memory stays bounded regardless of table size.
5. Each run writes to a fresh UTC date folder:
   `s3://engineering-s3-data-share/crm-customers/<YYYY-MM-DD>/file_1.csv`,
   `file_2.csv`, … Every file carries the header row.
6. If the source returns 0 rows, no files are written (logged as a warning).

`BATCH_SIZE` is an env var (default `100000`) so it can be tuned without a code change.

## Layout

```
crm-profile-klaviyo-pipeline/
├── __main__.py            Pulumi: Lambda + EventBridge + IAM
├── Pulumi.yaml
├── Pulumi.dev.yaml
├── requirements.txt
├── README.md
├── REPLICATION.md
└── src/crm_profile_klaviyo_lambda/
    ├── handler.py         the Snowflake -> S3 exporter
    └── (vendored deps: snowflake-connector, cryptography, boto3, …)
```

## Infrastructure (Pulumi)

`__main__.py` deploys, into the **data-team account `051826722213`**:

- `crm-profile-klaviyo-export` — the Lambda (python3.11, 900s timeout, 2048 MB).
- `crm-profile-klaviyo-schedule` — EventBridge rule `cron(0 8 * * ? *)`.
- `crm-profile-klaviyo-role` / `-policy` — IAM role limited to `PutObject`
  under `crm-customers/*` of the existing bucket, plus CloudWatch Logs.

The destination bucket **already exists** and is not managed here.

## Deploy

```bash
cd crm-profile-klaviyo-pipeline
pulumi stack init healfz-org/dev          # new stack in the healfz-org org
pulumi config set aws:region eu-west-2

# Supply the Snowflake key at deploy time (base64 PEM, PKCS8) — baked into the
# Lambda env. Do NOT commit it.
export SNOWFLAKE_PRIVATE_KEY="$(cat ~/path/to/key.b64)"

pulumi up
```

Logs land in CloudWatch at `/aws/lambda/crm-profile-klaviyo-export`.

## Cross-account replication

The Lambda only writes to the bucket in the data account. Getting those objects
into the engineering account (`908519936890`) is an S3 replication rule on the
bucket — see **[REPLICATION.md](REPLICATION.md)**.

## Notes / limits

- Lambda hard-caps at 15 min. A full-table `SELECT *` ordered by `CUSTOMER_ID`
  on a very large mart must finish writing within that window. If the export ever
  outgrows 15 min, move the same `handler.py` logic to Fargate/Glue (no code
  change to the batching loop) and keep EventBridge as the trigger.
- `ARRAY` columns (`LAST_CHECKOUT_ITEMS`, `TOP_3_BRANDS_BY_PURCHASES`,
  `HEALTH_STATE_SIGNALS`, etc.) are serialized as their JSON string form in the CSV.
