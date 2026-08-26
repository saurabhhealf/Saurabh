"""
Product Enrichment (Supabase) Pipeline — Pulumi infrastructure definition.

Deploys one Lambda + one EventBridge rule:
  - product-enrichment-supabase (Lambda)
      Reads a FULL snapshot of these Supabase tables, via the REST API:
        product-enrichment.products
        product-enrichment.ingredients
      Writes:
        s3://sources-data/supabase/{YYYY-MM-DD}/{YYYY-MM-DD_HH:MM:SS}/products.csv
        s3://sources-data/supabase/{YYYY-MM-DD}/{YYYY-MM-DD_HH:MM:SS}/ingredients.csv
      Fires at 06:00 and 18:00 UTC daily via a single EventBridge rule.

Names are deliberately distinct from the existing product-enrichment
(Snowflake) pipeline, whose IAM role, Lambda and EventBridge rule live in the
same account and would otherwise be clobbered.

The destination bucket is NOT managed here — Pulumi only grants the Lambda
permission to write into it (same approach as the crm-klaviyo pipeline).

Deploy: see README.md. Short version —
  pulumi stack select healf-org/product-enrichment-supabase/dev
  pulumi config set --secret supabaseServiceKey '<service_role key>'
  pulumi up
"""

import os
import json
import pulumi
import pulumi_aws as aws

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
# Destination: bucket `sources-data`, top-level prefix `supabase/`.
BUCKET_NAME = "sources-data"
S3_PREFIX = "supabase"

# Single rule, both slots. 06:00 and 18:00 UTC every day.
SCHEDULE = "cron(0 6,18 * * ? *)"

# Postgres schema holding the tables (NOT the Supabase project name).
SUPABASE_SCHEMA = "product-enrichment"
SUPABASE_TABLES = "products,ingredients"

# PostgREST caps a page at its `max-rows` setting (1000 by default). Asking for
# more than the cap is harmless — the handler detects the real page size from
# the first response and pages accordingly.
PAGE_SIZE = "1000"

NAME = "product-enrichment-supabase"

config = pulumi.Config()

# Project URL, e.g. https://dvldaqewdnrycaecuhjw.supabase.co
supabase_url = config.get("supabaseUrl") or os.environ.get("SUPABASE_URL", "")

# service_role key. It bypasses row-level security, so it is a full-access
# credential — always set it with `pulumi config set --secret`.
supabase_service_key = config.get_secret("supabaseServiceKey") or pulumi.Output.secret(
    os.environ.get("SUPABASE_SERVICE_KEY", "")
)

if not supabase_url:
    raise Exception(
        "Missing supabaseUrl. Set it with:  pulumi config set supabaseUrl "
        "https://<project-ref>.supabase.co\nSee README.md > 'Supabase credentials'."
    )

# ---------------------------------------------------------------------------
# IAM role for the Lambda
# ---------------------------------------------------------------------------
role = aws.iam.Role(
    f"{NAME}-role",
    name=f"{NAME}-role",
    assume_role_policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
        }],
    }),
)

aws.iam.RolePolicy(
    f"{NAME}-policy",
    name=f"{NAME}-policy",
    role=role.id,
    policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:ListBucket",
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/{S3_PREFIX}/*",
                ],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
        ],
    }),
)

# ---------------------------------------------------------------------------
# Lambda function
# CloudWatch logs will appear at: /aws/lambda/product-enrichment-supabase
# ---------------------------------------------------------------------------
export_lambda = aws.lambda_.Function(
    f"{NAME}-lambda",
    name=NAME,
    code=pulumi.AssetArchive({".": pulumi.FileArchive("./src/product_enrichment_supabase_lambda")}),
    handler="handler.main",
    runtime="python3.11",
    role=role.arn,
    timeout=900,       # 15 min (Lambda max)
    memory_size=1024,
    # CSV is spooled to /tmp before upload, so give it room.
    ephemeral_storage=aws.lambda_.FunctionEphemeralStorageArgs(size=2048),
    environment={"variables": {
        "SUPABASE_URL":         supabase_url,
        "SUPABASE_SERVICE_KEY": supabase_service_key,
        "SUPABASE_SCHEMA":      SUPABASE_SCHEMA,
        "SUPABASE_TABLES":      SUPABASE_TABLES,
        "PAGE_SIZE":            PAGE_SIZE,
        "S3_BUCKET_NAME":       BUCKET_NAME,
        "S3_PREFIX":            S3_PREFIX,
    }},
)

# ---------------------------------------------------------------------------
# EventBridge rule — one rule covering both daily slots (06:00 and 18:00 UTC)
# ---------------------------------------------------------------------------
event_rule = aws.cloudwatch.EventRule(
    f"{NAME}-schedule",
    name=f"{NAME}-schedule",
    schedule_expression=SCHEDULE,
    description="Fires the product-enrichment-supabase Lambda at 06:00 and 18:00 UTC daily.",
)

aws.cloudwatch.EventTarget(
    f"{NAME}-target",
    rule=event_rule.name,
    arn=export_lambda.arn,
)

aws.lambda_.Permission(
    f"{NAME}-perm",
    action="lambda:InvokeFunction",
    function=export_lambda.name,
    principal="events.amazonaws.com",
    source_arn=event_rule.arn,
)

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
pulumi.export("lambda_name", export_lambda.name)
pulumi.export("lambda_arn", export_lambda.arn)
pulumi.export("event_rule_name", event_rule.name)
pulumi.export("schedule", SCHEDULE)
pulumi.export("s3_destination", f"s3://{BUCKET_NAME}/{S3_PREFIX}/")
