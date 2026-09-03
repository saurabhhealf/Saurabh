"""
CRM Profile (Klaviyo mart) Pipeline — Pulumi infrastructure definition.

Deploys one Lambda + one EventBridge rule into the DATA-TEAM account (051826722213):
  - crm-klaviyo-export (Lambda)
      Reads the full snapshot of:
        healf.healf_bi.mart_crm_profile_klaviyo   (ordered by CUSTOMER_ID ASC)
      Writes, in 100k-row batches:
        s3://engineering-s3-data-share/crm-customers/{YYYY-MM-DD}/file_{N}.csv
      Each run creates a new UTC date folder; every timestamp is rendered in UTC.
      Fires at 08:00 UTC daily via EventBridge.

The destination bucket (engineering-s3-data-share) already exists and is NOT
managed here — Pulumi only grants the Lambda permission to write into it.
Cross-account replication to the engineering account (908519936890) is configured
separately; see REPLICATION.md.

Deploy: run `pulumi up` from this folder.
"""

import os
import json
import pulumi
import pulumi_aws as aws

BUCKET_NAME = "engineering-s3-data-share"
S3_PREFIX = "crm-customers"
SCHEDULE = "cron(0 8 * * ? *)"  # 08:00 UTC daily
BATCH_SIZE = "50000"

snowflake_private_key = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "")

# ---------------------------------------------------------------------------
# IAM role for the Lambda
# ---------------------------------------------------------------------------
role = aws.iam.Role(
    "crm-klaviyo-role",
    name="crm-klaviyo-role",
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
    "crm-klaviyo-policy",
    name="crm-klaviyo-policy",
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
# CloudWatch logs will appear at: /aws/lambda/crm-klaviyo-export
# ---------------------------------------------------------------------------
crm_lambda = aws.lambda_.Function(
    "crm-klaviyo-lambda",
    name="crm-klaviyo-export",
    code=pulumi.AssetArchive({".": pulumi.FileArchive("./src/crm_profile_klaviyo_lambda")}),
    handler="handler.main",
    runtime="python3.11",
    role=role.arn,
    timeout=900,        # 15 min (Lambda max)
    memory_size=4096,   # 4 GB — keyset pagination keeps memory flat, but headroom helps
    environment={"variables": {
        "SNOWFLAKE_USER":        "SJ_SERVICE_USER",
        "SNOWFLAKE_ACCOUNT":     "GWNDCGK-GN77379",
        "SNOWFLAKE_WAREHOUSE":   "HEALF_WH",
        "SNOWFLAKE_DATABASE":    "HEALF",
        "SNOWFLAKE_SCHEMA":      "HEALF_BI",
        "SNOWFLAKE_ROLE":        "PC_THOUGHTSPOT_ROLE",
        "S3_BUCKET_NAME":        BUCKET_NAME,
        "BATCH_SIZE":            BATCH_SIZE,
        "SNOWFLAKE_PRIVATE_KEY": snowflake_private_key,
    }},
)

# ---------------------------------------------------------------------------
# EventBridge rule — fires at 08:00 UTC every day
# ---------------------------------------------------------------------------
event_rule = aws.cloudwatch.EventRule(
    "crm-klaviyo-schedule",
    name="crm-klaviyo-schedule",
    schedule_expression=SCHEDULE,
    description="Fires the crm-klaviyo-export Lambda at 08:00 UTC daily.",
)

aws.cloudwatch.EventTarget(
    "crm-klaviyo-target",
    rule=event_rule.name,
    arn=crm_lambda.arn,
)

aws.lambda_.Permission(
    "crm-klaviyo-perm",
    action="lambda:InvokeFunction",
    function=crm_lambda.name,
    principal="events.amazonaws.com",
    source_arn=event_rule.arn,
)

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------
pulumi.export("lambda_arn", crm_lambda.arn)
pulumi.export("event_rule_name", event_rule.name)
pulumi.export("s3_destination", f"s3://{BUCKET_NAME}/{S3_PREFIX}/")
