import json
import pulumi
import pulumi_aws as aws

config = pulumi.Config()

assume_role_policy = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Effect": "Allow",
      "Principal": { "Service": "lambda.amazonaws.com" }
    }
  ]
}"""

# Shared Lambda layer for third-party dependencies (requests)
requests_layer = aws.lambda_.LayerVersion(
    "requestsLayer",
    layer_name="requests_layer",
    compatible_runtimes=["python3.13"],
    compatible_architectures=["x86_64"],
    code=pulumi.AssetArchive({".": pulumi.FileArchive("./lambda/layers/requests")}),
)

# Secrets policy: Klaviyo + Gorgias
secrets_read_policy = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": [
        "arn:aws:secretsmanager:*:*:secret:klaviyo*",
        "arn:aws:secretsmanager:*:*:secret:gorgias_email*",
        "arn:aws:secretsmanager:*:*:secret:gorgias_api_key*"
      ]
    }
  ]
}"""

# =========================
# Existing Klaviyo (UNCHANGED)
# =========================

# --- Events Resources ---
events_bucket = aws.s3.Bucket("klaviyo-events-bucket")
events_role = aws.iam.Role("eventsLambdaRole", assume_role_policy=assume_role_policy)

aws.iam.RolePolicyAttachment(
    "eventsLambdaPolicy",
    role=events_role.id,
    policy_arn="arn:aws:iam::aws:policy/AWSLambdaExecute",
)
aws.iam.RolePolicy("eventsSecretsAccess", role=events_role.id, policy=secrets_read_policy)

events_lambda = aws.lambda_.Function(
    "klaviyo-event-grab-lambda",
    role=events_role.arn,
    runtime="python3.13",
    handler="index.handler",
    code=pulumi.AssetArchive({".": pulumi.FileArchive("./lambda/klaviyo_events")}),
    timeout=600,
    layers=[requests_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={"KLAVIYO_EVENTS_BUCKET": events_bucket.bucket}
    ),
)

# --- Profiles Resources ---
profiles_bucket = aws.s3.Bucket("klaviyo-profiles-bucket")
profiles_role = aws.iam.Role("profilesLambdaRole", assume_role_policy=assume_role_policy)

aws.iam.RolePolicyAttachment(
    "profilesLambdaPolicy",
    role=profiles_role.id,
    policy_arn="arn:aws:iam::aws:policy/AWSLambdaExecute",
)
aws.iam.RolePolicy("profilesSecretsAccess", role=profiles_role.id, policy=secrets_read_policy)

profiles_backfill_queue = aws.sqs.Queue(
    "profilesBackfillQueue",
    name="profiles-backfill.fifo",
    fifo_queue=True,
    content_based_deduplication=True,
    visibility_timeout_seconds=900,
)

aws.iam.RolePolicyAttachment(
    "profilesSQSPolicy",
    role=profiles_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
)

aws.iam.RolePolicy(
    "profilesSQSSendPolicy",
    role=profiles_role.id,
    policy=profiles_backfill_queue.arn.apply(
        lambda arn: f"""{{
          "Version":"2012-10-17",
          "Statement":[{{
            "Effect":"Allow",
            "Action":["sqs:SendMessage"],
            "Resource":"{arn}"
          }}]
        }}"""
    ),
)

profiles_lambda = aws.lambda_.Function(
    "klaviyo-profile-grab-lambda",
    role=profiles_role.arn,
    runtime="python3.13",
    handler="index.handler",
    code=pulumi.AssetArchive({".": pulumi.FileArchive("./lambda/klaviyo_profiles")}),
    timeout=600,
    reserved_concurrent_executions=1,
    layers=[requests_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "KLAVIYO_PROFILES_BUCKET": profiles_bucket.bucket,
            "KLAVIYO_PROFILES_BACKFILL_QUEUE_URL": profiles_backfill_queue.url,
        }
    ),
)

# =========================
# Gorgias
# =========================

GORGIAS_BUCKET_NAME = "sources-data"
GORGIAS_S3_PREFIX = "gorgias"
gorgias_code = pulumi.AssetArchive({".": pulumi.FileArchive("./lambda/gorgias")})

# -------------------------
# DynamoDB state store (shared)
# -------------------------
gorgias_state_table = aws.dynamodb.Table(
    "gorgiasBackfillState",
    name="gorgias_backfill_state",
    attributes=[aws.dynamodb.TableAttributeArgs(name="job_start_id", type="S")],
    hash_key="job_start_id",
    billing_mode="PAY_PER_REQUEST",
)
pulumi.export("gorgias_state_table_name", gorgias_state_table.name)

# -------------------------
# Shared roles for orchestrated streams (tickets/users/messages/surveys)
# -------------------------
gorgias_streams_worker_role = aws.iam.Role(
    "gorgias-streams-worker-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-streams-worker-basic",
    role=gorgias_streams_worker_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicyAttachment(
    "gorgias-streams-worker-sqs-exec",
    role=gorgias_streams_worker_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-streams-worker-secrets",
    role=gorgias_streams_worker_role.id,
    policy=secrets_read_policy,
)
aws.iam.RolePolicy(
    "gorgias-streams-worker-s3put",
    role=gorgias_streams_worker_role.id,
    policy=f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["s3:PutObject"],
        "Resource":"arn:aws:s3:::{GORGIAS_BUCKET_NAME}/{GORGIAS_S3_PREFIX}/*"
      }}]
    }}""",
)
aws.iam.RolePolicy(
    "gorgias-streams-worker-ddb",
    role=gorgias_streams_worker_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)

gorgias_streams_orchestrator_role = aws.iam.Role(
    "gorgias-streams-orchestrator-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-streams-orchestrator-basic",
    role=gorgias_streams_orchestrator_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-streams-orchestrator-ddb",
    role=gorgias_streams_orchestrator_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)


# -------------------------
# Tickets: DAILY pipeline
# -------------------------
gorgias_tickets_daily_rule = aws.cloudwatch.EventRule(
    "gorgias-tickets-daily-rule",
    name="gorgias-tickets-daily-rule",
    schedule_expression="rate(1 minute)",  # Fires every minute, orchestrator checks time
)

gorgias_tickets_daily_dlq = aws.sqs.Queue(
    "gorgias-tickets-daily-dlq",
    name="gorgias-tickets-daily-dlq",
    message_retention_seconds=1209600,
)

gorgias_tickets_daily_q = aws.sqs.Queue(
    "gorgias-tickets-daily-queue",
    name="gorgias-tickets-daily",
    visibility_timeout_seconds=900,
    receive_wait_time_seconds=20,
    message_retention_seconds=1209600,
    redrive_policy=gorgias_tickets_daily_dlq.arn.apply(lambda arn: f"""{{
      "deadLetterTargetArn": "{arn}",
      "maxReceiveCount": 5
    }}"""),
)
pulumi.export("gorgias_tickets_daily_queue_url", gorgias_tickets_daily_q.url)

gorgias_tickets_daily_role = aws.iam.Role(
    "gorgias-tickets-daily-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-tickets-daily-basic",
    role=gorgias_tickets_daily_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicyAttachment(
    "gorgias-tickets-daily-sqs-exec",
    role=gorgias_tickets_daily_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-tickets-daily-secrets",
    role=gorgias_tickets_daily_role.id,
    policy=secrets_read_policy,
)
aws.iam.RolePolicy(
    "gorgias-tickets-daily-s3put",
    role=gorgias_tickets_daily_role.id,
    policy=f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["s3:PutObject"],
        "Resource":"arn:aws:s3:::{GORGIAS_BUCKET_NAME}/{GORGIAS_S3_PREFIX}/*"
      }}]
    }}""",
)
aws.iam.RolePolicy(
    "gorgias-tickets-daily-ddb",
    role=gorgias_tickets_daily_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)

gorgias_tickets_daily_fn = aws.lambda_.Function(
    "gorgias-tickets-daily-lambda",
    name="gorgias-tickets-daily-worker",
    role=gorgias_tickets_daily_role.arn,
    runtime="python3.13",
    handler="gorgias_tickets.tickets.handler",
    code=gorgias_code,
    timeout=600,
    reserved_concurrent_executions=1,
    layers=[requests_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "S3_BUCKET": GORGIAS_BUCKET_NAME,
            "S3_PREFIX_BASE": GORGIAS_S3_PREFIX,
            "PAGE_SIZE": "100",
            "PAGES_PER_INVOCATION": "100",
            "STREAM_NAME": "tickets",
        }
    ),
)

aws.lambda_.EventSourceMapping(
    "gorgias-tickets-daily-esm",
    event_source_arn=gorgias_tickets_daily_q.arn,
    function_name=gorgias_tickets_daily_fn.arn,
    batch_size=1,
)
pulumi.export("gorgias_tickets_daily_lambda_name", gorgias_tickets_daily_fn.name)

gorgias_tickets_daily_orch_role = aws.iam.Role(
    "gorgias-tickets-daily-orch-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-tickets-daily-orch-basic",
    role=gorgias_tickets_daily_orch_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-tickets-daily-orch-ddb",
    role=gorgias_tickets_daily_orch_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)
aws.iam.RolePolicy(
    "gorgias-tickets-daily-orch-sqs-send",
    role=gorgias_tickets_daily_orch_role.id,
    policy=gorgias_tickets_daily_q.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["sqs:SendMessage"],
        "Resource":"{arn}"
      }}]
    }}"""),
)

gorgias_tickets_daily_orch_fn = aws.lambda_.Function(
    "gorgias-tickets-daily-orch-lambda",
    name="gorgias-tickets-daily-orchestrator",
    role=gorgias_tickets_daily_orch_role.arn,
    runtime="python3.13",
    handler="gorgias_orchestrator.orchestrator.handler",
    code=gorgias_code,
    timeout=60,
    layers=[],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "BACKFILL_QUEUE_URL": gorgias_tickets_daily_q.url,
            "STREAM_NAME": "tickets",  # Important for orchestrator to know the stream
        }
    ),
)
pulumi.export("gorgias_tickets_daily_orchestrator_name", gorgias_tickets_daily_orch_fn.name)

aws.cloudwatch.EventTarget(
    "gorgias-tickets-daily-target",
    rule=gorgias_tickets_daily_rule.name,
    arn=gorgias_tickets_daily_orch_fn.arn,
    input='{"job_start_id":"gorgias_tickets_daily"}', 
)

aws.lambda_.Permission(
    "gorgias-tickets-daily-invoke-permission",
    action="lambda:InvokeFunction",
    function=gorgias_tickets_daily_orch_fn.name,
    principal="events.amazonaws.com",
    source_arn=gorgias_tickets_daily_rule.arn,
)


# -------------------------
# Customers: DAILY pipeline
# -------------------------
gorgias_orchestrator_rule = aws.cloudwatch.EventRule(
    "gorgias-orchestrator-every-minute",
    name="gorgias-customers-daily-rule",  # Add explicit name
    schedule_expression="rate(1 minute)",
)

gorgias_customers_dlq = aws.sqs.Queue(
    "gorgias-customers-dlq",
    name="gorgias-customers-dlq",
    message_retention_seconds=1209600,
)

gorgias_customers_q = aws.sqs.Queue(
    "gorgias-customers-queue",
    name="gorgias-customers",
    visibility_timeout_seconds=900,
    receive_wait_time_seconds=20,
    message_retention_seconds=1209600,
    redrive_policy=gorgias_customers_dlq.arn.apply(lambda arn: f"""{{
      "deadLetterTargetArn": "{arn}",
      "maxReceiveCount": 5
    }}"""),
)
pulumi.export("gorgias_customers_queue_url", gorgias_customers_q.url)

gorgias_customers_role = aws.iam.Role(
    "gorgias-customers-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-customers-basic",
    role=gorgias_customers_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicyAttachment(
    "gorgias-customers-sqs-exec",
    role=gorgias_customers_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-customers-secrets",
    role=gorgias_customers_role.id,
    policy=secrets_read_policy,
)
aws.iam.RolePolicy(
    "gorgias-customers-s3put",
    role=gorgias_customers_role.id,
    policy=f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["s3:PutObject"],
        "Resource":"arn:aws:s3:::{GORGIAS_BUCKET_NAME}/{GORGIAS_S3_PREFIX}/*"
      }}]
    }}""",
)
aws.iam.RolePolicy(
    "gorgias-customers-ddb",
    role=gorgias_customers_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)

gorgias_customers_fn = aws.lambda_.Function(
    "gorgias-customers-lambda",
    name="gorgias-customers-worker",
    role=gorgias_customers_role.arn,
    runtime="python3.13",
    handler="gorgias_customers.customers.handler",
    code=gorgias_code,
    timeout=600,
    reserved_concurrent_executions=1,
    layers=[requests_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "S3_BUCKET": GORGIAS_BUCKET_NAME,
            "S3_PREFIX_BASE": GORGIAS_S3_PREFIX,
            "PAGE_SIZE": "100",
            "PAGES_PER_INVOCATION": "100", 
        }
    ),
)

aws.lambda_.EventSourceMapping(
    "gorgias-customers-esm",
    event_source_arn=gorgias_customers_q.arn,
    function_name=gorgias_customers_fn.arn,
    batch_size=1,
)
pulumi.export("gorgias_customers_lambda_name", gorgias_customers_fn.name)

gorgias_orchestrator_role = aws.iam.Role(
    "gorgias-orchestrator-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-orchestrator-basic",
    role=gorgias_orchestrator_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-orchestrator-ddb",
    role=gorgias_orchestrator_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)
aws.iam.RolePolicy(
    "gorgias-orchestrator-sqs-send",
    role=gorgias_orchestrator_role.id,
    policy=gorgias_customers_q.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["sqs:SendMessage"],
        "Resource":"{arn}"
      }}]
    }}"""),
)
aws.iam.RolePolicy(
    "gorgias-orchestrator-events-disable",
    role=gorgias_orchestrator_role.id,
    policy=gorgias_orchestrator_rule.arn.apply(lambda rule_arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["events:DisableRule"],
        "Resource":"{rule_arn}"
      }}]
    }}"""),
)

gorgias_orchestrator_fn = aws.lambda_.Function(
    "gorgias-orchestrator-lambda",
    name="gorgias-customers-orchestrator",
    role=gorgias_orchestrator_role.arn,
    runtime="python3.13",
    handler="gorgias_orchestrator.orchestrator.handler",
    code=gorgias_code,
    timeout=60,
    layers=[],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "BACKFILL_QUEUE_URL": gorgias_customers_q.url,
            "VISIBILITY_TIMEOUT_SEC": "900",
            "LEASE_BUFFER_SEC": "60",
            "ORCHESTRATOR_RULE_NAME": gorgias_orchestrator_rule.name,
            "STREAM_NAME": "customers",
            "DAILY_START_HOUR": "2",
        }
    ),
)
pulumi.export("gorgias_orchestrator_lambda_name", gorgias_orchestrator_fn.name)

aws.cloudwatch.EventTarget(
    "gorgias-orchestrator-target",
    rule=gorgias_orchestrator_rule.name,
    arn=gorgias_orchestrator_fn.arn,
    input='{"job_start_id":"gorgias_customers_daily"}', 
)

aws.lambda_.Permission(
    "gorgias-orchestrator-invoke-permission",
    action="lambda:InvokeFunction",
    function=gorgias_orchestrator_fn.name,
    principal="events.amazonaws.com",
    source_arn=gorgias_orchestrator_rule.arn,
)

# -------------------------
# Messages: DAILY pipeline (NEW)
# -------------------------
gorgias_messages_daily_rule = aws.cloudwatch.EventRule(
    "gorgias-messages-daily-rule",
    name="gorgias-messages-daily-rule",
    schedule_expression="rate(1 minute)",
)

gorgias_messages_daily_dlq = aws.sqs.Queue(
    "gorgias-messages-daily-dlq",
    name="gorgias-messages-daily-dlq",
    message_retention_seconds=1209600,
)

gorgias_messages_daily_q = aws.sqs.Queue(
    "gorgias-messages-daily-queue",
    name="gorgias-messages-daily",
    visibility_timeout_seconds=900,
    receive_wait_time_seconds=20,
    message_retention_seconds=1209600,
    redrive_policy=gorgias_messages_daily_dlq.arn.apply(lambda arn: f"""{{
      "deadLetterTargetArn": "{arn}",
      "maxReceiveCount": 5
    }}"""),
)
pulumi.export("gorgias_messages_daily_queue_url", gorgias_messages_daily_q.url)

gorgias_messages_daily_role = aws.iam.Role(
    "gorgias-messages-daily-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-messages-daily-basic",
    role=gorgias_messages_daily_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicyAttachment(
    "gorgias-messages-daily-sqs-exec",
    role=gorgias_messages_daily_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-messages-daily-secrets",
    role=gorgias_messages_daily_role.id,
    policy=secrets_read_policy,
)
aws.iam.RolePolicy(
    "gorgias-messages-daily-s3put",
    role=gorgias_messages_daily_role.id,
    policy=f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["s3:PutObject"],
        "Resource":"arn:aws:s3:::{GORGIAS_BUCKET_NAME}/{GORGIAS_S3_PREFIX}/*"
      }}]
    }}""",
)
aws.iam.RolePolicy(
    "gorgias-messages-daily-ddb",
    role=gorgias_messages_daily_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)

gorgias_messages_daily_fn = aws.lambda_.Function(
    "gorgias-messages-daily-lambda",
    name="gorgias-messages-daily-worker",
    role=gorgias_messages_daily_role.arn,
    runtime="python3.13",
    handler="gorgias_messages.messages.handler",
    code=gorgias_code,
    timeout=600,
    memory_size=512,  
    reserved_concurrent_executions=1,
    layers=[requests_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "S3_BUCKET": GORGIAS_BUCKET_NAME,
            "S3_PREFIX_BASE": GORGIAS_S3_PREFIX,
            "PAGE_SIZE": "100",
            "PAGES_PER_INVOCATION": "100",
            "STREAM_NAME": "messages",
        }
    ),
)

aws.lambda_.EventSourceMapping(
    "gorgias-messages-daily-esm",
    event_source_arn=gorgias_messages_daily_q.arn,
    function_name=gorgias_messages_daily_fn.arn,
    batch_size=1,
)
pulumi.export("gorgias_messages_daily_lambda_name", gorgias_messages_daily_fn.name)

gorgias_messages_daily_orch_role = aws.iam.Role(
    "gorgias-messages-daily-orch-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-messages-daily-orch-basic",
    role=gorgias_messages_daily_orch_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-messages-daily-orch-ddb",
    role=gorgias_messages_daily_orch_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)
aws.iam.RolePolicy(
    "gorgias-messages-daily-orch-sqs-send",
    role=gorgias_messages_daily_orch_role.id,
    policy=gorgias_messages_daily_q.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["sqs:SendMessage"],
        "Resource":"{arn}"
      }}]
    }}"""),
)
aws.iam.RolePolicy(
    "gorgias-messages-daily-orch-events-disable",
    role=gorgias_messages_daily_orch_role.id,
    policy=gorgias_messages_daily_rule.arn.apply(lambda rule_arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["events:DisableRule"],
        "Resource":"{rule_arn}"
      }}]
    }}"""),
)

gorgias_messages_daily_orch_fn = aws.lambda_.Function(
    "gorgias-messages-daily-orch-lambda",
    name="gorgias-messages-daily-orchestrator",
    role=gorgias_messages_daily_orch_role.arn,
    runtime="python3.13",
    handler="gorgias_orchestrator.orchestrator.handler",
    code=gorgias_code,
    timeout=60,
    layers=[],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "BACKFILL_QUEUE_URL": gorgias_messages_daily_q.url,
            "STREAM_NAME": "messages",
            "DAILY_START_HOUR": "4", # 04:00 UTC
        }
    ),
)
pulumi.export("gorgias_messages_daily_orchestrator_name", gorgias_messages_daily_orch_fn.name)

aws.cloudwatch.EventTarget(
    "gorgias-messages-daily-target",
    rule=gorgias_messages_daily_rule.name,
    arn=gorgias_messages_daily_orch_fn.arn,
    input='{"job_start_id":"gorgias_messages_daily"}', 
)

aws.lambda_.Permission(
    "gorgias-messages-daily-invoke-permission",
    action="lambda:InvokeFunction",
    function=gorgias_messages_daily_orch_fn.name,
    principal="events.amazonaws.com",
    source_arn=gorgias_messages_daily_rule.arn,
)

# -------------------------
# Satisfaction Surveys: DAILY pipeline (NEW)
# -------------------------
gorgias_surveys_daily_rule = aws.cloudwatch.EventRule(
    "gorgias-surveys-daily-rule",
    name="gorgias-surveys-daily-rule",
    schedule_expression="cron(0 5 * * ? *)", # Fires at 05:00 UTC every day
)

gorgias_surveys_daily_dlq = aws.sqs.Queue(
    "gorgias-surveys-daily-dlq",
    name="gorgias-surveys-daily-dlq",
    message_retention_seconds=1209600,
)

gorgias_surveys_daily_q = aws.sqs.Queue(
    "gorgias-surveys-daily-queue",
    name="gorgias-surveys-daily",
    visibility_timeout_seconds=900,
    receive_wait_time_seconds=20,
    message_retention_seconds=1209600,
    redrive_policy=gorgias_surveys_daily_dlq.arn.apply(lambda arn: f"""{{
      "deadLetterTargetArn": "{arn}",
      "maxReceiveCount": 5
    }}"""),
)
pulumi.export("gorgias_surveys_daily_queue_url", gorgias_surveys_daily_q.url)

gorgias_surveys_daily_role = aws.iam.Role(
    "gorgias-surveys-daily-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-surveys-daily-basic",
    role=gorgias_surveys_daily_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicyAttachment(
    "gorgias-surveys-daily-sqs-exec",
    role=gorgias_surveys_daily_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-surveys-daily-secrets",
    role=gorgias_surveys_daily_role.id,
    policy=secrets_read_policy,
)
aws.iam.RolePolicy(
    "gorgias-surveys-daily-s3put",
    role=gorgias_surveys_daily_role.id,
    policy=f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["s3:PutObject"],
        "Resource":"arn:aws:s3:::{GORGIAS_BUCKET_NAME}/{GORGIAS_S3_PREFIX}/*"
      }}]
    }}""",
)
aws.iam.RolePolicy(
    "gorgias-surveys-daily-ddb",
    role=gorgias_surveys_daily_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)

gorgias_surveys_daily_fn = aws.lambda_.Function(
    "gorgias-surveys-daily-lambda",
    name="gorgias-surveys-daily-worker",
    role=gorgias_surveys_daily_role.arn,
    runtime="python3.13",
    handler="gorgias_satisfaction_surveys.satisfaction_surveys.handler",
    code=gorgias_code,
    timeout=600,
    memory_size=512,  
    reserved_concurrent_executions=1,
    layers=[requests_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "S3_BUCKET": GORGIAS_BUCKET_NAME,
            "S3_PREFIX_BASE": GORGIAS_S3_PREFIX,
            "PAGE_SIZE": "100",
            "PAGES_PER_INVOCATION": "100",
            "STREAM_NAME": "satisfaction_surveys",
        }
    ),
)

aws.lambda_.EventSourceMapping(
    "gorgias-surveys-daily-esm",
    event_source_arn=gorgias_surveys_daily_q.arn,
    function_name=gorgias_surveys_daily_fn.arn,
    batch_size=1,
)
pulumi.export("gorgias_surveys_daily_lambda_name", gorgias_surveys_daily_fn.name)

gorgias_surveys_daily_orch_role = aws.iam.Role(
    "gorgias-surveys-daily-orch-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-surveys-daily-orch-basic",
    role=gorgias_surveys_daily_orch_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-surveys-daily-orch-ddb",
    role=gorgias_surveys_daily_orch_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)
aws.iam.RolePolicy(
    "gorgias-surveys-daily-orch-sqs-send",
    role=gorgias_surveys_daily_orch_role.id,
    policy=gorgias_surveys_daily_q.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["sqs:SendMessage"],
        "Resource":"{arn}"
      }}]
    }}"""),
)
aws.iam.RolePolicy(
    "gorgias-surveys-daily-orch-events-disable",
    role=gorgias_surveys_daily_orch_role.id,
    policy=gorgias_surveys_daily_rule.arn.apply(lambda rule_arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["events:DisableRule"],
        "Resource":"{rule_arn}"
      }}]
    }}"""),
)

gorgias_surveys_daily_orch_fn = aws.lambda_.Function(
    "gorgias-surveys-daily-orch-lambda",
    name="gorgias-surveys-daily-orchestrator",
    role=gorgias_surveys_daily_orch_role.arn,
    runtime="python3.13",
    handler="gorgias_orchestrator.orchestrator.handler",
    code=gorgias_code,
    timeout=60,
    layers=[],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "BACKFILL_QUEUE_URL": gorgias_surveys_daily_q.url,
            "STREAM_NAME": "satisfaction_surveys",
            "DAILY_START_HOUR": "5", # 05:00 UTC
        }
    ),
)
pulumi.export("gorgias_surveys_daily_orchestrator_name", gorgias_surveys_daily_orch_fn.name)

aws.cloudwatch.EventTarget(
    "gorgias-surveys-daily-target",
    rule=gorgias_surveys_daily_rule.name,
    arn=gorgias_surveys_daily_orch_fn.arn,
    input='{"job_start_id":"gorgias_satisfaction_surveys_daily"}', 
)

aws.lambda_.Permission(
    "gorgias-surveys-daily-invoke-permission",
    action="lambda:InvokeFunction",
    function=gorgias_surveys_daily_orch_fn.name,
    principal="events.amazonaws.com",
    source_arn=gorgias_surveys_daily_rule.arn,
)

# -------------------------
# Users: DAILY pipeline (Full Snapshot)
# -------------------------
gorgias_users_daily_rule = aws.cloudwatch.EventRule(
    "gorgias-users-daily-rule",
    name="gorgias-users-daily-rule",
    schedule_expression="cron(0 6 * * ? *)", # Fires at 06:00 UTC
)

gorgias_users_daily_dlq = aws.sqs.Queue(
    "gorgias-users-daily-dlq",
    name="gorgias-users-daily-dlq",
    message_retention_seconds=1209600,
)

gorgias_users_daily_q = aws.sqs.Queue(
    "gorgias-users-daily-queue",
    name="gorgias-users-daily",
    visibility_timeout_seconds=900,
    receive_wait_time_seconds=20,
    message_retention_seconds=1209600,
    redrive_policy=gorgias_users_daily_dlq.arn.apply(lambda arn: f"""{{
      "deadLetterTargetArn": "{arn}",
      "maxReceiveCount": 5
    }}"""),
)
pulumi.export("gorgias_users_daily_queue_url", gorgias_users_daily_q.url)

gorgias_users_daily_role = aws.iam.Role(
    "gorgias-users-daily-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-users-daily-basic",
    role=gorgias_users_daily_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicyAttachment(
    "gorgias-users-daily-sqs-exec",
    role=gorgias_users_daily_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-users-daily-secrets",
    role=gorgias_users_daily_role.id,
    policy=secrets_read_policy,
)
aws.iam.RolePolicy(
    "gorgias-users-daily-s3put",
    role=gorgias_users_daily_role.id,
    policy=f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["s3:PutObject"],
        "Resource":"arn:aws:s3:::{GORGIAS_BUCKET_NAME}/{GORGIAS_S3_PREFIX}/*"
      }}]
    }}""",
)
aws.iam.RolePolicy(
    "gorgias-users-daily-ddb",
    role=gorgias_users_daily_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)

gorgias_users_daily_fn = aws.lambda_.Function(
    "gorgias-users-daily-lambda",
    name="gorgias-users-daily-worker",
    role=gorgias_users_daily_role.arn,
    runtime="python3.13",
    handler="gorgias_users.users.handler",
    code=gorgias_code,
    timeout=600,
    memory_size=512,  
    reserved_concurrent_executions=1,
    layers=[requests_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "S3_BUCKET": GORGIAS_BUCKET_NAME,
            "S3_PREFIX_BASE": GORGIAS_S3_PREFIX,
            "PAGE_SIZE": "100",
            "PAGES_PER_INVOCATION": "100",
            "STREAM_NAME": "users",
        }
    ),
)

aws.lambda_.EventSourceMapping(
    "gorgias-users-daily-esm",
    event_source_arn=gorgias_users_daily_q.arn,
    function_name=gorgias_users_daily_fn.arn,
    batch_size=1,
)
pulumi.export("gorgias_users_daily_lambda_name", gorgias_users_daily_fn.name)

gorgias_users_daily_orch_role = aws.iam.Role(
    "gorgias-users-daily-orch-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "gorgias-users-daily-orch-basic",
    role=gorgias_users_daily_orch_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)
aws.iam.RolePolicy(
    "gorgias-users-daily-orch-ddb",
    role=gorgias_users_daily_orch_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
        "Resource":"{arn}"
      }}]
    }}"""),
)
aws.iam.RolePolicy(
    "gorgias-users-daily-orch-sqs-send",
    role=gorgias_users_daily_orch_role.id,
    policy=gorgias_users_daily_q.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["sqs:SendMessage"],
        "Resource":"{arn}"
      }}]
    }}"""),
)
aws.iam.RolePolicy(
    "gorgias-users-daily-orch-events-disable",
    role=gorgias_users_daily_orch_role.id,
    policy=gorgias_users_daily_rule.arn.apply(lambda rule_arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[{{
        "Effect":"Allow",
        "Action":["events:DisableRule"],
        "Resource":"{rule_arn}"
      }}]
    }}"""),
)

gorgias_users_daily_orch_fn = aws.lambda_.Function(
    "gorgias-users-daily-orch-lambda",
    name="gorgias-users-daily-orchestrator",
    role=gorgias_users_daily_orch_role.arn,
    runtime="python3.13",
    handler="gorgias_orchestrator.orchestrator.handler",
    code=gorgias_code,
    timeout=60,
    layers=[],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "BACKFILL_QUEUE_URL": gorgias_users_daily_q.url,
            "STREAM_NAME": "users",
            "DAILY_START_HOUR": "6",
        }
    ),
)
pulumi.export("gorgias_users_daily_orchestrator_name", gorgias_users_daily_orch_fn.name)

aws.cloudwatch.EventTarget(
    "gorgias-users-daily-target",
    rule=gorgias_users_daily_rule.name,
    arn=gorgias_users_daily_orch_fn.arn,
    input='{"job_start_id":"gorgias_users_daily"}', 
)

aws.lambda_.Permission(
    "gorgias-users-daily-invoke-permission",
    action="lambda:InvokeFunction",
    function=gorgias_users_daily_orch_fn.name,
    principal="events.amazonaws.com",
    source_arn=gorgias_users_daily_rule.arn,
)

# -------------------------
# Legacy FIFO streams (kept so Pulumi won't delete existing FIFO resources)
# -------------------------
def make_gorgias_stream(name: str, handler: str, max_concurrency: int = 1):
    queue = aws.sqs.Queue(
        f"gorgias-{name}-queue",
        name=f"gorgias-{name}.fifo",
        fifo_queue=True,
        content_based_deduplication=True,
        visibility_timeout_seconds=900,
    )

    role = aws.iam.Role(
        f"gorgias-{name}-role",
        assume_role_policy=assume_role_policy,
    )

    aws.iam.RolePolicyAttachment(
        f"gorgias-{name}-basic",
        role=role.id,
        policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    )
    aws.iam.RolePolicyAttachment(
        f"gorgias-{name}-sqs-exec",
        role=role.id,
        policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
    )
    aws.iam.RolePolicy(
        f"gorgias-{name}-secrets",
        role=role.id,
        policy=secrets_read_policy,
    )
    aws.iam.RolePolicy(
        f"gorgias-{name}-s3put",
        role=role.id,
        policy=f"""{{
          "Version":"2012-10-17",
          "Statement":[{{
            "Effect":"Allow",
            "Action":["s3:PutObject"],
            "Resource":"arn:aws:s3:::{GORGIAS_BUCKET_NAME}/{GORGIAS_S3_PREFIX}/*"
          }}]
        }}""",
    )
    aws.iam.RolePolicy(
        f"gorgias-{name}-sqssend",
        role=role.id,
        policy=queue.arn.apply(lambda arn: f"""{{
          "Version":"2012-10-17",
          "Statement":[{{
            "Effect":"Allow",
            "Action":["sqs:SendMessage"],
            "Resource":"{arn}"
          }}]
        }}"""),
    )
    
    # --- ADDED: DynamoDB Permissions for Legacy Streams ---
    aws.iam.RolePolicy(
        f"gorgias-{name}-ddb",
        role=role.id,
        policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
          "Version":"2012-10-17",
          "Statement":[{{
            "Effect":"Allow",
            "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
            "Resource":"{arn}"
          }}]
        }}"""),
    )

    fn = aws.lambda_.Function(
        f"gorgias-{name}-lambda",
        role=role.arn,
        runtime="python3.13",
        handler=handler,
        code=gorgias_code,
        timeout=600,
        reserved_concurrent_executions=max_concurrency,
        layers=[requests_layer.arn],
        environment=aws.lambda_.FunctionEnvironmentArgs(
            variables={
                "BACKFILL_QUEUE_URL": queue.url,
                "STREAM_NAME": name,
                # --- ADDED: Missing Env Vars for Shared Code ---
                "STATE_TABLE": gorgias_state_table.name, 
                "S3_BUCKET": GORGIAS_BUCKET_NAME,
                "S3_PREFIX_BASE": GORGIAS_S3_PREFIX,
            }
        ),
    )

    aws.lambda_.EventSourceMapping(
        f"gorgias-{name}-esm",
        event_source_arn=queue.arn,
        function_name=fn.arn,
        batch_size=1,
    )

    pulumi.export(f"gorgias_{name}_queue_url", queue.url)
    pulumi.export(f"gorgias_{name}_lambda_name", fn.name)
    return queue, fn


# -------------------------
# Orchestrated streams (no "33d"; use "-backfill-" to avoid FIFO collisions)
# Fixes:
#   - short AWS Lambda names (avoid 64-char limit)
#   - unique Pulumi logical names (avoid duplicate URN)
# -------------------------
def make_gorgias_orchestrated_stream(
    name: str,
    handler: str,
    job_start_id: str,
    schedule_expression: str = "rate(1 minute)",
    max_concurrency: int = 1,
):
    dlq = aws.sqs.Queue(
        f"gorgias-{name}-backfill-dlq",
        name=f"gorgias-{name}-backfill-dlq",
    )

    q = aws.sqs.Queue(
        f"gorgias-{name}-backfill-queue",
        name=f"gorgias-{name}-backfill-queue",
        visibility_timeout_seconds=900,
        redrive_policy=dlq.arn.apply(lambda arn: f"""{{
          "deadLetterTargetArn": "{arn}",
          "maxReceiveCount": 5
        }}"""),
    )

    pulumi.export(f"gorgias_{name}_backfill_queue_url", q.url)

    # Worker lambda (shared worker role)
    worker_fn = aws.lambda_.Function(
        f"gorgias-{name}-backfill-worker",     # Pulumi logical name (unique)
        name=f"gorgias-{name}-bf",             # AWS name (short)
        role=gorgias_streams_worker_role.arn,
        runtime="python3.13",
        handler=handler,
        code=gorgias_code,
        timeout=600,
        memory_size=512,  # Set to 512MB to match daily worker
        reserved_concurrent_executions=max_concurrency,
        layers=[requests_layer.arn],
        environment=aws.lambda_.FunctionEnvironmentArgs(
            variables={
                "STATE_TABLE": gorgias_state_table.name,
                "S3_BUCKET": GORGIAS_BUCKET_NAME,
                "S3_PREFIX_BASE": GORGIAS_S3_PREFIX,
                "PAGE_SIZE": "100",
                "PAGES_PER_INVOCATION": "100", 
                "FILTER_TO_CUTOFF": "false",
                "STREAM_NAME": name,
            }
        ),
    )

    aws.lambda_.EventSourceMapping(
        f"gorgias-{name}-backfill-esm",
        event_source_arn=q.arn,
        function_name=worker_fn.arn,
        batch_size=1,
    )

    pulumi.export(f"gorgias_{name}_backfill_lambda_name", worker_fn.name)

    # EventBridge rule (explicit AWS name for easy CLI enable/disable)
    rule = aws.cloudwatch.EventRule(
        f"gorgias-{name}-backfill-orchestrator-rule",
        name=f"gorgias-{name}-backfill-orchestrator-rule",
        schedule_expression=schedule_expression,
    )

    # Allow shared orchestrator role to SendMessage to this queue
    aws.iam.RolePolicy(
        f"gorgias-streams-orch-{name}-sqs-send",
        role=gorgias_streams_orchestrator_role.id,
        policy=q.arn.apply(lambda arn: f"""{{
          "Version":"2012-10-17",
          "Statement":[{{
            "Effect":"Allow",
            "Action":["sqs:SendMessage"],
            "Resource":"{arn}"
          }}]
        }}"""),
    )

    # Allow shared orchestrator role to disable this rule
    aws.iam.RolePolicy(
        f"gorgias-streams-orch-{name}-events-disable",
        role=gorgias_streams_orchestrator_role.id,
        policy=rule.arn.apply(lambda rule_arn: f"""{{
          "Version":"2012-10-17",
          "Statement":[{{
            "Effect":"Allow",
            "Action":["events:DisableRule"],
            "Resource":"{rule_arn}"
          }}]
        }}"""),
    )

    # Orchestrator lambda (shared orchestrator role)
    orch_fn = aws.lambda_.Function(
        f"gorgias-{name}-backfill-orchestrator",  # Pulumi logical name (unique)
        name=f"gorgias-{name}-bf-orch",           # AWS name (short)
        role=gorgias_streams_orchestrator_role.arn,
        runtime="python3.13",
        handler="gorgias_orchestrator.orchestrator.handler",
        code=gorgias_code,
        timeout=60,
        layers=[requests_layer.arn],
        environment=aws.lambda_.FunctionEnvironmentArgs(
            variables={
                "STATE_TABLE": gorgias_state_table.name,
                "BACKFILL_QUEUE_URL": q.url,
                "ORCHESTRATOR_RULE_NAME": rule.name,
                "VISIBILITY_TIMEOUT_SEC": "900",
                "LEASE_BUFFER_SEC": "60",
                "STREAM_NAME": name,
            }
        ),
    )

    aws.cloudwatch.EventTarget(
        f"gorgias-{name}-backfill-orchestrator-target",
        rule=rule.name,
        arn=orch_fn.arn,
        target_id=f"g-{name}-bf",
        input=json.dumps({"job_start_id": job_start_id}),
    )

    aws.lambda_.Permission(
        f"gorgias-{name}-backfill-orchestrator-invoke",
        action="lambda:InvokeFunction",
        function=orch_fn.name,
        principal="events.amazonaws.com",
        source_arn=rule.arn,
    )

    pulumi.export(f"gorgias_{name}_backfill_orchestrator_rule_name", rule.name)
    pulumi.export(f"gorgias_{name}_backfill_orchestrator_lambda_name", orch_fn.name)

    return q, worker_fn


# -------------------------
# Orchestrated Gorgias streams (no 33d anywhere)
# -------------------------
gorgias_tickets_backfill_q, gorgias_tickets_backfill_fn = make_gorgias_orchestrated_stream(
    "tickets",
    "gorgias_tickets.tickets.handler",
    job_start_id="gorgias_tickets_backfill",
)

gorgias_surveys_backfill_q, gorgias_surveys_backfill_fn = make_gorgias_orchestrated_stream(
    "satisfaction_surveys",
    "gorgias_satisfaction_surveys.satisfaction_surveys.handler",
    job_start_id="gorgias_satisfaction_surveys_backfill",
)

gorgias_users_backfill_q, gorgias_users_backfill_fn = make_gorgias_orchestrated_stream(
    "users",
    "gorgias_users.users.handler",
    job_start_id="gorgias_users_backfill",
)

gorgias_messages_backfill_q, gorgias_messages_backfill_fn = make_gorgias_orchestrated_stream(
    "messages",
    "gorgias_messages.messages.handler",
    job_start_id="gorgias_messages_backfill",
)

# =========================
# Exports
# =========================
pulumi.export("events_bucket_name", events_bucket.bucket)
pulumi.export("events_lambda_function_name", events_lambda.name)
pulumi.export("profiles_bucket_name", profiles_bucket.bucket)
pulumi.export("profiles_lambda_function_name", profiles_lambda.name)
pulumi.export("profiles_backfill_queue_url", profiles_backfill_queue.url)