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


def make_gorgias_stream(name: str, handler: str, max_concurrency: int = 1):
    """
    Legacy pattern (still used for non-customers streams for now):
      - FIFO SQS queue
      - Lambda with self-send permission (recursion risk)
      - Event source mapping
    """
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
          "Statement":[
            {{
              "Effect":"Allow",
              "Action":["s3:PutObject"],
              "Resource":"arn:aws:s3:::{GORGIAS_BUCKET_NAME}/{GORGIAS_S3_PREFIX}/*"
            }}
          ]
        }}""",
    )

    # Legacy streams self-send (recursion risk, unchanged for now)
    aws.iam.RolePolicy(
        f"gorgias-{name}-sqssend",
        role=role.id,
        policy=queue.arn.apply(lambda arn: f"""{{
          "Version":"2012-10-17",
          "Statement":[
            {{
              "Effect":"Allow",
              "Action":["sqs:SendMessage"],
              "Resource":"{arn}"
            }}
          ]
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
# Customers: NON-RECURSIVE pipeline
#   EventBridge Rule -> Orchestrator -> SQS -> Worker -> DDB progress -> (next tick)
#   Orchestrator auto-disables the rule when DDB status becomes DONE/ERROR.
# -------------------------

# 1) Schedule rule (define FIRST so we can reference ARN in IAM policy + name in env)
gorgias_orchestrator_rule = aws.cloudwatch.EventRule(
    "gorgias-orchestrator-every-minute",
    schedule_expression="rate(1 minute)",
)

# 2) DLQ
gorgias_customers_dlq = aws.sqs.Queue(
    "gorgias-customers-dlq",
    name="gorgias-customers-dlq",
    message_retention_seconds=1209600,  # 14 days
)

# 3) Main queue (standard SQS)
gorgias_customers_q = aws.sqs.Queue(
    "gorgias-customers-queue",
    name="gorgias-customers",
    visibility_timeout_seconds=900,
    receive_wait_time_seconds=20,        # long polling (cheaper)
    message_retention_seconds=1209600,   # 14 days
    redrive_policy=gorgias_customers_dlq.arn.apply(lambda arn: f"""{{
      "deadLetterTargetArn": "{arn}",
      "maxReceiveCount": 5
    }}"""),
)
pulumi.export("gorgias_customers_queue_url", gorgias_customers_q.url)


# 4) Customers WORKER (SQS triggered, writes to S3, updates DDB, NEVER sends SQS)
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
      "Statement":[
        {{
          "Effect":"Allow",
          "Action":["s3:PutObject"],
          "Resource":"arn:aws:s3:::{GORGIAS_BUCKET_NAME}/{GORGIAS_S3_PREFIX}/*"
        }}
      ]
    }}""",
)

aws.iam.RolePolicy(
    "gorgias-customers-ddb",
    role=gorgias_customers_role.id,
    policy=gorgias_state_table.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[
        {{
          "Effect":"Allow",
          "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
          "Resource":"{arn}"
        }}
      ]
    }}"""),
)

gorgias_customers_fn = aws.lambda_.Function(
    "gorgias-customers-lambda",
    role=gorgias_customers_role.arn,
    runtime="python3.13",
    handler="gorgias_customers.customers.handler",
    code=gorgias_code,
    timeout=600,
    reserved_concurrent_executions=1,  # stable costs + avoid API bursts
    layers=[requests_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "S3_BUCKET": GORGIAS_BUCKET_NAME,
            "S3_PREFIX_BASE": GORGIAS_S3_PREFIX,
            "PAGE_SIZE": "100",
            "PAGES_PER_INVOCATION": "10",
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


# 5) ORCHESTRATOR (scheduled, enqueues ONE message per tick, disables its rule when DONE/ERROR)
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
      "Statement":[
        {{
          "Effect":"Allow",
          "Action":["dynamodb:GetItem","dynamodb:UpdateItem","dynamodb:PutItem"],
          "Resource":"{arn}"
        }}
      ]
    }}"""),
)

aws.iam.RolePolicy(
    "gorgias-orchestrator-sqs-send",
    role=gorgias_orchestrator_role.id,
    policy=gorgias_customers_q.arn.apply(lambda arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[
        {{
          "Effect":"Allow",
          "Action":["sqs:SendMessage"],
          "Resource":"{arn}"
        }}
      ]
    }}"""),
)

# ✅ allow orchestrator to disable ONLY this schedule rule
aws.iam.RolePolicy(
    "gorgias-orchestrator-events-disable",
    role=gorgias_orchestrator_role.id,
    policy=gorgias_orchestrator_rule.arn.apply(lambda rule_arn: f"""{{
      "Version":"2012-10-17",
      "Statement":[
        {{
          "Effect":"Allow",
          "Action":["events:DisableRule"],
          "Resource":"{rule_arn}"
        }}
      ]
    }}"""),
)

gorgias_orchestrator_fn = aws.lambda_.Function(
    "gorgias-orchestrator-lambda",
    role=gorgias_orchestrator_role.arn,
    runtime="python3.13",
    handler="gorgias_orchestrator.orchestrator.handler",
    code=gorgias_code,
    timeout=60,
    layers=[],  # orchestrator doesn't need requests
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "STATE_TABLE": gorgias_state_table.name,
            "BACKFILL_QUEUE_URL": gorgias_customers_q.url,
            "VISIBILITY_TIMEOUT_SEC": "900",
            "LEASE_BUFFER_SEC": "60",
            "ORCHESTRATOR_RULE_NAME": gorgias_orchestrator_rule.name,
        }
    ),
)
pulumi.export("gorgias_orchestrator_lambda_name", gorgias_orchestrator_fn.name)

aws.cloudwatch.EventTarget(
    "gorgias-orchestrator-target",
    rule=gorgias_orchestrator_rule.name,
    arn=gorgias_orchestrator_fn.arn,
    input='{"job_start_id":"gorgias_customers_backfill"}',
)

aws.lambda_.Permission(
    "gorgias-orchestrator-invoke-permission",
    action="lambda:InvokeFunction",
    function=gorgias_orchestrator_fn.name,
    principal="events.amazonaws.com",
    source_arn=gorgias_orchestrator_rule.arn,
)


# -------------------------
# Other Gorgias streams (legacy, unchanged for now)
# -------------------------
gorgias_tickets_q, gorgias_tickets_fn = make_gorgias_stream("tickets", "gorgias_tickets.tickets.handler")
gorgias_surveys_q, gorgias_surveys_fn = make_gorgias_stream("satisfaction_surveys", "gorgias_satisfaction_surveys.satisfaction_surveys.handler")
gorgias_users_q, gorgias_users_fn = make_gorgias_stream("users", "gorgias_users.users.handler")
gorgias_messages_q, gorgias_messages_fn = make_gorgias_stream("messages", "gorgias_messages.messages.handler")


# =========================
# Exports (existing)
# =========================
pulumi.export("events_bucket_name", events_bucket.bucket)
pulumi.export("events_lambda_function_name", events_lambda.name)
pulumi.export("profiles_bucket_name", profiles_bucket.bucket)
pulumi.export("profiles_lambda_function_name", profiles_lambda.name)
pulumi.export("profiles_backfill_queue_url", profiles_backfill_queue.url)
