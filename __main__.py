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
# Existing Klaviyo (unchanged)
# =========================

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

aws.lambda_.EventSourceMapping(
    "profilesBackfillSQSEventSource",
    event_source_arn=profiles_backfill_queue.arn,
    function_name=profiles_lambda.arn,
    batch_size=1,
)

profiles_cron_rule = aws.cloudwatch.EventRule(
    "klaviyo_profiles_cron",
    schedule_expression="cron(15 * * * ? *)",
)

aws.cloudwatch.EventTarget(
    "profilesCronTarget",
    rule=profiles_cron_rule.name,
    arn=profiles_lambda.arn,
)

aws.lambda_.Permission(
    "profilesCronInvokePermission",
    action="lambda:InvokeFunction",
    function=profiles_lambda.name,
    principal="events.amazonaws.com",
    source_arn=profiles_cron_rule.arn,
)

# =========================
# Gorgias (new)
# =========================

GORGIAS_BUCKET_NAME = "sources-data"
GORGIAS_S3_PREFIX = "gorgias"

gorgias_code = pulumi.AssetArchive({".": pulumi.FileArchive("./lambda/gorgias")})

def make_gorgias_stream(name: str, handler: str, max_concurrency: int = 1):
    """
    Creates:
      - FIFO SQS queue for the stream
      - IAM role with: logs, SQS exec, SQS send, secrets read, S3 put
      - Lambda function pointing to handler (e.g. tickets.handler)
      - Event source mapping SQS -> Lambda
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

    # Logs/basic execution
    aws.iam.RolePolicyAttachment(
        f"gorgias-{name}-basic",
        role=role.id,
        policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    )

    # Receive/Delete from SQS
    aws.iam.RolePolicyAttachment(
        f"gorgias-{name}-sqs-exec",
        role=role.id,
        policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
    )

    # Read secrets (gorgias_email + gorgias_api_key)
    aws.iam.RolePolicy(
        f"gorgias-{name}-secrets",
        role=role.id,
        policy=secrets_read_policy,
    )

    # Write to S3: s3://sources-data/gorgias/*
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

    # Allow SendMessage to *its own queue* (for next cursor)
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
                # non-secret env vars only
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


gorgias_tickets_q, gorgias_tickets_fn = make_gorgias_stream("tickets", "tickets.handler")
gorgias_surveys_q, gorgias_surveys_fn = make_gorgias_stream("satisfaction_surveys", "satisfaction_surveys.handler")
gorgias_customers_q, gorgias_customers_fn = make_gorgias_stream("customers", "customers.handler")
gorgias_users_q, gorgias_users_fn = make_gorgias_stream("users", "users.handler")
gorgias_messages_q, gorgias_messages_fn = make_gorgias_stream("messages", "messages.handler")


# Exports (existing)
pulumi.export("events_bucket_name", events_bucket.bucket)
pulumi.export("events_lambda_function_name", events_lambda.name)
pulumi.export("profiles_bucket_name", profiles_bucket.bucket)
pulumi.export("profiles_lambda_function_name", profiles_lambda.name)
pulumi.export("profiles_cron_rule_arn", profiles_cron_rule.arn)
pulumi.export("profiles_backfill_queue_name", profiles_backfill_queue.name)
pulumi.export("profiles_backfill_queue_arn", profiles_backfill_queue.arn)
pulumi.export("profiles_backfill_queue_url", profiles_backfill_queue.url)
