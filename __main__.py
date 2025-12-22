import pulumi
import pulumi_aws as aws

config = pulumi.Config()

assume_role_policy = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            }
        }
    ]
}"""

# Shared Lambda layer for third-party dependencies (requests)
requests_layer = aws.lambda_.LayerVersion(
    "requestsLayer",
    layer_name="requests_layer",
    compatible_runtimes=["python3.13"],
    compatible_architectures=["x86_64"],
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./lambda/layers/requests")
    }),
)

secrets_read_policy = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            "Resource": "arn:aws:secretsmanager:*:*:secret:klaviyo*"
        }
    ]
}"""

# --- Events Resources ---
events_bucket = aws.s3.Bucket("klaviyo-events-bucket")
events_role = aws.iam.Role("eventsLambdaRole", assume_role_policy=assume_role_policy)

aws.iam.RolePolicyAttachment(
    "eventsLambdaPolicy",
    role=events_role.id,
    policy_arn="arn:aws:iam::aws:policy/AWSLambdaExecute",
)

aws.iam.RolePolicy(
    "eventsSecretsAccess",
    role=events_role.id,
    policy=secrets_read_policy,
)

events_lambda = aws.lambda_.Function(
    "klaviyo-event-grab-lambda",
    role=events_role.arn,
    runtime="python3.13",
    handler="index.handler",
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./lambda/klaviyo_events")
    }),
    timeout=600,
    layers=[requests_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "KLAVIYO_EVENTS_BUCKET": events_bucket.bucket,
        }
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

aws.iam.RolePolicy(
    "profilesSecretsAccess",
    role=profiles_role.id,
    policy=secrets_read_policy,
)

# Keep the queue for manual ingestion if needed, but we will not link it to the Lambda
profiles_backfill_queue = aws.sqs.Queue(
    "profilesBackfillQueue",
    name="profiles-backfill.fifo",
    fifo_queue=True,
    content_based_deduplication=True,
    visibility_timeout_seconds=900,
)

# RECURSION REMOVAL: We only keep basic SQS execution logs if needed, 
# but we DO NOT attach the EventSourceMapping or SendMessage policy.
aws.iam.RolePolicyAttachment(
    "profilesSQSPolicy",
    role=profiles_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
)

profiles_lambda = aws.lambda_.Function(
    "klaviyo-profile-grab-lambda",
    role=profiles_role.arn,
    runtime="python3.13",
    handler="index.handler",
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./lambda/klaviyo_profiles")
    }),
    timeout=600,
    reserved_concurrent_executions=1,
    layers=[requests_layer.arn],
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "KLAVIYO_PROFILES_BUCKET": profiles_bucket.bucket,
            # Pass the URL but the Lambda code will no longer use it to send messages
            "KLAVIYO_PROFILES_BACKFILL_QUEUE_URL": profiles_backfill_queue.url,
        }
    ),
)

# DELETED: profilesBackfillSQSEventSource (Stops SQS from auto-triggering Lambda)
# DELETED: profiles_cron_rule (Stops hourly automated runs)
# DELETED: profilesCronTarget (Stops EventBridge connection)
# DELETED: profilesCronInvokePermission (Stops external permission to invoke)
# DELETED: profilesSQSSendPolicy (Stops Lambda from physically being able to trigger a loop)

pulumi.export("events_bucket_name", events_bucket.bucket)
pulumi.export("events_lambda_function_name", events_lambda.name)
pulumi.export("profiles_bucket_name", profiles_bucket.bucket)
pulumi.export("profiles_lambda_function_name", profiles_lambda.name)
pulumi.export("profiles_backfill_queue_url", profiles_backfill_queue.url)