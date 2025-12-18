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

# Events resources
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

# Profiles resources
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

profiles_backfill_queue = aws.sqs.Queue(
    "profilesBackfillQueue",
    name="profiles-backfill.fifo",
    fifo_queue=True,
    content_based_deduplication=True,
)

aws.iam.RolePolicyAttachment(
    "profilesSQSPolicy",
    role=profiles_role.id,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
)

aws.iam.RolePolicy(
    "profilesSQSSendPolicy",
    role=profiles_role.id,
    policy=pulumi.Output.all(profiles_backfill_queue.arn).apply(
        lambda args: f"""{{
    "Version": "2012-10-17",
    "Statement": [
        {{
            "Effect": "Allow",
            "Action": ["sqs:SendMessage"],
            "Resource": "{args[0]}"
        }}
    ]
}}"""
    ),
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

pulumi.export("events_bucket_name", events_bucket.bucket)
pulumi.export("events_lambda_function_name", events_lambda.name)
pulumi.export("profiles_bucket_name", profiles_bucket.bucket)
pulumi.export("profiles_lambda_function_name", profiles_lambda.name)
pulumi.export("profiles_cron_rule_arn", profiles_cron_rule.arn)
pulumi.export("profiles_backfill_queue_name", profiles_backfill_queue.name)
pulumi.export("profiles_backfill_queue_arn", profiles_backfill_queue.arn)
pulumi.export("profiles_backfill_queue_url", profiles_backfill_queue.url)
