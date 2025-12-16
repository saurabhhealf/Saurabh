import pulumi
import pulumi_aws as aws

config = pulumi.Config()
klaviyo_api_secret_name = config.require("klaviyoApiSecretName")

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

# Events resources
events_bucket = aws.s3.Bucket("klaviyo-events-bucket")

events_role = aws.iam.Role("eventsLambdaRole", assume_role_policy=assume_role_policy)

aws.iam.RolePolicyAttachment(
    "eventsLambdaPolicy",
    role=events_role.id,
    policy_arn="arn:aws:iam::aws:policy/AWSLambdaExecute",
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
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "KLAVIYO_EVENTS_BUCKET": events_bucket.bucket,
            "KLAVIYO_API_SECRET_NAME": klaviyo_api_secret_name,
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

profiles_lambda = aws.lambda_.Function(
    "klaviyo-profile-grab-lambda",
    role=profiles_role.arn,
    runtime="python3.13",
    handler="index.handler",
    code=pulumi.AssetArchive({
        ".": pulumi.FileArchive("./lambda/klaviyo_profiles")
    }),
    timeout=600,
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "KLAVIYO_PROFILES_BUCKET": profiles_bucket.bucket,
            "KLAVIYO_API_SECRET_NAME": klaviyo_api_secret_name,
        }
    ),
)

pulumi.export("events_bucket_name", events_bucket.bucket)
pulumi.export("events_lambda_function_name", events_lambda.name)
pulumi.export("profiles_bucket_name", profiles_bucket.bucket)
pulumi.export("profiles_lambda_function_name", profiles_lambda.name)
