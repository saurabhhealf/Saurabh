import pulumi
import pulumi_aws as aws

config = pulumi.Config()
klaviyo_api_secret_name = config.require("klaviyoApiSecretName")

# Create an S3 bucket
bucket = aws.s3.Bucket("klaviyo-events-bucket")

# Create a Lambda function
role = aws.iam.Role("lambdaRole",
    assume_role_policy="""{
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
}""")

policy = aws.iam.RolePolicyAttachment("lambdaPolicy",
    role=role.id,
    policy_arn="arn:aws:iam::aws:policy/AWSLambdaExecute")

lambda_function = aws.lambda_.Function("klaviyo-event-grab-lambda",
    role=role.arn,
    runtime="python3.13",
    handler="index.handler",
    code=pulumi.AssetArchive({
        '.': pulumi.FileArchive('./lambda')
    }),
    environment=aws.lambda_.FunctionEnvironmentArgs(
        variables={
            "KLAVIYO_EVENTS_BUCKET": bucket.bucket,
            "KLAVIYO_API_SECRET_NAME": klaviyo_api_secret_name,
        }
    ))

pulumi.export("bucket_name", bucket.bucket)
pulumi.export("lambda_function_name", lambda_function.name)
