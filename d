warning: in the working copy of '__main__.py', LF will be replaced by CRLF the next time Git touches it
[1mdiff --git a/__main__.py b/__main__.py[m
[1mindex abd1150..e14f3a1 100644[m
[1m--- a/__main__.py[m
[1m+++ b/__main__.py[m
[36m@@ -1,3 +1,4 @@[m
[32m+[m[32mimport json[m
 import pulumi[m
 import pulumi_aws as aws[m
 [m
[36m@@ -42,7 +43,6 @@[m [msecrets_read_policy = """{[m
   ][m
 }"""[m
 [m
[31m-[m
 # =========================[m
 # Existing Klaviyo (UNCHANGED)[m
 # =========================[m
[36m@@ -128,7 +128,6 @@[m [mprofiles_lambda = aws.lambda_.Function([m
     ),[m
 )[m
 [m
[31m-[m
 # =========================[m
 # Gorgias[m
 # =========================[m
[36m@@ -138,12 +137,23 @@[m [mGORGIAS_S3_PREFIX = "gorgias"[m
 [m
 gorgias_code = pulumi.AssetArchive({".": pulumi.FileArchive("./lambda/gorgias")})[m
 [m
[32m+[m[32m# -------------------------[m
[32m+[m[32m# DynamoDB state store (shared)  ✅ define BEFORE any helper uses it[m
[32m+[m[32m# -------------------------[m
[32m+[m[32mgorgias_state_table = aws.dynamodb.Table([m
[32m+[m[32m    "gorgiasBackfillState",[m
[32m+[m[32m    name="gorgias_backfill_state",[m
[32m+[m[32m    attributes=[aws.dynamodb.TableAttributeArgs(name="job_start_id", type="S")],[m
[32m+[m[32m    hash_key="job_start_id",[m
[32m+[m[32m    billing_mode="PAY_PER_REQUEST",[m
[32m+[m[32m)[m
[32m+[m[32mpulumi.export("gorgias_state_table_name", gorgias_state_table.name)[m
 [m
 def make_gorgias_stream(name: str, handler: str, max_concurrency: int = 1):[m
     """[m
[31m-    Legacy pattern (still used for non-customers streams for now):[m
[32m+[m[32m    Legacy pattern (kept so Pulumi won't delete existing FIFO resources):[m
       - FIFO SQS queue[m
[31m-      - Lambda with self-send permission (recursion risk)[m
[32m+[m[32m      - Lambda with self-send permission[m
       - Event source mapping[m
     """[m
     queue = aws.sqs.Queue([m
[36m@@ -192,7 +202,6 @@[m [mdef make_gorgias_stream(name: str, handler: str, max_concurrency: int = 1):[m
         }}""",[m
     )[m
 [m
[31m-    # Legacy streams self-send (recursion risk, unchanged for now)[m
     aws.iam.RolePolicy([m
         f"gorgias-{name}-sqssend",[m
         role=role.id,[m
[36m@@ -234,12 +243,9 @@[m [mdef make_gorgias_stream(name: str, handler: str, max_concurrency: int = 1):[m
 [m
     pulumi.export(f"gorgias_{name}_queue_url", queue.url)[m
     pulumi.export(f"gorgias_{name}_lambda_name", fn.name)[m
[31m-[m
     return queue, fn[m
 [m
 [m
[31m-[m
[31m-[m
 def make_gorgias_orchestrated_stream([m
     name: str,[m
     handler: str,[m
[36m@@ -252,9 +258,8 @@[m [mdef make_gorgias_orchestrated_stream([m
       - Standard SQS queue + DLQ[m
       - Worker Lambda reads SQS, writes S3, updates DDB (NO self-send)[m
       - Orchestrator Lambda runs on EventBridge, enqueues ONE message per tick[m
[31m-      - Orchestrator disables its rule once job is DONE/ERROR (handled inside gorgias_orchestrator)[m
[32m+[m[32m      - Orchestrator disables its rule once job is DONE/ERROR[m
     """[m
[31m-    # Queues[m
     dlq = aws.sqs.Queue([m
         f"gorgias-{name}-33d-dlq",[m
         name=f"gorgias-{name}-33d-dlq",[m
[36m@@ -272,7 +277,6 @@[m [mdef make_gorgias_orchestrated_stream([m
 [m
     pulumi.export(f"gorgias_{name}_33d_queue_url", q.url)[m
 [m
[31m-    # Worker role/policies[m
     role = aws.iam.Role([m
         f"gorgias-{name}-33d-role",[m
         assume_role_policy=assume_role_policy,[m
[36m@@ -326,7 +330,6 @@[m [mdef make_gorgias_orchestrated_stream([m
         }}"""),[m
     )[m
 [m
[31m-    # Worker lambda[m
     fn = aws.lambda_.Function([m
         f"gorgias-{name}-33d-lambda",[m
         role=role.arn,[m
[36m@@ -358,7 +361,6 @@[m [mdef make_gorgias_orchestrated_stream([m
 [m
     pulumi.export(f"gorgias_{name}_33d_lambda_name", fn.name)[m
 [m
[31m-    # Orchestrator (per stream)[m
     rule = aws.cloudwatch.EventRule([m
         f"gorgias-{name}-33d-orchestrator-rule",[m
         schedule_expression=schedule_expression,[m
[36m@@ -433,6 +435,9 @@[m [mdef make_gorgias_orchestrated_stream([m
                 "STATE_TABLE": gorgias_state_table.name,[m
                 "BACKFILL_QUEUE_URL": q.url,[m
                 "ORCHESTRATOR_RULE_NAME": rule.name,[m
[32m+[m[32m                # Keep these so the orchestrator lease matches queue VT[m
[32m+[m[32m                "VISIBILITY_TIMEOUT_SEC": "900",[m
[32m+[m[32m                "LEASE_BUFFER_SEC": "60",[m
             }[m
         ),[m
     )[m
[36m@@ -457,45 +462,50 @@[m [mdef make_gorgias_orchestrated_stream([m
 [m
     return q, fn[m
 [m
[32m+[m
 # -------------------------[m
[31m-# DynamoDB state store (shared)[m
[32m+[m[32m# Keep legacy FIFO streams so Pulumi doesn't delete existing ones[m
 # -------------------------[m
[31m-gorgias_state_table = aws.dynamodb.Table([m
[31m-    "gorgiasBackfillState",[m
[31m-    name="gorgias_backfill_state",[m
[31m-    attributes=[aws.dynamodb.TableAttributeArgs(name="job_start_id", type="S")],[m
[31m-    hash_key="job_start_id",[m
[31m-    billing_mode="PAY_PER_REQUEST",[m
[32m+[m[32mgorgias_tickets_fifo_q, gorgias_tickets_fifo_fn = make_gorgias_stream([m
[32m+[m[32m    "tickets",[m
[32m+[m[32m    "gorgias_tickets.tickets.handler",[m
[32m+[m[32m)[m
[32m+[m
[32m+[m[32mgorgias_surveys_fifo_q, gorgias_surveys_fifo_fn = make_gorgias_stream([m
[32m+[m[32m    "satisfaction_surveys",[m
[32m+[m[32m    "gorgias_satisfaction_surveys.satisfaction_surveys.handler",[m
[32m+[m[32m)[m
[32m+[m
[32m+[m[32mgorgias_users_fifo_q, gorgias_users_fifo_fn = make_gorgias_stream([m
[32m+[m[32m    "users",[m
[32m+[m[32m    "gorgias_users.users.handler",[m
 )[m
[31m-pulumi.export("gorgias_state_table_name", gorgias_state_table.name)[m
 [m
[32m+[m[32mgorgias_messages_fifo_q, gorgias_messages_fifo_fn = make_gorgias_stream([m
[32m+[m[32m    "messages",[m
[32m+[m[32m    "gorgias_messages.messages.handler",[m
[32m+[m[32m)[m
 [m
 # -------------------------[m
 # Customers: NON-RECURSIVE pipeline[m
[31m-#   EventBridge Rule -> Orchestrator -> SQS -> Worker -> DDB progress -> (next tick)[m
[31m-#   Orchestrator auto-disables the rule when DDB status becomes DONE/ERROR.[m
 # -------------------------[m
[31m-[m
[31m-# 1) Schedule rule (define FIRST so we can reference ARN in IAM policy + name in env)[m
 gorgias_orchestrator_rule = aws.cloudwatch.EventRule([m
     "gorgias-orchestrator-every-minute",[m
     schedule_expression="rate(1 minute)",[m
 )[m
 [m
[31m-# 2) DLQ[m
 gorgias_customers_dlq = aws.sqs.Queue([m
     "gorgias-customers-dlq",[m
     name="gorgias-customers-dlq",[m
[31m-    message_retention_seconds=1209600,  # 14 days[m
[32m+[m[32m    message_retention_seconds=1209600,[m
 )[m
 [m
[31m-# 3) Main queue (standard SQS)[m
 gorgias_customers_q = aws.sqs.Queue([m
     "gorgias-customers-queue",[m
     name="gorgias-customers",[m
     visibility_timeout_seconds=900,[m
[31m-    receive_wait_time_seconds=20,        # long polling (cheaper)[m
[31m-    message_retention_seconds=1209600,   # 14 days[m
[32m+[m[32m    receive_wait_time_seconds=20,[m
[32m+[m[32m    message_retention_seconds=1209600,[m
     redrive_policy=gorgias_customers_dlq.arn.apply(lambda arn: f"""{{[m
       "deadLetterTargetArn": "{arn}",[m
       "maxReceiveCount": 5[m
[36m@@ -503,8 +513,6 @@[m [mgorgias_customers_q = aws.sqs.Queue([m
 )[m
 pulumi.export("gorgias_customers_queue_url", gorgias_customers_q.url)[m
 [m
[31m-[m
[31m-# 4) Customers WORKER (SQS triggered, writes to S3, updates DDB, NEVER sends SQS)[m
 gorgias_customers_role = aws.iam.Role([m
     "gorgias-customers-role",[m
     assume_role_policy=assume_role_policy,[m
[36m@@ -565,7 +573,7 @@[m [mgorgias_customers_fn = aws.lambda_.Function([m
     handler="gorgias_customers.customers.handler",[m
     code=gorgias_code,[m
     timeout=600,[m
[31m-    reserved_concurrent_executions=1,  # sta