import json
import os
import time
import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_ddb = boto3.resource("dynamodb")
_sqs = boto3.client("sqs")

STATE_TABLE = os.environ["STATE_TABLE"]
QUEUE_URL = os.environ["BACKFILL_QUEUE_URL"]

VISIBILITY_TIMEOUT_SEC = int(os.environ.get("VISIBILITY_TIMEOUT_SEC", "300"))
LEASE_BUFFER_SEC = int(os.environ.get("LEASE_BUFFER_SEC", "60"))

TABLE = _ddb.Table(STATE_TABLE)


def handler(event, context):
    # Scheduler passes constant JSON input like {"job_start_id":"gorgias_customers_backfill"}
    job_start_id = (event or {}).get("job_start_id")
    if not job_start_id:
        raise ValueError("Missing job_start_id in event input")

    now = int(time.time())
    item = TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")
    if not item:
        logger.warning(f"[orchestrator] state missing job={job_start_id}")
        return {"enqueued": False, "reason": "missing_state"}

    status = item.get("status")
    in_flight = bool(item.get("in_flight", False))
    lease_until = int(item.get("lease_until", 0))
    page = int(item.get("page", 1))
    cursor = item.get("cursor")  # may be missing/None

    logger.info(f"[orchestrator] job={job_start_id} status={status} in_flight={in_flight} lease_until={lease_until} page={page}")

    if status != "RUNNING":
        return {"enqueued": False, "reason": f"status={status}"}

    if in_flight and lease_until > now:
        return {"enqueued": False, "reason": "lease_active"}

    # Acquire lease (prevents double-enqueue if scheduler overlaps / retries)
    new_lease = now + VISIBILITY_TIMEOUT_SEC + LEASE_BUFFER_SEC
    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            ConditionExpression="#status = :running AND (attribute_not_exists(#in_flight) OR #in_flight = :false OR #lease_until < :now)",
            UpdateExpression="SET #in_flight = :true, #lease_until = :lease, #updated_at = :now",
            ExpressionAttributeNames={
                "#status": "status",
                "#in_flight": "in_flight",
                "#lease_until": "lease_until",
                "#updated_at": "updated_at",
            },
            ExpressionAttributeValues={
                ":running": "RUNNING",
                ":false": False,
                ":true": True,
                ":now": now,
                ":lease": new_lease,
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("[orchestrator] lease not acquired (someone else owns it)")
            return {"enqueued": False, "reason": "lease_not_acquired"}
        raise

    # Send exactly one message
    body = {"job_start_id": job_start_id, "page": page}
    if cursor:
        body["cursor"] = cursor

    try:
        _sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(body))
        logger.info(f"[orchestrator] ENQUEUED job={job_start_id} page={page} cursor_present={bool(cursor)}")
        return {"enqueued": True, "job_start_id": job_start_id, "page": page}
    except Exception as e:
        # Release lease if enqueue fails
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            UpdateExpression="SET #in_flight = :false, #lease_until = :zero, #updated_at = :now, #last_error = :e",
            ExpressionAttributeNames={
                "#in_flight": "in_flight",
                "#lease_until": "lease_until",
                "#updated_at": "updated_at",
                "#last_error": "last_error",
            },
            ExpressionAttributeValues={":false": False, ":zero": 0, ":now": now, ":e": str(e)[:2000]},
        )
        raise
