import os
import json
import time
import logging
import boto3
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_ddb = boto3.resource("dynamodb")
_sqs = boto3.client("sqs")
_events = boto3.client("events")

STATE_TABLE = os.environ["STATE_TABLE"]
QUEUE_URL = os.environ["BACKFILL_QUEUE_URL"]
TABLE = _ddb.Table(STATE_TABLE)

# Stream-specific configuration for daily jobs
DAILY_CONFIG = {
    "customers": {
        "start_hour": 2,   # 02:00 UTC
        "days_back": 5,    # Yesterday
    },
    "tickets": {
        "start_hour": 3,   # 03:00 UTC
        "days_back": 5,    # Last 2 days
    },
}
DEFAULT_START_HOUR = 2
DEFAULT_DAYS_BACK = 1


def _disable_schedule_rule_if_configured(rule_name: str, reason: str) -> None:
    """Disables the EventBridge rule (Used only for backfill jobs)."""
    if not rule_name:
        return
    try:
        _events.disable_rule(Name=rule_name)
        logger.info(f"[orchestrator] Disabled schedule rule '{rule_name}' (reason={reason})")
    except ClientError as e:
        logger.warning(f"[orchestrator] Failed to disable rule '{rule_name}': {e}")


def handler(event, context):
    job_start_id = event.get("job_start_id")
    if not job_start_id:
        raise ValueError("Missing job_start_id in event input")

    # Determine Mode: "Daily" vs "Backfill" based on job ID string
    IS_DAILY = "daily" in job_start_id
    ORCHESTRATOR_RULE_NAME = os.environ.get("ORCHESTRATOR_RULE_NAME", "").strip()
    STREAM_NAME = os.environ.get("STREAM_NAME", "").strip().lower()

    now = int(time.time())
    now_dt = datetime.now(timezone.utc)

    # ==========================================
    # 1. DAILY JOB LOGIC (Customers, Tickets, etc.)
    # ==========================================
    if IS_DAILY:
        # Get stream-specific config
        config = DAILY_CONFIG.get(STREAM_NAME, {})
        daily_start_hour = config.get("start_hour", DEFAULT_START_HOUR)
        days_back = config.get("days_back", DEFAULT_DAYS_BACK)

        # Only run during the designated hour
        '''
        if now_dt.hour != daily_start_hour:
            return {
                "enqueued": False,
                "reason": "outside_daily_window",
                "stream": STREAM_NAME,
                "current_hour": now_dt.hour,
                "expected_hour": daily_start_hour,
            }
'''
        # Calculate Target Date
        target_date = (now_dt - timedelta(days=days_back)).strftime("%Y-%m-%d")

        item = TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")

        # Seed state if missing
        if not item:
            logger.info(f"[orchestrator] Seeding new daily job {job_start_id} for {target_date}")
            TABLE.put_item(Item={
                "job_start_id": job_start_id,
                "status": "RUNNING",
                "page": 1,
                "in_flight": False,
                "lease_until": 0,
                "run_date": target_date,
            })
            item = TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")

        # CHECK FOR NEW DAY -> RESET STATE
        stored_run_date = item.get("run_date", "")
        if stored_run_date != target_date:
            logger.info(f"[orchestrator] New day detected! Resetting {job_start_id} for {target_date}")
            TABLE.update_item(
                Key={"job_start_id": job_start_id},
                UpdateExpression="SET #status=:r, #page=:p, #run_date=:rd, #lease_until=:z REMOVE #cursor",
                ExpressionAttributeNames={
                    "#status": "status",
                    "#page": "page",
                    "#run_date": "run_date",
                    "#lease_until": "lease_until",
                    "#cursor": "cursor",
                },
                ExpressionAttributeValues={
                    ":r": "RUNNING",
                    ":p": 1,
                    ":rd": target_date,
                    ":z": 0,
                },
            )
            item = TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")

        # If already DONE for today, just return (do NOT disable rule)
        if item.get("status") == "DONE":
            return {"enqueued": False, "reason": "done_for_day", "run_date": target_date}

    # ==========================================
    # 2. BACKFILL JOB LOGIC (Full historical pulls)
    # ==========================================
    else:
        item = TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")
        if not item:
            logger.warning(f"[orchestrator] state missing job={job_start_id}")
            _disable_schedule_rule_if_configured(ORCHESTRATOR_RULE_NAME, "missing_state")
            return {"enqueued": False, "reason": "missing_state"}

        status = item.get("status")
        if status in ["DONE", "ERROR"]:
            _disable_schedule_rule_if_configured(ORCHESTRATOR_RULE_NAME, f"status_{status}")
            return {"enqueued": False, "reason": f"status={status}"}

    # ==========================================
    # 3. COMMON LOGIC (Lease & Enqueue)
    # ==========================================
    status = item.get("status")
    if status != "RUNNING":
        return {"enqueued": False, "reason": f"status={status}"}

    in_flight = bool(item.get("in_flight", False))
    lease_until = int(item.get("lease_until", 0))

    if in_flight and lease_until > now:
        return {"enqueued": False, "reason": "lease_active"}

    # Acquire Lease (15 min)
    new_lease = now + 900
    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            ConditionExpression="attribute_not_exists(#in_flight) OR #in_flight = :false OR #lease_until < :now",
            UpdateExpression="SET #in_flight = :true, #lease_until = :lease, #updated_at = :now",
            ExpressionAttributeNames={
                "#in_flight": "in_flight",
                "#lease_until": "lease_until",
                "#updated_at": "updated_at",
            },
            ExpressionAttributeValues={
                ":false": False,
                ":true": True,
                ":now": now,
                ":lease": new_lease,
            },
        )
    except ClientError:
        logger.info("[orchestrator] lease not acquired")
        return {"enqueued": False, "reason": "lease_not_acquired"}

    # Prepare Message
    page = int(item.get("page", 1))
    cursor = item.get("cursor")

    body = {"job_start_id": job_start_id, "page": page}
    if cursor:
        body["cursor"] = cursor

    # Pass target_date to daily workers so they know what to filter
    if IS_DAILY:
        body["target_date"] = item.get("run_date")

    try:
        _sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(body))
        logger.info(f"[orchestrator] ENQUEUED job={job_start_id} page={page}")
        return {"enqueued": True, "job_start_id": job_start_id, "page": page}
    except Exception as e:
        # Release lease on error
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            UpdateExpression="SET #in_flight = :false, #lease_until = :zero",
            ExpressionAttributeNames={
                "#in_flight": "in_flight",
                "#lease_until": "lease_until",
            },
            ExpressionAttributeValues={
                ":false": False,
                ":zero": 0,
            },
        )
        raise