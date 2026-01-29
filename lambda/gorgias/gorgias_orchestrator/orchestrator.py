"""
orchestrator.py — Scheduling & Backfill Logic

Handles:
1. Daily triggers (checking time windows).
2. One-off Backfills (disabling rules when done).
3. State management & Locking.
"""

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
# Note: The Orchestrator expects "BACKFILL_QUEUE_URL" env var to be set 
# to the correct queue for the specific lambda instance.
QUEUE_URL = os.environ["BACKFILL_QUEUE_URL"] 
TABLE = _ddb.Table(STATE_TABLE)

# --- Configuration ---
# "frequency": "daily" -> Checks 'start_hour'. Target is Yesterday.
SCHEDULE_CONFIG = {
    "customers": {
        "frequency": "daily",
        "start_hour": 2,   # 02:00 UTC
        "days_back": 1,
    },
    "tickets": {
        "frequency": "daily",
        "start_hour": 3,   # 03:00 UTC
        "days_back": 1,
    },
    # "messages": {
    #     # DISABLED to allow manual backfill to run without collision.
    #     # Uncomment when backfill is done to resume daily schedule.
    #     "frequency": "daily",
    #     "start_hour": 4,   # 04:00 UTC
    #     "days_back": 1,
    # },
    "satisfaction_surveys": {
        "frequency": "daily",
        "start_hour": 5,   # 05:00 UTC
        "days_back": 1,
    },
    "users": {
        "frequency": "daily",
        "start_hour": 6,   # 06:00 UTC
        "days_back": 1,
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

    # Determine Mode based on job ID string
    IS_SCHEDULED = "daily" in job_start_id
    
    ORCHESTRATOR_RULE_NAME = os.environ.get("ORCHESTRATOR_RULE_NAME", "").strip()
    STREAM_NAME = os.environ.get("STREAM_NAME", "").strip().lower()

    now = int(time.time())
    now_dt = datetime.now(timezone.utc)

    # ==========================================
    # 1. SCHEDULED JOB LOGIC (Daily)
    # ==========================================
    if IS_SCHEDULED:
        config = SCHEDULE_CONFIG.get(STREAM_NAME, {})
        if not config:
            # If config is missing (e.g., messages commented out), we skip nicely.
            logger.info(f"[orchestrator] Schedule disabled for {STREAM_NAME}. Skipping.")
            return {"enqueued": False, "reason": "schedule_disabled"}

        start_hour = config.get("start_hour", DEFAULT_START_HOUR)
        days_back = config.get("days_back", DEFAULT_DAYS_BACK)

        # Strict Window Check: Only run if current hour matches start_hour
        if now_dt.hour != start_hour:
            return {
                "enqueued": False,
                "reason": "outside_daily_window",
                "stream": STREAM_NAME,
                "current_hour": now_dt.hour,
                "expected_hour": start_hour,
            }
        
        # Target: Yesterday (e.g., "2026-01-29")
        target_reference = (now_dt - timedelta(days=days_back)).strftime("%Y-%m-%d")

        # --- C. STATE MANAGEMENT ---
        item = TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")

        # Seed state if missing
        if not item:
            logger.info(f"[orchestrator] Seeding new job {job_start_id} for {target_reference}")
            TABLE.put_item(Item={
                "job_start_id": job_start_id,
                "status": "RUNNING",
                "page": 1,
                "in_flight": False,
                "lease_until": 0,
                "run_reference": target_reference, 
            })
            item = TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")

        # CHECK FOR NEW WINDOW -> RESET STATE
        stored_ref = item.get("run_reference") or item.get("run_date", "")

        if stored_ref != target_reference:
            logger.info(f"[orchestrator] New window! Resetting {job_start_id} to {target_reference}")
            TABLE.update_item(
                Key={"job_start_id": job_start_id},
                UpdateExpression="SET #status=:r, #page=:p, #run_ref=:ref, #lease_until=:z REMOVE #cursor",
                ExpressionAttributeNames={
                    "#status": "status",
                    "#page": "page",
                    "#run_ref": "run_reference", 
                    "#lease_until": "lease_until",
                    "#cursor": "cursor",
                },
                ExpressionAttributeValues={
                    ":r": "RUNNING",
                    ":p": 1,
                    ":ref": target_reference,
                    ":z": 0,
                },
            )
            item = TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")

        # If already DONE for this specific window, return
        if item.get("status") == "DONE":
            return {"enqueued": False, "reason": "done_for_window", "reference": target_reference}

    # ==========================================
    # 2. BACKFILL JOB LOGIC (One-off / Continuous)
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
                ":lease": now + 900, 
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

    # Pass the calculated target (Day) to the worker if scheduled
    if IS_SCHEDULED:
        body["target_date"] = item.get("run_reference") or item.get("run_date")

    try:
        _sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(body))
        logger.info(f"[orchestrator] ENQUEUED job={job_start_id} page={page}")
        return {"enqueued": True, "job_start_id": job_start_id, "page": page}
    except Exception as e:
        # Release lease on error
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            UpdateExpression="SET #in_flight = :false, #lease_until = :zero",
            ExpressionAttributeNames={"#in_flight": "in_flight", "#lease_until": "lease_until"},
            ExpressionAttributeValues={":false": False, ":zero": 0},
        )
        raise