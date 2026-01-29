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

STREAM_NAME = os.environ.get("STREAM_NAME", "").strip().lower()
ORCHESTRATOR_RULE_NAME = os.environ.get("ORCHESTRATOR_RULE_NAME", "").strip()

# ---- Scheduled daily config (UNCHANGED PATTERN) ----
SCHEDULE_CONFIG = {
    "customers": {"start_hour": 2, "days_back": 1},
    "tickets": {"start_hour": 3, "days_back": 1},
    # "messages": {"start_hour": 4, "days_back": 1},  # leave disabled unless you want daily too
    "satisfaction_surveys": {"start_hour": 5, "days_back": 1},
    "users": {"start_hour": 6, "days_back": 1},
}

DEFAULT_START_HOUR = int(os.environ.get("DAILY_START_HOUR", "2"))
DEFAULT_DAYS_BACK = int(os.environ.get("DAILY_DAYS_BACK", "1"))

# ---- Watchdog thresholds ----
STALE_SECONDS = int(os.environ.get("STALE_SECONDS", "180"))            # consider stuck if no update in 3 min
POKE_COOLDOWN_SECONDS = int(os.environ.get("POKE_COOLDOWN_SECONDS", "60"))

# ---- Backfill auto-seed (so you never manually create DDB state) ----
BACKFILL_AUTO_SEED = os.environ.get("BACKFILL_AUTO_SEED", "true").lower() == "true"
BACKFILL_START_CURSOR = os.environ.get("BACKFILL_START_CURSOR", "").strip() or None
BACKFILL_START_PAGE = int(os.environ.get("BACKFILL_START_PAGE", "1"))

# If true: when we seed a missing backfill job, we immediately enqueue once.
BACKFILL_FORCE_FIRST_POKE = os.environ.get("BACKFILL_FORCE_FIRST_POKE", "true").lower() == "true"

def _disable_rule(reason: str):
    if not ORCHESTRATOR_RULE_NAME:
        return
    try:
        _events.disable_rule(Name=ORCHESTRATOR_RULE_NAME)
        logger.info(f"[orchestrator] disabled rule={ORCHESTRATOR_RULE_NAME} reason={reason}")
    except ClientError as e:
        logger.warning(f"[orchestrator] failed to disable rule: {e}")

def _get_state(job_start_id: str):
    return TABLE.get_item(Key={"job_start_id": job_start_id}).get("Item")

def _seed_state(job_start_id: str, run_reference: str | None = None, cursor: str | None = None, page: int = 1):
    item = {
        "job_start_id": job_start_id,
        "status": "RUNNING",
        "page": page,
        "in_flight": False,
        "lease_until": 0,
        "updated_at": int(time.time()),
        "last_poke_at": 0,
    }
    if run_reference:
        item["run_reference"] = run_reference
    if cursor:
        item["cursor"] = cursor

    TABLE.put_item(Item=item)

def _reset_for_new_window(job_start_id: str, target_reference: str):
    TABLE.update_item(
        Key={"job_start_id": job_start_id},
        UpdateExpression=(
            "SET #status=:r, #page=:p, #run_ref=:ref, #lease_until=:z, #in_flight=:f, #updated_at=:u "
            "REMOVE #cursor, #lease_owner"
        ),
        ExpressionAttributeNames={
            "#status": "status",
            "#page": "page",
            "#run_ref": "run_reference",
            "#lease_until": "lease_until",
            "#in_flight": "in_flight",
            "#updated_at": "updated_at",
            "#cursor": "cursor",
            "#lease_owner": "lease_owner",
        },
        ExpressionAttributeValues={
            ":r": "RUNNING",
            ":p": 1,
            ":ref": target_reference,
            ":z": 0,
            ":f": False,
            ":u": int(time.time()),
        },
    )

def _should_poke(state: dict, now: int):
    """
    Poke when:
      - not in_flight (worker idle)
      - lease expired (worker died)
      - stale updated_at (worker stuck)
    """
    in_flight = bool(state.get("in_flight", False))
    lease_until = int(state.get("lease_until", 0) or 0)
    updated_at = int(state.get("updated_at", 0) or 0)

    stale = (updated_at == 0) or (updated_at < (now - STALE_SECONDS))
    lease_expired = lease_until <= now

    if not in_flight:
        return True, "not_in_flight"
    if lease_expired:
        return True, "lease_expired"
    if stale:
        return True, "stale"
    return False, "healthy"

def _poke_worker(job_start_id: str, state: dict, is_scheduled: bool):
    page = int(state.get("page", 1))
    cursor = state.get("cursor")

    body = {"job_start_id": job_start_id, "page": page}
    if cursor:
        body["cursor"] = cursor
    if is_scheduled:
        body["target_date"] = state.get("run_reference") or state.get("run_date")

    _sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(body))

def handler(event, context):
    job_start_id = event.get("job_start_id")
    if not job_start_id:
        raise ValueError("Missing job_start_id")

    IS_SCHEDULED = "daily" in job_start_id
    now = int(time.time())
    now_dt = datetime.now(timezone.utc)

    # -------------------------
    # 1) Scheduled daily logic
    # -------------------------
    if IS_SCHEDULED:
        cfg = SCHEDULE_CONFIG.get(STREAM_NAME, {})
        if not cfg:
            logger.info(f"[orchestrator] schedule disabled for stream={STREAM_NAME}")
            return {"enqueued": False, "reason": "schedule_disabled"}

        start_hour = int(cfg.get("start_hour", DEFAULT_START_HOUR))
        days_back = int(cfg.get("days_back", DEFAULT_DAYS_BACK))

        if now_dt.hour != start_hour:
            return {"enqueued": False, "reason": "outside_daily_window"}

        target_reference = (now_dt - timedelta(days=days_back)).strftime("%Y-%m-%d")

        state = _get_state(job_start_id)
        if not state:
            logger.info(f"[orchestrator] seeding daily job={job_start_id} ref={target_reference}")
            _seed_state(job_start_id, run_reference=target_reference, page=1)
            state = _get_state(job_start_id)

        stored_ref = state.get("run_reference") or state.get("run_date", "")
        if stored_ref != target_reference:
            logger.info(f"[orchestrator] new daily window job={job_start_id} ref={target_reference}")
            _reset_for_new_window(job_start_id, target_reference)
            state = _get_state(job_start_id)

        if state.get("status") == "DONE":
            return {"enqueued": False, "reason": "done_for_window"}

    # -------------------------
    # 2) Backfill logic (AUTO-SEED)
    # -------------------------
    else:
        state = _get_state(job_start_id)

        if not state:
            if not BACKFILL_AUTO_SEED:
                _disable_rule("missing_state")
                return {"enqueued": False, "reason": "missing_state_disabled"}

            logger.info(f"[orchestrator] backfill missing_state -> seeding job={job_start_id}")
            _seed_state(job_start_id, cursor=BACKFILL_START_CURSOR, page=BACKFILL_START_PAGE)
            state = _get_state(job_start_id)

            if BACKFILL_FORCE_FIRST_POKE:
                _poke_worker(job_start_id, state, is_scheduled=False)
                try:
                    TABLE.update_item(
                        Key={"job_start_id": job_start_id},
                        UpdateExpression="SET last_poke_at=:t",
                        ExpressionAttributeValues={":t": now},
                    )
                except Exception:
                    pass
                return {"enqueued": True, "reason": "seeded_force_poke"}

        status = state.get("status")
        if status in ("DONE", "ERROR"):
            _disable_rule(f"status_{status}")
            return {"enqueued": False, "reason": f"status={status}"}

    # -------------------------
    # 3) Watchdog poke
    # -------------------------
    state = state or _get_state(job_start_id)
    if not state:
        return {"enqueued": False, "reason": "state_missing"}

    if state.get("status") != "RUNNING":
        return {"enqueued": False, "reason": f"status={state.get('status')}"}

    last_poke_at = int(state.get("last_poke_at", 0) or 0)
    if last_poke_at and (now - last_poke_at) < POKE_COOLDOWN_SECONDS:
        return {"enqueued": False, "reason": "poke_cooldown"}

    needs_poke, why = _should_poke(state, now)
    if not needs_poke:
        return {"enqueued": False, "reason": why}

    _poke_worker(job_start_id, state, IS_SCHEDULED)

    try:
        TABLE.update_item(
            Key={"job_start_id": job_start_id},
            UpdateExpression="SET last_poke_at=:t",
            ExpressionAttributeValues={":t": now},
        )
    except Exception:
        pass

    logger.info(
        f"[orchestrator] poked job={job_start_id} reason={why} page={state.get('page')} "
        f"cursor={'yes' if state.get('cursor') else 'no'}"
    )
    return {"enqueued": True, "reason": why}
