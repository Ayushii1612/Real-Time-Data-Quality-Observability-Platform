import json
from datetime import datetime, timedelta
from typing import Optional
import redis
import sqlalchemy as sa
from config.settings import REDIS_URL, DATABASE_URL, SLA_PREDICTION_MINUTES
from config.logging_config import get_logger

logger = get_logger(__name__)
engine = sa.create_engine(DATABASE_URL)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

SLA_DDL = """
CREATE TABLE IF NOT EXISTS sla_definitions (
    pipeline        VARCHAR(128) PRIMARY KEY,
    deadline_utc    TIME NOT NULL,
    priority        INTEGER DEFAULT 2,     -- 1=CRITICAL, 2=HIGH, 3=MEDIUM
    cost_per_hour   FLOAT DEFAULT 1000.0,  -- breach cost USD/hr
    owner_team      VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS sla_events (
    id          SERIAL PRIMARY KEY,
    pipeline    VARCHAR(128),
    event_type  VARCHAR(64),    -- STARTED, COMPLETED, BREACHED, PREDICTED_BREACH
    ts          TIMESTAMP DEFAULT NOW(),
    details     JSONB
);
"""

SAMPLE_SLA_DEFINITIONS = [
    ("orders_etl",        "06:00:00", 1, 5000.0,  "data-eng"),
    ("payments_stream",   "07:30:00", 1, 8000.0,  "payments-team"),
    ("report.executive_kpis", "08:00:00", 1, 12000.0, "analytics"),
    ("inventory_sync",    "09:00:00", 2, 2000.0,  "ops-team"),
    ("fraud_detection",   "05:30:00", 1, 15000.0, "security"),
]

def init_sla_db():
    with engine.connect() as conn:
        conn.execute(sa.text(SLA_DDL))
        conn.commit()
    # Seed SLA definitions
    with engine.begin() as conn:
        for pipeline, deadline, priority, cost, team in SAMPLE_SLA_DEFINITIONS:
            conn.execute(sa.text("""
                INSERT INTO sla_definitions (pipeline, deadline_utc, priority, cost_per_hour, owner_team)
                VALUES (:p, :d, :pr, :c, :t)
                ON CONFLICT (pipeline) DO NOTHING
            """), {"p": pipeline, "d": deadline, "pr": priority, "c": cost, "t": team})
    logger.info("[SLA] Database initialized with sample SLA definitions")

def get_sla_definition(pipeline: str) -> Optional[dict]:
    with engine.connect() as conn:
        row = conn.execute(sa.text("""
            SELECT pipeline, deadline_utc, priority, cost_per_hour, owner_team
            FROM sla_definitions WHERE pipeline=:p
        """), {"p": pipeline}).fetchone()
    if row:
        return {"pipeline": row.pipeline, "deadline": str(row.deadline_utc),
                "priority": row.priority, "cost_per_hour": row.cost_per_hour, "team": row.owner_team}
    return None

def record_pipeline_start(pipeline: str):
    with engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO sla_events (pipeline, event_type, ts)
            VALUES (:p, 'STARTED', NOW())
        """), {"p": pipeline})
    redis_client.setex(f"sla:start:{pipeline}", 86400, datetime.utcnow().isoformat())
    logger.info(f"[SLA] Pipeline started: {pipeline}")

def record_pipeline_complete(pipeline: str):
    start_str = redis_client.get(f"sla:start:{pipeline}")
    if not start_str:
        logger.warning(f"[SLA] No start time found for {pipeline}")
        return

    start_time = datetime.fromisoformat(start_str)
    duration_minutes = (datetime.utcnow() - start_time).total_seconds() / 60

    sla = get_sla_definition(pipeline)
    status = "ON_TIME"
    if sla:
        deadline = datetime.combine(datetime.utcnow().date(),
                                    datetime.strptime(sla["deadline"], "%H:%M:%S").time())
        if datetime.utcnow() > deadline:
            status = "BREACHED"
            logger.error(f"[SLA] BREACH — {pipeline} missed deadline {sla['deadline']}")

    with engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO sla_events (pipeline, event_type, ts, details)
            VALUES (:p, :et, NOW(), :d)
        """), {"p": pipeline, "et": "COMPLETED",
               "d": json.dumps({"duration_minutes": round(duration_minutes, 2), "status": status})})

def predict_sla_breach(pipeline: str, current_progress_pct: float) -> dict:
    """
    Predict if pipeline will breach SLA based on current progress.
    Uses linear extrapolation of elapsed time vs progress.
    Returns: {'will_breach': bool, 'minutes_until_deadline': float, 'risk_level': str}
    """
    start_str = redis_client.get(f"sla:start:{pipeline}")
    sla = get_sla_definition(pipeline)

    if not start_str or not sla or current_progress_pct <= 0:
        return {"will_breach": False, "risk_level": "UNKNOWN"}

    start_time = datetime.fromisoformat(start_str)
    elapsed_minutes = (datetime.utcnow() - start_time).total_seconds() / 60
    deadline = datetime.combine(datetime.utcnow().date(),
                                datetime.strptime(sla["deadline"], "%H:%M:%S").time())
    minutes_until_deadline = (deadline - datetime.utcnow()).total_seconds() / 60

    # Linear extrapolation: estimated_total = elapsed / progress_pct
    if current_progress_pct < 100:
        estimated_total = elapsed_minutes / (current_progress_pct / 100)
        estimated_remaining = estimated_total - elapsed_minutes
    else:
        estimated_remaining = 0

    will_breach = estimated_remaining > minutes_until_deadline
    risk_level = "HIGH" if will_breach else ("MEDIUM" if minutes_until_deadline < SLA_PREDICTION_MINUTES else "LOW")

    result = {
        "pipeline": pipeline,
        "will_breach": will_breach,
        "minutes_until_deadline": round(minutes_until_deadline, 1),
        "estimated_remaining_minutes": round(estimated_remaining, 1),
        "current_progress_pct": current_progress_pct,
        "risk_level": risk_level,
        "hourly_breach_cost": sla["cost_per_hour"]
    }

    if will_breach:
        logger.error(f"[SLA] PREDICTED BREACH — {pipeline} — deadline in {minutes_until_deadline:.1f}min, need {estimated_remaining:.1f}min more")
        redis_client.setex(f"sla:predicted_breach:{pipeline}", 3600, json.dumps(result))

    return result