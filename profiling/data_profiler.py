import json
from datetime import datetime
from typing import Optional
import redis
import sqlalchemy as sa
from sqlalchemy.orm import Session
from config.settings import REDIS_URL, DATABASE_URL, BASELINE_WINDOW_DAYS
from config.logging_config import get_logger

logger = get_logger(__name__)

# SQLAlchemy setup
engine = sa.create_engine(DATABASE_URL)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

PROFILES_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_profiles (
    id          SERIAL PRIMARY KEY,
    pipeline    VARCHAR(128) NOT NULL,
    batch_id    VARCHAR(256) NOT NULL,
    ts          TIMESTAMP DEFAULT NOW(),
    row_count   INTEGER,
    null_rates  JSONB,
    schema_ver  VARCHAR(64),
    freshness_lag_s INTEGER,
    duplicate_rate FLOAT
);

CREATE TABLE IF NOT EXISTS baselines (
    pipeline        VARCHAR(128) NOT NULL,
    metric_name     VARCHAR(128) NOT NULL,
    seasonal_slot   INTEGER NOT NULL,   -- 0-167 (hour of week)
    mean            FLOAT NOT NULL,
    stddev          FLOAT NOT NULL DEFAULT 1.0,
    sample_count    INTEGER DEFAULT 1,
    last_updated    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (pipeline, metric_name, seasonal_slot)
);
"""

def init_db():
    with engine.connect() as conn:
        conn.execute(sa.text(PROFILES_DDL))
        conn.commit()
    logger.info("Database tables initialized")

def _get_seasonal_slot(ts: Optional[datetime] = None) -> int:
    """Return hour-of-week slot 0-167 for seasonal baseline lookup."""
    t = ts or datetime.utcnow()
    return t.weekday() * 24 + t.hour

def store_profile(pipeline: str, metrics: dict):
    """Persist a profiled micro-batch to PostgreSQL and cache in Redis."""
    ts = datetime.utcnow()
    m = metrics.get("metrics", {})

    with engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO pipeline_profiles
                (pipeline, batch_id, ts, row_count, null_rates, schema_ver, freshness_lag_s, duplicate_rate)
            VALUES
                (:pipeline, :batch_id, :ts, :row_count, :null_rates, :schema_ver, :freshness_lag_s, :duplicate_rate)
        """), {
            "pipeline": pipeline,
            "batch_id": metrics.get("batch_id", "unknown"),
            "ts": ts,
            "row_count": m.get("row_count"),
            "null_rates": json.dumps(m.get("null_rates", {})),
            "schema_ver": m.get("schema_version"),
            "freshness_lag_s": m.get("data_freshness_lag_seconds"),
            "duplicate_rate": m.get("duplicate_rate")
        })

    # Cache latest profile in Redis for fast API access
    redis_key = f"profile:latest:{pipeline}"
    redis_client.setex(redis_key, 300, json.dumps({
        "pipeline": pipeline,
        "ts": ts.isoformat(),
        "row_count": m.get("row_count"),
        "freshness_lag_s": m.get("data_freshness_lag_seconds"),
    }))
    logger.info(f"[Profiler] Stored profile for {pipeline} — rows={m.get('row_count')}")

def get_latest_profile(pipeline: str) -> Optional[dict]:
    data = redis_client.get(f"profile:latest:{pipeline}")
    return json.loads(data) if data else None