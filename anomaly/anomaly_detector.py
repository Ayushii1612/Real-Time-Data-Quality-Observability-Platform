import json
from datetime import datetime
from typing import Optional
from scipy import stats
import numpy as np
import redis
import sqlalchemy as sa
from config.settings import (
    DATABASE_URL, REDIS_URL,
    ANOMALY_Z_SCORE_WARNING, ANOMALY_Z_SCORE_CRITICAL,
    KS_TEST_P_VALUE_THRESHOLD
)
from config.logging_config import get_logger
from profiling.baseline_manager import get_baseline, update_baseline

logger = get_logger(__name__)
engine = sa.create_engine(DATABASE_URL)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

ANOMALY_DDL = """
CREATE TABLE IF NOT EXISTS anomaly_events (
    id          SERIAL PRIMARY KEY,
    pipeline    VARCHAR(128),
    metric_name VARCHAR(128),
    severity    VARCHAR(32),
    z_score     FLOAT,
    observed    FLOAT,
    expected    FLOAT,
    seasonal_slot INTEGER,
    ts          TIMESTAMP DEFAULT NOW(),
    resolved    BOOLEAN DEFAULT FALSE
);
"""

def init_anomaly_db():
    with engine.connect() as conn:
        conn.execute(sa.text(ANOMALY_DDL))
        conn.commit()

def classify_severity(z_score: float) -> str:
    if abs(z_score) >= ANOMALY_Z_SCORE_CRITICAL:
        return "CRITICAL"
    elif abs(z_score) >= ANOMALY_Z_SCORE_WARNING:
        return "WARNING"
    return "NORMAL"

def z_score_check(pipeline: str, metric_name: str, value: float) -> Optional[dict]:
    """Check value against seasonal baseline using z-score."""
    slot = datetime.utcnow().weekday() * 24 + datetime.utcnow().hour
    baseline = get_baseline(pipeline, metric_name, slot)

    if not baseline or baseline["count"] < 10:
        # Not enough data to detect anomalies yet
        update_baseline(pipeline, metric_name, value)
        return None

    z = (value - baseline["mean"]) / baseline["stddev"]
    severity = classify_severity(z)

    update_baseline(pipeline, metric_name, value)

    if severity != "NORMAL":
        event = {
            "pipeline": pipeline,
            "metric_name": metric_name,
            "severity": severity,
            "z_score": round(z, 3),
            "observed": value,
            "expected": round(baseline["mean"], 2),
            "seasonal_slot": slot,
            "ts": datetime.utcnow().isoformat()
        }
        _persist_anomaly(event)
        _cache_anomaly(event)
        logger.warning(f"[Anomaly] {severity} — {pipeline}/{metric_name} z={z:.2f} observed={value} expected={baseline['mean']:.2f}")
        return event

    return None

def ks_test_distribution_drift(pipeline: str, current_values: list, metric_name: str) -> bool:
    """
    Kolmogorov-Smirnov test: compare current batch distribution
    against 30-day historical distribution stored in Redis.
    """
    hist_key = f"dist_history:{pipeline}:{metric_name}"
    hist_raw = redis_client.lrange(hist_key, 0, 500)

    if len(hist_raw) < 50:
        # Push current batch values to history
        for v in current_values:
            redis_client.rpush(hist_key, v)
        redis_client.ltrim(hist_key, -500, -1)
        redis_client.expire(hist_key, 86400 * 30)
        return False

    historical = [float(x) for x in hist_raw]
    _, p_value = stats.ks_2samp(current_values, historical)

    if p_value < KS_TEST_P_VALUE_THRESHOLD:
        logger.warning(f"[KS-Test] Distribution drift detected in {pipeline}/{metric_name} p={p_value:.4f}")
        return True

    # Update history
    for v in current_values:
        redis_client.rpush(hist_key, v)
    redis_client.ltrim(hist_key, -500, -1)
    return False

def process_metrics_for_anomalies(pipeline: str, metrics: dict) -> list:
    """Main entry point: check all metrics in a batch for anomalies."""
    anomalies = []
    m = metrics.get("metrics", {})

    # Row count anomaly
    if m.get("row_count"):
        result = z_score_check(pipeline, "row_count", float(m["row_count"]))
        if result:
            anomalies.append(result)

    # Null rate anomalies per column
    for col, null_rate in m.get("null_rates", {}).items():
        result = z_score_check(pipeline, f"null_rate_{col}", float(null_rate))
        if result:
            anomalies.append(result)

    # Freshness lag
    if m.get("data_freshness_lag_seconds"):
        result = z_score_check(pipeline, "freshness_lag", float(m["data_freshness_lag_seconds"]))
        if result:
            anomalies.append(result)

    return anomalies

def _persist_anomaly(event: dict):
    with engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO anomaly_events
                (pipeline, metric_name, severity, z_score, observed, expected, seasonal_slot, ts)
            VALUES
                (:pipeline, :metric_name, :severity, :z_score, :observed, :expected, :seasonal_slot, :ts)
        """), event)

def _cache_anomaly(event: dict):
    key = f"anomaly:active:{event['pipeline']}"
    redis_client.setex(key, 3600, json.dumps(event))