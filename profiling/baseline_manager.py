import math
import sqlalchemy as sa
from datetime import datetime, timedelta
from config.settings import DATABASE_URL, BASELINE_UPDATE_ALPHA, SEASONAL_SLOTS
from config.logging_config import get_logger

logger = get_logger(__name__)
engine = sa.create_engine(DATABASE_URL)

def get_baseline(pipeline: str, metric_name: str, seasonal_slot: int) -> dict:
    """Fetch seasonal baseline for a specific hour-of-week slot."""
    with engine.connect() as conn:
        row = conn.execute(sa.text("""
            SELECT mean, stddev, sample_count
            FROM baselines
            WHERE pipeline=:p AND metric_name=:m AND seasonal_slot=:s
        """), {"p": pipeline, "m": metric_name, "s": seasonal_slot}).fetchone()

    if row:
        return {"mean": row.mean, "stddev": max(row.stddev, 1.0), "count": row.sample_count}
    return None

def update_baseline(pipeline: str, metric_name: str, value: float):
    """
    Exponential smoothing update for the current hour-of-week slot.
    mean_new = alpha * value + (1 - alpha) * mean_old
    """
    slot = datetime.utcnow().weekday() * 24 + datetime.utcnow().hour
    existing = get_baseline(pipeline, metric_name, slot)
    alpha = BASELINE_UPDATE_ALPHA

    if existing:
        new_mean = alpha * value + (1 - alpha) * existing["mean"]
        # Welford variance approximation via exponential smoothing
        diff = value - existing["mean"]
        new_var = (1 - alpha) * (existing["stddev"] ** 2 + alpha * diff ** 2)
        new_stddev = max(math.sqrt(new_var), 1.0)
        with engine.begin() as conn:
            conn.execute(sa.text("""
                UPDATE baselines
                SET mean=:mean, stddev=:stddev, sample_count=sample_count+1, last_updated=NOW()
                WHERE pipeline=:p AND metric_name=:m AND seasonal_slot=:s
            """), {"mean": new_mean, "stddev": new_stddev, "p": pipeline, "m": metric_name, "s": slot})
    else:
        # Seed baseline on first occurrence
        with engine.begin() as conn:
            conn.execute(sa.text("""
                INSERT INTO baselines (pipeline, metric_name, seasonal_slot, mean, stddev, sample_count)
                VALUES (:p, :m, :s, :mean, :stddev, 1)
                ON CONFLICT (pipeline, metric_name, seasonal_slot) DO NOTHING
            """), {"p": pipeline, "m": metric_name, "s": slot, "mean": value, "stddev": max(abs(value * 0.1), 10.0)})

    logger.debug(f"[Baseline] Updated {pipeline}/{metric_name} slot={slot}")