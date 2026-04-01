import json
import uuid
from datetime import datetime
import redis
import sqlalchemy as sa
import requests
import anthropic
from config.settings import (
    REDIS_URL, DATABASE_URL, ANTHROPIC_API_KEY, SLACK_WEBHOOK_URL
)
from config.logging_config import get_logger
from lineage.lineage_tracker import get_downstream_impact, get_upstream_lineage

logger = get_logger(__name__)
engine = sa.create_engine(DATABASE_URL)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

INCIDENT_DDL = """
CREATE TABLE IF NOT EXISTS incidents (
    id              VARCHAR(64) PRIMARY KEY,
    pipeline        VARCHAR(128),
    severity        VARCHAR(32),
    title           VARCHAR(512),
    root_cause      TEXT,
    status          VARCHAR(32) DEFAULT 'OPEN',
    time_to_detect  INTERVAL,
    time_to_resolve INTERVAL,
    opened_at       TIMESTAMP DEFAULT NOW(),
    resolved_at     TIMESTAMP,
    postmortem      TEXT,
    anomaly_data    JSONB
);
"""

def init_incident_db():
    with engine.connect() as conn:
        conn.execute(sa.text(INCIDENT_DDL))
        conn.commit()

def create_incident(pipeline: str, anomaly_event: dict) -> str:
    """Auto-create incident from anomaly detection. Returns incident ID."""
    incident_id = f"INC-{str(uuid.uuid4())[:8].upper()}"
    severity = anomaly_event.get("severity", "WARNING")
    metric = anomaly_event.get("metric_name", "unknown")
    z_score = anomaly_event.get("z_score", 0)

    # Get upstream cause candidates
    upstream = get_upstream_lineage(pipeline)
    root_cause_hypothesis = (
        f"Anomaly in {metric} (z={z_score}). "
        f"Possible upstream sources: {', '.join([u['node'] for u in upstream[:3]])}"
        if upstream else f"Anomaly in {metric} (z={z_score}). No upstream lineage found."
    )

    with engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO incidents
                (id, pipeline, severity, title, root_cause, anomaly_data)
            VALUES
                (:id, :pipeline, :severity, :title, :root_cause, :anomaly_data)
        """), {
            "id": incident_id,
            "pipeline": pipeline,
            "severity": severity,
            "title": f"{severity}: {metric} anomaly in {pipeline}",
            "root_cause": root_cause_hypothesis,
            "anomaly_data": json.dumps(anomaly_event)
        })

    # Cache active incident
    redis_client.setex(f"incident:active:{pipeline}", 86400, json.dumps({
        "id": incident_id, "severity": severity, "pipeline": pipeline,
        "ts": datetime.utcnow().isoformat()
    }))

    logger.error(f"[Incident] Created {incident_id} — {severity} on {pipeline}")

    # Notify downstream consumers
    _notify_downstream(pipeline, incident_id, severity)
    _send_slack_alert(pipeline, incident_id, severity, root_cause_hypothesis)

    return incident_id

def resolve_incident(incident_id: str) -> dict:
    """Mark incident as resolved and trigger post-mortem generation."""
    with engine.begin() as conn:
        conn.execute(sa.text("""
            UPDATE incidents
            SET status='RESOLVED', resolved_at=NOW(),
                time_to_resolve = NOW() - opened_at
            WHERE id=:id
        """), {"id": incident_id})

    # Fetch full incident data for post-mortem
    with engine.connect() as conn:
        row = conn.execute(sa.text(
            "SELECT * FROM incidents WHERE id=:id"
        ), {"id": incident_id}).fetchone()

    if row:
        postmortem = generate_postmortem(dict(row._mapping))
        with engine.begin() as conn:
            conn.execute(sa.text(
                "UPDATE incidents SET postmortem=:pm WHERE id=:id"
            ), {"pm": postmortem, "id": incident_id})
        logger.info(f"[Incident] Resolved {incident_id} — post-mortem generated")
        return {"id": incident_id, "postmortem": postmortem}

    return {"id": incident_id, "postmortem": "Incident not found"}

def generate_postmortem(incident: dict) -> str:
    """Use Claude API to auto-generate post-mortem report."""
    if not ANTHROPIC_API_KEY:
        return "Post-mortem generation requires ANTHROPIC_API_KEY"

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""Generate a concise data engineering post-mortem report for the following incident:

Pipeline: {incident.get('pipeline')}
Severity: {incident.get('severity')}
Title: {incident.get('title')}
Root Cause Hypothesis: {incident.get('root_cause')}
Anomaly Data: {incident.get('anomaly_data')}
Time to Detect: {incident.get('time_to_detect')}
Time to Resolve: {incident.get('time_to_resolve')}

Structure the post-mortem with sections:
1. Summary (2 sentences)
2. Timeline
3. Root Cause
4. Impact Assessment
5. Action Items (3-5 concrete items)
6. Prevention Measures

Be concise and technical."""

    try:
        message = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text
    except Exception as e:
        logger.error(f"[Incident] Post-mortem generation failed: {e}")
        return f"Post-mortem generation failed: {str(e)}"

def _notify_downstream(pipeline: str, incident_id: str, severity: str):
    """Notify all downstream consumer teams about the incident."""
    impacted = get_downstream_impact(pipeline)
    teams = set(item.get("owner_team") for item in impacted if item.get("owner_team"))
    if teams:
        logger.warning(f"[Incident] {incident_id} — downstream teams notified: {teams}")
    # In production: send per-team Slack/email notifications here

def _send_slack_alert(pipeline: str, incident_id: str, severity: str, root_cause: str):
    if not SLACK_WEBHOOK_URL:
        return
    payload = {
        "text": f"🚨 *{severity} INCIDENT* — `{incident_id}`\n"
                f"Pipeline: `{pipeline}`\n"
                f"Root cause: {root_cause[:200]}\n"
                f"Dashboard: http://localhost:3000"
    }
    try:
        requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")