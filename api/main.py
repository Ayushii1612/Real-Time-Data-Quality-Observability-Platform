import json
from typing import List, Optional
from datetime import datetime
import redis
import sqlalchemy as sa
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from config.settings import REDIS_URL, DATABASE_URL
from config.logging_config import get_logger
from api.models import PipelineHealth, ActiveIncident, LineageImpact, SLAStatus
from lineage.lineage_tracker import get_downstream_impact
from sla.sla_monitor import get_sla_definition, predict_sla_breach

logger = get_logger(__name__)
app = FastAPI(
    title="Data Observability Platform API",
    description="Real-time data quality monitoring, lineage tracking, and SLA management",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

engine = sa.create_engine(DATABASE_URL)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)


@app.get("/health")
def health_check():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/pipeline/{name}/health", response_model=PipelineHealth)
def get_pipeline_health(name: str):
    """Get real-time health status for a pipeline."""
    # Latest profile from Redis cache
    profile_raw = redis_client.get(f"profile:latest:{name}")
    profile = json.loads(profile_raw) if profile_raw else {}

    # Active anomalies
    anomaly_raw = redis_client.get(f"anomaly:active:{name}")
    anomaly = json.loads(anomaly_raw) if anomaly_raw else None

    # Active incidents
    incident_raw = redis_client.get(f"incident:active:{name}")
    incident = json.loads(incident_raw) if incident_raw else None

    # SLA prediction
    sla_def = get_sla_definition(name)
    sla_risk = "UNKNOWN"
    if sla_def:
        prediction = predict_sla_breach(name, current_progress_pct=50.0)
        sla_risk = prediction.get("risk_level", "LOW")

    # Overall status
    if incident and incident.get("severity") == "CRITICAL":
        status = "RED"
    elif anomaly:
        status = "YELLOW"
    else:
        status = "GREEN"

    return PipelineHealth(
        pipeline=name,
        status=status,
        last_row_count=profile.get("row_count"),
        freshness_lag_seconds=profile.get("freshness_lag_s"),
        active_anomalies=1 if anomaly else 0,
        active_incidents=1 if incident else 0,
        sla_risk_level=sla_risk,
        last_updated=profile.get("ts", datetime.utcnow().isoformat())
    )


@app.get("/incidents/active", response_model=List[ActiveIncident])
def get_active_incidents(severity: Optional[str] = Query(None, description="Filter by severity")):
    """Get all active incidents."""
    query = "SELECT * FROM incidents WHERE status='OPEN'"
    params = {}
    if severity:
        query += " AND severity=:severity"
        params["severity"] = severity.upper()
    query += " ORDER BY opened_at DESC LIMIT 50"

    with engine.connect() as conn:
        rows = conn.execute(sa.text(query), params).fetchall()

    return [ActiveIncident(
        id=r.id, pipeline=r.pipeline, severity=r.severity,
        title=r.title, root_cause=r.root_cause or "",
        opened_at=r.opened_at, status=r.status
    ) for r in rows]


@app.get("/lineage/{table}/impact", response_model=LineageImpact)
def get_lineage_impact(table: str, depth: int = Query(5, ge=1, le=10)):
    """Get downstream impact of a table/pipeline failure."""
    downstream = get_downstream_impact(table, depth=depth)
    return LineageImpact(
        source_table=table,
        downstream_nodes=downstream,
        total_impacted=len(downstream)
    )


@app.get("/sla/{pipeline}/status", response_model=SLAStatus)
def get_sla_status(pipeline: str, progress_pct: float = Query(50.0, ge=0, le=100)):
    """Get real-time SLA status and breach prediction."""
    sla_def = get_sla_definition(pipeline)
    if not sla_def:
        raise HTTPException(status_code=404, detail=f"No SLA definition for pipeline: {pipeline}")

    prediction = predict_sla_breach(pipeline, progress_pct)
    return SLAStatus(
        pipeline=pipeline,
        will_breach=prediction.get("will_breach", False),
        minutes_until_deadline=prediction.get("minutes_until_deadline"),
        risk_level=prediction.get("risk_level", "UNKNOWN"),
        current_progress_pct=progress_pct,
        hourly_breach_cost=sla_def.get("cost_per_hour")
    )


@app.get("/anomalies/recent")
def get_recent_anomalies(limit: int = Query(20, ge=1, le=100)):
    """Get most recent anomaly events."""
    with engine.connect() as conn:
        rows = conn.execute(sa.text("""
            SELECT pipeline, metric_name, severity, z_score, observed, expected, ts
            FROM anomaly_events
            ORDER BY ts DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()
    return [dict(r._mapping) for r in rows]


@app.post("/incidents/{incident_id}/resolve")
def resolve_incident_endpoint(incident_id: str):
    """Resolve an incident and trigger post-mortem generation."""
    from incidents.incident_manager import resolve_incident
    result = resolve_incident(incident_id)
    return result


@app.get("/pipelines/overview")
def get_all_pipelines_overview():
    """Grafana-compatible endpoint: all pipeline statuses in one call."""
    with engine.connect() as conn:
        rows = conn.execute(sa.text("""
            SELECT DISTINCT pipeline FROM pipeline_profiles
            ORDER BY pipeline
        """)).fetchall()

    pipelines = [r.pipeline for r in rows]
    results = []
    for p in pipelines:
        try:
            health = get_pipeline_health(p)
            results.append(health.model_dump())
        except Exception:
            pass
    return results