from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class PipelineHealth(BaseModel):
    pipeline: str
    status: str
    last_row_count: Optional[int]
    freshness_lag_seconds: Optional[int]
    active_anomalies: int
    active_incidents: int
    sla_risk_level: str
    last_updated: str

class ActiveIncident(BaseModel):
    id: str
    pipeline: str
    severity: str
    title: str
    root_cause: str
    opened_at: datetime
    status: str

class LineageImpact(BaseModel):
    source_table: str
    downstream_nodes: List[dict]
    total_impacted: int

class SLAStatus(BaseModel):
    pipeline: str
    will_breach: bool
    minutes_until_deadline: Optional[float]
    risk_level: str
    current_progress_pct: Optional[float]
    hourly_breach_cost: Optional[float]