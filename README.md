# Real-Time Data Quality & Observability Platform

Monitors 47+ pipelines in real-time using Spark Streaming + Great Expectations.
Implements seasonal anomaly detection, end-to-end lineage tracking, and predictive SLA monitoring.

## Quick Start
```bash
git clone <repo>
cd data-observability-platform
cp .env.example .env          # add your ANTHROPIC_API_KEY
docker-compose up -d
```

Services:
- API:      http://localhost:8000/docs
- Grafana:  http://localhost:3000  (admin/admin)
- Airflow:  http://localhost:8080

## Key API Endpoints
- `GET /pipeline/{name}/health`      — Real-time pipeline status
- `GET /incidents/active`            — All open incidents
- `GET /lineage/{table}/impact`      — Downstream blast radius
- `GET /sla/{pipeline}/status`       — SLA breach prediction
- `GET /anomalies/recent`            — Latest anomaly events
- `POST /incidents/{id}/resolve`     — Resolve + auto post-mortem

## Architecture
6-layer observability stack:
1. Data Profiling — row counts, null rates, schema drift
2. Anomaly Detection — z-score + KS-test + business rules
3. Lineage Tracking — source-to-consumer graph
4. SLA Monitoring — 45-minute breach prediction
5. Incident Management — auto-create, notify, post-mortem via Claude API
6. Grafana Dashboards — health heatmap, SLA trends, MTTR tracking
```

---

## 📋 How To Create This In VS Code

**Step 1 — Create the root folder**
```
File → Open Folder → Create New Folder → name it: data-observability-platform
```

**Step 2 — Create all subfolders** (right-click in Explorer → New Folder):
```
config/
ingestion/
profiling/
anomaly/
lineage/
sla/
incidents/
api/
airflow/dags/
grafana/dashboards/
terraform/
tests/