import json
import redis
import sqlalchemy as sa
from datetime import datetime
from config.settings import REDIS_URL, DATABASE_URL
from config.logging_config import get_logger

logger = get_logger(__name__)
engine = sa.create_engine(DATABASE_URL)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

LINEAGE_DDL = """
CREATE TABLE IF NOT EXISTS lineage_nodes (
    id          SERIAL PRIMARY KEY,
    node_id     VARCHAR(256) UNIQUE NOT NULL,
    node_type   VARCHAR(64),       -- TABLE, STREAM, PIPELINE, REPORT
    owner_team  VARCHAR(128),
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lineage_edges (
    id              SERIAL PRIMARY KEY,
    source_node     VARCHAR(256),
    target_node     VARCHAR(256),
    job_name        VARCHAR(256),
    job_version     VARCHAR(64),
    transform_type  VARCHAR(64),
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

def init_lineage_db():
    with engine.connect() as conn:
        conn.execute(sa.text(LINEAGE_DDL))
        conn.commit()

def register_node(node_id: str, node_type: str, owner_team: str = "unknown"):
    with engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO lineage_nodes (node_id, node_type, owner_team)
            VALUES (:node_id, :node_type, :owner_team)
            ON CONFLICT (node_id) DO NOTHING
        """), {"node_id": node_id, "node_type": node_type, "owner_team": owner_team})
    logger.info(f"[Lineage] Registered node: {node_id} ({node_type})")

def register_edge(source: str, target: str, job_name: str, job_version: str = "v1", transform_type: str = "ETL"):
    with engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO lineage_edges (source_node, target_node, job_name, job_version, transform_type)
            VALUES (:s, :t, :j, :v, :tt)
        """), {"s": source, "t": target, "j": job_name, "v": job_version, "tt": transform_type})
    # Invalidate cached downstream impact
    redis_client.delete(f"lineage:impact:{source}")
    logger.info(f"[Lineage] Edge registered: {source} → {target} via {job_name}")

def get_downstream_impact(node_id: str, depth: int = 5) -> list:
    """BFS traversal to find all downstream consumers of a node."""
    cache_key = f"lineage:impact:{node_id}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)

    visited = set()
    queue = [node_id]
    impact = []

    with engine.connect() as conn:
        for _ in range(depth):
            if not queue:
                break
            next_queue = []
            for current in queue:
                if current in visited:
                    continue
                visited.add(current)
                rows = conn.execute(sa.text("""
                    SELECT le.target_node, ln.owner_team, le.job_name
                    FROM lineage_edges le
                    LEFT JOIN lineage_nodes ln ON le.target_node = ln.node_id
                    WHERE le.source_node = :node
                """), {"node": current}).fetchall()

                for row in rows:
                    impact.append({
                        "node": row.target_node,
                        "owner_team": row.owner_team,
                        "job": row.job_name
                    })
                    next_queue.append(row.target_node)
            queue = next_queue

    redis_client.setex(cache_key, 300, json.dumps(impact))
    return impact

def get_upstream_lineage(node_id: str) -> list:
    """Trace upstream to find root causes."""
    with engine.connect() as conn:
        rows = conn.execute(sa.text("""
            SELECT le.source_node, ln.owner_team, le.job_name, le.job_version
            FROM lineage_edges le
            LEFT JOIN lineage_nodes ln ON le.source_node = ln.node_id
            WHERE le.target_node = :node
        """), {"node": node_id}).fetchall()
    return [{"node": r.source_node, "team": r.owner_team, "job": r.job_name, "version": r.job_version} for r in rows]

def seed_sample_lineage():
    """Seed a realistic lineage graph for demo purposes."""
    nodes = [
        ("raw.orders", "TABLE", "data-eng"),
        ("raw.payments", "TABLE", "data-eng"),
        ("clean.orders", "TABLE", "data-eng"),
        ("orders_etl", "PIPELINE", "data-eng"),
        ("payments_stream", "PIPELINE", "payments-team"),
        ("agg.daily_revenue", "TABLE", "analytics"),
        ("report.executive_kpis", "REPORT", "analytics"),
        ("ml.fraud_features", "TABLE", "ml-platform"),
    ]
    edges = [
        ("raw.orders",    "clean.orders",          "orders_etl",     "v2.1", "CLEAN"),
        ("clean.orders",  "agg.daily_revenue",     "revenue_agg",    "v1.5", "AGGREGATE"),
        ("raw.payments",  "ml.fraud_features",     "payments_stream","v3.0", "FEATURE_ENG"),
        ("agg.daily_revenue", "report.executive_kpis", "report_gen", "v1.0", "REPORT"),
    ]
    for node_id, node_type, team in nodes:
        register_node(node_id, node_type, team)
    for s, t, j, v, tt in edges:
        register_edge(s, t, j, v, tt)
    logger.info("[Lineage] Sample lineage graph seeded")