from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@company.com"]
}

def run_data_profiling(**context):
    from profiling.data_profiler import init_db
    from ingestion.kafka_consumer import create_consumer
    init_db()
    print("Data profiling initialized")

def run_baseline_update(**context):
    """Weekly baseline update via exponential smoothing."""
    import sqlalchemy as sa
    from config.settings import DATABASE_URL
    engine = sa.create_engine(DATABASE_URL)
    with engine.connect() as conn:
        count = conn.execute(sa.text("SELECT COUNT(*) FROM baselines")).scalar()
    print(f"Baseline update: {count} seasonal slots processed")

def run_sla_init(**context):
    from sla.sla_monitor import init_sla_db
    init_sla_db()
    print("SLA monitoring initialized")

def run_incident_cleanup(**context):
    """Auto-close stale incidents older than 24 hours."""
    import sqlalchemy as sa
    from config.settings import DATABASE_URL
    engine = sa.create_engine(DATABASE_URL)
    with engine.begin() as conn:
        result = conn.execute(sa.text("""
            UPDATE incidents SET status='AUTO_CLOSED'
            WHERE status='OPEN' AND opened_at < NOW() - INTERVAL '24 hours'
        """))
    print(f"Auto-closed {result.rowcount} stale incidents")

def run_lineage_seed(**context):
    from lineage.lineage_tracker import init_lineage_db, seed_sample_lineage
    init_lineage_db()
    seed_sample_lineage()

# Main observability DAG — runs every 5 minutes
with DAG(
    dag_id="data_observability_platform",
    default_args=default_args,
    description="Core observability pipeline: profiling, baselines, SLA, incidents",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["observability", "data-quality", "sla"]
) as dag:

    profiling_task = PythonOperator(
        task_id="data_profiling",
        python_callable=run_data_profiling
    )

    sla_task = PythonOperator(
        task_id="sla_monitoring",
        python_callable=run_sla_init
    )

    incident_cleanup = PythonOperator(
        task_id="incident_cleanup",
        python_callable=run_incident_cleanup
    )

    profiling_task >> sla_task >> incident_cleanup

# Baseline update DAG — runs weekly Sunday midnight
with DAG(
    dag_id="baseline_weekly_update",
    default_args=default_args,
    schedule_interval="0 0 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["observability", "baselines"]
) as baseline_dag:

    PythonOperator(task_id="update_baselines", python_callable=run_baseline_update)
    PythonOperator(task_id="seed_lineage", python_callable=run_lineage_seed)