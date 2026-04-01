from datetime import datetime, timezone
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_PIPELINE_METRICS_TOPIC
from config.logging_config import get_logger

logger = get_logger(__name__)

PIPELINES = [
    "orders_etl", "inventory_sync", "user_events", "payments_stream",
    "product_catalog", "recommendations_ml", "fraud_detection",
    "reporting_aggregate", "customer_360", "realtime_dashboard"
]

def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=3,
        acks="all"
    )

def generate_pipeline_metrics(pipeline_name: str) -> dict:
    """Simulate realistic pipeline metrics with occasional anomalies."""
    is_anomaly = random.random() < 0.05  # 5% anomaly injection rate
    base_row_count = random.randint(8000, 12000)

    return {
        "pipeline_name": pipeline_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "batch_id": f"{pipeline_name}_{int(time.time())}",
        "metrics": {
            "row_count": base_row_count if not is_anomaly else base_row_count * random.choice([0.1, 3.5]),
            "null_rates": {
                "user_id": round(random.uniform(0.0, 0.02), 4),
                "email": round(random.uniform(0.0, 0.05) if not is_anomaly else 0.45, 4),
                "amount": round(random.uniform(0.0, 0.01), 4),
            },
            "processing_time_seconds": round(random.uniform(10, 60), 2),
            "schema_version": "v3.2.1",
            "data_freshness_lag_seconds": random.randint(5, 300),
            "duplicate_rate": round(random.uniform(0.0, 0.005), 5),
        },
        "status": "ANOMALY" if is_anomaly else "NORMAL"
    }

def run_producer():
    producer = create_producer()
    logger.info("Kafka producer started — sending pipeline metrics every 30s")
    try:
        while True:
            for pipeline in PIPELINES:
                metrics = generate_pipeline_metrics(pipeline)
                producer.send(
                    topic=KAFKA_PIPELINE_METRICS_TOPIC,
                    key=pipeline,
                    value=metrics
                )
                logger.debug(f"Sent metrics for {pipeline}")
            producer.flush()
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("Producer stopped")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()