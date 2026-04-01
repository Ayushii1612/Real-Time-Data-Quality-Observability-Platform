import json
from kafka import KafkaConsumer
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_PIPELINE_METRICS_TOPIC
from config.logging_config import get_logger

logger = get_logger(__name__)

def create_consumer(group_id: str = "observability-core") -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_PIPELINE_METRICS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        max_poll_records=50
    )

def consume_and_process(processor_fn):
    """
    Generic consumer loop. Pass any callable that accepts a metrics dict.
    Used by profiler, anomaly detector, SLA monitor etc.
    """
    consumer = create_consumer()
    logger.info(f"Consumer started on topic: {KAFKA_PIPELINE_METRICS_TOPIC}")
    try:
        for message in consumer:
            metrics = message.value
            pipeline = message.key.decode("utf-8") if message.key else "unknown"
            logger.debug(f"Received batch from {pipeline}")
            processor_fn(pipeline, metrics)
    except KeyboardInterrupt:
        logger.info("Consumer stopped")
    finally:
        consumer.close()