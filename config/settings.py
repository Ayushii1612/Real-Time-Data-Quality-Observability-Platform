import os
from dotenv import load_dotenv

load_dotenv()

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://obsuser:obspass@localhost:5432/observability")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_PIPELINE_METRICS_TOPIC = os.getenv("KAFKA_PIPELINE_METRICS_TOPIC", "pipeline-metrics")
KAFKA_ANOMALY_ALERTS_TOPIC = os.getenv("KAFKA_ANOMALY_ALERTS_TOPIC", "anomaly-alerts")

# Redis
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Claude API
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Alerts
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
PAGERDUTY_API_KEY = os.getenv("PAGERDUTY_API_KEY")

# Thresholds
ANOMALY_Z_SCORE_WARNING = 2.0
ANOMALY_Z_SCORE_CRITICAL = 3.5
KS_TEST_P_VALUE_THRESHOLD = 0.05
BASELINE_WINDOW_DAYS = 30
BASELINE_UPDATE_ALPHA = 0.1   # exponential smoothing factor
SEASONAL_SLOTS = 168          # 24 hours × 7 days
SLA_PREDICTION_MINUTES = 45
FALSE_POSITIVE_MIN_OCCURRENCES = 3