from config.logging_config import get_logger

logger = get_logger(__name__)

RULES = {
    "orders_etl": [
        {"field": "null_rates.user_id", "max": 0.01, "description": "user_id cannot be >1% null"},
        {"field": "null_rates.amount",  "max": 0.001, "description": "amount cannot be >0.1% null"},
        {"field": "row_count",          "min": 1000,  "description": "must process at least 1000 orders"},
    ],
    "payments_stream": [
        {"field": "null_rates.amount",  "max": 0.0001, "description": "payment amount null < 0.01%"},
        {"field": "duplicate_rate",     "max": 0.0005, "description": "duplicate rate < 0.05%"},
    ],
    "fraud_detection": [
        {"field": "freshness_lag",      "max": 30, "description": "fraud signals must be < 30s old"},
    ]
}

def validate_business_rules(pipeline: str, metrics: dict) -> list:
    """Validate pipeline-specific business rules. Return list of violations."""
    violations = []
    rules = RULES.get(pipeline, [])
    m = metrics.get("metrics", {})

    for rule in rules:
        field_parts = rule["field"].split(".")
        value = m
        for part in field_parts:
            value = value.get(part) if isinstance(value, dict) else None
            if value is None:
                break

        if value is None:
            continue

        if "max" in rule and float(value) > rule["max"]:
            violations.append({
                "pipeline": pipeline,
                "rule": rule["description"],
                "field": rule["field"],
                "observed": value,
                "threshold": rule["max"],
                "type": "MAX_BREACH"
            })
            logger.warning(f"[BusinessRule] {pipeline} — {rule['description']} — observed={value}")

        if "min" in rule and float(value) < rule["min"]:
            violations.append({
                "pipeline": pipeline,
                "rule": rule["description"],
                "field": rule["field"],
                "observed": value,
                "threshold": rule["min"],
                "type": "MIN_BREACH"
            })
            logger.warning(f"[BusinessRule] {pipeline} — {rule['description']} — observed={value}")

    return violations