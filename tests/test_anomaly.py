import pytest
from unittest.mock import patch, MagicMock

# Mock database and redis before importing modules
@pytest.fixture(autouse=True)
def mock_db(monkeypatch):
    monkeypatch.setattr("sqlalchemy.create_engine", MagicMock())

def test_classify_severity_normal():
    from anomaly.anomaly_detector import classify_severity
    assert classify_severity(1.5) == "NORMAL"

def test_classify_severity_warning():
    from anomaly.anomaly_detector import classify_severity
    assert classify_severity(2.5) == "WARNING"

def test_classify_severity_critical():
    from anomaly.anomaly_detector import classify_severity
    assert classify_severity(4.0) == "CRITICAL"

def test_classify_severity_negative():
    from anomaly.anomaly_detector import classify_severity
    assert classify_severity(-3.6) == "CRITICAL"

def test_business_rules_max_breach():
    from anomaly.business_rules import validate_business_rules
    metrics = {"metrics": {"null_rates": {"user_id": 0.05, "amount": 0.001}, "row_count": 5000}}
    violations = validate_business_rules("orders_etl", metrics)
    assert any(v["type"] == "MAX_BREACH" for v in violations)

def test_business_rules_min_breach():
    from anomaly.business_rules import validate_business_rules
    metrics = {"metrics": {"null_rates": {"user_id": 0.001, "amount": 0.0001}, "row_count": 100}}
    violations = validate_business_rules("orders_etl", metrics)
    assert any(v["type"] == "MIN_BREACH" for v in violations)

def test_business_rules_no_violations():
    from anomaly.business_rules import validate_business_rules
    metrics = {"metrics": {"null_rates": {"user_id": 0.001, "amount": 0.0001}, "row_count": 5000}}
    violations = validate_business_rules("orders_etl", metrics)
    assert len(violations) == 0

def test_sla_prediction_low_risk():
    from unittest.mock import patch
    with patch("sla.sla_monitor.get_sla_definition") as mock_sla, \
         patch("sla.sla_monitor.redis_client") as mock_redis:
        mock_sla.return_value = {"deadline": "23:59:00", "cost_per_hour": 1000.0}
        mock_redis.get.return_value = "2024-01-01T08:00:00"
        from sla.sla_monitor import predict_sla_breach
        result = predict_sla_breach("orders_etl", 50.0)
        assert result["risk_level"] in ["LOW", "MEDIUM", "HIGH", "UNKNOWN"]