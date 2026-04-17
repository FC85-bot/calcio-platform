from __future__ import annotations


def test_health_endpoint_exposes_request_id_and_db_latency(client):
    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.headers.get("X-Request-ID")

    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["request_id"] == response.headers["X-Request-ID"]
    assert isinstance(payload["database_latency_ms"], float)
    assert payload["database_latency_ms"] >= 0


def test_monitoring_summary_endpoint_returns_operational_snapshot(client):
    response = client.get("/api/v1/admin/monitoring/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["service"]
    assert "checks" in payload
    assert "pipelines" in payload
    assert "provider_latency" in payload
    assert payload["data_confidence"]["status"] in {"OK", "BROKEN"}
    assert isinstance(payload["data_confidence"]["critical_signal_count"], int)
    assert isinstance(payload["data_confidence"]["signals"], list)


def test_validation_error_response_is_readable_and_carries_request_id(client):
    response = client.get("/api/v1/matches", params={"limit": 0})

    assert response.status_code == 422
    payload = response.json()
    assert payload["detail"] == "request_validation_error"
    assert payload["request_id"] == response.headers["X-Request-ID"]
    assert isinstance(payload["errors"], list)
    assert payload["errors"]
