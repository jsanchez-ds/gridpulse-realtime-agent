from fastapi.testclient import TestClient

from src.serving import api as api_module


def test_health() -> None:
    with TestClient(api_module.app) as client:
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


def test_info(monkeypatch) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "x")
    monkeypatch.setenv("LLM_PROVIDER", "groq")
    from src.utils.config import get_env

    get_env.cache_clear()
    with TestClient(api_module.app) as client:
        r = client.get("/info")
        assert r.status_code == 200
        body = r.json()
        for k in ("llm_provider", "kafka_bootstrap", "anomaly_model", "energyscholar_url"):
            assert k in body
