"""FastAPI side-car — manual trigger of the agent for a given anomaly."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic import BaseModel, Field
from starlette.responses import Response

from src.agent.loop import AgentRun, handle_anomaly
from src.utils.config import get_env
from src.utils.logging import configure_logging, get_logger

log = get_logger(__name__)

AGENT_INVOCATIONS = Counter("gridpulse_agent_invocations_total", "Agent invocations", ["status"])

app = FastAPI(title="GridPulse API", version="0.1.0")


class AnomalyPayload(BaseModel):
    event_id: str
    timestamp_utc: str
    region: str
    load_mw: float
    anomaly_score: float
    hour: int | None = None
    day_of_week: int | None = None
    is_weekend: int | None = None


class AgentResponse(BaseModel):
    iterations: int
    elapsed_seconds: float
    tool_calls: list[dict]
    final_text: str | None
    posted_incident_id: str | None
    prompt_tokens: int
    completion_tokens: int
    hit_max_iterations: bool
    hit_budget: bool


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/info")
def info() -> dict[str, Any]:
    env = get_env()
    return {
        "llm_provider": env.llm_provider,
        "llm_model_override": env.llm_model_override,
        "kafka_bootstrap": env.kafka_bootstrap,
        "anomaly_model": f"{env.anomaly_model}@{env.anomaly_alias}",
        "energyscholar_url": env.energyscholar_url,
        "discord_configured": bool(env.discord_webhook_url),
    }


@app.post("/agent/run", response_model=AgentResponse)
def run_agent(payload: AnomalyPayload) -> AgentResponse:
    try:
        run: AgentRun = handle_anomaly(payload.model_dump())
        AGENT_INVOCATIONS.labels(status="ok").inc()
    except Exception:
        AGENT_INVOCATIONS.labels(status="error").inc()
        log.exception("agent.run.failed")
        raise

    return AgentResponse(
        iterations=run.iterations,
        elapsed_seconds=run.elapsed_seconds,
        tool_calls=run.tool_calls,
        final_text=run.final_text,
        posted_incident_id=run.posted_incident_id,
        prompt_tokens=run.total_prompt_tokens,
        completion_tokens=run.total_completion_tokens,
        hit_max_iterations=run.hit_max_iterations,
        hit_budget=run.hit_budget,
    )


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


configure_logging(level=get_env().log_level)
