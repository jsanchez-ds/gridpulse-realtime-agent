"""Typed config loader."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EnvSettings(BaseSettings):
    # LLM
    llm_provider: Literal["groq", "anthropic", "openai", "openrouter"] = "openrouter"
    llm_model_override: str = ""
    groq_api_key: str = ""
    openrouter_api_key: str = ""
    anthropic_api_key: str = ""
    openai_api_key: str = ""

    # Stream transport — "delta" (no Docker needed) or "kafka" (Redpanda)
    stream_transport: Literal["delta", "kafka"] = "delta"

    # Kafka (used when stream_transport == "kafka")
    kafka_bootstrap: str = "localhost:19092"
    kafka_topic_events: str = "grid_events"
    kafka_topic_anomalies: str = "grid_anomalies"
    kafka_group_id: str = "gridpulse"

    # Delta stream source (used when stream_transport == "delta")
    stream_delta_path: str = "./data/stream/events"

    # Source data
    replay_source: str = "../Nuevo proyecto/data/gold/load_features"
    replay_region: str = "CAL"
    replay_speed: float = 60.0

    # MLflow (anomaly model from Project 1)
    mlflow_tracking_uri: str = "../Nuevo proyecto/mlruns"
    anomaly_model: str = "energy-anomaly-detector"
    anomaly_alias: str = "staging"

    # EnergyScholar (Project 2)
    energyscholar_url: str = "http://localhost:8001"

    # Discord
    discord_webhook_url: str = ""

    # Paths
    delta_path: str = "./data/delta"

    # Agent
    agent_max_iterations: int = 5
    agent_cost_budget_tokens: int = 50_000

    # Langfuse
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    langfuse_host: str = "https://cloud.langfuse.com"

    # API / UI
    api_host: str = "0.0.0.0"
    api_port: int = 8002
    dashboard_port: int = 8503
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    @property
    def active_llm_api_key(self) -> str:
        keys = {
            "groq": self.groq_api_key,
            "anthropic": self.anthropic_api_key,
            "openai": self.openai_api_key,
            "openrouter": self.openrouter_api_key,
        }
        k = keys[self.llm_provider]
        if not k:
            raise RuntimeError(
                f"LLM_PROVIDER={self.llm_provider} but no matching API key in .env"
            )
        return k


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "configs" / "config.yaml"


@lru_cache(maxsize=1)
def load_yaml_config(path: Path | str = CONFIG_PATH) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=1)
def get_env() -> EnvSettings:
    return EnvSettings()  # type: ignore[call-arg]
