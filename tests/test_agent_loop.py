"""Agent loop test — LLM is mocked to exercise the orchestration without API calls."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.agent.loop import handle_anomaly
from src.tools.registry import Tool, ToolRegistry
from src.utils.llm import ChatResult, ToolCall


@pytest.fixture
def mock_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "groq")
    monkeypatch.setenv("GROQ_API_KEY", "dummy")
    from src.utils.config import get_env

    get_env.cache_clear()


@pytest.fixture
def one_tool_registry() -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(Tool(
        name="classify_severity",
        description="classify",
        parameters={"type": "object", "properties": {
            "anomaly_score": {"type": "number"},
            "load_mw": {"type": "number"},
            "region": {"type": "string"},
        }, "required": ["anomaly_score", "load_mw", "region"]},
        callable=lambda anomaly_score, load_mw, region: {"severity": "info", "region": region},
    ))
    return reg


def _mk_llm(responses):
    """Build a mock LLMClient that returns the given ChatResult list in order."""
    llm = MagicMock()
    llm.chat.side_effect = responses
    llm.provider = "groq"
    llm.model = "mock"
    return llm


def test_agent_stops_when_llm_emits_no_tool_calls(mock_env, one_tool_registry):
    """The agent should end immediately if the LLM returns plain text."""
    llm = _mk_llm([
        ChatResult(text="No action needed — looks normal.", tool_calls=[],
                   model="m", provider="groq",
                   prompt_tokens=10, completion_tokens=5, finish_reason="stop"),
    ])
    run = handle_anomaly(
        anomaly={"event_id": "x", "timestamp_utc": "2026-01-01T00:00:00Z",
                 "region": "CAL", "load_mw": 25000.0, "anomaly_score": -0.02},
        registry=one_tool_registry,
        llm=llm,
    )
    assert run.iterations == 1
    assert run.final_text and "no action" in run.final_text.lower()
    assert run.tool_calls == []
    assert not run.hit_max_iterations


def test_agent_executes_tool_then_finishes(mock_env, one_tool_registry):
    """Turn 1 → tool call. Turn 2 → final text."""
    llm = _mk_llm([
        ChatResult(
            text=None,
            tool_calls=[ToolCall(
                id="c1", name="classify_severity",
                arguments={"anomaly_score": -0.02, "load_mw": 25000.0, "region": "CAL"},
            )],
            model="m", provider="groq",
            prompt_tokens=10, completion_tokens=5, finish_reason="tool_calls",
        ),
        ChatResult(text="Severity is info, stopping.", tool_calls=[],
                   model="m", provider="groq",
                   prompt_tokens=12, completion_tokens=6, finish_reason="stop"),
    ])
    run = handle_anomaly(
        anomaly={"event_id": "x", "timestamp_utc": "2026-01-01T00:00:00Z",
                 "region": "CAL", "load_mw": 25000.0, "anomaly_score": -0.02},
        registry=one_tool_registry,
        llm=llm,
    )
    assert run.iterations == 2
    assert len(run.tool_calls) == 1
    assert run.tool_calls[0]["name"] == "classify_severity"
    assert run.final_text and "info" in run.final_text.lower()


def test_agent_hits_max_iterations(mock_env, one_tool_registry, monkeypatch):
    """If the LLM keeps calling tools forever, we stop at max_iterations."""
    # force max_iterations = 2 via config
    from src.utils.config import load_yaml_config

    cfg = load_yaml_config()
    cfg["agent"]["max_iterations"] = 2

    def infinite_tool_caller():
        counter = 0
        while True:
            counter += 1
            yield ChatResult(
                text=None,
                tool_calls=[ToolCall(
                    id=f"c{counter}", name="classify_severity",
                    arguments={"anomaly_score": -0.02, "load_mw": 1.0, "region": "CAL"},
                )],
                model="m", provider="groq",
                prompt_tokens=5, completion_tokens=1, finish_reason="tool_calls",
            )

    llm = MagicMock()
    llm.chat.side_effect = infinite_tool_caller()
    llm.provider = "groq"
    llm.model = "mock"

    run = handle_anomaly(
        anomaly={"event_id": "x", "timestamp_utc": "2026-01-01T00:00:00Z",
                 "region": "CAL", "load_mw": 25000.0, "anomaly_score": -0.02},
        registry=one_tool_registry,
        llm=llm,
    )
    assert run.iterations == 2
    assert run.hit_max_iterations is True
