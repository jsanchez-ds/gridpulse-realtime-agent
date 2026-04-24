"""
GridPulse agent loop.

Given an anomaly event, runs a reason→tool_call→observe loop using
OpenAI-compatible function calling until the model either posts an
incident report, decides no action is needed, or hits the safety limits
(max_iterations / cost budget).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from src.tools.registry import ToolRegistry, build_default_registry
from src.utils.config import get_env, load_yaml_config
from src.utils.llm import ChatResult, LLMClient, Message, ToolCall
from src.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class AgentRun:
    anomaly: dict
    iterations: int = 0
    tool_calls: list[dict] = field(default_factory=list)
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    final_text: str | None = None
    posted_incident_id: str | None = None
    elapsed_seconds: float = 0.0
    hit_max_iterations: bool = False
    hit_budget: bool = False


def _execute_tool(reg: ToolRegistry, call: ToolCall) -> dict:
    try:
        tool = reg.get(call.name)
    except KeyError as exc:
        return {"error": str(exc)}

    log.info("tool.exec.start", name=call.name, args_keys=list(call.arguments.keys()))
    start = time.time()
    try:
        result = tool.callable(**call.arguments)
    except TypeError as exc:
        # bad arguments — return a structured error so the model can recover
        result = {"error": f"argument mismatch: {exc!s}"}
    except Exception as exc:
        log.exception("tool.exec.failed", name=call.name, error=str(exc))
        result = {"error": f"{type(exc).__name__}: {exc!s}"}
    elapsed = round(time.time() - start, 3)
    log.info("tool.exec.done", name=call.name, elapsed_seconds=elapsed)
    return result


def _format_anomaly_prompt(anomaly: dict, region: str) -> str:
    return (
        f"Anomaly detected for region {region}:\n"
        f"  timestamp_utc : {anomaly.get('timestamp_utc')}\n"
        f"  load_mw       : {anomaly.get('load_mw'):.1f}\n"
        f"  anomaly_score : {anomaly.get('anomaly_score'):.4f}\n"
        f"\nPlease proceed with your steps."
    )


def handle_anomaly(
    anomaly: dict,
    registry: ToolRegistry | None = None,
    llm: LLMClient | None = None,
) -> AgentRun:
    env = get_env()
    cfg = load_yaml_config()

    reg = registry or build_default_registry()
    llm = llm or LLMClient()

    max_iter = cfg["agent"]["max_iterations"]
    token_budget = env.agent_cost_budget_tokens
    system_prompt = cfg["agent"]["system_prompt"].format(region=anomaly.get("region", "?"))

    run = AgentRun(anomaly=anomaly)
    start = time.time()

    messages: list[Message] = [
        Message(role="system", content=system_prompt),
        Message(role="user", content=_format_anomaly_prompt(anomaly, anomaly.get("region", "?"))),
    ]

    openai_tools = reg.to_openai_tools()

    for i in range(max_iter):
        run.iterations = i + 1
        result: ChatResult = llm.chat(messages, tools=openai_tools)

        run.total_prompt_tokens += result.prompt_tokens or 0
        run.total_completion_tokens += result.completion_tokens or 0

        if run.total_prompt_tokens + run.total_completion_tokens > token_budget:
            log.warning(
                "agent.budget.exceeded",
                used=run.total_prompt_tokens + run.total_completion_tokens,
                limit=token_budget,
            )
            run.hit_budget = True
            run.final_text = result.text or "(budget exceeded)"
            break

        # If model emitted tool calls, execute them and loop
        if result.tool_calls:
            # Record the assistant message that requested the tool calls
            messages.append(Message(
                role="assistant",
                content=result.text,
                tool_calls=[
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.name, "arguments": json.dumps(tc.arguments)},
                    }
                    for tc in result.tool_calls
                ],
            ))
            for call in result.tool_calls:
                observation = _execute_tool(reg, call)
                run.tool_calls.append({
                    "iteration": run.iterations,
                    "name": call.name,
                    "args": call.arguments,
                    "result_preview": str(observation)[:500],
                })
                if call.name == "post_incident_report" and observation.get("incident_id"):
                    run.posted_incident_id = observation["incident_id"]
                messages.append(Message(
                    role="tool",
                    tool_call_id=call.id,
                    content=json.dumps(observation, default=str)[:4000],
                ))
            continue

        # No more tool calls → we're done
        run.final_text = result.text or ""
        break
    else:
        run.hit_max_iterations = True
        log.warning("agent.max_iterations.hit", anomaly_id=anomaly.get("event_id"))

    run.elapsed_seconds = round(time.time() - start, 2)
    log.info(
        "agent.done",
        iterations=run.iterations,
        elapsed_seconds=run.elapsed_seconds,
        prompt_tokens=run.total_prompt_tokens,
        completion_tokens=run.total_completion_tokens,
        posted=run.posted_incident_id,
    )
    return run
