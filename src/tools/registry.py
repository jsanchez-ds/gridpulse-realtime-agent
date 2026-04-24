"""
Tool registry — maps tool names to Python callables and their JSON schema.

The schema is passed to the LLM as part of `tools=[...]` on every agent
turn. The callables are invoked by the agent loop when the LLM emits a
tool_call. Each callable must return a JSON-serialisable dict.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.utils.logging import get_logger

log = get_logger(__name__)


@dataclass
class Tool:
    name: str
    description: str
    parameters: dict                 # JSON Schema
    callable: Callable[..., dict]
    timeout_seconds: int = 20

    def to_openai_tool(self) -> dict:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        self._tools[tool.name] = tool

    def all(self) -> list[Tool]:
        return list(self._tools.values())

    def get(self, name: str) -> Tool:
        if name not in self._tools:
            raise KeyError(f"Unknown tool: {name!r}. Known: {list(self._tools)}")
        return self._tools[name]

    def to_openai_tools(self) -> list[dict]:
        return [t.to_openai_tool() for t in self._tools.values()]


def build_default_registry() -> ToolRegistry:
    """Wire up all tools we expose to the agent."""
    from src.tools.classify_severity import classify_severity
    from src.tools.get_24h_forecast import get_24h_forecast
    from src.tools.get_current_load import get_current_load
    from src.tools.post_incident_report import post_incident_report
    from src.tools.search_literature import search_literature

    reg = ToolRegistry()

    reg.register(Tool(
        name="classify_severity",
        description="Classify an anomaly's severity as one of {info, warning, critical}. "
                    "Use this FIRST on every anomaly.",
        parameters={
            "type": "object",
            "properties": {
                "anomaly_score": {"type": "number", "description": "Raw anomaly score from the detector"},
                "load_mw": {"type": "number"},
                "region": {"type": "string"},
            },
            "required": ["anomaly_score", "load_mw", "region"],
        },
        callable=classify_severity,
    ))

    reg.register(Tool(
        name="get_current_load",
        description="Get the latest observed load (MW) for a region from the stream.",
        parameters={
            "type": "object",
            "properties": {"region": {"type": "string"}},
            "required": ["region"],
        },
        callable=get_current_load,
    ))

    reg.register(Tool(
        name="get_24h_forecast",
        description="Get the next-24h LightGBM load forecast for a region "
                    "(from the Project-1 MLflow model).",
        parameters={
            "type": "object",
            "properties": {"region": {"type": "string"}},
            "required": ["region"],
        },
        callable=get_24h_forecast,
    ))

    reg.register(Tool(
        name="search_literature",
        description="Search the EnergyScholar RAG (Project 2) over energy-forecasting "
                    "arXiv papers. Returns grounded snippets with citations.",
        parameters={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Natural-language search query"},
            },
            "required": ["query"],
        },
        callable=search_literature,
    ))

    reg.register(Tool(
        name="post_incident_report",
        description="Post a short incident report to the Discord on-call channel and "
                    "persist it to Delta. Only call for severity >= warning.",
        parameters={
            "type": "object",
            "properties": {
                "severity": {"type": "string", "enum": ["info", "warning", "critical"]},
                "title": {"type": "string"},
                "body": {"type": "string", "description": "Short report (≤ 10 lines)"},
            },
            "required": ["severity", "title", "body"],
        },
        callable=post_incident_report,
    ))

    return reg
