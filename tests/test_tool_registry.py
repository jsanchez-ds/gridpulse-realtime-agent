import pytest

from src.tools.registry import Tool, ToolRegistry, build_default_registry


def test_registry_register_and_get() -> None:
    reg = ToolRegistry()
    t = Tool(
        name="echo",
        description="echo back",
        parameters={"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]},
        callable=lambda x: {"x": x},
    )
    reg.register(t)
    assert reg.get("echo") is t
    assert len(reg.all()) == 1


def test_registry_unknown_tool_raises() -> None:
    reg = ToolRegistry()
    with pytest.raises(KeyError):
        reg.get("nope")


def test_to_openai_tools_shape() -> None:
    reg = ToolRegistry()
    reg.register(Tool(
        name="classify_severity",
        description="...",
        parameters={"type": "object", "properties": {}},
        callable=lambda: {"ok": True},
    ))
    tools = reg.to_openai_tools()
    assert len(tools) == 1
    assert tools[0]["type"] == "function"
    assert tools[0]["function"]["name"] == "classify_severity"


def test_default_registry_has_all_tools() -> None:
    reg = build_default_registry()
    names = {t.name for t in reg.all()}
    assert names == {
        "classify_severity",
        "get_current_load",
        "get_24h_forecast",
        "search_literature",
        "post_incident_report",
    }
