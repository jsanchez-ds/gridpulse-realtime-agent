"""
LLM client — same provider abstraction as Project 2, now with function calling.

Adds `chat_with_tools()` which returns either plain text OR a list of tool
calls that the agent loop should execute and feed back as `tool` messages.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.config import get_env, load_yaml_config
from src.utils.logging import get_logger

log = get_logger(__name__)

Role = Literal["system", "user", "assistant", "tool"]


@dataclass
class Message:
    role: Role
    content: str | None = None
    tool_calls: list[dict] | None = None
    tool_call_id: str | None = None

    def to_openai(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"role": self.role}
        if self.content is not None:
            payload["content"] = self.content
        if self.tool_calls:
            payload["tool_calls"] = self.tool_calls
        if self.tool_call_id:
            payload["tool_call_id"] = self.tool_call_id
        return payload


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict


@dataclass
class ChatResult:
    text: str | None
    tool_calls: list[ToolCall] = field(default_factory=list)
    model: str = ""
    provider: str = ""
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    finish_reason: str = ""


class LLMClient:
    OPENAI_COMPATIBLE_BASE_URLS = {
        "groq": "https://api.groq.com/openai/v1",
        "openai": "https://api.openai.com/v1",
        "openrouter": "https://openrouter.ai/api/v1",
    }

    def __init__(
        self,
        provider: str | None = None,
        model: str | None = None,
        temperature: float = 0.2,
        max_tokens: int = 1024,
    ) -> None:
        env = get_env()
        self.provider = provider or env.llm_provider
        self.model = model or env.llm_model_override or self._default_model(self.provider)
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.api_key = env.active_llm_api_key
        self._client: Any = None

    @staticmethod
    def _default_model(provider: str) -> str:
        return {
            "groq": "llama-3.3-70b-versatile",
            "anthropic": "claude-3-5-sonnet-20241022",
            "openai": "gpt-4o-mini",
            "openrouter": "nvidia/nemotron-3-super-120b-a12b:free",
        }[provider]

    def _init_client(self) -> None:
        if self._client is not None:
            return
        if self.provider == "anthropic":
            from anthropic import Anthropic

            self._client = Anthropic(api_key=self.api_key)
        else:
            from openai import OpenAI

            self._client = OpenAI(
                api_key=self.api_key,
                base_url=self.OPENAI_COMPATIBLE_BASE_URLS[self.provider],
                max_retries=4,
                timeout=60.0,
            )

    @retry(stop=stop_after_attempt(6), wait=wait_exponential(multiplier=2, min=5, max=90), reraise=True)
    def chat(self, messages: list[Message], tools: list[dict] | None = None) -> ChatResult:
        """One chat completion. If `tools` is provided and the model chose to call
        one or more, `ChatResult.tool_calls` is populated."""
        self._init_client()
        if self.provider == "anthropic":
            return self._chat_anthropic(messages, tools)
        return self._chat_openai(messages, tools)

    def _chat_openai(self, messages: list[Message], tools: list[dict] | None) -> ChatResult:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [m.to_openai() for m in messages],
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"
        resp = self._client.chat.completions.create(**kwargs)

        choice = resp.choices[0]
        msg = choice.message

        calls: list[ToolCall] = []
        if getattr(msg, "tool_calls", None):
            import json as _json

            for tc in msg.tool_calls:
                try:
                    args = _json.loads(tc.function.arguments or "{}")
                except Exception:
                    args = {"_raw": tc.function.arguments}
                calls.append(ToolCall(id=tc.id, name=tc.function.name, arguments=args))

        usage = getattr(resp, "usage", None)
        return ChatResult(
            text=msg.content,
            tool_calls=calls,
            model=self.model,
            provider=self.provider,
            prompt_tokens=getattr(usage, "prompt_tokens", None),
            completion_tokens=getattr(usage, "completion_tokens", None),
            finish_reason=choice.finish_reason or "",
        )

    def _chat_anthropic(self, messages: list[Message], tools: list[dict] | None) -> ChatResult:
        # Anthropic tool format differs from OpenAI — translate. For v1 we only
        # implement the OpenAI path fully; the Anthropic adapter here handles
        # plain chat and raises if tools are requested.
        if tools:
            raise NotImplementedError(
                "Anthropic tool-use adapter not implemented in v1. "
                "Use LLM_PROVIDER=openrouter or openai when calling chat_with_tools."
            )
        sys_msgs = [m.content or "" for m in messages if m.role == "system"]
        non_sys = [
            {"role": m.role, "content": m.content or ""}
            for m in messages
            if m.role != "system"
        ]
        resp = self._client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            temperature=self.temperature,
            system="\n".join(sys_msgs) if sys_msgs else None,
            messages=non_sys,
        )
        text = "".join(b.text for b in resp.content if getattr(b, "text", None))
        return ChatResult(
            text=text,
            model=self.model,
            provider=self.provider,
            prompt_tokens=resp.usage.input_tokens,
            completion_tokens=resp.usage.output_tokens,
            finish_reason=resp.stop_reason or "",
        )
