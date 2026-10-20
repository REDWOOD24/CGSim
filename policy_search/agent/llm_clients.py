"""Provider-agnostic chat+tool-use client.

The agent loop (loop.py) only depends on the neutral types defined here
(ToolSpec, ToolCall, AssistantTurn, ToolResult) and the LLMClient interface.
Each concrete client is responsible for converting the loop's neutral
conversation history into its own provider's wire format on every call, and
translating the reply back -- so adding a new provider never touches loop.py.

No API key is ever hardcoded here: every client reads its credentials from
an environment variable at call time, named by the caller.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


@dataclass
class ToolSpec:
    name: str
    description: str
    input_schema: dict


@dataclass
class ToolCall:
    id: str
    name: str
    input: dict


@dataclass
class ToolResult:
    tool_call_id: str
    name: str
    content: str
    is_error: bool = False


@dataclass
class AssistantTurn:
    text: str | None
    tool_calls: list[ToolCall] = field(default_factory=list)
    stop_reason: str = ""
    usage: dict = field(default_factory=dict)


# A history entry is one of:
#   {"kind": "user", "text": str}
#   {"kind": "assistant", "turn": AssistantTurn}
#   {"kind": "tool_results", "results": list[ToolResult]}
HistoryEntry = dict


class LLMClient:
    def chat(self, system: str, tool_specs: list[ToolSpec], history: list[HistoryEntry]) -> AssistantTurn:
        raise NotImplementedError


class AnthropicClient(LLMClient):
    """Native Claude tool use via the Anthropic Messages API."""

    def __init__(self, model: str, max_tokens: int = 4096, api_key_env: str = "ANTHROPIC_API_KEY"):
        import anthropic  # local import: this dependency is optional per-provider

        api_key = os.environ.get(api_key_env)
        if not api_key:
            raise RuntimeError(f"Environment variable {api_key_env} is not set")
        self._client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens

    def chat(self, system: str, tool_specs: list[ToolSpec], history: list[HistoryEntry]) -> AssistantTurn:
        messages = []
        for entry in history:
            if entry["kind"] == "user":
                messages.append({"role": "user", "content": [{"type": "text", "text": entry["text"]}]})
            elif entry["kind"] == "assistant":
                turn: AssistantTurn = entry["turn"]
                content = []
                if turn.text:
                    content.append({"type": "text", "text": turn.text})
                for tc in turn.tool_calls:
                    content.append({"type": "tool_use", "id": tc.id, "name": tc.name, "input": tc.input})
                messages.append({"role": "assistant", "content": content})
            elif entry["kind"] == "tool_results":
                content = [
                    {"type": "tool_result", "tool_use_id": r.tool_call_id, "content": r.content, "is_error": r.is_error}
                    for r in entry["results"]
                ]
                messages.append({"role": "user", "content": content})

        tools = [{"name": t.name, "description": t.description, "input_schema": t.input_schema} for t in tool_specs]

        kwargs = {"model": self.model, "max_tokens": self.max_tokens, "system": system, "messages": messages}
        if tools:
            kwargs["tools"] = tools
        response = self._client.messages.create(**kwargs)

        text_parts = [b.text for b in response.content if b.type == "text"]
        tool_calls = [ToolCall(id=b.id, name=b.name, input=b.input) for b in response.content if b.type == "tool_use"]
        usage = {"input_tokens": response.usage.input_tokens, "output_tokens": response.usage.output_tokens}
        return AssistantTurn(text="\n".join(text_parts) if text_parts else None, tool_calls=tool_calls,
                              stop_reason=response.stop_reason, usage=usage)


class OpenAICompatibleClient(LLMClient):
    """Any server speaking the OpenAI chat-completions + function-calling
    wire format: the real OpenAI API, or a self-hosted/open-weight model
    server (vLLM, TGI, etc.) exposed the same way -- e.g. a local inference
    endpoint reached via `base_url`. Tool-calling fidelity depends entirely
    on that specific server/model; this client makes no assumptions beyond
    the standard wire format.
    """

    def __init__(self, model: str, base_url: str | None, api_key_env: str, max_tokens: int = 4096):
        import openai  # local import: this dependency is optional per-provider

        api_key = os.environ.get(api_key_env)
        if not api_key:
            raise RuntimeError(f"Environment variable {api_key_env} is not set")
        self._client = openai.OpenAI(base_url=base_url, api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens

    def chat(self, system: str, tool_specs: list[ToolSpec], history: list[HistoryEntry]) -> AssistantTurn:
        messages = [{"role": "system", "content": system}]
        for entry in history:
            if entry["kind"] == "user":
                messages.append({"role": "user", "content": entry["text"]})
            elif entry["kind"] == "assistant":
                turn: AssistantTurn = entry["turn"]
                msg = {"role": "assistant", "content": turn.text}
                if turn.tool_calls:
                    msg["tool_calls"] = [
                        {"id": tc.id, "type": "function",
                         "function": {"name": tc.name, "arguments": json.dumps(tc.input)}}
                        for tc in turn.tool_calls
                    ]
                messages.append(msg)
            elif entry["kind"] == "tool_results":
                for r in entry["results"]:
                    messages.append({"role": "tool", "tool_call_id": r.tool_call_id, "content": r.content})

        tools = [{"type": "function", "function": {"name": t.name, "description": t.description,
                                                     "parameters": t.input_schema}} for t in tool_specs]

        kwargs = {"model": self.model, "max_tokens": self.max_tokens, "messages": messages}
        if tools:
            kwargs["tools"] = tools
        response = self._client.chat.completions.create(**kwargs)
        choice = response.choices[0]
        message = choice.message
        tool_calls = []
        for tc in (message.tool_calls or []):
            try:
                args = json.loads(tc.function.arguments) if tc.function.arguments else {}
            except json.JSONDecodeError:
                args = {}
            tool_calls.append(ToolCall(id=tc.id, name=tc.function.name, input=args))
        usage = {}
        if response.usage:
            usage = {"input_tokens": response.usage.prompt_tokens, "output_tokens": response.usage.completion_tokens}
        return AssistantTurn(text=message.content, tool_calls=tool_calls, stop_reason=choice.finish_reason, usage=usage)


def make_llm_client(provider: str, model: str, base_url: str | None = None, api_key_env: str | None = None,
                     max_tokens: int = 4096) -> LLMClient:
    if provider == "anthropic":
        return AnthropicClient(model=model, max_tokens=max_tokens, api_key_env=api_key_env or "ANTHROPIC_API_KEY")
    if provider == "openai_compatible":
        return OpenAICompatibleClient(model=model, base_url=base_url, api_key_env=api_key_env or "OPENAI_API_KEY",
                                       max_tokens=max_tokens)
    raise ValueError(f"Unknown provider {provider!r}; expected 'anthropic' or 'openai_compatible'")
