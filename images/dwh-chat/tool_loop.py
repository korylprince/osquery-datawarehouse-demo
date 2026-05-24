"""Agentic OpenAI tool-call loop."""

from __future__ import annotations

import json
from typing import Awaitable, Callable, Optional

import openai
from mcp import ClientSession

from config import Endpoint

_MAX_ITERATIONS = 50
_MAX_TOOL_RESULT_BYTES = 8 * 1024  # 8 KB per tool result stored in history


def _sanitize_messages(messages: list[dict]) -> None:
    """Ensure every assistant tool_call has a matching tool result message.

    If a chat was cancelled mid-tool-execution, the messages list may contain
    an assistant message with ``tool_calls`` but no corresponding tool result
    messages for each ``tool_call_id``.  This patches in placeholder responses
    so the next API call doesn't fail with a provider validation error.
    """
    i = 0
    while i < len(messages):
        msg = messages[i]
        if msg.get("role") == "assistant":
            tool_calls = msg.get("tool_calls") or []
            if tool_calls:
                expected_ids = {tc["id"] for tc in tool_calls if tc.get("id")}
                # Scan forward collecting tool result ids
                j = i + 1
                while j < len(messages) and messages[j].get("role") == "tool":
                    expected_ids.discard(messages[j].get("tool_call_id"))
                    j += 1
                # Patch in cancelled responses for any missing ids,
                # appending after existing tool results (at position j).
                missing = [tc for tc in tool_calls if tc.get("id") and tc.get("id") in expected_ids]
                for tc in missing:
                    messages.insert(
                        j,
                        {
                            "role": "tool",
                            "tool_call_id": tc["id"],
                            "content": "Tool call was cancelled.",
                        },
                    )
                    j += 1
        i += 1


def _parse_args(arguments: str | dict) -> dict:
    """Parse tool call arguments whether they arrive as a JSON string or dict."""
    if isinstance(arguments, dict):
        return arguments
    try:
        return json.loads(arguments)
    except (json.JSONDecodeError, TypeError):
        return {}


async def run_agent(
    messages: list[dict],
    endpoint: Endpoint,
    model: str,
    thinking: bool = False,
    tools: list[dict] = None,
    mcp_session: ClientSession = None,
    on_tool_result: Callable[[str, str, object], Awaitable[None]] = None,
    on_group_end: Callable[[], Awaitable[None]] = None,
    stream_callback: Optional[Callable[[str], Awaitable[None]]] = None,
) -> str:
    """
    Run the full tool-call loop. Returns the final assistant text.

    Tool calls are streamed into the UI as they execute via on_tool_result.
    Consecutive rounds with no assistant text between them form one "group".
    on_group_end is called when text breaks the sequence (or the loop finishes),
    allowing the caller to finalize the group's parent UI element.

    stream_callback receives tokens for the final (and any mid-loop) text
    response so messages appear below the tool-call steps.

    When thinking=True, reasoning_content is captured and echoed back in
    subsequent messages (required by DeepSeek thinking mode).
    """
    client = openai.AsyncOpenAI(
        base_url=endpoint.base_url + "/v1",
        api_key=endpoint.api_key or "none",
    )

    iteration = 0
    force_final = False
    in_group = False  # whether we have an open tool-call group

    while True:
        _sanitize_messages(messages)
        stream = await client.chat.completions.create(
            model=model,
            messages=messages,
            tools=tools if (tools and not force_final) else openai.NOT_GIVEN,
            tool_choice="auto" if (tools and not force_final) else openai.NOT_GIVEN,
            stream=True,
        )

        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        tool_calls_dict: dict[int, dict] = {}

        async for chunk in stream:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta

            if delta.content:
                content_parts.append(delta.content)

            if thinking:
                # Different providers use different field names for thinking content.
                # DeepSeek uses `reasoning_content`; OpenRouter (Kimi etc.) uses `reasoning`.
                rc = getattr(delta, "reasoning_content", None) or getattr(delta, "reasoning", None)
                if rc:
                    reasoning_parts.append(rc)

            if delta.tool_calls:
                for tcd in delta.tool_calls:
                    idx = tcd.index
                    if idx not in tool_calls_dict:
                        tool_calls_dict[idx] = {
                            "id": tcd.id or "",
                            "type": "function",
                            "function": {
                                "name": tcd.function.name or "",
                                "arguments": tcd.function.arguments or "",
                            },
                        }
                    else:
                        acc = tool_calls_dict[idx]
                        if tcd.id:
                            acc["id"] = tcd.id
                        if tcd.function.name:
                            acc["function"]["name"] += tcd.function.name
                        if tcd.function.arguments:
                            acc["function"]["arguments"] += tcd.function.arguments

        content = "".join(content_parts)
        reasoning_content = "".join(reasoning_parts)

        if tool_calls_dict and not force_final:
            tool_calls = [tool_calls_dict[i] for i in sorted(tool_calls_dict)]
            assistant_msg: dict = {
                "role": "assistant",
                "content": content or None,
                "tool_calls": tool_calls,
            }
            if reasoning_content:
                assistant_msg["reasoning_content"] = reasoning_content
            messages.append(assistant_msg)

            # Text alongside tool calls breaks the current group.
            if content and in_group:
                if on_group_end:
                    await on_group_end()
                in_group = False
                if stream_callback:
                    for part in content_parts:
                        await stream_callback(part)

            # Execute each tool and stream it into the current group immediately.
            for tc in tool_calls:
                args = _parse_args(tc["function"]["arguments"])
                result = await mcp_session.call_tool(tc["function"]["name"], args)

                if on_tool_result:
                    await on_tool_result(tc["function"]["name"], tc["function"]["arguments"], result.content)
                in_group = True

                full_content = json.dumps([c.model_dump() for c in result.content])
                if len(full_content) > _MAX_TOOL_RESULT_BYTES:
                    full_content = full_content[:_MAX_TOOL_RESULT_BYTES] + "\n...[truncated]"
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc["id"],
                        "content": full_content,
                    }
                )

            iteration += 1
            if iteration >= _MAX_ITERATIONS:
                messages.append(
                    {
                        "role": "system",
                        "content": (
                            "You have reached the maximum number of tool calls. "
                            "Please provide a final answer to the user now without calling any more tools."
                        ),
                    }
                )
                force_final = True
        else:
            # Final text response - close any open group, then stream the text.
            if in_group and on_group_end:
                await on_group_end()
            in_group = False
            if stream_callback and content:
                for part in content_parts:
                    await stream_callback(part)
            return content
