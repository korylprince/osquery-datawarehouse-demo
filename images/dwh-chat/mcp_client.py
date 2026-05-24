"""MCP Streamable HTTP client wrapper."""

from __future__ import annotations

import asyncio

import anyio._backends._asyncio as _anyio_asyncio
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# ---------------------------------------------------------------------------
# Fix: prevent _deliver_cancellation from pinning the CPU at 100%.
#
# anyio 4.13.0 reschedules _deliver_cancellation via call_soon() whenever
# tasks remain in a cancelled scope's _tasks set.  Under nest_asyncio (used
# by Chainlit), _run_once drains the entire _ready queue each iteration, so
# the freshly-added call_soon callback fires immediately on the next pass -
# before any task __step callbacks can run.  Tasks are starved, they never
# exit _tasks, and the loop spins at 100% CPU indefinitely.
#
# Fix: after _orig_deliver schedules itself via call_soon, replace that Handle
# with call_later(1ms).  call_later puts the callback in _scheduled rather than
# _ready, so nest_asyncio does NOT drain it in the current pass.  Tasks' __step
# callbacks run first, tasks make progress and exit, and the loop terminates
# naturally when _tasks becomes empty.
# ---------------------------------------------------------------------------
_orig_deliver = _anyio_asyncio.CancelScope._deliver_cancellation


def _throttled_deliver(self: _anyio_asyncio.CancelScope, origin: _anyio_asyncio.CancelScope) -> bool:
    result = _orig_deliver(self, origin)
    try:
        if origin is self and result and self._cancel_handle is not None:
            self._cancel_handle.cancel()
            self._cancel_handle = asyncio.get_running_loop().call_later(
                0.001, self._deliver_cancellation, origin
            )
    except Exception:
        pass  # never break anyio on patch errors
    return result


_anyio_asyncio.CancelScope._deliver_cancellation = _throttled_deliver


async def connect_mcp(url: str) -> tuple[ClientSession, list[dict], object]:
    """
    Opens a Streamable HTTP MCP session.

    Returns (session, openai_tools, exit_stack) where exit_stack must be kept
    alive for the duration of the session and closed when done.
    """
    from contextlib import AsyncExitStack

    exit_stack = AsyncExitStack()
    transport = await exit_stack.enter_async_context(
        streamablehttp_client(url)
    )
    read_stream, write_stream, _ = transport

    session = await exit_stack.enter_async_context(
        ClientSession(read_stream, write_stream)
    )
    await session.initialize()

    tools_result = await session.list_tools()
    openai_tools = [
        {
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description or "",
                "parameters": tool.inputSchema,
            },
        }
        for tool in tools_result.tools
    ]

    class _ExitStack:
        async def aclose(self) -> None:
            try:
                await exit_stack.aclose()
            except Exception:
                pass

    return session, openai_tools, _ExitStack()
