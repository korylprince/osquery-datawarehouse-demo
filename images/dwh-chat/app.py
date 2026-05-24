"""dwh-chat - Chainlit entry point."""

from __future__ import annotations

import asyncio
import json as _json
import os
from pathlib import Path

import chainlit as cl
import chainlit.data as cl_data
import openai
from chainlit.config import config as cl_config
from chainlit.context import local_steps
from chainlit.data.sql_alchemy import SQLAlchemyDataLayer
from chainlit.types import ThreadDict
from chainlit.utils import utc_now
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

import config as cfg
from mcp_client import connect_mcp
from tool_loop import run_agent

# ---------------------------------------------------------------------------
# UI config tweaks (applied before Chainlit reads config)
# ---------------------------------------------------------------------------
cl_config.ui.confirm_new_chat = False
cl_config.ui.custom_css = "/public/custom.css"
cl_config.ui.custom_js = "/public/custom.js"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_DATABASE_URL = (
    f"postgresql+asyncpg://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
    f"@{os.environ['DB_HOST']}:5432/{os.environ['DB_NAME']}"
)


class _SanitizingDataLayer(SQLAlchemyDataLayer):
    """Data layer that sanitizes stale chat_profile values on thread load.

    When the model configuration changes, threads saved with the old profile
    name would cause the frontend to crash because the profile no longer
    exists in the profiles list. This subclass rewrites any unknown
    chat_profile to the current default profile so threads can be resumed.
    """

    @staticmethod
    def _fix_thread_metadata(thread: ThreadDict) -> None:
        profiles = cfg.get_profiles()
        default_profile = next(iter(profiles), None)
        if not default_profile:
            return

        meta = thread.get("metadata")
        if meta is None:
            return

        if isinstance(meta, str):
            try:
                meta_dict = _json.loads(meta)
            except Exception:
                return
            stored = meta_dict.get("chat_profile")
            if stored and stored not in profiles:
                print(
                    f"[dwh-chat] Replacing unknown chat_profile {stored!r}"
                    f" with {default_profile!r}",
                    flush=True,
                )
                meta_dict["chat_profile"] = default_profile
                thread["metadata"] = _json.dumps(meta_dict)
        elif isinstance(meta, dict):
            stored = meta.get("chat_profile")
            if stored and stored not in profiles:
                print(
                    f"[dwh-chat] Replacing unknown chat_profile {stored!r}"
                    f" with {default_profile!r}",
                    flush=True,
                )
                meta["chat_profile"] = default_profile

    async def get_all_user_threads(
        self,
        user_id: str | None = None,
        thread_id: str | None = None,
    ):
        threads = await super().get_all_user_threads(
            user_id=user_id, thread_id=thread_id
        )
        if threads:
            for thread in threads:
                self._fix_thread_metadata(thread)
        return threads


_data_layer = _SanitizingDataLayer(conninfo=_DATABASE_URL)
cl_data._data_layer = _data_layer

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS users (
    "id"         TEXT PRIMARY KEY,
    "identifier" TEXT NOT NULL UNIQUE,
    "createdAt"  TEXT,
    "metadata"   JSONB
);

CREATE TABLE IF NOT EXISTS threads (
    "id"             TEXT PRIMARY KEY,
    "createdAt"      TEXT,
    "name"           TEXT,
    "userId"         TEXT REFERENCES users("id") ON DELETE SET NULL,
    "userIdentifier" TEXT,
    "tags"           TEXT[],
    "metadata"       JSONB
);

CREATE TABLE IF NOT EXISTS steps (
    "id"            TEXT PRIMARY KEY,
    "name"          TEXT NOT NULL,
    "type"          TEXT NOT NULL,
    "threadId"      TEXT NOT NULL REFERENCES threads("id") ON DELETE CASCADE,
    "parentId"      TEXT,
    "command"       TEXT,
    "modes"         TEXT,
    "streaming"     BOOLEAN,
    "waitForAnswer" BOOLEAN,
    "isError"       BOOLEAN,
    "metadata"      TEXT,
    "tags"          TEXT[],
    "input"         TEXT,
    "output"        TEXT,
    "createdAt"     TEXT,
    "start"         TEXT,
    "end"           TEXT,
    "generation"    TEXT,
    "showInput"     TEXT,
    "defaultOpen"   BOOLEAN,
    "autoCollapse"  BOOLEAN,
    "language"      TEXT,
    "icon"          TEXT
);
ALTER TABLE steps ADD COLUMN IF NOT EXISTS "defaultOpen"  BOOLEAN;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS "autoCollapse" BOOLEAN;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS "command"      TEXT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS "modes"        TEXT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS "icon"         TEXT;

CREATE TABLE IF NOT EXISTS elements (
    "id"           TEXT PRIMARY KEY,
    "threadId"     TEXT REFERENCES threads("id") ON DELETE CASCADE,
    "type"         TEXT,
    "url"          TEXT,
    "chainlitKey"  TEXT,
    "name"         TEXT NOT NULL,
    "display"      TEXT,
    "objectKey"    TEXT,
    "size"         TEXT,
    "props"        TEXT,
    "page"         INTEGER,
    "autoPlay"     BOOLEAN,
    "playerConfig" TEXT,
    "language"     TEXT,
    "forId"        TEXT,
    "mime"         TEXT
);

CREATE TABLE IF NOT EXISTS feedbacks (
    "id"       TEXT PRIMARY KEY,
    "forId"    TEXT NOT NULL,
    "value"    INTEGER NOT NULL,
    "threadId" TEXT,
    "comment"  TEXT
);

INSERT INTO users ("id", "identifier", "createdAt", "metadata")
VALUES ('anonymous', 'anonymous', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'), '{"role": "user"}')
ON CONFLICT DO NOTHING;
"""


@cl.on_app_startup
async def on_app_startup() -> None:
    """Create Chainlit schema tables if they don't exist."""
    engine = create_async_engine(_DATABASE_URL)
    try:
        async with engine.begin() as conn:
            for statement in _INIT_SQL.split(";"):
                stmt = statement.strip()
                if stmt:
                    await conn.execute(text(stmt))
    finally:
        await engine.dispose()

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (Path(__file__).parent / "system_prompt.md").read_text()

# ---------------------------------------------------------------------------
# MCP URL
# ---------------------------------------------------------------------------

_MCP_URL = os.environ.get(
    "MCP_URL",
    "http://data-warehouse-mcp.data-warehouse-mcp.svc.cluster.local:8080/mcp",
)

# ---------------------------------------------------------------------------
# Chat Profiles
# ---------------------------------------------------------------------------


@cl.set_chat_profiles
async def chat_profiles(current_user: cl.User) -> list[cl.ChatProfile]:
    profiles = []
    for profile_name in cfg.get_profiles():
        endpoint, model = cfg.get_profiles()[profile_name]
        profiles.append(
            cl.ChatProfile(
                name=profile_name,
                markdown_description=f"**{endpoint.name}** - `{model.id}`",
            )
        )
    return profiles


# ---------------------------------------------------------------------------
# Auth - single anonymous user shared across all browsers
# ---------------------------------------------------------------------------


@cl.header_auth_callback
def header_auth_callback(headers: dict) -> cl.User | None:
    return cl.User(identifier="anonymous", metadata={"role": "user", "provider": "header"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_content_str(result_content: object) -> str:
    """Extract text from MCP tool result content, pretty-printing JSON if possible."""
    texts = []
    for c in result_content:
        try:
            text_val = c.text if hasattr(c, "text") else c.model_dump().get("text", "")
            if text_val:
                texts.append(text_val)
        except Exception:
            texts.append(str(c))

    if texts:
        content_str = "\n".join(texts)
        # Pretty-print if it's a JSON value
        try:
            content_str = _json.dumps(_json.loads(content_str), indent=2)
        except Exception:
            pass
    else:
        try:
            content_str = _json.dumps([c.model_dump() for c in result_content], indent=2)
        except Exception:
            content_str = str(result_content)

    return content_str


async def _generate_title(endpoint: cfg.Endpoint, model: cfg.ModelConfig, user_msg: str, assistant_msg: str) -> str | None:
    """Call the model for a short chat title. Falls back to truncated user message."""
    try:
        client = openai.AsyncOpenAI(
            base_url=endpoint.base_url + "/v1",
            api_key=endpoint.api_key or "none",
        )
        # Thinking models need thinking disabled to avoid the reasoning tokens
        # consuming the entire token budget before content is produced.
        max_tokens = 2000
        extra: dict = {}
        if model.thinking:
            extra["extra_body"] = {"thinking": {"type": "disabled"}}
        # Use stream=True for compatibility - some models (e.g. GLM) return 500
        # on stream=False due to a llama.cpp chat-template serialization bug.
        stream = await client.chat.completions.create(
            model=model.id,
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Generate a short 4-6 word title for the chat below. "
                        "Reply with only the title, no punctuation or quotes.\n\n"
                        f"User: {user_msg[:300]}\n"
                        f"Assistant: {assistant_msg[:300]}"
                    ),
                }
            ],
            max_tokens=max_tokens,
            stream=True,
            **extra,
        )
        parts: list[str] = []
        async for chunk in stream:
            delta = chunk.choices[0].delta.content
            if delta:
                parts.append(delta)
        title = "".join(parts).strip().strip('"\'')
        print(f"[dwh-chat] Generated title from API: {title!r}")
        if title:
            return title
    except Exception as e:
        print(f"[dwh-chat] _generate_title API failed: {e}")

    # Fallback: use first few words of the user message as title.
    if user_msg:
        words = user_msg.split()
        title = " ".join(words[:6])
        if len(words) > 6:
            title += "..."
        print(f"[dwh-chat] Using fallback title: {title!r}")
        return title
    return None


# ---------------------------------------------------------------------------
# Chat lifecycle
# ---------------------------------------------------------------------------


@cl.on_chat_start
async def on_chat_start() -> None:
    profile_name = cl.user_session.get("chat_profile")
    profiles = cfg.get_profiles()

    if profile_name not in profiles:
        profile_name = next(iter(profiles))

    endpoint, model = profiles[profile_name]

    try:
        mcp_session, openai_tools, exit_stack = await connect_mcp(_MCP_URL)
    except Exception as e:
        await cl.Message(
            content=f"⚠️ Failed to connect to the data warehouse MCP server: {e}"
        ).send()
        return

    cl.user_session.set("endpoint", endpoint)
    cl.user_session.set("model", model)
    cl.user_session.set("mcp_session", mcp_session)
    cl.user_session.set("mcp_exit_stack", exit_stack)
    cl.user_session.set("openai_tools", openai_tools)
    cl.user_session.set(
        "messages",
        [{"role": "system", "content": _SYSTEM_PROMPT}],
    )


@cl.on_message
async def on_message(message: cl.Message) -> None:
    endpoint: cfg.Endpoint = cl.user_session.get("endpoint")
    model: cfg.ModelConfig = cl.user_session.get("model")
    mcp_session = cl.user_session.get("mcp_session")
    openai_tools: list[dict] = cl.user_session.get("openai_tools")
    messages: list[dict] = cl.user_session.get("messages")

    if mcp_session is None:
        await cl.Message(
            content="⚠️ No active MCP session. Please start a new conversation."
        ).send()
        return

    messages.append({"role": "user", "content": message.content})

    # Reconnect helper (retry once on failure).
    async def _ensure_mcp() -> bool:
        nonlocal mcp_session, openai_tools
        try:
            await mcp_session.list_tools()
            return True
        except Exception:
            try:
                old_stack = cl.user_session.get("mcp_exit_stack")
                if old_stack:
                    try:
                        await old_stack.aclose()
                    except Exception:
                        pass
                mcp_session, openai_tools, exit_stack = await connect_mcp(_MCP_URL)
                cl.user_session.set("mcp_session", mcp_session)
                cl.user_session.set("mcp_exit_stack", exit_stack)
                cl.user_session.set("openai_tools", openai_tools)
                return True
            except Exception as e:
                await cl.Message(
                    content=f"⚠️ Lost connection to data warehouse and could not reconnect: {e}"
                ).send()
                return False

    if not await _ensure_mcp():
        return

    # Parent step for the current tool-call group.
    # Key design: set ALL properties before send(), never call update().
    # update() can cause Chainlit's frontend to re-render the step at the wrong
    # position. A single send() event is immovable.
    group_step: cl.Step | None = None

    # Track all completed response messages so we can update() them in the DB.
    closed_msgs: list[cl.Message] = []

    async def on_tool_result(tool_name: str, arguments: str, result_content: object) -> None:
        nonlocal group_step, response_msg
        # If there's an in-progress response message from a mid-loop text, close
        # it now so the new tool group appears AFTER it, not inside it.
        if response_msg is not None:
            closed_msgs.append(response_msg)
            response_msg = None

        if group_step is None:
            group_step = cl.Step(name="tools", type="tool")
            await group_step.__aenter__()  # sets start, registers in local_steps, sends once

        # Set all properties BEFORE send() so no update() is ever needed.
        # parent_id picked up from local_steps (group_step is at top of stack).
        child = cl.Step(name=tool_name, type="tool")
        child.start = utc_now()
        child.parent_id = group_step.id
        content_str = _extract_content_str(result_content)
        trimmed = content_str[:4096] + ("…" if len(content_str) > 4096 else "")
        child.input = arguments
        child.output = f"```json\n{trimmed}\n```"
        child.end = utc_now()
        await child.send()  # single event, fully populated - no update() ever

    async def on_group_end() -> None:
        nonlocal group_step
        if group_step is not None:
            # Mark the group as finished so the frontend shows "Used tools".
            # We set end + update() ourselves rather than calling __aexit__(),
            # which also pops local_steps and can cause re-ordering side effects.
            group_step.end = utc_now()
            await group_step.update()
            current = local_steps.get()
            if current and group_step in current:
                current.remove(group_step)
                local_steps.set(current)
            group_step = None

    # Lazy response message: created on the first streamed token so it appears
    # below all tool-call Steps rather than above them.
    response_msg: cl.Message | None = None

    async def stream_callback(token: str) -> None:
        nonlocal response_msg
        if response_msg is None:
            response_msg = cl.Message(content="")
            await response_msg.send()
        await response_msg.stream_token(token)

    try:
        final_text = await run_agent(
            messages=messages,
            endpoint=endpoint,
            model=model.id,
            thinking=model.thinking,
            tools=openai_tools,
            mcp_session=mcp_session,
            on_tool_result=on_tool_result,
            on_group_end=on_group_end,
            stream_callback=stream_callback,
        )
    except Exception as e:
        if response_msg:
            await response_msg.remove()
        await cl.Message(content=f"⚠️ An error occurred: {e}").send()
        return

    print(f"[dwh-chat] run_agent done, final_text len={len(final_text)}", flush=True)
    if response_msg is None:
        response_msg = cl.Message(content=final_text)
        await response_msg.send()
    else:
        await response_msg.update()

    # Persist any mid-loop response messages that were closed by on_tool_result.
    for msg in closed_msgs:
        try:
            await msg.update()
        except Exception:
            pass

    messages.append({"role": "assistant", "content": final_text})
    cl.user_session.set("messages", messages)

    # Generate a title after the very first user message (best-effort, once per session).
    # Run as a background task so task_end is sent immediately and the input is re-enabled.
    if not cl.user_session.get("title_generated", False):
        cl.user_session.set("title_generated", True)
        user_msg = next((m["content"] for m in messages if m["role"] == "user"), "")

        async def _update_title() -> None:
            title = await _generate_title(endpoint, model, user_msg, final_text)
            if title:
                try:
                    thread_id = cl.context.session.thread_id
                    print(f"[dwh-chat] Updating thread {thread_id!r} title to {title!r}", flush=True)
                    await _data_layer.update_thread(thread_id=thread_id, name=title)
                    # Emit the first_interaction event so the frontend sidebar updates immediately.
                    await cl.context.emitter.emit(
                        "first_interaction",
                        {"interaction": title, "thread_id": thread_id},
                    )
                    print(f"[dwh-chat] Thread title updated OK", flush=True)
                except Exception as e:
                    print(f"[dwh-chat] Failed to update thread title: {e}", flush=True)
            else:
                print("[dwh-chat] No title generated (returned None)", flush=True)

        asyncio.create_task(_update_title())


@cl.on_chat_end
async def on_chat_end() -> None:
    exit_stack = cl.user_session.get("mcp_exit_stack")
    if exit_stack:
        try:
            await exit_stack.aclose()
        except Exception:
            pass


@cl.on_chat_resume
async def on_chat_resume(thread: ThreadDict) -> None:
    """Restore session state when a persisted chat thread is reopened."""
    profile_name = cl.user_session.get("chat_profile")
    profiles = cfg.get_profiles()

    if profile_name not in profiles:
        profile_name = next(iter(profiles))

    endpoint, model = profiles[profile_name]

    try:
        mcp_session, openai_tools, exit_stack = await connect_mcp(_MCP_URL)
    except Exception as e:
        await cl.Message(
            content=f"⚠️ Failed to reconnect to the data warehouse MCP server: {e}"
        ).send()
        return

    cl.user_session.set("endpoint", endpoint)
    cl.user_session.set("model", model)
    cl.user_session.set("mcp_session", mcp_session)
    cl.user_session.set("mcp_exit_stack", exit_stack)
    cl.user_session.set("openai_tools", openai_tools)

    # Reconstruct message history from persisted thread steps.
    messages: list[dict] = [{"role": "system", "content": _SYSTEM_PROMPT}]
    for step in thread.get("steps", []):
        step_type = step.get("type")
        content = step.get("output") or step.get("input") or ""
        if not content:
            continue
        if step_type == "user_message":
            messages.append({"role": "user", "content": content})
        elif step_type == "assistant_message":
            messages.append({"role": "assistant", "content": content})

    cl.user_session.set("messages", messages)

