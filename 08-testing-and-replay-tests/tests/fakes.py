"""Fake Activities for Workflow tests. Same *names* and signatures as the real ones — the
Workflow schedules `call_llm` by name, so a Worker that registers these runs them instead.
No model, no tool, no network: each fake is a rule plus a counter the test can read."""
from __future__ import annotations

import asyncio

from temporalio import activity
from temporalio.exceptions import ApplicationError

from workflows import ToolCall, ToolResult

calls = {"call_llm": 0, "execute_tool": 0}
tool_keys: list[str] = []


def reset() -> None:
    calls["call_llm"] = calls["execute_tool"] = 0
    cancelled["execute_tool"] = 0
    tool_keys.clear()


@activity.defn(name="call_llm")
async def fake_call_llm(prompt: str, key: str) -> str:
    """The stand-in model: ask for one tool call, then answer."""
    calls["call_llm"] += 1
    if "RESULT: ok" in prompt:
        return "FINAL: 42"
    return "TOOL: fetch the-answer"


@activity.defn(name="execute_tool")
async def fake_execute_tool(call: ToolCall) -> ToolResult:
    calls["execute_tool"] += 1
    tool_keys.append(call.key)
    return ToolResult(key=call.key, ok=True, output="42")


@activity.defn(name="execute_tool")
async def failing_execute_tool(call: ToolCall) -> ToolResult:
    """A tool that never works: `ok=False` every time. Tests the give-up branch."""
    calls["execute_tool"] += 1
    return ToolResult(key=call.key, ok=False, output="sandbox refused")


@activity.defn(name="execute_tool")
async def slow_execute_tool(call: ToolCall) -> ToolResult:
    """A tool that runs long and heartbeats: cancellation reaches it at the next heartbeat."""
    calls["execute_tool"] += 1
    tool_keys.append(call.key)
    if call.tool.startswith("undo_"):
        return ToolResult(key=call.key, ok=True, output="undone")
    try:
        for i in range(200):
            activity.heartbeat(i)
            await asyncio.sleep(0.05)
    except asyncio.CancelledError:
        cancelled["execute_tool"] += 1            # delivered on a heartbeat; re-raise it
        raise
    return ToolResult(key=call.key, ok=True, output="finally")


@activity.defn(name="execute_tool")
async def hanging_execute_tool(call: ToolCall) -> ToolResult:
    """A tool that hangs and never heartbeats: only a timeout ends it."""
    calls["execute_tool"] += 1
    await asyncio.sleep(3600)
    return ToolResult(key=call.key, ok=True, output="unreachable")


@activity.defn(name="execute_tool")
async def bad_arguments_execute_tool(call: ToolCall) -> ToolResult:
    """Fails non-retryably: the Workflow sees one ActivityError, no retries."""
    calls["execute_tool"] += 1
    raise ApplicationError("tool rejected the arguments", type="BadToolArguments", non_retryable=True)

cancelled = {"execute_tool": 0}


@activity.defn(name="call_llm")
async def fake_call_llm_two_tools(prompt: str, key: str) -> str:
    """Like the stand-in in workflows.py: fetch, then summarize, then answer."""
    calls["call_llm"] += 1
    n = prompt.count("RESULT: ok")
    return ["TOOL: fetch the-answer", "TOOL: summarize the-answer", "FINAL: 42"][min(n, 2)]
