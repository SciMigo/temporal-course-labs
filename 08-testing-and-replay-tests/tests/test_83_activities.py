"""8.3 — Activity tests with ActivityEnvironment: heartbeats, cancellation, idempotency by key."""
from __future__ import annotations

import asyncio

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

import workflows
from workflows import ToolCall, execute_tool


def call(key: str = "agent-test:3:9f3c", tool: str = "fetch") -> ToolCall:
    return ToolCall(key=key, tool=tool, args={"target": "temporal-docs"})


async def test_execute_tool_heartbeats_with_progress_details():
    env = ActivityEnvironment()
    beats: list[tuple] = []
    env.on_heartbeat = lambda *details: beats.append(details)
    result = await env.run(execute_tool, call())
    assert result.ok
    assert beats == [("fetch:1/3",), ("fetch:2/3",), ("fetch:3/3",)]


async def test_execute_tool_is_idempotent_by_key():
    env = ActivityEnvironment()
    first = await env.run(execute_tool, call())
    second = await env.run(execute_tool, call())      # same key: a retry after a Worker death
    assert first == second
    assert workflows.effects() == ["fetch(temporal-docs)"]          # one effect, not two


async def test_a_different_key_is_a_different_effect():
    env = ActivityEnvironment()
    await env.run(execute_tool, call(key="agent-test:3:aaaa"))
    await env.run(execute_tool, call(key="agent-test:5:bbbb"))
    assert workflows.effects() == ["fetch(temporal-docs)", "fetch(temporal-docs)"]


async def test_cancellation_stops_the_tool_before_its_side_effect():
    env = ActivityEnvironment()                       # one env per test: cancel() is sticky
    beats: list[tuple] = []
    env.on_heartbeat = lambda *details: beats.append(details)
    task = asyncio.create_task(env.run(execute_tool, call()))
    await asyncio.sleep(0.07)                         # after the first heartbeat, before the last
    env.cancel()                                      # what the Service does on the next heartbeat
    with pytest.raises(asyncio.CancelledError):
        await task
    assert beats, "a long tool must heartbeat or it can never be cancelled"
    assert workflows.effects() == []                  # cancelled before the effect


async def test_unknown_tool_is_a_non_retryable_application_error():
    env = ActivityEnvironment()
    with pytest.raises(ApplicationError) as err:
        await env.run(execute_tool, call(tool="rm-rf"))
    assert err.value.type == "BadToolArguments" and err.value.non_retryable
