"""The v2 loop in the time-skipping environment: the verify step runs, and a rejected
verdict counts as a failed attempt instead of being handed to the model as a result."""
from __future__ import annotations

import uuid

from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import workflows_v2
from workflows import ToolCall, ToolResult
from workflows_v2 import AgentRun, Verdict

calls = {"call_llm": 0, "execute_tool": 0, "evaluate": 0}


@activity.defn(name="call_llm")
async def fake_call_llm(prompt: str, key: str) -> str:
    calls["call_llm"] += 1
    return "FINAL: 42" if "RESULT: ok" in prompt else "TOOL: fetch the-answer"


@activity.defn(name="execute_tool")
async def fake_execute_tool(call: ToolCall) -> ToolResult:
    calls["execute_tool"] += 1
    return ToolResult(key=call.key, ok=True, output="42")


@activity.defn(name="evaluate")
async def picky_evaluate(result: ToolResult) -> Verdict:
    calls["evaluate"] += 1
    return Verdict(ok=calls["evaluate"] >= 2, reason="first answer never trusted")


async def test_v2_verifies_every_tool_result():
    for k in calls:
        calls[k] = 0
    tq = f"lab09-{uuid.uuid4()}"
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=tq, workflows=[AgentRun],
                          activities=[fake_call_llm, fake_execute_tool, picky_evaluate]):
            handle = await env.client.start_workflow(
                AgentRun.run, "find the answer", id=f"lab09-test-{uuid.uuid4()}", task_queue=tq)
            assert await handle.result() == "42"
            status = await handle.query(AgentRun.status)
    # tool -> rejected verdict (attempt 1) -> model asked again -> tool -> accepted -> answer
    assert calls == {"call_llm": 3, "execute_tool": 2, "evaluate": 2}
    assert status["attempts"] == 1
    assert "RESULT: error verify: first answer never trusted" in status["context_summary"]
    assert workflows_v2.BUILD_ID == "v2"
