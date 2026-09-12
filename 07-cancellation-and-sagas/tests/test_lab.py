"""Lab 7 self-check against the time-skipping test server:  python -m pytest tests/ -q

Activities run in real time even under time skipping, so the fake work is scaled down
(`workflows.SPEED`) and the crunch tool is a few chunks long.
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", ".."))

import workflows  # noqa: E402
from temporalio.api.enums.v1 import EventType  # noqa: E402
from temporalio.client import WorkflowFailureError  # noqa: E402
from temporalio.exceptions import ActivityError, CancelledError  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from workflows import (  # noqa: E402
    AgentRun,
    ToolOptions,
    allocate_sandbox,
    call_llm,
    execute_tool,
    register_endpoint,
    release_gpu,
    release_sandbox,
    reserve_gpu,
    start_model,
    stop_model,
    unregister_endpoint,
)

ACTIVITIES = [call_llm, execute_tool, reserve_gpu, allocate_sandbox, start_model, register_endpoint,
              unregister_endpoint, stop_model, release_sandbox, release_gpu]


@pytest.fixture(autouse=True)
def fast_activities(monkeypatch):
    monkeypatch.setattr(workflows, "SPEED", 0.05)      # 1 s of fake work -> 50 ms
    workflows.FINISHED_TOOLS.clear()


async def _annotated(handle) -> list[str]:
    """['ACTIVITY_TASK_SCHEDULED reserve_gpu', 'ACTIVITY_TASK_COMPLETED', ...] in history order."""
    out = []
    async for ev in handle.fetch_history_events():
        name = EventType.Name(ev.event_type).removeprefix("EVENT_TYPE_")
        if ev.HasField("activity_task_scheduled_event_attributes"):
            name += " " + ev.activity_task_scheduled_event_attributes.activity_type.name
        out.append(name)
    return out


def _scheduled(names: list[str]) -> list[str]:
    return [n.split(" ", 1)[1] for n in names if n.startswith("ACTIVITY_TASK_SCHEDULED ")]


def test_saga_runs_compensations_in_reverse_when_register_endpoint_fails() -> None:
    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            tq = f"lab-07-test-{uuid.uuid4()}"
            async with Worker(env.client, task_queue=tq, workflows=[AgentRun], activities=ACTIVITIES):
                handle = await env.client.start_workflow(
                    AgentRun.run, args=["deploy the fine-tuned model", ToolOptions()],
                    id=f"lab07-test-{uuid.uuid4()}", task_queue=tq)
                with pytest.raises(WorkflowFailureError) as exc:
                    await handle.result()
                assert isinstance(exc.value.cause, ActivityError)          # register_endpoint, retries spent
                assert (await handle.describe()).status.name == "FAILED"
                names = await _annotated(handle)
                assert _scheduled(names) == [
                    "call_llm", "reserve_gpu", "allocate_sandbox", "start_model", "register_endpoint",
                    "stop_model", "release_sandbox", "release_gpu",          # reverse order; no unregister_endpoint
                ]
                assert names.count("ACTIVITY_TASK_FAILED") == 1              # one failure Event after 3 attempts
                assert names.index("ACTIVITY_TASK_FAILED") < names.index("ACTIVITY_TASK_SCHEDULED stop_model")
                status = await handle.query(AgentRun.status)
                assert status["status"] == "failed" and status["rolled_back"] == ["stop_model", "release_sandbox", "release_gpu"]

    asyncio.run(go())


def test_cancel_without_heartbeats_the_tool_runs_to_the_end_anyway() -> None:
    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            tq = f"lab-07-test-{uuid.uuid4()}"
            async with Worker(env.client, task_queue=tq, workflows=[AgentRun], activities=ACTIVITIES):
                handle = await env.client.start_workflow(
                    AgentRun.run, args=["crunch the eval set", ToolOptions(heartbeat=False, chunks=30)],
                    id=f"lab07-test-{uuid.uuid4()}", task_queue=tq)
                while (await handle.query(AgentRun.status))["in_flight"] != "crunch":
                    await asyncio.sleep(0.05)
                await handle.cancel()
                with pytest.raises(WorkflowFailureError) as exc:
                    await handle.result()
                assert isinstance(exc.value.cause, CancelledError)
                assert (await handle.describe()).status.name == "CANCELED"
                names = await _annotated(handle)
                assert "ACTIVITY_TASK_CANCEL_REQUESTED" in names
                assert "ACTIVITY_TASK_CANCELED" not in names                 # nobody could tell the Activity
                assert _scheduled(names)[-1] == "release_sandbox"            # cleanup ran, unshielded, in `finally`
                assert (await handle.query(AgentRun.status))["rolled_back"] == ["release_sandbox"]
                # ... and the tool is still running on the Worker. Wait for it: it finishes every chunk.
                for _ in range(200):
                    if workflows.FINISHED_TOOLS:
                        break
                    await asyncio.sleep(0.05)
                assert workflows.FINISHED_TOOLS == [f"{handle.id}:1:crunch"]

    asyncio.run(go())


def test_cancel_with_heartbeats_stops_the_tool_and_cleanup_still_runs(monkeypatch) -> None:
    # The cancel rides back on the next heartbeat, up to 0.8 x HEARTBEAT_TIMEOUT later; the run must
    # still be open then for the acknowledgement to land in *its* history, so the cleanup is slow here.
    monkeypatch.setattr(workflows, "UNDO_SECONDS", 80)      # x SPEED 0.05 = 4 s

    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            tq = f"lab-07-test-{uuid.uuid4()}"
            async with Worker(env.client, task_queue=tq, workflows=[AgentRun], activities=ACTIVITIES):
                handle = await env.client.start_workflow(
                    AgentRun.run, args=["crunch the eval set", ToolOptions(heartbeat=True, chunks=200)],
                    id=f"lab07-test-{uuid.uuid4()}", task_queue=tq)
                while (await handle.query(AgentRun.status))["in_flight"] != "crunch":
                    await asyncio.sleep(0.05)
                await handle.cancel()
                with pytest.raises(WorkflowFailureError):
                    await handle.result()
                assert (await handle.describe()).status.name == "CANCELED"
                names = await _annotated(handle)
                assert "ACTIVITY_TASK_CANCEL_REQUESTED" in names
                assert "ACTIVITY_TASK_CANCELED" in names                     # the Activity acknowledged the cancel
                assert _scheduled(names)[-1] == "release_sandbox"
                assert workflows.FINISHED_TOOLS == []                        # it did not run to the end

    asyncio.run(go())


def test_shielded_tool_finishes_first_then_the_run_closes_cancelled() -> None:
    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            tq = f"lab-07-test-{uuid.uuid4()}"
            async with Worker(env.client, task_queue=tq, workflows=[AgentRun], activities=ACTIVITIES):
                handle = await env.client.start_workflow(
                    AgentRun.run, args=["crunch the eval set", ToolOptions(heartbeat=True, shield=True, chunks=30)],
                    id=f"lab07-test-{uuid.uuid4()}", task_queue=tq)
                while (await handle.query(AgentRun.status))["in_flight"] != "crunch":
                    await asyncio.sleep(0.05)
                await handle.cancel()
                with pytest.raises(WorkflowFailureError):
                    await handle.result()
                assert (await handle.describe()).status.name == "CANCELED"
                names = await _annotated(handle)
                assert "ACTIVITY_TASK_CANCEL_REQUESTED" not in names         # the shield kept the cancel off the Activity
                assert names.index("ACTIVITY_TASK_SCHEDULED execute_tool") < names.index("WORKFLOW_EXECUTION_CANCEL_REQUESTED")
                assert _scheduled(names)[-2:] == ["execute_tool", "release_sandbox"]
                assert names.count("ACTIVITY_TASK_COMPLETED") == 4           # llm, sandbox, the tool, the release
                assert workflows.FINISHED_TOOLS == [f"{handle.id}:1:crunch"]

    asyncio.run(go())
