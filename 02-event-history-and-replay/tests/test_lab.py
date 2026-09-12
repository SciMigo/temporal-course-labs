"""Check yourself: the history shape you annotated in 2.1, and that replay never re-runs call_llm.

Runs against Temporal's time-skipping test server (downloaded on first use), so the 20 s timer
costs nothing. Plain pytest; no plugin needed:  PYTHONPATH=.. pytest tests/
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))          # workflows
sys.path.insert(0, os.path.join(HERE, "..", ".."))    # common

import workflows  # noqa: E402
from temporalio.api.enums.v1 import EventType  # noqa: E402
from temporalio.client import WorkflowHistory  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Replayer, Worker  # noqa: E402

TASK_QUEUE = "lab-02-test"

# The reading's table, minus the Worker kill: one Worker, so the last Workflow Task lands on the
# sticky queue and there is no WorkflowTaskTimedOut / reschedule pair.
EXPECTED_ONE_STEP = [
    "WORKFLOW_EXECUTION_STARTED",
    "WORKFLOW_TASK_SCHEDULED", "WORKFLOW_TASK_STARTED", "WORKFLOW_TASK_COMPLETED",
    "ACTIVITY_TASK_SCHEDULED", "ACTIVITY_TASK_STARTED", "ACTIVITY_TASK_COMPLETED",
    "WORKFLOW_TASK_SCHEDULED", "WORKFLOW_TASK_STARTED", "WORKFLOW_TASK_COMPLETED",
    "TIMER_STARTED", "TIMER_FIRED",
    "WORKFLOW_TASK_SCHEDULED", "WORKFLOW_TASK_STARTED", "WORKFLOW_TASK_COMPLETED",
    "WORKFLOW_EXECUTION_COMPLETED",
]


def _types(history: WorkflowHistory) -> list[str]:
    return [EventType.Name(e.event_type).removeprefix("EVENT_TYPE_") for e in history.events]


async def _run(steps: int, wf_id: str) -> WorkflowHistory:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=TASK_QUEUE,
                          workflows=[workflows.AgentRun], activities=[workflows.call_llm]):
            handle = await env.client.start_workflow(
                workflows.AgentRun.run, args=["test goal", 20, steps], id=wf_id, task_queue=TASK_QUEUE
            )
            result = await handle.result()
            assert result.startswith("[model answer to:")
            return await handle.fetch_history()


def test_one_step_history_is_sixteen_events():
    history = asyncio.run(_run(1, "lab02-test-one-step"))
    assert _types(history) == EXPECTED_ONE_STEP


def test_each_step_costs_eleven_events():
    """2.1, last part: an Activity is 3 Events, the Workflow Task that delivers its result 3 more,
    the timer 2, and the Workflow Task that wakes after the timer 3 — 11 per step."""
    history = asyncio.run(_run(3, "lab02-test-three-steps"))
    assert len(history.events) == 4 + 3 * 11 + 1
    assert _types(history).count("ACTIVITY_TASK_SCHEDULED") == 3


def test_replay_does_not_invoke_call_llm():
    """The Activity function ran once, in the Worker. Replaying the history — full, or cut at the
    Workflow Task after TimerFired — issues the same Commands and never calls it again."""
    history = asyncio.run(_run(1, "lab02-test-replay"))
    before = workflows.CALL_LLM_INVOCATIONS

    async def replay(h: WorkflowHistory) -> None:
        await Replayer(workflows=[workflows.AgentRun]).replay_workflow(h)

    asyncio.run(replay(history))
    cut = WorkflowHistory(workflow_id=history.workflow_id, events=history.events[:13])
    assert _types(cut)[-1] == "WORKFLOW_TASK_SCHEDULED"
    asyncio.run(replay(cut))
    assert workflows.CALL_LLM_INVOCATIONS == before


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
