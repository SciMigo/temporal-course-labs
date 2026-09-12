"""Lab 5 self-check. Runs against the time-skipping test server (no dev server needed):

    python -m pytest tests/ -q
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))          # workflows.py
sys.path.insert(0, os.path.join(HERE, "..", ".."))    # common

from temporalio.client import WorkflowUpdateFailedError, WorkflowUpdateStage  # noqa: E402
from temporalio.service import RPCError  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from workflows import AgentRun, call_llm, expand_note  # noqa: E402


async def _update_events(handle) -> list[str]:
    """Update IDs of every accepted Update in history. A rejected Update writes no Update Event
    (the dev server writes nothing at all; the time-skipping test server records the Workflow
    Task that ran the validator, which is why this counts Update Events and not all Events)."""
    return [
        ev.workflow_execution_update_accepted_event_attributes.protocol_instance_id
        async for ev in handle.fetch_history_events()
        if ev.HasField("workflow_execution_update_accepted_event_attributes")
    ]


async def _signal_names(handle) -> list[str]:
    return [
        ev.workflow_execution_signaled_event_attributes.signal_name
        async for ev in handle.fetch_history_events()
        if ev.HasField("workflow_execution_signaled_event_attributes")
    ]


async def _start(env: WorkflowEnvironment, task_queue: str):
    return await env.client.start_workflow(
        AgentRun.run, "summarize the Temporal docs", id=f"lab05-test-{uuid.uuid4()}", task_queue=task_queue
    )


def test_pause_blocks_the_loop_and_resume_releases_it() -> None:
    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            tq = f"lab-05-test-{uuid.uuid4()}"
            async with Worker(env.client, task_queue=tq, workflows=[AgentRun], activities=[call_llm]):
                handle = await _start(env, tq)
                await handle.signal(AgentRun.pause)
                status = await handle.query(AgentRun.status)
                assert status["paused"] is True and status["status"] == "running"
                await env.sleep(60)                      # the step in flight finishes; the loop blocks
                step_when_paused = (await handle.query(AgentRun.status))["step"]
                assert step_when_paused <= 1
                await env.sleep(600)                     # ten minutes pass; nothing moves while paused
                assert (await handle.query(AgentRun.status))["step"] == step_when_paused
                await handle.signal(AgentRun.resume)
                await handle.result()                    # time-skipping runs the 20 timers instantly
                final = await handle.query(AgentRun.status)
                assert final["status"] == "done" and final["paused"] is False
                assert (await _signal_names(handle)) == ["pause", "resume"]

    asyncio.run(go())


def test_rejected_update_leaves_no_event_and_accepted_update_returns_the_goal() -> None:
    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            tq = f"lab-05-test-{uuid.uuid4()}"
            async with Worker(env.client, task_queue=tq, workflows=[AgentRun], activities=[call_llm]):
                handle = await _start(env, tq)
                await handle.signal(AgentRun.pause)
                await env.sleep(60)                      # the step in flight finishes; the loop blocks
                assert (await handle.query(AgentRun.status))["paused"] is True
                assert await _update_events(handle) == []
                with pytest.raises(WorkflowUpdateFailedError) as exc:
                    await handle.execute_update(AgentRun.change_goal, args=["   ", "cmd-9f20"])
                assert "goal must not be empty" in str(exc.value.cause)
                assert await _update_events(handle) == []         # rejected: no Update Event, ever

                goal = await handle.execute_update(AgentRun.change_goal, args=["write the tests first", "cmd-9f1e"])
                assert goal == "write the tests first"
                again = await handle.execute_update(AgentRun.change_goal, args=["write the tests first", "cmd-9f1e"])
                assert again == "write the tests first"           # retried: same answer, no second write
                assert len(await _update_events(handle)) == 2     # both deliveries accepted; one write
                status = await handle.query(AgentRun.status)
                assert status["goal"] == "write the tests first"
                assert status["notes"] == ["Operator: goal changed to: write the tests first"]
                await handle.signal(AgentRun.cancel_run)
                await handle.result()

    asyncio.run(go())


def test_retried_signal_is_recorded_twice_and_applied_once() -> None:
    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            tq = f"lab-05-test-{uuid.uuid4()}"
            async with Worker(env.client, task_queue=tq, workflows=[AgentRun], activities=[call_llm]):
                handle = await _start(env, tq)
                await handle.signal(AgentRun.pause)
                for _ in range(2):                       # the client's connection "broke"; it re-sends
                    await handle.signal(AgentRun.inject_context, args=["Prefer sources after 2025", "cmd-7f3a"])
                status = await handle.query(AgentRun.status)
                assert status["notes"] == ["Operator: Prefer sources after 2025"]
                assert status["processed_command_ids"] == ["cmd-7f3a"]
                assert (await _signal_names(handle)).count("inject_context") == 2
                await handle.signal(AgentRun.cancel_run)
                result = await handle.result()
                assert "Operator: Prefer sources after 2025" in result.splitlines()
                assert (await handle.query(AgentRun.status))["status"] == "cancelled"

    asyncio.run(go())


@pytest.mark.parametrize("drain", [False, True])
def test_continue_as_new_under_a_live_handler_loses_it_unless_drained(drain: bool) -> None:
    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            tq = f"lab-05-test-{uuid.uuid4()}"
            async with Worker(env.client, task_queue=tq, workflows=[AgentRun], activities=[call_llm, expand_note]):
                handle = await _start(env, tq)
                first_run = (await handle.describe()).run_id
                updates = [
                    await handle.start_update(AgentRun.enrich_context, args=[text, f"cmd-{n}"],
                                              wait_for_stage=WorkflowUpdateStage.ACCEPTED)
                    for n, text in enumerate(["Prefer primary sources", "Cite line numbers"])
                ]
                await handle.signal(AgentRun.pause)
                await handle.signal(AgentRun.request_continue_as_new, drain)
                outcomes = []
                for u in updates:
                    try:
                        outcomes.append(await u.result())
                    except (WorkflowUpdateFailedError, RPCError) as e:
                        # dev server 1.31: WorkflowUpdateFailedError (AcceptedUpdateCompletedWorkflow);
                        # the time-skipping test server: RPCError "workflow execution already completed"
                        outcomes.append(f"lost: {type(e).__name__}")
                status = await handle.query(AgentRun.status)           # the new run
                assert (await handle.describe()).run_id != first_run
                assert status["paused"] is True                         # the pause rode along
                if drain:
                    assert outcomes == ["Prefer primary sources (expanded)", "Cite line numbers (expanded)"]
                    assert status["processed_command_ids"] == ["cmd-0", "cmd-1"]
                    assert len(status["notes"]) == 2
                else:
                    assert all(o.startswith("lost:") for o in outcomes)                # both lost
                    assert status["notes"] == [] and status["processed_command_ids"] == []
                await handle.signal(AgentRun.cancel_run)
                await handle.result()

    asyncio.run(go())
