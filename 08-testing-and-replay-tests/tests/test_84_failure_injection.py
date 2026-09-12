"""8.4 — Failure injection: Worker crash, Activity timeout, duplicate execution, cancellation.

Each is a test, not a war story. Every one ends with the recurring question: what survived,
and what did the returning Worker do with it."""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import uuid
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.api.enums.v1 import PendingActivityState, TimeoutType
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import fakes
import workflows
from workflows import AgentRun, ToolCall, ToolResult, execute_tool

HERE = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(autouse=True)
def _reset_fakes():
    fakes.reset()


async def pending_activity_started(handle) -> bool:
    """`describe` shows what the history does not: the attempt currently running."""
    desc = await handle.describe()
    return any(a.state == PendingActivityState.PENDING_ACTIVITY_STATE_STARTED
               for a in desc.raw_description.pending_activities)


# ---------------------------------------------------------------- 1. Worker crash
async def test_worker_crash_mid_activity_the_next_worker_finishes_the_run():
    """kill -9 a Worker while execute_tool is running; a fresh Worker finishes the run.

    Runs on `start_local()` — a real dev server in a subprocess — rather than the time-skipping
    server, because a crash exercises two Service timeouts the time-skipping server was observed
    not to fire for a dead Worker's sticky queue: the Activity's heartbeat timeout (5 s) and the
    sticky Workflow Task's schedule-to-start timeout (5 s). Expect ~10 s of real time; that is
    the honest cost of a crash, and the history below is what you saw in lab 1."""
    tq = f"lab08-{uuid.uuid4()}"
    async with await WorkflowEnvironment.start_local() as env:
        address = env.client.service_client.config.target_host
        proc = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "crashable_worker.py")],
            env={**os.environ, "TEMPORAL_ADDRESS": address, "TEMPORAL_NAMESPACE": env.client.namespace,
                 "TASK_QUEUE": tq},
            stdout=subprocess.PIPE, text=True)
        try:
            assert proc.stdout.readline().strip() == "ready"
            handle = await env.client.start_workflow(
                AgentRun.run, "survive a crash", id=f"lab08-test-{uuid.uuid4()}", task_queue=tq)
            while not await pending_activity_started(handle):   # execute_tool is running in the subprocess
                await asyncio.sleep(0.1)
            proc.send_signal(signal.SIGKILL)                       # kill -9, not Ctrl-C
            proc.wait(timeout=10)
            # Nothing reports the death. The Service learns of it only when the heartbeat stops
            # arriving; heartbeat_timeout later the attempt is timed out and attempt 2 scheduled.
            async with Worker(env.client, task_queue=tq, workflows=[AgentRun],
                              activities=[fakes.fake_call_llm, fakes.fake_execute_tool]):
                assert await asyncio.wait_for(handle.result(), timeout=60) == "42"
            events = (await handle.fetch_history()).events
            started = [e.activity_task_started_event_attributes for e in events
                       if e.HasField("activity_task_started_event_attributes")]
            # The history records one ActivityTaskStarted per *closed* Activity, carrying the
            # attempt that closed it: attempt 2, on the new Worker, with attempt 1's failure —
            # a heartbeat timeout — as last_failure. Attempt 1 itself left no event.
            tool_start = next(a for a in started if a.attempt == 2)
            assert not tool_start.identity.startswith("crashable-")
            assert tool_start.last_failure.timeout_failure_info.timeout_type == TimeoutType.TIMEOUT_TYPE_HEARTBEAT
            # And the dead Worker's sticky queue: its Workflow Task timed out on schedule-to-start
            # before the Service re-offered it on the normal queue to the Worker that was alive.
            assert any(e.HasField("workflow_task_timed_out_event_attributes") and
                       e.workflow_task_timed_out_event_attributes.timeout_type == TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_START
                       for e in events)
        finally:
            if proc.poll() is None:
                proc.kill()


# ---------------------------------------------------------------- 2. Activity timeout
@activity.defn(name="call_llm")
async def call_llm_that_stops_on_error(prompt: str, key: str) -> str:
    if "RESULT: error" in prompt:
        return "FINAL: gave up, the tool timed out"
    return "TOOL: fetch the-answer"


async def test_activity_timeout_is_retried_then_surfaces_as_an_error_the_workflow_handles():
    tq = f"lab08-{uuid.uuid4()}"
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=tq, workflows=[AgentRun],
                          activities=[call_llm_that_stops_on_error, fakes.hanging_execute_tool]):
            handle = await env.client.start_workflow(
                AgentRun.run, "wait forever", id=f"lab08-test-{uuid.uuid4()}", task_queue=tq)
            # A hung tool never heartbeats. Each attempt dies on heartbeat_timeout (5 s); the
            # RetryPolicy allows 3. Time is skipped past each one instead of waited for.
            for _ in range(3):
                await env.sleep(timedelta(seconds=7))
            result = await handle.result()
            assert result == "gave up, the tool timed out"
            status = await handle.query(AgentRun.status)
            assert "RESULT: error TimeoutError" in status["context_summary"]
            assert status["attempts"] == 1
            assert fakes.calls["execute_tool"] == 3          # three attempts ran, none finished


# ---------------------------------------------------------------- 3. Duplicate execution
@activity.defn(name="execute_tool")
async def execute_tool_reported_late(call: ToolCall) -> ToolResult:
    """The real tool, but attempt 1 dies *after* the side effect and before reporting — the
    duplicate-execution injection. Attempt 2 receives the same ToolCall, same key."""
    result = await execute_tool(call)
    if activity.info().attempt == 1:
        raise ApplicationError("worker lost after the effect", type="LostReport")
    return result


@activity.defn(name="execute_tool")
async def execute_tool_ignoring_its_key(call: ToolCall) -> ToolResult:
    """The bug this injection exists to find: a tool with no system of record."""
    workflows._EFFECT_LOG.append(f"{call.tool}({call.args.get('target', '')})")
    if activity.info().attempt == 1:
        raise ApplicationError("worker lost after the effect", type="LostReport")
    return ToolResult(key=call.key, ok=True, output="done")


async def _run_duplicate_injection(tool) -> list[str]:
    tq = f"lab08-{uuid.uuid4()}"
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=tq, workflows=[AgentRun],
                          activities=[fakes.fake_call_llm, tool]):
            await env.client.execute_workflow(
                AgentRun.run, "do it once", id=f"lab08-test-{uuid.uuid4()}", task_queue=tq)
    return workflows.effects()


async def test_duplicate_delivery_with_the_same_key_has_one_effect():
    assert await _run_duplicate_injection(execute_tool_reported_late) == ["fetch(the-answer)"]


async def test_the_injection_catches_a_tool_that_ignores_its_key():
    # This is what the suite is for. A tool without a system of record double-fires under the
    # exact retry the happy path never exercises. (Asserting the bug so the lesson is visible;
    # in your own suite this test asserts == 1 and the tool that fails it gets fixed.)
    assert await _run_duplicate_injection(execute_tool_ignoring_its_key) == ["fetch(the-answer)", "fetch(the-answer)"]


# ---------------------------------------------------------------- 4. Cancellation
async def test_cancel_during_a_long_tool_stops_it_and_runs_the_compensation():
    tq = f"lab08-{uuid.uuid4()}"
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=tq, workflows=[AgentRun],
                          activities=[fakes.fake_call_llm_two_tools, fakes.slow_execute_tool]):
            handle = await env.client.start_workflow(
                AgentRun.run, "take your time", id=f"lab08-test-{uuid.uuid4()}", task_queue=tq)
            # Let the first tool (fetch) finish so there is something to compensate, then
            # cancel while the second (summarize) is running.
            while fakes.calls["execute_tool"] < 2 or not await pending_activity_started(handle):
                await asyncio.sleep(0.05)
            await handle.cancel()
            with pytest.raises(WorkflowFailureError) as err:
                await handle.result()
            assert isinstance(err.value.cause, CancelledError)
            # The tool learns of it on its next *sent* heartbeat — the SDK throttles heartbeats
            # to 80% of heartbeat_timeout (5 s), so this can take up to ~4 s of real time.
            for _ in range(120):
                if fakes.cancelled["execute_tool"]:
                    break
                await asyncio.sleep(0.05)
            assert fakes.cancelled["execute_tool"] == 1           # the running tool was cancelled ...
            assert [k for k in fakes.tool_keys if k.endswith(":undo")], "... and undo_fetch ran"
            assert (await handle.query(AgentRun.status))["status"] == "cancelled"
