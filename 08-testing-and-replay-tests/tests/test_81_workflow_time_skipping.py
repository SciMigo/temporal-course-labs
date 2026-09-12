"""8.1 — Workflow tests in the time-skipping environment, plus plan() with no environment."""
from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import fakes
from workflows import CHECKPOINT_TIMEOUT, MAX_TOOL_FAILURES, AgentRun, Step


@pytest.fixture(autouse=True)
def _reset_fakes():
    fakes.reset()


# ---------------------------------------------------------------- plan(): no environment at all
def test_plan_first_step_asks_the_model():
    agent = AgentRun()
    agent.goal = "summarize the Temporal docs"
    step = agent.plan()
    assert step == Step("llm", prompt="Goal: summarize the Temporal docs\nMake a plan.")


def test_plan_turns_a_tool_answer_into_a_tool_step():
    agent = AgentRun()
    agent.step, agent.context_summary = 1, "\nTOOL: fetch temporal-docs"
    assert agent.plan() == Step("tool", tool="fetch", args={"target": "temporal-docs"})


def test_plan_finishes_on_final_and_gives_up_after_three_failures():
    agent = AgentRun()
    agent.step, agent.context_summary = 3, "\nFINAL: done"
    assert agent.plan().kind == "finish"
    agent.context_summary, agent.attempts = "\nRESULT: error x", MAX_TOOL_FAILURES
    assert agent.plan().kind == "finish"          # the give-up branch, no model asked


# ---------------------------------------------------------------- the loop, Activities mocked
async def test_agent_loop_calls_one_tool_then_answers():
    tq = f"lab08-{uuid.uuid4()}"
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=tq, workflows=[AgentRun],
                          activities=[fakes.fake_call_llm, fakes.fake_execute_tool]):
            result = await env.client.execute_workflow(
                AgentRun.run, "find the answer", id=f"lab08-test-{uuid.uuid4()}", task_queue=tq)
    assert result == "42"
    assert fakes.calls == {"call_llm": 2, "execute_tool": 1}
    # The key the Workflow handed the tool: workflow_id:step:hash — step 1 is the tool step.
    assert fakes.tool_keys[0].split(":")[1] == "1"


async def test_agent_gives_up_after_three_failed_tool_steps():
    tq = f"lab08-{uuid.uuid4()}"
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=tq, workflows=[AgentRun],
                          activities=[fakes.fake_call_llm, fakes.failing_execute_tool]):
            result = await env.client.execute_workflow(
                AgentRun.run, "find the answer", id=f"lab08-test-{uuid.uuid4()}", task_queue=tq)
    assert result == f"gave up after {MAX_TOOL_FAILURES} failed tool steps"
    assert fakes.calls["execute_tool"] == MAX_TOOL_FAILURES


async def test_paused_agent_expires_after_72_hours_in_under_a_second():
    """The 72-hour checkpoint: paused before its first step, never resumed."""
    tq = f"lab08-{uuid.uuid4()}"
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=tq, workflows=[AgentRun],
                          activities=[fakes.fake_call_llm, fakes.fake_execute_tool]):
            handle = await env.client.start_workflow(
                AgentRun.run, "needs approval", id=f"lab08-test-{uuid.uuid4()}", task_queue=tq,
                start_signal="pause")               # delivered before the first Workflow Task
            result = await handle.result()
            assert result == "expired: no resume within 72h"
            # The history really contains a 72-hour timer; the test did not wait for it.
            timers = [e for e in (await handle.fetch_history()).events
                      if e.HasField("timer_started_event_attributes")]
            assert len(timers) == 1
            assert timers[0].timer_started_event_attributes.start_to_fire_timeout.ToTimedelta() == CHECKPOINT_TIMEOUT
            assert fakes.calls["call_llm"] == 0     # it never got past the checkpoint
    assert CHECKPOINT_TIMEOUT == timedelta(hours=72)


async def test_resume_signal_releases_the_checkpoint():
    tq = f"lab08-{uuid.uuid4()}"
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=tq, workflows=[AgentRun],
                          activities=[fakes.fake_call_llm, fakes.fake_execute_tool]):
            handle = await env.client.start_workflow(
                AgentRun.run, "needs approval", id=f"lab08-test-{uuid.uuid4()}", task_queue=tq,
                start_signal="pause")
            # Time skipping only advances while the Workflow is blocked *and* nobody holds the
            # clock: a query then a signal is the human coming back a day later.
            assert (await handle.query(AgentRun.status))["status"] == "paused"
            await handle.signal(AgentRun.resume)
            assert await handle.result() == "42"
