"""Lab 6 self-check against the time-skipping test server:  python -m pytest tests/ -q"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", ".."))

from temporalio.api.enums.v1 import EventType  # noqa: E402
from temporalio.client import Client  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from workflows import (  # noqa: E402
    MAX_STEPS,
    RESEARCH_TASK_QUEUE,
    STEPS_PER_RUN,
    AgentRun,
    ResearchAgent,
    call_llm,
    fetch_source,
)


async def _events(client: Client, workflow_id: str, run_id: str) -> list:
    return [ev async for ev in client.get_workflow_handle(workflow_id, run_id=run_id).fetch_history_events()]


async def _run_chain(client: Client, workflow_id: str, first_run_id: str) -> list[list]:
    """Histories of every run, following WorkflowExecutionContinuedAsNew.new_execution_run_id."""
    chain, run_id = [], first_run_id
    while run_id:
        events = await _events(client, workflow_id, run_id)
        chain.append(events)
        last = events[-1]
        run_id = (last.workflow_execution_continued_as_new_event_attributes.new_execution_run_id
                  if last.HasField("workflow_execution_continued_as_new_event_attributes") else None)
    return chain


def _names(events) -> list[str]:
    return [EventType.Name(ev.event_type).removeprefix("EVENT_TYPE_") for ev in events]


async def _with_workers(env: WorkflowEnvironment, goal: str, inject: bool):
    tq = f"lab-06-test-{uuid.uuid4()}"
    agent = Worker(env.client, task_queue=tq, workflows=[AgentRun], activities=[call_llm])
    research = Worker(env.client, task_queue=RESEARCH_TASK_QUEUE, workflows=[ResearchAgent], activities=[fetch_source])
    async with agent, research:
        handle = await env.client.start_workflow(AgentRun.run, goal, id=f"lab06-test-{uuid.uuid4()}", task_queue=tq)
        if inject:
            await handle.signal(AgentRun.inject_context, args=["Prefer sources after 2025", "cmd-7f3a"])
            first = await handle.query(AgentRun.status)          # answered by run 1
            assert first["run_id"] == handle.result_run_id and first["processed_command_ids"] == ["cmd-7f3a"]
            await env.sleep(6)                                   # skip ahead: 4 steps of 0.5 s per run -> run 3 or so
            later = await handle.query(AgentRun.status)          # same handle, a later run answers
            assert later["run_id"] != first["run_id"] and later["step"] > first["step"]
            assert later["processed_command_ids"] == ["cmd-7f3a"]   # the dedupe set travelled in AgentState
        result = await handle.result()                 # follows the chain; time-skipping runs every timer
        return handle, result


def test_one_workflow_id_several_run_ids_each_history_short() -> None:
    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            handle, result = await _with_workers(env, "summarize the Temporal docs", inject=True)
            chain = await _run_chain(env.client, handle.id, handle.result_run_id)
            assert len(chain) == MAX_STEPS // STEPS_PER_RUN == 5
            for events in chain[:-1]:
                assert _names(events)[-1] == "WORKFLOW_EXECUTION_CONTINUED_AS_NEW"
                assert len(events) < 80                # each run's history is short; the state travels as AgentState
            assert _names(chain[-1])[-1] == "WORKFLOW_EXECUTION_COMPLETED"
            started = chain[1][0].workflow_execution_started_event_attributes
            assert started.continued_execution_run_id == handle.result_run_id   # run 2 names run 1 as its origin
            # the operator note was signalled to run 1 and is in the final result: AgentState carried it
            assert result.count("Operator: Prefer sources after 2025") == 1
            assert (await env.client.get_workflow_handle(handle.id, run_id=handle.result_run_id).describe()).status.name == "CONTINUED_AS_NEW"

    asyncio.run(go())


def test_research_child_has_its_own_history_and_one_event_pair_in_the_parent() -> None:
    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            handle, result = await _with_workers(env, "summarize the Temporal docs", inject=False)
            assert "3 sources on sources for: summarize the Temporal docs" in result
            child_id = f"{handle.id}/research/1"
            child = env.client.get_workflow_handle(child_id)
            child_names = _names([ev async for ev in child.fetch_history_events()])
            assert child_names.count("ACTIVITY_TASK_COMPLETED") == 3     # the child's steps live in the child's history
            assert (await child.describe()).status.name == "COMPLETED"
            parent_names = [n for run in await _run_chain(env.client, handle.id, handle.result_run_id) for n in _names(run)]
            assert parent_names.count("START_CHILD_WORKFLOW_EXECUTION_INITIATED") == 1
            assert parent_names.count("CHILD_WORKFLOW_EXECUTION_STARTED") == 1
            assert parent_names.count("CHILD_WORKFLOW_EXECUTION_COMPLETED") == 1
            assert "fetch_source" not in str(parent_names)

    asyncio.run(go())


def test_child_failure_reaches_the_parent_as_a_catchable_error() -> None:
    async def go() -> None:
        async with await WorkflowEnvironment.start_time_skipping() as env:
            handle, result = await _with_workers(env, "summarize the unreliable Temporal docs", inject=False)
            assert "research 1 failed: source registry rejected the task" in result
            assert result.count("[model answer") == MAX_STEPS - 1     # every other step still ran
            parent_names = [n for run in await _run_chain(env.client, handle.id, handle.result_run_id) for n in _names(run)]
            assert parent_names.count("CHILD_WORKFLOW_EXECUTION_FAILED") == 1
            child = env.client.get_workflow_handle(f"{handle.id}/research/1")
            assert (await child.describe()).status.name == "FAILED"

    asyncio.run(go())
