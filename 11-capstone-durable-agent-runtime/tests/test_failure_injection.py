"""Lab 11.3 — the failure-injection suite: crash, timeout, duplicate, cancel (both kinds), plus the
control surface and the continue-as-new bound. Time-skipping test server; no Docker; no pytest-asyncio
(a module-scoped event loop drives each test's coroutine)."""
import asyncio
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.api.enums.v1 import EventType, IndexedValueType
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest
from temporalio.client import WorkflowFailureError, WorkflowUpdateFailedError
from temporalio.common import SearchAttributePair, TypedSearchAttributes
from temporalio.exceptions import CancelledError
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agentrun import (AGENT_WORKFLOWS, CPU_TOOLS, GPU_TOOLS, OWNER, AgentRun, ResearchAgent, ToolCall,
                      ToolResult, call_llm, compensate, evaluate, execute_tool)
from agentrun import activities as ledgers
from run_200_steps import walk_runs
from worker_gpu import execute_tool_on_gpu


# ------------------------------------------------------------------ harness
@pytest.fixture(scope="module")
def loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def env(loop):
    async def start():
        env = await WorkflowEnvironment.start_time_skipping()
        await env.client.operator_service.add_search_attributes(AddSearchAttributesRequest(
            namespace="default", search_attributes={
                "AgentStatus": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
                "CurrentStep": IndexedValueType.INDEXED_VALUE_TYPE_INT,
                "Owner": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD}))
        return env
    env = loop.run_until_complete(start())
    yield env
    loop.run_until_complete(env.shutdown())


@pytest.fixture(autouse=True)
def fresh_ledgers():
    ledgers.reset_ledgers()


def pools(env, *, cpu=None, gpu=None, identity="A", sticky=True):
    """The three Worker pools, as one list of context managers. `sticky=False` disables the Workflow
    cache: every Workflow Task then goes to the shared queue and is a full replay. The crash test needs
    it because the time-skipping test server never times out a dead Worker's sticky queue (observed:
    the Workflow Task after the crash sat there for 30 s+); a real server moves it after ~10 s."""
    return [
        Worker(env.client, task_queue=AGENT_WORKFLOWS, workflows=[AgentRun, ResearchAgent], identity=f"wf-{identity}",
               max_cached_workflows=0 if not sticky else 1000),
        Worker(env.client, task_queue=CPU_TOOLS, activities=cpu or [call_llm, execute_tool, evaluate, compensate], identity=f"cpu-{identity}"),
        Worker(env.client, task_queue=GPU_TOOLS, activities=gpu or [execute_tool_on_gpu], identity=f"gpu-{identity}"),
    ]


async def start(env, wf_id: str, max_steps: int, tier: str = "team"):
    return await env.client.start_workflow(
        AgentRun.run, "summarize the Temporal docs", id=wf_id, task_queue=AGENT_WORKFLOWS,
        search_attributes=TypedSearchAttributes([SearchAttributePair(OWNER, "acct_test")]),
        memo={"tier": tier, "max_steps": max_steps})


async def wait_for(handle, pred, timeout=20.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        s = await handle.query(AgentRun.status)
        if pred(s):
            return s
        assert asyncio.get_event_loop().time() < deadline, f"timed out waiting; last status {s}"
        await asyncio.sleep(0.05)


async def search_attrs(handle):
    d = await handle.describe()
    return {k.name: v for k, v in d.typed_search_attributes if k.name in ("AgentStatus", "CurrentStep", "Owner")}


def events(history, kind):
    return [ev for ev in history.events if ev.event_type == getattr(EventType, f"EVENT_TYPE_{kind}")]


# ------------------------------------------------------------------ 1. worker crash
def test_worker_crash_mid_run_loses_no_work_and_spends_nothing_twice(loop, env):
    """Every pool dies while a tool is running. New pools with new identities finish the run.
    Nothing already recorded is re-executed; the interrupted attempt is retried from its heartbeat."""
    async def body():
        pool_a = pools(env, identity="A", sticky=False)
        tasks = [asyncio.ensure_future(w.run()) for w in pool_a]
        handle = await start(env, "crash-42", max_steps=6)
        await wait_for(handle, lambda s: s["step"] >= 2)           # step 2 = the GPU tool, in flight
        for t in tasks:                                            # the crash: no graceful drain
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # With no Worker alive, a Query cannot be answered (a Worker computes it from replayed state) …
        with pytest.raises(RPCError):
            await handle.query(AgentRun.status, rpc_timeout=timedelta(seconds=2))
        # … but Describe can: the Service owns the execution's state, and it is still RUNNING.
        mid = await handle.describe()
        assert mid.status.name == "RUNNING"
        wf, cpu, gpu = pools(env, identity="B")
        async with wf, cpu, gpu:
            result = await handle.result()
        assert result["status"] == "done" and result["steps"] == 6
        # One bill per key: no LLM key was charged twice, no tool effect happened twice.
        assert len(ledgers.LLM_CALLS) == len(set(ledgers.LLM_CALLS))
        assert all(n == 1 for n in ledgers.TOOL_SIDE_EFFECTS.values()), dict(ledgers.TOOL_SIDE_EFFECTS)
        history = await handle.fetch_history()
        identities = {ev.workflow_task_started_event_attributes.identity for ev in events(history, "WORKFLOW_TASK_STARTED")}
        assert {"wf-A", "wf-B"} <= identities, identities            # two processes ran one execution
        assert (await search_attrs(handle)) == {"AgentStatus": "done", "CurrentStep": 6, "Owner": "acct_test"}
    loop.run_until_complete(body())


# ------------------------------------------------------------------ 2. activity timeout
def test_stalled_attempt_times_out_on_heartbeat_and_the_retry_resumes(loop, env):
    """Attempt 1 heartbeats twice then hangs. The heartbeat timeout fails it; attempt 2 starts from
    the last heartbeat's tick. One effect, one retry, no timeout *event* — retries are not history."""
    @activity.defn(name="execute_tool")
    async def hangs_once(call: ToolCall) -> ToolResult:
        if activity.info().attempt == 1:
            activity.heartbeat({"tick": 2, "of": 4})
            await asyncio.sleep(30)     # never heartbeats again → heartbeat timeout (1 s in tests)
        return await execute_tool(call)

    async def body():
        wf, cpu, gpu = pools(env, cpu=[call_llm, hangs_once, evaluate, compensate])
        async with wf, cpu, gpu:
            handle = await start(env, "timeout-42", max_steps=2)   # llm, then the `search` tool
            result = await handle.result()
        assert result["steps"] == 2 and result["attempts"] == 1, result
        assert list(ledgers.TOOL_SIDE_EFFECTS.values()) == [1]
        history = await handle.fetch_history()
        assert events(history, "ACTIVITY_TASK_TIMED_OUT") == [], "a retried timeout leaves no event; only the final failure would"
        assert len(events(history, "ACTIVITY_TASK_COMPLETED")) == 3   # call_llm, execute_tool (attempt 2), evaluate
    loop.run_until_complete(body())


# ------------------------------------------------------------------ 3. duplicate execution
def test_completion_lost_after_the_effect_is_not_a_second_effect(loop, env):
    """Attempt 1 does the work, writes the ledger, then dies before its completion reaches the
    Service. Attempt 2 presents the same key and gets the row back: duplicate=True, one effect."""
    @activity.defn(name="execute_tool")
    async def loses_completion_once(call: ToolCall) -> ToolResult:
        result = await execute_tool(call)
        if activity.info().attempt == 1:
            raise RuntimeError("worker died after the effect, before reporting")
        return result

    async def body():
        wf, cpu, gpu = pools(env, cpu=[call_llm, loses_completion_once, evaluate, compensate])
        async with wf, cpu, gpu:
            handle = await start(env, "dup-42", max_steps=2)
            result = await handle.result()
        assert result["attempts"] == 1 and result["duplicates_suppressed"] == 1, result
        assert list(ledgers.TOOL_SIDE_EFFECTS.values()) == [1]
        history = await handle.fetch_history()
        assert len(events(history, "ACTIVITY_TASK_FAILED")) == 0, "a retried failure is not a history event either"
    loop.run_until_complete(body())


# ------------------------------------------------------------------ 4. cooperative cancel (Signal)
def test_cancel_run_signal_finishes_the_step_then_stops_cleanly(loop, env):
    async def body():
        wf, cpu, gpu = pools(env)
        async with wf, cpu, gpu:
            handle = await start(env, "cancelrun-42", max_steps=8)
            await wait_for(handle, lambda s: s["step"] >= 1)
            await handle.signal(AgentRun.cancel_run)
            result = await handle.result()                         # completes, does not fail
        assert result["status"] == "cancelled" and 1 <= result["steps"] < 8
        assert ledgers.COMPENSATED == [], "nothing was held at a decision point"
        assert (await search_attrs(handle))["AgentStatus"] == "cancelled"
    loop.run_until_complete(body())


# ------------------------------------------------------------------ 5. Temporal cancel (handle.cancel)
def test_temporal_cancel_during_the_gpu_step_runs_the_compensation(loop, env):
    """Cancel while the GPU tool is in flight. The Activity is asked to stop; the Workflow sees the
    cancellation, undoes what it holds (release_gpu), publishes `cancelled`, and closes as CANCELED."""
    @activity.defn(name="execute_tool")
    async def slow_gpu_tool(call: ToolCall) -> ToolResult:
        for tick in range(200):                                    # long, but heartbeating → cancellable
            activity.heartbeat({"tick": tick})
            await asyncio.sleep(0.1)
        return await execute_tool(call)

    async def body():
        wf, cpu, gpu = pools(env, gpu=[slow_gpu_tool])
        async with wf, cpu, gpu:
            handle = await start(env, "cancel-42", max_steps=8)
            held = await wait_for(handle, lambda s: s["compensations"])
            await handle.cancel()
            with pytest.raises(WorkflowFailureError) as err:
                await handle.result()
            assert isinstance(err.value.cause, CancelledError)
        assert ledgers.COMPENSATED == held["compensations"] == [f"release_gpu:{held['compensations'][0].split(':', 1)[1]}"]
        desc = await handle.describe()
        assert desc.status.name == "CANCELED"
        assert (await search_attrs(handle))["AgentStatus"] == "cancelled"
        history = await handle.fetch_history()
        scheduled = [ev.activity_task_scheduled_event_attributes.activity_type.name for ev in events(history, "ACTIVITY_TASK_SCHEDULED")]
        assert scheduled[-1] == "compensate", scheduled
        assert len(events(history, "WORKFLOW_EXECUTION_CANCEL_REQUESTED")) == 1
    loop.run_until_complete(body())


# ------------------------------------------------------------------ 6. control surface
def test_pause_update_validator_and_command_dedupe(loop, env):
    async def body():
        wf, cpu, gpu = pools(env)
        async with wf, cpu, gpu:
            handle = await start(env, "control-42", max_steps=8)
            await handle.signal(AgentRun.pause)
            await wait_for(handle, lambda s: s["paused"] and s["status"] == "paused")
            assert (await search_attrs(handle))["AgentStatus"] == "paused"
            with pytest.raises(WorkflowUpdateFailedError):          # rejected by the validator: never in history
                await handle.execute_update(AgentRun.change_goal, args=["", "c0"])
            assert (await handle.execute_update(AgentRun.change_goal, args=["write the defense", "c1"])).startswith("goal changed")
            assert (await handle.execute_update(AgentRun.change_goal, args=["write the defense", "c1"])).startswith("ignored")
            await handle.signal(AgentRun.inject_context, args=["hurry", "c2"])
            await handle.signal(AgentRun.inject_context, args=["hurry", "c2"])
            s = await wait_for(handle, lambda s: "c2" in s["processed_command_ids"])
            assert s["processed_command_ids"] == ["c1", "c2"] and s["goal"] == "write the defense"
            await handle.signal(AgentRun.resume)
            result = await handle.result()
        assert result["status"] == "done" and result["steps"] == 8
        assert result["context_tail"].count("[injected: hurry]") <= 1
        history = await handle.fetch_history()
        assert len(events(history, "WORKFLOW_EXECUTION_UPDATE_ACCEPTED")) == 2   # c1 twice; the rejected c0 is absent
        assert len(events(history, "WORKFLOW_EXECUTION_SIGNALED")) == 4           # pause, inject, inject, resume
    loop.run_until_complete(body())


# ------------------------------------------------------------------ 7. continue-as-new bound
def test_continue_as_new_keeps_every_run_under_the_bound(loop, env):
    async def body():
        wf, cpu, gpu = pools(env)
        async with wf, cpu, gpu:
            handle = await start(env, "can-42", max_steps=20)        # STEPS_PER_RUN=8 → runs of 8, 8, 4
            result = await handle.result()
        assert result["steps"] == 20
        runs = await walk_runs(env.client, handle.id, handle.result_run_id)
        assert [r["closed_as"] for r in runs] == ["CONTINUED_AS_NEW", "CONTINUED_AS_NEW", "COMPLETED"]
        assert max(r["events"] for r in runs) <= 8 * 14, runs
        assert len({r["run_id"] for r in runs}) == 3
        assert (await search_attrs(handle)) == {"AgentStatus": "done", "CurrentStep": 20, "Owner": "acct_test"}
        assert await (await handle.describe()).memo_value("tier") == "team"
    loop.run_until_complete(body())
