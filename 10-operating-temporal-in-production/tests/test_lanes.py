"""Lab 10 self-check. Runs on the SDK's time-skipping test server (downloaded once; no Docker).
No pytest-asyncio: each test drives its coroutine on a module-scoped event loop."""
import asyncio

import pytest
from temporalio.api.enums.v1 import EventType, IndexedValueType
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest
from temporalio.client import WorkflowFailureError
from temporalio.common import SearchAttributePair, TypedSearchAttributes
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Worker

from lanes import AGENT_WORKFLOWS, CPU_TOOLS, GPU_TOOLS
from worker_gpu import execute_tool_on_gpu
from workflows import OWNER, AgentRun, ToolCall, call_llm, evaluate, execute_tool


@pytest.fixture(scope="module")
def loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def env(loop):
    async def start():
        env = await WorkflowEnvironment.start_time_skipping()
        # The test server, like the dev server, rejects an unregistered Search Attribute
        # ("search attribute Owner is not defined"); register them the way setup_search_attributes.py does.
        await env.client.operator_service.add_search_attributes(AddSearchAttributesRequest(
            namespace="default", search_attributes={
                "AgentStatus": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
                "CurrentStep": IndexedValueType.INDEXED_VALUE_TYPE_INT,
                "Owner": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD}))
        return env
    env = loop.run_until_complete(start())
    yield env
    loop.run_until_complete(env.shutdown())


def workflow_worker(env):
    return Worker(env.client, task_queue=AGENT_WORKFLOWS, workflows=[AgentRun])


def cpu_worker(env):
    return Worker(env.client, task_queue=CPU_TOOLS, activities=[call_llm, execute_tool, evaluate])


def gpu_worker(env):
    return Worker(env.client, task_queue=GPU_TOOLS, activities=[execute_tool_on_gpu], max_concurrent_activities=1)


async def start(env, wf_id: str, tier: str):
    return await env.client.start_workflow(
        AgentRun.run, "summarize the Temporal docs", id=wf_id, task_queue=AGENT_WORKFLOWS,
        search_attributes=TypedSearchAttributes([SearchAttributePair(OWNER, "acct_test")]), memo={"tier": tier})


def gpu_scheduled_events(history):
    return [ev.activity_task_scheduled_event_attributes for ev in history.events
            if ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
            and ev.activity_task_scheduled_event_attributes.task_queue.name == GPU_TOOLS]


def test_each_activity_runs_on_the_lane_the_workflow_named(loop, env):
    async def body():
        async with workflow_worker(env), cpu_worker(env), gpu_worker(env):
            handle = await start(env, "agent-42", "enterprise")
            result = await handle.result()
            assert result["lanes_used"] == [CPU_TOOLS, CPU_TOOLS, GPU_TOOLS]
            desc = await handle.describe()
            sa = {k.name: v for k, v in desc.typed_search_attributes}
            assert sa["AgentStatus"] == "done" and sa["CurrentStep"] == 3 and sa["Owner"] == "acct_test"
            history = await handle.fetch_history()
            [gpu] = gpu_scheduled_events(history)
            assert gpu.priority.priority_key == 1, "enterprise tier → priority 1 on the GPU lane"
    loop.run_until_complete(body())


def test_batch_tier_schedules_gpu_work_at_priority_5(loop, env):
    async def body():
        async with workflow_worker(env), cpu_worker(env), gpu_worker(env):
            handle = await start(env, "agent-batch", "batch")
            await handle.result()
            [gpu] = gpu_scheduled_events(await handle.fetch_history())
            assert gpu.priority.priority_key == 5
    loop.run_until_complete(body())


def test_gpu_lane_without_workers_fails_at_schedule_to_start(loop, env):
    async def body():
        async with workflow_worker(env), cpu_worker(env):  # no GPU pool
            handle = await start(env, "agent-stalled", "batch")
            with pytest.raises(WorkflowFailureError) as err:
                await handle.result()
            assert isinstance(err.value.cause, ApplicationError) and err.value.cause.type == "LaneStalled"
            history = await handle.fetch_history()
            timed_out = [ev for ev in history.events if ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT]
            assert len(timed_out) == 1, "Schedule-to-Start is not retried: one timeout event, no second attempt"
            assert timed_out[0].activity_task_timed_out_event_attributes.failure.timeout_failure_info.timeout_type == 2  # SCHEDULE_TO_START
            sa = {k.name: v for k, v in (await handle.describe()).typed_search_attributes}
            assert sa["AgentStatus"] == "stalled" and sa["CurrentStep"] == 2
    loop.run_until_complete(body())


def test_gpu_worker_refuses_a_cpu_tool(loop):
    async def body():
        with pytest.raises(ApplicationError) as err:
            await ActivityEnvironment().run(execute_tool_on_gpu, ToolCall("k", "search", {"q": "x"}))
        assert err.value.type == "BadArguments" and err.value.non_retryable
    loop.run_until_complete(body())
