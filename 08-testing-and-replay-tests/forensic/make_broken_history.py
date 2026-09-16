"""8.5 — reproduce the broken AgentRun execution on your dev server and export its history.

    TASK_QUEUE=lab-08 python forensic/make_broken_history.py     # ~10 s; leaves the execution OPEN

Do not read past this docstring until you have written your five answers. The whole point of
the exercise is that the history alone must answer them.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from common import TASK_QUEUE, connect  # noqa: E402
from temporalio import activity  # noqa: E402
from temporalio.exceptions import ApplicationError  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from workflows import AgentRun, ToolCall, ToolResult, call_llm  # noqa: E402

WORKFLOW_ID = os.environ.get("WORKFLOW_ID", "agent-42-broken")
OUT = Path(__file__).resolve().parent.parent / "tests" / "histories" / f"forensic_{WORKFLOW_ID}.json"


@activity.defn(name="execute_tool")
async def sabotaged_execute_tool(call: ToolCall) -> ToolResult:
    attempt = activity.info().attempt
    if attempt == 1:
        raise ApplicationError("sandbox not ready", type="ToolTransientError")   # retryable
    if attempt == 2:
        activity.heartbeat(f"{call.tool}:1/3")      # got this far ...
        await asyncio.sleep(3600)                    # ... then stopped heartbeating
    raise ApplicationError("tool rejected the arguments: target must be a URL",
                           type="BadToolArguments", non_retryable=True)


async def main() -> None:
    client = await connect()
    # Two Workers, so the Workflow Worker can disappear while the Activity Worker keeps going.
    workflow_worker = Worker(client, task_queue=TASK_QUEUE, workflows=[AgentRun], identity="worker-a")
    activity_worker = Worker(client, task_queue=TASK_QUEUE, activities=[call_llm, sabotaged_execute_tool],
                             identity="worker-c")
    wf_task = asyncio.create_task(workflow_worker.run())
    act_task = asyncio.create_task(activity_worker.run())
    handle = await client.start_workflow(AgentRun.run, "summarize the Temporal docs",
                                         id=WORKFLOW_ID, task_queue=TASK_QUEUE)
    print(f"started {handle.id}")

    async def tool_scheduled() -> bool:
        async for e in handle.fetch_history_events():
            if (e.HasField("activity_task_scheduled_event_attributes")
                    and e.activity_task_scheduled_event_attributes.activity_type.name == "execute_tool"):
                return True
        return False

    while not await tool_scheduled():
        await asyncio.sleep(0.2)
    await workflow_worker.shutdown()                 # no Workflow Worker from here on
    await wf_task
    print("workflow worker gone; letting the tool fail its attempts")

    async def tool_closed() -> bool:
        async for e in handle.fetch_history_events():
            if e.HasField("activity_task_failed_event_attributes"):
                return True
        return False

    while not await tool_closed():
        await asyncio.sleep(0.5)
    await asyncio.sleep(1.0)                          # let the Service write the tail
    await activity_worker.shutdown()
    act_task.cancel()
    history = await handle.fetch_history()
    OUT.write_text(history.to_json())
    print(f"wrote {len(history.events)} events to {OUT}; {handle.id} is still open — do not start a Worker yet")


if __name__ == "__main__":
    asyncio.run(main())
