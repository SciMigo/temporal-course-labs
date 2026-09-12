"""What the Service recorded when a Worker's Workflow Task failed — and what it is still retrying.

    python failures.py agent-42
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.api.enums.v1 import EventType, TimeoutType, WorkflowTaskFailedCause  # noqa: E402


async def main() -> None:
    wf_id = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("WORKFLOW_ID", "agent-42")
    client = await connect()
    handle = client.get_workflow_handle(wf_id)
    async for ev in handle.fetch_history_events():
        name = EventType.Name(ev.event_type).removeprefix("EVENT_TYPE_")
        if ev.HasField("workflow_task_failed_event_attributes"):
            a = ev.workflow_task_failed_event_attributes
            cause = WorkflowTaskFailedCause.Name(a.cause).removeprefix("WORKFLOW_TASK_FAILED_CAUSE_")
            print(f"{ev.event_id:3d}  {name}  cause={cause}  worker={a.identity}\n     {a.failure.message}")
        elif ev.HasField("workflow_task_timed_out_event_attributes"):
            a = ev.workflow_task_timed_out_event_attributes
            print(f"{ev.event_id:3d}  {name}  timeout_type={TimeoutType.Name(a.timeout_type).removeprefix('TIMEOUT_TYPE_')}")
        elif ev.HasField("workflow_task_scheduled_event_attributes"):
            a = ev.workflow_task_scheduled_event_attributes
            if a.attempt > 1:
                print(f"{ev.event_id:3d}  {name}  attempt={a.attempt}  (earlier attempts failed without an Event)")
        elif name in ("WORKFLOW_EXECUTION_COMPLETED", "WORKFLOW_EXECUTION_FAILED", "WORKFLOW_EXECUTION_TERMINATED"):
            print(f"{ev.event_id:3d}  {name}")
    desc = await handle.describe()
    raw = desc.raw_description
    print(f"status: {desc.status.name}")
    if raw.HasField("pending_workflow_task"):
        t = raw.pending_workflow_task
        print(f"pending Workflow Task: attempt={t.attempt} state={t.state}")


if __name__ == "__main__":
    asyncio.run(main())
