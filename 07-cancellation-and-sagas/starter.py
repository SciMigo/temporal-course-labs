"""Start AgentRun with one tool call, optionally cancel it, wait, then print the annotated history.

    python starter.py                                 # 7.1 / 7.2: goal "deploy ..." -> the GPU saga; register_endpoint fails
    python starter.py --tool crunch --cancel-after 4  # 7.3: a 20 s tool with no heartbeat; cancel while it runs
    python starter.py --tool crunch --cancel-after 4 --heartbeat
    python starter.py --tool crunch --cancel-after 4 --heartbeat --shield
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect  # noqa: E402
from temporalio.api.enums.v1 import EventType  # noqa: E402
from temporalio.client import Client, WorkflowFailureError  # noqa: E402
from workflows import AgentRun, ToolOptions  # noqa: E402


async def history(client: Client, workflow_id: str) -> None:
    handle = client.get_workflow_handle(workflow_id)
    async for ev in handle.fetch_history_events():
        name = EventType.Name(ev.event_type).removeprefix("EVENT_TYPE_")
        detail = ""
        if ev.HasField("activity_task_scheduled_event_attributes"):
            detail = ev.activity_task_scheduled_event_attributes.activity_type.name
        elif ev.HasField("activity_task_started_event_attributes"):
            detail = f"attempt={ev.activity_task_started_event_attributes.attempt}"
        elif ev.HasField("activity_task_failed_event_attributes"):
            detail = ev.activity_task_failed_event_attributes.failure.message
        elif ev.HasField("activity_task_timed_out_event_attributes"):
            detail = ev.activity_task_timed_out_event_attributes.failure.message
        elif ev.HasField("workflow_task_started_event_attributes"):
            detail = f"identity={ev.workflow_task_started_event_attributes.identity}"
        elif ev.HasField("workflow_execution_failed_event_attributes"):
            detail = ev.workflow_execution_failed_event_attributes.failure.message
        which = ev.WhichOneof("attributes")
        if which in ("activity_task_completed_event_attributes", "activity_task_canceled_event_attributes",
                     "activity_task_failed_event_attributes", "activity_task_timed_out_event_attributes",
                     "activity_task_cancel_requested_event_attributes"):
            detail = f"{detail} (scheduled by event {getattr(ev, which).scheduled_event_id})".strip()
        print(f"{ev.event_id:4d}  {name:38s} {detail}")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=os.environ.get("WORKFLOW_ID", "agent-42"))
    ap.add_argument("--tool", choices=["deploy_model", "crunch"], default="deploy_model")
    ap.add_argument("--heartbeat", action="store_true", help="execute_tool heartbeats every chunk")
    ap.add_argument("--shield", action="store_true", help="AgentRun shields the in-flight execute_tool")
    ap.add_argument("--chunks", type=int, default=20, help="length of the crunch tool in seconds")
    ap.add_argument("--cancel-after", type=float, default=0, help="seconds after start to call handle.cancel()")
    ap.add_argument("--no-wait", action="store_true")
    args = ap.parse_args()

    goal = "deploy the fine-tuned model" if args.tool == "deploy_model" else "crunch the eval set"
    options = ToolOptions(heartbeat=args.heartbeat, shield=args.shield, chunks=args.chunks)

    client = await connect()
    handle = await client.start_workflow(AgentRun.run, args=[goal, options], id=args.id, task_queue=TASK_QUEUE)
    print(f"started {handle.id} run={handle.result_run_id} tool={args.tool} options={options}")
    print(f"open http://localhost:8233/namespaces/default/workflows/{handle.id}")
    if args.no_wait:
        return

    if args.cancel_after > 0:
        await asyncio.sleep(args.cancel_after)
        print(f"status before cancel: {await handle.query(AgentRun.status)}")
        await handle.cancel()
        print("handle.cancel() sent — a WorkflowExecutionCancelRequested Event; the Workflow sees it at its next await")

    try:
        print("result:", await handle.result())
    except WorkflowFailureError as e:
        print(f"execution closed with {type(e.cause).__name__}: {e.cause}")
    desc = await handle.describe()
    print(f"close status: {desc.status.name}")
    print(f"final status(): {await handle.query(AgentRun.status)}")
    print("\nhistory:")
    await history(client, handle.id)


if __name__ == "__main__":
    asyncio.run(main())
