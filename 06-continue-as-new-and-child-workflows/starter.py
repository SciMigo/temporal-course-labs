"""Start AgentRun, send one operator note mid-run, wait for the result, then print the run chain."""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect  # noqa: E402
from temporalio.api.enums.v1 import EventType  # noqa: E402
from temporalio.client import Client  # noqa: E402
from workflows import AgentRun  # noqa: E402


async def run_chain(client: Client, workflow_id: str, first_run_id: str) -> list[dict]:
    """Follow the continue-as-new chain from the first run: each run's last Event names the next Run ID."""
    chain, run_id = [], first_run_id
    while run_id:
        handle = client.get_workflow_handle(workflow_id, run_id=run_id)
        desc = await handle.describe()
        events = [ev async for ev in handle.fetch_history_events()]
        last = events[-1]
        nxt = None
        if last.HasField("workflow_execution_continued_as_new_event_attributes"):
            nxt = last.workflow_execution_continued_as_new_event_attributes.new_execution_run_id
        chain.append({"run_id": run_id, "status": desc.status.name, "events": len(events),
                      "last_event": EventType.Name(last.event_type).removeprefix("EVENT_TYPE_")})
        run_id = nxt
    return chain


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=os.environ.get("WORKFLOW_ID", "agent-42"))
    ap.add_argument("--goal", default="summarize the Temporal docs",
                    help='a goal containing "unreliable" makes the ResearchAgent child fail (6.2)')
    ap.add_argument("--inject-after", type=float, default=1.0,
                    help="seconds after start to send one inject_context Signal (0 = none)")
    args = ap.parse_args()

    client = await connect()
    handle = await client.start_workflow(AgentRun.run, args.goal, id=args.id, task_queue=TASK_QUEUE)
    first_run_id = handle.result_run_id
    print(f"started {handle.id} first run={first_run_id}; open http://localhost:8233/namespaces/default/workflows/{handle.id}")

    if args.inject_after > 0:
        await asyncio.sleep(args.inject_after)
        await handle.signal(AgentRun.inject_context, args=["Prefer sources after 2025", "cmd-7f3a"])
        status = await handle.query(AgentRun.status)
        print(f"inject_context sent; it landed in run={status['run_id']} at step {status['step']}")

    result = await handle.result()          # follows the chain: the handle is the Workflow ID, not a run
    print("result:")
    print(result)
    print(f"\nfinal status(): {await handle.query(AgentRun.status)}")

    print("\nrun chain (one Workflow ID, several Run IDs):")
    for i, run in enumerate(await run_chain(client, handle.id, first_run_id), 1):
        print(f"  run {i}: {run['run_id']}  {run['status']:17s} {run['events']:3d} events  last={run['last_event']}")


if __name__ == "__main__":
    asyncio.run(main())
