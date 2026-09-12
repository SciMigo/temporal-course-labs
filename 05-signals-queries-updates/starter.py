"""Start AgentRun and wait for its result. Talk to it from another terminal with control.py."""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect  # noqa: E402
from workflows import AgentRun  # noqa: E402


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=os.environ.get("WORKFLOW_ID", "agent-42"), help="Workflow ID (= agent ID)")
    ap.add_argument("--goal", default="summarize the Temporal docs")
    ap.add_argument("--no-wait", action="store_true", help="start and return; do not wait for the result")
    args = ap.parse_args()

    client = await connect()
    handle = await client.start_workflow(AgentRun.run, args.goal, id=args.id, task_queue=TASK_QUEUE)
    print(f"started {handle.id} run={handle.result_run_id}; open http://localhost:8233/namespaces/default/workflows/{handle.id}")
    if args.no_wait:
        return
    print("result:")
    print(await handle.result())


if __name__ == "__main__":
    asyncio.run(main())
