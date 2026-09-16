"""Start AgentRun with a fixed Workflow ID.

    python starter.py                           # agent-42: 15 s timer, 5 s time budget, up to 5 steps
    python starter.py --id coinflip-1 --no-wait # return immediately; kill the Worker during the timer
    python starter.py --budget 300              # 3.3: give the model-driven run room for two steps
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect, show_history  # noqa: E402
from workflows import AgentRun  # noqa: E402


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=os.environ.get("WORKFLOW_ID", "agent-42"))
    ap.add_argument("--sleep", type=int, default=15, help="timer after each step (kill the Worker inside it)")
    ap.add_argument("--budget", type=int, default=5, help="plan() finishes once the run is older than this")
    ap.add_argument("--steps", type=int, default=5, help="hard cap on steps")
    ap.add_argument("--no-wait", action="store_true")
    args = ap.parse_args()

    client = await connect()
    handle = await client.start_workflow(
        AgentRun.run,
        args=["summarize the Temporal docs", args.sleep, args.budget, args.steps],
        id=args.id,
        task_queue=TASK_QUEUE,
    )
    print(f"started {handle.id} run={handle.result_run_id}; open http://localhost:8233/namespaces/default/workflows/{handle.id}")
    if args.no_wait:
        return
    print("result:", await handle.result())
    await show_history(client, handle.id)


if __name__ == "__main__":
    asyncio.run(main())
