"""Start AgentRun with a fixed Workflow ID and wait for it.

    python starter.py                       # agent-42, one step, 20 s timer
    python starter.py --steps 3 --sleep 2   # the counting exercise: three steps, short timers
    python starter.py --no-wait             # return as soon as it is started
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect, show_history  # noqa: E402
from workflows import TASK_QUEUE, AgentRun  # noqa: E402


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=os.environ.get("WORKFLOW_ID", "agent-42"))
    ap.add_argument("--sleep", type=int, default=20, help="timer length in seconds (kill the Worker inside it)")
    ap.add_argument("--steps", type=int, default=1, help="how many call_llm steps before finish")
    ap.add_argument("--no-wait", action="store_true")
    args = ap.parse_args()

    client = await connect()
    handle = await client.start_workflow(
        AgentRun.run,
        args=["summarize the Temporal docs", args.sleep, args.steps],
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
