"""Start AgentRun in one of the lab's scenarios and print what the Workflow made of the outcome.

    python starter.py                          # default: call_llm, echo tool with the specified options, finish
    python starter.py schedule_to_start        # 4.1  (also: start_to_close, schedule_to_close, heartbeat)
    python starter.py shards                   # 4.2
    python starter.py charge | send            # 4.3
    python starter.py finetune                 # 4.5
    python starter.py heartbeat --id agent-42-hb --no-wait

The Workflow ID defaults to agent-42-<scenario>. The Workflow catches the ActivityError, so the
*result* names the exception class and timeout type; the history has the Event.
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from workflows import SCENARIOS, TASK_QUEUE, AgentRun  # noqa: E402


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("scenario", nargs="?", default="default", choices=sorted(SCENARIOS))
    ap.add_argument("--id", default=None)
    ap.add_argument("--no-wait", action="store_true")
    args = ap.parse_args()
    wf_id = args.id or f"{os.environ.get('WORKFLOW_ID_PREFIX', 'agent-42')}-{args.scenario}"

    client = await connect()
    handle = await client.start_workflow(
        AgentRun.run, args=["summarize the Temporal docs", args.scenario], id=wf_id, task_queue=TASK_QUEUE
    )
    print(f"started {handle.id} scenario={args.scenario}; open http://localhost:8233/namespaces/default/workflows/{handle.id}")
    if args.no_wait:
        return
    print("result:", await handle.result())
    print(f"history: python history.py {handle.id}")


if __name__ == "__main__":
    asyncio.run(main())
