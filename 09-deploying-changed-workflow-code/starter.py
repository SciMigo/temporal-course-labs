"""Start an AgentRun. Options for the versioning exercises:

    python starter.py                                  # id agent-42, the class's behavior (PINNED)
    python starter.py --id agent-43 --pause            # start parked at the 72 h checkpoint
    python starter.py --id agent-44 --auto-upgrade     # versioning override: AUTO_UPGRADE
    python starter.py --id agent-42 --wait             # block for the result, print the history
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect, show_history  # noqa: E402
from temporalio.common import AutoUpgradeVersioningOverride  # noqa: E402
from workflows import AgentRun  # noqa: E402

from routing import describe_versioning  # noqa: E402


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=os.environ.get("WORKFLOW_ID", "agent-42"))
    ap.add_argument("--goal", default="summarize the Temporal docs")
    ap.add_argument("--pause", action="store_true", help="deliver `pause` as the first event: parks at the checkpoint")
    ap.add_argument("--auto-upgrade", action="store_true", help="start with an AUTO_UPGRADE versioning override")
    ap.add_argument("--wait", action="store_true", help="wait for the result and print the history")
    ns = ap.parse_args()
    client = await connect()
    handle = await client.start_workflow(
        AgentRun.run, ns.goal, id=ns.id, task_queue=TASK_QUEUE,
        start_signal="pause" if ns.pause else None,
        versioning_override=AutoUpgradeVersioningOverride() if ns.auto_upgrade else None)
    print(f"started {handle.id} run={handle.result_run_id}; open http://localhost:8233")
    await asyncio.sleep(1.0)                                  # let the first Workflow Task land
    print(await describe_versioning(client, handle.id))
    if ns.wait:
        print("result:", await handle.result())
        await show_history(client, handle.id)


if __name__ == "__main__":
    asyncio.run(main())
