"""Drive a running AgentRun from outside.

    python control.py status  [--id agent-42]
    python control.py pause | resume | cancel-run          (Signals)
    python control.py cancel                                (Temporal cancellation: handle.cancel())
    python control.py inject "some text" --command-id c1    (Signal with dedupe)
    python control.py goal "new goal" --command-id c2       (Update with validator; repeat it and watch it be ignored)
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402

from agentrun import AgentRun, workflow_id  # noqa: E402


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["status", "pause", "resume", "cancel-run", "cancel", "inject", "goal"])
    p.add_argument("text", nargs="?", default="")
    p.add_argument("--id", default="agent-42")
    p.add_argument("--command-id", default="cmd-1")
    a = p.parse_args()
    handle = (await connect()).get_workflow_handle(workflow_id(a.id))
    if a.command == "status":
        for k, v in (await handle.query(AgentRun.status)).items():
            print(f"  {k:22s} {v}")
    elif a.command == "pause":
        await handle.signal(AgentRun.pause); print("pause signalled")
    elif a.command == "resume":
        await handle.signal(AgentRun.resume); print("resume signalled")
    elif a.command == "cancel-run":
        await handle.signal(AgentRun.cancel_run); print("cancel_run signalled (cooperative)")
    elif a.command == "cancel":
        await handle.cancel(); print("cancellation requested (WorkflowExecutionCancelRequested)")
    elif a.command == "inject":
        await handle.signal(AgentRun.inject_context, args=[a.text, a.command_id]); print("inject_context signalled")
    elif a.command == "goal":
        print("update result:", await handle.execute_update(AgentRun.change_goal, args=[a.text, a.command_id]))


if __name__ == "__main__":
    asyncio.run(main())
