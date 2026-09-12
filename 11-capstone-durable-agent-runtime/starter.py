"""Start one AgentRun: `python starter.py [--id agent-42] [--owner acct_7f3a] [--tier batch] [--max-steps 8] [--no-wait]`."""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect, show_history  # noqa: E402
from temporalio.common import SearchAttributePair, TypedSearchAttributes  # noqa: E402

from agentrun import AGENT_WORKFLOWS, OWNER, AgentRun, workflow_id  # noqa: E402


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--goal", default="summarize the Temporal docs")
    p.add_argument("--id", default="agent-42")
    p.add_argument("--owner", default="acct_7f3a")
    p.add_argument("--tier", default="batch", choices=["enterprise", "team", "batch"])
    p.add_argument("--max-steps", type=int, default=8)
    p.add_argument("--no-wait", action="store_true")
    a = p.parse_args()
    client = await connect()
    handle = await client.start_workflow(
        AgentRun.run, a.goal, id=workflow_id(a.id), task_queue=AGENT_WORKFLOWS,
        search_attributes=TypedSearchAttributes([SearchAttributePair(OWNER, a.owner)]),
        memo={"tier": a.tier, "max_steps": a.max_steps})
    print(f"started {handle.id} run={handle.result_run_id} owner={a.owner} tier={a.tier} max_steps={a.max_steps}")
    print(f"UI: http://localhost:8233/namespaces/default/workflows/{handle.id}/{handle.result_run_id}/history")
    if a.no_wait:
        return
    print("result:", await handle.result())
    await show_history(client, handle.id)


if __name__ == "__main__":
    asyncio.run(main())
