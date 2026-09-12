"""Start one AgentRun. `--tier enterprise` puts its GPU step at priority 1; `--tier batch` at 5.
`Owner` is a Search Attribute from the first event; tier rides in memo."""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect, show_history  # noqa: E402
from temporalio.common import SearchAttributePair, TypedSearchAttributes  # noqa: E402

from lanes import AGENT_WORKFLOWS, workflow_id  # noqa: E402
from workflows import OWNER, AgentRun  # noqa: E402


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--goal", default="summarize the Temporal docs")
    p.add_argument("--id", default="agent-42", help="Workflow ID = agent id (LAB_PREFIX is prepended)")
    p.add_argument("--owner", default="acct_7f3a", help="opaque account id; never an email (Search Attributes are unencrypted)")
    p.add_argument("--tier", default="batch", choices=["enterprise", "team", "batch"])
    p.add_argument("--no-wait", action="store_true", help="start and return; do not wait for the result")
    a = p.parse_args()

    client = await connect()
    handle = await client.start_workflow(
        AgentRun.run, a.goal, id=workflow_id(a.id), task_queue=AGENT_WORKFLOWS,
        search_attributes=TypedSearchAttributes([SearchAttributePair(OWNER, a.owner)]),
        memo={"tier": a.tier},
    )
    print(f"started {handle.id} run={handle.result_run_id} owner={a.owner} tier={a.tier}")
    print(f"UI: http://localhost:8233/namespaces/default/workflows/{handle.id}/{handle.result_run_id}/history")
    if a.no_wait:
        return
    print("result:", await handle.result())
    await show_history(client, handle.id)


if __name__ == "__main__":
    asyncio.run(main())
