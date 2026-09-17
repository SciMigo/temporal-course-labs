import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect, show_history  # noqa: E402
from workflows import AgentRun  # noqa: E402
from temporalio.exceptions import WorkflowAlreadyStartedError


async def main() -> None:
    client = await connect()
    try:
        handle = await client.start_workflow(
            AgentRun.run, "summarize the Temporal docs", id="agent-42", task_queue=TASK_QUEUE
        )
    except WorkflowAlreadyStartedError:
        print(
            "agent-42 is already running. After crashing the Worker, run the Worker again "
            "to resume this Workflow; do not start agent-42 a second time.",
            file=sys.stderr,
        )
        raise SystemExit(2) from None
    print(f"started {handle.id} on {TASK_QUEUE!r} run={handle.result_run_id}; open http://localhost:8233")
    print("result:", await handle.result())
    await show_history(client, handle.id)


if __name__ == "__main__":
    asyncio.run(main())
