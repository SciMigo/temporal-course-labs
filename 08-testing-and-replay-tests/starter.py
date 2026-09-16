import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect, show_history  # noqa: E402
from workflows import AgentRun  # noqa: E402

# Override when several people share one dev server: WORKFLOW_ID=lab08-agent-42 python starter.py
WORKFLOW_ID = os.environ.get("WORKFLOW_ID", "agent-42")


async def main() -> None:
    client = await connect()
    handle = await client.start_workflow(
        AgentRun.run, "summarize the Temporal docs", id=WORKFLOW_ID, task_queue=TASK_QUEUE
    )
    print(f"started {handle.id} run={handle.result_run_id}; open http://localhost:8233")
    print("result:", await handle.result())
    await show_history(client, handle.id)


if __name__ == "__main__":
    asyncio.run(main())
