"""The `agent-workflows` pool: Workflow code only. Deliberately boring, so you can run many of
these and redeploy them twice a day without touching a GPU."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

from lanes import AGENT_WORKFLOWS  # noqa: E402
from workflows import AgentRun  # noqa: E402


async def main() -> None:
    client = await connect()
    worker = Worker(client, task_queue=AGENT_WORKFLOWS, workflows=[AgentRun],
                    identity=f"wf-{os.getpid()}@{AGENT_WORKFLOWS}")
    print(f"workflow worker pid={os.getpid()} polling {AGENT_WORKFLOWS!r} (Workflow Tasks only)")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
