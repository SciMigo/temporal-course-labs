"""The `agent-workflows` pool: AgentRun and ResearchAgent, Workflow code only."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

from agentrun import AGENT_WORKFLOWS, AgentRun, ResearchAgent  # noqa: E402


async def main() -> None:
    client = await connect()
    worker = Worker(client, task_queue=AGENT_WORKFLOWS, workflows=[AgentRun, ResearchAgent],
                    identity=f"wf-{os.getpid()}@{AGENT_WORKFLOWS}")
    print(f"workflow worker pid={os.getpid()} polling {AGENT_WORKFLOWS!r}: AgentRun, ResearchAgent; kill -9 {os.getpid()} to crash it")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
