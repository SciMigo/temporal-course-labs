import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from workflows import TASK_QUEUE, AgentRun, call_llm  # noqa: E402


async def main() -> None:
    client = await connect()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[AgentRun],
        activities=[call_llm],
    )
    print(f"worker pid={os.getpid()} polling {TASK_QUEUE!r}; kill -9 {os.getpid()} to crash it")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
