import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from workflows import IDEMPOTENT, STATE_DIR, AgentRun, call_llm, charge, execute_tool  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


async def main() -> None:
    client = await connect()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[AgentRun],
        activities=[call_llm, execute_tool, charge],
    )
    print(
        f"worker pid={os.getpid()} polling {TASK_QUEUE!r} state={STATE_DIR} "
        f"idempotent={'on' if IDEMPOTENT else 'OFF'}; kill -9 {os.getpid()} to crash it",
        flush=True,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
