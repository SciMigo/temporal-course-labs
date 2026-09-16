import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from workflows import BREAK, MODEL_URL, AgentRun, call_llm  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logging.getLogger("temporalio.worker._workflow_instance").setLevel(logging.ERROR)  # the traceback is in history


async def main() -> None:
    client = await connect()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[AgentRun],
        activities=[call_llm],
    )
    print(
        f"worker pid={os.getpid()} polling {TASK_QUEUE!r} with LAB03_BREAK={BREAK!r}"
        f"{' model=' + MODEL_URL if MODEL_URL else ''}; kill -9 {os.getpid()} to crash it",
        flush=True,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
