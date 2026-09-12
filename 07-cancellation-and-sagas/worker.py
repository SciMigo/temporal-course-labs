import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect  # noqa: E402
from temporalio import activity  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from workflows import (  # noqa: E402
    AgentRun,
    allocate_sandbox,
    call_llm,
    execute_tool,
    register_endpoint,
    release_gpu,
    release_sandbox,
    reserve_gpu,
    start_model,
    stop_model,
    unregister_endpoint,
)


def _one_line_failures(record: logging.LogRecord) -> bool:
    """register_endpoint fails on purpose, three times; keep the SDK's line, drop its traceback."""
    if record.getMessage().startswith("Completing activity as failed"):
        record.exc_info = None
    return True


async def main() -> None:
    # The Activities narrate what they do; this lab is read from the Worker's terminal as much as from history.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("temporalio").setLevel(logging.WARNING)
    logging.getLogger("temporalio.activity").setLevel(logging.INFO)     # our activity.logger lines
    logging.getLogger("temporalio.activity").addFilter(_one_line_failures)
    activity.logger.activity_info_on_message = False                    # no context dict after every line
    client = await connect()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[AgentRun],
        activities=[call_llm, execute_tool, reserve_gpu, allocate_sandbox, start_model, register_endpoint,
                    unregister_endpoint, stop_model, release_sandbox, release_gpu],
    )
    print(f"worker pid={os.getpid()} polling {TASK_QUEUE!r}; kill -9 {os.getpid()} to crash it")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
