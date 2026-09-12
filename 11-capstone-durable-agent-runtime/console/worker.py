"""The console lane: EvalRun Workflows + the projection Activity, on `lab115-console`."""
import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities import record_run_event
from config import CONSOLE_QUEUE, NAMESPACE, TEMPORAL_ADDRESS
from workflows import EvalRun


async def main() -> None:
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=NAMESPACE)
    worker = Worker(client, task_queue=CONSOLE_QUEUE, workflows=[EvalRun], activities=[record_run_event],
                    identity=f"console-{os.getpid()}@{CONSOLE_QUEUE}")
    print(f"console worker pid={os.getpid()} polling {CONSOLE_QUEUE!r}: EvalRun, record_run_event", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
