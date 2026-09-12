"""The GPU lane: `run_case` only, GPU_SLOTS concurrent, on `lab115-gpu-eval`. Kill it mid-run and watch the
cases resume from their heartbeat tick on the next one."""
import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities import run_case
from config import GPU_EVAL_QUEUE, GPU_SLOTS, NAMESPACE, TEMPORAL_ADDRESS


async def main() -> None:
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=NAMESPACE)
    worker = Worker(client, task_queue=GPU_EVAL_QUEUE, activities=[run_case], max_concurrent_activities=GPU_SLOTS,
                    identity=f"gpu-eval-{os.getpid()}@{GPU_EVAL_QUEUE}")
    print(f"gpu-eval worker pid={os.getpid()} polling {GPU_EVAL_QUEUE!r} with {GPU_SLOTS} slot(s)", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
