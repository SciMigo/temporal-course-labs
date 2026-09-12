"""A Worker to kill. 8.4 starts it as a subprocess against the test server and SIGKILLs it
mid-Activity; the Service must then hand the Activity attempt to the next Worker."""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from temporalio.client import Client  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

import fakes  # noqa: E402
from workflows import AgentRun  # noqa: E402


async def main() -> None:
    client = await Client.connect(os.environ["TEMPORAL_ADDRESS"], namespace=os.environ["TEMPORAL_NAMESPACE"])
    worker = Worker(client, task_queue=os.environ["TASK_QUEUE"], workflows=[AgentRun],
                    activities=[fakes.fake_call_llm, fakes.slow_execute_tool], identity=f"crashable-{os.getpid()}",
                    max_cached_workflows=int(os.environ.get("MAX_CACHED_WORKFLOWS", "1000")))
    print("ready", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
