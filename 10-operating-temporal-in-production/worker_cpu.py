"""The `cpu-tools` pool: cheap, autoscaled. Registers exactly the Activities that need no GPU."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

from lanes import CPU_TOOLS  # noqa: E402
from workflows import call_llm, evaluate, execute_tool  # noqa: E402


async def main() -> None:
    client = await connect()
    worker = Worker(client, task_queue=CPU_TOOLS, activities=[call_llm, execute_tool, evaluate],
                    identity=f"cpu-{os.getpid()}@{CPU_TOOLS}")
    print(f"cpu worker pid={os.getpid()} polling {CPU_TOOLS!r}: call_llm, execute_tool, evaluate")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
