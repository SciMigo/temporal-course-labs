"""The `cpu-tools` pool: call_llm, execute_tool (CPU tools), evaluate, compensate."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

from agentrun import CPU_TOOLS, call_llm, compensate, evaluate, execute_tool  # noqa: E402


async def main() -> None:
    client = await connect()
    worker = Worker(client, task_queue=CPU_TOOLS, activities=[call_llm, execute_tool, evaluate, compensate],
                    identity=f"cpu-{os.getpid()}@{CPU_TOOLS}")
    print(f"cpu worker pid={os.getpid()} polling {CPU_TOOLS!r}: call_llm, execute_tool, evaluate, compensate")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
