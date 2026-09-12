"""The `gpu-tools` pool: scarce, preemptible. One Activity slot by default (GPU_SLOTS) so the
dispatch order on the lane is observable one Task at a time (lab 10.3).

The set of Activities registered here is exactly the set that needs a GPU. Any Worker polling
`gpu-tools` must register all of them, so a CPU-only tool arriving here is a routing bug — the
guard below turns it into a non-retryable failure instead of a silently wrong answer."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio import activity  # noqa: E402
from temporalio.exceptions import ApplicationError  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

from lanes import GPU_TOOL_NAMES, GPU_TOOLS  # noqa: E402
from workflows import ToolCall, ToolResult, execute_tool  # noqa: E402

GPU_SLOTS = int(os.environ.get("GPU_SLOTS", "1"))


@activity.defn(name="execute_tool")
async def execute_tool_on_gpu(call: ToolCall) -> ToolResult:
    if call.tool not in GPU_TOOL_NAMES:
        raise ApplicationError(f"{call.tool} does not need a GPU; it was routed to {GPU_TOOLS}",
                               type="BadArguments", non_retryable=True)
    return await execute_tool(call)


async def main() -> None:
    client = await connect()
    worker = Worker(client, task_queue=GPU_TOOLS, activities=[execute_tool_on_gpu],
                    max_concurrent_activities=GPU_SLOTS, identity=f"gpu-{os.getpid()}@{GPU_TOOLS}")
    print(f"gpu worker pid={os.getpid()} polling {GPU_TOOLS!r} with {GPU_SLOTS} slot(s): execute_tool (GPU tools only)")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
