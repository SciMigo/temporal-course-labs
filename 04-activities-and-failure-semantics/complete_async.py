"""The GPU scheduler's callback, 45 minutes later — from any process that has a Client.

    python complete_async.py            # completes every token found under .state/async/
    python complete_async.py --fail     # or fails them

Nothing here knows about the Worker; the Activity is completed through the Service by task token.
"""
import argparse
import asyncio
import glob
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.exceptions import ApplicationError  # noqa: E402
from workflows import STATE_DIR, ToolResult  # noqa: E402


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fail", action="store_true")
    args = ap.parse_args()
    client = await connect()
    tokens = sorted(glob.glob(os.path.join(STATE_DIR, "async", "*.token")))
    if not tokens:
        print("no pending task tokens under", os.path.join(STATE_DIR, "async"))
    for path in tokens:
        with open(path, "rb") as f:
            token = f.read()
        handle = client.get_async_activity_handle(task_token=token)
        await handle.heartbeat("job finished")
        if args.fail:
            await handle.fail(ApplicationError("GPU job failed", type="GpuJobFailed"))
        else:
            await handle.complete(ToolResult(key="finetune", tool="finetune", output="artifact://agent-7b-ft"))
        os.remove(path)
        print(f"{'failed' if args.fail else 'completed'} the Activity behind {os.path.basename(path)}")


if __name__ == "__main__":
    asyncio.run(main())
