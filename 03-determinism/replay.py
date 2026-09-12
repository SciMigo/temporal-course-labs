"""Force a replay without killing anything: run the Worker's current code against a history.

    python replay.py agent-42              # replay 5 times, stop at the first mismatch
    python replay.py agent-42 --times 20   # the coin flip: count how many replays pass by luck

The code that replays is whatever LAB03_BREAK selects in *this* process — so
`LAB03_BREAK=clock python replay.py x` replays the broken build and
`python replay.py x` replays the fixed build against the same history.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import workflows  # noqa: E402
from common import connect  # noqa: E402
from temporalio.worker import Replayer  # noqa: E402

logging.basicConfig(level=logging.WARNING)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("workflow_id", nargs="?", default=os.environ.get("WORKFLOW_ID", "agent-42"))
    ap.add_argument("--times", type=int, default=5)
    args = ap.parse_args()

    client = await connect()
    history = await client.get_workflow_handle(args.workflow_id).fetch_history()
    print(f"code: LAB03_BREAK={workflows.BREAK!r}; history: {len(history.events)} events of {args.workflow_id}")
    clean = 0
    for i in range(1, args.times + 1):
        try:
            await Replayer(workflows=[workflows.AgentRun]).replay_workflow(history)
            clean += 1
            print(f"replay {i}: clean")
        except Exception as e:
            msg = str(e)
            m = re.search(r'message: "([^"]*)"', msg)
            print(f"replay {i}: {type(e).__name__}")
            print(f"  {m.group(1) if m else msg}")
            ev = re.search(r"HistoryEvent\(id: (\d+), (\w+)\)", msg)
            if ev:
                print(f"  -> the code's Command did not match Event {ev.group(1)} ({ev.group(2)}). "
                      f"Open that Event in the UI and find the `await` that produced the Command.")
            break
    else:
        print(f"{clean}/{args.times} replays clean")


if __name__ == "__main__":
    asyncio.run(main())
