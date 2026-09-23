"""Replay AgentRun's code against a history, exactly as a fresh Worker would — locally,
without connecting a Worker to the Workflow's Task Queue and without running any Activity.

    python replay.py agent-42                          # fetch the live history and replay it
    python replay.py --file histories/agent-42.json    # replay a saved history
    python replay.py --file histories/agent-42.json --first 13   # 2.4: cut it, see what goes live

What to watch: the Workflow's "live:" log lines. While the SDK is replaying they are suppressed;
the first one printed is the first Command the code would issue for real. And
CALL_LLM_INVOCATIONS: the Activity function is never called during a replay.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import workflows  # noqa: E402
from common import connect  # noqa: E402
from temporalio.client import WorkflowHistory  # noqa: E402
from temporalio.worker import Replayer  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logging.getLogger("temporalio.worker").setLevel(logging.WARNING)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("workflow_id", nargs="?", default=os.environ.get("WORKFLOW_ID", "agent-42"))
    ap.add_argument("--file")
    ap.add_argument("--first", type=int, help="replay only Events 1..N (cut at a WorkflowTaskScheduled)")
    args = ap.parse_args()

    if args.file:
        with open(args.file) as f:
            history = WorkflowHistory.from_json(args.workflow_id, f.read())
    else:
        client = await connect()
        history = await client.get_workflow_handle(args.workflow_id).fetch_history()
    if args.first:
        history = WorkflowHistory(workflow_id=history.workflow_id, events=history.events[: args.first])

    print(f"replaying {len(history.events)} events of {history.workflow_id}; "
          f"CALL_LLM_INVOCATIONS before = {workflows.CALL_LLM_INVOCATIONS}")
    try:
        await Replayer(workflows=[workflows.AgentRun]).replay_workflow(history)
        print("REPLAY CLEAN: every Command the code issued matched the Event history")
    except Exception as e:  # temporalio.workflow.NondeterminismError, in lab 3
        print(f"REPLAY FAILED: {type(e).__name__}: {e}")
    print(f"CALL_LLM_INVOCATIONS after = {workflows.CALL_LLM_INVOCATIONS}")


if __name__ == "__main__":
    asyncio.run(main())
