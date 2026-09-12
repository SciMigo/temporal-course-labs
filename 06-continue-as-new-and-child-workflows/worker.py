"""Worker for lab 6.

    python worker.py                   # AgentRun + ResearchAgent, both queues (one process)
    python worker.py --role agent      # AgentRun and call_llm only, on TASK_QUEUE
    python worker.py --role research   # ResearchAgent and fetch_source only, on RESEARCH_TASK_QUEUE

6.2 wants the child on a Worker you can kill without touching the parent: run one of each role.
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE, connect  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from workflows import RESEARCH_TASK_QUEUE, AgentRun, ResearchAgent, call_llm, fetch_source  # noqa: E402


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", choices=["both", "agent", "research"], default="both")
    args = ap.parse_args()

    client = await connect()
    workers = []
    if args.role in ("both", "agent"):
        workers.append(Worker(client, task_queue=TASK_QUEUE, workflows=[AgentRun], activities=[call_llm]))
    if args.role in ("both", "research"):
        workers.append(Worker(client, task_queue=RESEARCH_TASK_QUEUE, workflows=[ResearchAgent], activities=[fetch_source]))
    queues = [w.task_queue for w in workers]
    print(f"worker pid={os.getpid()} role={args.role} polling {queues}; kill -9 {os.getpid()} to crash it")
    await asyncio.gather(*(w.run() for w in workers))


if __name__ == "__main__":
    asyncio.run(main())
