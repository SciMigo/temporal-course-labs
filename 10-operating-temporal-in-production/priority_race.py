"""Lab 10.3 — does the lane honour Priority / Fairness? Build a backlog on `gpu-tools` with no GPU
Worker, then bring up ONE single-slot GPU Worker and read the dispatch order off the histories.

    python priority_race.py                     # N batch runs (priority 5) queued first, then 1 enterprise run (priority 1)
    python priority_race.py --mode fairness     # 2 tenants, same priority, enqueued a a a a b b b b

Everything runs in this process (workflow + cpu Workers first, gpu Worker last) so the order in
which Tasks enter the backlog is exactly the order printed. Prediction (upstream rules, as of
2026-09): priority mode → the enterprise run's GPU Task starts before every batch Task that was
queued ahead of it; fairness mode → a b a b … if `matching.enableFairness` is on, a a a a b b b b
(FIFO) if it is not. Whatever you see, write it down with the server version.
"""
import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.api.enums.v1 import EventType  # noqa: E402
from temporalio.client import Client, WorkflowHandle  # noqa: E402
from temporalio.common import SearchAttributePair, TypedSearchAttributes  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

from lanes import AGENT_WORKFLOWS, CPU_TOOLS, GPU_TOOLS, workflow_id  # noqa: E402
from five_questions import q5_lane  # noqa: E402
from worker_gpu import execute_tool_on_gpu  # noqa: E402
from workflows import OWNER, AgentRun, call_llm, evaluate, execute_tool  # noqa: E402


async def start(client: Client, base: str, owner: str, tier: str) -> WorkflowHandle:
    return await client.start_workflow(
        AgentRun.run, f"race {base}", id=workflow_id(base), task_queue=AGENT_WORKFLOWS,
        search_attributes=TypedSearchAttributes([SearchAttributePair(OWNER, owner)]), memo={"tier": tier})


async def wait_gpu_pending(handle: WorkflowHandle) -> None:
    """Until the run's next Activity is the GPU tool, sitting in the backlog with nobody to take it."""
    while True:
        d = await handle.describe()
        for pa in d.raw_description.pending_activities:
            if pa.activity_type.name == "execute_tool" and (await handle.query(AgentRun.status))["step"] == 2:
                return
        await asyncio.sleep(0.2)


async def gpu_start_time(handle: WorkflowHandle) -> tuple[float, int, str]:
    """(ActivityTaskStarted time of the GPU Task, its priority_key, identity of the Worker) from history."""
    history = await handle.fetch_history()
    gpu_scheduled = {}
    for ev in history.events:
        if ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            a = ev.activity_task_scheduled_event_attributes
            if a.task_queue.name == GPU_TOOLS:
                gpu_scheduled[ev.event_id] = a.priority.priority_key
        elif ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            a = ev.activity_task_started_event_attributes
            if a.scheduled_event_id in gpu_scheduled:
                return ev.event_time.ToDatetime().timestamp(), gpu_scheduled[a.scheduled_event_id], a.identity
    raise RuntimeError(f"{handle.id}: GPU Task never started")


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["priority", "fairness"], default="priority")
    p.add_argument("--batch", type=int, default=4, help="runs queued ahead (priority mode) / per tenant (fairness mode)")
    p.add_argument("--tag", default=time.strftime("%H%M%S"), help="suffix so Workflow IDs are fresh on every run")
    a = p.parse_args()
    os.environ.setdefault("GPU_SCHEDULE_TO_START_SECONDS", "180")

    client = await connect()
    wf_worker = Worker(client, task_queue=AGENT_WORKFLOWS, workflows=[AgentRun], identity=f"wf-{os.getpid()}@{AGENT_WORKFLOWS}")
    cpu_worker = Worker(client, task_queue=CPU_TOOLS, activities=[call_llm, execute_tool, evaluate], identity=f"cpu-{os.getpid()}@{CPU_TOOLS}")
    gpu_worker = Worker(client, task_queue=GPU_TOOLS, activities=[execute_tool_on_gpu], max_concurrent_activities=1,
                        identity=f"gpu-{os.getpid()}@{GPU_TOOLS}")
    async with wf_worker, cpu_worker:
        queued: list[tuple[str, WorkflowHandle]] = []
        if a.mode == "priority":
            plan = [(f"race-{a.tag}-batch-{i}", "acct_batch", "batch") for i in range(1, a.batch + 1)]
            plan.append((f"race-{a.tag}-enterprise", "acct_ent", "enterprise"))
        else:
            plan = [(f"race-{a.tag}-a-{i}", "acct_a", "batch") for i in range(1, a.batch + 1)]
            plan += [(f"race-{a.tag}-b-{i}", "acct_b", "batch") for i in range(1, a.batch + 1)]
        for base, owner, tier in plan:
            # Strictly one at a time: each run must be IN the GPU backlog before the next is started,
            # so enqueue order is the order of this list.
            h = await start(client, base, owner, tier)
            await wait_gpu_pending(h)
            queued.append((owner, h))
            print(f"queued  {h.id:40s} owner={owner:10s} tier={tier}")
        print("--- backlog before the GPU Worker exists:")
        await q5_lane(client, GPU_TOOLS)
        print(f"--- starting one GPU Worker with 1 slot on {GPU_TOOLS!r}")
        async with gpu_worker:
            await asyncio.gather(*(h.result() for _, h in queued))
        rows = []
        for owner, h in queued:
            t, prio, ident = await gpu_start_time(h)
            rows.append((t, h.id, owner, prio))
        t0 = min(r[0] for r in rows)
        print("--- dispatch order observed (ActivityTaskStarted on the GPU lane):")
        for i, (t, wid, owner, prio) in enumerate(sorted(rows), 1):
            print(f"  {i:2d}. +{t - t0:5.2f}s  {wid:40s} owner={owner:10s} priority_key={prio}")
        enqueue_order = [h.id for _, h in queued]
        observed = [wid for _, wid, _, _ in sorted(rows)]
        print("--- enqueue order was:", " > ".join(x.rsplit('-', 2)[-2] + "-" + x.rsplit('-', 2)[-1] for x in enqueue_order))
        print("--- observed order is:", " > ".join(x.rsplit('-', 2)[-2] + "-" + x.rsplit('-', 2)[-1] for x in observed))
        print("FIFO" if observed == enqueue_order else "NOT FIFO: the lane reordered the backlog")


if __name__ == "__main__":
    asyncio.run(main())
