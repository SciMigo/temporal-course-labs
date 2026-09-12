"""Lab 11.2 — a 200-step run that continues-as-new, and proof that no run's history got long.

    python run_200_steps.py [--steps 200] [--id agent-200] [--external-workers] [--bound N]

By default the three Worker pools run inside this process (so one command does the whole thing);
with --external-workers it only starts the run and expects worker_workflows.py / worker_cpu.py /
worker_gpu.py to be up. Afterwards it walks the run chain — one Workflow ID, several Run IDs — and
asserts every run's event count is under the bound.
"""
import argparse
import asyncio
import os
import sys
import time

os.environ.setdefault("AGENTRUN_TOOL_SECONDS", "0.02")   # tiny sleeps: the point is the history, not the wait
os.environ.setdefault("AGENTRUN_LLM_SECONDS", "0.005")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.api.enums.v1 import EventType  # noqa: E402
from temporalio.common import SearchAttributePair, TypedSearchAttributes  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

from agentrun import (AGENT_WORKFLOWS, CPU_TOOLS, GPU_TOOLS, OWNER, AgentRun, ResearchAgent,  # noqa: E402
                      call_llm, compensate, evaluate, execute_tool, workflow_id)
from agentrun.workflows import STEPS_PER_RUN  # noqa: E402
from worker_gpu import execute_tool_on_gpu  # noqa: E402


async def walk_runs(client, wf_id: str, first_run_id: str) -> list[dict]:
    """Follow WorkflowExecutionContinuedAsNew links from the first run to the last."""
    runs, run_id = [], first_run_id
    while run_id:
        history = await client.get_workflow_handle(wf_id, run_id=run_id).fetch_history()
        last = history.events[-1]
        closed_as = EventType.Name(last.event_type).removeprefix("EVENT_TYPE_WORKFLOW_EXECUTION_")
        nxt = last.workflow_execution_continued_as_new_event_attributes.new_execution_run_id if last.HasField(
            "workflow_execution_continued_as_new_event_attributes") else ""
        runs.append({"run_id": run_id, "events": len(history.events), "closed_as": closed_as})
        run_id = nxt
    return runs


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--steps", type=int, default=200)
    p.add_argument("--id", default="agent-200")
    p.add_argument("--external-workers", action="store_true")
    p.add_argument("--bound", type=int, default=STEPS_PER_RUN * 14, help="max events any single run may have")
    a = p.parse_args()

    client = await connect()
    workers = [] if a.external_workers else [
        Worker(client, task_queue=AGENT_WORKFLOWS, workflows=[AgentRun, ResearchAgent], identity=f"wf-{os.getpid()}"),
        Worker(client, task_queue=CPU_TOOLS, activities=[call_llm, execute_tool, evaluate, compensate], identity=f"cpu-{os.getpid()}"),
        Worker(client, task_queue=GPU_TOOLS, activities=[execute_tool_on_gpu], identity=f"gpu-{os.getpid()}"),
    ]
    for w in workers:
        await w.__aenter__()
    try:
        t0 = time.time()
        handle = await client.start_workflow(
            AgentRun.run, f"run {a.steps} steps", id=workflow_id(a.id), task_queue=AGENT_WORKFLOWS,
            search_attributes=TypedSearchAttributes([SearchAttributePair(OWNER, "acct_7f3a")]),
            memo={"tier": "team", "max_steps": a.steps})
        print(f"started {handle.id} first run={handle.result_run_id}; continue-as-new every {STEPS_PER_RUN} steps")
        result = await handle.result()          # follows the continue-as-new chain to the final run
        print(f"finished in {time.time() - t0:.1f}s: {result['status']} after {result['steps']} steps, "
              f"attempts={result['attempts']} duplicates_suppressed={result['duplicates_suppressed']}")
        runs = await walk_runs(client, handle.id, handle.result_run_id)
        print(f"{'run':>4}  {'run_id':36s} {'events':>6}  closed as")
        for i, r in enumerate(runs, 1):
            print(f"{i:4d}  {r['run_id']:36s} {r['events']:6d}  {r['closed_as']}")
        worst = max(r["events"] for r in runs)
        print(f"one Workflow ID, {len(runs)} Run IDs; longest history {worst} events; bound {a.bound}")
        assert worst <= a.bound, f"a run's history reached {worst} events, over the bound {a.bound}"
        assert all(r["closed_as"] == "CONTINUED_AS_NEW" for r in runs[:-1]) and runs[-1]["closed_as"] == "COMPLETED"
        desc = await handle.describe()          # the latest run
        sa = {k.name: v for k, v in desc.typed_search_attributes if k.name in ("AgentStatus", "CurrentStep", "Owner")}
        print(f"latest run: status={desc.status.name} search attributes={sa} memo tier={await desc.memo_value('tier')}")
        assert sa == {"AgentStatus": "done", "CurrentStep": a.steps, "Owner": "acct_7f3a"}, sa
        n = 0
        async for wf in client.list_workflows(f"WorkflowType = 'ResearchAgent' AND WorkflowId STARTS_WITH '{handle.id}/research/'"):
            n += 1
        print(f"ResearchAgent children under {handle.id}/research/: {n}")
        print("OK: history bounded, search attributes and memo carried across every continue-as-new")
    finally:
        for w in workers:
            await w.__aexit__(None, None, None)


if __name__ == "__main__":
    asyncio.run(main())
