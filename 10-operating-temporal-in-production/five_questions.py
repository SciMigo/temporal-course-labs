"""The operator's five questions, from the Python client (lab 10.2).

    python five_questions.py five <workflow-id>      # all five, naming the surface that answered each
    python five_questions.py history <workflow-id>   # Q1  what happened            (fetch_history)
    python five_questions.py describe <workflow-id>  # Q2-4 what Temporal thinks     (describe)
    python five_questions.py list "<list filter>"    # fleet: which runs             (list_workflows)
    python five_questions.py lane <task-queue>       # Q5  who is polling, backlog   (DescribeTaskQueue)

CLI equivalents: `temporal workflow show|describe -w <id>`, `temporal workflow list --query`,
`temporal task-queue describe --task-queue <lane> --task-queue-type activity`.
"""
import asyncio
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import NAMESPACE, connect  # noqa: E402
from temporalio.api.enums.v1 import DescribeTaskQueueMode, EventType, TaskQueueType  # noqa: E402
from temporalio.api.taskqueue.v1 import TaskQueue  # noqa: E402
from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest  # noqa: E402
from temporalio.client import Client  # noqa: E402

from lanes import AGENT_WORKFLOWS, CPU_TOOLS, GPU_TOOLS  # noqa: E402

TERMINAL = {"ACTIVITY_TASK_COMPLETED", "ACTIVITY_TASK_FAILED", "ACTIVITY_TASK_TIMED_OUT", "ACTIVITY_TASK_CANCELED"}


def _name(event) -> str:
    return EventType.Name(event.event_type).removeprefix("EVENT_TYPE_")


async def q1_history(client: Client, wf_id: str) -> None:
    """Q1 What happened? — the Event History, in order (strongly consistent)."""
    history = await client.get_workflow_handle(wf_id).fetch_history()
    print(f"Q1 what happened: {len(history.events)} events (fetch_history)")
    scheduled = {}
    for ev in history.events:
        detail = ""
        if ev.HasField("activity_task_scheduled_event_attributes"):
            a = ev.activity_task_scheduled_event_attributes
            scheduled[ev.event_id] = a.activity_type.name
            detail = f"{a.activity_type.name} on {a.task_queue.name}"
            if a.priority.priority_key:
                detail += f" priority_key={a.priority.priority_key}"
        elif ev.HasField("activity_task_started_event_attributes"):
            a = ev.activity_task_started_event_attributes
            detail = f"{scheduled.get(a.scheduled_event_id, '?')} attempt={a.attempt} by {a.identity}"
        elif ev.HasField("activity_task_timed_out_event_attributes"):
            a = ev.activity_task_timed_out_event_attributes
            detail = f"{a.failure.message} retry_state={a.retry_state}"
        elif ev.HasField("workflow_task_started_event_attributes"):
            detail = ev.workflow_task_started_event_attributes.identity
        elif ev.HasField("workflow_execution_failed_event_attributes"):
            detail = ev.workflow_execution_failed_event_attributes.failure.message
        elif ev.HasField("upsert_workflow_search_attributes_event_attributes"):
            detail = ", ".join(sorted(ev.upsert_workflow_search_attributes_event_attributes.search_attributes.indexed_fields))
        elif ev.HasField("timer_started_event_attributes"):
            detail = f"fires in {ev.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta()}"
        print(f"  {ev.event_id:4d}  {_name(ev):36s} {detail}")


async def q2_q4_describe(client: Client, wf_id: str) -> None:
    """Q2 What did Temporal think happened? Q3 What side effects may have occurred? Q4 What next?
    — all from DescribeWorkflowExecution (the current view, strongly consistent)."""
    d = await client.get_workflow_handle(wf_id).describe()
    sa = {k.name: v for k, v in d.typed_search_attributes}
    print(f"Q2 what Temporal thinks: status={d.status.name} task_queue={d.task_queue} history_length={d.history_length}")
    print(f"   search attributes: {sa}")
    raw = d.raw_description
    if not raw.pending_activities:
        print("   pending activities: none")
    for pa in raw.pending_activities:
        details = [p.data.decode(errors='replace') for p in pa.heartbeat_details.payloads] if pa.HasField("heartbeat_details") else []
        print(f"   pending {pa.activity_type.name} state={pa.state} attempt={pa.attempt} max={pa.maximum_attempts} "
              f"last_worker={pa.last_worker_identity!r} last_failure={pa.last_failure.message!r} heartbeat={details}")
    # Q3: any ActivityTaskStarted without a terminal event is an attempt that ran somewhere.
    history = await client.get_workflow_handle(wf_id).fetch_history()
    open_attempts = {}
    for ev in history.events:
        if ev.HasField("activity_task_started_event_attributes"):
            open_attempts[ev.activity_task_started_event_attributes.scheduled_event_id] = ev.event_id
        elif _name(ev) in TERMINAL:
            attrs = getattr(ev, _name(ev).lower() + "_event_attributes")
            open_attempts.pop(attrs.scheduled_event_id, None)
    print(f"Q3 side effects: {len(open_attempts)} attempt(s) started without a terminal event; "
          f"attempt counts above say how many times an idempotency key was presented")
    if raw.pending_activities:
        nxt = [f"{pa.activity_type.name} next attempt {pa.scheduled_time.ToDatetime().replace(tzinfo=timezone.utc).isoformat()}"
               for pa in raw.pending_activities if pa.HasField("scheduled_time")]
        print(f"Q4 what next: {nxt}")
    else:
        print("Q4 what next: no pending Activity; pending timers/children:",
              [f"timer {t.timer_id}" for t in raw.pending_timers] if hasattr(raw, "pending_timers") else "n/a",
              [c.workflow_id for c in raw.pending_children])
    print(f"   (now {datetime.now(timezone.utc).isoformat(timespec='seconds')})")


async def q5_lane(client: Client, lane: str) -> None:
    """Q5 What if every Worker disappears now? — the lane's pollers and backlog (DescribeTaskQueue)."""
    r = await client.workflow_service.describe_task_queue(DescribeTaskQueueRequest(
        namespace=NAMESPACE, task_queue=TaskQueue(name=lane),
        task_queue_types=[TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY, TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW],
        api_mode=DescribeTaskQueueMode.DESCRIBE_TASK_QUEUE_MODE_ENHANCED, report_stats=True, report_pollers=True))
    now = datetime.now(timezone.utc)
    for _build, vinfo in r.versions_info.items():
        for ttype, tinfo in vinfo.types_info.items():
            kind = TaskQueueType.Name(ttype).removeprefix("TASK_QUEUE_TYPE_").lower()
            age = tinfo.stats.approximate_backlog_age.ToTimedelta()
            print(f"Q5 lane {lane} [{kind}]: backlog={tinfo.stats.approximate_backlog_count} age={age} "
                  f"increase_rate={tinfo.stats.tasks_add_rate - tinfo.stats.tasks_dispatch_rate:+.3f}/s")
            for p in tinfo.pollers:
                seen = now - p.last_access_time.ToDatetime().replace(tzinfo=timezone.utc)
                print(f"   poller {p.identity} last poll {seen.total_seconds():.0f}s ago"
                      + ("  <- gone (a Worker polls every ~60 s; the list forgets it after 5 min)" if seen.total_seconds() > 90 else ""))
            if not tinfo.pollers:
                print("   no pollers")


async def fleet(client: Client, query: str) -> None:
    print(f"list_workflows({query!r}):")
    n = 0
    async for wf in client.list_workflows(query):
        n += 1
        sa = {k.name: v for k, v in wf.typed_search_attributes}
        print(f"  {wf.id:32s} {wf.status.name:10s} AgentStatus={sa.get('AgentStatus')} CurrentStep={sa.get('CurrentStep')} Owner={sa.get('Owner')}")
    print(f"  ({n} executions; visibility is eventually consistent)")


async def main(argv: list[str]) -> None:
    client = await connect()
    cmd, arg = (argv + [None, None])[:2]
    if cmd == "history":
        await q1_history(client, arg)
    elif cmd == "describe":
        await q2_q4_describe(client, arg)
    elif cmd == "list":
        await fleet(client, arg)
    elif cmd == "lane":
        await q5_lane(client, arg)
    elif cmd == "five":
        await q1_history(client, arg)
        await q2_q4_describe(client, arg)
        for lane in (AGENT_WORKFLOWS, CPU_TOOLS, GPU_TOOLS):
            await q5_lane(client, lane)
    else:
        print(__doc__)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
