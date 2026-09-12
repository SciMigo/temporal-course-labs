"""Print a Workflow's Event History with the facts you annotate in 2.1 — or save it as JSON.

    python history.py agent-42                  # one row per Event, with the attributes that matter
    python history.py agent-42 --json h.json    # the same history as data (Replayer input)
    python history.py agent-42 --first 13       # only Events 1..13 (2.4: cut the history mid-way)
    python history.py --file histories/x.json   # print a saved JSON history instead

The "caused by" column of the reading's table is deliberately NOT printed — writing it is the
exercise. Everything printed is read straight off the Event's attributes.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.api.enums.v1 import EventType, TaskQueueKind, TimeoutType, WorkflowTaskFailedCause  # noqa: E402
from temporalio.api.history.v1 import HistoryEvent  # noqa: E402
from temporalio.client import WorkflowHistory  # noqa: E402


def _payloads(payloads) -> str:
    out = []
    for p in payloads:
        try:
            out.append(p.data.decode("utf-8"))
        except UnicodeDecodeError:
            out.append(f"<{len(p.data)} bytes>")
    return ", ".join(out)


def _queue(tq) -> str:
    kind = TaskQueueKind.Name(tq.kind).removeprefix("TASK_QUEUE_KIND_").lower()
    return f"{tq.name} [{kind}]"


def describe(event: HistoryEvent) -> str:
    """The attributes that let you say what caused the Event."""
    t = event.event_type
    if t == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
        a = event.workflow_execution_started_event_attributes
        return (f"type={a.workflow_type.name} input=[{_payloads(a.input.payloads)}] "
                f"task_queue={a.task_queue.name} client={a.identity} attempt={a.attempt}")
    if t == EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
        a = event.workflow_task_scheduled_event_attributes
        return f"queue={_queue(a.task_queue)} attempt={a.attempt}"
    if t == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED:
        return f"worker={event.workflow_task_started_event_attributes.identity}"
    if t == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
        return f"worker={event.workflow_task_completed_event_attributes.identity}  (Commands follow)"
    if t == EventType.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
        a = event.workflow_task_timed_out_event_attributes
        return f"timeout_type={TimeoutType.Name(a.timeout_type).removeprefix('TIMEOUT_TYPE_')}"
    if t == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
        a = event.workflow_task_failed_event_attributes
        cause = WorkflowTaskFailedCause.Name(a.cause).removeprefix("WORKFLOW_TASK_FAILED_CAUSE_")
        return f"cause={cause} worker={a.identity} message={a.failure.message!r}"
    if t == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
        a = event.activity_task_scheduled_event_attributes
        return (f"activity={a.activity_type.name} activity_id={a.activity_id} "
                f"queue={a.task_queue.name} input=[{_payloads(a.input.payloads)}]")
    if t == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
        a = event.activity_task_started_event_attributes
        return f"worker={a.identity} attempt={a.attempt} scheduled_event_id={a.scheduled_event_id}"
    if t == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
        a = event.activity_task_completed_event_attributes
        return f"result=[{_payloads(a.result.payloads)}] worker={a.identity}"
    if t == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED:
        a = event.activity_task_failed_event_attributes
        return f"message={a.failure.message!r}"
    if t == EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
        a = event.activity_task_timed_out_event_attributes
        tt = TimeoutType.Name(a.failure.timeout_failure_info.timeout_type).removeprefix("TIMEOUT_TYPE_")
        return f"timeout_type={tt}"
    if t == EventType.EVENT_TYPE_TIMER_STARTED:
        a = event.timer_started_event_attributes
        return f"timer_id={a.timer_id} fires_after={a.start_to_fire_timeout.ToTimedelta()}"
    if t == EventType.EVENT_TYPE_TIMER_FIRED:
        a = event.timer_fired_event_attributes
        return f"timer_id={a.timer_id} started_event_id={a.started_event_id}"
    if t == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
        a = event.workflow_execution_completed_event_attributes
        return f"result=[{_payloads(a.result.payloads)}]"
    if t == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
        return f"message={event.workflow_execution_failed_event_attributes.failure.message!r}"
    return ""


def print_history(history: WorkflowHistory, first: int | None = None) -> None:
    t0 = None
    for event in history.events:
        if first is not None and event.event_id > first:
            break
        ts = event.event_time.ToDatetime()
        t0 = t0 or ts
        name = EventType.Name(event.event_type).removeprefix("EVENT_TYPE_")
        print(f"{event.event_id:3d}  +{(ts - t0).total_seconds():6.2f}s  {name:30s} {describe(event)}")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("workflow_id", nargs="?", default=os.environ.get("WORKFLOW_ID", "agent-42"))
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--file", help="print a saved JSON history instead of fetching one")
    ap.add_argument("--json", metavar="OUT", help="also write the history as JSON to OUT")
    ap.add_argument("--first", type=int, help="show (and save) only Events 1..N")
    args = ap.parse_args()

    if args.file:
        with open(args.file) as f:
            history = WorkflowHistory.from_json(args.workflow_id, f.read())
    else:
        client = await connect()
        history = await client.get_workflow_handle(args.workflow_id, run_id=args.run_id).fetch_history()

    print_history(history, args.first)
    if args.json:
        events = history.events if args.first is None else history.events[: args.first]
        with open(args.json, "w") as f:
            f.write(WorkflowHistory(workflow_id=history.workflow_id, events=events).to_json())
        print(f"wrote {len(events)} events to {args.json}")


if __name__ == "__main__":
    asyncio.run(main())
