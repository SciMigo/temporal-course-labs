"""The Activity side of a history: every attempt the Service recorded, with timings.

    python history.py agent-42-start_to_close
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.api.enums.v1 import EventType, RetryState, TimeoutType  # noqa: E402


def _payloads(payloads) -> str:
    return ", ".join(p.data.decode("utf-8", "replace") for p in payloads)


def _failure(f) -> str:
    parts = []
    if f.HasField("timeout_failure_info"):
        ti = f.timeout_failure_info
        parts.append(f"timeout_type={TimeoutType.Name(ti.timeout_type).removeprefix('TIMEOUT_TYPE_')}")
        if ti.last_heartbeat_details.payloads:
            parts.append(f"last_heartbeat_details=[{_payloads(ti.last_heartbeat_details.payloads)}]")
    if f.HasField("application_failure_info"):
        ai = f.application_failure_info
        parts.append(f"type={ai.type!r} non_retryable={ai.non_retryable}")
    if f.message:
        parts.append(f"message={f.message!r}")
    if f.HasField("cause"):
        parts.append(f"cause=({_failure(f.cause)})")
    return " ".join(parts)


async def main() -> None:
    wf_id = sys.argv[1] if len(sys.argv) > 1 else "agent-42-default"
    client = await connect()
    handle = client.get_workflow_handle(wf_id)
    t0 = None
    async for ev in handle.fetch_history_events():
        ts = ev.event_time.ToDatetime()
        t0 = t0 or ts
        name = EventType.Name(ev.event_type).removeprefix("EVENT_TYPE_")
        d = ""
        if ev.HasField("activity_task_scheduled_event_attributes"):
            a = ev.activity_task_scheduled_event_attributes
            rp = a.retry_policy
            d = (f"activity={a.activity_type.name} queue={a.task_queue.name} "
                 f"s2s={a.schedule_to_start_timeout.ToTimedelta()} stc={a.start_to_close_timeout.ToTimedelta()} "
                 f"s2c={a.schedule_to_close_timeout.ToTimedelta()} hb={a.heartbeat_timeout.ToTimedelta()} "
                 f"max_attempts={rp.maximum_attempts}")
        elif ev.HasField("activity_task_started_event_attributes"):
            a = ev.activity_task_started_event_attributes
            d = f"worker={a.identity} attempt={a.attempt}"
            if a.HasField("last_failure"):
                d += f" last_failure=({_failure(a.last_failure)})"
        elif ev.HasField("activity_task_completed_event_attributes"):
            a = ev.activity_task_completed_event_attributes
            d = f"result=[{_payloads(a.result.payloads)[:120]}]"
        elif ev.HasField("activity_task_failed_event_attributes"):
            a = ev.activity_task_failed_event_attributes
            d = f"retry_state={RetryState.Name(a.retry_state).removeprefix('RETRY_STATE_')} {_failure(a.failure)}"
        elif ev.HasField("activity_task_timed_out_event_attributes"):
            a = ev.activity_task_timed_out_event_attributes
            d = f"retry_state={RetryState.Name(a.retry_state).removeprefix('RETRY_STATE_')} {_failure(a.failure)}"
        elif ev.HasField("workflow_execution_completed_event_attributes"):
            d = f"result=[{_payloads(ev.workflow_execution_completed_event_attributes.result.payloads)[:200]}]"
        elif ev.HasField("workflow_task_started_event_attributes"):
            d = f"worker={ev.workflow_task_started_event_attributes.identity}"
        print(f"{ev.event_id:3d}  +{(ts - t0).total_seconds():6.2f}s  {name:28s} {d}")
    desc = (await handle.describe()).raw_description
    for pa in desc.pending_activities:
        hb = _payloads(pa.heartbeat_details.payloads) if pa.HasField("heartbeat_details") else ""
        print(f"pending activity: {pa.activity_type.name} attempt={pa.attempt} state={pa.state} "
              f"heartbeat_details=[{hb}] last_failure=({_failure(pa.last_failure) if pa.HasField('last_failure') else ''})")


if __name__ == "__main__":
    asyncio.run(main())
