"""Shared helpers for the labs. Nothing here is Temporal-magic; read it."""
from __future__ import annotations

import os

from temporalio.api.enums.v1 import EventType
from temporalio.client import Client

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
# The course's task queue is `agent-runs`. Override per lab when several labs share one dev server
# (a Worker from lab 03 must not pick up lab 05's Workflow Tasks): TASK_QUEUE=lab-05 python worker.py
TASK_QUEUE = os.environ.get("TASK_QUEUE", "agent-runs")


async def connect() -> Client:
    return await Client.connect(TEMPORAL_ADDRESS, namespace=NAMESPACE)


async def show_history(client: Client, workflow_id: str, run_id: str | None = None) -> None:
    """Print the event history as `N  EventType  (key details)` — the format the labs annotate."""
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    async for event in handle.fetch_history_events():
        raw = event.event_type
        name = (raw.name if hasattr(raw, "name") else EventType.Name(raw)).removeprefix("EVENT_TYPE_")
        detail = ""
        if event.HasField("activity_task_scheduled_event_attributes"):
            detail = event.activity_task_scheduled_event_attributes.activity_type.name
        elif event.HasField("timer_started_event_attributes"):
            detail = f"{event.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta()}"
        elif event.HasField("workflow_task_started_event_attributes"):
            detail = event.workflow_task_started_event_attributes.identity
        print(f"{event.event_id:4d}  {name:36s} {detail}")
