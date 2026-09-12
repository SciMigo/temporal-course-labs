"""Send messages to a running AgentRun from the command line.

    python control.py pause|resume|cancel|status|history [--id agent-42]
    python control.py inject "Prefer sources after 2025" --command-id cmd-7f3a [--twice]
    python control.py change-goal "write the tests first" --command-id cmd-9f1e [--twice]
    python control.py interleave [--no-drain]          # 5.5: two slow Updates + pause + continue-as-new

`--twice` simulates a client whose connection broke after the first send: it sends the
same message again with the same command_id, exactly as a retry loop would.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.client import Client, WorkflowUpdateFailedError, WorkflowUpdateStage  # noqa: E402
from temporalio.api.enums.v1 import EventType  # noqa: E402
from temporalio.service import RPCError  # noqa: E402
from workflows import AgentRun  # noqa: E402


async def history(client: Client, workflow_id: str) -> None:
    """Like common.show_history, plus the detail the lab reads: which Signal, which Update, which Activity."""
    handle = client.get_workflow_handle(workflow_id)
    async for ev in handle.fetch_history_events():
        name = EventType.Name(ev.event_type).removeprefix("EVENT_TYPE_")
        detail = ""
        if ev.HasField("workflow_execution_signaled_event_attributes"):
            detail = ev.workflow_execution_signaled_event_attributes.signal_name
        elif ev.HasField("workflow_execution_update_accepted_event_attributes"):
            a = ev.workflow_execution_update_accepted_event_attributes
            detail = f"{a.accepted_request.input.name} update_id={a.protocol_instance_id}"
        elif ev.HasField("workflow_execution_update_completed_event_attributes"):
            detail = f"update_id={ev.workflow_execution_update_completed_event_attributes.meta.update_id}"
        elif ev.HasField("activity_task_scheduled_event_attributes"):
            detail = ev.activity_task_scheduled_event_attributes.activity_type.name
        elif ev.HasField("workflow_task_started_event_attributes"):
            detail = f"identity={ev.workflow_task_started_event_attributes.identity}"
        print(f"{ev.event_id:4d}  {name:38s} {detail}")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("verb", choices=["pause", "resume", "cancel", "status", "history", "inject", "change-goal",
                                     "interleave"])
    ap.add_argument("text", nargs="?", default="", help="the note (inject) or the new goal (change-goal)")
    ap.add_argument("--id", default=os.environ.get("WORKFLOW_ID", "agent-42"))
    ap.add_argument("--command-id", default=None, help="idempotency key chosen once per command; reused on retry")
    ap.add_argument("--twice", action="store_true", help="send the same message twice (simulated retry)")
    ap.add_argument("--reuse-update-id", action="store_true",
                    help="change-goal only: also reuse command_id as the Temporal Update ID, so the Service dedupes it")
    ap.add_argument("--no-drain", action="store_true",
                    help="interleave only: continue-as-new without waiting for handlers (reproduces the lost handler)")
    args = ap.parse_args()

    client = await connect()
    handle = client.get_workflow_handle_for(AgentRun.run, args.id)
    sends = 2 if args.twice else 1

    try:
        await dispatch(ap, args, client, handle, sends)
    except RPCError as e:
        if e.status.name == "NOT_FOUND":
            print(f"{e.message}: {args.id!r} is not running — a Signal or Update needs a live execution")
        else:
            raise


async def dispatch(ap, args, client: Client, handle, sends: int) -> None:
    if args.verb == "pause":
        await handle.signal(AgentRun.pause)                     # returns once the Service recorded it
        print("pause signalled (recorded in history; no Worker needed to accept it)")
    elif args.verb == "resume":
        await handle.signal(AgentRun.resume)
        print("resume signalled")
    elif args.verb == "cancel":
        await handle.signal(AgentRun.cancel_run)
        print("cancel_run signalled — the loop exits at its next check; the execution closes as Completed")
    elif args.verb == "status":
        try:
            print(await handle.query(AgentRun.status, rpc_timeout=timedelta(seconds=10)))
        except RPCError as e:
            print(f"query failed: {e.status.name}: {e.message} — a Query is answered by a Worker; is one running?")
    elif args.verb == "history":
        await history(client, args.id)
    elif args.verb == "inject":
        if not args.command_id:
            ap.error("inject needs --command-id")
        for i in range(sends):
            await handle.signal(AgentRun.inject_context, args=[args.text, args.command_id])
            print(f"send {i + 1}: inject_context({args.text!r}, {args.command_id!r}) recorded")
        print("status:", await handle.query(AgentRun.status))
    elif args.verb == "change-goal":
        if not args.command_id:
            ap.error("change-goal needs --command-id")
        for i in range(sends):
            try:
                new_goal = await handle.execute_update(
                    AgentRun.change_goal, args=[args.text, args.command_id],
                    id=args.command_id if args.reuse_update_id else None,
                )
                print(f"send {i + 1}: accepted; handler returned {new_goal!r}")
            except WorkflowUpdateFailedError as e:
                print(f"send {i + 1}: rejected: {type(e.cause).__name__}: {e.cause}")
        print("status:", await handle.query(AgentRun.status))
    elif args.verb == "interleave":
        await interleave(client, handle, drain=not args.no_drain)


async def interleave(client: Client, handle, drain: bool) -> None:
    """5.5: two `enrich_context` Updates (each awaits a slow Activity), a `pause`, and a
    continue-as-new request, sent back to back. Then wait for the Updates and read the new run."""
    old_run = (await handle.describe()).run_id
    tag = old_run[:4]
    updates = []
    for n, text in enumerate(["Prefer primary sources", "Cite line numbers"], start=1):
        u = await handle.start_update(                          # accepted = the Service has it; not finished
            AgentRun.enrich_context, args=[text, f"cmd-{tag}-{n}"], wait_for_stage=WorkflowUpdateStage.ACCEPTED,
        )
        updates.append((text, u))
        print(f"update {n}: enrich_context({text!r}) accepted; handler now awaiting expand_note")
    await handle.signal(AgentRun.pause)
    print("pause signalled")
    await handle.signal(AgentRun.request_continue_as_new, drain)
    print(f"request_continue_as_new(drain={drain}) signalled")
    for text, u in updates:
        try:
            print(f"update {text!r}: handler returned {await u.result()!r}")
        except Exception as e:                                  # noqa: BLE001 — print what the caller sees
            cause = getattr(e, "cause", None)
            print(f"update {text!r}: LOST — {type(e).__name__}: {cause or e}")
    old = await client.get_workflow_handle(handle.id, run_id=old_run).describe()
    latest = await handle.describe()                                # the handle has no run_id: latest run
    print(f"old run {old_run[:8]} closed as {old.status.name}; latest run {latest.run_id[:8]} is {latest.status.name}")
    print("status of the new run:", await handle.query(AgentRun.status))
    print("hint: `python control.py resume` when you are done reading; `history` shows the new run")


if __name__ == "__main__":
    asyncio.run(main())
