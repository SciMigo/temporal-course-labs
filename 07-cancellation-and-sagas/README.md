# Lab 7 — Cancellation and Compensation

Goal: make AgentRun undo what it did when step four of four fails, prove the undo list survives a
Worker death, and then find out what `handle.cancel()` does — and does not do — to an Activity
that is already running.


Same program as lab 6 plus, in `workflows.py`: `gpu_lane()` (four Activities, each success
appending its undo to `self.compensations`), `rollback()` (the list in reverse, run from `finally`),
and a `crunch` tool that is deliberately long. Reading page: "Cancellation and Compensation".

## Setup

```bash
cd labs/07-cancellation-and-sagas
export TASK_QUEUE=lab-07
python worker.py                     # terminal 1 — the Activities narrate what they do; read this terminal
python starter.py                    # terminal 2 — starts agent-42 with the deploy_model tool
```

`starter.py --tool crunch --cancel-after 4 [--heartbeat] [--shield]` is 7.3. `--id`/`WORKFLOW_ID`
if `agent-42` is taken. The starter waits for the run to close, then prints the annotated history.

## 7.1 The saga: `register_endpoint` fails, three undos run in reverse

`plan()` turns a goal that starts with "deploy" into the `deploy_model` tool, and `gpu_lane()` runs
`reserve_gpu → allocate_sandbox → start_model → register_endpoint`. The last one raises an
`ApplicationError` every time; its `RetryPolicy(maximum_attempts=3)` ends it. Run the starter:

```text
execution closed with ActivityError: Activity task failed
close status: FAILED
final status(): {'compensations': [], 'rolled_back': ['stop_model', 'release_sandbox', 'release_gpu'], 'status': 'failed', 'step': 1}

  11  ACTIVITY_TASK_SCHEDULED                reserve_gpu
  13  ACTIVITY_TASK_COMPLETED                (scheduled by event 11)
  17  ACTIVITY_TASK_SCHEDULED                allocate_sandbox
  19  ACTIVITY_TASK_COMPLETED                (scheduled by event 17)
  23  ACTIVITY_TASK_SCHEDULED                start_model
  25  ACTIVITY_TASK_COMPLETED                (scheduled by event 23)
  29  ACTIVITY_TASK_SCHEDULED                register_endpoint
  30  ACTIVITY_TASK_STARTED                  attempt=3
  31  ACTIVITY_TASK_FAILED                   endpoint registry rejected the name for model-in-sandbox-on-gpu-for-agent-42
  35  ACTIVITY_TASK_SCHEDULED                stop_model
  41  ACTIVITY_TASK_SCHEDULED                release_sandbox
  47  ACTIVITY_TASK_SCHEDULED                release_gpu
  53  WORKFLOW_EXECUTION_FAILED              Activity task failed
```

Read it against the code:

1. Three successes, one `ACTIVITY_TASK_FAILED`. Attempts 1 and 2 are not Events — `ACTIVITY_TASK_STARTED`
   is written only when the Activity closes, and it says `attempt=3`. The Worker terminal has all
   three: `register_endpoint attempt 1 ... rejected`, `attempt 2`, `attempt 3`. Retry was
   configuration; nothing in the Workflow ran between attempts.
2. Then the undos, in reverse: `stop_model`, `release_sandbox`, `release_gpu`. No
   `unregister_endpoint` — it was never appended, because its forward step never succeeded.
   That is the whole Saga: append after success, run backwards on failure.
3. The run closed **Failed** with the original `ActivityError`, after the rollback. `rollback()`
   runs from `finally`, which is why it also runs for a cancel (7.3).
4. The undos take their identifiers from what the forward steps returned (`gpu-for-agent-42`,
   `sandbox-on-gpu-for-agent-42`) — the key a second attempt of `release_gpu` would reuse.

## 7.2 Kill the Worker during the rollback

Each undo takes 3 s. Start a fresh run and watch the Worker terminal; when it prints
`release_sandbox ...: attempt 1, starting` (the second undo), `kill -9` the Worker. Wait ten seconds
— nothing moves; the UI shows `release_sandbox` as a pending Activity of a Running Workflow — then
start the Worker again. From the run we recorded (old pid 3800398, new pid 3800679):

```text
old Worker:  09:01:25 stop_model model-in-sandbox-on-gpu-for-agent-42: attempt 1, starting
             09:01:28 stop_model ...: done
             09:01:28 release_sandbox sandbox-on-gpu-for-agent-42: attempt 1, starting        <- kill -9 here
new Worker:  09:01:39 release_sandbox sandbox-on-gpu-for-agent-42: attempt 2, starting
             09:01:42 release_sandbox ...: done
             09:01:42 release_gpu gpu-for-agent-42: attempt 1, starting
             09:01:45 release_gpu ...: done

  35  ACTIVITY_TASK_SCHEDULED                stop_model
  37  ACTIVITY_TASK_COMPLETED                (scheduled by event 35)
  41  ACTIVITY_TASK_SCHEDULED                release_sandbox
  42  ACTIVITY_TASK_STARTED                  attempt=2
  43  ACTIVITY_TASK_COMPLETED                (scheduled by event 41)
  45  WORKFLOW_TASK_STARTED                  identity=3800679@...
  47  ACTIVITY_TASK_SCHEDULED                release_gpu
  53  WORKFLOW_EXECUTION_FAILED              Activity task failed
```

What happened: the new Worker replayed the history and rebuilt `self.compensations` from the three
recorded successes, then resumed `rollback()` at the first undo with no result in history —
`release_sandbox`, whose Event 41 already existed. It did not re-run `stop_model`; that result was
in Event 37. The eleven-second gap is `STEP_TIMEOUT` (10 s: the Service can only learn the first
attempt died when its Start-To-Close expires) plus the 1 s retry interval, and `attempt=2` is the
at-least-once execution the reading warned about: `release_sandbox` must tolerate running twice.

> **A saga does not guarantee rollback.** `rollback()` is ordinary durable work: each undo is an
> Activity with its own retry policy, and an undo that exhausts its retries raises out of
> `rollback()` — which this Workflow calls from `finally`. Two consequences worth designing for
> before you ship one. The undos after the failing one never run (`release_gpu` is still owed while
> `release_sandbox` is stuck), and the exception from the rollback replaces the forward failure that
> caused it, so the reason you were rolling back is the one you lose. A production saga keeps both
> failures, retries an undo far longer than a forward step, records "compensation incomplete" as a
> first-class state an operator can list, and lets the independent undos proceed rather than
> stopping at the first one that will not. Lab 11's capstone asks you to decide this for `AgentRun`.

## 7.3 The nasty one: cancel a tool that does not heartbeat

The `crunch` tool runs 20 chunks of one second inside a sandbox (`allocate_sandbox` first, so there
is one undo on the list). `--cancel-after 4` makes the starter call `handle.cancel()` four seconds
in. Three runs, and read the **Worker terminal** as much as the history each time.

### a. No heartbeats: `python starter.py --tool crunch --cancel-after 4`

```text
status before cancel: {'compensations': ['release_sandbox'], 'in_flight': 'crunch', 'status': 'running', 'step': 1}
handle.cancel() sent — a WorkflowExecutionCancelRequested Event; the Workflow sees it at its next await
execution closed with CancelledError: Workflow cancelled
close status: CANCELED
final status(): {'rolled_back': ['release_sandbox'], 'status': 'cancelled', 'step': 1}

  17  ACTIVITY_TASK_SCHEDULED                execute_tool
  18  WORKFLOW_EXECUTION_CANCEL_REQUESTED
  22  ACTIVITY_TASK_CANCEL_REQUESTED         (scheduled by event 17)
  23  ACTIVITY_TASK_SCHEDULED                release_sandbox
  25  ACTIVITY_TASK_COMPLETED                (scheduled by event 23)
  29  WORKFLOW_EXECUTION_CANCELED
```

The Workflow did everything right: it saw the request at its next await (the tool), asked the
Service to cancel the Activity (22), released the sandbox from `finally` (23–25) — no shield, the
one cancellation had already been delivered — and closed **Canceled**. Now the Worker terminal:

```text
09:02:27 execute_tool crunch: chunk 1/20
09:02:28 execute_tool crunch: chunk 2/20
09:02:29 release_sandbox sandbox-on-agent-42:1:crunch: attempt 1, starting
09:02:29 execute_tool crunch: chunk 3/20
...
09:02:32 release_sandbox ...: done
09:02:32 execute_tool crunch: chunk 6/20
...
09:02:46 execute_tool crunch: chunk 20/20
09:02:46 execute_tool crunch: finished all 20 chunks
WARN temporalio_sdk_core::worker::activities: Activity not found on completion. This may happen if the
     activity has already been cancelled but completed anyway. ... "workflow execution already completed"
```

The tool ran all twenty chunks *after* the cancel, past the sandbox it was running in being
released, and its result was thrown away. There is no `ACTIVITY_TASK_CANCELED` in history because
nothing ever told the Activity: it never spoke to the Service while it ran, and cancellation only
travels on heartbeat responses. You paid for sixteen seconds of a tool nobody wanted.

### b. Heartbeats: `python starter.py --tool crunch --cancel-after 4 --heartbeat`

The diff is two lines: `activity.heartbeat(i)` in the chunk loop, and `heartbeat_timeout=3 s` on
the call (which also sets the heartbeat send interval to 0.8 × 3 s).

```text
  18  WORKFLOW_EXECUTION_CANCEL_REQUESTED
  22  ACTIVITY_TASK_CANCEL_REQUESTED         (scheduled by event 17)
  23  ACTIVITY_TASK_SCHEDULED                release_sandbox
  25  ACTIVITY_TASK_CANCELED                 (scheduled by event 17)
  30  ACTIVITY_TASK_COMPLETED                (scheduled by event 23)
  34  WORKFLOW_EXECUTION_CANCELED

09:10:02 release_sandbox ...: attempt 1, starting
09:10:04 execute_tool crunch: chunk 4/20
09:10:05 execute_tool crunch: cancelled at chunk 5/20
09:10:05 release_sandbox ...: done
```

Now there *is* an `ACTIVITY_TASK_CANCELED` (25): the request rode back on a heartbeat, the SDK
raised `CancelledError` inside the Activity at its next `await`, the `except` logged and
re-raised. The tool stopped three seconds after the cancel — within one heartbeat interval — and
its acknowledgement landed while the rollback was still running. Same Workflow code as (a).

### c. Shield the in-flight call: `python starter.py --tool crunch --cancel-after 4 --heartbeat --shield`

`cpu_lane()` now awaits `asyncio.shield(tool)` and, on `CancelledError`, awaits the tool handle
itself before letting the cancel through.

```text
  17  ACTIVITY_TASK_SCHEDULED                execute_tool
  18  WORKFLOW_EXECUTION_CANCEL_REQUESTED
  22  ACTIVITY_TASK_STARTED                  attempt=1
  23  ACTIVITY_TASK_COMPLETED                (scheduled by event 17)
  27  ACTIVITY_TASK_SCHEDULED                release_sandbox
  29  ACTIVITY_TASK_COMPLETED                (scheduled by event 27)
  33  WORKFLOW_EXECUTION_CANCELED

09:09:49 execute_tool crunch: finished all 20 chunks
09:09:49 release_sandbox ...: attempt 1, starting
```

No `ACTIVITY_TASK_CANCEL_REQUESTED` at all: the cancel reached the `await`, not the Activity. The
tool completed (23) sixteen seconds after the request (18), *then* the sandbox was released,
*then* the run closed Canceled. Compare the three histories and say which one you want for a
40-minute fine-tune that is 90% done, and which for a runaway web crawl.

Two details in `run()` worth reading before you move on. The cancel that lands while an Activity
is awaited arrives as `ActivityError` whose `cause` is `CancelledError` — the in-flight Activity's
own cancellation — not as a bare `asyncio.CancelledError`; `run()` classifies both as
`"cancelled"`, and re-raising either closes the execution as Canceled. And the shield awaits the
*handle* (`workflow.start_activity(...)`), never the cancelled outer future a second time — that
one raises `CancelledError` immediately, which is the mistake our first draft made.

## 7.3b The three modes, side by side

| | Activity gets a cancel request? | Workflow waits for the tool? | What happens to the tool |
|---|---|---|---|
| No heartbeats | yes | no | runs to the end, never learns |
| Heartbeats | yes | no | sees the request on a heartbeat response, raises |
| `asyncio.shield` | **no** | yes, deliberately | finishes normally |

So "does the tool stop?" is not one question about heartbeats. It is two: does the Workflow ask for
the Activity to be cancelled at all, and — if it does — does the Activity ever come back to the
Service to hear the answer? Shielding answers the first no; a tool with no heartbeat answers the
second no.

## 7.4 Cancel is cooperative. Terminate is not.

Run the `crunch` case once more, and this time do not cancel it. Terminate it:

```bash
python starter.py --tool crunch --no-wait         # terminal 2
temporal workflow terminate --workflow-id agent-42 --reason "operator hard stop"
```

Measured on the labs' dev server (Temporal Server 1.31.2, 2026-09), the history simply stops:

```text
17  ACTIVITY_TASK_SCHEDULED        execute_tool
18  WORKFLOW_EXECUTION_TERMINATED
```

Compare that with 7.3a's cancel, which ran to `WORKFLOW_EXECUTION_CANCELED` at event 29 through
`ACTIVITY_TASK_CANCEL_REQUESTED` (22) and `release_sandbox` (23–25). Under terminate there is no
cancel request, no `finally`, no rollback — **event 18 is the last thing that will ever be written
to this Workflow.** Your Workflow code never runs again, so it cannot clean up; `self.compensations`
still listed `release_sandbox` and nothing released it.

Then read the Worker terminal: `execute_tool crunch: chunk 6/20`, `7/20`, and on. The Activity is a
process on a Worker, and nobody told it anything. The sandbox stays allocated until something
outside this Workflow reclaims it.

- **Cancel** when the Workflow should clean up after itself: it is a request the code observes at its
  next await, and everything in this lab's `finally` depends on that.
- **Terminate** when you need it stopped now and accept that nothing cooperative happens — then go
  and reclaim the resources yourself. For an agent runtime holding GPUs, that is an operational
  decision, not a keyboard shortcut.

## The recurring question

If every Worker process dies right now: `self.compensations` is Workflow state, so a returning
Worker rebuilds it from the recorded successes and resumes the rollback at the first undo without a
result (7.2). A cancel request already in history is delivered once, at the first await after
replay — it is not lost with the Worker. The Activity that was in flight is a Worker-side process:
without heartbeats the Service only notices it is gone when Start-To-Close expires; with them, one
Heartbeat Timeout later. Keep that apart from cancellation *delivery*: a cancel request is not
waiting on a timeout at all — it rides back on the next heartbeat round-trip, which is why a tool
that never heartbeats never hears it. Its effect may have happened either way, which is why every
undo is keyed.

## Check yourself

`python -m pytest tests/ -q` — the reverse-order rollback with no `unregister_endpoint`; a cancel
without heartbeats leaving `ACTIVITY_TASK_CANCEL_REQUESTED` but no `ACTIVITY_TASK_CANCELED` while
the tool finishes every chunk on the Worker; a cancel with heartbeats producing `ACTIVITY_TASK_CANCELED`
and a tool that never finishes; and the shielded tool completing before `release_sandbox` runs.

## What you learned

- A saga records compensation decisions in Workflow state and runs completed undos in reverse order.
- Activity cancellation is cooperative; a non-heartbeating tool may keep running after a cancel request.
- Shielding an in-flight effect changes when compensation begins, and every undo still needs an idempotency key.

## Questions to answer

1. Why is there no `unregister_endpoint` compensation after registration fails?
2. After killing the Worker during rollback, which recorded facts let a new Worker continue the undo list?
3. Why can a cancel request appear in history while the Activity continues to do work?
4. What difference do heartbeats and shielding make to the final effect and compensation order?
