# Lab 5 — Signals, Queries, and Updates

Goal: give AgentRun (stage 5) a control surface — `pause` / `resume` / `cancel_run` /
`inject_context` Signals, a `status` Query, a `change_goal` Update — then prove four things by
reading history: a Signal is durable even when no Worker exists, a Query and a rejected Update
leave nothing behind, a message delivered twice is applied once, and a handler that is still
awaiting an Activity when the run ends is lost unless the main loop drains it first.


**Browser route (Option A):** Open [this lab](http://127.0.0.1:3000/lab/05-signals-queries-updates), click **Prepare this lab**, then use its action cards. Launches return immediately; **Output** shows progress and results. For a variation below, paste one `python ...` command into **Run another command**. The terminal blocks remain available for Option B.

Same program as labs 1–4. The one new line that matters is the first line of the loop in
`workflows.py`: `await workflow.wait_condition(lambda: not self.paused or self.status != "running")`.
Reading page: "Signals, Queries, and Updates".

## Setup

```bash
cd labs/05-signals-queries-updates
export TASK_QUEUE=lab-05             # each lab gets its own queue on the shared dev server
python worker.py                     # terminal 1 — prints its pid; you will kill -9 it in 5.2
python starter.py                    # terminal 2 — starts agent-42 and waits for the result
python control.py status             # terminal 3 — the client you drive by hand
```

`starter.py --id my-agent` (or `WORKFLOW_ID=my-agent`) if `agent-42` is already running on the
shared server; `control.py` reads the same flag/variable. A run finishes on its own after
`MAX_STEPS` steps of ~2 s each (about 45 s); `pause` it whenever you want more time.

## 5.1 Add the Signals and the Query

The handlers are already in `workflows.py`; read them before you run anything and answer: which of
`pause`, `status`, `change_goal` can be called while no Worker is alive? Then check:

1. `python control.py status` a few times. Output is a dict — `{'step': 2, 'status': 'running',
   'paused': False, ...}` — and `python control.py history | wc -l` does not move: a Query is
   answered from the Worker's memory and never written.
2. `python control.py pause`, then `status` again: `'paused': True`, `'status': 'running'`, and
   `step` stops changing. Whatever Activity or Timer was in flight when the Signal landed still
   finishes — the loop checks the flag at its top, not mid-step.
3. `python control.py history`. Find the Signal and what followed it (this is the run we recorded):

   ```text
     22  TIMER_STARTED
     23  WORKFLOW_EXECUTION_SIGNALED            pause
     24  WORKFLOW_TASK_SCHEDULED
     25  WORKFLOW_TASK_STARTED                  identity=3775121@lab-host
     26  WORKFLOW_TASK_COMPLETED
     27  TIMER_FIRED
     28  WORKFLOW_TASK_SCHEDULED
     29  WORKFLOW_TASK_STARTED                  identity=3775121@lab-host
     30  WORKFLOW_TASK_COMPLETED
   ```

   The Signal is an Event (23). The Worker ran a Workflow Task to apply it (24–26) — the handler is
   Workflow code. When the pending timer fired (27) the Worker ran another Workflow Task (28–30)
   that issued *no* command: the loop reached `wait_condition` and blocked. That is what a paused
   Workflow looks like — the last Event is a completed Workflow Task with nothing after it.
4. `python control.py cancel`. The `cancel_run` handler sets `self.status = "cancelled"`; the loop
   exits at its next check and `run()` returns. In the UI the execution is **Completed**, not
   Cancelled — this is your Signal, not Temporal's cancellation (lab 7).

## 5.2 Pause, kill every Worker, resume later

1. Start a fresh run (`python starter.py`), let it take a couple of steps, `python control.py pause`.
2. Kill the Worker: `kill -9 <pid>` (from terminal 1's banner). Confirm nothing is polling:
   `pgrep -af worker.py` is empty, and the UI shows no Workers on `lab-05`.
3. `python control.py status`. It hangs and then fails — `query failed: CANCELLED: Timeout expired`
   after the 10 s client timeout (we also saw `FAILED_PRECONDITION` once, when a Workflow Task was
   already sitting in the queue). A Query is a question to a Worker; with none alive there is no
   answer, and nothing about the Query is recorded.
4. `python control.py resume`. It returns immediately: the Service recorded the Signal and needs no
   Worker to do so. `python control.py history` now ends like this:

   ```text
     31  WORKFLOW_EXECUTION_SIGNALED            resume
     32  WORKFLOW_TASK_SCHEDULED
   ```

   A Workflow Task is scheduled and nobody picks it up. Leave it as long as you like — that gap is
   the "a day later" of the lecture; the run consumes no Worker compute while it waits.
5. Start a Worker again (`python worker.py`). Within a second, `status` answers and `step` climbs.
   History continues from Event 32 with a *different identity*:

   ```text
     33  WORKFLOW_TASK_STARTED                  identity=3776231@lab-host
     34  WORKFLOW_TASK_COMPLETED
     35  ACTIVITY_TASK_SCHEDULED                call_llm
   ```

   In the UI, compare the timestamps of Events 32 and 33: that is exactly how long no process
   existed for this execution. The new Worker replayed Events 1–32 (module 2), rebuilt
   `paused = False` from the `resume` Signal, and issued the next `call_llm` command.

## 5.3 `change_goal` as an Update with a validator

Pause the run first so the loop holds still (`control.py pause`), then:

1. A rejected call: `python control.py change-goal "   " --command-id cmd-9f20`

   ```text
   send 1: rejected: ApplicationError: ValueError: goal must not be empty
   ```

   The validator raised; the client got `WorkflowUpdateFailedError` with the `ValueError` as its
   cause — an error in the caller, not a log line on the Worker. Run `history | wc -l` before and
   after: the count is unchanged (19 → 19 in our run). A rejected Update leaves no Event, not even
   the Workflow Task that ran the validator.
2. An accepted call, sent twice to simulate a client that lost the response and retried:
   `python control.py change-goal "write the tests first" --command-id cmd-9f1e --twice`

   ```text
   send 1: accepted; handler returned 'write the tests first'
   send 2: accepted; handler returned 'write the tests first'
   status: {'goal': 'write the tests first', 'notes': ['Operator: goal changed to: write the tests first'], ...}
   ```

   Both deliveries were *accepted* — each got its own Update ID, so history shows two
   `WORKFLOW_EXECUTION_UPDATE_ACCEPTED` / `..._COMPLETED` pairs — but the handler found
   `cmd-9f1e` in `processed_command_ids` the second time and returned the current goal without
   writing again: one `goal changed to` note.
3. The Service's own dedupe, for comparison: add `--reuse-update-id`, which uses `command_id` as the
   Temporal Update ID too. Now history shows **one** accepted/completed pair
   (`update_id=cmd-9f1e`) for the two sends: the Service answered the retry from the first
   result without running the handler. That dedupe holds only within one run (module 6) and only
   when the client reuses the ID; `processed_command_ids` is what covers the rest.

## 5.4 Retry a Signal; watch it dedupe

`python control.py inject "Prefer sources after 2025" --command-id cmd-7f3a --twice`

```text
send 1: inject_context('Prefer sources after 2025', 'cmd-7f3a') recorded
send 2: inject_context('Prefer sources after 2025', 'cmd-7f3a') recorded
status: {..., 'notes': [..., 'Operator: Prefer sources after 2025'], 'processed_command_ids': ['cmd-7f3a', 'cmd-9f1e'], ...}
```

Both sends were recorded — `history` shows two `WORKFLOW_EXECUTION_SIGNALED inject_context`
Events, each followed by its own Workflow Task — and the note appears once. A Signal has no
identity beyond its payload, so the Service cannot do this for you; the `command_id` in Workflow
state can, and because it *is* Workflow state a Worker that replays this history makes the same
decision. Resume the run and read the result the starter prints: the note is there exactly once.

## 5.5 Handlers interleave: two Updates, a pause, and a continue-as-new

`enrich_context(text, command_id)` is an Update whose handler awaits an Activity (`expand_note`,
~6 s): the model expands an operator's note before it is folded in. While it awaits, the handler
has yielded — the main loop, a `pause`, a second `enrich_context` and a continue-as-new can all
happen underneath it. `request_continue_as_new` is a Signal that asks the *main loop* to checkpoint
into a fresh run (module 6 does this properly; here it exists so you can end a run under a live
handler). Note that the handler never calls `continue_as_new` itself — it sets a flag and the loop
acts on it at the top of the next iteration.

`control.py interleave` sends the whole scenario back to back: two `enrich_context` Updates
(waiting only for *accepted*, not for the result), a `pause`, and a continue-as-new request. Then
it waits for the two Update results and reads the new run.

1. Reproduce the bug. Start a fresh run and send the scenario with draining switched off:

   ```bash
   python starter.py --no-wait
   python control.py interleave --no-drain
   ```

   ```text
   update 1: enrich_context('Prefer primary sources') accepted; handler now awaiting expand_note
   update 2: enrich_context('Cite line numbers') accepted; handler now awaiting expand_note
   pause signalled
   request_continue_as_new(drain=False) signalled
   update 'Prefer primary sources': LOST — WorkflowUpdateFailedError: AcceptedUpdateCompletedWorkflow: Workflow Update failed because the Workflow completed before the Update completed.
   update 'Cite line numbers': LOST — WorkflowUpdateFailedError: AcceptedUpdateCompletedWorkflow: Workflow Update failed because the Workflow completed before the Update completed.
   old run 01a091a0 closed as CONTINUED_AS_NEW; latest run 947a4ce2 is RUNNING
   status of the new run: {'goal': ..., 'notes': [], 'paused': True, 'processed_command_ids': [], 'status': 'running', 'step': 2}
   ```

   Read what happened. Both Updates were *accepted* — `history` on the old run shows two
   `WORKFLOW_EXECUTION_UPDATE_ACCEPTED enrich_context` Events, then the `pause` Signal, then
   `request_continue_as_new`, then `WORKFLOW_EXECUTION_CONTINUED_AS_NEW` and nothing else: no
   `..._UPDATE_COMPLETED`. The first handler was awaiting `expand_note` and the second was waiting
   on the lock when the loop continued-as-new underneath them. Both callers got an error, and the
   new run has no notes and an **empty `processed_command_ids`** — so a client that retries those
   two Updates with the same `command_id` will not be deduplicated either; the ids died with the
   handlers. The Worker's log says the same thing:
   `UnfinishedUpdateHandlersWarning: [TMPRL1102] Workflow finished while update handlers are still
   running`, followed by `Activity not found on completion` when `expand_note` finished ~6 s later
   and tried to report to a run that no longer existed. The Activity ran to completion; its result
   was thrown away.

2. Fix it. The fix is one line in the main loop, before `continue_as_new` (and before any
   `return`): `await workflow.wait_condition(workflow.all_handlers_finished)`. It is already in
   `workflows.py`, guarded by the flag the Signal sets. Start a fresh run and send the scenario
   with draining on:

   ```bash
   python starter.py --no-wait --id agent-42-drain
   python control.py interleave --id agent-42-drain
   ```

   ```text
   update 'Prefer primary sources': handler returned 'Prefer primary sources (expanded)'
   update 'Cite line numbers': handler returned 'Cite line numbers (expanded)'
   old run 01a091a0 closed as CONTINUED_AS_NEW; latest run b0e4237e is RUNNING
   status of the new run: {..., 'notes': ['Operator: Prefer primary sources (expanded)', 'Operator: Cite line numbers (expanded)'],
                           'paused': True, 'processed_command_ids': ['cmd-01a0-1', 'cmd-01a0-2'], 'status': 'running', 'step': 2}
   ```

   The loop saw the continue-as-new request, waited about twelve seconds for both handlers (the
   lock serialized them, so the second `expand_note` started when the first finished), and only
   then continued. Both callers got their result; the new run carries both notes and both ids in
   its snapshot — the `WORKFLOW_EXECUTION_STARTED` Event of the new run has them as `run()`'s
   second argument. The pause carried over too (`'paused': True`); `python control.py resume` lets
   it finish.

3. Why the lock. Read `enrich_context`: the dedupe check happens *before* the await and the id is
   recorded *after* it. Without `async with self.lock`, two deliveries of the same `command_id`
   arriving together would both pass the check, both await, and both write — the `command_id`
   discipline of 5.4 only holds when the check-await-write sequence cannot interleave with itself.
   The lock serializes the handlers with each other; it does not serialize them with the main
   loop, which is why the loop drains handlers explicitly rather than taking the lock.

Three rules, then: handlers interleave with the loop and with each other; the main method drains
them before it returns or continues-as-new; nothing calls `continue_as_new` from a handler.

## The recurring question

If every Worker process dies right now, what survives? Every Signal and every accepted Update, as
Events; `processed_command_ids`, because replay re-applies those handlers in order. What does not:
Query answers, and rejected Updates — nothing was written. When a Worker comes back it replays to
the `wait_condition` it was blocked on and continues; a `resume` sent during the gap is already
waiting in history. A handler that was awaiting an Activity replays into the same await — a Worker
death does not lose it; only ending the run underneath it does (5.5).

## Check yourself

`python -m pytest tests/ -q` runs the same five claims against the time-skipping test server: the
pause holds across ten skipped minutes, the rejected Update leaves no Update Event, the retried
Update returns the same goal without a second write, the retried Signal is recorded twice and
applied once, and a continue-as-new under a live handler loses it without the drain and keeps it
with the drain.

## What you learned

- Signals are recorded by the Service even when the Worker is absent; Queries need a Worker and do not add history.
- An Update validator can reject a request before it is recorded, while a handler can deduplicate a retried command ID.
- A handler still awaiting an Activity must be drained before continue-as-new closes its run.

## Questions to answer

1. Which control operation still succeeds with every Worker stopped, and where is it recorded?
2. Why does `status` time out without a Worker while `resume` returns?
3. What appears in history for a rejected `change_goal` Update?
4. What state must be carried across continue-as-new so retried commands remain idempotent?
