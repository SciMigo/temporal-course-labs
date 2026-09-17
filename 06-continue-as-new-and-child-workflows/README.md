# Lab 6 — Continue-As-New and Child Workflows

Goal: keep AgentRun's history bounded without losing its identity, and delegate a research task to
a Child Workflow that has a history — and a Worker — of its own.


Same program as lab 5 plus two things in `workflows.py`: the continue-as-new check at the top of
the loop, with `AgentState` as the checkpoint, and a `research` step that `plan()` hands to
`ResearchAgent`. Reading page: "Continue-As-New and Child Workflows".

## Setup

```bash
cd labs/06-continue-as-new-and-child-workflows
export TASK_QUEUE=lab-06
python worker.py --role agent        # terminal 1: AgentRun + call_llm on lab-06
python worker.py --role research     # terminal 2: ResearchAgent + fetch_source on lab-06-research
python starter.py                    # terminal 3: starts agent-42, sends one note, prints the run chain
```

`python worker.py` with no `--role` runs both in one process, which is fine for 6.1. The child
runs on its own Task Queue only so that "the child's Worker" is a process you can kill in 6.2 —
by default a child inherits its parent's queue, and splitting queues on purpose is module 10.
Use `--id`/`WORKFLOW_ID` if `agent-42` is taken on the shared server. A full run is 20 steps and
takes about 30 s including the child.

## 6.1 Continue-as-new: one Workflow ID, several Run IDs

`STEPS_PER_RUN = 4` is a lab knob so the transition happens in a 20-step run; the check reads
`workflow.info().is_continue_as_new_suggested() or self.steps_this_run >= STEPS_PER_RUN`, and the
suggestion is the rule your production code keeps. Run the starter and read its output:

```text
started agent-42 first run=01a08fac-...; open http://localhost:8233/namespaces/default/workflows/agent-42
inject_context sent; it landed in run=01a08fac-... at step 1
result:
[model answer to: Step 0 toward: summarize the Temporal docs]
[model answer to: Step 1 toward: summarize the Temporal docs]
Operator: Prefer sources after 2025
...
3 sources on sources for: summarize the Temporal docs
...
[model answer to: Step 19 toward: summarize the Temporal docs]

final status(): {..., 'run_id': 'f268e931-...', 'status': 'done', 'step': 20, 'steps_this_run': 4, 'processed_command_ids': ['cmd-7f3a']}

run chain (one Workflow ID, several Run IDs):
  run 1: 01a08fac-...  CONTINUED_AS_NEW   53 events  last=WORKFLOW_EXECUTION_CONTINUED_AS_NEW
  run 2: fd3e9a8d-...  CONTINUED_AS_NEW   52 events  last=WORKFLOW_EXECUTION_CONTINUED_AS_NEW
  run 3: a60cb664-...  CONTINUED_AS_NEW   49 events  last=WORKFLOW_EXECUTION_CONTINUED_AS_NEW
  run 4: e14331bb-...  CONTINUED_AS_NEW   49 events  last=WORKFLOW_EXECUTION_CONTINUED_AS_NEW
  run 5: f268e931-...  COMPLETED          49 events  last=WORKFLOW_EXECUTION_COMPLETED
```

What to check, in the UI and in this output:

1. Open `agent-42` in the UI. The Workflow ID page lists five runs; four are Continued-As-New,
   one Completed. Each history is about 50 Events, not 250. `client.list_workflows('WorkflowId =
   "agent-42"')` returns the same five.
2. Open run 1. Its last Event is `WorkflowExecutionContinuedAsNew`, and its attributes carry the
   next Run ID and the `AgentState` payload — open it: `step: 4`, the goal, the summary so far, the
   `processed_command_ids`. Open run 2: Event 1 `WorkflowExecutionStarted` has
   `continued_execution_run_id` = run 1, and its input is that same `AgentState`. Nothing else
   crossed the boundary — no timers, no pending Activities, no local variables.
3. The starter waited on one handle and got the *final* result: `handle.result()` follows the
   chain by default, and `status()` on the same handle answered from whichever run was current
   (the `run_id` in `final status()` is run 5's).
4. The operator note. The starter sent `inject_context` one second in — it landed in run 1 at step 1
   — and the note is in the result that run 5 returned, once. It got there because `snapshot()`
   serializes `context_summary + pending_context` and `processed_command_ids` into `AgentState`.
   Delete `+ self.pending_context` from `snapshot()` and re-run a few times: a note the handler
   accepted right before a transition is gone, and nothing in history complains.
5. The drain. `await workflow.wait_condition(workflow.all_handlers_finished)` sits right before
   `continue_as_new`. Our handlers are all `def`, so it returns at once; make `inject_context`
   an `async def` that awaits a 2 s timer and the Worker warns (or the caller of an Update gets
   `NOT_FOUND`) the moment you take the drain out.

## 6.1b What is still unbounded?

Continue-as-new bounded the *history*. Look at what it did not bound. Every run passes `AgentState`
to the next one, and two of its fields only ever grow:

```text
history          bounded by continue-as-new      ✅
context_summary  appended to, run after run      ❌
processed_command_ids  one entry per command     ❌
```

The payload of the next run is an input like any other: it is stored in that run's
`WorkflowExecutionStarted` Event, replayed on every Workflow Task, and counted against the same size
limits module 2 measured. An agent that runs for a month ships its whole past into every new run.

1. Run with `--steps 60` and read the `AgentState` input on the last run's Event 1. Compare its size
   with run 1's. Extrapolate to a month.
2. Say what a production `AgentRun` would carry instead. The shapes that work: a rolling or
   hierarchical summary that is rewritten rather than appended to; a *window* of recent command IDs
   with the older ones moved to an external dedupe table keyed by `(workflow_id, command_id)`; blob
   references instead of transcripts. The rule: what crosses a continue-as-new boundary should be
   the state the next run needs to decide, not the record of how it got here.

## 6.2 `ResearchAgent`: kill the child's Worker

`plan()` returns `research` at step 6 (run 2). AgentRun starts `ResearchAgent` with the ID
`agent-42/research/1` and awaits it; the child reads three sources with a 3 s durable timer after
each, so it lives for about 10 s. With both Workers running:

1. Start a fresh run. When `agent-42/research/1` appears in the UI as Running (about 6 s in), `kill
   -9` the **research** Worker (terminal 2). Leave the agent Worker alone.
2. Both executions stay **Running**. The parent has nothing to do — it is waiting on a single
   pending Child Workflow — and its history does not move. The child's timer fires with no Worker
   to act on it. Query the child's `progress` now (`client.get_workflow_handle("agent-42/research/1").query("progress")`):
   it times out — nobody can answer.
3. Start the research Worker again. The child finishes on the new process. Its history, from the
   run we recorded (the first Worker was pid 3791118, the second 3791435):

   ```text
     11  TIMER_STARTED
     12  TIMER_FIRED                                   08:54:02
     13  WORKFLOW_TASK_SCHEDULED
     14  WORKFLOW_TASK_TIMED_OUT                       08:54:12
     15  WORKFLOW_TASK_SCHEDULED
     16  WORKFLOW_TASK_STARTED      identity=3791435@...
     17  WORKFLOW_TASK_COMPLETED
     18  ACTIVITY_TASK_SCHEDULED    fetch_source
   ```

   The timer fired (12) while no Worker existed; the Workflow Task (13) was first offered to the
   dead Worker's sticky queue and timed out there (14, ten seconds later), then was rescheduled
   (15) and picked up by the new identity (16). The child replayed its own 15 Events and continued
   with source 2.
4. Now the parent. In run 2 of `agent-42`, the child is three Events:

   ```text
     27  START_CHILD_WORKFLOW_EXECUTION_INITIATED   agent-42/research/1
     28  CHILD_WORKFLOW_EXECUTION_STARTED
     32  CHILD_WORKFLOW_EXECUTION_COMPLETED           (20 s later)
   ```

   No `fetch_source`, no child timers, nothing about the crash. The child's 40 Events are the
   child's. That is what earns this delegation a Child Workflow rather than one Activity: not
   length — an Activity can be long, heartbeat and retry too — but a durable lifecycle of its own,
   with its own history, its own state and its own messaging surface, reported to the parent as one
   recorded result.
5. Make the child fail: `python starter.py --goal "summarize the unreliable Temporal docs"`.
   `ResearchAgent.run` raises a non-retryable `ApplicationError` for that goal. The child closes
   **Failed**; the parent's run 2 records `CHILD_WORKFLOW_EXECUTION_FAILED  source registry
   rejected the task`; the `except ChildWorkflowError` branch adds `research 1 failed: source
   registry rejected the task` to the summary, bumps `attempts`, and the run finishes all 20 steps
   — the failure was a value the parent decided about, exactly as an `ActivityError` would be.

Stretch: make the child outlive the parent. Changing `parent_close_policy` alone will not do it —
`await workflow.execute_child_workflow(...)` means the parent cannot finish before the child by
construction. You need the asynchronous form as well:

```python
handle = await workflow.start_child_workflow(   # returns once the child has STARTED
    ResearchAgent.run,
    args=[...],
    id=f"{workflow.info().workflow_id}/research/{self.state.research_n}",
    parent_close_policy=ParentClosePolicy.ABANDON,
)
# do NOT await handle.result(): let the parent go on and finish
```

`start_child_workflow` returns when the child is started, not when it is done, and awaiting that
start matters: the child must reach `ChildWorkflowExecutionStarted` before the parent closes, or
there is nothing for the policy to abandon. Put the research step near the end (step 19) so the
parent finishes first, then watch the child keep running with its parent closed — and under
`TERMINATE`, the default in this lab, watch the same child die with it.

## 6.3 Activity or Child Workflow?

For each delegated task an agent might take, write **Activity** or **Child Workflow** and one line
of justification. Then compare with "Six delegations, sorted" on the reading page — and with the
two choices this lab already made (`call_llm`, `ResearchAgent`).

| Task | Activity or Child? | Why (one line) |
|---|---|---|
| Fetch a URL | | |
| Summarize a document with the model | | |
| Run a test suite in a container (5 min, progress available) | | |
| Research a company across 30 sources with a human review in the middle | | |
| Monitor a deployment for a week after the agent that started it is done | | |
| Send a notification | | |

Two follow-ups: which of the six needs a Parent Close Policy other than the default, and why is
the answer to "the code is getting long, let's split it into a child" still Activity?

## The recurring question

If every Worker process dies right now: the current run's history survives and so do the closed
runs'; the `AgentState` you passed is the new run's first Event, so a returning Worker replays only
the *current* run. A Signal that races a continue-as-new is not lost: Temporal guarantees it is
processed either by the run that is closing or by its continuation. Do not write code that depends on
which — the mechanism (buffering it forward, or rewinding the Workflow Task that tried to continue so
the current run handles the Signal first) is Temporal's business and has changed before.
`ResearchAgent`'s history is its own execution: parent and child replay independently, on whichever
Workers come back, and the parent's replay stops at "waiting for child" until the child reports.

## Check yourself

`python -m pytest tests/ -q` — five runs with short histories and run 2 naming run 1 as its origin,
the note and the dedupe set travelling in `AgentState`, `status()` answering on one handle across
runs, the child's steps in the child's history and one Initiated/Started/Completed triple in the
parent's, and a failing child surfacing as `CHILD_WORKFLOW_EXECUTION_FAILED` that the parent
catches and keeps going after.

## What you learned

- Continue-as-new closes one Run and starts another under the same Workflow ID with an explicit state snapshot.
- A Child Workflow has its own history and can run on a separate Task Queue and Worker.
- The parent records the child's lifecycle; it does not absorb the child's internal Events.

## Questions to answer

1. Which fields must `AgentState` carry into the next Run to preserve behavior and deduplication?
2. How can one Workflow ID have several Run IDs without one unbounded Event History?
3. What changes in the parent history when the Research Worker dies?
4. Why does recovering the parent not require replaying the child's history inside the parent?
