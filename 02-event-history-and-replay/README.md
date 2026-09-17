# Lab 2 — Event History and Replay

Goal: read the real history of lab 1's `AgentRun` event by event, predict what the Service will
append next, hand the execution from one Worker to another and diff what each one saw, then replay
a history by hand and check yourself against the SDK.


**Browser route (Option A):** Open [this lab](http://127.0.0.1:3000/lab/02-event-history-and-replay), click **Prepare this lab**, then use its action cards. Launches return immediately; **Output** shows progress and results. For a variation below, paste one `python ...` command into **Run another command**. The terminal blocks remain available for Option B.

The program is unchanged from lab 1 (`plan()` → `call_llm` → 20 s timer → finish). Two
instruments were added to `workflows.py`: a `CALL_LLM_INVOCATIONS` counter inside the Activity,
and `workflow.logger` lines before every `await` — the SDK suppresses those during replay, so the
first one a Worker prints is the first Command it actually issued. Reading page: "Event History
and Replay".

One model underlies every step below. Keep it in view:

```text
   WORKER                                          SERVICE
   Workflow code
        |
   execute_activity()  --- ScheduleActivityTask -->  ActivityTaskScheduled
                                                     (the Activity runs on a Worker)
                                                     ActivityTaskCompleted
        |                                                   |
   Workflow code  <------ next Workflow Task --------------- +
        |
      REPLAY: run the same code again from line one
        |
   execute_activity()  --- same Command? --> yes: consume the recorded result, call nothing
                                         \-> no:  history is exhausted; issue the Command for real
```

```bash
export TASK_QUEUE=lab-02          # every terminal; keeps this lab's Workers off other labs' queues
python worker.py                  # terminal 1
python starter.py                 # terminal 2: starts agent-42, waits, prints the history
python history.py agent-42        # terminal 3: the same history with the attributes you need
```

Every statement below about what history shows was read off the dev server with SDK 1.32.0.

## 2.1 Annotate the real history

1. Run the Worker and the starter. When `starter.py` returns, run `python history.py agent-42`.
   You get one row per Event with the attributes that matter: which queue a Workflow Task was
   scheduled on (`[normal]` or `[sticky]`), which Worker identity started it, the Activity's
   input and result, the timer's duration.
2. For every Event write the Command or external cause, in the shape of the reading's table
   ("Client: start_workflow", "Command ScheduleActivityTask", "Service clock", …). The reading
   gives the answer for the 16-event shape; do it without looking, then compare.
3. Check three things against the reading:
   - Events 8 and 13 are scheduled on a **sticky** queue named `<worker identity>-<hex>`; in this
     16-event run only event 2 uses the shared queue `lab-02`. That is sticky execution in the log.
     (A crashed Worker adds a second `lab-02` scheduling; that history is in the Advanced section.)
   - Every `worker=` identity is `<pid>@<host>` — the same process for all of 1–11 in a run
     with one Worker.
   - `ActivityTaskStarted` (6) carries `attempt=1` and `scheduled_event_id=5`. The Worker began
     running the Activity when the Service dispatched the Task; the *Event* is written only when the
     attempt reaches a terminal outcome, so history does not gain a row per retry. While an Activity
     is in flight and retrying, `ActivityTaskScheduled` is the only Activity Event you will see
     (lab 4 watches one retry).
4. The invocation counter. Worker 1's log shows `call_llm ... (invocation #1 in pid …)` exactly
   once. Now run `python replay.py agent-42`: it replays the same history in-process with the
   SDK's `Replayer` and prints `CALL_LLM_INVOCATIONS before = 0 … after = 0` and `REPLAY CLEAN`.
   The Activity body did not run; its recorded result (event 7) was handed to the code.
5. Count. `python starter.py --id agent-42-steps --steps 3 --sleep 2`, then
   `python history.py agent-42-steps | wc -l`: **38 events**. Four to start (1–4), eleven per
   step (three for the Activity, three for the Workflow Task that delivers its result, two for
   the timer, three for the Workflow Task that wakes after it), one to complete. Now compute
   the step at which an agent making one model call and one tool call per step reaches the
   10,240-event warning (as of 2026-09). The arithmetic matters less than the rule behind it:
   **history grows with orchestration interactions — Activities, timers, signals, Workflow Tasks —
   not with the lines of Python you execute.** A pure loop over a list costs nothing; one tool call
   per iteration costs about eleven Events. That rule is what makes module 6's Continue-As-New
   necessary.

## 2.2 Predict the next three Events

1. `python starter.py --no-wait`, then within the 20 s: `python history.py agent-42`. The last
   row is `11  TIMER_STARTED  fires_after=0:00:20`.
2. Write down Events 12, 13 and 14 — type, and who causes each (the Service clock; the Service;
   which Worker).
3. After the timer: `python history.py agent-42` again. Observed with the one Worker still
   running: `12 TIMER_FIRED`, `13 WORKFLOW_TASK_SCHEDULED queue=…[sticky]`,
   `14 WORKFLOW_TASK_STARTED worker=<the same pid>`, then `15 WORKFLOW_TASK_COMPLETED`,
   `16 WORKFLOW_EXECUTION_COMPLETED` whose result payload is the same bytes as event 7's result,
   because `run()` returns `self.context_summary`.
4. Note what 12 and 13 have in common: no Worker identity anywhere. The Service appended both.

## 2.3 Two-Worker handoff, and the diff

1. Terminal 1: `python worker.py` — note the pid. Terminal 2: `python starter.py --no-wait`.
2. Within the 20 s, save what Worker 1 saw and crash it:
   `python history.py agent-42 > before.txt`, then `kill -9 <pid>`.
3. Wait until the Web UI shows `WorkflowTaskTimedOut` (about 30 s after the kill: the timer
   fires at 20 s, the stale Task times out 10 s later), then terminal 1: `python worker.py` —
   a new pid, so a new identity.
4. When it completes: `python history.py agent-42 > after.txt` and `diff before.txt after.txt`.
   The diff is only additions after event 11: the prefix is byte-identical, the continuation
   is appended. Nothing was rewritten — history is append-only.
5. Read Worker 2's terminal. It printed exactly one `live:` line —
   `live: returning -> Command CompleteWorkflowExecution` — and **no** `call_llm` line. It
   replayed 1–15 silently (the logger is muted while replaying), the `await`s on the Activity and
   the timer resolved from history, and the first thing it did for real was `return`.

   Say precisely what that means. During Worker 2's replay, which of these ran?

   ```text
   A. neither await workflow.execute_activity(...) nor the call_llm function
   B. await workflow.execute_activity(...) only
   C. call_llm only
   D. both
   ```

   **B.** Worker 2 executed the Workflow line `await workflow.execute_activity(call_llm, ...)`
   again — it had to, to rebuild `self.context_summary` — and the SDK matched the Command that line
   produced against `ActivityTaskScheduled` (5), then handed back the result recorded in
   `ActivityTaskCompleted` (7). The `call_llm` body never ran; the counter proves it. "Replay skips
   the Activity line" is the wrong sentence: replay re-runs the *call* and reuses the *outcome*.

## 2.4 Hand-replay

`histories/agent-42.json` is a real history, captured with the Worker kill above. Cut it after
event 13 — the `WorkflowTaskScheduled` that follows `TimerFired` — and play Worker 2:

1. `python history.py --file histories/agent-42.json --first 13`. With `workflows.py` open, walk
   `run()` from line one. For each `await`, name the Command the SDK builds and the Events it
   matches it against. Write down which `await` (or `return`) is the first with no Event to
   match — and therefore the first Command Worker 2 issues for real.
2. Check: `python replay.py --file histories/agent-42.json --first 13`. (Cutting a history is a
   teaching trick, so you can watch exactly where replay stops. In production you run `Replayer`
   over *complete* histories, to check that new code is still compatible with them — that is lab 8.) The only `live:` line
   printed is the answer. `--first 11` prints nothing live: the code is blocked on a timer that
   has not fired in this history, so there is no Command to issue.
3. Variation: the 38-event three-step history, cut after event 24 (the `WorkflowTaskScheduled`
   after the second `TimerFired`). Which step number does `plan()` see, and what does it
   schedule? `python history.py agent-42-steps --first 24 --json cut.json`, then
   `python replay.py --file cut.json`.
4. One paragraph, replacing your lab 1 paragraph: where *was* the program counter stored? Use
   the words Command, Event, Workflow Task and replay, and say which lines of `run()` ran on
   Worker 2 and which only appeared to.

## Check yourself

`PYTHONPATH=.. pytest tests/` runs the same program on Temporal's time-skipping test server and
asserts the 16-event shape, the 11-events-per-step arithmetic, and that a replay — full or cut at
event 13 — leaves `CALL_LLM_INVOCATIONS` untouched.

## Done when
Answer these without looking anything up. They are the whole of modules 2 and 3.

1. What is the relationship between a Command and an Event, and who writes each?
2. What exactly re-executes during replay, and what does not?
3. Why does `call_llm` not run again, even though the line that calls it does?
4. Where does the `await` get its old return value from?
5. How does the Worker know replay has caught up with the present?
6. Why does any of this force Workflow code to be deterministic? (That is lab 3.)

## Advanced: sticky execution and Workflow Task failures
Optional, and nothing here changes the model above. Sticky execution is a caching optimization: it
decides *whether a replay was needed for a given Workflow Task*, never whether the outcome is
correct. The defaults below are SDK implementation details and may change.

**The sticky-to-shared footprint.** This is the detail the reading told you to read off the real
thing. With no Worker polling when the timer fired, `histories/agent-42-sticky-timeout.json` (18 events)
shows:

```text
12  +20.18s  TIMER_FIRED
13  +20.18s  WORKFLOW_TASK_SCHEDULED   queue=<worker1 identity>-<hex> [sticky]
14  +30.18s  WORKFLOW_TASK_TIMED_OUT   timeout_type=SCHEDULE_TO_START
15  +30.18s  WORKFLOW_TASK_SCHEDULED   queue=lab-02 [normal]
16  +34.62s  WORKFLOW_TASK_STARTED     worker=<worker2 identity>
```
The Task was offered on the dead Worker's sticky queue, nobody took it, and 10.0 s later
it was timed out and re-offered on the shared queue. That 10 s is the Python Worker's
`sticky_queue_schedule_to_start_timeout` default — not the 5 s the reading quotes from
upstream; subtract event 13's time from event 14's yourself.

You will not always see the pair. `histories/agent-42.json` (16 events) is the same
experiment with Worker 2 started 2 s *after* the timer fired: it took the Task 2.5 s after
event 13, straight off the sticky-scheduled Task, with no timed-out Event. In a third run
where the timer fired only ~6 s after the kill and a new Worker was already polling, the
Task still waited out the 10 s and produced the pair. Upstream documents the reschedule, not its
footprint.

*Experiment note, not documentation:* our reading of those three runs is that the Service
bypasses a sticky queue whose Worker has been silent long enough and honours it otherwise. It is
a hypothesis to test. The Events above are the facts, and neither the hypothesis nor its answer
changes anything about replay.

**Stalling a Worker inside a Workflow Task.** Instead of killing Worker 1, stall it: add a
`time.sleep(15)` under
`workflow.unsafe.sandbox_unrestricted()` before the first `await`. You will not reach the
10 s Workflow Task Timeout the reading describes: the SDK gets there first. Observed, 2.08 s
after `WorkflowTaskStarted`:
`WORKFLOW_TASK_FAILED cause=WORKFLOW_WORKER_UNHANDLED_FAILURE message="[TMPRL1101] Potential
deadlock detected: workflow didn't yield within 2 second(s)."` — the Python SDK's deadlock
detector fails the Task itself (the Service then retries it, off-history, forever, since
every attempt stalls the same way). Producing `WorkflowTaskTimedOut START_TO_CLOSE` by hand
needs the process to die in the milliseconds between `WorkflowTaskStarted` and
`WorkflowTaskCompleted`; we did not manage it.

Blocking calls inside Workflow code are lab 3's subject. There they are the determinism rule; here
they are a footnote.

## What you learned

- The Event History records commands and results; replay follows it until the Worker reaches new work.
- A Worker handoff may add Workflow Task timeout and reschedule Events without repeating a completed Activity.
- Event IDs, task identities, and Activity attempts let you explain exactly what ran before and after a crash.

## Questions to answer

1. Which Event contains the model answer that the second Worker receives during replay?
2. What history change tells you a timer fired while no Worker was polling?
3. Why can two Workers run the same Workflow code without making two model calls?
4. What is the difference between a Workflow Task retry and an Activity retry in the history?
