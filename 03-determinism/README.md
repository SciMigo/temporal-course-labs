# Lab 3 — Determinism

Goal: break replay three ways — randomness, the wall clock, a network read — force a replay after
each, read the error the SDK gives you, and fix it. By the end you can read a
`NondeterminismError`, name the event id where history and code disagreed, and point at the
`await` that produced the mismatching Command.


`workflows.py` is lab 2's `AgentRun` plus the two things a real agent is tempted to do in Workflow
code: consult the clock in `plan()` (a time budget) and consult the model in `plan()` (the next
step). The environment variable `LAB03_BREAK` selects which *build* a Worker runs — `none`
(the fixed program), `random`, `clock`, `network`, and the three `-naive` variants. It is an
environment variable and not a Workflow argument on purpose: a fix is a redeploy, and the same
history has to replay under the fixed build. Reading page: "Determinism".

```bash
export TASK_QUEUE=lab-03
LAB03_BREAK=random python worker.py     # terminal 1: the build under test
python starter.py --id coinflip-1       # terminal 2: 15 s timer after each step, 5 s time budget
python replay.py coinflip-1 --times 8   # replay the *current process's* build against the history
python failures.py coinflip-1           # WorkflowTaskFailed events, and the Task the Service is still retrying
```

Two ways to force a replay, both used below: `kill -9` the Worker during the 15 s timer and start
another (the next Workflow Task is replayed from event 1), or `replay.py`, which runs the SDK's
`Replayer` over the fetched history with no Worker and no Service involvement.

Every error line quoted below was produced on the dev server with SDK 1.32.0.

## 3.1 Randomness

1. The naive version first. `LAB03_BREAK=random-naive python worker.py`, then
   `python starter.py --id coinflip-naive --no-wait`. The run never gets past its first
   Workflow Task. `python failures.py coinflip-naive` shows:

   ```text
     4  WORKFLOW_TASK_FAILED  cause=WORKFLOW_WORKER_UNHANDLED_FAILURE  worker=<pid>@<host>
        Cannot access random.random.__call__ from inside a workflow. If this is code from a module
        not used in a workflow or known to only be used deterministically from a workflow, mark the
        import as pass through.
     5  WORKFLOW_TASK_SCHEDULED  attempt=2  (earlier attempts failed without an Event)
   status: RUNNING
   pending Workflow Task: attempt=2 ...
   ```
   That is the sandbox, not replay: `RestrictedWorkflowAccessError` before there is any history
   to disagree with. Note what the Service did with it — a Workflow Task failure, retried with
   backoff, the execution still `RUNNING`. Nothing is lost; the Worker is simply wrong. Stop the
   Worker; terminate `coinflip-naive` in the UI (or leave it — it costs no Worker compute).
2. Get past the sandbox. `LAB03_BREAK=random` uses `random.Random().random()` — constructing a
   `Random` is allowed so seeded generators keep working, and an *unseeded* one is seeded from
   the OS. That fairly stands in for a third-party library the sandbox cannot see into. Run it:
   `LAB03_BREAK=random python worker.py`, `python starter.py --id coinflip-1`. It completes (one
   Worker, no replay, nothing to disagree with).
3. Force replays: `LAB03_BREAK=random python replay.py coinflip-1 --times 8`. Each replay is a
   fresh coin. We got three clean replays and then:

   ```text
   replay 4: NondeterminismError
     [TMPRL1100] Nondeterminism error: Activity machine does not handle this event: HistoryEvent(id: 5, TimerStarted)
     -> the code's Command did not match Event 5 (TimerStarted). ...
   ```
   Read it: the code (this replay's coin said "no jitter") proposed `ScheduleActivityTask`; at
   position 5 history has `TimerStarted` (the original coin said "jitter"). The Activity state
   machine was handed a timer Event it cannot accept. If the original run drew "no jitter" you
   will see the mirror image — `Timer machine does not handle this event: HistoryEvent(id: 5,
   ActivityTaskScheduled)`. Open event 5 in the UI; the `await` that produced the Command is
   `await asyncio.sleep(1)` or `execute_activity(call_llm)`, one line apart.
4. Count how many replays passed by luck. That number is why this bug ships: the code is wrong
   on every run and fails on only some replays.
5. The kill route, to see what the Service does when a real Worker hits this:
   `python starter.py --id coinflip-2 --no-wait`, `kill -9` the Worker during the timer, start
   it again (still `LAB03_BREAK=random`). The Workflow Task after `TimerFired` replays from
   event 1 with a new coin. Half the time the coin agrees and the run completes with no trace;
   half the time `failures.py coinflip-2` shows `WORKFLOW_TASK_FAILED
   cause=NON_DETERMINISTIC_ERROR` — and then the Service *retries the Workflow Task*, the retry
   draws again, and the run completes anyway a few seconds later. A retry that passes by luck is
   still a bug.
6. Fix: `workflow.random().random()` — a `random.Random` seeded from history, so every replay
   draws the original coin. That is `LAB03_BREAK=none`. `python replay.py coinflip-1 --times 8`
   with the fixed build may *still* fail: the fixed coin need not equal the unseeded coin the
   broken run drew. A code change cannot repair a history the old code produced — that is
   module 9's problem. Start a fresh run under the fixed build; it replays clean every time.

## 3.2 The wall clock

`plan()` finishes the run once it is older than its time budget (5 s by default).

1. `LAB03_BREAK=clock-naive` (`datetime.now()` in `plan()`): the same sandbox failure —
   `Cannot access datetime.datetime.now.__call__ from inside a workflow ...` at event 4,
   retried forever. Stop it.
2. `LAB03_BREAK=clock` reads the wall clock under `workflow.unsafe.sandbox_unrestricted()`.
   `LAB03_BREAK=clock python worker.py`, `python starter.py --id clock-1`. Step 0 runs at
   age 0 s, well inside the budget, so `plan()` schedules `call_llm`; after the 15 s timer step 1
   is over budget and the run finishes. 16 events, looks fine.
3. `LAB03_BREAK=clock python replay.py clock-1`. Unlike the coin, this fails on **every**
   replay, because every replay happens later than the original run:

   ```text
   replay 1: NondeterminismError
     [TMPRL1100] Nondeterminism error: Complete workflow machine does not handle this event: HistoryEvent(id: 5, ActivityTaskScheduled)
   ```
   On replay `plan()` at step 0 sees a run that is already minutes old, returns `finish`, and the
   code proposes `CompleteWorkflowExecution`; history has `call_llm` being scheduled at 5.
4. Now the kill route, and watch a production incident in miniature.
   `python starter.py --id clock-2 --no-wait`; `kill -9` the Worker during the timer; start it
   again with `LAB03_BREAK=clock`. It never recovers: `python failures.py clock-2` shows one
   `WORKFLOW_TASK_FAILED cause=NON_DETERMINISTIC_ERROR` with the message above, followed by a
   `WORKFLOW_TASK_SCHEDULED` whose `attempt=` keeps climbing (we watched it reach 4) while the
   status stays `RUNNING`. Only the first failure is written to history; the later attempts are
   tracked in the Service's mutable state, and the pending Workflow Task's attempt count is
   where you read them.
5. The fix is a redeploy: stop the broken Worker, start `python worker.py` (no `LAB03_BREAK`).
   Within seconds the run completes — `failures.py clock-2` now ends in
   `WORKFLOW_EXECUTION_COMPLETED`, and the retried `WORKFLOW_TASK_SCHEDULED` reads the attempt
   that finally succeeded (5, in our run). The fixed `plan()` uses `workflow.now()`, the time of
   the Workflow Task being replayed: at step 0 it is the start time again, the run is 0 s old,
   and the code proposes exactly what history holds. Same history, fixed code, clean replay —
   `python replay.py clock-2` confirms it. Nothing was restarted from scratch; the execution
   simply continued once a correct Worker existed.

## 3.3 A network read in `plan()`

The model is a separate process, `model_server.py`: it answers `next: continue` the first time it
is asked and `next: finish` every time after, and prints a line per request. Terminal 3:
`python model_server.py`.

1. `LAB03_BREAK=network-naive` (`urllib.request.urlopen()` in `plan()`): the sandbox again —
   `Cannot access urllib.request.urlopen.__call__ from inside a workflow ...`. Stop it.
2. `LAB03_BREAK=network python worker.py`, `python starter.py --id net-1 --budget 300`. `plan()`
   asks the model at step 0 (`model call #1 -> 'next: continue'`), schedules `call_llm`, waits
   15 s, asks again at step 1 (`model call #2 -> 'next: finish'`) and finishes. Two model calls,
   two lines in terminal 3.
3. `LAB03_BREAK=network python replay.py net-1`. Terminal 3 prints
   `model call #3: 'Step 0 for: …' -> 'next: finish'` — the replay paid for a model call the
   original run already made — and the answer is different, so `plan()` at step 0 now says
   `finish`:

   ```text
   [TMPRL1100] Nondeterminism error: Complete workflow machine does not handle this event: HistoryEvent(id: 5, TimerStarted)
   ```
   (`TimerStarted` if the run's jitter coin came up, `ActivityTaskScheduled` if not — either
   way the code proposed `CompleteWorkflowExecution` where history has a Command from an `llm`
   step.) Even when a live model happens to answer the same thing twice, you have paid twice;
   when it does not, the run is stuck exactly like `clock-2`: on the kill route
   (`python starter.py --id net-2 --no-wait --budget 300`, `kill -9` during the timer, restart
   with `LAB03_BREAK=network`) `failures.py net-2` shows `WORKFLOW_TASK_FAILED
   cause=NON_DETERMINISTIC_ERROR` and terminal 3 prints a new `model call` for **every** retry
   of the Workflow Task — a stuck run that keeps billing. Start
   `LAB03_MODEL_URL=http://127.0.0.1:8765 python worker.py` (the fixed build) and it completes:
   the recorded `call_llm` answer already says `next: finish`, so the fixed `plan()` makes the
   decisions history holds.
4. Move the read into the Activity. `LAB03_MODEL_URL=http://127.0.0.1:8765 python worker.py`
   (no `LAB03_BREAK`): `call_llm` asks the model, and `plan()` decides from
   `self.context_summary` — the recorded answer. `python starter.py --id net-fixed --budget 300
   --sleep 5` runs two steps: `call_llm` twice (`next: continue`, then `next: finish`), then
   finish. `python replay.py net-fixed --times 3`: three clean replays and terminal 3 prints
   nothing. Both answers are in history as `ActivityTaskCompleted` results; the replay hands
   them to `plan()`, which makes the same two decisions.
5. One sentence, written down: why does the recorded answer suffice? (The reading's version:
   the model's answer is an effect; what `plan()` does with it is a decision.)

## Check yourself

`PYTHONPATH=.. pytest tests/` runs the fixed build and asserts clean replays; runs the `clock`
build and asserts the replay fails at event 5 once the budget has passed, then replays clean
under the fixed build; and runs the `network` build against an in-process model server, asserting
the replay costs a third model call and fails, while the fixed build's replay costs none.

## What you learned

- Replay requires the same Workflow commands in the same order for the same history.
- Reading randomness, wall time, or a network result in Workflow decisions can change those commands.
- Move effects into Activities and replay old histories before deploying changed Workflow code.

## Questions to answer

1. At which recorded Event does the broken clock build first disagree with its history?
2. Why can a network read in `plan()` create an extra model call during replay?
3. Which value should be recorded as an Activity result, and which decision stays in Workflow code?
4. Why does starting a fixed Worker recover a stuck run without restarting its Workflow ID?
