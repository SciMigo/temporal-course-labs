# Lab 11 — Capstone: a durable agent runtime

Goal: assemble the AgentRun you built one capability at a time into one runnable program on
separate lanes; run it for 200 steps and prove no run's history grew; break it every way the
requirements say it will be broken and watch it recover; then defend every box in writing.
Reading page: "Capstone — A Durable Agent Runtime" (module 11 of the course).

Everything is the same names as L01–L10. Nothing here is new; the exam is that it is all in one place.

| File | What it is |
|---|---|
| `agentrun/workflows.py` | `AgentRun` (`plan()`, the loop, Signals, Query, Update, continue-as-new, cancellation, compensations) and `ResearchAgent` |
| `agentrun/activities.py` | `call_llm`, `execute_tool`, `evaluate`, `compensate` — fakes that keep **ledgers by idempotency key** (`LLM_CACHE`, `TOOL_LEDGER`, `TOOL_SIDE_EFFECTS`, `COMPENSATED`) |
| `agentrun/models.py` | `Step`, `ToolCall`, `ToolResult`, `Verdict`, `AgentState` (the continue-as-new snapshot) |
| `agentrun/lanes.py`, `agentrun/search_attributes.py` | `agent-workflows` / `cpu-tools` / `gpu-tools`; `AgentStatus` / `CurrentStep` / `Owner` |
| `worker_workflows.py`, `worker_cpu.py`, `worker_gpu.py` | one process per lane, as in lab 10 |
| `starter.py`, `control.py` | start a run; drive it (`status`, `pause`, `resume`, `cancel-run`, `cancel`, `inject`, `goal`) |
| `setup_search_attributes.py` | same as lab 10; run once per dev server |
| `run_200_steps.py` | 11.2: the long run, the Run ID chain, the history bound |
| `tests/test_failure_injection.py` | 11.3: crash, timeout, duplicate, both cancels, control surface, continue-as-new bound |
| `DEFENSE.md`, `DEFENSE_ANSWERS.md` | 11.4: the template you fill, and the answers |
| `console/` | 11.5: the evaluation console, a product on a Workflow (`EvalRun`, SQLite projection, FastAPI + SSE) |

```bash
cd labs && docker compose up -d && cd 11-capstone-durable-agent-runtime
export LAB_PREFIX=lab11-      # shared dev server only
python setup_search_attributes.py
```

## 11.1 Assemble it, run it, kill it

Read `agentrun/workflows.py` top to bottom before running anything. Check that you can point at
each of these without searching: the decision (`plan()`), the three effects and the lane each names,
the child, the four Signals, the Query, the Update and its validator, the snapshot, the
continue-as-new condition, the two cancellation shapes, the compensation list.

Three pools, one terminal each, then a run:

```bash
python worker_workflows.py     # workflow worker pid=… polling 'lab11-agent-workflows': AgentRun, ResearchAgent; kill -9 … to crash it
python worker_cpu.py           # cpu worker pid=… polling 'lab11-cpu-tools': call_llm, execute_tool, evaluate, compensate
python worker_gpu.py           # gpu worker pid=… polling 'lab11-gpu-tools' with 1 slot(s)
python starter.py --id agent-42 --tier enterprise --max-steps 8
```

`plan()` cycles `llm → search (cpu) → embed (gpu) → research-or-llm`; every fourth cycle the fourth
step is a `ResearchAgent` child. Observed for 8 steps: `status done, steps 8, children_this_run
['lab11-agent-42/research/3']`, Search Attributes `{'AgentStatus': 'done', 'CurrentStep': 8, 'Owner': 'acct_7f3a'}`,
and in the history one `START_CHILD_WORKFLOW_EXECUTION_INITIATED` / `CHILD_WORKFLOW_EXECUTION_STARTED` /
`CHILD_WORKFLOW_EXECUTION_COMPLETED` triple. Open the child in the UI: its id is the parent's id plus
`/research/3`, and it has a history of its own.

**Drive it.** Start a longer one and use `control.py` while it runs (`AGENTRUN_TOOL_SECONDS=1.5` slows the fakes so you have time):

```bash
python starter.py --id agent-42 --max-steps 8 --no-wait
python control.py pause;  python control.py status          # paused True, status paused, step N
python control.py goal "" --command-id c0                    # WorkflowUpdateFailedError: rejected by the validator, never in history
python control.py goal "write the defense document" --command-id c1   # update result: goal changed from … to …
python control.py goal "write the defense document" --command-id c1   # update result: ignored: command c1 already applied
python control.py inject "the user is in a hurry" --command-id c2      # twice: the second is a no-op
python control.py status                                     # goal …, processed_command_ids ['c1', 'c2']
python control.py resume
```

All of the above is what was observed. In the history: `WORKFLOW_EXECUTION_SIGNALED ×4` (pause,
inject, inject, resume), `WORKFLOW_EXECUTION_UPDATE_ACCEPTED ×2` and `…_COMPLETED ×2` — for `c1`
twice; the rejected `c0` left nothing, and the second `c1` was accepted (the validator does not see
the dedupe set; the handler does) and returned `ignored`.

**Kill it.** `kill -9` the *workflow* worker while a run is mid-way, start a new one, and read the
handoff off the history:

```bash
python starter.py --id agent-kill --max-steps 12 --no-wait
python control.py status --id agent-kill      # step 6
kill -9 <workflow worker pid>; python worker_workflows.py
```

Observed (`fetch_history`, Workflow Task events, dev server 1.31.2):

```
first WFT by wf-3821487@lab11-agent-workflows at event 3   09:21:04.190
    73 WORKFLOW_TASK_TIMED_OUT  timeout_type=SCHEDULE_TO_START
first WFT by wf-3821651@lab11-agent-workflows at event 75  09:21:17.704
WFT scheduled 09:21:17.681 (event 74) → started by the new worker +0.0s
result: steps=12 status=done
```

The first Workflow Task after the crash went to the dead Worker's *sticky* queue (the one it used
to keep the Workflow cached), sat there for the sticky Schedule-to-Start timeout (~10 s here), timed
out — that is the `WORKFLOW_TASK_TIMED_OUT` you see — and was rescheduled to the shared
`agent-workflows` queue, where the new Worker took it 23 ms later, replayed 73 events, and continued
from step 6. Every Activity in the history ran exactly once. That ten-second gap is the price of the
cache, and it is the only cost of the crash.

## 11.2 Two hundred steps, one Workflow ID

```bash
python run_200_steps.py --steps 200 --id agent-200        # hosts the three pools in-process; --external-workers to use yours
```

`STEPS_PER_RUN` (env `AGENTRUN_STEPS_PER_RUN`, default 50) is the cadence; `is_continue_as_new_suggested()`
is the other trigger and stays in the code as the rule — the cadence is the lab's way of making the
event visible in 200 steps. Observed (36.8 s, `AGENTRUN_TOOL_SECONDS=0.02`):

```
 run  run_id                               events  closed as
   1  01a08fc4-a309-7a41-b02e-83f518ba7cca    516  CONTINUED_AS_NEW
   2  5e477cf4-784c-41e0-bcca-939e44c330bf    519  CONTINUED_AS_NEW
   3  198e1b37-91c5-4f33-9c62-cfcfa449ebef    516  CONTINUED_AS_NEW
   4  25e2af5a-d4fc-461c-90d7-5fc3bc0c906d    516  COMPLETED
one Workflow ID, 4 Run IDs; longest history 519 events; bound 700
latest run: status=COMPLETED search attributes={'AgentStatus': 'done', 'CurrentStep': 200, 'Owner': 'acct_7f3a'} memo tier=team
ResearchAgent children under lab11-agent-200/research/: 13
```

The script asserts the bound with `fetch_history()` on every run it finds by following
`WorkflowExecutionContinuedAsNew.new_execution_run_id` from the first run. About ten events per
step; 50 steps is ~520 events, comfortably under the bound and two orders of magnitude under the
history limits (as of 2026-09, see upstream — the rule is the suggestion flag, not the number).
`Owner` (set at start), `AgentStatus`/`CurrentStep` (upserted) and the `tier`/`max_steps` memo all
carried over each continue-as-new without being passed again: `continue_as_new(args=[goal, snapshot])`
inherits memo and Search Attributes unless you override them. In the UI, the Workflow ID shows one
execution with four runs; `WorkflowId STARTS_WITH 'lab11-agent-200'` lists the parent and 13 children.

One design bug shipped and fixed while writing this: the loop used to decide "continue-as-new" *before*
asking `plan()` whether it was done, so a 200-step run ended with a fifth run of 7 events whose only
job was to notice it had nothing to do. Plan first; continue only if there is more work.

## 11.3 The failure-injection suite

```bash
python -m pytest tests/           # 7 passed in 13.45s
```

Runs on the SDK's time-skipping test server (no Docker); run it from this directory, one lab per
`pytest` invocation (lab 10 and lab 11 both have a flat `worker_gpu` module; collected together they
import each other's and hang). Each test injects one failure by
registering its own `execute_tool` under the same Activity name — the Workflow is untouched:

| Test | Injection | What it asserts |
|---|---|---|
| `test_worker_crash_mid_run_loses_no_work_and_spends_nothing_twice` | all three pools cancelled mid-GPU-step; new pools with new identities | run finishes 6/6; every LLM key billed once; every tool effect once; both Worker identities in the history; `AgentStatus=done`. Also: with no Worker alive a **Query** hits its deadline (`Query deadline of 1999 milliseconds exceeded`) while **Describe** answers — a Query is computed by a Worker |
| `test_stalled_attempt_times_out_on_heartbeat_and_the_retry_resumes` | attempt 1 heartbeats to tick 2, then hangs | attempt 2 resumes from tick 2; `attempts == 1`; one effect; **no `ACTIVITY_TASK_TIMED_OUT` event** — a retried timeout is not history |
| `test_completion_lost_after_the_effect_is_not_a_second_effect` | attempt 1 does the work, writes the ledger, raises | one effect; attempt 2 returns `duplicate=True`; no `ACTIVITY_TASK_FAILED` event |
| `test_cancel_run_signal_finishes_the_step_then_stops_cleanly` | `cancel_run` Signal | completes (not fails) with `status cancelled`; nothing to compensate at a decision point |
| `test_temporal_cancel_during_the_gpu_step_runs_the_compensation` | `handle.cancel()` while a heartbeating GPU tool runs | `WorkflowFailureError` with `CancelledError`; `compensate("release_gpu:…")` is the last Activity scheduled; `AgentStatus=cancelled`; closed `CANCELED` |
| `test_pause_update_validator_and_command_dedupe` | the control surface | as in 11.1; `UPDATE_ACCEPTED ×2`, `SIGNALED ×4` |
| `test_continue_as_new_keeps_every_run_under_the_bound` | 20 steps at `STEPS_PER_RUN=8` | runs close `CONTINUED_AS_NEW, CONTINUED_AS_NEW, COMPLETED`; three Run IDs; every run ≤ 112 events; SAs and memo on the last run |

Two things the suite taught while being written, both now in the code:

- **Cancellation has two shapes.** If the Workflow is waiting on a timer or condition,
  `handle.cancel()` arrives as `asyncio.CancelledError`. If it is waiting on an Activity or child, the
  Activity is asked to cancel first and the Workflow gets an `ActivityError` whose *cause* is Temporal's
  `CancelledError`. The first version of `run()` caught only the first shape; the second went straight
  through, and the dev server recorded `WORKFLOW_EXECUTION_CANCEL_REQUESTED → ACTIVITY_TASK_CANCEL_REQUESTED →
  WORKFLOW_EXECUTION_CANCELED` in one Workflow Task — no compensation, `AgentStatus` still `running`.
  `temporalio.exceptions.is_cancelled_exception()` recognises both; cleanup Activities run fine
  afterwards because the SDK cleared the task's cancellation when it sent the cancel command.
- **The test server does not time out a dead Worker's sticky queue.** The crash test's first version
  hung: after the crash, the next Workflow Task sat in pool A's sticky queue for 30 s+ (on the dev
  server it moved after ~10 s, above). The dying pool now runs with `max_cached_workflows=0`, so
  every Workflow Task goes to the shared queue and is a full replay — which is also the more honest
  demonstration.

Not in the suite: a v2 deploy mid-run (module 9's exercise; the code here is v1 only) and the
Nexus paragraph. Both are named in the reading; neither was verified here.

## 11.4 The defense

Fill in `DEFENSE.md` — the 12-row requirement→mechanism table, the six-row failure column, the
four-layer boundary, the ten questions, and your own evidence — without opening `DEFENSE_ANSWERS.md`.
Then diff. A row you could not fill from memory names the module to reread. The evidence section is
not optional: paste your Run ID list, your `kill -9` handoff events, and your `pytest` line.

## 11.5 From a Workflow to a product: the evaluation console

Appendix A4 ("From a Workflow to a product") argues six decisions. This slice runs them. It is a
different product from AgentRun on purpose: a model-evaluation console. A user submits a suite of
benchmark cases against a model, watches the run progress live, cancels it, retries only the failed
cases, and a reviewer approves the results before they publish. Same skills, new nouns.

| File | What it is |
|---|---|
| `console/workflows.py` | `EvalRun`: cases fan out as `run_case` Activities on the `gpu-eval` lane; every state change is projected by `record_run_event`; `approve` is a validated Update; `handle.cancel()` is Temporal cancellation |
| `console/activities.py` | `run_case` (fake GPU: heartbeating ticks, a seeded benchmark-failure rate that is *recorded*, a hiccup rate the RetryPolicy *absorbs*) and `record_run_event` |
| `console/projection.py` | the read model in SQLite: `runs`, `run_events(run_id, seq)`; the idempotent upsert, keyset pagination, SSE framing |
| `console/api.py` | FastAPI: `POST /runs`, `GET /runs`, `GET /runs/{id}`, `GET /runs/{id}/events` (SSE), `POST .../cancel`, `.../retry-failed`, `.../approve` |
| `console/worker.py`, `console/worker_gpu.py` | the `console` lane (Workflow + projection) and the `gpu-eval` lane (`run_case`, `GPU_SLOTS` slots) |
| `console/setup_search_attributes.py` | registers `Org`, `RunStatus`, `Model`, `EvalParentRunId` |
| `console/tests/test_console.py` | the contract as six tests (time-skipping server, no Docker) |

Two ownership rules carry the whole design. **Temporal owns what is true; SQLite owns what the UI
reads.** The console never lists runs from Visibility: Visibility is eventually consistent and
rate-limited, and it is the operator's view, not the product's. And **the Workflow owns the even
sequence numbers, the console the odd ones.** The Workflow projects every state change as seq
`2·n`; the console writes its own events, `cancel_requested` and `retry_requested`, at the next odd
number, so the two can never collide.

```bash
cd labs/11-capstone-durable-agent-runtime/console
pip install -r ../../requirements.txt          # adds fastapi + uvicorn
export LAB_PREFIX=lab115-                       # shared dev server only
python setup_search_attributes.py
python worker.py &                              # console lane
python worker_gpu.py &                          # gpu-eval lane
uvicorn api:app --port 8115 &
H='-H X-Org:acme -H Content-Type:application/json'
curl -s $H -X POST localhost:8115/runs -d '{"run_id":"r1","cases":12,"fail_fraction":0.25}'
curl -sN $H localhost:8115/runs/lab115-r1/events            # SSE; Ctrl-C, then resume:
curl -sN $H 'localhost:8115/runs/lab115-r1/events?after=6'  # or send Last-Event-ID: 6
curl -s  $H localhost:8115/runs                             # the list, from SQLite
curl -s  $H -X POST localhost:8115/runs/lab115-r1/retry-failed
curl -s  $H -X POST localhost:8115/runs/lab115-r1/approve -d '{"reviewer":"alice","command_id":"c1"}'
```

Do each of these and write down what you see before reading the recorded result.

1. **Reconnect the stream.** Disconnect mid-run and reconnect with `?after=<last id>`. Count
   duplicates and gaps.
2. **Another tenant.** Read the run with `X-Org: globex`.
3. **The stranger and the double click.** Approve as `mallory`, then as `alice`, then as `alice` again.
4. **Retry twice.** Press Retry-failed twice in quick succession.
5. **Cancel during a case.** Start a 40-case run with `fail_fraction` 0, cancel after two seconds,
   and time `cancelling` → `cancelled`.
6. **Kill between a write and its completion.** Stop `worker.py` with `kill -9` while cases are
   finishing and restart it. Then count `case_done` rows per case in `run_events`.

Recorded on the course dev server (Temporal CLI dev server, `temporalio` 1.32.0, 2026-09-11):

| Step | What happened |
|---|---|
| 1 | Resuming after seq 6 delivered seq 8, 10, … 30, then `event: end` at `awaiting_review`. Nothing was sent twice and nothing was skipped. `Last-Event-ID: 26` works the same way. |
| 2 | `404`. Another org's run is indistinguishable from no run. |
| 3 | `mallory` got `422 approve rejected: 'mallory' is not a reviewer for org 'acme'`, and the rejection left no Event in history. `alice` got `200 published by alice (command c1)`. The second `alice` got `409 run is already published`. |
| 4 | The first press started `lab115-r1-retry-1` over the three failed cases with `parent_run_id` in its Memo and `EvalParentRunId` set. The second got `409 ... already exists`: the Workflow ID is the dedupe, so a race between two clicks still starts one child. |
| 5 | `cancelling` showed at once from the console's own `cancel_requested` event. `cancelled` followed 1.3 s later with 8 of 40 cases done; in-flight cases stopped at their next heartbeat. |
| 6 | Killed at 8 of 24 cases done and restarted 12 s later, the run reached `awaiting_review` with 24 `case_done` rows for 24 cases and gapless even sequence numbers. Whether a write lands in the window between INSERT and completion depends on timing, so the test suite forces it: the first attempt of two writes raises after the INSERT, the retries insert nothing, and there is still one row per case. |

```bash
python -m pytest tests -q    # 6 passed
```

**Where the slice stops.** SQLite stands in for Postgres. `X-Org` stands in for authentication.
The fake GPU's per-process ledger stands in for a results table. The UI is `curl`. None of that
changes a decision, and each one names the component you would put in its place.

## The recurring question, one last time

If every Worker process dies right now: everything AgentRun decided, every recorded result, every
pending timer, Signal and child, every lane's backlog survive in the Service. The next Worker on each
lane replays or resumes; nothing is re-spent, nothing is lost, nothing was waiting in a process.
You have now watched that sentence be true seven ways in a test suite and once with `kill -9`.
