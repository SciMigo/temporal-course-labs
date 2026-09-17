# Lab 8 — Testing and Replay Tests

Goal: test `AgentRun` three ways — Workflow tests in the time-skipping environment, Activity
tests with a fake context, and replay tests over recorded histories — then inject failures on
purpose, and finally read a broken history with no source and say what happened.

## Objective

**Why:** Turn the failure behavior of the agent into repeatable tests.

**By the end:** Use time-skipping, replay, Activity fakes, and injected failures to catch regressions before deployment.

## Before you run

- **Mechanism:** Workflow tests exercise logic with mocked Activities and time skipping. Replay tests check old histories against changed Workflow code. Activity tests check effects and heartbeat behavior.
- **Predict:** Predict which test catches a newly inserted Workflow Command and which test catches a duplicate external effect.
- **Look for:** For each test, name the failure it detects and one failure it cannot. Use the forensic exercise to check those claims against history.
- **Terminal route:** From this lab directory, run `source ../.venv/bin/activate` before copying any `python ...` command below. The browser action cards select the Python environment for you.


`workflows.py` is AgentRun at stage 8: the loop from labs 1–7 (`plan()`, `call_llm`,
`execute_tool` with timeouts / RetryPolicy / heartbeats / idempotency key, the L05 Signals,
Query and Update, the L06 snapshot and continue-as-new, the L07 compensation list). No real
model is called anywhere in this lab: `call_llm` is a three-rule stand-in and the tests replace it.

```
labs/08-testing-and-replay-tests/
  workflows.py  worker.py  starter.py     the program, as in lab 1
  export_history.py                       history → tests/histories/<workflow-id>.json  (8.2)
  tests/
    conftest.py                           runs `async def` tests with plain asyncio (no plugin needed)
    fakes.py                              @activity.defn(name="call_llm") / (name="execute_tool") fakes
    test_81_workflow_time_skipping.py     8.1
    test_82_replay.py                     8.2
    test_83_activities.py                 8.3
    test_84_failure_injection.py          8.4   (+ crashable_worker.py, the Worker it kills)
    histories/lab08-agent-42.json         a clean run recorded under this code
    histories/forensic_lab08-agent-42-broken.json    8.5, no source
  forensic/make_broken_history.py         reproduces 8.5 on your dev server — do not read it first
  forensic/answers.md                     check yourself after 8.5
```

Setup, as in lab 1: `docker compose up -d` in `labs/`, a venv with `requirements.txt`. Run all
the tests with `python -m pytest` from this directory (observed: `21 passed in 50.41s`; the
failure-injection suite is the slow part, ~48 s of real waiting, explained in 8.4). Only 8.2's
export and 8.5's reproduction need the dev server; every test starts its own test server.

If you share one dev server with other people: `export TASK_QUEUE=lab-08` and pass a prefixed
Workflow ID (`WORKFLOW_ID=lab08-agent-42 python starter.py`). The checked-in histories were
recorded that way, which is why their IDs carry the prefix.

## 8.1 Workflow tests: three days in a second

The ladder this lab climbs, and the rule that orders it — prove each property with the cheapest
test that can prove it:

```text
pure decision logic      plain unit test, no Temporal at all      plan(), snapshot()
Workflow orchestration   WorkflowEnvironment (time-skipping)      8.1
history compatibility    Replayer over real histories             8.2
Activity behaviour       ActivityEnvironment, faked context       8.3
failure semantics        a real Worker you kill, retry, cancel    8.4
```

Nothing below 8.4 needs a container; nothing above 8.1 needs three days.

```bash
python -m pytest tests/test_81_workflow_time_skipping.py -v
```

Three `plan()` tests need no environment at all — `plan()` is a pure function of state, so
`AgentRun()` is constructed, fields are set, `plan()` is called and the `Step` is asserted.

Three loop tests run in `WorkflowEnvironment.start_time_skipping()` with `fakes.fake_call_llm`
and `fakes.fake_execute_tool` registered under the real Activities' *names*. Read `fakes.py`
first: the fake model asks for one tool call, then answers "FINAL: 42".

1. `test_agent_loop_calls_one_tool_then_answers` — result `"42"`; exactly two model calls and
   one tool call; the key handed to the tool is `<workflow-id>:1:<hash>` (step 1 is the tool step).
2. `test_agent_gives_up_after_three_failed_tool_steps` — swap in `failing_execute_tool`
   (`ok=False` every time) and the loop takes the give-up branch after `MAX_TOOL_FAILURES`.
3. `test_paused_agent_expires_after_72_hours_in_under_a_second` — the Workflow is started with
   `start_signal="pause"`, so the first event is the pause and the loop parks at the checkpoint.
   Nobody sends `resume`. The test then asserts on the history it fetched: one `TimerStarted`
   whose `start_to_fire_timeout` is exactly 72 hours. Observed: the six tests together take
   about two seconds. That is time skipping: timers are fast-forwarded whenever the Workflow is
   blocked and no Activity is running.
4. `test_resume_signal_releases_the_checkpoint` — same start, then a `status` Query (returns
   `"paused"`) and a `resume` Signal; the run finishes with `"42"`.

Write one more: pause the agent *after* its first tool call (send the Signal from the fake
tool's side, or from a second task), and assert the history's timer comes after the
`ActivityTaskCompleted`.

## 8.2 Replay tests: the history is the fixture

A replay suite is worth exactly its corpus. One happy-path history proves one path compatible.
Keep a history per still-running code revision and per branch that matters — the failure path, the
cancelled run, the one that continued-as-new — and add the history of every incident you debug,
because that shape has already proven it can happen.

Export a history. On a box with the CLI:

```bash
temporal workflow show --workflow-id agent-42 --output json > tests/histories/agent-42.json
```

Here the CLI lives in the dev-server container, and there is a Python exporter that does the
same thing through the client (`handle.fetch_history()` → `WorkflowHistory.to_json()`):

```bash
docker exec labs-temporal-1 temporal workflow show -w lab08-agent-42 --output json > tests/histories/lab08-agent-42.json
python export_history.py lab08-agent-42          # same file, no CLI needed
```

`tests/histories/lab08-agent-42.json` is one such export: 35 events, recorded with this
`workflows.py` (`python worker.py` + `WORKFLOW_ID=lab08-agent-42 python starter.py`).

```bash
python -m pytest tests/test_82_replay.py -v
```

`test_history_replays_under_current_code` is parametrised over every file in `tests/histories/`
(including the forensic one — a partial history replays fine as long as the Commands match).
`Replayer(workflows=[AgentRun]).replay_workflow(history)` raises on non-determinism.

Now break it. In `workflows.py`, add one line at the top of `run()`:

```python
        await asyncio.sleep(1)      # a Command the recorded histories never saw
```

and run the replay test again. Observed:

```
NondeterminismError: [TMPRL1100] Nondeterminism error: Timer machine does not handle this event: HistoryEvent(id: 5, ActivityTaskScheduled)
```

Event 5 of the recorded history is `ActivityTaskScheduled call_llm`; the new code's first
Command is a timer. This is the exact error a Worker running the new code would raise on
`lab08-agent-42`'s next Workflow Task — you produced it from a file, before any deploy.
`test_changed_workflow_fails_replay` does the same thing without editing `workflows.py`: a
subclass registered under the name `AgentRun` with the extra timer, asserted to fail on every
history. Revert your edit.

Preview of module 9 — make both worlds agree:

```python
        if workflow.patched("extra-timer"):
            await asyncio.sleep(1)
```

Observed: the v1 history replays clean again (no marker → old branch). A new execution
records a `MarkerRecorded` event and takes the timer — observed in lab 9's recording of its own
patch, which does this for a real change.

## 8.3 Activity tests: a fake context

```bash
python -m pytest tests/test_83_activities.py -v
```

`ActivityEnvironment().run(execute_tool, call)` runs the real Activity with a fake context — no
Worker, no server.

- `test_execute_tool_heartbeats_with_progress_details` — `env.on_heartbeat` captures every
  `activity.heartbeat(...)`: `[("fetch:1/3",), ("fetch:2/3",), ("fetch:3/3",)]`. Those details
  are what a retry, or a forensic reader, sees as "how far it got".
- `test_execute_tool_is_idempotent_by_key` — run it twice with the same `key`, assert one
  effect (`workflows.effects() == ["fetch(temporal-docs)"]`). A different key is a different
  effect. The "system of record" here is a dict in the process — enough for this test, and
  exactly what 8.4 shows is not enough for a Worker death.
- `test_cancellation_stops_the_tool_before_its_side_effect` — start the Activity as a task,
  `env.cancel()` after the first heartbeat, expect `asyncio.CancelledError` and no effect. One
  environment per test: `cancel()` is sticky.
- `test_unknown_tool_is_a_non_retryable_application_error` — the `BadToolArguments` you will
  meet again in 8.5.

## 8.4 Failure injection: crash, timeout, duplicate, cancel

```bash
python -m pytest tests/test_84_failure_injection.py -v --durations=0
```

Observed durations: crash 17.8 s, timeout 15.2 s, cancel 14.5 s, the two duplicate tests
< 0.4 s. The slow ones are slow for real reasons; each docstring says which.

1. **Worker crash** — `crashable_worker.py` is started as a subprocess with the slow,
   heartbeating tool; the test waits until `describe` shows `execute_tool` STARTED, then
   `SIGKILL`s it (`kill -9`, not Ctrl-C). Nothing reports the death: the Service learns of it
   when the heartbeat stops (heartbeat_timeout 5 s), schedules attempt 2, and a fresh in-process
   Worker finishes the run. Assertions on the history: the tool's `ActivityTaskStarted` says
   `attempt: 2` on the new Worker with `last_failure` = a **heartbeat timeout**, and there is a
   `WorkflowTaskTimedOut SCHEDULE_TO_START` — the dead Worker's sticky queue, timed out and
   re-offered on the normal queue. This one test runs on `WorkflowEnvironment.start_local()`
   (a real dev server in a subprocess) because the time-skipping server was observed **not** to
   fire those two timeouts for a dead Worker's sticky queue; the run then waited forever.
2. **Activity timeout** — `hanging_execute_tool` never heartbeats. Each attempt dies on the 5 s
   heartbeat timeout, the RetryPolicy allows 3, then `execute_activity` raises and the Workflow
   records `RESULT: error TimeoutError` and asks the model, which gives up. `env.sleep(7 s)` is
   called three times to push the clock past each attempt — time skipping does not advance
   while an Activity is running, so this still costs ~15 s of real time.
3. **Duplicate execution** — `execute_tool_reported_late` runs the real tool and then, on
   attempt 1, raises *after* the side effect: the Worker died between doing and reporting.
   Attempt 2 gets the same `ToolCall`, same key: `effects() == ["fetch(the-answer)"]`, once.
   The second test runs the same injection against `execute_tool_ignoring_its_key` and asserts
   the bug — two effects — so you can see what the injection catches; in your own suite that
   test asserts `== 1` and the tool that fails it gets fixed. The happy path never touches this.
4. **Cancellation** — `fake_call_llm_two_tools` asks for `fetch` then `summarize`; the test
   cancels during `summarize`, expects `WorkflowFailureError` with a `CancelledError` cause,
   that the running tool saw `CancelledError` on its next heartbeat (the SDK throttles
   heartbeats to 80 % of the timeout, so up to ~4 s), that `undo_fetch` ran (the L07
   compensation), and that `status` reports `"cancelled"`.

The cancellation injection found a bug in this file while the lab was being written. On
Workflow cancellation, `execute_activity` raises `ActivityError` whose *cause* is
`CancelledError` — and `_run_tool`'s `except ActivityError` treated that as a failed tool step,
recorded it, and kept going; the run completed as if nothing had happened. The fix is the
`isinstance(err.cause, CancelledError): raise` you now see there. Cancellation ≠ failure.

## 8.5 Forensic: one history, no source

`tests/histories/forensic_lab08-agent-42-broken.json` is the Event History of an `AgentRun`
execution that stopped. You do not get the code that produced it — assume the Worker image is
gone. Read it:

```bash
python -c "import json; [print(e['eventId'], e['eventType']) for e in json.load(open('tests/histories/forensic_lab08-agent-42-broken.json'))['events']]"
python -m json.tool tests/histories/forensic_lab08-agent-42-broken.json | less     # every attribute
```

To have it *live* on your own dev server — so `describe`, the Web UI and "start a Worker" work —
run the reproduction and do not open the script:

```bash
TASK_QUEUE=lab-08 python forensic/make_broken_history.py        # ~10 s; leaves agent-42-broken open
docker exec labs-temporal-1 temporal workflow show -w agent-42-broken --detailed
docker exec labs-temporal-1 temporal workflow describe -w agent-42-broken
```

Write down, with the event number that answers each:

1. Which Activity attempt failed?
2. Was it retried? Will it be?
3. Did the side effect possibly occur?
4. What state is the Workflow in now?
5. What happens mechanically if a compatible Worker starts now? Separate what you can predict
   from history alone from what you cannot predict without the Workflow source.

Hints, not answers: intermediate attempts are not events; `lastFailure` on an
`ActivityTaskStarted` describes the attempt before it; a `retryState` says whether the Service
is done; the last two events are not the same task queue; and the idempotency key is in the
scheduled event's input.

Then check yourself against `forensic/answers.md`. Then start a Worker:

```bash
TASK_QUEUE=lab-08 python worker.py
```

and watch what the execution does (the answers file records what was observed). The two places
your answers were wrong are the two things the history does not record.

Which is the lesson. Three systems know three different things, and a production incident needs
all three:

```text
Event History        what Temporal orchestrated: inputs, results, timers, signals, commands
your Workflow code   what those facts mean as application state, and what happens next
your external system what side effects actually reached the world
```

The forensic questions you could answer came from the first. The variables came from the second.
"Did the fetch actually happen?" belongs to the third, and nothing in Temporal will ever answer
it — which is why every Activity in this course carries an idempotency key and why the capstone
asks where your own record of effects lives.

## If every Worker process dies right now, what information survives, and what happens when a Worker comes back?

In the time-skipping environment the rule is unchanged: the test server holds the history;
mocked Activities leave recorded results, not effects. A replay test *is* a Worker coming back
— against a file, in CI, before any real Worker sees the new code. The forensic history is the
whole of what survived; the five questions are what a returning Worker will do with it.

## What you learned

- Time-skipping tests check Workflow decisions quickly; Activity tests check effects with a fake context.
- Replay tests detect incompatible Workflow code against saved real histories before deployment.
- A history can show what Temporal recorded, but it cannot prove whether an external effect happened.

## Questions to answer

1. Which test would catch a changed command order in a months-old run before deployment?
2. Why does a mocked Activity result survive replay while the mock itself does not run again?
3. In the forensic history, what can you infer about Worker crashes, retries, and the missing external effect?
4. Which part of the failure-injection suite requires real waiting instead of time skipping?
