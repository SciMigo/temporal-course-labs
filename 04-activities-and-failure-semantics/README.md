# Lab 4 — Activities and Failure Semantics

Goal: trigger each of the four Activity timeouts on purpose and read its Event; watch a heartbeat
checkpoint carry a tool run across a Worker death; make a side effect happen twice, then key it so
it cannot; and write down `execute_tool`'s timeouts, retry policy, heartbeat and key with a reason
for each number.

## Objective

**Why:** Decide how an Activity should recover from timeout, crash, and duplicate execution.

**By the end:** Identify four timeout events, resume work from a heartbeat, and use a stable key to prevent a duplicate effect.


`AgentRun` gains its second Activity here: `execute_tool(call: ToolCall) -> ToolResult`, keyed,
heartbeating, with a `RetryPolicy`. `AgentRun.run(goal, scenario)` plans three steps — `call_llm`,
one tool call, finish — and `scenario` picks the tool and the options the Workflow invokes it with
(the `SCENARIOS` table in `workflows.py` is Workflow code: timeouts are the caller's decision).
Tools write their side effects to files under `.state/` so you can count them. The Workflow catches
the `ActivityError`, so the *result* names the exception class and the timeout type, and the
history holds the Event. Reading page: "Activities and Failure Semantics".

```bash
export TASK_QUEUE=lab-04
python worker.py                          # terminal 1 (and a second one for 4.2 / 4.3)
python starter.py start_to_close          # terminal 2: agent-42-start_to_close
python history.py agent-42-start_to_close # the Activity Events with attempts, timeouts, causes
```

Every Event and message quoted below was read off the dev server with SDK 1.32.0. `+Ns` is
seconds since the execution started.

## 4.1 One timeout each

`python starter.py <scenario>` for each of the four, then `python history.py agent-42-<scenario>`.
Observed:

| scenario | how it is provoked | the Event | the Workflow sees |
|---|---|---|---|
| `schedule_to_start` | `echo` scheduled on `lab-04-gpu`, a queue nobody polls; `schedule_to_start_timeout=2s` | `+2.18s ACTIVITY_TASK_TIMED_OUT retry_state=TIMEOUT timeout_type=SCHEDULE_TO_START` — no `ActivityTaskStarted` at all: it was never picked up | `TimeoutError type=SCHEDULE_TO_START: activity ScheduleToStart timeout` |
| `start_to_close` | `sleep 5s` tool, `start_to_close_timeout=1s`, `maximum_attempts=1` | `+0.19s ACTIVITY_TASK_STARTED attempt=1`, `+1.19s ACTIVITY_TASK_TIMED_OUT retry_state=MAXIMUM_ATTEMPTS_REACHED timeout_type=START_TO_CLOSE` | `TimeoutError type=START_TO_CLOSE` |
| `schedule_to_close` | `fail` tool (raises `ApplicationError` type `ToolCrash`), retries every 0.5 s, `schedule_to_close_timeout=3s` | `+3.17s ACTIVITY_TASK_TIMED_OUT retry_state=TIMEOUT timeout_type=SCHEDULE_TO_CLOSE message='Not enough time to schedule next retry before activity ScheduleToClose timeout, giving up retrying' cause=(type='ToolCrash' … 'tool crashed on attempt 3')` — three attempts ran (Worker log), and **no** `ActivityTaskStarted` was written for any of them | `TimeoutError type=SCHEDULE_TO_CLOSE` |
| `heartbeat` | `sleep 5s` tool that never heartbeats, `heartbeat_timeout=1s`, `maximum_attempts=1` | `+1.19s ACTIVITY_TASK_TIMED_OUT retry_state=MAXIMUM_ATTEMPTS_REACHED timeout_type=HEARTBEAT` | `TimeoutError type=HEARTBEAT` |

Things to notice:

1. `ActivityTaskScheduled` records the options the Workflow asked for — read the
   `s2s= stc= s2c= hb= max_attempts=` columns. In `schedule_to_close` the Service **capped
   Start-To-Close to 3 s** (we asked for 10 s; `stc=0:00:03` is what was recorded) and also
   set Schedule-To-Start to 3 s: a per-attempt timeout can never exceed the whole budget.
2. `ActivityTaskStarted` is written when the attempt closes, with `attempt=` on it. A chain of
   retries that ends in a timeout during a backoff wait leaves no Started Event at all — the
   attempts are visible only in the Worker's log and in the `cause` of the final Event.
3. `retry_state` tells you *why* no further attempt was made: `MAXIMUM_ATTEMPTS_REACHED`,
   `TIMEOUT` (the Schedule-To-Close budget), or — for Schedule-To-Start — `TIMEOUT` again,
   because that timeout is not retried by design (a retry would re-queue to the same empty queue).
4. `python starter.py` with no scenario is the reading's specified call (`echo` tool, 2 min /
   15 min / 10 s heartbeat / 3 attempts): 17 events, `execute_tool` completes at `+0.21s`.
   That is your starting point for 4.4.
5. Same Event, different server: `PYTHONPATH=.. pytest tests/` runs `schedule_to_close` on the
   time-skipping test server, which skips the backoff so six attempts fit in the budget and
   then writes `ActivityTaskFailed retry_state=TIMEOUT` with the last `ApplicationError` instead
   of a timed-out Event. Same meaning — the budget ended the chain — different footprint; the
   test accepts both. Read `retry_state`, not just the Event type.

## 4.2 Heartbeat checkpoint across a Worker kill

The `shards` tool runs 10 shards of 1 s each, appends a line to `.state/effects.log` per shard,
and calls `activity.heartbeat(i + 1)` after each — its checkpoint. The Workflow invokes it with
`heartbeat_timeout=3s`, `start_to_close_timeout=60s`, 3 attempts.

1. Two Workers: terminal 1 `python worker.py`, terminal 3 `python worker.py`. Terminal 2:
   `python starter.py shards --no-wait`. One Worker's terminal starts printing
   `shard N done -> heartbeat(N+1)`.
2. When it prints `shard 5 done -> heartbeat(6)` (60%), `kill -9` that Worker.
3. Watch the other terminal. Observed, 3 s after the kill:
   `shards tool attempt 2: resuming at shard 5/10 (heartbeat details=[5])`, then shards 5–9.
   `python history.py agent-42-shards`:

   ```text
   11  + 0.17s  ACTIVITY_TASK_SCHEDULED   activity=execute_tool ... stc=0:01:00 hb=0:00:03 max_attempts=3
   12  +10.02s  ACTIVITY_TASK_STARTED     worker=<worker 2> attempt=2 last_failure=(timeout_type=HEARTBEAT message='activity Heartbeat timeout')
   13  +15.04s  ACTIVITY_TASK_COMPLETED   result=[{... "output":"10 shards" ...}]
   ```
   Attempt 1 has no Event of its own; it lives in attempt 2's `last_failure`. Without the
   heartbeat timeout the Service would have waited out the 60 s Start-To-Close before retrying.
4. `cat .state/effects.log`. Shards 0–5 by the first pid, then **shard 5 again** by the second,
   then 6–9. The checkpoint was one step stale: `heartbeat(6)` was called, but the process died
   before the Worker delivered it (heartbeats are throttled and batched; a `kill -9` cannot
   flush). This is the reading's warning made visible — make the resumed step safe to repeat.
5. No second terminal? `python starter.py shards_hang`: attempt 1 goes silent after shard 5
   instead of dying. The Service cannot tell a hung Worker from a dead one; the footprint is the
   same and the test suite uses it.

## 4.3 An Activity runs twice, until you key it

`charge(order_id)` appends `charged order-1 …` to `.state/ledger.txt`, then holds 6 s before
returning — the window in which the Worker "dies after the side effect and before the completion".
The Workflow invokes it with `start_to_close_timeout=10s`, 3 attempts.

1. `rm -f .state/ledger.txt`. Two Workers again. `python starter.py charge --no-wait`. The
   Worker that took it prints `charged order-1 (attempt 1); holding 6s before reporting --
   kill -9 <pid> now`. Do that within the 6 s.
2. The run completes about 17 s later: the Service waited out the 10 s Start-To-Close (it
   cannot distinguish slow from dead), retried after 1 s, and the other Worker charged again.
   `cat .state/ledger.txt`: two lines, two pids, `attempt 1` and `attempt 2`. The history has
   one `ActivityTaskStarted attempt=2 last_failure=(timeout_type=START_TO_CLOSE …)` and one
   `ActivityTaskCompleted result=["charged order-1"]`. As far as Temporal is concerned the
   Activity completed exactly once. The card was charged twice.
3. The same kill against the agent's tool: `python starter.py send --no-wait` and kill the
   Worker that prints `sent (attempt 1); holding 6s …`. Observed: `.state/effects.log` has
   `<key> sent by pid A attempt 1` and `<key> sent by pid B attempt 2`, the Workflow result is
   `send: sent`, and history shows `ACTIVITY_TASK_STARTED attempt=2 last_failure=(timeout_type=
   START_TO_CLOSE …)` at `+11.27s` followed by `ACTIVITY_TASK_COMPLETED` at `+17.29s`. Nothing
   in the history says "twice".
4. The fix is the key, enforced by the callee. Restart both Workers with
   `LAB04_IDEMPOTENT=1 python worker.py`: `charge` looks `order_id` up in the ledger before
   charging, and the `send` tool looks `ToolCall.key` up in `effects.log` before sending.
   Repeat the kill. Observed for `charge`: the ledger has **one** line, attempt 2 logs
   `charge order-1: already in the ledger, returning without charging (attempt 2)`, history
   shows `ACTIVITY_TASK_STARTED attempt=2 last_failure=(timeout_type=START_TO_CLOSE …)` then
   `ACTIVITY_TASK_COMPLETED result=["already charged order-1"]`, and the Workflow result is
   `charge: already charged order-1`.
5. `ToolCall.key` is `f"{workflow_id}:{step}:{sha256(args)[:12]}"`, computed once in Workflow
   code (look at `run()`), so every attempt of the same call carries the same key and a
   *different* step or *different* arguments get a different one. Why not the Run ID, which
   upstream suggests? Module 6.
6. `call_llm` already does this: its answers are cached under the key in `.state/llm_cache/`,
   and the Worker log says `billed` on a miss and `cache HIT` on a retry. Delete the cache dir
   and run any scenario twice with the same Workflow ID to see both lines.

## 4.4 Specify `execute_tool`

Write the invocation of `execute_tool` for the agent — timeouts, retry policy, heartbeat, key —
each with a one-line justification. Start from `SCENARIOS["default"]` and the reading's
"The agent's tool call, specified", and answer, for each number, what you observed in 4.1–4.3
that would go wrong without it:

- Start-To-Close — what did the `start_to_close` and `charge` runs show it detects, and how
  long after the Worker died?
- Schedule-To-Close — what bounded the `fail` tool's retries when no attempt count did?
- Heartbeat — how much sooner did the `shards` kill get noticed than Start-To-Close would have?
- Schedule-To-Start — when would you set it, given that it is never retried?
- `RetryPolicy` — which failure types must not retry? (Try `execute_tool` with an unknown tool
  name: it raises `ApplicationError(type="InvalidToolArguments", non_retryable=True)`.)
- The key — what is enforced where, and why can Temporal not do it for you?

## 4.5 Asynchronous completion (short)

The `finetune` tool is the 45-minute GPU job: it writes its task token to `.state/async/` and
calls `activity.raise_complete_async()`. The function returns; the Activity does not complete.

1. `python starter.py finetune --no-wait`; `python history.py agent-42-finetune` shows
   `ACTIVITY_TASK_SCHEDULED` and then `pending activity: execute_tool attempt=1` — an open
   Activity with no Worker slot holding it (kill the Worker if you like; nothing changes).
2. From any process with a Client — the scheduler's callback — `python complete_async.py`. It
   heartbeats and completes the Activity by task token. Observed: `ACTIVITY_TASK_COMPLETED
   result=[{"key":"finetune","output":"artifact://agent-7b-ft","tool":"finetune"}]` and the
   Workflow result ends in `finetune: artifact://agent-7b-ft`. `--fail` fails it instead.
3. Note what did not change: the Workflow code, the `ActivityTaskScheduled` Event, and the
   5-minute Start-To-Close that still bounds the wait.

## Check yourself

`PYTHONPATH=.. pytest tests/` (about a minute; Activity sleeps are real): one Event and one
exception per timeout; the `shards_hang` checkpoint resumes attempt 2 at shard 5 with no shard
run twice; `send_hang` / `charge_hang` produce two effects unkeyed and one keyed; and an
Activity completed from outside the Worker by task token.

## What you learned

- Schedule-to-Start, Start-to-Close, Schedule-to-Close, and Heartbeat timeouts answer different questions.
- A heartbeat checkpoint can let a retry resume useful work, but the last heartbeat may be behind the last side effect.
- A stable idempotency key protects an external effect when an Activity attempt runs twice.

## Questions to answer

1. Which timeout proves that no Worker started the Activity, and which only bounds one attempt?
2. After the Worker dies at shard 5, where does the retry resume and why?
3. How can a charge happen twice while the Workflow completes only once?
4. What should the idempotency key identify: a run, an attempt, or the intended effect?
