# 8.5 — Answers

Do not open this before you have written your five answers down.

History: `tests/histories/forensic_lab08-agent-42-broken.json` (Workflow ID `lab08-agent-42-broken`,
16 events, recorded 2026-09-11 on Temporal Server 1.31.2 / temporalio 1.32.0). Event numbers
below are the `eventId` fields in that file.

## The events, annotated

```
 1  WorkflowExecutionStarted       AgentRun, task queue lab-08, input "summarize the Temporal docs"
 2  WorkflowTaskScheduled
 3  WorkflowTaskStarted            identity worker-a
 4  WorkflowTaskCompleted
 5  ActivityTaskScheduled          call_llm   retry: 3 attempts, 1 s → ×2.0 ; no heartbeat timeout
 6  ActivityTaskStarted            attempt 1, identity worker-c, lastFailure: none
 7  ActivityTaskCompleted          "TOOL: fetch temporal-docs"
 8  WorkflowTaskScheduled
 9  WorkflowTaskStarted            identity worker-a
10  WorkflowTaskCompleted
11  ActivityTaskScheduled          execute_tool   input key "lab08-agent-42-broken:1:d3f35deb8827", tool fetch
                                   retry: maximumAttempts 3, initialInterval 1 s, backoff 1.0 ; heartbeatTimeout 5 s
12  ActivityTaskStarted            attempt 3, identity worker-c,
                                   lastFailure: "activity Heartbeat timeout" (TIMEOUT_TYPE_HEARTBEAT, source Server)
13  ActivityTaskFailed             ApplicationError type "BadToolArguments", non_retryable,
                                   message "tool rejected the arguments: target must be a URL",
                                   retryState RETRY_STATE_NON_RETRYABLE_FAILURE
14  WorkflowTaskScheduled          task queue "worker-a-<uuid>" kind STICKY (normalName lab-08)
15  WorkflowTaskTimedOut           TIMEOUT_TYPE_SCHEDULE_TO_START   ← nobody polled worker-a's sticky queue
16  WorkflowTaskScheduled          task queue lab-08 kind NORMAL     ← last event; nobody has picked it up
```

## 1. Which Activity attempt failed?

Attempt **3** of `execute_tool` (event 12: `attempt: 3`) failed with the non-retryable
`BadToolArguments` (event 13). Attempts 1 and 2 also failed — you know that only because the
one `ActivityTaskStarted` the history contains says `attempt: 3`. Intermediate attempts leave
no events of their own. The Service generates `ActivityTaskStarted` when it dispatches the
Task, but withholds it from history until the Activity Execution reaches a terminal outcome —
so history carries one such Event, describing the attempt that closed it, instead of one per
retry. While the Activity was still pending, the
attempt counter lived only in `temporal workflow describe` (Pending Activities), not in history.

## 2. Was it retried?

Yes, twice. Event 11 carries the RetryPolicy (`maximumAttempts: 3`); event 12 says attempt 3
ran. Why attempt 2 ended is recorded — `lastFailure` on event 12 is a **heartbeat timeout**:
attempt 2 stopped heartbeating (heartbeatTimeout 5 s) and the Service gave up on it. Why
attempt 1 ended is **not recorded anywhere**: `lastFailure` holds only the immediately
preceding attempt's failure. Will it be retried again? No: event 13's
`retryState: RETRY_STATE_NON_RETRYABLE_FAILURE` — the Activity is closed. (Had attempt 3 been
a retryable error, the RetryPolicy was exhausted anyway: `RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED`.)

## 3. Did the side effect possibly occur?

**Possibly, yes — and the history cannot say.** Three attempts of `fetch temporal-docs` ran.
History names the Worker for the *terminal* attempt only: event 12 says attempt 3 ran on
worker-c. Attempts 1 and 2 left no `ActivityTaskStarted` of their own, so which Worker ran them
is not recorded anywhere — do not assume it was the same one. Attempt 3 was rejected before doing anything (`BadToolArguments` is the tool
refusing its input). Attempt 2 *started* — an attempt can only time out on heartbeat after
it has started — and then went silent; a heartbeat timeout means the Service stopped hearing
from it, not that the code stopped running. The Worker may have completed the fetch after the
Service had already written it off. (This history's `lastFailure` carries no
`lastHeartbeatDetails`, so how far attempt 2 got is not recorded either.) Attempt 1: unknown. The only handle on the question is the idempotency key in event 11's input,
`lab08-agent-42-broken:1:d3f35deb8827`: if the tool's system of record has an entry for that
key, the effect happened once; if it does not, it did not. Temporal records that the attempts
*ran*; whether they *did* something is in your database, not here.

## 4. What state is the Workflow in now?

**Open, with a Workflow Task scheduled that no Worker has picked up** (event 16). Note the
pair 14–15: the task was first offered to worker-a's sticky queue, timed out on
schedule-to-start because worker-a is gone, and was re-offered on the normal queue `lab-08`.

Split that answer in two, because the exercise gave you no source and the two halves have
different epistemic status.

**Temporal execution state — determinable from history alone.** The execution is open. No
Activity is pending (`describe` shows Pending Activities: 0 — the Activity closed at event 13).
A Workflow Task is scheduled on the normal queue and no Worker has started it. Event 13 has not
been applied to any Workflow code yet; that is what the pending task is for.

**Application state — not determinable from history alone.** Event History records inputs,
Activity results, timers, Signals and Commands. It does not serialize `self.step`,
`self.context_summary` or `self.attempts`. Those exist only as the product of *history + the
Workflow Definition*, reconstructed by replay. Hand this history to compatible code and it
rebuilds `goal` = "summarize the Temporal docs", `step` = 1, `context_summary` =
"\nTOOL: fetch temporal-docs", `attempts` = 0, `status` = "running" — but that is the code
talking, not the history. With the Worker image gone, the honest answer is the first paragraph
plus "the rest needs the code".

## 5. What happens mechanically if a compatible Worker starts now?

**Predictable from history alone:** it polls `lab-08`, receives the pending Workflow Task,
replays events 1–13, and at event 13 the `execute_activity` call raises `ActivityError` (cause
`ApplicationError("BadToolArguments")`). **Not predictable without the source:** what the
Workflow catches, what it mutates, and therefore which Command comes next. From the history alone you can
say: the code will issue *some* Command; the Service will accept it if it is consistent with
events 1–13. Predict: either the Workflow fails/compensates, or it records the error and asks
the model again.

**What actually happened** (observed 2026-09-11 by starting `worker.py` on `lab-08`, events
16–49 of the continued execution):

```
17  WorkflowTaskStarted        a fresh identity (pid 3811222)
19  ActivityTaskScheduled      call_llm   prompt now contains "RESULT: error BadToolArguments"
21  ActivityTaskCompleted      "TOOL: fetch temporal-docs"        ← the model asks for the same tool again
25  ActivityTaskScheduled      execute_tool  key "lab08-agent-42-broken:3:d3f35deb8827"   ← step 3, a NEW key
27  ActivityTaskCompleted      ok "fetch: temporal-docs done"
...                            summarize, FINAL
49  WorkflowExecutionCompleted "the agent fetched and summarized the docs"
```

`status` query afterwards: `attempts: 1, step: 7, status: "done"`. The code recorded the
failure (`RESULT: error BadToolArguments`), asked the model what to do, and the model said
"fetch" again. The second fetch ran under a **different key** (`:3:` instead of `:1:`) because
the key is `workflow_id:step:hash` and the step had moved on. So if attempt 2 of the first
call did complete the fetch (question 3), the returning Worker fetched it **twice** — the
idempotency key protects a *retry of the same Activity*, not a *re-plan by the Workflow*. That
is the design bug the forensic exercise is there to make you notice: the key that dedupes a
tool call should be derived from what the call *is* (tool + args), not from where in the
loop it happened.

## The two things the history does not record

1. Why attempt 1 failed (only the last failure survives).
2. Whether attempts 1 and 2 had a side effect (only your system of record knows).

Both are things your own Activities must record if you want to answer them next time.
