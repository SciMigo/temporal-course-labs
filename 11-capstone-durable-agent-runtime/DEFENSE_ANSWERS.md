# AgentRun — architecture defense (answers)

Filled in from the course design notes and from what the lab 11 code and tests
actually do. Where a claim is about upstream behaviour it carries "as of 2026-09"; where it is
about this lab it names the file or test that shows it.

## 1. The failure contract

| Box | A Worker dies here | On retry | What history records |
|---|---|---|---|
| `plan()` | Replayed from history on the next Worker; nothing is lost because nothing happened outside history | Not applicable — it is a decision | Commands only (the Activity it scheduled, the timer it started) |
| `call_llm` | The attempt fails at Start-to-Close or heartbeat; the Activity is retried; the idempotency key returns the cached answer | Same key, same answer, no second bill | `ActivityTaskScheduled` / `Started` / `Completed` with the result; the failed attempt is *not* an event |
| `execute_tool` | Heartbeat timeout fails the attempt quickly; the next attempt resumes from the last heartbeat's details | Idempotent by key; a completed-but-unreported effect comes back from the ledger as `duplicate=True` | Same three events, plus heartbeat details in Describe while pending |
| `ResearchAgent` | The child replays on its own; the parent does not notice | The child's own retry policy | `StartChildWorkflowExecutionInitiated` / `ChildWorkflowExecutionStarted` / `Completed` in the parent |
| pause wait | The condition is Workflow state; nothing polls while paused; the next Worker replays into the same wait | Not applicable | `WorkflowExecutionSignaled` (pause, resume); `UpsertWorkflowSearchAttributes` for `paused` |
| `continue_as_new` | The new run starts atomically as the old one closes; state arrives as the next run's arguments | Not applicable | `WorkflowExecutionContinuedAsNew` with `new_execution_run_id`; the new run's `WorkflowExecutionStarted` carries the snapshot |

Observed on the dev server (server 1.31.2, SDK 1.32.0): a `kill -9` of the workflow Worker at step
6 of 12 produced `WORKFLOW_TASK_TIMED_OUT` (sticky Schedule-to-Start, ~10 s), a rescheduled Workflow
Task, and the new Worker's first task 23 ms later; the run finished with every Activity executed once.

## 2. Requirement → mechanism

| # | Requirement | Mechanism | Box in AgentRun | Where you saw it |
|---|---|---|---|---|
| 1 | Runs for up to 72 hours | A Workflow Execution outlives every process. Waiting is a durable timer or `wait_condition`; no Worker is occupied while waiting | `run()` loop; `wait_condition(not paused)` | L01 (`kill -9` during the sleep); 11.1 `kill -9` handoff |
| 2 | Makes 1,000+ model calls | Each call is an Activity whose result is recorded once; the loop is ordinary code over recorded results | `call_llm` on `cpu-tools` (production: `premium-models`) | 11.2 — 200 steps, 4 Run IDs, every result in history |
| 3 | Tool calls can fail | `RetryPolicy` (bad arguments non-retryable) + Start-to-Close + Schedule-to-Close + heartbeat timeout; a retry is server-side, invisible to the caller | `execute_tool` with `TOOL_RETRY`, `TOOL_START_TO_CLOSE`, `TOOL_HEARTBEAT` | `test_stalled_attempt_times_out_on_heartbeat_and_the_retry_resumes` — attempt 2 resumed from heartbeat tick 2; no timeout event |
| 4 | Expensive calls must not accidentally duplicate | Idempotency key `wf_id:step:hash(...)` chosen in Workflow code, so a retry presents the same key; the Activity keeps a ledger by key | `_llm()` / `_tool()` build the key; `LLM_CACHE`, `TOOL_LEDGER` in `activities.py` | `test_completion_lost_after_the_effect_is_not_a_second_effect` — one effect, `duplicate=True` on attempt 2 |
| 5 | GPUs disappear | A `gpu-tools` lane with Schedule-to-Start (nobody took it) and heartbeat (somebody died holding it) timeouts; the L07 `compensations` list to release what a cancelled run holds | `_tool()` for `GPU_TOOL_NAMES`; `compensations`; `_compensate()` | Lab 10.1 stall (`ACTIVITY_TASK_TIMED_OUT ScheduleToStart`); `test_temporal_cancel_during_the_gpu_step_runs_the_compensation` |
| 6 | Workers deploy twice a day; old executions survive | Worker Versioning: executions pinned to the version that started them, upgrade at continue-as-new (GA 2026-03-30; upgrade-on-CaN Public Preview, as of 2026-09); `workflow.patched` for in-place changes | L09's `verify` step; not exercised in lab 11 code | L09 (v1 and v2 side by side) |
| 7 | User can pause and resume | Signals set a flag; the loop waits on `wait_condition`; a paused Workflow consumes no Worker compute while waiting | `pause` / `resume` / `cancel_run` handlers | `test_pause_update_validator_and_command_dedupe`; lab 10.2 fleet query `AgentStatus = 'paused'` |
| 8 | User can change instructions mid-run | Update: a tracked write with a validator (rejected = never in history) and a `command_id` so a retried Update is applied once | `change_goal` + `@change_goal.validator`; `processed_command_ids` | same test — empty goal → `WorkflowUpdateFailedError`; `c1` twice → `ignored`; history has `UPDATE_ACCEPTED ×2`, none for `c0` |
| 9 | User can ask what it is doing | Query: a read of Workflow state, answered by a Worker from replayed state, never recorded | `status()` | every lab; `test_worker_crash…` shows a Query needs a Worker (deadline exceeded with none alive) while `describe()` does not |
| 10 | May delegate to sub-agents | Child Workflow: its own history, replay and retry; the parent sees one result and a stable id | `ResearchAgent`, id `f"{wf_id}/research/{step}"` | 11.2 — 13 children under `agent-200/research/` |
| 11 | History cannot grow forever | `continue_as_new(AgentState)` when `is_continue_as_new_suggested()` or every `STEPS_PER_RUN` steps; memo and Search Attributes carry over | `_should_continue_as_new()`, `snapshot()`, `run(goal, state)` | 11.2 — longest run 519 events for 50 steps; `test_continue_as_new_keeps_every_run_under_the_bound` |
| 12 | Operators need exact execution diagnostics | History (what happened), Describe (what Temporal thinks, pending attempts, heartbeat details), Search Attributes for finding, `DescribeTaskQueue` for lanes | `_publish()` → `AgentStatus`/`CurrentStep`; `Owner` at start | Lab 10.2 five questions; `five_questions.py` |

## 3. Where Temporal ends

| Layer | Owns | For AgentRun, concretely |
|---|---|---|
| Temporal | Where execution logically is: the step, the outstanding attempt, the pending timer, the lane a Task waits on | `agent-42` is at step 137, attempt 2 of `embed` is outstanding on `gpu-tools`, paused since Tuesday |
| Kubernetes | Where processes run: one Deployment per lane, autoscaled on that lane's backlog age; pod identity in the history | `gpu-tools` pods on the GPU node pool; `Worker(identity=f"{pod}@{lane}")` |
| Kafka | Streams between systems — not execution, not history | Outside events reach the run as Signals (a consumer calling Signal-With-Start); the run emits through an Activity that produces |
| Postgres | The system of record for results: what the provider charged, what the customer owns, whether the answer was good | The ledgers in `activities.py` are its stand-in: rows keyed by the idempotency keys, written by Activities, outliving retention |

The engine that rendered this course draws the same line: `MiniLectureCourseWorkflow` keeps the
legs, retries, heartbeats, yields and the review checkpoint in Temporal, and leaves the job store,
`agent_state.json`, artifacts, `usage.json` and the verifier database outside it. History answers
what execution happened; it does not answer whether the lecture was good.

## 4. The ten questions

1. **What survives a Worker crash?** Everything in the Service: the Event History (every decision and every recorded result), pending timers, queued Tasks with their attempt counts, Signals, Search Attributes. Nothing that lived only in the process — and the program was written so nothing does.
2. **How does replay reconstruct state?** A fresh Worker runs the Workflow code from the top, and every time the code asks for something (an Activity result, a timer), the SDK answers from history instead of doing it again; the code arrives at the same state having executed no effect.
3. **Why deterministic?** Because replay must make the same decisions in the same order to match the history; a decision that depends on the clock, randomness or a network call would diverge and the SDK would (rightly) refuse to continue.
4. **Why can an Activity execute twice?** Because the Service cannot tell "the Worker died before starting" from "the Worker did the work and died before reporting"; it retries, so at-least-once is the guarantee and idempotency by key is your job.
5. **Activity retry vs. Workflow replay?** A retry re-executes an *effect* (server-side, invisible in history until the final outcome); a replay re-executes *decisions* with no effects at all. Retries cost; replays are free.
6. **Signal vs. Query vs. Update?** Signal: an asynchronous write, recorded, no reply. Query: a read of current state, answered by a Worker, not recorded. Update: a tracked write with a validator and a result, recorded when accepted.
7. **Activity vs. Child Workflow?** An Activity is one effect with timeouts and retries and no state of its own; a Child Workflow is its own durable program with its own history, replay, Signals and children — use it when the work has decisions, not just an effect.
8. **When continue-as-new?** When the Service says so (`is_continue_as_new_suggested()`), and on a cadence you choose; carry a snapshot of what `plan()` needs, never the history; drain handlers first.
9. **How to deploy changed code?** Never change a decision an old history already recorded: pin running executions to the version that started them and upgrade at continue-as-new (Worker Versioning), or `patched()` for a change that must apply in place.
10. **Where does Temporal end?** Temporal owns where execution logically is; Kubernetes owns where processes run; Kafka owns streams; Postgres owns the results and the money. The boundary is the row your Activity writes, keyed by the idempotency key.
