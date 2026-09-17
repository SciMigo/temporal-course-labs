# Lab 10 — Operating Temporal in production

Goal: take the AgentRun you have (stage 9) off its single `agent-runs` queue, run its three kinds of
work on three lanes with three Worker pools, stall one pool and read the stall from the outside, then
answer the operator's five questions about a run you did not start — from the UI and from the Python
client. Reading page: "Operating Temporal in Production"; long form in appendices A1 and A3.

## Objective

**Why:** Operate the agent as separate Workflow, CPU, and GPU lanes.

**By the end:** Register search attributes, route work to Task Queues, and diagnose a stalled lane from the UI and client.

## Before you run

- **Mechanism:** Task Queues route work to Worker pools; they are not the Event History. Search Attributes help operators find runs, while pending Tasks and timeouts reveal a stalled lane.
- **Predict:** If the GPU pool disappears, predict what stays durable and where waiting work becomes visible.
- **Look for:** Check the queue backlog and pollers, `schedule_to_start_timeout`, and the run’s Search Attributes before and after the pool returns.
- **Terminal route:** From this lab directory, run `source ../.venv/bin/activate` before copying any `python ...` command below. The browser action cards select the Python environment for you.


Nothing in the agent loop changes. What changes is one keyword argument per Activity call
(`task_queue=`), one timeout you had probably been leaving unset (`schedule_to_start_timeout`), and
three Search Attributes the run publishes about itself.

| File | What it is |
|---|---|
| `lanes.py` | the three Task Queue names, spelled once; `LANE_FOR_TOOL`; `LAB_PREFIX` |
| `workflows.py` | AgentRun stage 10: `plan()` → `call_llm` → `search` (cpu) → `embed` (gpu) → finish; `pause`/`resume`, `status`; `_publish()` upserts `AgentStatus`/`CurrentStep` |
| `worker_workflows.py` | the `agent-workflows` pool: Workflow types only |
| `worker_cpu.py` | the `cpu-tools` pool: `call_llm`, `execute_tool`, `evaluate` |
| `worker_gpu.py` | the `gpu-tools` pool: `execute_tool` for GPU tools only, one slot (`GPU_SLOTS`) |
| `starter.py` | start one run: `--owner`, `--tier enterprise|team|batch`, `--id`, `--no-wait` |
| `setup_search_attributes.py` | registers `AgentStatus` (Keyword), `CurrentStep` (Int), `Owner` (Keyword) through the Operator Service |
| `five_questions.py` | the five questions from the Python client: `history`, `describe`, `list`, `lane`, `five` |
| `priority_race.py` | 10.3: build a GPU backlog, add one single-slot GPU Worker, print the dispatch order |
| `tests/` | self-check on the SDK's time-skipping test server: `python -m pytest tests/` |

## Setup

```bash
cd labs && docker compose up -d && cd 10-operating-temporal-in-production
export LAB_PREFIX=lab10-        # only on a shared dev server: queues become lab10-agent-workflows …, IDs lab10-agent-42
python setup_search_attributes.py
```

`setup_search_attributes.py` is the registration step the reading describes as
`temporal operator search-attribute create --name AgentStatus --type Keyword`, done from Python with
`client.operator_service.add_search_attributes(AddSearchAttributesRequest(namespace=..., search_attributes={...}))`
against the *running* server — no restart. Observed:

```
created AgentStatus  KEYWORD
created CurrentStep  INT
created Owner        KEYWORD
namespace default custom attributes: ['AgentStatus', 'CurrentStep', 'Owner']
```

Run it again and it prints `exists` for each. Skip it and the first `_publish()` fails every Workflow
Task with `search attribute AgentStatus is not defined` — the run is stuck, not failed, until someone
registers the key. The equivalent at server start, if you own the server, is
`temporal server start-dev --search-attribute AgentStatus=Keyword --search-attribute CurrentStep=Int --search-attribute Owner=Keyword`
(verified on a throwaway container in 10.3 below).

The `temporal` CLI is inside the compose container; there is no binary on the host:

```bash
alias temporal='docker exec labs-temporal-1 temporal'      # or: docker compose exec temporal temporal …
temporal --version      # temporal version 1.8.3 (Server 1.31.2, UI 2.50.1)  ← the server these notes were taken on
```

## 10.1 Three lanes, three pools; stall one

Three terminals, one pool each — a pool is a process that polls one lane and registers exactly the
code that lane needs:

```bash
python worker_workflows.py   # workflow worker pid=… polling 'lab10-agent-workflows' (Workflow Tasks only)
python worker_cpu.py         # cpu worker pid=… polling 'lab10-cpu-tools': call_llm, execute_tool, evaluate
python worker_gpu.py         # gpu worker pid=… polling 'lab10-gpu-tools' with 1 slot(s): execute_tool (GPU tools only)
```

Fourth terminal: `python starter.py`. The result names the lane that answered each step, and the
history shows every `ACTIVITY_TASK_SCHEDULED` carrying its lane (`python five_questions.py history lab10-agent-42`):

```
result: {'lanes_used': ['lab10-cpu-tools', 'lab10-cpu-tools', 'lab10-gpu-tools'], 'steps': 3, …}
   5  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES    AgentStatus, CurrentStep
   6  ACTIVITY_TASK_SCHEDULED              call_llm on lab10-cpu-tools
   7  ACTIVITY_TASK_STARTED                call_llm attempt=1 by cpu-3778641@lab10-cpu-tools
  13  ACTIVITY_TASK_SCHEDULED              execute_tool on lab10-cpu-tools
  26  ACTIVITY_TASK_SCHEDULED              execute_tool on lab10-gpu-tools priority_key=5
```

The Workflow Tasks all ran on `wf-<pid>@lab10-agent-workflows`; the Workflow never learned a host,
only a lane. (`worker_gpu.py` registers its `execute_tool` under the same Activity name — if a
CPU-only tool ever lands on the GPU lane it fails non-retryably with `BadArguments` instead of
quietly running on the wrong hardware. Registering the same name on two lanes is the whole trick.)

**Now stall the GPU pool.** Stop `worker_gpu.py` (Ctrl-C is fine here) and start a run with a short
Schedule-to-Start on the GPU step:

```bash
GPU_SCHEDULE_TO_START_SECONDS=15 python starter.py --id agent-stall --no-wait     # course value: 5 minutes
temporal task-queue describe --task-queue lab10-gpu-tools --task-queue-type activity
```

Observed while the Task sat in the lane:

```
Task Queue Statistics:
    BuildID    TaskQueueType  ApproximateBacklogCount  ApproximateBacklogAge  BacklogIncreaseRate  TasksAddRate  TasksDispatchRate
  UNVERSIONED  activity                             1  5.80730335s                     0.17219743    0.17219743                  0
Pollers:
    BuildID    TaskQueueType           Identity            LastAccessTime  RatePerSecond
  UNVERSIONED  activity       gpu-3777831@lab10-gpu-tools  39 seconds ago         100000
```

Backlog 1, age rising, dispatch rate 0. The pollers list still names the Worker you just killed —
the list is who polled in the last five minutes, and a live Worker polls at least once a minute, so
`LastAccessTime` over ~90 s means "gone", not "busy". `python five_questions.py lane lab10-gpu-tools`
prints the same numbers from the SDK's `DescribeTaskQueue` and flags stale pollers.

Fifteen seconds later the run is `Failed`. The history (`python five_questions.py history lab10-agent-stall`):

```
  26  ACTIVITY_TASK_SCHEDULED              execute_tool on lab10-gpu-tools priority_key=5
  27  ACTIVITY_TASK_TIMED_OUT              activity ScheduleToStart timeout retry_state=3
  31  UPSERT_WORKFLOW_SEARCH_ATTRIBUTES    AgentStatus, CurrentStep
  32  WORKFLOW_EXECUTION_FAILED            lane stalled: no Worker took embed within SCHEDULE_TO_START
```

Two things to notice. There is **no `ACTIVITY_TASK_STARTED`** between 26 and 27 — nothing ran, so
nothing can have had a side effect; that is what the timeout type tells an operator. And the
RetryPolicy on that call says `maximum_attempts=3`, yet there is one timeout event and no second
attempt: `retry_state=3` is `RETRY_STATE_TIMEOUT` — a Schedule-to-Start timeout is not retried
(retrying a Task nobody will pick up changes nothing). The Workflow catches the `ActivityError`,
checks `cause.type == TimeoutType.SCHEDULE_TO_START`, publishes `AgentStatus = stalled` and fails
with a named `ApplicationError("LaneStalled")` — so the fleet query in 10.2 can find every run that
died this way.

**Bring the pool back before the deadline.** Same thing with the course-like value, and start the
GPU Worker while the Task is queued:

```bash
python starter.py --id agent-drain --no-wait                     # default GPU_SCHEDULE_TO_START_SECONDS=60
temporal task-queue describe --task-queue lab10-gpu-tools --task-queue-type activity   # backlog 1–2, age climbing
python worker_gpu.py                                              # in the GPU terminal
temporal task-queue describe --task-queue lab10-gpu-tools --task-queue-type activity   # a few seconds later
```

Observed, before and after the Worker appeared:

```
  UNVERSIONED  activity   2  30.815259925s   0.06490294   0.06490294   0
  UNVERSIONED  activity   0  0s             -0.0037108138 0.029458467  0.03316928
  poller gpu-3779448@lab10-gpu-tools  5 seconds ago
```

The run completed with `lanes_used = [cpu, cpu, gpu]`. The backlog said 2 with only one live run
queued: the already-timed-out Task from `agent-stall` was still counted — `ApproximateBacklogCount`
is approximate, and a dead Task leaves the backlog when a poller pulls and discards it.

Write down: which lanes kept moving while `gpu-tools` was empty (both — `call_llm` and `search`
completed; only the `embed` step waited), and which single fact told you the pool was gone before
any run failed (backlog age climbing with dispatch rate 0 and no fresh poller).

## 10.2 The five questions, from the UI and from Python

Start a few runs so there is a fleet, and pause one right after starting it:

```bash
TOOL_SECONDS=3 python starter.py --id agent-44 --owner acct_7f3a --tier team --no-wait
temporal workflow signal -w lab10-agent-44 --name pause        # or: handle.signal(AgentRun.pause)
```

Observed a few seconds later — the Query, the Describe and the fleet query agree, and the run is
`RUNNING` as far as Temporal is concerned while `AgentStatus` says `paused` (a paused Workflow consumes
no Worker compute while waiting; its Workflow Task completed and nothing is polling for it):

```
query:       {'paused': True, 'status': 'paused', 'step': 2, 'lanes_used': ['lab10-cpu-tools', 'lab10-cpu-tools'], …}
describe SA: {'AgentStatus': 'paused', 'CurrentStep': 2, 'Owner': 'acct_7f3a'}
list_workflows("WorkflowType = 'AgentRun' AND AgentStatus = 'paused' AND Owner = 'acct_7f3a'"):
  lab10-agent-44   RUNNING   AgentStatus=paused CurrentStep=2 Owner=acct_7f3a
```

`temporal workflow signal -w lab10-agent-44 --name resume` and it finishes (`steps 3`). One bug this
lab shipped and fixed on the way: the loop republished `running` after the in-flight step completed,
overwriting the `paused` the Signal handler had just published — `paused: True, status: running`,
and the fleet query found nothing. The loop now republishes the handler's status. The Search
Attribute is only as truthful as the last upsert.

**Find, then read.** Visibility finds; History and Describe read. In the UI filter box, and in
`python five_questions.py list "<filter>"`, and in `temporal workflow list --query "<filter>"` — the same string:

```
WorkflowType = 'AgentRun' AND AgentStatus = 'stalled'
Owner = 'acct_7f3a' AND WorkflowId STARTS_WITH 'lab10-'
WorkflowType = 'AgentRun' AND AgentStatus = 'paused' AND Owner = 'acct_7f3a'
```

Observed:

```
list_workflows("WorkflowType = 'AgentRun' AND AgentStatus = 'stalled'"):
  lab10-agent-stall                FAILED     AgentStatus=stalled CurrentStep=2 Owner=acct_7f3a
list_workflows("Owner = 'acct_7f3a' AND WorkflowId STARTS_WITH 'lab10-'"):
  lab10-agent-drain                COMPLETED  AgentStatus=done CurrentStep=3 Owner=acct_7f3a
  lab10-agent-stall                FAILED     AgentStatus=stalled CurrentStep=2 Owner=acct_7f3a
  lab10-agent-42                   COMPLETED  AgentStatus=done CurrentStep=3 Owner=acct_7f3a
```

`Owner` was set by the starter on the first event (`start_workflow(search_attributes=TypedSearchAttributes([SearchAttributePair(OWNER, …)]))`);
`AgentStatus`/`CurrentStep` are upserted by `_publish()` and each upsert is an
`UPSERT_WORKFLOW_SEARCH_ATTRIBUTES` event in History — it replays like any other Command. The
Workflow's `status` Query stays the authoritative answer for one run; the Search Attribute is the
eventually-consistent fleet view of the same field.

**Then the five questions**, on a run you did not start. Pick one from the list; run
`python five_questions.py five <workflow-id>` and, in parallel, open the same run in the UI. Name the
surface that answered each:

| # | Question | Python | UI / CLI |
|---|---|---|---|
| 1 | What happened? | `handle.fetch_history()` — every event, in order | Event History / `temporal workflow show -w <id>` |
| 2 | What did Temporal think happened? | `handle.describe()` — `status`, `typed_search_attributes`, `raw_description.pending_activities` (state, attempt, last failure, last Worker identity) | summary + Pending Activities / `temporal workflow describe -w <id>` |
| 3 | What side effects may have occurred? | from the history: every `ACTIVITY_TASK_STARTED` with no terminal event is an attempt that ran; from describe: the attempt count and heartbeat details | Activity events; Pending Activities → heartbeat details |
| 4 | What happens next? | `pending_activities[].scheduled_time` (next attempt), pending timers, pending children | Pending Activities / Timers |
| 5 | What if every Worker disappears now? | `workflow_service.describe_task_queue(...)` per lane: backlog count, age, pollers | Task Queues page / `temporal task-queue describe --task-queue <lane> --task-queue-type activity` |

For `lab10-agent-stall` the script answered: 32 events; `status=FAILED`, no pending Activities,
`AgentStatus=stalled CurrentStep=2`; **0 attempts started without a terminal event** (so the GPU
tool never ran anywhere — the third question's answer for this run is "none"); nothing next; and for
each lane the pollers with their last-poll age. The fifth question is the only one Visibility helps
with, and only its "how many, which ones" half; the first four come from History and Describe, per
execution, strongly consistent.

## 10.3 Priority on the shared GPU lane — what this server actually does

Every GPU call in `workflows.py` carries a priority derived from the run's tier:

```python
priority=Priority(priority_key=PRIORITY_FOR_TIER[self.tier],       # enterprise 1, team 3, batch 5
                  fairness_key=self.owner or None,                   # one virtual queue per tenant
                  fairness_weight=FAIRNESS_WEIGHT_FOR_TIER[self.tier])
```

It is recorded on the `ACTIVITY_TASK_SCHEDULED` event (`priority_key=5` above), which is how you
check it landed. `priority_race.py` runs the experiment end to end: it hosts the workflow and CPU
Workers in-process, starts N batch runs **one at a time, each confirmed sitting in the GPU backlog
before the next starts**, then one enterprise run, prints the backlog, then starts one GPU Worker
with one slot and reads the `ACTIVITY_TASK_STARTED` order off the histories.

```bash
python priority_race.py --mode priority --batch 4      # 4 × priority 5 queued first, then 1 × priority 1
python priority_race.py --mode fairness --batch 3      # tenants a and b, same priority, enqueued a a a b b b
```

Prediction from the upstream rules (as of 2026-09): priority strict across tiers, so the enterprise
Task starts first although it was queued last; fairness round-robin across keys within a tier, so
`a b a b a b`. What was observed, on `temporalio/temporal:latest` = server 1.31.2, SDK 1.32.0:

| Server configuration | priority mode (enqueued `b1 b2 b3 b4 ent`) | fairness mode (enqueued `a1 a2 a3 b1 b2 b3`) |
|---|---|---|
| the lab's compose server, defaults | `b2 b3 b1 **ent** b4` — neither FIFO nor priority | `b2 a1 a3 b1 a2 b3` — neither |
| `--dynamic-config-value matching.numTaskqueueReadPartitions=1` and `…WritePartitions=1` | `**ent** b1 b2 b3 b4` — priority honoured, FIFO within the tier | `a1 a2 a3 b1 b2 b3` — strict FIFO, no fairness |
| partitions=1 **and** `--dynamic-config-value matching.enableFairness=true` | `**ent** b1 b2 b3 b4` | `a1 b1 a2 b2 a3 b3` — round-robin across tenants |

So on this server: **Priority needs no flag** — it is on by default, exactly as upstream says — but
the dev server's default of four Task Queue partitions spreads a five-Task backlog across partitions,
and a single poller drains them in partition order, so a small experiment shows priority only *within*
a partition (in the default run, `ent` did start before `b4`, which shared its partition). That is
the upstream limitation stated literally: both controls are enforced within one partition. **Fairness
needs `matching.enableFairness=true`**; without it dispatch within a tier is FIFO. The last two rows
were taken on throwaway containers started as

```bash
docker run -d --rm --name lab10-probe -p 7234:7233 -p 8234:8233 temporalio/temporal:latest server start-dev --ip 0.0.0.0 \
  --dynamic-config-value matching.numTaskqueueReadPartitions=1 --dynamic-config-value matching.numTaskqueueWritePartitions=1 \
  --dynamic-config-value matching.enableFairness=true \
  --search-attribute AgentStatus=Keyword --search-attribute CurrentStep=Int --search-attribute Owner=Keyword
TEMPORAL_ADDRESS=localhost:7234 python priority_race.py --mode priority
```

(the `--search-attribute` form of registration, verified there: `temporal operator search-attribute list` showed all three).
Neither `temporal task-queue config get` nor `describe` reports whether fairness is on — the only way
to know is the experiment, which is the point of the exercise. Record your own row: server version,
flags, observed order. In production, weights are set without a deploy
(`temporal task-queue config set --fairness-key-weight enterprise=5.0`, upstream, as of 2026-09; not
exercised here).

## Self-check

```bash
python -m pytest tests/      # 4 tests: routing per lane + Search Attributes + priority on the scheduled event;
                             # stalled lane → one Schedule-to-Start timeout, no retry, AgentStatus=stalled;
                             # GPU Worker refuses a CPU tool
```

Run each lab's tests from that lab's directory, one lab per `pytest` invocation: the labs are
self-contained and reuse flat module names (`worker_gpu`, `conftest`), so collecting two labs in one
process imports the wrong one (observed: a combined run from `labs/` hangs).
The tests run on the SDK's time-skipping test server, which also refuses unregistered Search
Attributes; the fixture registers them with the same Operator Service call as
`setup_search_attributes.py`. No Docker needed for the tests.

## The recurring question

If every Worker process dies right now: every lane's backlog, every pending Activity with its attempt
count, every timer and every Search Attribute survive in the Service. Workers returning to any lane
drain that lane; a lane with no Workers stalls only the Activities routed to it, and the
Schedule-to-Start timeout and `task-queue describe` are how that stall becomes visible — you saw both.

## What you learned

- Workflow, CPU, and GPU Task Queues are separate lanes with separate Worker pools and backlogs.
- A Schedule-to-Start timeout identifies work that no Worker picked up; Search Attributes make stalled runs findable.
- Queue age, dispatch rate, and last poll time are stronger operational signals than a stale poller name alone.

## Questions to answer

1. Which Event proves the GPU Activity never started during a lane stall?
2. Why does adding a CPU Worker not drain a GPU backlog?
3. What can the five-question operator view tell you without opening Worker logs?
4. How would you distinguish a slow GPU Activity from a GPU lane with no live Worker?
