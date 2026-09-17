# Lab 9 — Deploying Changed Workflow Code

Goal: `agent-42` started two months ago on AgentRun v1. Today you deploy v2, which inserts a
`verify` step after `execute_tool` (it runs the `evaluate` Activity and feeds the `Verdict` back
into state). Ship it three ways: fail the replay test and patch (9.1), run v1 and v2 Workers
side by side with Worker Versioning (9.2), and let a pinned agent upgrade at its next
continue-as-new (9.3).


```
labs/09-deploying-changed-workflow-code/
  workflows.py                 v1 — lab 8's AgentRun + the continue-as-new upgrade point; NO verify step
  workflows_v2.py              v2 — verify inserted; the naive change AND the final code (deploy 3)
  workflows_v2_patched.py      deploy 1: `if workflow.patched("insert-verify-step")`
  workflows_v2_deprecated.py   deploy 2: `workflow.deprecate_patch("insert-verify-step")`
  code_versions.py             AGENTRUN_CODE=v1|v2|v2_patched|v2_deprecated → which file a Worker runs
  worker.py                    one Worker; BUILD_ID=... makes it a version of deployment `agent-runs`
  starter.py                   --id / --pause / --auto-upgrade / --wait
  routing.py                   describe / set-current / set-ramping / show / resume — the CLI forms, in Python
  export_history.py            as in lab 8
  tests/test_91_patching.py    the replay matrix (9.1)
  tests/test_92_v2_loop.py     the v2 loop in the time-skipping environment
  tests/histories/             lab08-agent-42 (v1), lab09-agent-43 (patched), lab09-agent-44 (v2 final)
```

`python -m pytest` from this directory — observed `17 passed in 1.61s`; nothing here needs the
dev server except 9.2 and 9.3. Shared server: `export TASK_QUEUE=lab-09 DEPLOYMENT_NAME=lab09-agent-runs`
and prefix Workflow IDs with `lab09-`; the commands below are written that way because that is how
they were run. Versions are marked **(as of 2026-09, temporalio 1.32.0, Server 1.31.2)**: this is
the most volatile page in the course, check the upstream page before relying on any shape here.

The v2 change is one method — read `workflows_v2.py` first. Where v1's `_tool_step` records the
tool result, v2 first schedules `evaluate`; at that point in a v1 history the next event is
`ActivityTaskScheduled call_llm`.

## 9.1 Patch: fail the replay test, then make both branches agree

```bash
python -m pytest tests/test_91_patching.py -v
```

`test_replay_matrix` replays each recorded history under each of the four files. Every cell
below was observed; the error text is what `Replayer` (and a real Worker) reports:

| history ↓ / code → | `v1` | `v2` (naive / final) | `v2_patched` | `v2_deprecated` |
|---|---|---|---|---|
| `lab08-agent-42` — v1, no marker | clean | **fails** `Activity type of scheduled event 'call_llm' does not match activity type of activity command 'evaluate'` | clean | **fails** (same message) |
| `lab09-agent-43` — patched, marker + evaluate | **fails** `Non-deprecated patch marker encountered for change insert-verify-step, but there is no corresponding change command!` | **fails** (same) | clean | clean |
| `lab09-agent-44` — v2 final, evaluate, no marker | **fails** `scheduled event 'evaluate' does not match ... command 'call_llm'` | clean | **fails** (same) | clean |

Walk it as the three deploys:

1. **The naive change.** Deploy `workflows_v2.py` over `agent-42`'s v1 history: the `v2` column,
   first row. `v1` scheduled `call_llm` after the tool; `v2` schedules `evaluate` at the same
   event. This is module 8's replay test doing its job before the deploy.
2. **Deploy 1 — `patched`.** `workflows_v2_patched.py`: `if workflow.patched("insert-verify-step")`
   → verify; `else: pass`. The `v2_patched` column: the v1 history replays clean (no marker → old
   branch) and a new execution records the marker and verifies. Record one yourself:
   `AGENTRUN_CODE=v2_patched python worker.py`, `python starter.py --id lab09-agent-43 --wait`,
   `python export_history.py lab09-agent-43`. Observed in that history: event 17
   `MARKER_RECORDED`, event 19 `ActivityTaskScheduled evaluate`.
3. **Deploy 2 — `deprecate_patch`.** `workflows_v2_deprecated.py` drops the branch and keeps the
   marker. Its column: the patched history is clean, the v1 history **is not** — deploy 2 waits
   until no execution can still take the v1 branch. "Closed" is not that line: a closed history is
   still replayed by your CI corpus (8.2), by a reset, and by any investigation — and 9.1's matrix
   shows a *retained* v1 history failing under naive v2 long after its execution ended. Retention
   is the clean boundary: keep the compatibility branch for as long as you keep histories you may
   replay.
4. **Deploy 3 — remove the call.** `workflows_v2.py` again, now read as the final code. Its
   column: the patched history **fails** — "non-deprecated patch marker encountered … no
   corresponding change command". Deploy 3 waits until every execution that *recorded the
   marker* is gone too. Two waits, not one; the matrix is the argument.

`test_the_naive_change_breaks_old_histories`, `test_the_patch_makes_both_branches_replay_clean`,
`test_deprecating_the_patch_needs_no_pre_patch_execution_left` and
`test_removing_the_patch_call_needs_no_marked_execution_left` are the four sentences above as
single tests. `test_92_v2_loop.py` checks the verify step behaves (a rejected `Verdict` counts as
a failed attempt; the model is asked again) in the time-skipping environment.

## 9.2 Worker Versioning: v1 and v2 side by side

**No server flag was needed.** The dev server in `docker-compose.yml` (Server 1.31.2) accepted
a versioned Worker as-is: `WorkerDeploymentConfig(version=WorkerDeploymentVersion(deployment_name,
build_id), use_worker_versioning=True, default_versioning_behavior=PINNED)` (see `worker.py`),
and the deployment appeared in `temporal worker deployment describe`. Worker Versioning is GA
(2026-03-30); Pinned / Auto-Upgrade are its core concepts.

One observed trap before you start. The upstream shape declares the behavior per Workflow
Type — `@workflow.defn(versioning_behavior=VersioningBehavior.PINNED)` — and the first version
of this lab did. Server 1.31.2 then **rejected every Workflow Task completion from an
unversioned Worker** (`versioning behavior cannot be specified without deployment options being
set with versioned mode`, a WARN in the Worker log): the task timed out after 10 s, was
rescheduled, timed out again, and the history showed nothing but `WorkflowTaskScheduled /
WorkflowTaskStarted / WorkflowTaskTimedOut`. A Worker that cannot complete Workflow Tasks looks
exactly like no Worker at all. The lab therefore declares PINNED as the Worker deployment's
default so the same code also runs unversioned (9.1); if you annotate the class, run every
Worker with a `BUILD_ID`.

Four terminals; `export TASK_QUEUE=lab-09 DEPLOYMENT_NAME=lab09-agent-runs` in each.
(`AGENTRUN_TOOL_TICK_SECONDS` slows the tool when you need to catch a run mid-step; 9.2b uses it.)

```bash
# 1. a v1 Worker, versioned, and make v1 the Current Version
AGENTRUN_CODE=v1 BUILD_ID=v1 python worker.py
python routing.py set-current v1
#   deployment lab09-agent-runs: current=lab09-agent-runs.v1 ramping=- (0%)
#     build v1             CURRENT

# 2. two executions parked at the 72 h checkpoint: one pinned (the class default), one auto-upgrade
python starter.py --id lab09-agent-42 --pause
#   lab09-agent-42 ... status=RUNNING behavior=PINNED version=lab09-agent-runs.v1
python starter.py --id lab09-agent-46 --pause --auto-upgrade
#   lab09-agent-46 ... status=RUNNING behavior=PINNED version=lab09-agent-runs.v1 override=AUTO_UPGRADE

# 3. a v2 Worker beside it; ramp, then switch
AGENTRUN_CODE=v2 BUILD_ID=v2 python worker.py
python routing.py set-ramping v2 10
#     build v2             RAMPING        build v1             CURRENT
python routing.py set-current v2
#   deployment lab09-agent-runs: current=lab09-agent-runs.v2 ramping=- (0%)
#     build v2             CURRENT        build v1             DRAINING

# 4. a new execution after the switch, then wake all three
python starter.py --id lab09-agent-45 --pause
#   lab09-agent-45 ... behavior=PINNED version=lab09-agent-runs.v2
for w in lab09-agent-42 lab09-agent-46 lab09-agent-45; do python routing.py resume $w; done
for w in lab09-agent-42 lab09-agent-46 lab09-agent-45; do python routing.py show $w; done
```

Observed after the resumes:

```
lab09-agent-42  status=COMPLETED behavior=PINNED version=lab09-agent-runs.v1
lab09-agent-46  status=COMPLETED behavior=PINNED version=lab09-agent-runs.v2 override=AUTO_UPGRADE
lab09-agent-45  status=COMPLETED behavior=PINNED version=lab09-agent-runs.v2
```

and in the histories (Worker identities on `WorkflowTaskStarted`, plus whether `evaluate` was
ever scheduled): `agent-42` — every task on the v1 Worker, no `evaluate`; `agent-46` — tasks on
v1 *then* v2, `evaluate` scheduled: the auto-upgrade execution moved at its next Workflow Task
after `set-current v2`; `agent-45` — v2 only. `v1` reads `DRAINING` in `describe` while
`agent-42` is open on it and stays so until the last pinned execution on it closes — that, not
the deploy, is when the v1 Workers can go. Read the same in the Web UI: each execution's
Versioning Info block, and Worker Deployments in the left nav.

### 9.2b Auto-upgrade does not make incompatible code safe

`agent-46` moved from v1 to v2 mid-run and finished. Read why carefully, because the wrong lesson
is available here: **Worker Versioning did not make naive v2 replay-safe.** `agent-46` was parked
at the checkpoint *before* its history crossed the code path v2 changed, so the v2 Worker replayed
a prefix that v1 and v2 agree on. Auto-upgrade means the run may change build mid-flight — which
is precisely why it must stay replay-compatible with the history it already has.

Park one past the divergence and watch it break. The tool is quick by default, so slow it down
enough to send `pause` while a step is in flight:

```bash
export TASK_QUEUE=lab-09 DEPLOYMENT_NAME=lab09-agent-runs AGENTRUN_TOOL_TICK_SECONDS=1
AGENTRUN_CODE=v1 BUILD_ID=v1 python worker.py     ;  python routing.py set-current v1
python starter.py --id lab09-agent-47 --pause --auto-upgrade
python routing.py resume lab09-agent-47   # then, ~1.8 s later, while execute_tool is running:
#   (send `pause` again — the run parks at the next checkpoint, one execute_tool already in history)
AGENTRUN_CODE=v2 BUILD_ID=v2 python worker.py     ;  python routing.py set-current v2
python routing.py resume lab09-agent-47
```

Measured (Server 1.31.2, temporalio 1.32.0, 2026-09):

```text
44  WORKFLOW_TASK_FAILED   cause=24
    [TMPRL1100] Nondeterminism error: Activity type of scheduled event 'call_llm'
    does not match activity type of activity command 'evaluate'
45  WORKFLOW_TASK_SCHEDULED        ← and again, and again: the task retries forever
```

The execution stays RUNNING with a failing Workflow Task — 9.1's nondeterminism, arriving through
the deployment system instead of through a redeploy. Now do it again with the patched build
(`AGENTRUN_CODE=v2_patched BUILD_ID=v2p`, `set-current v2p`): zero `WorkflowTaskFailed`, the run
completes, `evaluate` scheduled only where `workflow.patched()` allows it.

So the three deployment strategies differ in exactly one respect — whether a run can be made to
replay history under code that did not write it:

| Strategy | Incompatible code change allowed? | Why |
|---|---|---|
| Pinned, same run | yes, between builds | the run never leaves the build that started it |
| **Auto-upgrade, same run** | **no** | the new build must replay the existing history — patch it |
| Pinned + upgrade at continue-as-new | yes, at the boundary | the next run starts on a fresh history |
| Unversioned + `patched()` | yes, via the compatibility branch | both histories produce matching Commands |

Pinning buys you the freedom to change code. Auto-upgrade buys you a fleet that converges. Only
continue-as-new buys you both, which is 9.3.

CLI forms, from `--help` of the CLI in the container (1.8.3), for a box that has it —
`routing.py` is what the lab actually ran:

```bash
docker exec labs-temporal-1 temporal worker deployment describe --name lab09-agent-runs           # verified
docker exec labs-temporal-1 temporal worker deployment set-current-version --deployment-name lab09-agent-runs --build-id v2
docker exec labs-temporal-1 temporal worker deployment set-ramping-version --deployment-name lab09-agent-runs --build-id v2 --percentage 10
docker exec labs-temporal-1 temporal workflow update-options -w lab09-agent-42 --versioning-override-behavior auto_upgrade
```

Ramp it for real: leave `v2` at 10 % and start ten executions with different IDs; count who lands
where (routing by Workflow ID hash — expect roughly one). Not run here.

## 9.3 Pinned + continue-as-new: the upgrade point of a long-lived agent

**Upgrade on Continue-as-New is Public Preview (as of 2026-09; "experimental" in the SDK
docstring).** The mechanism in `workflows.py`, `_continue_as_new()`: when
`workflow.info().is_target_worker_deployment_version_changed()` is true, the run continues-as-new
with `initial_versioning_behavior=ContinueAsNewVersioningBehavior.AUTO_UPGRADE` — the new run
starts on the Workflow's **Target Version**, the version it would move to next, and then pins
there; otherwise the new run inherits the pinned version. In the run below Target happens to equal
Current (`v2`), which is why the observation reads the same either way; under a ramp it need not. The course rule for *when* to continue-as-new is `is_continue_as_new_suggested()`;
the lab knob `AGENTRUN_STEPS_PER_RUN=2` makes it happen after two steps so you can watch.

```bash
export TASK_QUEUE=lab-09 DEPLOYMENT_NAME=lab09-agent-runs AGENTRUN_STEPS_PER_RUN=2
AGENTRUN_CODE=v1 BUILD_ID=v1 python worker.py     ;  python routing.py set-current v1
python starter.py --id lab09-agent-47 --pause      #  behavior=PINNED version=lab09-agent-runs.v1
AGENTRUN_CODE=v2 BUILD_ID=v2 python worker.py     ;  python routing.py set-current v2
python routing.py resume lab09-agent-47
python routing.py show lab09-agent-47              #  a few seconds later
```

Observed: `lab09-agent-47 run=6f2186a2… status=COMPLETED behavior=PINNED version=lab09-agent-runs.v2`
— a different Run ID from the one the starter printed, on v2. The chain, one Workflow ID,
three Run IDs (`temporal workflow list --query "WorkflowId = 'lab09-agent-47'"` or the UI):

```
run 76c96fb2  PINNED v1   tasks on the v1 Worker   call_llm, execute_tool            → ContinuedAsNew
run c5908cff  PINNED v2   tasks on the v2 Worker   call_llm, execute_tool, evaluate  → ContinuedAsNew
                          its WorkflowExecutionStarted carries inherited_auto_upgrade_info
                          (source_deployment_version build_id "v1")
run 6f2186a2  PINNED v2   tasks on the v2 Worker   call_llm                          → Completed
                          its WorkflowExecutionStarted carries inherited_pinned_version build_id "v2"
```

Run 1 noticed the target change at its continue-as-new and started run 2 with AUTO_UPGRADE; run 2
landed on v2 (its first `evaluate` proves the code), pinned there, and run 3 inherited that pin
because nothing had changed since. Two upstream limitations to keep in mind: the target-changed
flag is refreshed per Workflow Task, so a parked agent notices only when it wakes (the `resume`
above did that), and v2 must accept v1's continue-as-new input — keep `AgentState` additive.

Not verified here (mark as expected, read the runbook first): what happens when every v1 Worker
is gone while `agent-42` is still pinned to v1 — its task waits, and the way out is a versioning
override (`temporal workflow update-options --versioning-override-behavior pinned --versioning-
override-deployment-name … --versioning-override-build-id v2`) onto a build whose code can
replay its history, i.e. `v2_patched`, never `v2`. The 9.1 matrix already told you why.

## If every Worker process dies right now, what information survives, and what happens when a Worker comes back?

The history survives, and it was written by v1 code; nothing in it says which version is coming
back. With the patch: a v2 Worker replays a history with no marker, takes the old branch, and
continues cleanly. With versioning: the Service routes a pinned execution's task only to a v1
Worker; if none returns, the task waits — that is the recovery page. With neither: a v2 Worker
replays, fails the Workflow Task with the error in the 9.1 matrix, and the execution stalls
until code and history agree again.

## What you learned

- A patch marker lets old histories take the old branch while new runs use changed Workflow code.
- Worker Versioning routes pinned executions to compatible builds; routing alone does not make incompatible code replayable.
- Continue-as-new gives a long-lived agent a controlled upgrade point with a fresh history.

## Questions to answer

1. What does `workflow.patched()` return when replaying a history with no patch marker?
2. What happens to a pinned v1 execution if every v1 Worker disappears?
3. Why is sending an old run straight to naive v2 unsafe even if v2 is the current deployment?
4. At what boundary can the long-lived agent move to a new build without replaying its old history there?
