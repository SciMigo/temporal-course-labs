"""Lab 9: AgentRun v1 — the code that has been running for two months.

Byte-for-byte the loop from lab 8 (plan → call_llm → execute_tool, Signals, Query, Update,
72-hour checkpoint, continue-as-new, compensation) plus the three things this lab is about:

* Pinned by default. `worker.py` declares `default_versioning_behavior=PINNED` on its
  deployment config, so with a versioned Worker (`BUILD_ID=... python worker.py`) every
  AgentRun finishes its run on the build that started it. (Declaring it on the class —
  `@workflow.defn(versioning_behavior=...)` — is the upstream shape, but Server 1.31.2 then
  rejects every Workflow Task completion from an *unversioned* Worker, and 9.1 runs unversioned.)
* `AGENTRUN_STEPS_PER_RUN` — a lab knob. The course rule is `is_continue_as_new_suggested()`;
  setting the knob to 2 makes the agent continue-as-new after two steps so you can watch the
  boundary without 500 steps.
* Upgrade on continue-as-new (Public Preview as of 2026-09): when the Service reports a new
  Target Version, the next run starts with AUTO_UPGRADE and lands on it, then pins there.

v1 has no verify step. `workflows_v2*.py` are the change.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ContinueAsNewVersioningBehavior
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

TASK_QUEUE = os.environ.get("TASK_QUEUE", "agent-runs")
BUILD_ID = "v1"                                 # what this file is, for log lines
# Lab knob: continue-as-new after this many steps per run (0 = only when the Service suggests it).
STEPS_PER_RUN = int(os.environ.get("AGENTRUN_STEPS_PER_RUN", "0"))

# The agent parks here when paused and waits for a human. Three days, not three seconds:
# the time-skipping test environment is what makes this testable.
CHECKPOINT_TIMEOUT = timedelta(hours=72)
MAX_TOOL_FAILURES = 3          # plan() gives up after this many failed tool steps
TOOL_HEARTBEATS = 3            # execute_tool heartbeats this many times per attempt
# ... this far apart. A knob, because 9.2b needs the tool slow enough to send `pause` while a
# step is in flight: AGENTRUN_TOOL_TICK_SECONDS=1 makes one tool call take ~3 s.
TOOL_TICK_SECONDS = float(os.environ.get("AGENTRUN_TOOL_TICK_SECONDS", "0.05"))


# ---------------------------------------------------------------- data types
@dataclass
class Step:
    kind: str                  # "llm" | "tool" | "finish"
    prompt: str = ""
    tool: str = ""
    args: dict = field(default_factory=dict)


@dataclass
class ToolCall:
    key: str                   # idempotency key: f"{workflow_id}:{step}:{sha256(tool+args)[:12]}"
    tool: str
    args: dict


@dataclass
class ToolResult:
    key: str
    ok: bool
    output: str


@dataclass
class AgentState:
    """The snapshot continue_as_new carries into the next run (L06)."""
    step: int
    goal: str
    context_summary: str
    attempts: int
    processed_command_ids: list[str]


# ---------------------------------------------------------------- activities
@activity.defn
async def call_llm(prompt: str, key: str) -> str:
    """The model call as an Activity. This stand-in follows three rules so the loop is
    deterministic: with no tool result in the prompt it asks for `fetch`; with one it asks
    for `summarize`; with two it answers. Tests replace it with their own fake."""
    activity.logger.info("call_llm key=%s", key)
    ok_results = prompt.count("RESULT: ok")
    if ok_results == 0:
        return "TOOL: fetch temporal-docs"
    if ok_results == 1:
        return "TOOL: summarize temporal-docs"
    return "FINAL: the agent fetched and summarized the docs"


# The tool's system of record: one effect per key. In production this is a table keyed by
# the idempotency key; here it is a dict in the Worker process, which is exactly enough to
# test "run it twice with the same key, assert one effect" (8.3) and exactly *not* enough to
# survive a Worker death (8.4 shows why the real one must be outside the process).
_EFFECTS: dict[str, ToolResult] = {}
_EFFECT_LOG: list[str] = []    # every side effect that actually happened, in order


def effects() -> list[str]:
    return list(_EFFECT_LOG)


def reset_effects() -> None:
    _EFFECTS.clear()
    _EFFECT_LOG.clear()


@activity.defn
async def execute_tool(call: ToolCall) -> ToolResult:
    """Run a tool. Heartbeats so a long tool can be cancelled and its progress seen from
    `describe`; idempotent by `call.key` so a retry after a Worker death does not repeat
    the side effect."""
    prior = _EFFECTS.get(call.key)
    if prior is not None:
        activity.logger.info("execute_tool key=%s already done; returning recorded result", call.key)
        return prior
    if call.tool not in ("fetch", "summarize", "undo_fetch", "undo_summarize"):
        raise ApplicationError(f"unknown tool {call.tool!r}", type="BadToolArguments", non_retryable=True)
    for i in range(TOOL_HEARTBEATS):
        # Heartbeat details are what a retry (or a forensic reader) sees as "how far it got".
        activity.heartbeat(f"{call.tool}:{i + 1}/{TOOL_HEARTBEATS}")
        await asyncio.sleep(TOOL_TICK_SECONDS)   # cancellation arrives here
    _EFFECT_LOG.append(f"{call.tool}({call.args.get('target', '')})")   # the side effect
    result = ToolResult(key=call.key, ok=True, output=f"{call.tool}: {call.args.get('target', '')} done")
    _EFFECTS[call.key] = result
    return result


# ---------------------------------------------------------------- the workflow
@workflow.defn
class AgentRun:
    def __init__(self) -> None:
        self.step = 0
        self.status = "running"
        self.goal = ""
        self.context_summary = ""
        self.attempts = 0
        self.paused = False
        self.processed_command_ids: set[str] = set()
        self.compensations: list[ToolCall] = []

    # -- plan(): Workflow code, a pure function of state. Never calls a model. --
    def plan(self) -> Step:
        if self.status == "cancelling":
            return Step("finish")
        if self.attempts >= MAX_TOOL_FAILURES:
            return Step("finish")
        last = self.context_summary.rsplit("\n", 1)[-1]
        if self.step == 0 or not last:
            return Step("llm", prompt=f"Goal: {self.goal}\nMake a plan.")
        if last.startswith("TOOL:"):
            tool, _, target = last[len("TOOL:"):].strip().partition(" ")
            return Step("tool", tool=tool, args={"target": target})
        if last.startswith("FINAL:"):
            return Step("finish")
        # A RESULT line (ok or error): ask the model what to do next.
        return Step("llm", prompt=f"Goal: {self.goal}\n{self.context_summary}\nWhat next?")

    def snapshot(self) -> AgentState:
        return AgentState(self.step, self.goal, self.context_summary, self.attempts,
                          sorted(self.processed_command_ids))

    # -- Signals / Query / Update (L05) --
    @workflow.signal
    def pause(self) -> None:
        self.paused = True

    @workflow.signal
    def resume(self) -> None:
        self.paused = False

    @workflow.signal
    def cancel_run(self) -> None:
        self.status = "cancelling"

    @workflow.signal
    def inject_context(self, text: str, command_id: str) -> None:
        if command_id in self.processed_command_ids:
            return                                   # a retried Signal is ignored
        self.processed_command_ids.add(command_id)
        self.context_summary += f"\nNOTE: {text}"

    @workflow.query
    def status(self) -> dict:
        return {"step": self.step, "status": self.status, "goal": self.goal,
                "attempts": self.attempts, "paused": self.paused,
                "context_summary": self.context_summary}

    @workflow.update
    def change_goal(self, goal: str, command_id: str) -> str:
        if command_id in self.processed_command_ids:
            return self.goal                         # a retried Update returns the same answer
        self.processed_command_ids.add(command_id)
        self.goal = goal
        return self.goal

    @change_goal.validator
    def _validate_change_goal(self, goal: str, command_id: str) -> None:
        if not goal.strip():
            raise ValueError("goal must not be empty")

    # -- the loop --
    @workflow.run
    async def run(self, goal: str, state: AgentState | None = None) -> str:
        self.goal = goal
        if state is not None:                        # continued-as-new: restore the snapshot
            self.step, self.goal = state.step, state.goal
            self.context_summary, self.attempts = state.context_summary, state.attempts
            self.processed_command_ids = set(state.processed_command_ids)
        run_start_step = self.step
        workflow.logger.info("AgentRun %s starting at step %d", BUILD_ID, self.step)
        try:
            while True:
                if self.paused:
                    self.status = "paused"
                    try:
                        await workflow.wait_condition(
                            lambda: not self.paused or self.status == "cancelling",
                            timeout=CHECKPOINT_TIMEOUT)
                    except asyncio.TimeoutError:
                        self.status = "expired"
                        return "expired: no resume within 72h"
                    if self.status != "cancelling":
                        self.status = "running"
                step = self.plan()
                if step.kind == "finish":
                    self.status = "cancelled" if self.status == "cancelling" else "done"
                    if self.status == "cancelled":
                        await self._compensate()
                    return self._final_answer()
                if step.kind == "llm":
                    await self._llm_step(step)
                elif step.kind == "tool":
                    await self._tool_step(step)
                self.step += 1
                if self._should_continue_as_new(run_start_step):
                    self._continue_as_new()
        except (asyncio.CancelledError, ActivityError) as err:
            # Workflow cancellation (handle.cancel()) is cooperative: it surfaces at the await
            # we are blocked on — as asyncio.CancelledError on a timer or wait_condition, as an
            # ActivityError whose cause is CancelledError on a running Activity. (Lab 8.4's
            # cancellation injection found the version of this code that treated the second
            # form as a failed tool step and kept going.) Undo what was done, then let the
            # cancellation stand so the execution closes as Cancelled.
            if isinstance(err, ActivityError) and not isinstance(err.cause, CancelledError):
                raise
            self.status = "cancelled"
            await self._compensate()
            raise

    def _should_continue_as_new(self, run_start_step: int) -> bool:
        if workflow.info().is_continue_as_new_suggested():
            return True
        return STEPS_PER_RUN > 0 and self.step - run_start_step >= STEPS_PER_RUN

    def _continue_as_new(self) -> None:
        """The upgrade point of a long-lived pinned agent (Public Preview as of 2026-09). When
        the Service has told this run that a newer Target Version exists, the next run starts
        with AUTO_UPGRADE — it lands on the current version, then pins there. Otherwise the
        next run inherits this run's pinned version."""
        behavior = None
        if workflow.info().is_target_worker_deployment_version_changed():
            workflow.logger.info("target version changed; next run will auto-upgrade")
            behavior = ContinueAsNewVersioningBehavior.AUTO_UPGRADE
        workflow.continue_as_new(args=[self.goal, self.snapshot()], initial_versioning_behavior=behavior)

    async def _llm_step(self, step: Step) -> None:
        key = f"{workflow.info().workflow_id}:{self.step}:{_sha(step.prompt)}"
        answer = await workflow.execute_activity(
            call_llm, args=[step.prompt, key],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3))
        self.context_summary += f"\n{answer}"

    async def _tool_step(self, step: Step) -> None:
        call, result = await self._run_tool(step)
        if result is None:
            return                                   # failure already recorded
        self._record_tool_result(call, result)

    async def _run_tool(self, step: Step) -> tuple[ToolCall, ToolResult | None]:
        """Schedule execute_tool; on a spent RetryPolicy record the failure and return None."""
        call = ToolCall(
            key=f"{workflow.info().workflow_id}:{self.step}:{_sha(step.tool + repr(sorted(step.args.items())))}",
            tool=step.tool, args=step.args)
        try:
            result = await workflow.execute_activity(
                execute_tool, call,
                start_to_close_timeout=timedelta(seconds=30),
                heartbeat_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1),
                                         backoff_coefficient=1.0, maximum_attempts=3))
        except ActivityError as err:
            if isinstance(err.cause, CancelledError):
                raise            # cancellation is not a failed tool step; run() handles it
            self.attempts += 1
            self.context_summary += f"\nRESULT: error {_failure_label(err)}"
            return call, None
        return call, result

    def _record_tool_result(self, call: ToolCall, result: ToolResult) -> None:
        if result.ok:
            self.context_summary += f"\nRESULT: ok {result.output}"
            self.compensations.append(ToolCall(key=call.key + ":undo", tool=f"undo_{call.tool}", args=call.args))
        else:
            self.attempts += 1
            self.context_summary += f"\nRESULT: error {result.output}"

    async def _compensate(self) -> None:
        """L07: undo completed tool steps in reverse. Each undo is itself a keyed tool call."""
        while self.compensations:
            undo = self.compensations.pop()
            await workflow.execute_activity(
                execute_tool, undo, start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3))

    def _final_answer(self) -> str:
        if self.status == "cancelled":
            return "cancelled"
        if self.attempts >= MAX_TOOL_FAILURES:
            return f"gave up after {self.attempts} failed tool steps"
        last = self.context_summary.rsplit("\n", 1)[-1]
        return last.removeprefix("FINAL:").strip()


def _failure_label(err: ActivityError) -> str:
    """'BadToolArguments' for an ApplicationError with a type, else the failure class name
    ('TimeoutError', 'CancelledError'): what plan() and the forensic reader get to see."""
    cause = err.cause
    if isinstance(cause, ApplicationError) and cause.type:
        return cause.type
    return type(cause).__name__


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:12]
