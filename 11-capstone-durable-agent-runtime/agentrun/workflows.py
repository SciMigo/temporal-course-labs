"""AgentRun, the full program. Every box of the capstone diagram is a method or a call here.

    plan()                       decision, Workflow code, pure function of state
    call_llm / execute_tool /    Activities on lanes, keyed, timed out, retried, heartbeating
    evaluate
    ResearchAgent                Child Workflow, id = f"{wf_id}/research/{step}"
    pause/resume/cancel_run/     Signals (async writes)
    inject_context
    change_goal                  Update with validator (tracked write); command_id dedupe
    status                       Query (read)
    continue_as_new(AgentState)  when the Service suggests it, or every STEPS_PER_RUN steps
    AgentStatus/CurrentStep      Search Attributes, upserted where state changes
    compensations                the module 7 saga list, run in reverse on cancel
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import timedelta

from temporalio import workflow
from temporalio.common import Priority, RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError, TimeoutType, is_cancelled_exception

from .activities import call_llm, compensate, evaluate, execute_tool
from .lanes import AGENT_WORKFLOWS, CPU_TOOLS, GPU_TOOL_NAMES, LANE_FOR_TOOL
from .models import AgentState, Step, ToolCall, ToolResult, Verdict
from .search_attributes import AGENT_STATUS, CURRENT_STEP, OWNER

# Knobs. Course values in comments; the lab shrinks them so a 200-step run takes seconds.
STEPS_PER_RUN = int(os.environ.get("AGENTRUN_STEPS_PER_RUN", "50"))      # continue-as-new cadence
RESEARCH_EVERY = int(os.environ.get("AGENTRUN_RESEARCH_EVERY", "4"))     # a ResearchAgent child every N cycles
DEFAULT_MAX_STEPS = 8
TOOL_START_TO_CLOSE = timedelta(seconds=float(os.environ.get("AGENTRUN_TOOL_START_TO_CLOSE_SECONDS", "60")))  # course: 30 min
TOOL_HEARTBEAT = timedelta(seconds=float(os.environ.get("AGENTRUN_TOOL_HEARTBEAT_SECONDS", "5")))            # course: 30 s
GPU_SCHEDULE_TO_START = timedelta(seconds=float(os.environ.get("AGENTRUN_GPU_SCHEDULE_TO_START_SECONDS", "120")))  # course: 5 min

LLM_RETRY = RetryPolicy(initial_interval=timedelta(milliseconds=200), backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=5), maximum_attempts=5,
                        non_retryable_error_types=["BadArguments"])
TOOL_RETRY = RetryPolicy(initial_interval=timedelta(milliseconds=200), backoff_coefficient=2.0,
                         maximum_interval=timedelta(seconds=5), maximum_attempts=5,
                         non_retryable_error_types=["BadArguments"])
PRIORITY_FOR_TIER = {"enterprise": 1, "team": 3, "batch": 5}
FAIRNESS_WEIGHT_FOR_TIER = {"enterprise": 5.0, "team": 3.0, "batch": 1.0}


def _digest(*parts: str) -> str:
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:12]


@workflow.defn
class ResearchAgent:
    """The sub-agent (module 6). Its own history, its own replay; the parent sees one result."""

    @workflow.run
    async def run(self, question: str) -> str:
        key = f"{workflow.info().workflow_id}:0:{_digest(question)}"
        return await workflow.execute_activity(
            call_llm, args=[f"research: {question}", key], task_queue=CPU_TOOLS,
            schedule_to_close_timeout=timedelta(minutes=2), start_to_close_timeout=timedelta(seconds=30),
            retry_policy=LLM_RETRY)


@workflow.defn
class AgentRun:
    def __init__(self) -> None:
        self.step = 0
        self.status = "starting"
        self.goal = ""
        self.context_summary = ""
        self.attempts = 0                  # retries that happened (sum of attempt-1 over results)
        self.paused = False
        self.cancel_requested = False
        self.processed_command_ids: set[str] = set()
        self.compensations: list[str] = []
        self.owner = ""
        self.tier = "batch"
        self.max_steps = DEFAULT_MAX_STEPS
        self.run_started_at_step = 0
        self.duplicates_suppressed = 0
        self.children: list[str] = []

    # ------------------------------------------------------------------ decisions
    def plan(self) -> Step:
        """Pure function of state. Never calls a model (module 3)."""
        if self.step >= self.max_steps:
            return Step("finish")
        phase = self.step % 4
        if phase == 0:
            return Step("llm", prompt=f"step {self.step}: plan the next move toward: {self.goal}")
        if phase == 1:
            return Step("tool", tool="search", args={"q": self.goal, "step": self.step})
        if phase == 2:
            return Step("tool", tool="embed", args={"text": self.context_summary[:40], "step": self.step})
        if (self.step // 4) % RESEARCH_EVERY == 0:
            return Step("research", question=f"what is still missing for: {self.goal}")
        return Step("llm", prompt=f"step {self.step}: reflect on: {self.context_summary[:40]}")

    def snapshot(self) -> AgentState:
        return AgentState(self.step, self.goal, self.context_summary, self.attempts, sorted(self.processed_command_ids))

    def _publish(self, status: str) -> None:
        self.status = status
        workflow.upsert_search_attributes([AGENT_STATUS.value_set(status), CURRENT_STEP.value_set(self.step)])

    def _should_continue_as_new(self) -> bool:
        if self.step == self.run_started_at_step:
            return False  # never on the first step of a run: that would loop forever
        return workflow.info().is_continue_as_new_suggested() or (self.step - self.run_started_at_step) >= STEPS_PER_RUN

    # ------------------------------------------------------------------ control surface
    @workflow.signal
    def pause(self) -> None:
        self.paused = True
        self._publish("paused")

    @workflow.signal
    def resume(self) -> None:
        self.paused = False
        self._publish("running")

    @workflow.signal
    def cancel_run(self) -> None:
        """Cooperative cancel: the loop notices at its next decision, compensates, returns cleanly.
        (handle.cancel() is the other kind — Temporal's — handled in run() below.)"""
        self.cancel_requested = True
        self.paused = False

    @workflow.signal
    def inject_context(self, text: str, command_id: str) -> None:
        if command_id in self.processed_command_ids:
            return  # a retried Signal is delivered again; the command id makes the second delivery a no-op
        self.processed_command_ids.add(command_id)
        self.context_summary = f"{self.context_summary}\n[injected: {text}]"

    @workflow.query
    def status(self) -> dict:
        return {"step": self.step, "status": self.status, "goal": self.goal, "paused": self.paused,
                "attempts": self.attempts, "duplicates_suppressed": self.duplicates_suppressed,
                "compensations": list(self.compensations), "children": list(self.children),
                "processed_command_ids": sorted(self.processed_command_ids), "max_steps": self.max_steps,
                "run_id": workflow.info().run_id, "run_started_at_step": self.run_started_at_step,
                "history_length": workflow.info().get_current_history_length()}

    @workflow.update
    def change_goal(self, goal: str, command_id: str) -> str:
        if command_id in self.processed_command_ids:
            return f"ignored: command {command_id} already applied"
        self.processed_command_ids.add(command_id)
        old, self.goal = self.goal, goal
        self.context_summary = f"{self.context_summary}\n[goal changed: {goal}]"
        return f"goal changed from {old!r} to {goal!r}"

    @change_goal.validator
    def _validate_change_goal(self, goal: str, command_id: str) -> None:
        # Rejected here = never in history. Accepted = a tracked write the client sees complete.
        if not goal.strip():
            raise ValueError("goal must not be empty")
        if len(goal) > 500:
            raise ValueError("goal longer than 500 characters")
        if not command_id:
            raise ValueError("command_id is required")

    # ------------------------------------------------------------------ effects
    async def _llm(self, step: Step) -> str:
        key = f"{workflow.info().workflow_id}:{self.step}:{_digest(step.prompt)}"
        return await workflow.execute_activity(
            call_llm, args=[step.prompt, key], task_queue=CPU_TOOLS,  # production: premium-models
            schedule_to_close_timeout=timedelta(minutes=2), start_to_close_timeout=timedelta(seconds=30),
            retry_policy=LLM_RETRY)

    async def _tool(self, step: Step) -> ToolResult:
        call = ToolCall(f"{workflow.info().workflow_id}:{self.step}:{_digest(step.tool, repr(sorted(step.args.items())))}",
                        step.tool, step.args)
        gpu = step.tool in GPU_TOOL_NAMES
        if gpu:
            # Module 7's saga in one line: what must be undone if this run is cancelled holding a GPU.
            self.compensations.append(f"release_gpu:{call.key}")
        result: ToolResult = await workflow.execute_activity(
            execute_tool, call, task_queue=LANE_FOR_TOOL[step.tool],
            schedule_to_start_timeout=GPU_SCHEDULE_TO_START if gpu else timedelta(minutes=2),
            start_to_close_timeout=TOOL_START_TO_CLOSE, heartbeat_timeout=TOOL_HEARTBEAT,
            retry_policy=TOOL_RETRY,
            priority=Priority(priority_key=PRIORITY_FOR_TIER[self.tier], fairness_key=self.owner or None,
                              fairness_weight=FAIRNESS_WEIGHT_FOR_TIER[self.tier]) if gpu else Priority())
        if gpu:
            self.compensations.remove(f"release_gpu:{call.key}")
        self.attempts += result.attempt - 1
        if result.duplicate:
            self.duplicates_suppressed += 1
        verdict: Verdict = await workflow.execute_activity(
            evaluate, result, task_queue=CPU_TOOLS, start_to_close_timeout=timedelta(seconds=30), retry_policy=TOOL_RETRY)
        self.context_summary = f"{result.output} ({verdict.note})"
        return result

    async def _research(self, step: Step) -> str:
        child_id = f"{workflow.info().workflow_id}/research/{self.step}"
        self.children.append(child_id)
        answer = await workflow.execute_child_workflow(
            ResearchAgent.run, step.question, id=child_id, task_queue=AGENT_WORKFLOWS,
            execution_timeout=timedelta(minutes=10))
        self.context_summary = f"{self.context_summary}\n[research: {answer}]"
        return answer

    async def _compensate(self) -> None:
        for name in reversed(self.compensations):
            await workflow.execute_activity(compensate, name, task_queue=CPU_TOOLS,
                                            start_to_close_timeout=timedelta(seconds=30), retry_policy=TOOL_RETRY)
        self.compensations.clear()

    async def _execute(self, step: Step) -> None:
        try:
            if step.kind == "llm":
                self.context_summary = await self._llm(step)
            elif step.kind == "tool":
                await self._tool(step)
            elif step.kind == "research":
                await self._research(step)
        except ActivityError as err:
            if isinstance(err.cause, TimeoutError) and err.cause.type == TimeoutType.SCHEDULE_TO_START:
                self._publish("stalled")
                raise ApplicationError(f"lane stalled: nobody took {step.tool} in time", type="LaneStalled",
                                       non_retryable=True) from err
            raise

    # ------------------------------------------------------------------ the loop
    @workflow.run
    async def run(self, goal: str, state: AgentState | None = None) -> dict:
        info = workflow.info()
        self.owner = info.typed_search_attributes.get(OWNER) or ""
        self.tier = workflow.memo_value("tier", "batch", type_hint=str)
        self.max_steps = workflow.memo_value("max_steps", DEFAULT_MAX_STEPS, type_hint=int)
        if state is None:
            self.goal = goal
        else:  # a continued run: the snapshot is the whole past
            self.step, self.goal, self.context_summary, self.attempts = state.step, state.goal, state.context_summary, state.attempts
            self.processed_command_ids = set(state.processed_command_ids)
        self.run_started_at_step = self.step
        self._publish("running")
        try:
            while True:
                await workflow.wait_condition(lambda: not self.paused or self.cancel_requested)
                if self.cancel_requested:
                    await self._compensate()
                    self._publish("cancelled")
                    return self._summary()
                step = self.plan()
                if step.kind == "finish":
                    self._publish("done")
                    return self._summary()
                if self._should_continue_as_new():   # more work to do, and this run has done its share
                    await workflow.wait_condition(workflow.all_handlers_finished)  # never drop an Update mid-flight
                    self._publish("continuing")
                    workflow.continue_as_new(args=[self.goal, self.snapshot()])
                await self._execute(step)
                self.step += 1
                self._publish("paused" if self.paused else "running")
        except BaseException as err:
            # Temporal cancellation (handle.cancel()) arrives in one of two shapes: asyncio.CancelledError if we were
            # waiting on a timer/condition, or an ActivityError/ChildWorkflowError whose cause is Temporal's
            # CancelledError if we were waiting on an Activity or child (it was told to cancel first).
            # `is_cancelled_exception` recognises both. Cleanup Activities still run here: the SDK cleared the task's
            # cancellation when it sent the cancel command, so the Workflow is not yet complete.
            if not is_cancelled_exception(err):
                raise
            await self._compensate()
            self._publish("cancelled")
            raise

    def _summary(self) -> dict:
        return {"status": self.status, "steps": self.step, "attempts": self.attempts,
                "duplicates_suppressed": self.duplicates_suppressed, "children_this_run": list(self.children),
                "context_tail": self.context_summary[-160:]}
