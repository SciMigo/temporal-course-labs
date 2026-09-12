"""Lab 6: AgentRun, stage 6 — continue-as-new and the ResearchAgent child.

Two additions to stage 5. (1) The loop checks, at the top of every step, whether it
should checkpoint into a fresh run: `workflow.info().is_continue_as_new_suggested()`
is the rule; `STEPS_PER_RUN` is a lab knob so the transition happens in a 20-step
run instead of a 20,000-step one. The checkpoint is `AgentState`, passed as the
next run's `run()` arguments. (2) `plan()` can now return a `research` step, which
AgentRun delegates to `ResearchAgent` — a Child Workflow with its own history,
its own timers and its own Worker.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import sys
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError, ChildWorkflowError
from temporalio.workflow import ParentClosePolicy

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE  # noqa: E402

# The child runs on its own queue only so that "the child's Worker" is a separate process you
# can kill in 6.2 (`python worker.py --role research`). By default a child inherits its parent's
# Task Queue; splitting queues on purpose is module 10.
RESEARCH_TASK_QUEUE = f"{TASK_QUEUE}-research"

MAX_STEPS = 20          # the loop finishes on its own after this many steps
STEPS_PER_RUN = 4       # lab knob: continue-as-new after this many steps in one run -> 5 runs
STEP_SECONDS = 0.5      # durable timer between steps
RESEARCH_STEP = 6       # plan() delegates to ResearchAgent at this step (run 2 of 5)
SOURCES = 3             # ResearchAgent reads this many sources ...
SOURCE_SECONDS = 3      # ... with a durable timer after each: ~10 s to kill its Worker


@dataclass
class AgentState:
    """The checkpoint: everything the next run needs to decide. A summary, never the transcript."""
    step: int
    goal: str
    context_summary: str = ""
    attempts: int = 0
    processed_command_ids: list[str] = field(default_factory=list)   # module 5's dedupe set rides along
    research_n: int = 0


@dataclass
class Step:
    kind: str          # "llm" | "research" | "finish"
    prompt: str = ""
    task: str = ""


@dataclass
class Report:
    task: str
    summary: str
    sources: int


@activity.defn
async def call_llm(prompt: str, key: str) -> str:
    activity.logger.info("call_llm key=%s", key)
    return f"[model answer to: {prompt[:48]}]"


@activity.defn
async def fetch_source(task: str, n: int) -> str:
    activity.logger.info("fetch_source %s #%d", task, n)
    return f"source {n}: notes on {task}"


@workflow.defn
class ResearchAgent:
    """A delegated unit with a lifecycle: several steps, timers, a Query of its own, and its own
    history in the UI. As one Activity this would be one giant timeout and all-or-nothing retry."""

    def __init__(self) -> None:
        self.notes: list[str] = []

    @workflow.run
    async def run(self, task: str) -> Report:
        if "unreliable" in task:                      # 6.2, second half: a child that fails
            raise ApplicationError("source registry rejected the task", non_retryable=True)
        for n in range(1, SOURCES + 1):
            note = await workflow.execute_activity(
                fetch_source, args=[task, n], start_to_close_timeout=timedelta(seconds=10)
            )
            self.notes.append(note)
            await asyncio.sleep(SOURCE_SECONDS)       # a durable timer per source: kill this Worker here
        return Report(task=task, summary=f"{len(self.notes)} sources on {task}", sources=len(self.notes))

    @workflow.query
    def progress(self) -> int:
        return len(self.notes)


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
        self.pending_context = ""       # accepted by a handler, not yet folded in: part of the snapshot
        self.research_n = 0
        self.steps_this_run = 0         # not in AgentState on purpose: it is about this run

    def restore(self, state: AgentState) -> None:
        self.step = state.step
        self.goal = state.goal
        self.context_summary = state.context_summary
        self.attempts = state.attempts
        self.processed_command_ids = set(state.processed_command_ids)
        self.research_n = state.research_n

    def snapshot(self) -> AgentState:
        return AgentState(
            step=self.step,
            goal=self.goal,
            context_summary=self.context_summary + self.pending_context,   # accepted, unprocessed: carry it
            attempts=self.attempts,
            processed_command_ids=sorted(self.processed_command_ids),
            research_n=self.research_n,
        )

    def plan(self) -> Step:
        """Workflow code: a pure function of state."""
        if self.step >= MAX_STEPS:
            return Step("finish")
        if self.step == RESEARCH_STEP:
            return Step("research", task=f"sources for: {self.goal}")
        return Step("llm", prompt=f"Step {self.step} toward: {self.goal}")

    @workflow.run
    async def run(self, goal: str, state: AgentState | None = None) -> str:
        self.goal = goal
        if state:
            self.restore(state)                        # a new run picks up where the old one left off
        while self.status == "running":
            if self.step < MAX_STEPS and (
                workflow.info().is_continue_as_new_suggested() or self.steps_this_run >= STEPS_PER_RUN
            ):
                await workflow.wait_condition(workflow.all_handlers_finished)   # drain Signals/Updates first
                workflow.continue_as_new(args=[self.goal, self.snapshot()])      # nothing after this line runs
            await workflow.wait_condition(lambda: not self.paused or self.status != "running")
            if self.status != "running":
                break
            step = self.plan()
            if step.kind == "finish":
                self.status = "done"
                break
            if step.kind == "research":
                self.research_n += 1
                try:
                    report = await workflow.execute_child_workflow(
                        ResearchAgent.run, step.task,
                        id=f"{workflow.info().workflow_id}/research/{self.research_n}",
                        task_queue=RESEARCH_TASK_QUEUE,
                        parent_close_policy=ParentClosePolicy.TERMINATE,
                    )
                    self.context_summary += f"\n{report.summary}"
                except ChildWorkflowError as e:        # the child failed: decide, as with an ActivityError
                    self.attempts += 1
                    self.context_summary += f"\nresearch {self.research_n} failed: {e.cause}"
            else:
                key = f"{workflow.info().workflow_id}:{self.step}:{hashlib.sha256(step.prompt.encode()).hexdigest()[:12]}"
                answer = await workflow.execute_activity(
                    call_llm, args=[step.prompt, key], start_to_close_timeout=timedelta(seconds=10)
                )
                self.context_summary = answer if not self.context_summary else f"{self.context_summary}\n{answer}"
            if self.pending_context:                   # fold operator notes at a step boundary
                self.context_summary += self.pending_context
                self.pending_context = ""
            self.step += 1
            self.steps_this_run += 1
            await asyncio.sleep(STEP_SECONDS)
        await workflow.wait_condition(workflow.all_handlers_finished)
        return self.context_summary

    @workflow.signal
    def pause(self) -> None:
        self.paused = True

    @workflow.signal
    def resume(self) -> None:
        self.paused = False

    @workflow.signal
    def cancel_run(self) -> None:
        self.status = "cancelled"

    @workflow.signal
    def inject_context(self, text: str, command_id: str) -> None:
        if command_id in self.processed_command_ids:
            return
        self.processed_command_ids.add(command_id)
        self.pending_context += f"\nOperator: {text}"

    @workflow.query
    def status(self) -> dict:
        return {
            "run_id": workflow.info().run_id,          # changes at every continue-as-new; the handle does not
            "step": self.step,
            "steps_this_run": self.steps_this_run,
            "status": self.status,
            "paused": self.paused,
            "goal": self.goal,
            "research_n": self.research_n,
            "attempts": self.attempts,
            "processed_command_ids": sorted(self.processed_command_ids),
        }
