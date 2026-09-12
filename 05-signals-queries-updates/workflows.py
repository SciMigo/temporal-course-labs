"""Lab 5: AgentRun, stage 5 — the control surface.

Stage 4 gave AgentRun timeouts, retries and idempotency keys. Stage 5 lets a
client talk to it while it runs: Signals `pause`, `resume`, `cancel_run`,
`inject_context(text, command_id)`; the Query `status()`; the Update
`change_goal(goal, command_id)` with a validator. Handlers are Workflow code —
same determinism rules, and they interleave with the main loop.

The loop itself is unchanged in spirit: plan a step, call the model, fold the
answer into `context_summary`, wait a durable timer, repeat. The one new line
that matters is the `wait_condition` at the top of the loop: that is where a
paused agent waits, and while it waits no Worker needs to exist.

Exercise 5.5 adds the interleaving case: `enrich_context` is an Update whose
handler awaits an Activity, so it runs concurrently with the main loop and with
other handlers; `request_continue_as_new` makes the loop checkpoint into a fresh
run (module 6 does this properly) so you can see what happens to a handler that
is still awaiting when the run ends.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import sys
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE  # noqa: E402,F401  (re-exported for worker.py / starter.py)

MAX_STEPS = 20      # the loop finishes on its own after this many steps (~45 s)
STEP_SECONDS = 2    # durable timer between steps — long enough to send messages by hand
NOTE_SECONDS = 6    # how long `expand_note` takes: long enough to continue-as-new underneath it (5.5)


@dataclass
class AgentState:
    """The snapshot continue-as-new carries into the next run (5.5). Module 6 makes this the
    real thing; here it exists so the new run keeps its step, its notes and its dedupe set."""
    step: int
    context_summary: str = ""
    paused: bool = False
    processed_command_ids: list[str] = field(default_factory=list)


@dataclass
class Step:
    kind: str          # "llm" | "finish"
    prompt: str = ""


@activity.defn
async def call_llm(prompt: str, key: str) -> str:
    """The model call as an Activity (stage 1 fakes the model; stage 4 put a
    cache behind `key`). Kept trivial so the lab is about the control surface."""
    activity.logger.info("call_llm key=%s", key)
    return f"[model answer to: {prompt[:48]}]"


@activity.defn
async def expand_note(text: str) -> str:
    """A slow Activity an Update handler awaits (5.5): the model expands an operator's note."""
    activity.logger.info("expand_note %r", text)
    await asyncio.sleep(NOTE_SECONDS)
    return f"{text} (expanded)"


def fold(summary: str, answer: str) -> str:
    """Workflow code: pure string work. Keep every operator note, replace the
    model's last answer with the new one. (A real agent would summarize.)"""
    notes = [line for line in summary.splitlines() if line.startswith("Operator:")]
    return "\n".join(notes + [answer])


@workflow.defn
class AgentRun:
    def __init__(self) -> None:
        self.step = 0
        self.status = "running"          # running | done | cancelled
        self.goal = ""
        self.context_summary = ""
        self.attempts = 0
        self.paused = False
        self.processed_command_ids: set[str] = set()   # Workflow state: rebuilt by replay
        self.lock = asyncio.Lock()                     # 5.5: one mutating handler at a time
        self.continue_requested = False                # 5.5: set by a Signal; acted on by the loop
        self.drain_before_continue = True              # 5.5: the fix; the lab turns it off first

    def snapshot(self) -> AgentState:
        return AgentState(
            step=self.step, context_summary=self.context_summary, paused=self.paused,
            processed_command_ids=sorted(self.processed_command_ids),
        )

    def plan(self) -> Step:
        """Workflow code: a pure function of state. It never calls a model."""
        if self.step >= MAX_STEPS:
            return Step("finish")
        return Step("llm", prompt=f"Step {self.step} toward: {self.goal}")

    @workflow.run
    async def run(self, goal: str, state: AgentState | None = None) -> str:
        self.goal = goal
        if state is not None:                           # 5.5: the run after a continue-as-new
            self.step = state.step
            self.context_summary = state.context_summary
            self.paused = state.paused
            self.processed_command_ids = set(state.processed_command_ids)
        while self.status == "running":
            # pause: the main loop blocks here. Nothing polls; no Worker is required.
            await workflow.wait_condition(
                lambda: not self.paused or self.status != "running" or self.continue_requested
            )
            if self.continue_requested:                 # 5.5: checkpoint into a fresh run
                if self.drain_before_continue:
                    # The fix: a handler still awaiting an Activity finishes before the run ends.
                    await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(args=[self.goal, self.snapshot()])   # nothing after this runs
            if self.status != "running":
                break                                   # cancel_run: exit cleanly (true Cancellation is lab 7)
            step = self.plan()
            if step.kind == "finish":
                self.status = "done"
                break
            key = f"{workflow.info().workflow_id}:{self.step}:{hashlib.sha256(step.prompt.encode()).hexdigest()[:12]}"
            answer = await workflow.execute_activity(
                call_llm, args=[step.prompt, key], start_to_close_timeout=timedelta(seconds=10)
            )
            self.context_summary = fold(self.context_summary, answer)
            self.step += 1
            await asyncio.sleep(STEP_SECONDS)           # TimerStarted / TimerFired
        # Drain: an `async` handler still awaiting something must not be cut off by our return.
        await workflow.wait_condition(workflow.all_handlers_finished)
        return self.context_summary

    # ---- Signals: asynchronous writes, recorded as WorkflowExecutionSignaled ----

    @workflow.signal
    def pause(self) -> None:
        self.paused = True

    @workflow.signal
    def resume(self) -> None:
        self.paused = False

    @workflow.signal
    def cancel_run(self) -> None:
        self.status = "cancelled"                       # the loop exits at its next check

    @workflow.signal
    def request_continue_as_new(self, drain: bool = True) -> None:
        """5.5: ask the main loop to continue-as-new. Never call continue_as_new from a handler;
        the loop does it. `drain=False` reproduces the bug (the run ends under a live handler)."""
        self.drain_before_continue = drain
        self.continue_requested = True

    @workflow.signal
    def inject_context(self, text: str, command_id: str) -> None:
        if command_id in self.processed_command_ids:    # the client retried after a broken connection
            return                                      # the first delivery already did the work
        self.processed_command_ids.add(command_id)
        self.context_summary += f"\nOperator: {text}"

    # ---- Query: a synchronous read; answered by a live Worker; never in history ----

    @workflow.query
    def status(self) -> dict:
        return {
            "step": self.step,
            "status": self.status,
            "paused": self.paused,
            "goal": self.goal,
            "notes": [l for l in self.context_summary.splitlines() if l.startswith("Operator:")],
            "processed_command_ids": sorted(self.processed_command_ids),
        }

    # ---- Update: a synchronous, tracked write; validated first ----

    @workflow.update
    async def change_goal(self, goal: str, command_id: str) -> str:
        if command_id in self.processed_command_ids:    # a retried Update: same answer, no second write
            return self.goal
        self.processed_command_ids.add(command_id)
        self.goal = goal
        self.context_summary += f"\nOperator: goal changed to: {goal}"
        return self.goal

    @change_goal.validator
    def _check_goal(self, goal: str, command_id: str) -> None:
        if not goal.strip():
            raise ValueError("goal must not be empty")  # rejected: never enters history

    # ---- Update whose handler awaits an Activity (5.5): it interleaves with everything else ----

    @workflow.update
    async def enrich_context(self, text: str, command_id: str) -> str:
        async with self.lock:                           # a second enrich_context waits here
            if command_id in self.processed_command_ids:
                return "ignored"                        # a retry after the first one finished
            expanded = await workflow.execute_activity(  # the handler yields: the loop, a pause, a
                expand_note, text, start_to_close_timeout=timedelta(seconds=30)   # continue-as-new can all happen here
            )
            self.processed_command_ids.add(command_id)  # after the await, under the lock: a retry
            self.context_summary += f"\nOperator: {expanded}"   # that arrived meanwhile is deduped
            return expanded

    @enrich_context.validator
    def _check_note(self, text: str, command_id: str) -> None:
        if not text.strip():
            raise ValueError("note must not be empty")
