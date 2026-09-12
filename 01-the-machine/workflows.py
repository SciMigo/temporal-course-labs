"""Lab 1: AgentRun, stage 1 — plan one step, call the model once, sleep, finish.

This is the program the whole course grows. Nothing here is Temporal-magic: an
Activity is a function with a recorded outcome; a Workflow is code whose
decisions Temporal persists so a fresh Worker can reconstruct them.
"""
from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow

import os
TASK_QUEUE = os.environ.get("TASK_QUEUE", "agent-runs")


@dataclass
class Step:
    kind: str          # "llm" | "finish"
    prompt: str = ""


@activity.defn
async def call_llm(prompt: str, key: str) -> str:
    """The model call as an Activity. `key` is the idempotency key a retry
    would reuse (module 4 puts a cache behind it). Stage 1 fakes the model."""
    activity.logger.info("call_llm key=%s", key)
    return f"[model answer to: {prompt[:40]}]"


@workflow.defn
class AgentRun:
    def __init__(self) -> None:
        self.step = 0
        self.status = "running"
        self.goal = ""
        self.context_summary = ""

    def plan(self) -> Step:
        """Workflow code: a pure function of state. It never calls a model."""
        if self.step == 0:
            return Step("llm", prompt=f"Make a one-line plan for: {self.goal}")
        return Step("finish")

    @workflow.run
    async def run(self, goal: str, sleep_seconds: int = 30) -> str:
        self.goal = goal
        while True:
            step = self.plan()
            if step.kind == "finish":
                self.status = "done"
                return self.context_summary
            key = f"{workflow.info().workflow_id}:{self.step}:{hashlib.sha256(step.prompt.encode()).hexdigest()[:12]}"
            answer = await workflow.execute_activity(
                call_llm, args=[step.prompt, key], start_to_close_timeout=timedelta(seconds=10)
            )
            self.context_summary = answer
            self.step += 1
            # A durable timer: recorded as TimerStarted; fires as TimerFired; no process waits.
            await asyncio.sleep(sleep_seconds)
