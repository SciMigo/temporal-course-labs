"""Lab 2: AgentRun, stage 2 — the same program as lab 1, read through its Event History.

The code is lab 1's; the two additions are instruments, not behaviour:
  * `CALL_LLM_INVOCATIONS` counts how often the Activity function really ran in *this* process,
    so you can see that a replaying Worker never calls it.
  * `workflow.logger` lines before every await. Log lines are suppressed while the SDK is
    replaying and printed only once the code is "live" — so the first line a fresh Worker prints
    names the first Command it actually issued.
`max_steps` exists for the counting exercise (2.1, last part): every step costs one Activity
(three Events) plus the Workflow Task that delivers its result (three more).
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow


# Lives in the Worker process, outside the Workflow sandbox: a plain global.
CALL_LLM_INVOCATIONS = 0


@dataclass
class Step:
    kind: str          # "llm" | "finish"
    prompt: str = ""


@activity.defn
async def call_llm(prompt: str, key: str) -> str:
    """The model call as an Activity. Stage 2 still fakes the model; what matters here is that
    the function body runs once per attempt and never during replay."""
    global CALL_LLM_INVOCATIONS
    CALL_LLM_INVOCATIONS += 1
    activity.logger.info(
        "call_llm key=%s  (invocation #%d in pid %d)", key, CALL_LLM_INVOCATIONS, os.getpid()
    )
    return f"[model answer to: {prompt[:40]}]"


@workflow.defn
class AgentRun:
    def __init__(self) -> None:
        self.step = 0
        self.status = "running"
        self.goal = ""
        self.context_summary = ""

    def plan(self, max_steps: int) -> Step:
        """Workflow code: a pure function of state. It never calls a model."""
        if self.step < max_steps:
            return Step("llm", prompt=f"Step {self.step} of a plan for: {self.goal}")
        return Step("finish")

    @workflow.run
    async def run(self, goal: str, sleep_seconds: int = 20, max_steps: int = 1) -> str:
        self.goal = goal
        while True:
            step = self.plan(max_steps)
            if step.kind == "finish":
                self.status = "done"
                workflow.logger.info("live: returning -> Command CompleteWorkflowExecution")
                return self.context_summary
            key = f"{workflow.info().workflow_id}:{self.step}:{hashlib.sha256(step.prompt.encode()).hexdigest()[:12]}"
            workflow.logger.info("live: step %d -> Command ScheduleActivityTask(call_llm)", self.step)
            answer = await workflow.execute_activity(
                call_llm, args=[step.prompt, key], start_to_close_timeout=timedelta(seconds=10)
            )
            self.context_summary = answer
            self.step += 1
            workflow.logger.info("live: step %d -> Command StartTimer(%ds)", self.step, sleep_seconds)
            await asyncio.sleep(sleep_seconds)
