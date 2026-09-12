"""Lab 3: AgentRun, stage 3 — break replay three ways, read the error, then keep the rule.

The program is lab 2's, plus the two things a real agent is tempted to do in Workflow code:
consult the clock in `plan()` (a time budget) and consult the model in `plan()` (the next step).
`LAB03_BREAK` selects which *version of the code* a Worker runs — think "which build is
deployed", which is why it is an environment variable and not a Workflow argument: the fix
is a redeploy, and the same history must replay under the fixed code.

  LAB03_BREAK=          what the Worker runs                                  what you get
  none (default)        workflow.random(), workflow.now(), model via call_llm  clean replays
  random-naive          random.random() in run()                              sandbox: RestrictedWorkflowAccessError
  random                random.Random().random() in run()  (unseeded)         NondeterminismError, on some replays
  clock-naive           datetime.now() in plan()                              sandbox
  clock                 datetime.now() under sandbox_unrestricted() in plan()  NondeterminismError, on every replay after the budget
  network-naive         urllib.request.urlopen() in plan()                    sandbox
  network               the same under sandbox_unrestricted()                 NondeterminismError, and the replay paid for a model call

The model is `model_server.py` (3.3): it answers "next: continue" once, then "next: finish".
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import random
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from temporalio import activity, workflow

TASK_QUEUE = os.environ.get("TASK_QUEUE", "agent-runs")
BREAK = os.environ.get("LAB03_BREAK", "none")
# Set in 3.3 only. Empty => call_llm fakes the model (one step, then finish).
MODEL_URL = os.environ.get("LAB03_MODEL_URL", "")
DEFAULT_MODEL_URL = "http://127.0.0.1:8765"


def ask_model(prompt: str) -> str:
    """One HTTP GET to the model server. An effect: the answer depends on how often it was asked."""
    url = f"{MODEL_URL or DEFAULT_MODEL_URL}/decide?prompt={urllib.parse.quote(prompt)}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return resp.read().decode("utf-8").strip()


@dataclass
class Step:
    kind: str          # "llm" | "finish"
    prompt: str = ""


@activity.defn
async def call_llm(prompt: str, key: str) -> str:
    """The model call as an Activity. Its result is recorded in ActivityTaskCompleted, so on
    replay the Workflow gets the same answer without the model being asked again."""
    activity.logger.info("call_llm key=%s", key)
    if MODEL_URL:
        return ask_model(prompt)
    return f"[stub model answer to: {prompt[:40]}] next: finish"


@workflow.defn
class AgentRun:
    def __init__(self) -> None:
        self.step = 0
        self.status = "running"
        self.goal = ""
        self.context_summary = ""
        self.started_at: datetime | None = None
        self.budget_seconds = 0
        self.max_steps = 0

    def plan(self) -> Step:
        """Workflow code: a pure function of state. It never calls a model — except in the
        versions below that are here to be broken."""
        # --- the time budget: has the run been going for too long? -------------------------
        if BREAK == "clock-naive":
            now = datetime.now(timezone.utc)                      # sandbox stops this
        elif BREAK == "clock":
            with workflow.unsafe.sandbox_unrestricted():
                now = datetime.now(timezone.utc)                  # wall clock: different on replay
        else:
            now = workflow.now()                                  # history time: same on replay
        if now - self.started_at > timedelta(seconds=self.budget_seconds):
            return Step("finish")
        if self.step >= self.max_steps:
            return Step("finish")
        # --- the next action: ask the model, or decide from the recorded answer? -----------
        prompt = f"Step {self.step} for: {self.goal}. Context: {self.context_summary[-60:]}"
        if BREAK == "network-naive":
            decision = ask_model(prompt)                          # sandbox stops this
            return Step("llm", prompt) if "continue" in decision else Step("finish")
        if BREAK == "network":
            with workflow.unsafe.sandbox_unrestricted():
                decision = ask_model(prompt)                      # a different answer on replay
            return Step("llm", prompt) if "continue" in decision else Step("finish")
        if "next: finish" in self.context_summary:                # the recorded answer decides
            return Step("finish")
        return Step("llm", prompt)

    @workflow.run
    async def run(self, goal: str, sleep_seconds: int = 15, budget_seconds: int = 5, max_steps: int = 5) -> str:
        self.goal = goal
        self.budget_seconds = budget_seconds
        self.max_steps = max_steps
        self.started_at = workflow.now()
        while True:
            step = self.plan()
            workflow.logger.info("live: step %d plan -> %s", self.step, step.kind)
            if step.kind == "finish":
                self.status = "done"
                return self.context_summary
            # Spread load: sometimes wait a second before calling the model.
            if BREAK == "random-naive":
                jitter = random.random() < 0.5                    # sandbox stops this
            elif BREAK == "random":
                jitter = random.Random().random() < 0.5           # unseeded: a fresh coin on every replay
            else:
                jitter = workflow.random().random() < 0.5         # seeded from history: the same coin
            if jitter:
                await asyncio.sleep(1)
            key = f"{workflow.info().workflow_id}:{self.step}:{hashlib.sha256(step.prompt.encode()).hexdigest()[:12]}"
            self.context_summary = await workflow.execute_activity(
                call_llm, args=[step.prompt, key], start_to_close_timeout=timedelta(seconds=10)
            )
            self.step += 1
            await asyncio.sleep(sleep_seconds)
