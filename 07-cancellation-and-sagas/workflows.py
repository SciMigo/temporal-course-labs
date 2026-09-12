"""Lab 7: AgentRun, stage 7 — the GPU saga, and cancellation that actually stops things.

Two additions. (1) `execute_tool`'s GPU lane is four Activities orchestrated by
Workflow code — `reserve_gpu -> allocate_sandbox -> start_model -> register_endpoint` —
and after each success AgentRun appends the matching undo to `self.compensations`.
That list is Workflow state: when a later step fails, the undos run in reverse, and
a Worker that dies halfway through the rollback is replaced by one that replays the
list and finishes it. (2) `handle.cancel()` is Temporal's cancellation, delivered
once as `asyncio.CancelledError` at the Workflow's next await. The rollback runs in
`finally`, after that delivery, with no shield. What a cancel does to the Activity
that was in flight depends on two things: whether the Workflow propagates the cancellation to it at
all (7.3's shield does not), and, when it does, whether the Activity heartbeats — the request rides
back on a heartbeat response.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import sys
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import TASK_QUEUE  # noqa: E402,F401

SPEED = 1.0                                   # scale for every fake Activity's sleep (tests shrink it)
STEP_TIMEOUT = timedelta(seconds=10)          # forward steps and undos; also how long a dead Worker's attempt stays "running" (7.2)
FORWARD_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3)
UNDO_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=10)
UNDO_SECONDS = 3                              # each undo takes this long: the window to kill the Worker in 7.2
TOOL_CHUNKS = 20                              # execute_tool works in 1-second chunks
HEARTBEAT_TIMEOUT = timedelta(seconds=3)      # heartbeats are throttled to 0.8x this, so a cancel lands within ~2.4 s


@dataclass
class ToolCall:
    key: str
    tool: str                                 # "deploy_model" (GPU lane) | "crunch" (CPU lane)
    args: dict = field(default_factory=dict)


@dataclass
class ToolResult:
    key: str
    output: str


@dataclass
class ToolOptions:
    """Lab knobs for 7.3, passed to run() so the Workflow stays deterministic."""
    heartbeat: bool = False                   # execute_tool calls activity.heartbeat() each chunk
    shield: bool = False                      # AgentRun wraps the in-flight execute_tool in asyncio.shield
    chunks: int = TOOL_CHUNKS


@dataclass
class Step:
    kind: str                                 # "llm" | "tool" | "finish"
    prompt: str = ""
    tool: str = ""


async def _work(seconds: float) -> None:
    await asyncio.sleep(seconds * SPEED)


@activity.defn
async def call_llm(prompt: str, key: str) -> str:
    return f"[model answer to: {prompt[:48]}]"


# ---- the GPU lane, forward: each step returns the identifier its undo will need ----

@activity.defn
async def reserve_gpu(key: str) -> str:
    await _work(1)
    gpu = f"gpu-for-{key.split(':')[0]}"          # the reservation id; release_gpu is keyed on it
    activity.logger.info("reserve_gpu -> %s", gpu)
    return gpu


@activity.defn
async def allocate_sandbox(gpu: str) -> str:
    await _work(1)
    box = f"sandbox-on-{gpu}"
    activity.logger.info("allocate_sandbox -> %s", box)
    return box


@activity.defn
async def start_model(box: str) -> str:
    await _work(1)
    model = f"model-in-{box}"
    activity.logger.info("start_model -> %s", model)
    return model


@activity.defn
async def register_endpoint(model: str) -> str:
    await _work(0.2)
    activity.logger.info("register_endpoint attempt %d for %s: rejected", activity.info().attempt, model)
    raise ApplicationError(f"endpoint registry rejected the name for {model}")   # retryable; the policy ends it


# ---- the GPU lane, reverse: each undo may run more than once ----
# The stable resource id is what *lets* the external system make the undo idempotent; it does not
# make it idempotent. Releasing gpu-123 twice is safe only if the allocator treats the second
# release as a no-op rather than an error — or, worse, as releasing a gpu-123 that has since been
# handed to someone else. These fake Activities have no external state to get wrong.

async def _undo(what: str, resource: str) -> None:
    activity.logger.info("%s %s: attempt %d, starting", what, resource, activity.info().attempt)
    await _work(UNDO_SECONDS)
    activity.logger.info("%s %s: done", what, resource)


@activity.defn
async def unregister_endpoint(endpoint: str) -> None:
    await _undo("unregister_endpoint", endpoint)


@activity.defn
async def stop_model(model: str) -> None:
    await _undo("stop_model", model)


@activity.defn
async def release_sandbox(box: str) -> None:
    await _undo("release_sandbox", box)


@activity.defn
async def release_gpu(gpu: str) -> None:
    await _undo("release_gpu", gpu)


# ---- the CPU lane: one long tool ----

FINISHED_TOOLS: list[str] = []                # in-process marker for the tests: which tool calls ran to the end


@activity.defn
async def execute_tool(call: ToolCall) -> ToolResult:
    """A long tool in 1-second chunks. With `heartbeat` off it never talks to the Service while it
    runs, so nothing can tell it the run was cancelled. With it on, the cancellation rides back on a
    heartbeat response and the SDK raises CancelledError at the next await."""
    chunks = int(call.args.get("chunks", TOOL_CHUNKS))
    heartbeat = bool(call.args.get("heartbeat", False))
    i = 0
    try:
        for i in range(1, chunks + 1):
            if heartbeat:
                activity.heartbeat(i)             # progress out, cancellation (if any) back
            await _work(1)
            activity.logger.info("execute_tool %s: chunk %d/%d", call.tool, i, chunks)
    except asyncio.CancelledError:
        activity.logger.info("execute_tool %s: cancelled at chunk %d/%d", call.tool, i, chunks)
        raise                                     # re-raise, or the Activity does not appear cancelled
    FINISHED_TOOLS.append(call.key)
    activity.logger.info("execute_tool %s: finished all %d chunks", call.tool, chunks)
    return ToolResult(key=call.key, output=f"{call.tool}: {chunks} chunks done")


@workflow.defn
class AgentRun:
    def __init__(self) -> None:
        self.step = 0
        self.status = "running"                    # running | done | failed | cancelled
        self.goal = ""
        self.context_summary = ""
        self.attempts = 0
        self.compensations: list = []              # [(undo_activity, arg)] — Workflow state, so replay rebuilds it
        self.options = ToolOptions()
        self.in_flight = ""
        self.rolled_back: list[str] = []

    def plan(self) -> Step:
        if self.step == 0:
            return Step("llm", prompt=f"Plan one tool call for: {self.goal}")
        if self.step == 1:
            return Step("tool", tool="deploy_model" if self.goal.startswith("deploy") else "crunch")
        return Step("finish")

    @workflow.run
    async def run(self, goal: str, options: ToolOptions | None = None) -> str:
        self.goal = goal
        self.options = options or ToolOptions()
        try:
            while self.status == "running":
                step = self.plan()
                if step.kind == "finish":
                    self.status = "done"
                    break
                if step.kind == "llm":
                    key = f"{workflow.info().workflow_id}:{self.step}:{hashlib.sha256(step.prompt.encode()).hexdigest()[:12]}"
                    self.context_summary = await workflow.execute_activity(
                        call_llm, args=[step.prompt, key], start_to_close_timeout=timedelta(seconds=10)
                    )
                else:
                    call = ToolCall(key=f"{workflow.info().workflow_id}:{self.step}:{step.tool}", tool=step.tool,
                                    args={"heartbeat": self.options.heartbeat, "chunks": self.options.chunks})
                    result = await (self.gpu_lane(call) if step.tool == "deploy_model" else self.cpu_lane(call))
                    self.context_summary += f"\n{result.output}"
                self.step += 1
        except asyncio.CancelledError:
            self.status = "cancelled"              # Temporal's cancel, delivered once at the await above
            raise                                  # ... after `finally` has run the rollback
        except ActivityError as e:
            if isinstance(e.cause, CancelledError):
                self.status = "cancelled"          # the same cancel, but it landed while an Activity was in
            else:                                  # flight: it arrives wrapped as that Activity's cancellation
                self.status = "failed"             # a step failed for good (its retries are spent)
            raise
        finally:
            if self.status in ("cancelled", "failed"):
                await self.rollback()              # runs normally: the one cancellation was already delivered
        return self.context_summary

    async def gpu_lane(self, call: ToolCall) -> ToolResult:
        """Four steps, each recorded; the undo list grows with every success."""
        self.compensations = []
        gpu = await workflow.execute_activity(reserve_gpu, call.key, start_to_close_timeout=STEP_TIMEOUT)
        self.compensations.append((release_gpu, gpu))
        box = await workflow.execute_activity(allocate_sandbox, gpu, start_to_close_timeout=STEP_TIMEOUT)
        self.compensations.append((release_sandbox, box))
        model = await workflow.execute_activity(start_model, box, start_to_close_timeout=STEP_TIMEOUT)
        self.compensations.append((stop_model, model))
        endpoint = await workflow.execute_activity(                       # fails after FORWARD_RETRY is spent
            register_endpoint, model, start_to_close_timeout=STEP_TIMEOUT, retry_policy=FORWARD_RETRY
        )
        self.compensations.append((unregister_endpoint, endpoint))
        return ToolResult(key=call.key, output=f"deployed at {endpoint}")

    async def cpu_lane(self, call: ToolCall) -> ToolResult:
        """A sandbox, then one long tool inside it. The sandbox is the resource a cancel must give back."""
        self.compensations = []
        box = await workflow.execute_activity(allocate_sandbox, call.key, start_to_close_timeout=STEP_TIMEOUT)
        self.compensations.append((release_sandbox, box))
        self.in_flight = call.tool
        tool = workflow.start_activity(              # a handle: the Activity is scheduled now, awaited below
            execute_tool, call,
            start_to_close_timeout=timedelta(seconds=self.options.chunks + 30),
            heartbeat_timeout=HEARTBEAT_TIMEOUT if self.options.heartbeat else None,
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        if self.options.shield:
            try:
                result = await asyncio.shield(tool)  # the cancel reaches this await, not the Activity
            except asyncio.CancelledError:
                result = await tool                  # let the in-flight work finish, then close as cancelled
                self.context_summary += f"\n{result.output}"
                raise
        else:
            result = await tool                      # cancelling this await cancels the Activity too
        self.in_flight = ""
        return result

    async def rollback(self) -> None:
        """The saga's reverse path. Each undo is an Activity with its own retry policy."""
        for undo, arg in reversed(self.compensations):
            await workflow.execute_activity(undo, arg, start_to_close_timeout=STEP_TIMEOUT, retry_policy=UNDO_RETRY)
            self.rolled_back.append(undo.__name__)
        self.compensations = []

    @workflow.query
    def status(self) -> dict:
        return {
            "step": self.step,
            "status": self.status,
            "in_flight": self.in_flight,
            "compensations": [undo.__name__ for undo, _ in self.compensations],
            "rolled_back": self.rolled_back,
        }
