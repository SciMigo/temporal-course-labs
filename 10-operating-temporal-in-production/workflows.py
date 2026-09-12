"""Lab 10: AgentRun, stage 10 — the same program, split across compute lanes.

Nothing about the agent loop changes. What changes is that every Activity call names the lane it
needs (`task_queue=`), carries a Schedule-to-Start timeout so an empty lane becomes visible, and
the run publishes `AgentStatus` / `CurrentStep` Search Attributes so an operator can find it among
thousands without opening a history.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import Priority, RetryPolicy, SearchAttributeKey
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError, TimeoutType

from lanes import CPU_TOOLS, GPU_TOOL_NAMES, GPU_TOOLS, LANE_FOR_TOOL

# ---------------------------------------------------------------- Search Attributes (registered
# once per Namespace by setup_search_attributes.py; the Workflow only writes them)
AGENT_STATUS = SearchAttributeKey.for_keyword("AgentStatus")
CURRENT_STEP = SearchAttributeKey.for_int("CurrentStep")
OWNER = SearchAttributeKey.for_keyword("Owner")

# Lab knobs. The course values are in the comments; the lab shrinks them so you can watch.
GPU_SCHEDULE_TO_START = timedelta(seconds=int(os.environ.get("GPU_SCHEDULE_TO_START_SECONDS", "60")))  # course: 5 min
TOOL_SECONDS = float(os.environ.get("TOOL_SECONDS", "3"))  # how long the fake tool "computes"

# Priority on the shared GPU lane: 1 is served first, 5 last, default 3 (upstream, as of 2026-09).
PRIORITY_FOR_TIER = {"enterprise": 1, "team": 3, "batch": 5}
FAIRNESS_WEIGHT_FOR_TIER = {"enterprise": 5.0, "team": 3.0, "batch": 1.0}


@dataclass
class Step:
    kind: str  # "llm" | "tool" | "finish"
    prompt: str = ""
    tool: str = ""
    args: dict = field(default_factory=dict)


@dataclass
class ToolCall:
    key: str
    tool: str
    args: dict


@dataclass
class ToolResult:
    key: str
    tool: str
    output: str
    attempt: int
    task_queue: str  # which lane answered — the Workflow never chose a host, only a lane
    worker_pid: int


@dataclass
class Verdict:
    ok: bool
    note: str


# ---------------------------------------------------------------- Activities
@activity.defn
async def call_llm(prompt: str, key: str) -> str:
    activity.logger.info("call_llm key=%s on %s", key, activity.info().task_queue)
    return f"[model answer to: {prompt[:40]}]"


@activity.defn
async def execute_tool(call: ToolCall) -> ToolResult:
    """One registration for both lanes; the Worker that polls `gpu-tools` must be the one with a GPU.
    The fake heartbeats every 0.5 s so a dead Worker is noticed at the heartbeat timeout, not
    at Start-to-Close."""
    info = activity.info()
    activity.logger.info("execute_tool %s key=%s attempt=%d on %s", call.tool, call.key, info.attempt, info.task_queue)
    ticks = max(1, int(TOOL_SECONDS / 0.5))
    for i in range(ticks):
        activity.heartbeat({"tick": i, "of": ticks})
        await asyncio.sleep(TOOL_SECONDS / ticks)
    return ToolResult(call.key, call.tool, f"[{call.tool} output for {call.args}]", info.attempt, info.task_queue, os.getpid())


@activity.defn
async def evaluate(result: ToolResult) -> Verdict:
    return Verdict(ok=bool(result.output), note=f"{result.tool} answered from {result.task_queue}")


# ---------------------------------------------------------------- The Workflow
@workflow.defn
class AgentRun:
    def __init__(self) -> None:
        self.step = 0
        self.status = "starting"
        self.goal = ""
        self.context_summary = ""
        self.attempts = 0
        self.paused = False
        self.tier = "batch"
        self.owner = ""
        self.lanes_used: list[str] = []

    # -- decisions: pure functions of state ---------------------------------------------------
    def plan(self) -> Step:
        if self.step == 0:
            return Step("llm", prompt=f"Make a three-step plan for: {self.goal}")
        if self.step == 1:
            return Step("tool", tool="search", args={"q": self.goal})       # cpu-tools
        if self.step == 2:
            return Step("tool", tool="embed", args={"text": self.context_summary[:60]})  # gpu-tools
        return Step("finish")

    def _key(self, *parts: str) -> str:
        digest = hashlib.sha256(":".join(parts).encode()).hexdigest()[:12]
        return f"{workflow.info().workflow_id}:{self.step}:{digest}"

    def _publish(self, status: str) -> None:
        """The Query reads self.status (authoritative); the fleet view reads the Search Attributes
        (eventually consistent). The upsert is a Command recorded in History and replays."""
        self.status = status
        workflow.upsert_search_attributes([AGENT_STATUS.value_set(status), CURRENT_STEP.value_set(self.step)])

    # -- control surface ---------------------------------------------------------------------
    @workflow.signal
    def pause(self) -> None:
        self.paused = True
        self._publish("paused")

    @workflow.signal
    def resume(self) -> None:
        self.paused = False
        self._publish("running")

    @workflow.query
    def status(self) -> dict:
        return {"step": self.step, "status": self.status, "goal": self.goal, "paused": self.paused,
                "tier": self.tier, "owner": self.owner, "lanes_used": self.lanes_used}

    # -- effects: each names its lane ---------------------------------------------------------
    async def _run_tool(self, step: Step) -> ToolResult:
        call = ToolCall(self._key(step.tool, repr(sorted(step.args.items()))), step.tool, step.args)
        lane = LANE_FOR_TOOL[step.tool]
        gpu = step.tool in GPU_TOOL_NAMES
        result: ToolResult = await workflow.execute_activity(
            execute_tool, call,
            task_queue=lane,
            # Queued with no Worker on the lane → this is the timeout that fires (lab 10.1).
            schedule_to_start_timeout=GPU_SCHEDULE_TO_START if gpu else timedelta(minutes=1),
            start_to_close_timeout=timedelta(minutes=2),
            heartbeat_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=3, non_retryable_error_types=["BadArguments"]),
            # Lab 10.3: a Task Queue property, set per call. Same lane, different service order.
            priority=Priority(
                priority_key=PRIORITY_FOR_TIER[self.tier],
                fairness_key=self.owner or None,
                fairness_weight=FAIRNESS_WEIGHT_FOR_TIER[self.tier],
            ) if gpu else Priority(),
        )
        self.lanes_used.append(result.task_queue)
        verdict: Verdict = await workflow.execute_activity(
            evaluate, result, task_queue=CPU_TOOLS, start_to_close_timeout=timedelta(seconds=30),
        )
        self.context_summary = f"{result.output} ({verdict.note})"
        return result

    @workflow.run
    async def run(self, goal: str) -> dict:
        self.goal = goal
        # Who owns this run and how it is prioritised come in from the starter: Owner as a Search
        # Attribute (so it is queryable), tier as memo (metadata the Workflow reads, nobody queries).
        self.owner = workflow.info().typed_search_attributes.get(OWNER) or ""
        self.tier = workflow.memo_value("tier", "batch", type_hint=str)
        self._publish("running")
        while True:
            await workflow.wait_condition(lambda: not self.paused)
            step = self.plan()
            if step.kind == "finish":
                self._publish("done")
                return {"summary": self.context_summary, "lanes_used": self.lanes_used, "steps": self.step}
            try:
                if step.kind == "llm":
                    self.context_summary = await workflow.execute_activity(
                        call_llm, args=[step.prompt, self._key(step.prompt)],
                        task_queue=CPU_TOOLS,  # production: the `premium-models` lane, rate-limited
                        schedule_to_start_timeout=timedelta(minutes=1),
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                    self.lanes_used.append(CPU_TOOLS)
                else:
                    await self._run_tool(step)
            except ActivityError as err:
                cause = err.cause
                if isinstance(cause, TimeoutError) and cause.type == TimeoutType.SCHEDULE_TO_START:
                    # No Worker on that lane took the Task before the deadline. Record it where an
                    # operator will look (Search Attribute), then fail loudly.
                    self._publish("stalled")
                    raise ApplicationError(
                        f"lane stalled: no Worker took {step.tool or 'call_llm'} within {cause.type.name}",
                        type="LaneStalled", non_retryable=True) from err
                raise
            self.step += 1
            # A pause Signal may have landed while the step ran: republish *its* status, not ours.
            self._publish("paused" if self.paused else "running")
