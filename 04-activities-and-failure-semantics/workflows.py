"""Lab 4: AgentRun, stage 4 — `execute_tool` joins `call_llm`, with timeouts, a RetryPolicy,
heartbeats and an idempotency key. Everything in this file is at-least-once; the key is what
makes that safe.

AgentRun.run(goal, scenario) plans three steps: call_llm, one tool call, finish. `scenario`
selects the tool and how the Workflow invokes it (the timeouts and retry policy are Workflow
code — see SCENARIOS). The tools themselves live in `execute_tool` and write their side effects
to plain files under `.state/` so you can count how many times an effect really happened:

    .state/effects.log     one line per tool side effect  (4.2, 4.3)
    .state/ledger.txt      one line per charge            (4.3)
    .state/llm_cache/      call_llm's answers by key       (idempotent by construction)
    .state/async/          task tokens of Activities waiting for asynchronous completion (4.5)

LAB04_IDEMPOTENT=1 turns on the fix in 4.3: `charge` and the `send` tool look their key up
before acting.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from common import TASK_QUEUE  # the NOBODY queue further down is named after it

STATE_DIR = os.environ.get("LAB04_STATE_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), ".state"))
IDEMPOTENT = os.environ.get("LAB04_IDEMPOTENT", "") == "1"


# ----------------------------------------------------------------------------- data types

@dataclass
class ToolCall:
    key: str            # idempotency key, computed once in Workflow code
    tool: str
    args: dict = field(default_factory=dict)


@dataclass
class ToolResult:
    key: str
    tool: str
    output: str


@dataclass
class Step:
    kind: str           # "llm" | "tool" | "charge" | "finish"
    prompt: str = ""
    tool: str = ""
    args: dict = field(default_factory=dict)


# ----------------------------------------------------------------------------- side-effect files

def _path(*parts: str) -> str:
    os.makedirs(os.path.join(STATE_DIR, *parts[:-1]), exist_ok=True)
    return os.path.join(STATE_DIR, *parts)


def _append(name: str, line: str) -> None:
    with open(_path(name), "a") as f:
        f.write(line + "\n")


def _lines(name: str) -> list[str]:
    try:
        with open(_path(name)) as f:
            return f.read().splitlines()
    except FileNotFoundError:
        return []


async def _hold(seconds: float) -> None:
    """The Worker 'dies' here in 4.3: the effect is done, the completion not yet reported."""
    await asyncio.sleep(seconds)


# ----------------------------------------------------------------------------- activities

@activity.defn
async def call_llm(prompt: str, key: str) -> str:
    """Keyed and cached: attempt 2 returns attempt 1's answer instead of paying again."""
    cache = _path("llm_cache", hashlib.sha256(key.encode()).hexdigest()[:16] + ".txt")
    if os.path.exists(cache):
        with open(cache) as f:
            activity.logger.info("call_llm key=%s cache HIT (attempt %d)", key, activity.info().attempt)
            return f.read()
    answer = f"[model answer to: {prompt[:40]}]"
    with open(cache, "w") as f:
        f.write(answer)
    activity.logger.info("call_llm key=%s billed (attempt %d)", key, activity.info().attempt)
    return answer


@activity.defn
async def charge(order_id: str, hold_seconds: float = 6.0, hang_first: bool = False) -> str:
    """The canonical illustration: the card is charged, then the Worker dies before reporting.
    `hang_first`: attempt 1 never reports (a hung Worker) — the Service cannot tell it from a dead one."""
    info = activity.info()
    if IDEMPOTENT and any(line.startswith(f"charged {order_id} ") for line in _lines("ledger.txt")):
        activity.logger.info("charge %s: already in the ledger, returning without charging (attempt %d)", order_id, info.attempt)
        return f"already charged {order_id}"
    _append("ledger.txt", f"charged {order_id} by pid {os.getpid()} attempt {info.attempt}")
    activity.logger.info("charged %s (attempt %d); holding %.0fs before reporting -- kill -9 %d now",
                         order_id, info.attempt, hold_seconds, os.getpid())
    await _hold(3600 if hang_first and info.attempt == 1 else hold_seconds)
    return f"charged {order_id}"


@activity.defn
async def execute_tool(call: ToolCall) -> ToolResult:
    info = activity.info()
    log = activity.logger

    if call.tool == "echo":
        return ToolResult(call.key, call.tool, f"echo: {call.args.get('text', '')}")

    if call.tool == "sleep":                           # no heartbeat: the Service hears nothing
        seconds = float(call.args.get("seconds", 5))
        log.info("sleep tool: %.0fs of silence (attempt %d)", seconds, info.attempt)
        await asyncio.sleep(seconds)
        return ToolResult(call.key, call.tool, f"slept {seconds}s")

    if call.tool == "fail":
        log.info("fail tool: raising (attempt %d)", info.attempt)
        raise ApplicationError(
            f"tool crashed on attempt {info.attempt}",
            type=call.args.get("type", "ToolCrash"),
            non_retryable=bool(call.args.get("non_retryable", False)),
        )

    if call.tool == "shards":                          # 4.2: heartbeat with a checkpoint
        total = int(call.args.get("shards", 10))
        done = info.heartbeat_details[0] if info.heartbeat_details else 0
        log.info("shards tool attempt %d: resuming at shard %d/%d (heartbeat details=%s)",
                 info.attempt, done, total, list(info.heartbeat_details))
        for i in range(done, total):
            await asyncio.sleep(1)                     # the work of one shard
            _append("effects.log", f"{call.key} shard {i} by pid {os.getpid()} attempt {info.attempt}")
            activity.heartbeat(i + 1)                  # checkpoint: the next attempt resumes here
            log.info("shard %d done -> heartbeat(%d)   [pid %d]", i, i + 1, os.getpid())
            if info.attempt == 1 and call.args.get("hang_after") == i + 1:
                log.info("attempt 1 hangs here: no more heartbeats (the Service cannot tell this from a dead Worker)")
                await asyncio.sleep(3600)
        return ToolResult(call.key, call.tool, f"{total} shards")

    if call.tool == "send":                            # 4.3: the effect, then the death
        if IDEMPOTENT and any(line.startswith(f"{call.key} sent ") for line in _lines("effects.log")):
            log.info("send: key %s already in effects.log, returning the recorded outcome (attempt %d)", call.key, info.attempt)
            return ToolResult(call.key, call.tool, "already sent (deduplicated by key)")
        _append("effects.log", f"{call.key} sent by pid {os.getpid()} attempt {info.attempt}")
        hold = float(call.args.get("hold_seconds", 6))
        log.info("sent (attempt %d); holding %.0fs before reporting -- kill -9 %d now", info.attempt, hold, os.getpid())
        await _hold(3600 if call.args.get("hang_first") and info.attempt == 1 else hold)
        return ToolResult(call.key, call.tool, "sent")

    if call.tool == "finetune":                        # 4.5: asynchronous completion
        token_file = _path("async", hashlib.sha256(call.key.encode()).hexdigest()[:16] + ".token")
        with open(token_file, "wb") as f:
            f.write(info.task_token)
        log.info("finetune: job submitted with task token; %s; the Activity stays open, the Worker is free",
                 token_file)
        activity.raise_complete_async()

    raise ApplicationError(f"unknown tool {call.tool!r}", type="InvalidToolArguments", non_retryable=True)


# ----------------------------------------------------------------------------- the scenarios (Workflow code)

NOBODY = TASK_QUEUE + "-gpu"   # a Task Queue no Worker polls

SCENARIOS: dict[str, dict] = {
    # the specified execute_tool call from the reading — 4.4 starts from this
    "default": dict(tool="echo", args={"text": "hello"}, options=dict(
        start_to_close_timeout=timedelta(minutes=2),
        schedule_to_close_timeout=timedelta(minutes=15),
        heartbeat_timeout=timedelta(seconds=10),
        retry_policy=RetryPolicy(maximum_attempts=3, backoff_coefficient=2.0,
                                 non_retryable_error_types=["InvalidToolArguments"]),
    )),
    # 4.1 — one timeout each
    "schedule_to_start": dict(tool="echo", args={"text": "gpu"}, options=dict(
        task_queue=NOBODY,                                  # queued where no Worker polls
        schedule_to_start_timeout=timedelta(seconds=2),
        start_to_close_timeout=timedelta(seconds=10),
    )),
    "start_to_close": dict(tool="sleep", args={"seconds": 5}, options=dict(
        start_to_close_timeout=timedelta(seconds=1),
        retry_policy=RetryPolicy(maximum_attempts=1),       # so the timeout is final, not retried
    )),
    "schedule_to_close": dict(tool="fail", args={}, options=dict(
        start_to_close_timeout=timedelta(seconds=10),
        schedule_to_close_timeout=timedelta(seconds=3),      # the budget for all attempts
        retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=500), backoff_coefficient=1.0),
    )),
    "heartbeat": dict(tool="sleep", args={"seconds": 5}, options=dict(
        start_to_close_timeout=timedelta(seconds=30),
        heartbeat_timeout=timedelta(seconds=1),              # 5 s of silence is 4 s too many
        retry_policy=RetryPolicy(maximum_attempts=1),
    )),
    # 4.2 — checkpoint resume
    "shards": dict(tool="shards", args={"shards": 10}, options=dict(
        start_to_close_timeout=timedelta(seconds=60),
        heartbeat_timeout=timedelta(seconds=3),
        retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3),
    )),
    "shards_hang": dict(tool="shards", args={"shards": 8, "hang_after": 5}, options=dict(
        start_to_close_timeout=timedelta(seconds=60),
        heartbeat_timeout=timedelta(seconds=3),          # silence is noticed in 3 s, not 60
        retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3),
    )),
    # 4.3 — duplicate execution under a Worker death
    "send": dict(tool="send", args={"hold_seconds": 6}, options=dict(
        start_to_close_timeout=timedelta(seconds=10),        # a live attempt reports in 6 s; a dead one is noticed in 10 s
        retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3),
    )),
    "charge": dict(tool="charge", args={"order_id": "order-1", "hold_seconds": 6}, options=dict(
        start_to_close_timeout=timedelta(seconds=10),
        retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3),
    )),
    # the same two without a kill: attempt 1 does the effect and then never reports
    "send_hang": dict(tool="send", args={"hold_seconds": 1, "hang_first": True}, options=dict(
        start_to_close_timeout=timedelta(seconds=3),
        retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3),
    )),
    "charge_hang": dict(tool="charge", args={"order_id": "order-1", "hold_seconds": 1, "hang_first": True}, options=dict(
        start_to_close_timeout=timedelta(seconds=3),
        retry_policy=RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3),
    )),
    # 4.5 — asynchronous completion
    "finetune": dict(tool="finetune", args={"model": "agent-7b"}, options=dict(
        start_to_close_timeout=timedelta(minutes=5),
        retry_policy=RetryPolicy(maximum_attempts=1),
    )),
}


@workflow.defn
class AgentRun:
    def __init__(self) -> None:
        self.step = 0
        self.status = "running"
        self.goal = ""
        self.context_summary = ""
        self.attempts = 0
        self.scenario = "default"

    def plan(self) -> Step:
        """Workflow code: a pure function of state."""
        if self.step == 0:
            return Step("llm", prompt=f"Make a one-line plan for: {self.goal}")
        if self.step == 1:
            spec = SCENARIOS[self.scenario]
            return Step("charge" if spec["tool"] == "charge" else "tool", tool=spec["tool"], args=spec["args"])
        return Step("finish")

    @workflow.run
    async def run(self, goal: str, scenario: str = "default") -> str:
        self.goal = goal
        self.scenario = scenario
        wid = workflow.info().workflow_id
        while True:
            step = self.plan()
            if step.kind == "finish":
                self.status = "done"
                return self.context_summary

            if step.kind == "llm":
                key = f"{wid}:{self.step}:{hashlib.sha256(step.prompt.encode()).hexdigest()[:12]}"
                self.context_summary = await workflow.execute_activity(
                    call_llm, args=[step.prompt, key],
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
                self.step += 1
                continue

            options = SCENARIOS[self.scenario]["options"]
            try:
                if step.kind == "charge":
                    out = await workflow.execute_activity(
                        charge, args=[step.args["order_id"], step.args["hold_seconds"], step.args.get("hang_first", False)],
                        **options,
                    )
                else:
                    digest = hashlib.sha256(json.dumps(step.args, sort_keys=True).encode()).hexdigest()[:12]
                    call = ToolCall(key=f"{wid}:{self.step}:{digest}", tool=step.tool, args=step.args)
                    result = await workflow.execute_activity(execute_tool, call, **options)
                    out = result.output
                self.context_summary += f" | {step.tool}: {out}"
            except ActivityError as e:
                # The failure is history; what to do about it is Workflow code.
                self.attempts += 1
                cause = e.cause
                kind = getattr(getattr(cause, "type", None), "name", getattr(cause, "type", None))
                self.context_summary += f" | {step.tool} FAILED: {type(cause).__name__} type={kind}: {cause}"
            self.step += 1
