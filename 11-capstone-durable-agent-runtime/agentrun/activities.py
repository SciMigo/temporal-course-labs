"""The effects, faked. Every one is idempotent by key, and every one records what it did in a
module-level ledger so a test (or you) can count real work against attempts.

In production the ledgers are a table keyed by the idempotency key — the rows Postgres owns (module
10's tenth question). Here they live in the Worker process, which is exactly enough to show the
mechanism: a retry with the same key finds the row and does not spend again.
"""
from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from dataclasses import replace

from temporalio import activity

from .models import ToolCall, ToolResult, Verdict

LLM_SECONDS = float(os.environ.get("AGENTRUN_LLM_SECONDS", "0.05"))
TOOL_SECONDS = float(os.environ.get("AGENTRUN_TOOL_SECONDS", "0.2"))
HEARTBEAT_TICKS = 4  # heartbeats per tool call; details carry the tick reached

LLM_CACHE: dict[str, str] = {}              # key → answer
LLM_CALLS: list[str] = []                   # every *billed* call, by key
TOOL_LEDGER: dict[str, ToolResult] = {}     # key → the result of the one time the effect happened
TOOL_SIDE_EFFECTS: dict[str, int] = defaultdict(int)  # key → how many times the effect really ran
COMPENSATED: list[str] = []                 # compensations that ran, in order


def reset_ledgers() -> None:
    LLM_CACHE.clear(); LLM_CALLS.clear(); TOOL_LEDGER.clear(); TOOL_SIDE_EFFECTS.clear(); COMPENSATED.clear()


@activity.defn
async def call_llm(prompt: str, key: str) -> str:
    """One bill per key. A retry that presents the same key gets the cached answer."""
    if key in LLM_CACHE:
        activity.logger.info("call_llm cache hit key=%s attempt=%d", key, activity.info().attempt)
        return LLM_CACHE[key]
    await asyncio.sleep(LLM_SECONDS)
    LLM_CALLS.append(key)
    answer = f"[answer #{len(LLM_CALLS)} to: {prompt[:48]}]"
    LLM_CACHE[key] = answer
    return answer


@activity.defn
async def execute_tool(call: ToolCall) -> ToolResult:
    """Keyed, heartbeating, resumable. The effect is written to the ledger *last*, so an attempt
    that dies half-way leaves no row and the retry redoes the work; an attempt that finished the
    work but whose completion never reached the Service leaves a row and the retry returns it."""
    info = activity.info()
    if call.key in TOOL_LEDGER:
        activity.logger.info("execute_tool duplicate suppressed key=%s attempt=%d", call.key, info.attempt)
        return replace(TOOL_LEDGER[call.key], attempt=info.attempt, duplicate=True)
    start_tick = 0
    if info.heartbeat_details:  # a previous attempt of this same Activity got this far
        start_tick = int(info.heartbeat_details[0].get("tick", 0))
    for tick in range(start_tick, HEARTBEAT_TICKS):
        activity.heartbeat({"tick": tick + 1, "of": HEARTBEAT_TICKS})
        await asyncio.sleep(TOOL_SECONDS / HEARTBEAT_TICKS)
    TOOL_SIDE_EFFECTS[call.key] += 1
    result = ToolResult(call.key, call.tool, f"[{call.tool} output for {call.args}]", info.attempt, info.task_queue)
    TOOL_LEDGER[call.key] = result
    return result


@activity.defn
async def evaluate(result: ToolResult) -> Verdict:
    ok = bool(result.output)
    return Verdict(ok=ok, score=1.0 if ok else 0.0, note=f"{result.tool} from {result.task_queue} attempt {result.attempt}")


@activity.defn
async def compensate(name: str) -> str:
    """Undo one saga step (module 7). Idempotent: undoing twice is the same as once."""
    if name not in COMPENSATED:
        COMPENSATED.append(name)
    return f"compensated {name}"
