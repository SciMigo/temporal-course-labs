"""Lab 9: AgentRun v2 — a `verify` step inserted after `execute_tool`.

The change is one method. After every tool result, v2 runs the `evaluate` Activity and feeds
the `Verdict` back into state: a rejected result counts as a failed attempt instead of being
handed to the model as if it were fine. Everything else is inherited from v1 unchanged.

This file is BOTH the naive change (deploy it over v1 histories and the replay test fails —
exercise 9.1) AND the final code of the patch lifecycle (deploy 3: no `patched()` call left).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

from workflows import AgentRun as AgentRunV1
from workflows import AgentState, Step, ToolResult, call_llm, execute_tool  # noqa: F401  (re-exported for worker.py)

BUILD_ID = "v2"


@dataclass
class Verdict:
    ok: bool
    reason: str = ""


@activity.defn
async def evaluate(result: ToolResult) -> Verdict:
    """The verifier as an Activity. This stand-in accepts any result that reports completion."""
    activity.logger.info("evaluate key=%s", result.key)
    if result.ok and result.output.endswith("done"):
        return Verdict(ok=True)
    return Verdict(ok=False, reason=f"unverifiable output: {result.output[:40]!r}")


@workflow.defn(name="AgentRun")
class AgentRun(AgentRunV1):
    @workflow.run                                    # the SDK wants @workflow.run on the class it registers
    async def run(self, goal: str, state: AgentState | None = None) -> str:
        return await super().run(goal, state)

    async def _tool_step(self, step: Step) -> None:
        call, result = await self._run_tool(step)
        if result is None:
            return
        # NEW in v2: the verify step. A v1 history has no ActivityTaskScheduled for `evaluate`
        # here — where v1 went on to schedule call_llm, v2 schedules evaluate.
        verdict = await workflow.execute_activity(
            evaluate, result, start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3))
        if not verdict.ok:
            self.attempts += 1
            self.context_summary += f"\nRESULT: error verify: {verdict.reason}"
            return
        self._record_tool_result(call, result)
