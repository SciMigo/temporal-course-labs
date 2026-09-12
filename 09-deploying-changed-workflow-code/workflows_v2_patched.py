"""Lab 9: AgentRun v2, patched — deploy 1 of the patch lifecycle.

`workflow.patched("insert-verify-step")` is a branch tied to a marker. A new execution records
the marker and takes the verify branch; a v1 history has no marker, so replay takes the old
branch and the Commands still match. Both kinds of history replay clean under this file.
"""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from workflows import AgentRun as AgentRunV1
from workflows import AgentState, Step, call_llm, execute_tool  # noqa: F401
from workflows_v2 import evaluate  # noqa: F401

BUILD_ID = "v2-patched"
PATCH = "insert-verify-step"


@workflow.defn(name="AgentRun")
class AgentRun(AgentRunV1):
    @workflow.run                                    # the SDK wants @workflow.run on the class it registers
    async def run(self, goal: str, state: AgentState | None = None) -> str:
        return await super().run(goal, state)

    async def _tool_step(self, step: Step) -> None:
        call, result = await self._run_tool(step)
        if result is None:
            return
        if workflow.patched(PATCH):                  # new executions: marker recorded, verify runs
            verdict = await workflow.execute_activity(
                evaluate, result, start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3))
            if not verdict.ok:
                self.attempts += 1
                self.context_summary += f"\nRESULT: error verify: {verdict.reason}"
                return
        else:                                        # v1 histories: no marker, no verify, as before
            pass
        self._record_tool_result(call, result)
