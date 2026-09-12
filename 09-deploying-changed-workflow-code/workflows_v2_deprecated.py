"""Lab 9: AgentRun v2, patch deprecated — deploy 2 of the patch lifecycle.

Ship this once no open execution is still on the v1 branch. `deprecate_patch` keeps
recording the marker for executions that already have it but removes the branch: the code
reads as if verify had always been there. A pre-patch (v1) history can NOT replay under this
file any more — that is what "no open execution on the old branch" guards.
"""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from workflows import AgentRun as AgentRunV1
from workflows import AgentState, Step, call_llm, execute_tool  # noqa: F401
from workflows_v2 import evaluate  # noqa: F401
from workflows_v2_patched import PATCH

BUILD_ID = "v2-deprecated"


@workflow.defn(name="AgentRun")
class AgentRun(AgentRunV1):
    @workflow.run                                    # the SDK wants @workflow.run on the class it registers
    async def run(self, goal: str, state: AgentState | None = None) -> str:
        return await super().run(goal, state)

    async def _tool_step(self, step: Step) -> None:
        call, result = await self._run_tool(step)
        if result is None:
            return
        workflow.deprecate_patch(PATCH)              # no branch left; marker kept for old histories
        verdict = await workflow.execute_activity(
            evaluate, result, start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3))
        if not verdict.ok:
            self.attempts += 1
            self.context_summary += f"\nRESULT: error verify: {verdict.reason}"
            return
        self._record_tool_result(call, result)
