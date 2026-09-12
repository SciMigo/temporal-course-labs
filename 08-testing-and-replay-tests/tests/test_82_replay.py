"""8.2 — Replay tests: every history in tests/histories/ must replay under the current AgentRun.

A replay test is a Worker coming back — against a file, in CI, before any real Worker sees the
new code. `Replayer` feeds the recorded events to the Workflow code and checks that the code
issues the same Commands the history recorded; a mismatch is the non-determinism error a real
Worker would raise on its first Workflow Task."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from temporalio import workflow
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from workflows import AgentRun, AgentState

HISTORIES = Path(__file__).parent / "histories"


def load(path: Path) -> WorkflowHistory:
    # The filename is the Workflow ID (export_history.py names files that way), so from_json
    # gets it for nothing. `temporal workflow show --output json` and the UI download match.
    return WorkflowHistory.from_json(path.stem, path.read_text())


@pytest.mark.parametrize("path", sorted(HISTORIES.glob("*.json")), ids=lambda p: p.stem)
async def test_history_replays_under_current_code(path: Path):
    await Replayer(workflows=[AgentRun]).replay_workflow(load(path))   # raises on non-determinism


# ---------------------------------------------------------------- the failure, on purpose
@workflow.defn(name="AgentRun")            # same Workflow Type: the history says "AgentRun"
class AgentRunWithAnExtraTimer(AgentRun):
    """The change 9.1 warns about, in its smallest form: one new Command before the first
    Activity. Every recorded history says "schedule call_llm" where this code says "start a
    timer"; the replay must fail on every one of them."""

    @workflow.run
    async def run(self, goal: str, state: AgentState | None = None) -> str:
        await asyncio.sleep(1)                 # NEW: a Command the histories never recorded
        return await super().run(goal, state)


@pytest.mark.parametrize("path", sorted(HISTORIES.glob("*.json")), ids=lambda p: p.stem)
async def test_changed_workflow_fails_replay(path: Path):
    result = await Replayer(workflows=[AgentRunWithAnExtraTimer]).replay_workflow(
        load(path), raise_on_replay_failure=False)
    assert result.replay_failure is not None, "the changed code replayed clean — it should not"
    assert isinstance(result.replay_failure, workflow.NondeterminismError)
    # The message names the event the code could not match: the first ActivityTaskScheduled.
    assert "ActivityTaskScheduled" in str(result.replay_failure)
