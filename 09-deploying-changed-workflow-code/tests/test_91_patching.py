"""9.1 — The replay matrix: every recorded history against every code version.

Rows are histories in tests/histories/ (the Workflow ID is the filename); columns are the four
files of the patch lifecycle. A cell is "clean" or the NondeterminismError a Worker would raise
on that history's next Workflow Task. Read the matrix before the assertions: it is the whole
lesson of exercise 9.1 in one table."""
from __future__ import annotations

from pathlib import Path

import pytest
from temporalio import workflow
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

import workflows
import workflows_v2
import workflows_v2_deprecated
import workflows_v2_patched

HISTORIES = Path(__file__).parent / "histories"
CODE = {"v1": workflows, "v2": workflows_v2, "v2_patched": workflows_v2_patched,
        "v2_deprecated": workflows_v2_deprecated}

# history -> {code: replays clean?}   (every cell observed with temporalio 1.32.0)
#   lab08-agent-42        recorded under v1: no marker, no evaluate
#   lab09-agent-43        recorded under v2_patched: marker + evaluate
#   lab09-agent-44        recorded under v2 (final): evaluate, no marker
#
# Read down the v2 column: the naive/final code fails BOTH the v1 history (it schedules
# `evaluate` where the history has `call_llm`) AND the patched history ("Non-deprecated patch
# marker encountered ... but there is no corresponding change command"). Removing the
# `patched()` call is deploy 3, and it waits until every execution that recorded the marker is
# closed and out of retention — not merely until the v1 ones are.
EXPECTED = {
    "lab08-agent-42": {"v1": True,  "v2": False, "v2_patched": True,  "v2_deprecated": False},
    "lab09-agent-43": {"v1": False, "v2": False, "v2_patched": True,  "v2_deprecated": True},
    "lab09-agent-44": {"v1": False, "v2": True,  "v2_patched": False, "v2_deprecated": True},
}


def load(path: Path) -> WorkflowHistory:
    return WorkflowHistory.from_json(path.stem, path.read_text())


async def replay(code: str, history: WorkflowHistory):
    return await Replayer(workflows=[CODE[code].AgentRun]).replay_workflow(
        history, raise_on_replay_failure=False)


@pytest.mark.parametrize("code", list(CODE))
@pytest.mark.parametrize("stem", list(EXPECTED))
async def test_replay_matrix(stem: str, code: str):
    result = await replay(code, load(HISTORIES / f"{stem}.json"))
    clean = result.replay_failure is None
    assert clean == EXPECTED[stem][code], (
        f"{stem} under {code}: expected {'clean' if EXPECTED[stem][code] else 'a failure'}, "
        f"got {result.replay_failure!r}")
    if not clean:
        assert isinstance(result.replay_failure, workflow.NondeterminismError)


# The three sentences of 9.1, as tests you can run one at a time.
async def test_the_naive_change_breaks_old_histories():
    result = await replay("v2", load(HISTORIES / "lab08-agent-42.json"))
    assert isinstance(result.replay_failure, workflow.NondeterminismError)
    # v1 scheduled call_llm after the tool; v2 schedules evaluate at the same point.
    assert ("Activity type of scheduled event 'call_llm' does not match activity type of "
            "activity command 'evaluate'") in str(result.replay_failure)


async def test_the_patch_makes_both_branches_replay_clean():
    for stem in ("lab08-agent-42", "lab09-agent-43"):
        result = await replay("v2_patched", load(HISTORIES / f"{stem}.json"))
        assert result.replay_failure is None, (stem, result.replay_failure)


async def test_deprecating_the_patch_needs_no_pre_patch_execution_left():
    ok = await replay("v2_deprecated", load(HISTORIES / "lab09-agent-43.json"))
    assert ok.replay_failure is None
    old = await replay("v2_deprecated", load(HISTORIES / "lab08-agent-42.json"))
    assert isinstance(old.replay_failure, workflow.NondeterminismError)


async def test_removing_the_patch_call_needs_no_marked_execution_left():
    result = await replay("v2", load(HISTORIES / "lab09-agent-43.json"))
    assert "Non-deprecated patch marker encountered for change insert-verify-step" in str(result.replay_failure)
