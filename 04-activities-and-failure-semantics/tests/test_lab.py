"""Check yourself: each timeout is observable, a checkpoint survives an attempt, an Activity is
at-least-once until you key it, and an Activity can complete from outside the Worker.

Runs on the time-skipping test server; Activity sleeps are real seconds, so this takes ~1 min.
    PYTHONPATH=.. pytest tests/
"""
from __future__ import annotations

import asyncio
import glob
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", ".."))

import workflows  # noqa: E402
from temporalio.api.enums.v1 import EventType, RetryState, TimeoutType  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

TASK_QUEUE = "lab-04-test"


@pytest.fixture
def state_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows, "STATE_DIR", str(tmp_path))
    return tmp_path


async def _run(scenario: str, wf_id: str, before_result=None):
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=TASK_QUEUE, workflows=[workflows.AgentRun],
                          activities=[workflows.call_llm, workflows.execute_tool, workflows.charge]):
            handle = await env.client.start_workflow(
                workflows.AgentRun.run, args=["test goal", scenario], id=wf_id, task_queue=TASK_QUEUE
            )
            if before_result is not None:
                await before_result(env.client, handle)
            result = await handle.result()
            history = await handle.fetch_history()
            return result, history


def _terminal_activity_events(history) -> list[str]:
    """'TIMED_OUT:<type>' or 'FAILED:<retry_state>' for every closed execute_tool attempt chain."""
    out = []
    for e in history.events:
        if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
            tt = e.activity_task_timed_out_event_attributes.failure.timeout_failure_info.timeout_type
            out.append("TIMED_OUT:" + TimeoutType.Name(tt).removeprefix("TIMEOUT_TYPE_"))
        elif e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED:
            rs = e.activity_task_failed_event_attributes.retry_state
            out.append("FAILED:" + RetryState.Name(rs).removeprefix("RETRY_STATE_"))
    return out


@pytest.mark.parametrize("scenario,expected", [
    ("schedule_to_start", "SCHEDULE_TO_START"),
    ("start_to_close", "START_TO_CLOSE"),
    ("heartbeat", "HEARTBEAT"),
])
def test_each_timeout_is_one_event_and_one_exception(state_dir, scenario, expected):
    result, history = asyncio.run(_run(scenario, f"lab04-test-{scenario}"))
    assert _terminal_activity_events(history) == [f"TIMED_OUT:{expected}"]
    assert f"FAILED: TimeoutError type={expected}" in result


def test_schedule_to_close_bounds_all_attempts(state_dir):
    """The budget for the whole retry chain. Its footprint depends on the server: the dev server
    writes ActivityTaskTimedOut(SCHEDULE_TO_CLOSE) carrying the last attempt's failure as cause;
    the time-skipping test server (which skips the backoff, so more attempts fit) writes
    ActivityTaskFailed with retry_state=TIMEOUT and the last ApplicationError. Either way: one
    terminal Event, several attempts, and the Workflow's decision afterwards."""
    result, history = asyncio.run(_run("schedule_to_close", "lab04-test-schedule_to_close"))
    terminal = _terminal_activity_events(history)
    assert terminal in (["TIMED_OUT:SCHEDULE_TO_CLOSE"], ["FAILED:TIMEOUT"])
    assert "FAILED: TimeoutError type=SCHEDULE_TO_CLOSE" in result or "FAILED: ApplicationError type=ToolCrash" in result
    started = [e.activity_task_started_event_attributes for e in history.events
               if e.HasField("activity_task_started_event_attributes")]
    if started[-1].scheduled_event_id > 5:          # a Started Event for the tool, if any, names attempt > 1
        assert started[-1].attempt > 1


def test_heartbeat_checkpoint_resumes_the_next_attempt(state_dir):
    """4.2: attempt 1 goes silent after shard 5; the heartbeat timeout fails it; attempt 2 reads
    heartbeat details [5] and runs shards 5..7 only. Shards 0..4 happened exactly once."""
    result, history = asyncio.run(_run("shards_hang", "lab04-test-shards"))
    assert result.endswith("| shards: 8 shards")
    lines = (state_dir / "effects.log").read_text().splitlines()
    shards = [int(line.split(" shard ")[1].split()[0]) for line in lines]
    assert shards == list(range(8))                               # no shard ran twice
    assert [line.split("attempt ")[1] for line in lines] == ["1"] * 5 + ["2"] * 3
    started = [e for e in history.events if e.HasField("activity_task_started_event_attributes")]
    assert started[-1].activity_task_started_event_attributes.attempt == 2


def test_tool_effect_happens_twice_until_keyed(state_dir, monkeypatch):
    """4.3: the send tool does its effect, then attempt 1 never reports. The Service cannot tell
    hung from dead, times the attempt out, retries, and the effect happens again. Keyed, attempt
    2 finds the key and returns without acting."""
    monkeypatch.setattr(workflows, "IDEMPOTENT", False)
    result, _ = asyncio.run(_run("send_hang", "lab04-test-send-dup"))
    sent = [l for l in (state_dir / "effects.log").read_text().splitlines() if " sent " in l]
    assert len(sent) == 2 and result.endswith("| send: sent")

    (state_dir / "effects.log").unlink()
    monkeypatch.setattr(workflows, "IDEMPOTENT", True)
    result, _ = asyncio.run(_run("send_hang", "lab04-test-send-keyed"))
    sent = [l for l in (state_dir / "effects.log").read_text().splitlines() if " sent " in l]
    assert len(sent) == 1 and result.endswith("| send: already sent (deduplicated by key)")


def test_charge_is_at_least_once_until_keyed(state_dir, monkeypatch):
    monkeypatch.setattr(workflows, "IDEMPOTENT", False)
    asyncio.run(_run("charge_hang", "lab04-test-charge-dup"))
    assert len((state_dir / "ledger.txt").read_text().splitlines()) == 2
    (state_dir / "ledger.txt").unlink()
    monkeypatch.setattr(workflows, "IDEMPOTENT", True)
    result, _ = asyncio.run(_run("charge_hang", "lab04-test-charge-keyed"))
    assert len((state_dir / "ledger.txt").read_text().splitlines()) == 1
    assert result.endswith("| charge: already charged order-1")


def test_async_completion_from_outside_the_worker(state_dir):
    """4.5: the Activity returns with raise_complete_async(); a Client completes it by task token."""
    async def complete_from_outside(client, handle):
        tokens = []
        for _ in range(50):
            tokens = glob.glob(str(state_dir / "async" / "*.token"))
            if tokens:
                break
            await asyncio.sleep(0.1)
        assert tokens, "the Activity never wrote its task token"
        with open(tokens[0], "rb") as f:
            token = f.read()
        await client.get_async_activity_handle(task_token=token).complete(
            workflows.ToolResult(key="k", tool="finetune", output="artifact://done")
        )

    result, history = asyncio.run(_run("finetune", "lab04-test-finetune", complete_from_outside))
    assert result.endswith("| finetune: artifact://done")
    assert any(e.HasField("activity_task_completed_event_attributes") for e in history.events)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
