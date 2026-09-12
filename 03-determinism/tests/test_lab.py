"""Check yourself: the fixed build replays clean; the broken builds do not — and you can see why.

Runs on Temporal's time-skipping test server. The build under test is selected the way the
Worker selects it, through LAB03_BREAK, set before each run (the sandbox re-imports
workflows.py per execution, so the variable is read fresh).  PYTHONPATH=.. pytest tests/
"""
from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
from http.server import ThreadingHTTPServer

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", ".."))

import model_server  # noqa: E402
import workflows  # noqa: E402
from temporalio.client import WorkflowHistory  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Replayer, Worker  # noqa: E402
from temporalio.workflow import NondeterminismError  # noqa: E402

TASK_QUEUE = "lab-03-test"


async def _run(wf_id: str, budget: int = 5) -> WorkflowHistory:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=TASK_QUEUE,
                          workflows=[workflows.AgentRun], activities=[workflows.call_llm]):
            handle = await env.client.start_workflow(
                workflows.AgentRun.run, args=["test goal", 15, budget, 5], id=wf_id, task_queue=TASK_QUEUE
            )
            await handle.result()
            return await handle.fetch_history()


def _replay(history: WorkflowHistory) -> Exception | None:
    async def go():
        await Replayer(workflows=[workflows.AgentRun]).replay_workflow(history)
    try:
        asyncio.run(go())
        return None
    except Exception as e:  # noqa: BLE001
        return e


@pytest.fixture
def build(monkeypatch):
    """Select the build: build("clock") is `LAB03_BREAK=clock python worker.py`."""
    def select(mode: str | None) -> None:
        if mode is None:
            monkeypatch.delenv("LAB03_BREAK", raising=False)
        else:
            monkeypatch.setenv("LAB03_BREAK", mode)
    return select


def test_fixed_build_replays_clean(build):
    build(None)
    history = asyncio.run(_run("lab03-test-fixed"))
    assert _replay(history) is None


def test_clock_break_fails_replay_once_the_budget_has_passed(build):
    """3.2: the run passed step 0 with the wall clock inside the 1 s budget; any replay after
    that reads a later wall clock, plans `finish`, and proposes CompleteWorkflowExecution where
    history has the Activity (or the jitter timer) at event 5."""
    build("clock")
    history = asyncio.run(_run("lab03-test-clock", budget=1))
    time.sleep(1.5)
    err = _replay(history)
    assert isinstance(err, NondeterminismError)
    assert "HistoryEvent(id: 5" in str(err)
    # The fix replays the same history clean: workflow.now() is history time.
    build(None)
    assert _replay(history) is None


def test_model_in_plan_fails_replay_and_pays_again(build, monkeypatch):
    """3.3: with the model asked inside plan(), the replay asks it again (request #3) and gets a
    different answer. With the model behind call_llm, the replay asks nothing."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), model_server.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}"
    try:
        model_server.REQUESTS = 0
        monkeypatch.setenv("LAB03_MODEL_URL", url)
        build("network")
        history = asyncio.run(_run("lab03-test-network", budget=300))
        assert model_server.REQUESTS == 2                      # step 0: continue; step 1: finish
        assert isinstance(_replay(history), NondeterminismError)
        assert model_server.REQUESTS == 3                      # the replay paid for one more

        model_server.REQUESTS = 0
        build(None)
        monkeypatch.setattr(workflows, "MODEL_URL", url)       # call_llm runs outside the sandbox
        history = asyncio.run(_run("lab03-test-network-fixed", budget=300))
        assert model_server.REQUESTS == 2
        assert history.events and sum(
            1 for e in history.events if e.HasField("activity_task_scheduled_event_attributes")
        ) == 2                                                  # two call_llm steps, model-driven
        assert _replay(history) is None
        assert model_server.REQUESTS == 2                      # replay asked nothing
    finally:
        server.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
