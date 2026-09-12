"""Lab 11.5 — the evaluation console's contract, as tests.

    projection   a replayed (run_id, seq) changes nothing; an old event never moves a run backwards
    SSE          a client that reconnects with its last seq receives nothing twice; the stream ends at a terminal state
    publish      cases on the gpu-eval lane → awaiting_review → the approve Update publishes; the validator
                 rejects a stranger and a replayed command_id, and neither rejection reaches history
    lost write   the projection Activity writes its row, then the Worker "dies" before completing: the
                 retry finds the row and inserts nothing — still one case_done per case
    cancel       handle.cancel() stops in-flight cases within one heartbeat; the read model ends `cancelled`
    retry        the retry run covers exactly the failed case ids and points at its parent

Time-skipping test server, no Docker; the API's HTTP layer is exercised against the live dev server
in the README walk-through, its pure helpers here."""
import asyncio
import uuid
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.api.enums.v1 import EventType, IndexedValueType
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest
from temporalio.client import WorkflowFailureError, WorkflowUpdateFailedError
from temporalio.exceptions import CancelledError
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import activities as acts
import projection
from api import retry_failed_input, start_search_attributes
from config import CONSOLE_QUEUE, DB_PATH, GPU_EVAL_QUEUE, TERMINAL_STATUSES, workflow_id
from models import EvalRunInput
from workflows import EvalRun


# ------------------------------------------------------------------ harness
@pytest.fixture(scope="module")
def loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def env(loop):
    async def start():
        env = await WorkflowEnvironment.start_time_skipping()
        await env.client.operator_service.add_search_attributes(AddSearchAttributesRequest(
            namespace="default", search_attributes={
                name: IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD for name in ("Org", "RunStatus", "Model", "EvalParentRunId")}))
        return env
    env = loop.run_until_complete(start())
    yield env
    loop.run_until_complete(env.shutdown())


@pytest.fixture
def db():
    conn = projection.connect(DB_PATH)
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def fresh_ledgers():
    acts.reset_ledgers()


def workers(env, record=acts.record_run_event):
    return [Worker(env.client, task_queue=CONSOLE_QUEUE, workflows=[EvalRun], activities=[record]),
            Worker(env.client, task_queue=GPU_EVAL_QUEUE, activities=[acts.run_case], max_concurrent_activities=4)]


def new_input(n=8, fail=0.25, **kw) -> EvalRunInput:
    return EvalRunInput(run_id=workflow_id(f"run-{uuid.uuid4().hex[:6]}"), model="fake-model-7b", suite="smoke",
                        case_ids=[f"case-{i:03d}" for i in range(1, n + 1)], org=kw.pop("org", "acme"),
                        fail_fraction=fail, **kw)


async def start(env, inp):
    return await env.client.start_workflow(EvalRun.run, inp, id=inp.run_id, task_queue=CONSOLE_QUEUE,
                                           search_attributes=start_search_attributes(inp, None, False),
                                           memo={"parent_run_id": ""})


async def wait_status(handle, want, timeout=30.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        st = await handle.query(EvalRun.progress)
        if st["status"] in want:
            return st
        assert asyncio.get_running_loop().time() < deadline, f"stuck at {st['status']}"
        await asyncio.sleep(0.1)


async def event_types(handle):
    return [EventType.Name(e.event_type).removeprefix("EVENT_TYPE_") async for e in handle.fetch_history_events()]


# ------------------------------------------------------------------ projection + SSE (pure SQLite)
def test_projection_is_idempotent_and_never_moves_backwards(db):
    rid = f"proj-{uuid.uuid4().hex[:6]}"
    st = {"org": "acme", "model": "m", "suite": "s", "total": 3}
    assert projection.apply_event(db, rid, 2, "run_created", {"state": {**st, "status": "queued", "done": 0}})
    assert projection.apply_event(db, rid, 4, "status_changed", {"state": {**st, "status": "running", "done": 0}})
    assert projection.apply_event(db, rid, 6, "case_done", {"state": {**st, "status": "running", "done": 1}})
    # the retried Activity: same (run_id, seq) → no insert, no change
    assert not projection.apply_event(db, rid, 6, "case_done", {"state": {**st, "status": "running", "done": 1}})
    # a late duplicate of an OLD event cannot pull the row back to `queued`
    assert not projection.apply_event(db, rid, 2, "run_created", {"state": {**st, "status": "queued", "done": 0}})
    row = projection.get_run(db, rid)
    assert (row["status"], row["done"]) == ("running", 1)
    assert [e.seq for e in projection.events_after(db, rid, 0)] == [2, 4, 6]
    # the console's own event takes the next odd seq and cannot collide with the Workflow's next even one
    ev = projection.console_event(db, rid, "cancel_requested", {"by": "acme"}, status="cancelling")
    assert ev.seq == 7 and projection.get_run(db, rid)["status"] == "cancelling"
    assert projection.apply_event(db, rid, 8, "status_changed", {"state": {**st, "status": "cancelled", "done": 1}})
    assert projection.get_run(db, rid)["status"] == "cancelled"


def test_sse_reconnect_from_last_seq_sends_nothing_twice(db):
    rid = f"sse-{uuid.uuid4().hex[:6]}"
    st = {"org": "acme", "model": "m", "suite": "s", "total": 2}
    for seq, status, done in [(2, "queued", 0), (4, "running", 0), (6, "running", 1)]:
        projection.apply_event(db, rid, seq, "e", {"state": {**st, "status": status, "done": done}})
    frames, cursor, terminal = projection.poll_frames(db, rid, 0, TERMINAL_STATUSES)
    assert [f.split("\n")[0] for f in frames] == ["id: 2", "id: 4", "id: 6"] and cursor == 6 and not terminal
    # the connection drops; two more events land while the client is away
    projection.apply_event(db, rid, 8, "e", {"state": {**st, "status": "running", "done": 2}})
    projection.apply_event(db, rid, 10, "e", {"state": {**st, "status": "cancelled", "done": 2}})
    frames, cursor, terminal = projection.poll_frames(db, rid, 6, TERMINAL_STATUSES)   # Last-Event-ID: 6
    assert [f.split("\n")[0] for f in frames] == ["id: 8", "id: 10", "event: end"] and terminal
    # the stream generator ends by itself after the terminal frame, never sleeping once
    sleeps = []
    out = list(projection.stream_events(db, rid, 0, sleep=sleeps.append, terminal_statuses=TERMINAL_STATUSES))
    assert len(out) == 6 and out[-1].startswith("event: end") and sleeps == []


# ------------------------------------------------------------------ the Workflow, end to end
def test_run_publishes_through_the_validated_update(env, loop, db):
    async def body():
        inp = new_input(n=8, fail=0.25)
        async with Worker(env.client, task_queue=CONSOLE_QUEUE, workflows=[EvalRun], activities=[acts.record_run_event]), \
                   Worker(env.client, task_queue=GPU_EVAL_QUEUE, activities=[acts.run_case], max_concurrent_activities=4):
            handle = await start(env, inp)
            st = await wait_status(handle, {"awaiting_review", "failed"})
            assert st["status"] == "awaiting_review" and st["done"] == 8
            with pytest.raises(WorkflowUpdateFailedError) as rejected:
                await handle.execute_update(EvalRun.approve, args=["mallory", "cmd-0"])
            assert "not a reviewer" in str(rejected.value.cause)                   # the reason rides on .cause
            assert await handle.execute_update(EvalRun.approve, args=["alice", "cmd-1"]) == "published by alice (command cmd-1)"
            result = await handle.result()
            # the replayed click: publishing closed the run, so there is no open Workflow left to take an
            # Update. The server refuses it (the API answers 409 from the read model before it gets here)
            with pytest.raises(RPCError):
                await handle.execute_update(EvalRun.approve, args=["alice", "cmd-1"])
            types = await event_types(handle)
        assert result.status == "published" and result.approved_by == "alice"
        # two rejections, one acceptance: only the accepted Update reached history
        assert types.count("WORKFLOW_EXECUTION_UPDATE_ACCEPTED") == 1
        assert types.count("WORKFLOW_EXECUTION_UPDATE_COMPLETED") == 1
        row = projection.get_run(db, inp.run_id)
        assert (row["status"], row["done"], row["total"], row["org"]) == ("published", 8, 8, "acme")
        seqs = [e.seq for e in projection.events_after(db, inp.run_id, 0)]
        assert seqs == list(range(2, 2 * len(seqs) + 1, 2))                         # every Workflow seq, even, no gaps
        kinds = [e.kind for e in projection.events_after(db, inp.run_id, 0)]
        assert kinds.count("case_done") == 8
        # every case ran on the GPU exactly once, however many hiccups the RetryPolicy absorbed
        assert all(acts.CASE_EFFECTS[f"{inp.run_id}:{c}"] == 1 for c in inp.case_ids)
    loop.run_until_complete(body())


def test_projection_write_survives_a_worker_death_before_completion(env, loop, db):
    died = {"left": 2}

    @activity.defn(name="record_run_event")
    async def record_then_die(run_id, seq, kind, payload):
        inserted = await acts.record_run_event(run_id, seq, kind, payload)
        if kind == "case_done" and died["left"] > 0 and activity.info().attempt == 1:
            died["left"] -= 1
            raise RuntimeError(f"worker died after writing seq {seq}, before reporting completion")
        return inserted

    async def body():
        inp = new_input(n=4, fail=0.0)
        async with Worker(env.client, task_queue=CONSOLE_QUEUE, workflows=[EvalRun], activities=[record_then_die]), \
                   Worker(env.client, task_queue=GPU_EVAL_QUEUE, activities=[acts.run_case]):
            handle = await start(env, inp)
            await wait_status(handle, {"awaiting_review"})
            await handle.execute_update(EvalRun.approve, args=["bob", "cmd-x"])
            await handle.result()
        assert died["left"] == 0                                                   # two writes were re-executed
        events = projection.events_after(db, inp.run_id, 0)
        assert [e.kind for e in events].count("case_done") == 4                     # …and still one row per case
        assert len({e.seq for e in events}) == len(events)
    loop.run_until_complete(body())


def test_cancel_stops_cases_within_a_heartbeat(env, loop, db, monkeypatch):
    # cases long enough to be mid-flight: 4 ticks of 1 s, so each tick's heartbeat clears the SDK's throttle
    # (0.8 × heartbeat_timeout) and carries the cancel back to the Activity
    monkeypatch.setattr(acts, "CASE_SECONDS", 4.0)

    async def body():
        inp = new_input(n=16, fail=0.0)
        async with Worker(env.client, task_queue=CONSOLE_QUEUE, workflows=[EvalRun], activities=[acts.record_run_event]), \
                   Worker(env.client, task_queue=GPU_EVAL_QUEUE, activities=[acts.run_case], max_concurrent_activities=4):
            handle = await start(env, inp)
            await wait_status(handle, {"running"})
            await asyncio.sleep(1.5)                                                # four cases are mid-flight
            projection.console_event(db, inp.run_id, "cancel_requested", {"by": "acme"}, status="cancelling")
            await handle.cancel()
            with pytest.raises(WorkflowFailureError) as err:
                await handle.result()
            assert isinstance(err.value.cause, CancelledError)
            types = await event_types(handle)
        assert "WORKFLOW_EXECUTION_CANCEL_REQUESTED" in types and types[-1] == "WORKFLOW_EXECUTION_CANCELED"
        assert acts.CANCELLED_AT_TICK, "an in-flight case should have observed the cancel at a heartbeat"
        assert sum(acts.CASE_EFFECTS.values()) < 16                                 # the rest never ran
        row = projection.get_run(db, inp.run_id)
        assert row["status"] == "cancelled"
        kinds = [e.kind for e in projection.events_after(db, inp.run_id, 0)]
        assert "cancel_requested" in kinds and kinds[-1] == "status_changed"
    loop.run_until_complete(body())


# ------------------------------------------------------------------ the retry run (pure)
def test_retry_run_covers_only_the_failed_cases():
    parent = {"run_id": "test115-run-abc", "model": "m", "suite": "s", "org": "acme"}
    inp = retry_failed_input(parent, {"failed_case_ids": ["case-003", "case-007"]}, 1)
    assert (inp.run_id, inp.case_ids, inp.org) == ("test115-run-abc-retry-1", ["case-003", "case-007"], "acme")
    with pytest.raises(ValueError):
        retry_failed_input(parent, {"failed_case_ids": []}, 1)
    sa = start_search_attributes(inp, parent["run_id"], parent_sa_registered=True)
    assert {p.key.name: p.value for p in sa.search_attributes}["EvalParentRunId"] == "test115-run-abc"
