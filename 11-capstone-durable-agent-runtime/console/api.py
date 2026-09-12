"""The evaluation console's HTTP surface. Reads come from SQLite (the projection); writes go to
Temporal (start, cancel, Update). Never Temporal visibility for a list — the projection is the
product's read model, visibility is the operator's.

    POST /runs                      start an EvalRun (202); Workflow ID == run_id
    GET  /runs?org=&status=&model=&cursor=&limit=     keyset pagination on (created_at, run_id)
    GET  /runs/{id}                 row + last events
    GET  /runs/{id}/events?after=   SSE, resumes from the cursor; polls SQLite every 0.5 s
    POST /runs/{id}/cancel          handle.cancel() + an immediate `cancel_requested` projection event
    POST /runs/{id}/retry-failed    new EvalRun over the failed case ids; id `{id}-retry-{n}`; memo parent_run_id
    POST /runs/{id}/approve         execute_update(EvalRun.approve)

Every request carries `X-Org`. A run that belongs to another org does not exist (404).
"""
from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from temporalio.api.operatorservice.v1 import ListSearchAttributesRequest
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.common import SearchAttributePair, TypedSearchAttributes
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode

import projection
from config import (ALL_STATUSES, CONSOLE_QUEUE, DB_PATH, DEFAULT_FAIL_FRACTION, DEFAULT_REVIEWERS, NAMESPACE,
                    TEMPORAL_ADDRESS, TERMINAL_STATUSES, workflow_id)
from models import EvalRunInput
from workflows import MODEL, ORG, PARENT_RUN_ID, RUN_STATUS, EvalRun

SSE_POLL_SECONDS = 0.5


class StartRun(BaseModel):
    run_id: str | None = None
    model: str = "fake-model-7b"
    suite: str = "smoke"
    case_ids: list[str] | None = None
    cases: int = Field(12, ge=1, le=500)
    fail_fraction: float = Field(DEFAULT_FAIL_FRACTION, ge=0.0, le=1.0)
    reviewers: list[str] = Field(default_factory=lambda: list(DEFAULT_REVIEWERS))


class Approve(BaseModel):
    reviewer: str
    command_id: str


# ------------------------------------------------------------------ pure helpers (tested directly)
def retry_failed_input(parent: dict[str, Any], state: dict[str, Any], n: int, *, reviewers: list[str] | None = None,
                       fail_fraction: float = DEFAULT_FAIL_FRACTION) -> EvalRunInput:
    """The retry run: only the failed case ids, id `{parent}-retry-{n}`, same org/model/suite."""
    failed = list(state.get("failed_case_ids") or [])
    if not failed:
        raise ValueError("nothing to retry: the run has no failed cases")
    return EvalRunInput(run_id=f"{parent['run_id']}-retry-{n}", model=parent["model"], suite=parent["suite"],
                        case_ids=failed, org=parent["org"], fail_fraction=fail_fraction,
                        reviewers=reviewers or list(DEFAULT_REVIEWERS))


def start_search_attributes(inp: EvalRunInput, parent_run_id: str | None, parent_sa_registered: bool) -> TypedSearchAttributes:
    pairs = [SearchAttributePair(ORG, inp.org), SearchAttributePair(MODEL, inp.model), SearchAttributePair(RUN_STATUS, "queued")]
    if parent_run_id and parent_sa_registered:
        pairs.append(SearchAttributePair(PARENT_RUN_ID, parent_run_id))
    return TypedSearchAttributes(pairs)


# ------------------------------------------------------------------ app
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = await Client.connect(TEMPORAL_ADDRESS, namespace=NAMESPACE)
    listed = await app.state.client.operator_service.list_search_attributes(ListSearchAttributesRequest(namespace=NAMESPACE))
    app.state.parent_sa_registered = "EvalParentRunId" in listed.custom_attributes
    app.state.db = projection.connect(DB_PATH)
    print(f"console api: temporal={TEMPORAL_ADDRESS} db={DB_PATH} EvalParentRunId registered={app.state.parent_sa_registered}")
    yield
    app.state.db.close()


app = FastAPI(title="EvalRun console", lifespan=lifespan)


def org_of(x_org: str | None = Header(default=None, alias="X-Org")) -> str:
    if not x_org:
        raise HTTPException(400, "X-Org header is required")
    return x_org


def owned_run(request: Request, run_id: str, org: str) -> dict[str, Any]:
    row = projection.get_run(request.app.state.db, run_id)
    if row is None or row["org"] != org:
        raise HTTPException(404, f"run {run_id!r} not found")   # another org's run is indistinguishable from none
    return row


async def _start(request: Request, inp: EvalRunInput, parent_run_id: str | None) -> dict[str, Any]:
    client: Client = request.app.state.client
    try:
        handle = await client.start_workflow(
            EvalRun.run, inp, id=workflow_id(inp.run_id), task_queue=CONSOLE_QUEUE,
            search_attributes=start_search_attributes(inp, parent_run_id, request.app.state.parent_sa_registered),
            memo={"parent_run_id": parent_run_id or ""})
    except Exception as err:  # WorkflowAlreadyStartedError is the one you will see
        if type(err).__name__ == "WorkflowAlreadyStartedError":
            raise HTTPException(409, f"run {inp.run_id!r} already exists")
        raise
    return {"run_id": inp.run_id, "workflow_id": handle.id, "run_id_temporal": handle.result_run_id,
            "status": "queued", "total": len(inp.case_ids), "parent_run_id": parent_run_id}


@app.post("/runs", status_code=202)
async def create_run(body: StartRun, request: Request, org: str = Depends(org_of)):
    run_id = workflow_id(body.run_id or f"run-{uuid.uuid4().hex[:8]}")
    case_ids = body.case_ids or [f"case-{i:03d}" for i in range(1, body.cases + 1)]
    inp = EvalRunInput(run_id=run_id, model=body.model, suite=body.suite, case_ids=case_ids, org=org,
                       fail_fraction=body.fail_fraction, reviewers=body.reviewers)
    return JSONResponse(await _start(request, inp, None), status_code=202)


@app.get("/runs")
def list_runs(request: Request, org: str = Depends(org_of), status: str | None = None, model: str | None = None,
              cursor: str | None = None, limit: int = Query(20, ge=1, le=200)):
    if status and status not in ALL_STATUSES:
        raise HTTPException(400, f"status must be one of {ALL_STATUSES}")
    try:
        page, next_cursor = projection.list_runs(request.app.state.db, org=org, status=status, model=model, cursor=cursor, limit=limit)
    except ValueError as err:
        raise HTTPException(400, str(err))
    return {"runs": page, "next_cursor": next_cursor}


@app.get("/runs/{run_id}")
def get_run(run_id: str, request: Request, org: str = Depends(org_of)):
    row = owned_run(request, run_id, org)
    events = projection.last_events(request.app.state.db, run_id, 10)
    return {**row, "state": projection.latest_state(request.app.state.db, run_id),
            "last_events": [e.as_dict() for e in events], "last_seq": events[-1].seq if events else 0}


@app.get("/runs/{run_id}/events")
async def run_events(run_id: str, request: Request, org: str = Depends(org_of), after: int = Query(0, ge=0),
                     last_event_id: str | None = Header(default=None, alias="Last-Event-ID")):
    """SSE. The cursor is `?after=<seq>` or the browser's automatic `Last-Event-ID`; whichever is larger wins.
    Frames carry `id: <seq>` so nothing with seq <= cursor is ever sent, across any number of reconnects."""
    owned_run(request, run_id, org)
    cursor = max(after, int(last_event_id) if last_event_id and last_event_id.isdigit() else 0)
    conn = projection.connect(DB_PATH)   # this stream's own connection; the app one is shared by other requests

    async def gen():
        try:
            yield f": resuming after seq {cursor}\n\n"
            pos = cursor
            while not await request.is_disconnected():
                frames, pos, terminal = projection.poll_frames(conn, run_id, pos, TERMINAL_STATUSES)
                for frame in frames:
                    yield frame
                if terminal:
                    return
                if not frames:
                    await asyncio.sleep(SSE_POLL_SECONDS)
        finally:
            conn.close()

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/runs/{run_id}/cancel", status_code=202)
async def cancel_run(run_id: str, request: Request, org: str = Depends(org_of)):
    """handle.cancel() is a request: the Workflow decides when it is cancelled (cases stop within one heartbeat).
    The console does not wait for that — it writes `cancel_requested` now, so the UI shows `cancelling` at once."""
    row = owned_run(request, run_id, org)
    if row["status"] in TERMINAL_STATUSES:
        raise HTTPException(409, f"run is already {row['status']}")
    client: Client = request.app.state.client
    try:
        await client.get_workflow_handle(workflow_id(run_id)).cancel()
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(404, f"run {run_id!r} not found")
        raise
    ev = projection.console_event(request.app.state.db, run_id, "cancel_requested", {"by": org}, status="cancelling")
    return {"run_id": run_id, "status": "cancelling", "event": ev.as_dict()}


@app.post("/runs/{run_id}/retry-failed", status_code=202)
async def retry_failed(run_id: str, request: Request, org: str = Depends(org_of)):
    db = request.app.state.db
    row = owned_run(request, run_id, org)
    if row["status"] not in TERMINAL_STATUSES | {"awaiting_review"}:
        raise HTTPException(409, f"run is {row['status']}; retry once it has stopped")
    state = projection.latest_state(db, run_id)
    n = projection.count_retries(db, run_id) + 1
    try:
        inp = retry_failed_input(row, state, n, fail_fraction=DEFAULT_FAIL_FRACTION)
    except ValueError as err:
        raise HTTPException(409, str(err))
    started = await _start(request, inp, parent_run_id=run_id)
    projection.console_event(db, run_id, "retry_requested", {"retry_run_id": inp.run_id, "case_ids": inp.case_ids})
    return {**started, "case_ids": inp.case_ids}


@app.post("/runs/{run_id}/approve")
async def approve_run(run_id: str, body: Approve, request: Request, org: str = Depends(org_of)):
    row = owned_run(request, run_id, org)
    if row["status"] in TERMINAL_STATUSES:       # a replayed click after publish: the run is closed, not missing
        raise HTTPException(409, f"run is already {row['status']}")
    client: Client = request.app.state.client
    try:
        result = await client.get_workflow_handle(workflow_id(run_id)).execute_update(
            EvalRun.approve, args=[body.reviewer, body.command_id])
    except WorkflowUpdateFailedError as err:
        cause = err.cause
        msg = cause.message if isinstance(cause, ApplicationError) else str(cause)
        raise HTTPException(422, f"approve rejected: {msg}")
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:   # the row exists, so the Workflow closed under us
            raise HTTPException(409, f"run {run_id!r} is closed")
        raise
    return {"run_id": run_id, "result": result}


@app.get("/healthz")
def healthz():
    return {"ok": True}
