"""EvalRun — a product sitting on a Workflow. Not AgentRun; the same skills.

    run_case             Activity on the gpu-eval lane, keyed run_id:case_id, heartbeating, RetryPolicy
    record_run_event     projection Activity on the console lane, after EVERY state change (seq = 2·n)
    Org/RunStatus/Model  Search Attributes, upserted where the status changes
    parent_run_id        memo, set by the console on a retry run
    approve              validated Update: reviewer allow-list, command_id dedupe, state check
    handle.cancel()      Temporal cancellation: cases stop within one heartbeat, then `cancelled`

State machine:  queued → running → awaiting_review → published
                                 ↘ failed                (every case failed: nothing to publish)
                any ── cancel ──→ cancelling → cancelled  (the console shows `cancelling` the moment it asks)
"""
from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy, SearchAttributeKey
from temporalio.exceptions import ActivityError, ApplicationError, is_cancelled_exception
from temporalio.workflow import ActivityCancellationType

with workflow.unsafe.imports_passed_through():
    from activities import record_run_event, run_case
    from config import CASE_PARALLELISM, GPU_EVAL_QUEUE
    from models import CaseResult, EvalRunInput, EvalRunResult

ORG = SearchAttributeKey.for_keyword("Org")
RUN_STATUS = SearchAttributeKey.for_keyword("RunStatus")
MODEL = SearchAttributeKey.for_keyword("Model")
PARENT_RUN_ID = SearchAttributeKey.for_keyword("EvalParentRunId")   # set by the console on a retry, if registered. `ParentRunId` itself is reserved by the system (Child Workflow lineage)

CASE_RETRY = RetryPolicy(initial_interval=timedelta(milliseconds=200), backoff_coefficient=2.0,
                         maximum_interval=timedelta(seconds=2), maximum_attempts=4,
                         non_retryable_error_types=["CaseFailed"])
PROJECTION_RETRY = RetryPolicy(initial_interval=timedelta(milliseconds=100), maximum_interval=timedelta(seconds=2))


@workflow.defn
class EvalRun:
    def __init__(self) -> None:
        self.inp: EvalRunInput | None = None
        self.status = "queued"
        self.done = 0
        self.failed_case_ids: list[str] = []
        self.results: dict[str, dict[str, Any]] = {}
        self.seq = 0                                   # Workflow event counter; projected as 2·seq
        self.processed_command_ids: set[str] = set()
        self.approved_by: str | None = None
        self.parent_run_id = ""

    # ------------------------------------------------------------------ state
    @property
    def total(self) -> int:
        return len(self.inp.case_ids) if self.inp else 0

    def state(self) -> dict[str, Any]:
        return {"run_id": self.inp.run_id, "org": self.inp.org, "model": self.inp.model, "suite": self.inp.suite,
                "status": self.status, "done": self.done, "total": self.total,
                "failed_case_ids": sorted(self.failed_case_ids), "parent_run_id": self.parent_run_id,
                "approved_by": self.approved_by, "created_at": self._created_at}

    async def _emit(self, kind: str, **payload: Any) -> None:
        """Every state change ends here: seq++, then the projection Activity. Awaited, so the event is
        durable in SQLite before the Workflow moves on; retried until it is."""
        self.seq += 1
        await workflow.execute_activity(
            record_run_event, args=[self.inp.run_id, 2 * self.seq, kind, {**payload, "state": self.state()}],
            start_to_close_timeout=timedelta(seconds=10), retry_policy=PROJECTION_RETRY)

    async def _transition(self, status: str, **payload: Any) -> None:
        self.status = status
        workflow.upsert_search_attributes([RUN_STATUS.value_set(status)])
        await self._emit("status_changed", **payload)

    # ------------------------------------------------------------------ control surface
    @workflow.query
    def progress(self) -> dict[str, Any]:
        return {**self.state(), "seq": self.seq, "processed_command_ids": sorted(self.processed_command_ids)}

    @workflow.update
    async def approve(self, reviewer: str, command_id: str) -> str:
        """The tracked write: accepted → in history → the run is published before the client sees the result."""
        self.processed_command_ids.add(command_id)
        self.approved_by = reviewer
        await self._transition("published", reviewer=reviewer, command_id=command_id)
        return f"published by {reviewer} (command {command_id})"

    @approve.validator
    def _validate_approve(self, reviewer: str, command_id: str) -> None:
        # Rejected here = never in history. The validator may read state; it must not change it.
        if not command_id:
            raise ValueError("command_id is required")
        if reviewer not in self.inp.reviewers:
            raise ValueError(f"{reviewer!r} is not a reviewer for org {self.inp.org!r}")
        if command_id in self.processed_command_ids:
            raise ValueError(f"command {command_id!r} already applied")
        if self.status != "awaiting_review":
            raise ValueError(f"run is {self.status}, not awaiting_review")

    # ------------------------------------------------------------------ the cases
    async def _case(self, case_id: str, gate: asyncio.Semaphore) -> None:
        async with gate:
            try:
                result: CaseResult = await workflow.execute_activity(
                    run_case, args=[self.inp.run_id, case_id, self.inp.fail_fraction], task_queue=GPU_EVAL_QUEUE,
                    schedule_to_start_timeout=timedelta(minutes=2), start_to_close_timeout=timedelta(seconds=30),
                    heartbeat_timeout=timedelta(seconds=1), retry_policy=CASE_RETRY,
                    cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
                self.results[case_id] = {"ok": True, "score": result.score, "attempt": result.attempt}
            except ActivityError as err:
                if isinstance(err.cause, ApplicationError) and err.cause.type == "CaseFailed":
                    self.failed_case_ids.append(case_id)
                    self.results[case_id] = {"ok": False, "score": 0.0}
                else:
                    raise
        self.done += 1
        await self._emit("case_done", case_id=case_id, **self.results[case_id])

    # ------------------------------------------------------------------ the run
    @workflow.run
    async def run(self, inp: EvalRunInput) -> EvalRunResult:
        self.inp = inp
        self._created_at = workflow.now().isoformat(timespec="milliseconds")
        self.parent_run_id = workflow.memo_value("parent_run_id", "", type_hint=str)
        workflow.upsert_search_attributes([ORG.value_set(inp.org), MODEL.value_set(inp.model), RUN_STATUS.value_set("queued")])
        await self._emit("run_created", parent_run_id=self.parent_run_id)
        tasks: list[asyncio.Task] = []
        try:
            await self._transition("running")
            gate = asyncio.Semaphore(CASE_PARALLELISM)
            tasks = [asyncio.create_task(self._case(cid, gate)) for cid in inp.case_ids]
            await asyncio.gather(*tasks)
            if self.total and len(self.failed_case_ids) == self.total:
                await self._transition("failed", reason="every case failed")
                return self._result()
            await self._transition("awaiting_review")
            await workflow.wait_condition(lambda: self.status == "published")
            await workflow.wait_condition(workflow.all_handlers_finished)
            return self._result()
        except BaseException as err:
            if not is_cancelled_exception(err):
                raise
            # handle.cancel(): every in-flight run_case was asked to stop (WAIT_CANCELLATION_COMPLETED means each
            # task below resolves only once its Activity confirmed). Project `cancelling` now, `cancelled` after.
            await self._transition("cancelling")
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._transition("cancelled", stopped_after=self.done)
            raise

    def _result(self) -> EvalRunResult:
        return EvalRunResult(self.inp.run_id, self.status, self.done, self.total, sorted(self.failed_case_ids), self.approved_by)
