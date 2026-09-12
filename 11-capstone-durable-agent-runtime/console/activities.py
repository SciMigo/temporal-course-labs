"""The two Activities. `run_case` is the fake GPU (gpu-eval lane); `record_run_event` is the projection
(console lane) — the only code that writes SQLite from inside a Workflow's life.

Both are idempotent by key: `run_case` by `run_id:case_id` (a ledger in the Worker process, the rows
Postgres would own in production), `record_run_event` by `(run_id, seq)` (the SQLite primary key).
"""
from __future__ import annotations

import asyncio
import random
from dataclasses import replace
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

import projection
from config import CASE_SECONDS, CASE_TICKS, DB_PATH, HICCUP_FRACTION
from models import CaseResult

CASE_LEDGER: dict[str, CaseResult] = {}      # key → the one time the case really ran
CASE_EFFECTS: dict[str, int] = {}            # key → how many times the GPU work really happened
CANCELLED_AT_TICK: dict[str, int] = {}       # key → tick at which a cancel was observed


def reset_ledgers() -> None:
    CASE_LEDGER.clear(); CASE_EFFECTS.clear(); CANCELLED_AT_TICK.clear()


@activity.defn
async def run_case(run_id: str, case_id: str, fail_fraction: float = 0.25) -> CaseResult:
    """Evaluate one benchmark case on the (fake) GPU.

    - sleeps CASE_SECONDS in CASE_TICKS heartbeating ticks; a cancel is observed at the next heartbeat
    - a *benchmark failure* (fraction `fail_fraction`, seeded by run_id:case_id so a run is reproducible and a
      retry run — new run_id — rolls again) is `ApplicationError(type="CaseFailed", non_retryable=True)`:
      the Workflow records it, the RetryPolicy does not fight it
    - a *transient GPU hiccup* (HICCUP_FRACTION, seeded per attempt) is a plain retryable error: the
      RetryPolicy retries it and the history shows the attempt count, never a failed case
    - the ledger makes a retry after a lost completion return the recorded result (`duplicate=True`)
    """
    info = activity.info()
    key = f"{run_id}:{case_id}"
    if key in CASE_LEDGER:
        activity.logger.info("run_case duplicate suppressed key=%s attempt=%d", key, info.attempt)
        return replace(CASE_LEDGER[key], attempt=info.attempt, duplicate=True)
    rng = random.Random(f"{key}:attempt{info.attempt}")
    if rng.random() < HICCUP_FRACTION:
        raise ApplicationError(f"GPU hiccup on {key} attempt {info.attempt}", type="GpuHiccup")
    start_tick = int(info.heartbeat_details[0].get("tick", 0)) if info.heartbeat_details else 0
    try:
        for tick in range(start_tick, CASE_TICKS):
            activity.heartbeat({"tick": tick + 1, "of": CASE_TICKS})
            await asyncio.sleep(CASE_SECONDS / CASE_TICKS)
    except asyncio.CancelledError:
        CANCELLED_AT_TICK[key] = tick + 1
        activity.logger.info("run_case cancelled key=%s at tick %d/%d", key, tick + 1, CASE_TICKS)
        raise
    CASE_EFFECTS[key] = CASE_EFFECTS.get(key, 0) + 1
    ok = random.Random(key).random() >= fail_fraction
    result = CaseResult(run_id, case_id, ok, round(random.Random(key + ":score").uniform(0.5, 1.0), 3) if ok else 0.0, info.attempt)
    CASE_LEDGER[key] = result
    if not ok:
        raise ApplicationError(f"case {case_id} failed the benchmark", type="CaseFailed", non_retryable=True)
    return result


@activity.defn
async def record_run_event(run_id: str, seq: int, kind: str, payload: dict[str, Any]) -> bool:
    """Project one Workflow state change into SQLite. Returns True if the event was new, False if this
    (run_id, seq) had already been recorded — a retried execution changes nothing."""
    conn = projection.connect(DB_PATH)
    try:
        return projection.apply_event(conn, run_id, int(seq), kind, payload)
    finally:
        conn.close()
