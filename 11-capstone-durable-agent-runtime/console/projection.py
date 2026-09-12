"""The read model. SQLite owns what the UI reads; Temporal owns what is true.

    runs(run_id, org, model, suite, status, done, total, created_at, parent_run_id)
    run_events(run_id, seq, kind, payload, ts, PRIMARY KEY (run_id, seq))

`apply_event` is the one write path: it is *idempotent by (run_id, seq)* — the projection Activity
that calls it may run twice (a retry after a lost completion), and the second execution must change
nothing. The `runs` row is rebuilt from the event's `state` payload, but only when that event is the
newest one for the run, so an out-of-order duplicate can never move a run backwards.

Sequence numbers: the Workflow owns the EVEN numbers (2, 4, 6, …: `2 * self.seq`). The console
itself writes ODD numbers (`cancel_requested`, `retry_requested`) in the gap after the newest event,
so a console-originated event can never collide with — or be overwritten by — a Workflow event.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    org           TEXT NOT NULL,
    model         TEXT NOT NULL,
    suite         TEXT NOT NULL,
    status        TEXT NOT NULL,
    done          INTEGER NOT NULL DEFAULT 0,
    total         INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    parent_run_id TEXT
);
CREATE INDEX IF NOT EXISTS runs_org_created ON runs(org, created_at DESC, run_id DESC);
CREATE TABLE IF NOT EXISTS run_events (
    run_id  TEXT NOT NULL,
    seq     INTEGER NOT NULL,
    kind    TEXT NOT NULL,
    payload TEXT NOT NULL,
    ts      TEXT NOT NULL,
    PRIMARY KEY (run_id, seq)
);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA)
    return conn


@dataclass
class Event:
    run_id: str
    seq: int
    kind: str
    payload: dict[str, Any]
    ts: str

    def as_dict(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "seq": self.seq, "kind": self.kind, "payload": self.payload, "ts": self.ts}


def _row_event(r: sqlite3.Row) -> Event:
    return Event(r["run_id"], r["seq"], r["kind"], json.loads(r["payload"]), r["ts"])


# ------------------------------------------------------------------ writes
def upsert_event(conn: sqlite3.Connection, run_id: str, seq: int, kind: str, payload: dict[str, Any], ts: str) -> bool:
    """INSERT the event; a second call with the same (run_id, seq) is a no-op. Returns True if inserted."""
    cur = conn.execute("INSERT OR IGNORE INTO run_events(run_id, seq, kind, payload, ts) VALUES (?,?,?,?,?)",
                       (run_id, seq, kind, json.dumps(payload, sort_keys=True), ts))
    return cur.rowcount == 1


def upsert_run(conn: sqlite3.Connection, *, run_id: str, org: str, model: str, suite: str, status: str,
               done: int, total: int, created_at: str, parent_run_id: str | None) -> None:
    conn.execute("""INSERT INTO runs(run_id, org, model, suite, status, done, total, created_at, parent_run_id)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(run_id) DO UPDATE SET
                      status=excluded.status, done=excluded.done, total=excluded.total""",
                 (run_id, org, model, suite, status, done, total, created_at, parent_run_id or None))


def apply_event(conn: sqlite3.Connection, run_id: str, seq: int, kind: str, payload: dict[str, Any],
                ts: str | None = None) -> bool:
    """The projection: record the event, then rebuild the run row from its `state` iff it is the newest.
    Idempotent: replaying (run_id, seq) inserts nothing and moves nothing. Returns True if the event was new."""
    ts = ts or now_iso()
    with conn:  # one transaction
        inserted = upsert_event(conn, run_id, seq, kind, payload, ts)
        newest = conn.execute("SELECT MAX(seq) FROM run_events WHERE run_id=?", (run_id,)).fetchone()[0]
        state = payload.get("state")
        if inserted and state and seq == newest:
            row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            upsert_run(conn,
                       run_id=run_id,
                       org=state.get("org", row["org"] if row else ""),
                       model=state.get("model", row["model"] if row else ""),
                       suite=state.get("suite", row["suite"] if row else ""),
                       status=state["status"], done=int(state.get("done", 0)), total=int(state.get("total", 0)),
                       created_at=row["created_at"] if row else state.get("created_at", ts),
                       parent_run_id=row["parent_run_id"] if row else state.get("parent_run_id"))
    return inserted


def console_event(conn: sqlite3.Connection, run_id: str, kind: str, payload: dict[str, Any],
                  status: str | None = None) -> Event:
    """A console-originated event (`cancel_requested`, `retry_requested`): takes the next ODD seq after
    the newest event, so it sorts after what the Workflow has said so far and can never collide with what
    it will say next (Workflow seqs are even). Optionally moves the run row's status (e.g. `cancelling`)."""
    ts = now_iso()
    with conn:
        newest = conn.execute("SELECT COALESCE(MAX(seq), 0) FROM run_events WHERE run_id=?", (run_id,)).fetchone()[0]
        seq = newest + 1 if newest % 2 == 0 else newest + 2
        payload = dict(payload, source="console")
        if status:
            if get_run(conn, run_id) is None:
                raise KeyError(run_id)
            payload["state"] = {**_state_of(conn, run_id), "status": status}
            conn.execute("UPDATE runs SET status=? WHERE run_id=?", (status, run_id))
        upsert_event(conn, run_id, seq, kind, payload, ts)
    return Event(run_id, seq, kind, payload, ts)


def _state_of(conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
    """The newest `state` payload the Workflow projected, or {}."""
    r = conn.execute("SELECT payload FROM run_events WHERE run_id=? AND payload LIKE '%\"state\"%' ORDER BY seq DESC LIMIT 1",
                     (run_id,)).fetchone()
    return json.loads(r["payload"]).get("state", {}) if r else {}


def latest_state(conn: sqlite3.Connection, run_id: str) -> dict[str, Any]:
    return _state_of(conn, run_id)


# ------------------------------------------------------------------ reads
def get_run(conn: sqlite3.Connection, run_id: str) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    return dict(r) if r else None


def encode_cursor(created_at: str, run_id: str) -> str:
    return f"{created_at}|{run_id}"


def decode_cursor(cursor: str) -> tuple[str, str]:
    created_at, _, run_id = cursor.partition("|")
    if not created_at or not run_id:
        raise ValueError("malformed cursor")
    return created_at, run_id


def list_runs(conn: sqlite3.Connection, *, org: str, status: str | None = None, model: str | None = None,
              cursor: str | None = None, limit: int = 20) -> tuple[list[dict[str, Any]], str | None]:
    """Newest first; keyset pagination on (created_at, run_id). Every query is scoped to one org."""
    limit = max(1, min(int(limit), 200))
    where, args = ["org=?"], [org]
    if status:
        where.append("status=?"); args.append(status)
    if model:
        where.append("model=?"); args.append(model)
    if cursor:
        c_created, c_id = decode_cursor(cursor)
        where.append("(created_at, run_id) < (?, ?)"); args += [c_created, c_id]
    rows = conn.execute(f"SELECT * FROM runs WHERE {' AND '.join(where)} ORDER BY created_at DESC, run_id DESC LIMIT ?",
                        (*args, limit + 1)).fetchall()
    page = [dict(r) for r in rows[:limit]]
    next_cursor = encode_cursor(page[-1]["created_at"], page[-1]["run_id"]) if len(rows) > limit else None
    return page, next_cursor


def events_after(conn: sqlite3.Connection, run_id: str, after: int, limit: int = 500) -> list[Event]:
    """Every event with seq > after, ascending. This is the whole SSE cursor contract: a client that
    reconnects with the last seq it saw gets nothing twice."""
    rows = conn.execute("SELECT * FROM run_events WHERE run_id=? AND seq>? ORDER BY seq ASC LIMIT ?",
                        (run_id, int(after), limit)).fetchall()
    return [_row_event(r) for r in rows]


def last_events(conn: sqlite3.Connection, run_id: str, n: int = 10) -> list[Event]:
    rows = conn.execute("SELECT * FROM run_events WHERE run_id=? ORDER BY seq DESC LIMIT ?", (run_id, n)).fetchall()
    return [_row_event(r) for r in reversed(rows)]


def count_retries(conn: sqlite3.Connection, run_id: str) -> int:
    return conn.execute("SELECT COUNT(*) FROM runs WHERE run_id LIKE ?", (f"{run_id}-retry-%",)).fetchone()[0]


# ------------------------------------------------------------------ SSE framing (pure; tested directly)
def sse_frame(event: Event) -> str:
    """One text/event-stream record. `id:` carries the seq so a browser EventSource resumes with Last-Event-ID."""
    return f"id: {event.seq}\nevent: {event.kind}\ndata: {json.dumps(event.as_dict(), sort_keys=True)}\n\n"


def poll_frames(conn: sqlite3.Connection, run_id: str, after: int, terminal_statuses=frozenset()) -> tuple[list[str], int, bool]:
    """One poll: frames for every event with seq > after, the new cursor, and whether a terminal state was
    reached (after which the stream ends with an `end` frame). Pure; this is what the SSE test drives."""
    frames, cursor, terminal = [], int(after), False
    for ev in events_after(conn, run_id, after):
        frames.append(sse_frame(ev))
        cursor = ev.seq
        if (ev.payload.get("state") or {}).get("status") in terminal_statuses:
            frames.append("event: end\ndata: {}\n\n")
            terminal = True
            break
    return frames, cursor, terminal


def stream_events(conn: sqlite3.Connection, run_id: str, after: int, *, sleep, poll_seconds: float = 0.5,
                  should_stop=lambda: False, terminal_statuses=frozenset()) -> Iterator[str]:
    """Poll SQLite every `poll_seconds`, yield frames for seq > cursor, advance the cursor; end after a terminal
    state. Synchronous, so a test can drive it with a fake `sleep`; `api.py` runs the same `poll_frames` loop
    with `asyncio.sleep`."""
    cursor = int(after)
    while not should_stop():
        frames, cursor, terminal = poll_frames(conn, run_id, cursor, terminal_statuses)
        yield from frames
        if terminal:
            return
        if not frames:
            sleep(poll_seconds)
