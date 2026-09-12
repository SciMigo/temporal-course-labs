"""Dataclasses crossing the Workflow boundary (Temporal's default JSON converter handles them)."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class EvalRunInput:
    run_id: str
    model: str
    suite: str
    case_ids: list[str]
    org: str
    fail_fraction: float = 0.25
    reviewers: list[str] = field(default_factory=lambda: ["alice", "bob"])


@dataclass
class CaseResult:
    run_id: str
    case_id: str
    ok: bool
    score: float
    attempt: int
    duplicate: bool = False


@dataclass
class EvalRunResult:
    run_id: str
    status: str
    done: int
    total: int
    failed_case_ids: list[str]
    approved_by: str | None = None
