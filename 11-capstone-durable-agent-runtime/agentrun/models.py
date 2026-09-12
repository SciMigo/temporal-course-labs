"""The data that crosses the Workflow/Activity boundary. Plain dataclasses: the SDK serialises them
as JSON, so every field is something a history can hold."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Step:
    kind: str                      # "llm" | "tool" | "research" | "finish"
    prompt: str = ""
    tool: str = ""
    args: dict = field(default_factory=dict)
    question: str = ""


@dataclass
class ToolCall:
    key: str                       # idempotency key: wf_id:step:hash(tool, args)
    tool: str
    args: dict


@dataclass
class ToolResult:
    key: str
    tool: str
    output: str
    attempt: int                   # which attempt produced it (>1 means a retry happened)
    task_queue: str                # the lane that answered
    duplicate: bool = False        # True when served from the ledger: a retry after the effect happened


@dataclass
class Verdict:
    ok: bool
    score: float
    note: str


@dataclass
class AgentState:
    """The snapshot continue_as_new carries into the next run. Only what plan() needs to go on;
    never the history."""
    step: int
    goal: str
    context_summary: str
    attempts: int
    processed_command_ids: list[str]
