"""Names and knobs for the evaluation console, spelled once. Every process (API, both Workers, the
setup script, the tests) imports these; nothing else reads the environment.

Shared dev server: LAB_PREFIX (default `lab115-`) prefixes both task queues and every Workflow ID."""
from __future__ import annotations

import os

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")

LAB_PREFIX = os.environ.get("LAB_PREFIX", "lab115-")
CONSOLE_QUEUE = os.environ.get("CONSOLE_QUEUE", LAB_PREFIX + "console")      # EvalRun + record_run_event
GPU_EVAL_QUEUE = os.environ.get("GPU_EVAL_QUEUE", LAB_PREFIX + "gpu-eval")   # run_case

DB_PATH = os.environ.get("EVAL_CONSOLE_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "eval_console.sqlite3"))
API_HOST = os.environ.get("EVAL_CONSOLE_HOST", "127.0.0.1")
API_PORT = int(os.environ.get("EVAL_CONSOLE_PORT", "8115"))

# The fake GPU: one case sleeps CASE_SECONDS in CASE_TICKS heartbeating ticks.
CASE_SECONDS = float(os.environ.get("EVAL_CASE_SECONDS", "0.8"))
CASE_TICKS = int(os.environ.get("EVAL_CASE_TICKS", "4"))
DEFAULT_FAIL_FRACTION = float(os.environ.get("EVAL_FAIL_FRACTION", "0.25"))     # benchmark failures (recorded, not retried)
HICCUP_FRACTION = float(os.environ.get("EVAL_HICCUP_FRACTION", "0.1"))           # transient GPU errors (retried by policy)
CASE_PARALLELISM = int(os.environ.get("EVAL_CASE_PARALLELISM", "4"))             # cases in flight per run
GPU_SLOTS = int(os.environ.get("GPU_SLOTS", "4"))                                # slots on the gpu-eval Worker
DEFAULT_REVIEWERS = tuple(x for x in os.environ.get("EVAL_REVIEWERS", "alice,bob").split(",") if x)

TERMINAL_STATUSES = frozenset({"published", "cancelled", "failed"})
ALL_STATUSES = ("queued", "running", "awaiting_review", "cancelling", "cancelled", "failed", "published")


def workflow_id(run_id: str) -> str:
    """Workflow ID == run_id; the prefix is applied once, never twice."""
    return run_id if run_id.startswith(LAB_PREFIX) else LAB_PREFIX + run_id
