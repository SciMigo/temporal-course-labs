"""Lane names for lab 10. Spell every Task Queue name once, here, and import it on both sides.

A name mismatch between Client and Worker is not an error in Temporal: it creates two different
Task Queues, the Worker receives nothing, and the Workflow does not progress.

The course names are the defaults. When several people share one dev server, set LAB_PREFIX so your
queues and Workflow IDs do not collide with theirs:  LAB_PREFIX=lab10- python worker_cpu.py
"""
from __future__ import annotations

import os

LAB_PREFIX = os.environ.get("LAB_PREFIX", "")

AGENT_WORKFLOWS = os.environ.get("AGENT_WORKFLOWS_QUEUE", LAB_PREFIX + "agent-workflows")
CPU_TOOLS = os.environ.get("CPU_TOOLS_QUEUE", LAB_PREFIX + "cpu-tools")
GPU_TOOLS = os.environ.get("GPU_TOOLS_QUEUE", LAB_PREFIX + "gpu-tools")

# Which lane each tool needs. Read in Workflow code, so the lane a step went to is replayed like
# any other decision (moving a tool between lanes mid-run is a module 9 change).
CPU_TOOL_NAMES = frozenset({"search", "parse", "fetch"})
GPU_TOOL_NAMES = frozenset({"embed", "vision", "rerank"})
LANE_FOR_TOOL = {**{t: CPU_TOOLS for t in CPU_TOOL_NAMES}, **{t: GPU_TOOLS for t in GPU_TOOL_NAMES}}


def workflow_id(base: str) -> str:
    """`agent-42` in the course; `lab10-agent-42` on a shared server."""
    return LAB_PREFIX + base
