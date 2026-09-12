"""Task Queue lanes (module 10). Spelled once; imported by the Workflow and by every Worker.
Course names are the defaults; LAB_PREFIX=lab11- keeps a shared dev server tidy."""
from __future__ import annotations

import os

LAB_PREFIX = os.environ.get("LAB_PREFIX", "")
AGENT_WORKFLOWS = os.environ.get("AGENT_WORKFLOWS_QUEUE", LAB_PREFIX + "agent-workflows")
CPU_TOOLS = os.environ.get("CPU_TOOLS_QUEUE", LAB_PREFIX + "cpu-tools")
GPU_TOOLS = os.environ.get("GPU_TOOLS_QUEUE", LAB_PREFIX + "gpu-tools")
# `premium-models` and `browser-tools` are the same mechanism again; the lab keeps three lanes.

CPU_TOOL_NAMES = frozenset({"search", "parse", "fetch"})
GPU_TOOL_NAMES = frozenset({"embed", "vision", "rerank"})
LANE_FOR_TOOL = {**{t: CPU_TOOLS for t in CPU_TOOL_NAMES}, **{t: GPU_TOOLS for t in GPU_TOOL_NAMES}}


def workflow_id(base: str) -> str:
    return LAB_PREFIX + base
