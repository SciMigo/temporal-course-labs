"""Which AgentRun to run: `AGENTRUN_CODE=v1|v2|v2_patched|v2_deprecated` (default v1)."""
from __future__ import annotations

import importlib
import os

CODE_MODULES = {
    "v1": "workflows",
    "v2": "workflows_v2",
    "v2_patched": "workflows_v2_patched",
    "v2_deprecated": "workflows_v2_deprecated",
}


def load(code: str | None = None):
    """Return (module, workflow class, activities) for a code version name."""
    code = code or os.environ.get("AGENTRUN_CODE", "v1")
    module = importlib.import_module(CODE_MODULES[code])
    activities = [module.call_llm, module.execute_tool]
    if hasattr(module, "evaluate"):
        activities.append(module.evaluate)          # v1 Workers do not know `evaluate`
    return module, module.AgentRun, activities
