"""Export a Workflow Execution's Event History as JSON, the format `Replayer` reads.

    python export_history.py agent-42                      # -> tests/histories/agent-42.json
    python export_history.py agent-42 --run-id <run-id>    # one specific run of the chain
    python export_history.py agent-42 --out some/path.json

This is the Python form of

    temporal workflow show --workflow-id agent-42 --output json > tests/histories/agent-42.json

for a box without the Temporal CLI. Both produce the same `temporal.api.history.v1.History`
JSON (the Web UI's "Download" button does too), and `WorkflowHistory.from_json` reads any of them.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402

HISTORIES = Path(__file__).parent / "tests" / "histories"


async def export(workflow_id: str, run_id: str | None, out: Path) -> int:
    client = await connect()
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    history = await handle.fetch_history()          # WorkflowHistory: the events, decoded
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(history.to_json())                # proto JSON, same shape as the CLI's
    return len(history.events)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("workflow_id")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--out", default=None, help=f"default: {HISTORIES}/<workflow_id>.json")
    ns = ap.parse_args()
    out = Path(ns.out) if ns.out else HISTORIES / f"{ns.workflow_id}.json"
    n = asyncio.run(export(ns.workflow_id, ns.run_id, out))
    print(f"wrote {n} events to {out}")


if __name__ == "__main__":
    main()
