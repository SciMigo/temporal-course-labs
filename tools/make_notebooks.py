#!/usr/bin/env python3
"""Generate a Jupyter notebook from a lab's README, with jupytext.

    python3 tools/make_notebooks.py            # regenerate all of them

The README stays the source of truth; notebooks are a build artifact, so the two cannot drift.
Only the labs that suit cells get one: 02 (read a history), 08 (testing), 09 (deploys), 10
(operations). The others run Workers across several terminals and killing them is the lesson —
a single kernel is the wrong shape for that, so they stay terminal-first.

Conversion rules:
  markdown prose      -> markdown cell
  ```python           -> code cell
  ```bash             -> code cell of `!` lines, unless it starts something long-lived
                         (a Worker, a tunnel, a server), which becomes a markdown note instead
  ```text             -> markdown cell (these are expected output, not input)
"""
from __future__ import annotations

import re
import sys
import hashlib
from difflib import SequenceMatcher
from pathlib import Path

import jupytext
import nbformat

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_LABS = ["02-event-history-and-replay", "08-testing-and-replay-tests",
                 "09-deploying-changed-workflow-code", "10-operating-temporal-in-production"]
# commands that do not return: they belong in a terminal, not a cell
BLOCKING = ("worker.py", "lab_server.py", "agent-runtime", "cloudflared", "tail -f",
            "temporal server", "docker compose up" )


# Commands a lab states inline in its prose rather than in a fenced block. Without these, a
# generated notebook for that lab would be all text and no cells.
EXTRA_CELLS: dict[str, list[tuple[str, str]]] = {
    "02-event-history-and-replay": [
        ("The history, annotated (2.1). Start a Worker in a terminal first, then run the starter.",
         "!python starter.py"),
        ("The same events with the attributes the lab asks you to read.",
         "!python history.py agent-42"),
        ("2.1 step 4 — replay it in-process and watch the Activity *not* run.",
         "!python replay.py agent-42"),
        ("2.4 — cut the captured history after event 13 and find the first live Command.",
         "!python history.py --file histories/agent-42.json --first 13\n"
         "!python replay.py --file histories/agent-42.json --first 13"),
        ("`--first 11` prints nothing live: the code is blocked on a timer this history never fired.",
         "!python replay.py --file histories/agent-42.json --first 11"),
        ("Check yourself.", "!PYTHONPATH=.. python -m pytest tests/ -q"),
    ],
}


def preamble(lab: str) -> tuple[str, str]:
    """The first two cells: what this is, and the working directory / queue every cell needs."""
    md = (f"# {lab}\n\n"
          "Generated from this lab's `README.md` — edit that, not this notebook "
          "(`python3 tools/make_notebooks.py` regenerates it).\n\n"
          "Before you start: `docker compose up -d` in the repo root, and read the lab page on "
          "[the course site](https://scimigo.com/learn/temporal-durable-execution). Cells that would "
          "start a Worker are left as notes — run those in a terminal, because killing them is the "
          "point of the exercise.")
    # No absolute paths: Jupyter starts the kernel in the notebook's own directory, and if you
    # opened it from the repo root instead, step into the lab.
    code = ("import os, sys\n"
            "from pathlib import Path\n"
            f"LAB = {lab!r}\n"
            "if Path.cwd().name != LAB and (Path.cwd() / LAB).is_dir():\n"
            "    os.chdir(Path.cwd() / LAB)\n"
            "sys.path[:0] = [os.getcwd(), str(Path.cwd().parent)]\n"
            f"os.environ['TASK_QUEUE'] = 'lab-{lab[:2]}'\n"
            "print('cwd', Path.cwd().name, '| TASK_QUEUE', os.environ['TASK_QUEUE'])")
    return md, code


def to_cells(md_text: str, lab: str) -> list:
    cells = []
    intro_md, intro_code = preamble(lab)
    cells.append(nbformat.v4.new_markdown_cell(intro_md))
    cells.append(nbformat.v4.new_code_cell(intro_code))

    prose: list[str] = []

    def flush() -> None:
        text = "\n".join(prose).strip()
        prose.clear()
        if text:
            cells.append(nbformat.v4.new_markdown_cell(text))

    lines, i = md_text.split("\n"), 0
    while i < len(lines):
        line = lines[i]
        if line.lstrip().startswith("```"):
            lang = line.lstrip()[3:].strip()
            block, i = [], i + 1
            while i < len(lines) and not lines[i].lstrip().startswith("```"):
                block.append(lines[i]); i += 1
            i += 1
            code = "\n".join(block).rstrip()
            if lang == "python":
                flush(); cells.append(nbformat.v4.new_code_cell(code))
            elif lang == "bash":
                if any(b in code for b in BLOCKING):
                    prose.append(f"Run this in a terminal:\n\n```bash\n{code}\n```")
                else:
                    flush()
                    shell = "\n".join(
                        ln if not ln.strip() or ln.lstrip().startswith("#") else "!" + ln
                        for ln in code.split("\n"))
                    cells.append(nbformat.v4.new_code_cell(shell))
            else:
                prose.append(f"```{lang}\n{code}\n```")
            continue
        if line.startswith("#") and prose:
            flush()
        prose.append(line); i += 1
    flush()

    extra = EXTRA_CELLS.get(lab, [])
    if extra:
        cells.append(nbformat.v4.new_markdown_cell(
            "## Run the commands\n\nThe lab states these in its text; here they are as cells."))
        for note, code in extra:
            cells.append(nbformat.v4.new_markdown_cell(note))
            cells.append(nbformat.v4.new_code_cell(code))
    return cells


def build(lab: str) -> Path:
    readme = ROOT / lab / "README.md"
    nb = nbformat.v4.new_notebook(cells=to_cells(readme.read_text(encoding="utf-8"), lab))
    nb.metadata["jupytext"] = {"main_language": "python"}
    nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
    out = ROOT / lab / f"lab-{lab[:2]}.ipynb"
    # Keep notebook cell identities stable across README-only edits. Jupyter uses IDs to track
    # cells; replacing every ID on each build makes a small prose change look like a full rewrite.
    old_cells = nbformat.read(out, as_version=4).cells if out.exists() else []
    signature = lambda cell: (cell.cell_type, cell.source)
    matcher = SequenceMatcher(None, list(map(signature, old_cells)),
                              list(map(signature, nb.cells)), autojunk=False)
    used_ids = set()
    for old_start, new_start, length in matcher.get_matching_blocks():
        for old_cell, new_cell in zip(old_cells[old_start:old_start + length],
                                      nb.cells[new_start:new_start + length]):
            if old_cell.id not in used_ids:
                new_cell.id = old_cell.id
                used_ids.add(new_cell.id)
    for cell in nb.cells:
        if cell.id in used_ids:
            continue
        seed = f"{lab}\0{cell.cell_type}\0{cell.source}".encode("utf-8")
        digest = hashlib.sha256(seed).hexdigest()
        cell.id = digest[:8]
        suffix = 1
        while cell.id in used_ids:
            cell.id = hashlib.sha256(seed + str(suffix).encode()).hexdigest()[:8]
            suffix += 1
        used_ids.add(cell.id)
    jupytext.write(nb, out, fmt="ipynb")
    return out


if __name__ == "__main__":
    labs = sys.argv[1:] or NOTEBOOK_LABS
    for lab in labs:
        path = build(lab)
        nb = nbformat.read(path, as_version=4)
        code = sum(1 for c in nb.cells if c.cell_type == "code")
        print(f"{path.relative_to(ROOT)}  {len(nb.cells)} cells ({code} code)")
