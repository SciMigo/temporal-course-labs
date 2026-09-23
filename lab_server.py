#!/usr/bin/env python3
"""Serve the labs as local pages that can run code on your own machine.

    python lab_server.py            # http://127.0.0.1:3000

Why a local page: agent-runtime (https://github.com/SciMigo/agent-runtime) trusts loopback
origins on any port, so this page can call the runtime on :9477 with no token and no pairing
prompt. Both sides are on loopback, so the browser's local-network rules never come into it and
nothing is exposed off your machine. Port 3000 is just the default; --port anything works.

Standard library only: no install step. The runtime and the Temporal dev server are both optional —
without them the pages are still the lab text, and every exercise can be run from a terminal.
"""
from __future__ import annotations

import html
import json
import re
import shlex
import socket
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUNTIME = "http://127.0.0.1:9477"
PORT = 3000
BULLET = re.compile(r"^\s*[-*] ")
NUMBER = re.compile(r"^\s*\d+\. ")

def labs() -> list[Path]:
    """The labs present in this checkout, found on every request.

    Not cached at import: a lab downloaded into this directory while the server
    runs should appear on the next page load, without a restart.
    """
    return sorted(p for p in ROOT.iterdir() if p.is_dir() and re.match(r"\d\d-", p.name) and (p / "lab.json").is_file())


def lab_spec(d: Path) -> dict:
    """Everything the local page needs to know about one lab, from its own folder.

    Each lab carries its action cards, extra environment and figure in lab.json,
    so a lab is self-contained: this server holds nothing specific to any lab,
    and a lab copied into the checkout brings everything its page needs.
    """
    return json.loads((d / "lab.json").read_text(encoding="utf-8"))


def lab_visual(d: Path) -> str:
    entry = lab_spec(d).get("figure")
    if not entry:
        return ""
    name = entry["svg"]
    if Path(name).name != name:
        raise ValueError(f"Invalid figure name: {name}")
    svg = (d / name).read_text(encoding="utf-8")
    svg = re.sub(r'\swidth="720"', "", svg, count=1)
    svg = re.sub(r'\sstyle="width:720px;max-width:none;height:auto"', "", svg, count=1)
    return ('<figure class="concept-figure" id="lab-visual">'
            '<span class="concept-hint">Swipe to explore diagram →</span>'
            '<div class="concept-scroll" tabindex="0" role="region" aria-label="Scrollable diagram">'
            + svg + '</div><figcaption>' + html.escape(entry["caption"]) + '</figcaption></figure>')


def base_env(d: Path) -> dict[str, str]:
    return dict(lab_spec(d).get("env") or {"TASK_QUEUE": f"lab-{d.name[:2]}"})


def runtime_is_local() -> bool:
    """True when the runtime URL points at this machine."""
    from urllib.parse import urlparse
    host = (urlparse(RUNTIME).hostname or "").lower()
    return host in ("localhost", "127.0.0.1", "::1")


def port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.25)
        return s.connect_ex(("127.0.0.1", port)) == 0


def runtime_up() -> bool:
    try:
        with urllib.request.urlopen(f"{RUNTIME}/health", timeout=1) as r:
            return r.status == 200
    except Exception:
        return False


# --------------------------------------------------------------- markdown (enough of it)
def md_to_html(md: str, lab: str) -> str:
    out, i, n = [], 0, 0
    lines = md.split("\n")
    while i < len(lines):
        line = lines[i]
        if line.lstrip().startswith("```"):
            lang = line.lstrip()[3:].strip()
            block, i = [], i + 1
            while i < len(lines) and not lines[i].lstrip().startswith("```"):
                block.append(lines[i]); i += 1
            i += 1
            import textwrap as _tw
            code = _tw.dedent("\n".join(block))
            n += 1
            out.append('<div class="code">'
                       f'<pre><code>{html.escape(code)}</code></pre>'
                       '<button type="button" class="copy">Copy</button></div>')
            continue
        if line.startswith("#"):
            lvl = len(line) - len(line.lstrip("#"))
            heading = line[lvl:].strip()
            out.append(f'<h{lvl} id="{heading_id(heading)}">{inline(heading)}</h{lvl}>')
        elif line.startswith("|"):
            rows = []
            while i < len(lines) and lines[i].startswith("|"):
                rows.append(lines[i]); i += 1
            out.append(table(rows)); continue
        elif BULLET.match(line) or NUMBER.match(line):
            tag = "ul" if BULLET.match(line) else "ol"
            pat = BULLET if tag == "ul" else NUMBER
            items = []
            while i < len(lines) and pat.match(lines[i]):
                text = [pat.sub("", lines[i], count=1).strip()]
                i += 1
                # a wrapped continuation line belongs to the item above it
                while (i < len(lines) and lines[i].strip() and not pat.match(lines[i])
                       and not BULLET.match(lines[i]) and not NUMBER.match(lines[i])
                       and not lines[i].lstrip().startswith(("```", "#", "|", ">"))):
                    text.append(lines[i].strip()); i += 1
                items.append(f"<li>{inline(' '.join(text))}</li>")
            out.append(f"<{tag}>" + "".join(items) + f"</{tag}>"); continue
        elif line.startswith(">"):
            out.append(f"<blockquote>{inline(line.lstrip('> '))}</blockquote>")
        elif line.strip():
            para = []
            while (i < len(lines) and lines[i].strip() and not lines[i].lstrip().startswith(("```", "#", "|", ">"))
                   and not BULLET.match(lines[i]) and not NUMBER.match(lines[i])):
                para.append(lines[i].strip()); i += 1
            out.append(f"<p>{inline(' '.join(para))}</p>"); continue
        i += 1
    return "\n".join(out)


def inline(t: str) -> str:
    t = html.escape(t)
    t = re.sub(r"`([^`]+)`", r"<code>\1</code>", t)
    t = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", t)
    t = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<em>\1</em>", t)
    return re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', t)


def heading_id(title: str) -> str:
    plain = re.sub(r"[`*_]", "", title.lower())
    return re.sub(r"[^a-z0-9]+", "-", plain).strip("-")


def readme_intro(text: str) -> tuple[str, str, str]:
    lines = text.splitlines()
    title = lines[0].lstrip("# ").strip()
    start = next((i for i, line in enumerate(lines) if line.startswith("Goal:")), 1)
    end = start
    while end < len(lines) and lines[end].strip():
        end += 1
    goal = " ".join(line.strip() for line in lines[start:end]).removeprefix("Goal: ")
    goal = goal[:1].upper() + goal[1:]
    rest = "\n".join(lines[end + 1:])
    return title, goal, rest


def table(rows: list[str]) -> str:
    cells = [[c.strip() for c in r.strip().strip("|").split("|")] for r in rows]
    cells = [c for c in cells if not all(set(x) <= set("-: ") for x in c)]
    if not cells:
        return ""
    head = "".join(f"<th>{inline(c)}</th>" for c in cells[0])
    body = "".join("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in r) + "</tr>" for r in cells[1:])
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


PAGE = (ROOT / "lab_page_template.html").read_text(encoding="utf-8")

def shell(title: str, body: str, nav: str = "", lab: str = "") -> bytes:
    rt, tp = runtime_up(), port_open(7233)
    banner = "" if runtime_is_local() else (
        '<div class="warn"><strong>Remote runtime.</strong> This page is sending your code to '
        f'<code>{html.escape(RUNTIME)}</code>, not to your own machine. Anyone who learns that URL '
        "and its token can run code on whatever host is behind it. Use a short-lived tunnel, and "
        "stop it when you finish the lab.</div>")
    values = {
        "TITLE": html.escape(title), "BODY": body, "NAV": nav, "BANNER": banner,
        "RUNTIME_JSON": json.dumps(RUNTIME), "LAB_JSON": json.dumps(lab),
        "ROOT_JSON": json.dumps(str(ROOT)),
        "RT_CLASS": "up" if rt else "down", "RT_TEXT": "ready" if rt else "not running",
        "TP_CLASS": "up" if tp else "down", "TP_TEXT": "ready" if tp else "not running",
    }
    page = PAGE
    for key, value in values.items():
        page = page.replace(f"%%{key}%%", value)
    return page.encode()


def index() -> bytes:
    cards = []
    present = labs()
    for p in present:
        title, goal, _ = readme_intro((p / "README.md").read_text(encoding="utf-8"))
        cards.append(f'<li><a class="index-card" href="/lab/{p.name}">'
                     f'<span class="eyebrow">LAB {p.name[:2]}</span><strong>{html.escape(title)}</strong>'
                     f'<p>{html.escape(goal)}</p></a></li>')
    body = f'''<section class="hero"><span class="eyebrow">DURABLE EXECUTION WITH TEMPORAL</span>
<h1>Learn by running the system</h1><p>Eleven labs build one durable agent. Each lab has browser
controls, the original exercise, and questions to check your understanding.</p></section>
<section class="lab-reading"><h2>Before you begin</h2><p>The lab server, Agent Runtime, and
Temporal should show ready above. If you followed Option A on the course page, open Lab 1 and
click <strong>Prepare this lab</strong>. Each later lab has its own process controls and Task Queue.</p>
<p>The <a href="https://scimigo.com/learn/temporal-durable-execution">course readings</a> explain
the concepts; these pages run the code locally.</p></section>
<h2>Choose a lab</h2><ol class="index-grid">{''.join(cards)}</ol>{more_labs(present)}'''
    return shell("Temporal labs", body)


def more_labs(present: list[Path]) -> str:
    """What to do about the labs this checkout does not have.

    The public starter carries the first labs; the rest come with the full
    course, downloaded one lab at a time from its lab page on scimigo.com.
    """
    if len(present) >= 11:
        return ""
    return ('<section class="lab-reading"><h2>The rest of the labs</h2><p>This checkout has '
            f'{len(present)} of the course\'s eleven labs. The others come with the full course: '
            'open a lab on <a href="https://scimigo.com/learn/temporal-durable-execution">the course '
            'site</a>, choose <strong>Download this lab</strong>, and unzip it into this directory. '
            'It appears here on the next page load.</p></section>')


def lab_page(name: str) -> bytes | None:
    present = labs()
    d = ROOT / name
    readme = d / "README.md"
    if d not in present or not readme.exists():
        return None
    files = sorted(p.name for p in d.glob("*.py"))
    lab_text = readme.read_text(encoding="utf-8")
    if name == "01-the-machine":
        # The README also serves the terminal path. The local page already has the buttons above,
        # so show the browser-specific first step instead of repeating terminal setup commands.
        start = lab_text.index("## 1.1 Bring it up")
        end = lab_text.index("## 1.2 Kill the Worker", start)
        lab_text = (lab_text[:start] + """## 1.1 Bring it up
Your setup services are already running. Click **Prepare this lab**, then **Run Worker** and
**Start Workflow** on the cards above. The starter keeps running in the background while you do the crash
experiment; its Output button shows progress and the eventual result.
You do not need to change directories or run Python commands in a terminal for Option A.

In the [Temporal Web UI](http://localhost:8233), find `agent-42`. Read the events:
`ActivityTaskScheduled  call_llm`, then `TimerStarted`. Write down every event id so far, the pending
timer, and the Worker identity on the Workflow Tasks.

""" + lab_text[end:])
    title, goal, exercise = readme_intro(lab_text)
    objective = ""
    match = re.search(r"(?m)^## Objective\n\n(\*\*Why:\*\* [^\n]+\n\n\*\*By the end:\*\* [^\n]+)\n\n", exercise)
    if match:
        objective = '<section class="objective-card" aria-labelledby="objective-heading"><h2 id="objective-heading">Objective</h2>' + md_to_html(match.group(1).strip(), name) + '</section>'
        exercise = exercise[:match.start()] + exercise[match.end():]
    before = ""
    match = re.search(r"(?m)^## Before you run\n\n(?:- [^\n]+\n)+\n?", exercise)
    if match:
        before = '<section class="before-card" aria-labelledby="before-heading"><h2 id="before-heading">Before you run</h2>' + md_to_html(match.group()[len('## Before you run'):].strip(), name) + '</section>'
        exercise = exercise[:match.start()] + exercise[match.end():]
    reading = md_to_html(exercise, name)
    reading += ('<h2>Files in this lab</h2><ul>'
             + "".join(f"<li><code>{html.escape(f)}</code></li>" for f in files) + "</ul>")
    toc = ''.join(f'<a href="#{heading_id(line[3:].strip())}">{html.escape(line[3:].strip())}</a>'
                  for line in exercise.splitlines() if line.startswith('## '))
    hero = (f'<section class="hero"><span class="eyebrow">LAB {name[:2]} / 11</span>'
            f'<h1>{html.escape(title)}</h1><p>{html.escape(goal)}</p></section>')
    body = (hero + objective + before + lab_visual(d) + console_panel(name, d) + '<div class="reading-layout" id="exercise">'
            f'<aside class="toc"><h2>On this page</h2>{toc}</aside>'
            f'<article class="lab-reading">{reading}</article></div>')
    idx = present.index(d)
    nav = ""
    if idx:
        nav += f'<a href="/lab/{present[idx-1].name}">← previous</a>'
    if idx + 1 < len(present):
        nav += f'<a href="/lab/{present[idx+1].name}">next →</a>'
    return shell(title, body, nav, name)


def console_panel(name: str, d: Path) -> str:
    '''Per-lab browser controls backed by the runtime's Python environment.'''
    lab_dir, labs_root = str(d), str(ROOT)
    prefix = f"import sys\nsys.path.insert(0, {labs_root!r})\nimport browser_runner as br\n"

    def code(call: str) -> str:
        return prefix + f"print(br.{call})"

    def button(label: str, snippet: str, cls: str = "") -> str:
        return (f'<button type="button" class="run {cls}" data-inline="{html.escape(snippet, quote=True)}">'
                f'{html.escape(label)}</button>')

    actions = lab_spec(d)["actions"]
    cards = []
    for item in actions:
        key = str(item["key"])
        title = str(item["title"])
        args = list(item["args"])
        env = dict(item.get("env") or {})
        command = "python " + " ".join(args)
        kind = "Worker" if item["kind"] == "worker" else "Command"
        launch_label = ("Run Worker" if name[:2] == "01" and key == "worker" else
                        "Start Workflow" if name[:2] == "01" and key == "starter" else "Run")
        launch = button(launch_label, code(f"start({key!r}, {title!r}, {args!r}, {env!r})"), "launch")
        output = button("Output", code(f"output({key!r})"), "view-output")
        method = "kill" if kind == "Worker" else "stop"
        stop = button("Crash Worker" if kind == "Worker" else "Stop", code(f"{method}({key!r})"), "danger")
        cards.append(f'''<article class="action-card" data-key="{html.escape(key)}">
<div class="action-meta">{kind}</div><h3>{html.escape(title)}</h3>
<p>{html.escape(str(item["detail"]))}</p><code>{html.escape(command)}</code>
<div class="action-buttons">{launch}{output}{stop}</div></article>''')

    setup = code(f"prepare({lab_dir!r}, {labs_root!r}, {base_env(d)!r})")
    status = code("status()")
    example_command = "python " + shlex.join(actions[0]["args"])
    return f'''<section class="lab-workspace" aria-labelledby="workspace-heading">
<div class="workspace-heading"><div><span class="eyebrow">BROWSER WORKSPACE</span>
<h2 id="workspace-heading">Run this lab</h2>
<p>Use the controls here if you chose Option A. Each command uses this lab's directory and Python
 environment. Commands launch in the background, so you can inspect history or crash a Worker while
 a run is active. <a href="#exercise">Read the exercise ↓</a></p></div></div>
<div class="setup-step"><span class="step-number">1</span><div><h3>Prepare</h3>
<p>Install this lab's packages and set its Task Queue. Temporal and the runtime should say ready above.</p>
{button("Prepare this lab", setup, "primary")}{button("Process status", status)}</div></div>
<div class="workspace-subhead"><span class="step-number">2</span><div><h3>Run and observe</h3>
<p>Choose a starting point below. Open Output to follow the process; Crash Worker sends SIGKILL.</p></div></div>
<div class="action-grid">{''.join(cards)}</div>
<div class="command-box"><h3>Run another command from the exercise</h3>
<p>Enter one Python command from this lab, such as <code>{html.escape(example_command)}</code>.
Use <code>python script.py</code> or <code>python -m pytest</code>; terminal setup commands like
<code>cd</code> and <code>export</code> do not run here. Each command gets its own output log.</p>
<div class="command-row"><input id="lab-command" aria-label="Python command" spellcheck="false"
placeholder="{html.escape(example_command, quote=True)}"><button type="button" class="run primary" id="command-run">Run Python command</button></div>
<div id="command-history" class="command-history"></div></div>
<div class="output-heading"><h3>Output</h3><span id="output-name">Select an action to see its output</span></div>
<pre class="out" id="out-panel" aria-live="polite">No action selected yet.</pre>
<details class="console-details"><summary>Advanced: Python console</summary>
<p>Run Python in this lab's runtime kernel. Use the cards or command field for scripts.</p>
<textarea id="console" rows="4" spellcheck="false"
placeholder="from common import connect, show_history&#10;await show_history(await connect(), 'agent-42')"></textarea>
<button type="button" class="run" id="console-run">Run Python</button>
<pre class="out" id="out-console" hidden></pre></details>
</section>'''


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path in ("/", "/en", "/en/"):
            return self.send(index())
        m = re.match(r"^/lab/([0-9]{2}-[a-z0-9-]+)/?$", self.path)
        if m:
            page = lab_page(m.group(1))
            return self.send(page) if page else self.send(b"no such lab", 404, "text/plain")
        self.send(b"not found", 404, "text/plain")

    def send(self, body: bytes, status: int = 200, ctype: str = "text/html; charset=utf-8") -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a) -> None:  # quiet
        pass


def main() -> None:
    global PORT, RUNTIME
    import argparse
    import sys

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=PORT,
                    help="port to serve on (default 3000; see the note about pairing below)")
    ap.add_argument("--runtime", default=RUNTIME, help=f"agent-runtime base URL (default {RUNTIME})")
    args = ap.parse_args()
    PORT, RUNTIME = args.port, args.runtime.rstrip("/")

    if sys.version_info < (3, 9):
        sys.exit(f"This needs Python 3.9+; you are on {sys.version.split()[0]}.")

    if port_open(PORT):
        sys.exit(
            f"Port {PORT} is already in use.\n"
            f"  Something else is on it (a dev server?). Run with --port 3001 instead —\n"
            f"  agent-runtime trusts any loopback origin, so the Run buttons work on any port."
        )

    rt, tp = runtime_up(), port_open(7233)
    print(f"labs      http://127.0.0.1:{PORT}")
    print(f"runtime   {RUNTIME}  ({'ready' if rt else 'not running'})")
    print(f"temporal  127.0.0.1:7233  ({'ready' if tp else 'not running'})")
    if not tp:
        print("\n  Start the Temporal dev server first:   docker compose up -d")
    if not rt:
        print("\n  The Run buttons need agent-runtime (it executes the code on this machine):")
        print("      curl -fsSL https://raw.githubusercontent.com/SciMigo/agent-runtime/main/scripts/install-macos.sh | bash")
        print("      agent-runtime serve --port 9477")
        print("  Without it the pages still show every lab, with copy buttons for the commands.")
    if not runtime_is_local():
        print("\n  WARNING: --runtime points off this machine (" + RUNTIME + ").")
        print("  Your code, and anything it can reach, goes to that host. Only do this with a")
        print("  tunnel you started yourself, with pairing on, and stop it when you are done.")
    print("\nCtrl-C to stop.")
    try:
        ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    main()
