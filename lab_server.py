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

LABS = sorted(p for p in ROOT.iterdir() if p.is_dir() and re.match(r"\d\d-", p.name))


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
            runnable = lang == "python"
            btn = (f'<button class="run" data-cell="c{n}">Run in your runtime</button>'
                   if runnable else '<button class="copy">Copy</button>')
            out.append(f'<div class="code"{f' data-lab="{html.escape(lab)}"' if runnable else ""}>'
                       f'<pre><code>{html.escape(code)}</code></pre>{btn}'
                       f'<div class="out" id="out-c{n}" hidden></div></div>')
            continue
        if line.startswith("#"):
            lvl = len(line) - len(line.lstrip("#"))
            out.append(f"<h{lvl}>{inline(line[lvl:].strip())}</h{lvl}>")
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


def table(rows: list[str]) -> str:
    cells = [[c.strip() for c in r.strip().strip("|").split("|")] for r in rows]
    cells = [c for c in cells if not all(set(x) <= set("-: ") for x in c)]
    if not cells:
        return ""
    head = "".join(f"<th>{inline(c)}</th>" for c in cells[0])
    body = "".join("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in r) + "</tr>" for r in cells[1:])
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


PAGE = """<!doctype html><meta charset="utf-8"><title>{title}</title>
<style>
:root{{--bg:#fff;--fg:#1a1a2e;--muted:#5b5b70;--border:#e0e0e8;--code:#f4f4f8;--accent:#0f5f8f;--ok:#1a7f4b;--bad:#b3261e}}
@media(prefers-color-scheme:dark){{:root{{--bg:#14141f;--fg:#e0e0e8;--muted:#a0a0b8;--border:#2a2a4e;--code:#1e1e2e;--accent:#7ba4d9;--ok:#3ddc84;--bad:#ff6b6b}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--fg);font:16px/1.6 system-ui,sans-serif}}
.wrap{{max-width:52rem;margin:0 auto;padding:1.5rem}}
nav{{position:sticky;top:0;background:var(--bg);border-bottom:1px solid var(--border);padding:.6rem 0;margin-bottom:1rem;font-size:.9rem}}
nav a{{color:var(--accent);margin-right:.8rem;text-decoration:none}}
.status{{display:flex;gap:1rem;flex-wrap:wrap;font-size:.85rem;color:var(--muted);margin-top:.4rem}}
.dot{{display:inline-block;width:.6rem;height:.6rem;border-radius:50%;margin-right:.35rem}}
.up{{background:var(--ok)}}.down{{background:var(--bad)}}
h1{{font-size:1.7rem}}h2{{font-size:1.25rem;margin-top:2rem}}h3{{font-size:1.05rem}}
pre{{background:var(--code);padding:.8rem;border-radius:6px;overflow-x:auto;margin:0}}
code{{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.9em}}
p code,li code,td code{{background:var(--code);padding:.1rem .3rem;border-radius:4px}}
table{{border-collapse:collapse;width:100%;margin:1rem 0;font-size:.92rem}}
th,td{{border:1px solid var(--border);padding:.4rem .6rem;text-align:left;vertical-align:top}}
blockquote{{border-left:4px solid var(--accent);padding:.4rem 1rem;margin:1rem 0;color:var(--muted)}}
.code{{margin:1rem 0}}
button.run{{margin-top:.4rem;font:inherit;font-size:.85rem;padding:.25rem .7rem;border:1px solid var(--border);
  background:var(--bg);color:var(--accent);border-radius:5px;cursor:pointer}}
button.run:disabled{{opacity:.5;cursor:default}}
.out{{background:var(--code);border-left:3px solid var(--accent);padding:.6rem;margin-top:.4rem;
  white-space:pre-wrap;font-family:ui-monospace,Menlo,monospace;font-size:.85em}}
.out.err{{border-left-color:var(--bad)}}
.hint{{color:var(--muted);font-size:.85rem}}
.warn{{border:1px solid #b3261e;border-left-width:4px;border-radius:6px;padding:.5rem .8rem;margin:.5rem 0;
  font-size:.88rem;background:rgba(179,38,30,.08)}}
.panel{{border:1px solid var(--border);border-radius:8px;padding:.9rem 1rem;margin:1rem 0;background:var(--code)}}
.panel h2{{margin-top:0}}.btns{{display:flex;gap:.5rem;flex-wrap:wrap}}
button.primary{{border-color:var(--accent)}}
button.copy{{margin-top:.4rem;font:inherit;font-size:.8rem;padding:.2rem .6rem;border:1px solid var(--border);
  background:var(--bg);color:var(--muted);border-radius:5px;cursor:pointer}}
textarea{{width:100%;font-family:ui-monospace,Menlo,monospace;font-size:.85rem;padding:.5rem;
  background:var(--bg);color:var(--fg);border:1px solid var(--border);border-radius:6px}}
details{{margin-top:.8rem}}summary{{cursor:pointer;color:var(--accent);font-size:.9rem}}
</style>
<div class="wrap">
<nav><a href="/">All labs</a>{nav}
{banner}<div class="status">
  <span><span class="dot {rt_cls}"></span>runtime :9477 {rt_txt}</span>
  <span><span class="dot {tp_cls}"></span>Temporal :7233 {tp_txt}</span>
  <span><a href="http://localhost:8233" target="_blank">Web UI</a></span>
</div></nav>
{body}
</div>
<script>
const RT = "{runtime}";
async function post(path, payload) {{
  const r = await fetch(RT + path, {{method:"POST", headers:{{"Content-Type":"application/json"}},
    body: JSON.stringify(payload)}});
  return {{status: r.status, json: await r.json().catch(() => null)}};
}}
function render(el, res) {{
  el.hidden = false; el.classList.remove("err");
  if (res.status !== 200) {{ el.classList.add("err"); el.textContent = "runtime said " + res.status + ": " +
    JSON.stringify(res.json); return; }}
  const j = res.json, parts = [];
  for (const o of (j.outputs || [])) {{
    const c = o.content || o;
    if (c.text) parts.push(c.text);
    else if (c.data && c.data["text/plain"]) parts.push(c.data["text/plain"]);
    else if (c.ename) parts.push(c.ename + ": " + c.evalue);
  }}
  if (j.error) {{ el.classList.add("err"); parts.push(j.error.ename + ": " + j.error.evalue); }}
  el.textContent = parts.join("\\n") || "(no output)";
}}
const LAB = document.querySelector(".btns")?.dataset.lab;
async function runCode(out, code, lab) {{
  out.hidden = false; out.classList.remove("err"); out.textContent = "running…";
  try {{ render(out, await post("/cell/run", {{lab_id: lab || LAB, code}})); }}
  catch (err) {{ out.classList.add("err");
    out.textContent = "Could not reach the runtime at " + RT + ".\\nStart it with:  agent-runtime serve\\n" + err; }}
}}
document.addEventListener("click", async (e) => {{
  const copy = e.target.closest("button.copy");
  if (copy) {{ await navigator.clipboard.writeText(copy.closest(".code").querySelector("code").textContent);
    copy.textContent = "Copied"; setTimeout(() => copy.textContent = "Copy", 1200); return; }}
  const btn = e.target.closest("button.run"); if (!btn) return;
  btn.disabled = true;
  if (btn.id === "console-run") {{
    await runCode(document.getElementById("out-console"), document.getElementById("console").value);
  }} else if (btn.dataset.inline !== undefined) {{
    await runCode(document.getElementById("out-panel"), btn.dataset.inline);
  }} else {{
    const box = btn.closest(".code");
    await runCode(box.querySelector(".out"), box.querySelector("code").textContent, box.dataset.lab);
  }}
  btn.disabled = false;
}});
</script>
"""


def shell(title: str, body: str, nav: str = "") -> bytes:
    rt, tp = runtime_up(), port_open(7233)
    banner = "" if runtime_is_local() else (
        '<div class="warn"><strong>Remote runtime.</strong> This page is sending your code to '
        f'<code>{html.escape(RUNTIME)}</code>, not to your own machine. Anyone who learns that URL '
        "and its token can run code on whatever host is behind it. Use a short-lived tunnel, and "
        "stop it when you finish the lab.</div>")
    return PAGE.format(
        banner=banner,
        title=html.escape(title), body=body, nav=nav, runtime=RUNTIME,
        rt_cls="up" if rt else "down", rt_txt="ready" if rt else "not running",
        tp_cls="up" if tp else "down", tp_txt="ready" if tp else "not running",
    ).encode()


def index() -> bytes:
    rows = "".join(
        f'<li><a href="/lab/{p.name}">{html.escape(p.name)}</a></li>' for p in LABS)
    body = f"""<h1>Durable Execution with Temporal — labs</h1>
<p>Eleven labs, one program. The reading for each module is on
<a href="https://scimigo.com/learn/temporal-durable-execution">the course site</a>; this server is
the code, running on your machine.</p>
<p>If you followed Option A on the course page, your three terminals are already set up. Check that
runtime and Temporal say <strong>ready</strong> above, then open Lab 1. Its <strong>Prepare this lab</strong>
button installs the Python packages and sets the Task Queue for that lab.</p>
<p class="hint">If either service says <strong>not running</strong>, return to the course setup and
start it before using the lab buttons.</p>
<h2>Labs</h2><ol>{rows}</ol>"""
    return shell("Temporal labs", body)


def lab_page(name: str) -> bytes | None:
    d = ROOT / name
    readme = d / "README.md"
    if not readme.exists():
        return None
    files = sorted(p.name for p in d.glob("*.py"))
    body = md_to_html(readme.read_text(encoding="utf-8"), name)
    body += ('<h2>Files in this lab</h2><ul>'
             + "".join(f"<li><code>{html.escape(f)}</code></li>" for f in files) + "</ul>")
    body = console_panel(name, d) + body
    idx = [p.name for p in LABS].index(name)
    nav = ""
    if idx:
        nav += f'<a href="/lab/{LABS[idx-1].name}">← previous</a>'
    if idx + 1 < len(LABS):
        nav += f'<a href="/lab/{LABS[idx+1].name}">next →</a>'
    return shell(name, body, nav)


def console_panel(name: str, d: Path) -> str:
    """The part a terminal usually does: prepare the env, start a Worker, kill it, run the starter.

    Every button is a Python snippet the runtime executes in this lab's own virtualenv. The Worker
    is a detached child process of the kernel, so `kill -9` from this page is the same signal the
    lab asks you to send from a second terminal.
    """
    lab_dir, labs_root = str(d), str(ROOT)
    queue = f"lab-{name[:2]}"
    prepare = (
        "import subprocess, sys, os\n"
        f"os.chdir({lab_dir!r})\n"
        f"sys.path[:0] = [{lab_dir!r}, {labs_root!r}]\n"
        f"os.environ['TASK_QUEUE'] = {queue!r}\n"
        "print(subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', '-r',\n"
        f"    os.path.join({labs_root!r}, 'requirements.txt')], capture_output=True, text=True).stderr[-400:])\n"
        "import temporalio; print('temporalio', temporalio.__version__ if hasattr(temporalio,'__version__') else 'ready')\n"
        f"print('cwd', os.getcwd(), '| TASK_QUEUE', os.environ['TASK_QUEUE'])"
    )
    worker = (
        "import subprocess, sys, os, time\n"
        "WORKER = globals().get('WORKER')\n"
        "if WORKER and WORKER.poll() is None: print('worker already running, pid', WORKER.pid)\n"
        "else:\n"
        "    WORKER = subprocess.Popen([sys.executable, 'worker.py'], cwd=os.getcwd(),\n"
        "        env={**os.environ, 'PYTHONUNBUFFERED':'1'},\n"
        "        stdout=open('worker.out','ab'), stderr=subprocess.STDOUT, start_new_session=True)\n"
        "    globals()['WORKER'] = WORKER; time.sleep(2)\n"
        "    print('worker pid', WORKER.pid, '| alive' if WORKER.poll() is None else '| died, see worker.out')"
    )
    kill = (
        "import os, signal, time\n"
        "W = globals().get('WORKER')\n"
        "if not W or W.poll() is not None: print('no worker running')\n"
        "else:\n"
        "    os.kill(W.pid, signal.SIGKILL); time.sleep(1)\n"
        "    print('SIGKILL sent to', W.pid, '| exit code', W.poll())"
    )
    starter = (
        "import subprocess, sys, os\n"
        "r = subprocess.run([sys.executable, 'starter.py'], cwd=os.getcwd(), env=os.environ,\n"
        "    capture_output=True, text=True, timeout=180)\n"
        "print(r.stdout[-3000:] or r.stderr[-3000:])"
    )
    tail = (
        "print(open('worker.out').read()[-3000:] if os.path.exists('worker.out') else 'no worker output yet')"
    )
    def b(label, code, primary=False):
        cls = "run primary" if primary else "run"
        return f'<button class="{cls}" data-inline="{html.escape(code)}">{label}</button>'
    return f"""<div class="panel">
<h2>Run it here</h2>
<p class="hint">With Option A already set up, use these buttons for this lab. <strong>Prepare this
lab</strong> installs its Python packages and sets its Task Queue; then start the Worker and run the
starter. The kill button sends <code>SIGKILL</code> to the Worker. Terminal commands in the exercise
below describe Option B.</p>
<div class="btns" data-lab="{html.escape(name)}">
  {b("1 · Prepare this lab", prepare, True)}
  {b("2 · Start Worker", worker)}
  {b("3 · Run starter.py", starter)}
  {b("kill -9 the Worker", kill)}
  {b("Worker output", tail)}
</div>
<div class="out" id="out-panel" hidden></div>
<details><summary>Python console</summary>
<textarea id="console" rows="4" spellcheck="false"
  placeholder="from common import connect, show_history&#10;await show_history(await connect(), 'agent-42')"></textarea>
<button class="run" id="console-run">Run</button>
<div class="out" id="out-console" hidden></div></details>
</div>"""


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
