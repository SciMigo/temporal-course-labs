# Durable Execution with Temporal — labs

The hands-on half of the course [**Durable Execution with Temporal**](https://scimigo.com/learn/temporal-durable-execution).
Eleven labs, one program: `AgentRun`, an agent-job runtime that grows one capability per module and
survives one deliberate failure per module. The reading pages live on the course site; the code you
run lives here.

Each lab kills something on purpose — a Worker, a Task Queue, a deploy, a tenant's assumptions — and
then asks you to explain what survived and why.

Choose the [terminal walkthrough](#on-a-macbook-start-to-finish) or the
[local page](#or-run-them-from-a-local-page). The local page handles each lab's Python environment,
Task Queue, process output, and Worker crashes for you.

The Worker runs on your machine, not in the container, so you can `kill -9` it from a second
terminal while watching the Web UI. That action is the day-one lesson.

## On a MacBook, start to finish

Two names collide in this course, so separate them first. **`AgentRun`** is the Workflow *you
build* — the program these eleven labs grow. **`agent-runtime`** is a separate, optional kernel that
lets a browser page run lab code on your machine; it belongs to the next section and nothing here
uses it.

The browser you *do* need is the Temporal **Web UI on :8233**. Reading event history is most of what
these labs ask of you.

### Prerequisites

- **Docker Desktop, running.** `docker compose up -d` needs the daemon, and the whale has to be
  started once after a reboot.
- **Python 3.10 or newer.** `temporalio` requires >= 3.10 and macOS ships 3.9 as `python3`. Check
  with `python3 -V`; if it says 3.9, `brew install python@3.12` and use `python3.12` below.
  `requirements.txt` pins `temporalio==1.32.0`, the SDK every recorded output in these labs came
  from, and 1.32.0 requires Python 3.10, so on 3.9 `pip install` stops with
  `No matching distribution found for temporalio==1.32.0`.

### The first run

```bash
git clone https://github.com/SciMigo/temporal-course-labs.git
cd temporal-course-labs

docker compose up -d                       # dev server: gRPC :7233, Web UI :8233
open http://localhost:8233                 # leave this tab open for the whole course

python3.12 -m venv .venv
./.venv/bin/pip install -r requirements.txt

export TASK_QUEUE=lab-01
cd 01-the-machine
../.venv/bin/python worker.py
```

In a second terminal, `cd` to the same `01-the-machine` directory (`pwd` in the first terminal
shows its full path), then run:

```bash
export TASK_QUEUE=lab-01
../.venv/bin/python starter.py
```

`starter.py` waits for the result, then prints the history. With nothing killed it ends at **event
16** — `WORKFLOW_EXECUTION_COMPLETED`, about thirty seconds after `TIMER_STARTED`.

### The experiment, and the one bit of timing that matters

The lesson is not that a Workflow survives a restart. It is *where the program lives while no
process is running it*. So kill the Worker and then **leave it dead**.

1. Start a run and let it reach the timer. Terminal 1 printed
   `worker pid=NNNNN polling 'lab-01'; kill -9 NNNNN to crash it` — use that pid.
2. `kill -9 NNNNN` from a **third** terminal. Terminal 2 is blocked inside `starter.py`, waiting on
   the Workflow result, and you want it to stay that way: it survives the whole outage and prints
   the finished history itself at the end. Not Ctrl-C on the Worker — that lets it shut down
   politely, and you want a crash.
3. **Wait at least 45 seconds before restarting anything.** The timer is 30 s and the sticky
   schedule-to-start timeout is a further 10 s. Restart too early and events 14 and 15 never appear,
   and they are the point.
4. Read the history with no Worker alive anywhere. Four events were written while your machine ran
   nothing of yours:

```text
12  TIMER_FIRED              the Service's clock, not your process
13  WORKFLOW_TASK_SCHEDULED  offered on the dead Worker's sticky queue
14  WORKFLOW_TASK_TIMED_OUT  sticky schedule-to-start expired
15  WORKFLOW_TASK_SCHEDULED  reoffered on the shared queue — and it waits here
```

5. Now `../.venv/bin/python worker.py` again from `01-the-machine`. The run finishes, and the identity on event 16 is a **different
   pid** from the one you killed:

```text
16  WORKFLOW_TASK_STARTED    <new pid>@<your-mac>
17  WORKFLOW_TASK_COMPLETED
18  WORKFLOW_EXECUTION_COMPLETED
```

That is `starter.py` in terminal 2 finally returning: it waited out the crash, the dead thirty
seconds and the restart, and printed a result computed by a process that no longer exists.

Sixteen events without the kill, eighteen with it, and a different pid on the last three. If you got
those numbers, the lab worked.

### When it does not

| Symptom | Cause |
|---|---|
| `starter.py` hangs, no error anywhere | `TASK_QUEUE` differs between your two terminals. Temporal creates a queue on first use and cannot know you meant another one. |
| `Cannot connect to the Docker daemon` | Docker Desktop is not running. |
| `pip`: `No matching distribution found for temporalio==1.32.0` | You are on macOS's stock Python 3.9. See prerequisites. |
| History stops at 13 | You restarted the Worker too quickly. |
| You scripted the kill and your shell died | `pkill -f worker.py` matches your own shell's command line too. Use the pid the Worker printed. |

`docker compose down -v` wipes all history and hands you a clean dev server.

## Or run them from a local page

Everything below runs on your own machine. After setup in three terminals, use the browser buttons
for each lab. Run commands in separate terminals as labeled.

**Terminal 1 — Temporal dev server** (needs Docker Desktop on macOS):

```bash
docker compose up -d
```

**Terminal 2 — agent-runtime** (Python 3.11+). On macOS, install it first:

```bash
curl -fsSL https://raw.githubusercontent.com/SciMigo/agent-runtime/main/scripts/install-macos.sh | bash
```

Wait for `Installed Agent Runtime` before starting it. A GitHub archive timeout means installation
failed; retry when GitHub is reachable or use the clone fallback below. An older installation may
still start after a failed upgrade.

```bash
agent-runtime serve --port 9477
```

On Linux, or as a clone fallback on macOS, run in a separate directory:

```bash
git clone https://github.com/SciMigo/agent-runtime.git
cd agent-runtime
python3 -m venv .venv
./.venv/bin/pip install -e .
./.venv/bin/agent-runtime serve --port 9477
```

**Terminal 3 — lab pages.** Open a terminal in `temporal-course-labs`. The server uses only Python's
standard library:

```bash
python3 lab_server.py
```

Then open **http://127.0.0.1:3000/** (HTTP, not HTTPS), exactly as printed by the server. If your
browser adds `/en`, the lab server accepts that path too. The strip at the top of every page shows
whether the runtime and Temporal are up.

Each lab has its own **Run this lab** workspace. Click **Prepare this lab** once, then use the cards
for that lab's Worker roles, scripts, or tests. Every launch returns immediately; **Output** updates
while the process runs. **Crash Worker** sends SIGKILL, so you can leave the Worker dead while
Temporal continues. For a variation in the exercise, paste one `python ...` command into **Run
another command**. The page uses the lab's directory and Python environment and saves output under
that lab's ignored `.browser-runs/` directory. The advanced Python console shares the same kernel.

The cards cover the main path through each lab. Python scripts and `python -m pytest` variations
work in the command field, including leading environment settings such as
`LAB03_BREAK=clock python worker.py`. Shell pipelines and Docker administration commands in the
terminal examples remain terminal commands.

The runtime trusts loopback origins on any port: no token to paste and no pairing prompt. Both ends
are on loopback, so nothing is exposed off your machine. If port 3000 is in use, run
`python3 lab_server.py --port 3001` and open `http://127.0.0.1:3001/`.

Without the runtime the pages still show the exercise text and copy buttons. The terminal path in
this README remains available.

## The labs

| | Lab | What you break |
|---|---|---|
| 01 | The Machine | kill the Worker during a durable timer; the timer fires without it |
| 02 | Event History and Replay | hand a live execution from one Worker to another and diff the histories |
| 03 | Determinism | break replay three ways and read each error |
| 04 | Activities and Failure Semantics | trigger all four timeouts; duplicate a tool call under a crash |
| 05 | Signals, Queries, Updates | pause across a Worker restart; make two Updates interleave |
| 06 | Continue-As-New and Child Workflows | one Workflow ID, many Runs; kill the child's Worker |
| 07 | Cancellation and Compensation | a saga that rolls back; a tool that ignores cancellation |
| 08 | Testing and Replay Tests | time-skipping, replay tests, failure injection, and a forensic history |
| 09 | Deploying Changed Workflow Code | patching, Worker Versioning, and a deploy that breaks replay |
| 10 | Operating Temporal in Production | read a broken execution from the UI and the CLI |
| 11 | Capstone: a Durable Agent Runtime | assemble it, injure it in five ways, defend the design |

## Conventions

Most labs use `worker.py` and `starter.py`; labs 10 and 11 split Workers into Workflow, CPU, and GPU
pools. `README.md` is the exercise text and `tests/` is what you run to check yourself. The browser
workspace sets each lab's queue and required prefix. In the terminal path, set `TASK_QUEUE` in
**every** terminal of a lab — a Worker and a Client on different queues wait for each other forever.

Common helpers in `common/`: `connect()` reads `TEMPORAL_ADDRESS` (default `localhost:7233`), and
`show_history(workflow_id)` prints the event list the way the labs annotate it.

Requirements: Docker, Python 3.10+. Everything runs locally against the dev server; no Temporal
Cloud account is needed or used.

## Running the labs from the course site

There is an advanced setup that puts the labs inside the course pages on scimigo.com instead of a
separate tab. It needs a tunnel, because a public page cannot reach your machine, and that means
briefly exposing your runtime to the internet behind a token. The trade-off, the recipe and the
rough edges are in [docs/remote-runtime.md](docs/remote-runtime.md). The localhost setup above needs
none of it.

## Notebooks, for the labs that suit them

Labs 2, 8, 9 and 10 also ship as notebooks (`lab-NN.ipynb` in each directory): they are mostly
reading histories and running tests, which is what cells are good at.

```bash
pip install jupyterlab && jupyter lab      # then open the lab's .ipynb
```

The other labs use the local browser workspace or terminals for their long-lived Workers. Notebook
cells are still useful for history inspection and tests; the browser workspace provides the
separate process controls needed for crash experiments.

The `README.md` in each lab is the source of truth. The notebooks are generated from it with
[jupytext](https://jupytext.readthedocs.io) — `python3 tools/make_notebooks.py` — so the two cannot
drift.

## Recorded histories

Some labs ship real Event Histories as fixtures (`histories/`, `tests/histories/`) so replay tests
and the forensic exercise work without you first reproducing a failure. They were captured on a
local dev server; Worker identities and paths in them are anonymized and mean nothing outside the
lab.

## License

Original lab code and text: MIT, © SciMigo — see `LICENSE`. Parts are adapted from Temporal's
MIT-licensed documentation and Python samples; see `THIRD_PARTY_NOTICES.md`. Temporal is a
trademark of Temporal Technologies Inc. This course is independent and not endorsed by Temporal.
