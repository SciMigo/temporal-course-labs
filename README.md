# Durable Execution with Temporal — labs

The hands-on half of the course [**Durable Execution with Temporal**](https://scimigo.com/learn/temporal-durable-execution).
Eleven labs, one program: `AgentRun`, an agent-job runtime that grows one capability per module and
survives one deliberate failure per module. The reading pages live on the course site; the code you
run lives here.

Each lab kills something on purpose — a Worker, a Task Queue, a deploy, a tenant's assumptions — and
then asks you to explain what survived and why.

```bash
git clone https://github.com/SciMigo/temporal-course-labs.git
cd temporal-course-labs

docker compose up -d                       # Temporal dev server: gRPC :7233, Web UI :8233
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export TASK_QUEUE=lab-01                   # in EVERY terminal for a given lab
cd 01-the-machine
python worker.py                           # terminal 1
python starter.py                          # terminal 2
```

The Worker runs on your machine, not in the container, so you can `kill -9` it from a second
terminal while watching the Web UI. That action is the day-one lesson.

## Or run them from a local page, without a terminal

```bash
python lab_server.py        # http://127.0.0.1:3000 — standard library only, nothing to install
```

Each lab gets a page with the exercise text and a **Run it here** panel: prepare this lab's
virtualenv, start the Worker, run `starter.py`, and `kill -9` the Worker — the same signal the lab
asks you to send from a second terminal, sent from a button. There is also a Python console scoped to
the lab's directory.

Code execution needs [agent-runtime](https://github.com/thinkinginmath/agent-runtime) running on your
machine (`agent-runtime serve`, port 9477). It executes in a virtualenv per lab, and the page is
served on port 3000, which the runtime already trusts, so there is no token to paste and no pairing
prompt. Both ends are on loopback: nothing is exposed off your machine, and your browser's
local-network rules never come into it.

Without the runtime the pages are still the lab text, with copy buttons on every command. The
terminal path in this README works exactly as written; the page is the same thing without a terminal.

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

`worker.py` registers that module's Workflows and Activities on task queue `lab-NN`; `starter.py`
starts an execution with a fixed Workflow ID so you can find it in the UI; `README.md` is the
exercise text; `tests/` is what you run to check yourself. Set `TASK_QUEUE` in **every** terminal of
a lab — a Worker and a Client on different queues wait for each other forever, with no error
anywhere.

Common helpers in `common/`: `connect()` reads `TEMPORAL_ADDRESS` (default `localhost:7233`), and
`show_history(workflow_id)` prints the event list the way the labs annotate it.

Requirements: Docker, Python 3.10+. Everything runs locally against the dev server; no Temporal
Cloud account is needed or used.

## Recorded histories

Some labs ship real Event Histories as fixtures (`histories/`, `tests/histories/`) so replay tests
and the forensic exercise work without you first reproducing a failure. They were captured on a
local dev server; Worker identities and paths in them are anonymized and mean nothing outside the
lab.

## License

Original lab code and text: MIT, © SciMigo — see `LICENSE`. Parts are adapted from Temporal's
MIT-licensed documentation and Python samples; see `THIRD_PARTY_NOTICES.md`. Temporal is a
trademark of Temporal Technologies Inc. This course is independent and not endorsed by Temporal.
