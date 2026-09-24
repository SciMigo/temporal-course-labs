# Lab 1 — The Machine

Goal: crash the process running `AgentRun`, watch Temporal advance the run without application
compute, and use the history to explain how a new Worker continues it.

## Objective

**Why:** A restart demo only proves that a process can restart. This experiment shows where a
Temporal program lives while no Worker process exists.

**By the end:** You will have one Workflow run, two Worker process identities, one model call, and
a history showing which work belonged to the Temporal Service.

## Before you run

- **Predict:** If the Worker dies during a timer, what can advance without it?
- **Watch:** Keep the Temporal Web UI open on `agent-42` and watch its Event History.
- **Do not start twice:** After the crash, restart only the Worker. The existing Workflow will
  continue.
- **Terminal route:** The browser workspace is the main path. Terminal commands are collected once
  at the end for learners who prefer them.

`AgentRun` is the program this course grows. In this first stage, Workflow code plans one step, the
`call_llm` Activity returns an answer, and a durable timer waits before the Workflow finishes.

## Run the experiment

Use the local browser workspace linked from this page. Its action cards choose the directory,
Python environment, and `lab-01` Task Queue for you.

### Checkpoint 1 — Start, then crash

1. Click **Prepare this lab** once.
2. Click **Run Worker**, then **Start Workflow**.
3. Open the Worker's **Output**. As soon as `call_llm key=...` appears, click **Crash Worker**.
   The Activity has completed and the Workflow's 30-second timer is now pending, so there is no
   need to race the clock while copying evidence.
4. Leave the Worker dead. Do not click **Start Workflow** again.

In the [Temporal Web UI](http://localhost:8233), open `agent-42` and confirm that its history
contains `ActivityTaskCompleted` and `TimerStarted`. Record the Worker identity on the latest
Workflow Task and the one `call_llm` line from Worker **Output**. The Workflow should remain
**Running**: it has not failed, rolled back, or completed.

### Checkpoint 2 — Observe progress with no Worker

Wait until at least 45 seconds after `TimerStarted`, then refresh the Event History. The 30-second
timer fires first; the dead Worker's sticky queue then needs roughly 10 seconds to time out.

Look for this sequence (event numbers can differ if you reran the experiment):

```text
TimerFired             the Service's clock advanced
WorkflowTaskScheduled  first offered on the dead Worker's sticky queue
WorkflowTaskTimedOut   no Worker took that sticky task
WorkflowTaskScheduled  reoffered on the shared queue, waiting for compute
```

This is the observation the lab is built around: new Events appeared while none of your application
code was running.

### Checkpoint 3 — Resume the existing run

1. Click **Run Worker** again. This starts a new Worker process.
2. Watch the existing `agent-42` run complete. Do not start another Workflow.
3. Compare the final Workflow Task identity with the identity you recorded before the crash.
4. Open Worker **Output** and count `call_llm key=...` again. On your first attempt, the total is
   still one. The browser keeps output from earlier attempts, so after a rerun count only the lines
   belonging to the current experiment.

The new Worker finished code that uses the Activity result, but it did not call the model again.
The recorded `ActivityTaskCompleted` Event supplied that result during replay.

## Explain the evidence

First, complete this short ownership table from what you observed:

| Thing | Worker or Temporal Service? |
|---|---|
| Run `plan()` | |
| Run the `call_llm` Activity | |
| Store the Event History | |
| Count down the durable timer | |
| Record `TimerFired` | |
| Rebuild Workflow state after the crash | |

Then connect each observation to a claim:

| Evidence | What it proves |
|---|---|
| `TimerFired` appeared with no Worker | |
| A Workflow Task waited until a new Worker arrived | |
| The final Workflow Task has a different Worker identity | |
| `call_llm` ran once in this experiment | |

Finally, answer one question in your own words:

> Where was the program counter while the Worker was dead?

<details>
<summary>Check your answer</summary>

Temporal persisted Events. A Worker re-executed deterministic Workflow code against those Events
to reconstruct the state. Temporal did not serialize and restore the Python coroutine stack.

</details>

## Optional extension — Two Workers (terminals only)

The browser workspace intentionally gives one process to each action card, so clicking **Run
Worker** twice reports that the Worker is already running. Use two terminal windows for this
extension.

Start two copies of `../.venv/bin/python worker.py` before starting a fresh Workflow ID. Kill the
Worker that handled the first Workflow Tasks while the timer is pending. Compare the delay after
`TimerFired` with the no-Worker outage above.

The surviving Worker can take the Task immediately. Sticky execution is a cache optimization, not
the source of durability; a Worker without the cache can reconstruct state by replay.

## Terminal reference

Use this route instead of the browser action cards, not in addition to them. From the repository
root, start the dev server and install the Python environment once:

```bash
docker compose up -d
python3.12 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

In the first lab terminal:

```bash
cd 01-the-machine
export TASK_QUEUE=lab-01
../.venv/bin/python worker.py
```

In a second terminal, enter the same directory and use the same Task Queue:

```bash
export TASK_QUEUE=lab-01
../.venv/bin/python starter.py
```

During the timer, run the `kill -9 <pid>` command printed by `worker.py`. After the history reaches
the shared-queue `WorkflowTaskScheduled` Event, run `../.venv/bin/python worker.py` again.

For the two-Worker extension, open a third terminal in `01-the-machine`, export the same Task Queue,
and start a second `../.venv/bin/python worker.py` before starting a fresh Workflow ID.

## You are done when

You can point to one Event or log line for each claim:

- the Service owned the timer;
- a new Worker continued the existing run;
- replay reused the completed Activity result; and
- no Python stack was restored.
