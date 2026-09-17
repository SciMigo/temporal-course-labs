# Lab 1 — The Machine

Goal: see AgentRun (stage 1: plan one step, call the model once, sleep, finish) survive the death
of the process running it, then say where the program counter went.

## Objective

**Why:** See which part of an agent run survives when its Worker process dies.

**By the end:** Start `AgentRun`, kill its Worker during a timer, and explain from the event history why another Worker can finish the same run.

`AgentRun` is the one program this course builds: an agent loop (decide a step, call a model or tool,
look at the result, repeat) whose parts have fixed names — `plan()` decides in Workflow code, `call_llm` and
`execute_tool` act as Activities. Stage 1 is deliberately tiny; every later lab adds one capability to this
same file. Reading page: "The program this course builds".

## 1.1 Bring it up
**Option A — local page:** Click **Prepare this lab**, then **Run** on the Worker and Start agent-42
cards above. The page installs the lab's packages, sets `TASK_QUEUE=lab-01`, and uses the right
working directory. The starter runs in the background while you do the crash experiment; click
**Output** on its card to see progress and the eventual result. Do not run the terminal commands below.

**Option B — terminals:** Leave the Temporal dev server from the course setup running. In the
`temporal-course-labs` directory, create a virtual environment with Python 3.11 or newer, install
the packages, and enter the Lab 1 directory. The command below uses Python 3.12; on Linux,
substitute another installed Python 3.11+ if needed.

```bash
python3.12 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cd 01-the-machine
export TASK_QUEUE=lab-01
../.venv/bin/python worker.py
```

In a second terminal, `cd` to that same `01-the-machine` directory (`pwd` in the first terminal
shows its full path), then run:

```bash
export TASK_QUEUE=lab-01
../.venv/bin/python starter.py
```

Both processes must use the same Task Queue. A mismatch leaves the Workflow waiting with no error;
both scripts print their queue so you can compare them.

In the [Temporal Web UI](http://localhost:8233), find `agent-42`. Read the events:
`ActivityTaskScheduled  call_llm`, then `TimerStarted`. Write down every event id so far, the pending
timer, and the Worker identity on the Workflow Tasks.

## 1.2 Kill the Worker, and leave it dead
The point of this experiment is not that a Workflow survives a restart. It is *where* the program
lives while no process is running it. So do not restart anything yet.

1. While the timer is pending (30 s), `kill -9` the Worker (Ctrl-C is too polite: it lets the Worker
   shut down cleanly; we want a crash). The pid is in the line the Worker printed at startup.
2. Confirm there is no Worker: the Workers tab in the UI, or `ps`. Application compute is now zero.
3. Wait until more than 30 s have passed since `TimerStarted`, then refresh the history. **New events
   appeared while nothing of yours was running.** On the labs' dev server (Temporal Server 1.31.2,
   2026-09) they are:

```text
12  TimerFired             the Service's clock, not your process
13  WorkflowTaskScheduled  offered on the dead Worker's sticky queue
14  WorkflowTaskTimedOut   sticky schedule-to-start expired (10 s on the Python Worker)
15  WorkflowTaskScheduled  reoffered on the shared queue — and it waits here
```

4. Note what did *not* happen: the Workflow did not fail, did not roll back, and did not finish. It
   is Running, with a Workflow Task scheduled and nobody to take it. The timer has already fired, so
   the execution is no longer waiting for time. It is waiting for compute.
5. Now click **Run** on the Worker card again (Option A), or run `../.venv/bin/python worker.py` in the Lab 1
   directory (Option B). The new Worker takes that waiting task, replays, and the Workflow
   completes: `WorkflowTaskStarted`, `WorkflowTaskCompleted`, `WorkflowExecutionCompleted`. Note the
   identity on the last Workflow Task — a different process from the one you killed.
6. Count the `call_llm key=...` log lines across both Worker terminals (the killed Worker's output is
   still on screen; on the local lab page, the Worker's **Output** shows both). There is exactly one, and it
   came from the Worker you killed. Yet the new Worker finished `AgentRun`, and `starter.py` printed
   the model's answer as the result (see the starter card's **Output** on the local page). The new process never called the model, and it ran
   `self.context_summary = answer` all the same. Where did `answer` come from?

Which events prove the Service was making progress while your application compute was absent? (12
and 13, and 14–15 after them.) And the answer to step 6 is event 7, `ActivityTaskCompleted`: the new
Worker ran `AgentRun.run` from the top, and when it reached the `await` on `call_llm`, the SDK handed
back the result recorded in event 7 instead of scheduling the Activity again. Workflow code re-runs;
completed Activities do not. Module 2 annotates this same history event by event.

## 1.3 Who does what
Fill this in from what you just watched, before reading module 1's answer.

| Thing | Worker or Temporal Service? |
|---|---|
| Run `plan()` | |
| Run the `call_llm` Activity | |
| Store the Event History | |
| Count down the durable timer | |
| Record `TimerFired` | |
| Reconstruct the Workflow's state after the crash | |

## 1.4 Where was the program counter stored?
First, pick one:

- **A.** Temporal serialized the Python coroutine stack and restored it.
- **B.** The Worker process was secretly still alive.
- **C.** Temporal persisted Events; a Worker re-executes deterministic Workflow code against those
  Events to reconstruct the state.
- **D.** `starter.py` held the state — it was still running.

Then write one paragraph in your own words. Do not say "Temporal saved the stack." Compare it with
the answer at the end of module 1's reading ("Where was the program counter stored?"), and keep your
paragraph: module 2 makes it precise.

## 1.5 Stretch: two Workers
**Predict first, in writing:** once a Worker has run a Workflow Task for `agent-42`, is that
execution bound to that process?

Then start two Workers before running the starter. While the timer is pending, `kill -9` whichever
one ran the Workflow Tasks so far (its pid is the Worker identity on events 3 and 9), and watch. Note
the time between `TimerFired` and the surviving Worker's `WorkflowTaskStarted`, and compare it with
events 13–15 in 1.2.

What we measured on the labs' dev server (Temporal Server 1.31.2, 2026-09): the surviving Worker
took the Task about 0.02 s after `TimerFired` in 3 runs of 3, with no `WorkflowTaskTimedOut`, and the
same happened when the second Worker was started 12 s *after* the kill. The 10 s pause in 1.2 showed
up only when no Worker at all was polling as the timer fired. A Worker caches the state of the
Workflows it runs, and the Service prefers to send their next Task back to it; module 2 names this
*sticky execution*. It is a caching optimization, not what makes the Workflow durable: the Worker
that takes over has no cache, and it rebuilds the state by replay.

## What you learned

- Temporal keeps the Event History and durable timer while no Worker is running.
- A new Worker rebuilds Workflow state by replaying code against recorded Events; it does not restore a Python stack.
- A completed Activity result is read from history during replay, so the model call does not run again.

## Questions to answer
You can answer all four without looking anything up:

1. Why can `starter.py` exit while the Workflow keeps existing?
2. Why can every Worker disappear while a timer is pending, and the timer still fire?
3. Why does restarting a Worker not start `AgentRun` over from the beginning?
4. Why is "Temporal stores the Python stack" the wrong explanation?
