# Lab 1 — The Machine

Goal: see AgentRun (stage 1: plan one step, call the model once, sleep, finish) survive the death
of the process running it, then say where the program counter went.

`AgentRun` is the one program this course builds: an agent loop (decide a step, call a model or tool,
look at the result, repeat) whose parts have fixed names — `plan()` decides in Workflow code, `call_llm` and
`execute_tool` act as Activities. Stage 1 is deliberately tiny; every later lab adds one capability to this
same file. Reading page: "The program this course builds".

## 1.1 Bring it up
1. `docker compose up -d` in `labs/`; open http://localhost:8233.
2. `export TASK_QUEUE=lab-01` in **every** terminal you open for this lab. The Worker polls that
   queue and the Client starts the Workflow on it; name them differently and the Workflow is created
   and then waits forever, with no error anywhere — Temporal creates Task Queues on first use and
   cannot know you meant a different one.
3. Terminal 1: `python worker.py`. Terminal 2: `python starter.py`.
4. In the UI, find `agent-42`. Read the events: `ActivityTaskScheduled  call_llm`, then `TimerStarted`.
   Write down every event id so far, the pending timer, and the Worker identity on the Workflow Tasks.

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
   is Running, with a Workflow Task scheduled and nobody to take it.
5. Now `python worker.py` again. The new Worker takes that waiting task, replays, and the Workflow
   completes: `WorkflowTaskStarted`, `WorkflowTaskCompleted`, `WorkflowExecutionCompleted`. Note the
   identity on the last Workflow Task — a different process from the one you killed.

Which events prove the Service was making progress while your application compute was absent? (12
and 13, and 14–15 after them.) Module 2 annotates this same history event by event.

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

Then write one paragraph in your own words. Do not say "Temporal saved the stack." Compare with
module 2 afterwards, and keep the paragraph: module 2 makes it precise.

## 1.5 Stretch: two Workers
**Predict first, in writing:** once a Worker has run a Workflow Task for `agent-42`, is that
execution bound to that process?

Then start two Workers before running the starter, kill whichever one picks up the first Workflow
Task, and watch. Affinity to one Worker (module 2 names it *sticky execution*) is a caching
optimization; it is not what makes the Workflow durable.

## Done when
You can answer all four without looking anything up:

1. Why can `starter.py` exit while the Workflow keeps existing?
2. Why can every Worker disappear while a timer is pending, and the timer still fire?
3. Why does restarting a Worker not start `AgentRun` over from the beginning?
4. Why is "Temporal stores the Python stack" the wrong explanation?
