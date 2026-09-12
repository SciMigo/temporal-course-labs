# AgentRun — architecture defense (template, lab 11.4)

Fill this in from memory, then check against `DEFENSE_ANSWERS.md`. One line per cell. If a cell
takes more than one line, you have not found the mechanism yet — you have found the module.

Author: ______________   Date: __________   Server version you ran it on: ______________

## 1. The failure contract (one line per box)

Write this before the diagram. For every box, answer eight questions: What survives a Worker death?
Can this operation happen twice? Where is its durable identity? Who retries it? Who cancels it?
What happens across a deployment? What goes into Event History? How does it Continue-As-New? The
table holds the three that fit in a line; the others land in section 2 (who retries it, across a
deployment, continue-as-new) and in the ten questions (who cancels it).

| Box | A Worker dies here | On retry | What history records |
|---|---|---|---|
| `plan()` | | | |
| `call_llm` (LLM Activity) | | | |
| `execute_tool` (Tool Activity) | | | |
| `ResearchAgent` (child) | | | |
| pause wait (`wait_condition`) | | | |
| `continue_as_new` | | | |

## 2. Requirement → mechanism (12 rows)

For each requirement: the *one* Temporal mechanism that satisfies it, the name of the box in the
diagram (a method, an Activity, a handler, a call), and the lab where you saw it fail and recover.

| # | Requirement | Mechanism | Box in AgentRun | Where you saw it |
|---|---|---|---|---|
| 1 | Runs for up to 72 hours | | | |
| 2 | Makes 1,000+ model calls | | | |
| 3 | Tool calls can fail | | | |
| 4 | Expensive calls must not accidentally duplicate | | | |
| 5 | GPUs disappear | | | |
| 6 | Workers deploy twice a day; old executions survive | | | |
| 7 | User can pause and resume | | | |
| 8 | User can change instructions mid-run | | | |
| 9 | User can ask what it is doing | | | |
| 10 | May delegate to sub-agents | | | |
| 11 | History cannot grow forever | | | |
| 12 | Operators need exact execution diagnostics | | | |

## 3. Where Temporal ends

| Layer | Owns | For AgentRun, concretely |
|---|---|---|
| Temporal | | |
| Kubernetes | | |
| Kafka | | |
| Postgres | | |

## 4. The ten questions (one sentence each)

1. What exactly survives when a Worker crashes?
2. How does replay reconstruct Workflow state?
3. Why must Workflow code be deterministic?
4. Why can an Activity execute twice?
5. Activity retry vs. Workflow replay?
6. Signal vs. Query vs. Update?
7. Activity vs. Child Workflow?
8. When should you continue-as-new?
9. What does the user see, and from which store? (product projection, never the visibility list)
9. How do you safely deploy changed Workflow code?
10. Where does Temporal end and Kubernetes / Kafka / Postgres begin?

## 5. Evidence

Paste, for your own run: the Run ID list from `run_200_steps.py`, the `kill -9` handoff events, and
`pytest` output from `tests/`. Anything you cannot paste, you have not defended.
