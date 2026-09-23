"""Process controls for the local lab pages.

Each agent-runtime kernel imports this module into its own Python process, so a lab's
Workers and client commands share one session without sharing state with other labs.
"""

from __future__ import annotations

import importlib.metadata
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Process:
    label: str
    command: tuple[str, ...]
    handle: subprocess.Popen[bytes]
    log: Path


@dataclass
class Session:
    lab_dir: Path
    root: Path
    base_env: dict[str, str]
    logs: Path
    processes: dict[str, Process] = field(default_factory=dict)
    next_command: int = 1

    def __post_init__(self) -> None:
        self.lab_dir = self.lab_dir.resolve()
        self.root = self.root.resolve()
        self.logs = self.logs.resolve()

    def start(self, key: str, label: str, args: list[str], env: dict[str, str] | None = None) -> str:
        if not re.fullmatch(r"[a-z0-9-]+", key):
            raise ValueError("Invalid action key")
        existing = self.processes.get(key)
        if existing and existing.handle.poll() is None:
            return f"{label} is already running (pid {existing.handle.pid})."
        command = (sys.executable, *args)
        log = self.logs / f"{key}.log"
        child_env = {**os.environ, **self.base_env, **(env or {}), "PYTHONUNBUFFERED": "1"}
        with log.open("ab") as output:
            handle = subprocess.Popen(
                command, cwd=self.lab_dir, env=child_env, stdin=subprocess.DEVNULL,
                stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
            )
        self.processes[key] = Process(label, command, handle, log)
        return f"Started {label} (pid {handle.pid}). Output appears below; the page stays usable."

    def output(self, key: str) -> str:
        process = self.processes.get(key)
        if not process:
            return "This action has not run in this browser session."
        exit_code = process.handle.poll()
        state = f"running · pid {process.handle.pid}" if exit_code is None else f"exited · code {exit_code}"
        with process.log.open("rb") as source:
            source.seek(max(0, process.log.stat().st_size - 10000))
            body = source.read().decode("utf-8", errors="replace").strip()
        return f"{process.label} · {state}\n\n{body or '(waiting for output)'}"

    def kill(self, key: str) -> str:
        process = self.processes.get(key)
        if not process or process.handle.poll() is not None:
            return "No running process for this action."
        os.killpg(process.handle.pid, signal.SIGKILL)
        process.handle.wait(timeout=5)
        return f"Sent SIGKILL to {process.label} (pid {process.handle.pid})."

    def stop(self, key: str) -> str:
        process = self.processes.get(key)
        if not process or process.handle.poll() is not None:
            return "No running process for this action."
        os.killpg(process.handle.pid, signal.SIGTERM)
        return f"Asked {process.label} (pid {process.handle.pid}) to stop."

    def status(self) -> str:
        if not self.processes:
            return "Prepared. No lab processes have started yet."
        lines = []
        for key, process in self.processes.items():
            code = process.handle.poll()
            state = f"running (pid {process.handle.pid})" if code is None else f"exited (code {code})"
            lines.append(f"{key}: {state} — {process.label}")
        return "\n".join(lines)


SESSION: Session | None = None


def prepare(lab_dir: str, root: str, env: dict[str, str]) -> str:
    """Install requirements in this kernel, then begin a fresh set of logs."""
    global SESSION
    target = Path(lab_dir).resolve()
    labs_root = Path(root).resolve()
    if target.parent != labs_root:
        raise ValueError("Lab directory must be directly inside the labs repository")
    if SESSION and any(p.handle.poll() is None for p in SESSION.processes.values()):
        return "This lab already has running processes. Use their output controls, or stop them before preparing again."
    os.chdir(target)
    os.environ.update(env)
    for path in (str(target), str(labs_root)):
        if path not in sys.path:
            sys.path.insert(0, path)
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "-r", str(labs_root / "requirements.txt")],
        capture_output=True, text=True, env={**os.environ, "PIP_DISABLE_PIP_VERSION_CHECK": "1"},
    )
    if result.returncode:
        raise RuntimeError("Package install failed:\n" + (result.stderr or result.stdout)[-3000:])
    logs = target / ".browser-runs" / str(time.time_ns())
    logs.mkdir(parents=True)
    SESSION = Session(target, labs_root, dict(env), logs)
    version = importlib.metadata.version("temporalio")
    queue = env.get("TASK_QUEUE", "set by the lab")
    return f"Ready · temporalio {version} · Task Queue {queue}\n{target}"


def _session() -> Session:
    if SESSION is None:
        raise RuntimeError("Click Prepare this lab first.")
    return SESSION


def start(key: str, label: str, args: list[str], env: dict[str, str] | None = None) -> str:
    return _session().start(key, label, args, env)


def output(key: str) -> str:
    return _session().output(key)


def kill(key: str) -> str:
    return _session().kill(key)


def stop(key: str) -> str:
    return _session().stop(key)


def status() -> str:
    return _session().status()


def run_command(raw: str) -> str:
    """Run a Python script or pytest command from the current lab, without a shell."""
    session = _session()
    if len(raw) > 1000:
        raise ValueError("Command is too long")
    words = shlex.split(raw)
    env: dict[str, str] = {}
    while words and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", words[0]):
        name, value = words.pop(0).split("=", 1)
        if name in {"PATH", "PYTHONPATH", "PYTHONHOME"}:
            raise ValueError(f"{name} cannot be set here")
        env[name] = value
    if not words or words.pop(0) not in {"python", "python3", "python3.11", "python3.12"}:
        raise ValueError("Use a command beginning with python, for example: python history.py agent-42")
    if not words:
        raise ValueError("Name a Python script or use python -m pytest")
    if words[0] == "-m":
        if len(words) < 2 or words[1] not in {"pytest", "uvicorn"}:
            raise ValueError("Only python -m pytest and python -m uvicorn are supported here")
    else:
        script = (session.lab_dir / words[0]).resolve()
        if script.suffix != ".py" or not script.is_file() or not script.is_relative_to(session.lab_dir):
            raise ValueError("Choose a .py file inside this lab")
    if any(word in {"|", ">", "<", "&&", ";"} for word in words):
        raise ValueError("Shell pipes and redirects are unavailable here; run one Python command at a time")
    key = f"command-{session.next_command}"
    session.next_command += 1
    label = " ".join(shlex.quote(word) for word in words)
    return key + "\n" + session.start(key, label, words, env)
