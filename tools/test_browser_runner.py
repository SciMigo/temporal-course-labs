"""Focused checks for the browser process controls."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import browser_runner


class BrowserRunnerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.lab = self.root / "01-test"
        self.lab.mkdir()
        logs = self.lab / ".browser-runs" / "test"
        logs.mkdir(parents=True)
        browser_runner.SESSION = browser_runner.Session(
            self.lab, self.root, {"TASK_QUEUE": "lab-01"}, logs
        )

    def tearDown(self) -> None:
        session = browser_runner.SESSION
        if session:
            for process in session.processes.values():
                if process.handle.poll() is None:
                    session.kill(next(key for key, value in session.processes.items() if value is process))
        browser_runner.SESSION = None
        self.temp.cleanup()

    def test_command_uses_lab_directory_and_environment(self) -> None:
        (self.lab / "inspect.py").write_text(
            "import os\nprint(os.getcwd(), os.environ['TASK_QUEUE'], os.environ['SCENARIO'])\n"
        )
        response = browser_runner.run_command("SCENARIO=check python inspect.py")
        key = response.splitlines()[0]
        browser_runner.SESSION.processes[key].handle.wait(timeout=5)
        output = browser_runner.output(key)
        self.assertIn(str(self.lab) + " lab-01 check", output)
        self.assertIn("exited · code 0", output)

    def test_crash_stops_only_selected_process(self) -> None:
        (self.lab / "wait.py").write_text("import time\ntime.sleep(60)\n")
        session = browser_runner.SESSION
        session.start("worker-1", "Worker 1", ["wait.py"])
        session.start("worker-2", "Worker 2", ["wait.py"])
        self.assertIn("SIGKILL", session.kill("worker-1"))
        self.assertIsNone(session.processes["worker-2"].handle.poll())

    def test_command_rejects_shell_and_files_outside_lab(self) -> None:
        with self.assertRaises(ValueError):
            browser_runner.run_command("python ../outside.py")
        with self.assertRaises(ValueError):
            browser_runner.run_command("python inspect.py | cat")


if __name__ == "__main__":
    unittest.main()
