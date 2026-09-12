"""Knobs go into the environment before `config` is imported: every console module reads them once.
Fast fake GPU, a throwaway SQLite file, a test prefix so nothing collides with a live console."""
import os
import sys
import tempfile

CONSOLE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_db = os.path.join(tempfile.mkdtemp(prefix="eval-console-test-"), "console.sqlite3")
os.environ["EVAL_CONSOLE_DB"] = _db
os.environ["LAB_PREFIX"] = "test115-"
os.environ["EVAL_CASE_SECONDS"] = "0.2"
os.environ["EVAL_CASE_TICKS"] = "4"
os.environ["EVAL_HICCUP_FRACTION"] = "0.1"
os.environ["EVAL_CASE_PARALLELISM"] = "4"
sys.path.insert(0, CONSOLE_DIR)
