"""Test knobs must be in the environment before `workflows` is imported — the Worker's sandbox
re-imports the module and reads them again, so os.environ is the one channel both sides see."""
import os
import sys

LAB_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("LAB_PREFIX", "test10-")
os.environ["GPU_SCHEDULE_TO_START_SECONDS"] = "2"
os.environ["TOOL_SECONDS"] = "0.2"
sys.path.insert(0, LAB_DIR)
sys.path.insert(0, os.path.dirname(LAB_DIR))
