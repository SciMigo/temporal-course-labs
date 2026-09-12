"""Knobs go into the environment before `agentrun` is imported: the Worker's sandbox re-imports the
package and reads them again. Small timeouts so a failure is seconds, not minutes."""
import os
import sys

LAB_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ["LAB_PREFIX"] = "test11-"
os.environ["AGENTRUN_STEPS_PER_RUN"] = "8"
os.environ["AGENTRUN_TOOL_SECONDS"] = "0.4"
os.environ["AGENTRUN_LLM_SECONDS"] = "0.01"
os.environ["AGENTRUN_TOOL_HEARTBEAT_SECONDS"] = "1"
os.environ["AGENTRUN_TOOL_START_TO_CLOSE_SECONDS"] = "3"
os.environ["AGENTRUN_GPU_SCHEDULE_TO_START_SECONDS"] = "2"
sys.path.insert(0, LAB_DIR)
sys.path.insert(0, os.path.dirname(LAB_DIR))
