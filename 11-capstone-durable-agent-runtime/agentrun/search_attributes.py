"""Registered once per Namespace (setup_search_attributes.py); written by Workflow code."""
from temporalio.common import SearchAttributeKey

AGENT_STATUS = SearchAttributeKey.for_keyword("AgentStatus")
CURRENT_STEP = SearchAttributeKey.for_int("CurrentStep")
OWNER = SearchAttributeKey.for_keyword("Owner")
