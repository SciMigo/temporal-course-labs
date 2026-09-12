"""Test plumbing: import the lab's modules, run `async def` tests without a plugin."""
from __future__ import annotations

import asyncio
import inspect
import os
import sys

import pytest

LAB_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, LAB_DIR)                                 # workflows.py
sys.path.insert(0, os.path.dirname(LAB_DIR))                # common/


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    """Run coroutine tests on a fresh event loop. stdlib only — no pytest-asyncio needed."""
    if inspect.iscoroutinefunction(pyfuncitem.obj):
        kwargs = {name: pyfuncitem.funcargs[name] for name in pyfuncitem._fixtureinfo.argnames}
        asyncio.run(pyfuncitem.obj(**kwargs))
        return True
    return None


@pytest.fixture(autouse=True)
def _fresh_tool_effects():
    """Each test starts with an empty tool system-of-record."""
    import workflows
    workflows.reset_effects()
    yield
    workflows.reset_effects()
