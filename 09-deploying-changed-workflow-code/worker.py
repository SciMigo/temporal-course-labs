"""A Worker for one code version, optionally as one version of a Worker Deployment.

    AGENTRUN_CODE=v1 python worker.py                          # unversioned, v1 code (9.1)
    AGENTRUN_CODE=v1 BUILD_ID=v1 python worker.py              # versioned: deployment agent-runs, build v1 (9.2)
    AGENTRUN_CODE=v2 BUILD_ID=v2 python worker.py              # ... build v2, side by side

Env: TASK_QUEUE (agent-runs), DEPLOYMENT_NAME (agent-runs), AGENTRUN_STEPS_PER_RUN (0; 9.3 uses 2).
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import connect  # noqa: E402
from temporalio.common import VersioningBehavior  # noqa: E402
from temporalio.worker import Worker, WorkerDeploymentConfig, WorkerDeploymentVersion  # noqa: E402

from code_versions import load  # noqa: E402
from workflows import TASK_QUEUE  # noqa: E402

DEPLOYMENT_NAME = os.environ.get("DEPLOYMENT_NAME", "agent-runs")
BUILD_ID = os.environ.get("BUILD_ID")               # unset -> an unversioned Worker


async def main() -> None:
    module, agent_run, activities = load()
    client = await connect()
    deployment_config = None
    if BUILD_ID:
        deployment_config = WorkerDeploymentConfig(   # as of 2026-09 (temporalio 1.32); see upstream page
            version=WorkerDeploymentVersion(deployment_name=DEPLOYMENT_NAME, build_id=BUILD_ID),
            use_worker_versioning=True,
            # Every AgentRun is pinned: it finishes its run on the build that started it. The
            # upstream shape is `@workflow.defn(versioning_behavior=...)` per Workflow Type; a
            # Worker-level default keeps the same code runnable by an unversioned Worker (9.1).
            default_versioning_behavior=VersioningBehavior.PINNED,
        )
    worker = Worker(client, task_queue=TASK_QUEUE, workflows=[agent_run], activities=activities,
                    deployment_config=deployment_config,
                    identity=f"{BUILD_ID or 'unversioned'}-{module.BUILD_ID}-{os.getpid()}")
    version = f"{DEPLOYMENT_NAME}.{BUILD_ID}" if BUILD_ID else "unversioned"
    print(f"worker pid={os.getpid()} code={module.BUILD_ID} version={version} polling {TASK_QUEUE!r}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
