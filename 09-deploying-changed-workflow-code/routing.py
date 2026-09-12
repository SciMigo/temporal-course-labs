"""Deployment routing from Python, for a box without the Temporal CLI. Each subcommand names
the CLI form it mirrors (verified against CLI 1.8.3 / Server 1.31.2).

    python routing.py describe                      # temporal worker deployment describe --name agent-runs
    python routing.py set-current v2                # temporal worker deployment set-current-version --deployment-name agent-runs --build-id v2
    python routing.py set-ramping v2 10             # temporal worker deployment set-ramping-version ... --build-id v2 --percentage 10
    python routing.py show agent-42                 # temporal workflow describe -w agent-42   (the Versioning Info block)
    python routing.py resume agent-42               # temporal workflow signal -w agent-42 --name resume

Env: DEPLOYMENT_NAME (agent-runs), TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import NAMESPACE, connect  # noqa: E402
from temporalio.api.enums.v1 import VersioningBehavior, WorkerDeploymentVersionStatus  # noqa: E402
from temporalio.api.workflowservice.v1 import (  # noqa: E402
    DescribeWorkerDeploymentRequest,
    SetWorkerDeploymentCurrentVersionRequest,
    SetWorkerDeploymentRampingVersionRequest,
)
from temporalio.client import Client  # noqa: E402

DEPLOYMENT_NAME = os.environ.get("DEPLOYMENT_NAME", "agent-runs")


async def describe_deployment(client: Client) -> str:
    resp = await client.workflow_service.describe_worker_deployment(
        DescribeWorkerDeploymentRequest(namespace=NAMESPACE, deployment_name=DEPLOYMENT_NAME))
    info = resp.worker_deployment_info
    rc = info.routing_config
    lines = [f"deployment {info.name}: current={rc.current_version or '__unversioned__'}"
             f" ramping={rc.ramping_version or '-'} ({rc.ramping_version_percentage:g}%)"]
    for v in info.version_summaries:
        status = WorkerDeploymentVersionStatus.Name(v.status).removeprefix("WORKER_DEPLOYMENT_VERSION_STATUS_")
        lines.append(f"  build {v.deployment_version.build_id:14s} {status}")
    return "\n".join(lines)


async def set_current(client: Client, build_id: str) -> None:
    await client.workflow_service.set_worker_deployment_current_version(
        SetWorkerDeploymentCurrentVersionRequest(
            namespace=NAMESPACE, deployment_name=DEPLOYMENT_NAME, build_id=build_id,
            identity="routing.py"))


async def set_ramping(client: Client, build_id: str, percentage: float) -> None:
    await client.workflow_service.set_worker_deployment_ramping_version(
        SetWorkerDeploymentRampingVersionRequest(
            namespace=NAMESPACE, deployment_name=DEPLOYMENT_NAME, build_id=build_id,
            percentage=percentage, identity="routing.py"))


async def describe_versioning(client: Client, workflow_id: str) -> str:
    """The Versioning Info block of `temporal workflow describe`: behavior, version, override."""
    desc = await client.get_workflow_handle(workflow_id).describe()
    info = desc.raw_description.workflow_execution_info
    vi = info.versioning_info
    behavior = VersioningBehavior.Name(vi.behavior).removeprefix("VERSIONING_BEHAVIOR_")
    version = f"{vi.deployment_version.deployment_name}.{vi.deployment_version.build_id}" \
        if vi.deployment_version.build_id else (vi.version or "unversioned")
    override = ""
    if vi.HasField("versioning_override"):
        o = vi.versioning_override
        if o.HasField("pinned"):
            override = f" override=PINNED:{o.pinned.version.deployment_name}.{o.pinned.version.build_id}"
        elif o.HasField("auto_upgrade"):
            override = " override=AUTO_UPGRADE"
    transition = ""
    if vi.HasField("version_transition"):
        t = vi.version_transition.deployment_version
        transition = f" transitioning-to={t.deployment_name}.{t.build_id}"
    return (f"{workflow_id} run={desc.run_id} status={desc.status.name} "
            f"behavior={behavior} version={version}{override}{transition}")


async def main(argv: list[str]) -> None:
    client = await connect()
    cmd, args = argv[0], argv[1:]
    if cmd == "describe":
        print(await describe_deployment(client))
    elif cmd == "set-current":
        await set_current(client, args[0]); print(await describe_deployment(client))
    elif cmd == "set-ramping":
        await set_ramping(client, args[0], float(args[1])); print(await describe_deployment(client))
    elif cmd == "show":
        print(await describe_versioning(client, args[0]))
    elif cmd == "resume":
        await client.get_workflow_handle(args[0]).signal("resume"); print(f"resume -> {args[0]}")
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:] or ["describe"]))
