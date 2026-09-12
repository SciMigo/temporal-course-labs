"""Register the Search Attributes EvalRun writes — Org, RunStatus, Model (and EvalParentRunId, which the console
sets on a retry run; `ParentRunId` is reserved by the system for Child Workflow lineage) — through the Operator Service, once per Namespace. Same call as lab 10:
`temporal operator search-attribute create --name RunStatus --type Keyword`."""
import asyncio

from temporalio.api.enums.v1 import IndexedValueType
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest, ListSearchAttributesRequest
from temporalio.client import Client

from config import NAMESPACE, TEMPORAL_ADDRESS

WANTED = {name: IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD for name in ("Org", "RunStatus", "Model", "EvalParentRunId")}


async def main() -> None:
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=NAMESPACE)
    existing = dict((await client.operator_service.list_search_attributes(ListSearchAttributesRequest(namespace=NAMESPACE))).custom_attributes)
    missing = {k: v for k, v in WANTED.items() if k not in existing}
    for k in WANTED:
        if k in existing:
            print(f"exists  {k:12s} {IndexedValueType.Name(existing[k]).removeprefix('INDEXED_VALUE_TYPE_')}")
    if missing:
        await client.operator_service.add_search_attributes(AddSearchAttributesRequest(namespace=NAMESPACE, search_attributes=missing))
        for k, v in missing.items():
            print(f"created {k:12s} {IndexedValueType.Name(v).removeprefix('INDEXED_VALUE_TYPE_')}")
    listed = await client.operator_service.list_search_attributes(ListSearchAttributesRequest(namespace=NAMESPACE))
    assert all(k in listed.custom_attributes for k in WANTED), listed.custom_attributes
    print("namespace", NAMESPACE, "custom attributes:", sorted(listed.custom_attributes))


if __name__ == "__main__":
    asyncio.run(main())
