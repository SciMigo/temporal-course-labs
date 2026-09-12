"""Register the custom Search Attributes AgentRun writes: AgentStatus, CurrentStep, Owner.

A Search Attribute must exist in the Namespace before a Workflow upserts it (an upsert of an
unregistered key fails the Workflow Task, forever, until someone registers it). Registration is
an operator action, once per Namespace, through the Operator Service — the same call as
`temporal operator search-attribute create --name AgentStatus --type Keyword`.

Equivalent at server start (dev server only):
    temporal server start-dev --search-attribute AgentStatus=Keyword --search-attribute CurrentStep=Int --search-attribute Owner=Keyword
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import NAMESPACE, connect  # noqa: E402
from temporalio.api.enums.v1 import IndexedValueType  # noqa: E402
from temporalio.api.operatorservice.v1 import AddSearchAttributesRequest, ListSearchAttributesRequest  # noqa: E402

WANTED = {
    "AgentStatus": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
    "CurrentStep": IndexedValueType.INDEXED_VALUE_TYPE_INT,
    "Owner": IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
}


async def main() -> None:
    client = await connect()
    listed = await client.operator_service.list_search_attributes(ListSearchAttributesRequest(namespace=NAMESPACE))
    existing = dict(listed.custom_attributes)
    missing = {k: v for k, v in WANTED.items() if k not in existing}
    for k in WANTED:
        if k in existing:
            print(f"exists  {k:12s} {IndexedValueType.Name(existing[k]).removeprefix('INDEXED_VALUE_TYPE_')}")
    if missing:
        await client.operator_service.add_search_attributes(
            AddSearchAttributesRequest(namespace=NAMESPACE, search_attributes=missing))
        for k, v in missing.items():
            print(f"created {k:12s} {IndexedValueType.Name(v).removeprefix('INDEXED_VALUE_TYPE_')}")
    listed = await client.operator_service.list_search_attributes(ListSearchAttributesRequest(namespace=NAMESPACE))
    assert all(k in listed.custom_attributes for k in WANTED), listed.custom_attributes
    print("namespace", NAMESPACE, "custom attributes:", sorted(listed.custom_attributes))


if __name__ == "__main__":
    asyncio.run(main())
