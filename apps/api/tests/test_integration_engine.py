import os
import uuid

import pytest
from flowapi.database import SessionFactory
from flowapi.executor import claim_node, run_claimed_node
from flowapi.graph import Edge, Graph, Node
from flowapi.models import ExecutionStatus, NodeExecution, NodeStatus, Project, Workflow
from flowapi.service import create_execution, publish, update_draft
from sqlalchemy import select

pytestmark = pytest.mark.skipif(os.environ.get("FLOWAPI_INTEGRATION") != "1", reason="requires PostgreSQL")


def graph(value: str) -> Graph:
    return Graph(
        nodes=[
            Node(id="trigger", type="manual_trigger"),
            Node(id="set", type="set", configuration={"values": {"version": value}}),
        ],
        edges=[Edge(id="edge", source_node_id="trigger", target_node_id="set")],
    )


async def run_all_nodes(execution_id: uuid.UUID) -> None:
    worker_id = uuid.uuid4()
    while True:
        async with SessionFactory() as db:
            ready = (
                await db.scalars(
                    select(NodeExecution).where(
                        NodeExecution.execution_id == execution_id,
                        NodeExecution.status == NodeStatus.READY,
                    )
                )
            ).first()
        if ready is None:
            return
        async with SessionFactory() as db:
            assert await claim_node(db, ready.id, worker_id)
        async with SessionFactory() as db:
            await run_claimed_node(db, ready.id)


async def test_real_execution_and_cross_version_pinning() -> None:
    unique = uuid.uuid4().hex
    async with SessionFactory() as db:
        project = Project(name="Integration", slug=f"integration-{unique}")
        db.add(project)
        await db.flush()
        workflow = Workflow(project_id=project.id, name="Pinned", slug=f"pinned-{unique}")
        db.add(workflow)
        await db.commit()
        workflow_id = workflow.id

    async with SessionFactory() as db:
        await update_draft(db, workflow_id, 1, graph("v1"))
    async with SessionFactory() as db:
        version_one = await publish(db, workflow_id)
        version_one_id = version_one.id
    async with SessionFactory() as db:
        execution_a = await create_execution(db, workflow_id, "manual", {"request": "a"})
        execution_a_id = execution_a.id

    # Publish v2 while execution A still has runnable v1 node state.
    async with SessionFactory() as db:
        await update_draft(db, workflow_id, 2, graph("v2"))
    async with SessionFactory() as db:
        version_two = await publish(db, workflow_id)
        assert version_two.id != version_one_id
    async with SessionFactory() as db:
        execution_b = await create_execution(db, workflow_id, "manual", {"request": "b"})
        execution_b_id = execution_b.id

    await run_all_nodes(execution_a_id)
    await run_all_nodes(execution_b_id)

    async with SessionFactory() as db:
        refreshed_a = await db.get(type(execution_a), execution_a_id)
        refreshed_b = await db.get(type(execution_b), execution_b_id)
        assert refreshed_a and refreshed_b
        assert refreshed_a.workflow_version_id == version_one_id
        assert refreshed_b.workflow_version_id == version_two.id
        assert refreshed_a.status is ExecutionStatus.COMPLETED
        assert refreshed_b.status is ExecutionStatus.COMPLETED
        outputs_a = (await db.scalars(select(NodeExecution).where(NodeExecution.execution_id == execution_a_id))).all()
        outputs_b = (await db.scalars(select(NodeExecution).where(NodeExecution.execution_id == execution_b_id))).all()
        assert next(row.output_data for row in outputs_a if row.node_id == "set") == {"version": "v1"}
        assert next(row.output_data for row in outputs_b if row.node_id == "set") == {"version": "v2"}
