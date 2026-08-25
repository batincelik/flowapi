import asyncio

from sqlalchemy import select

from .database import SessionFactory
from .graph import Edge, Graph, Node
from .models import Project, Workflow
from .service import publish, update_draft

DEMOS = {
    "branching": Graph(
        nodes=[
            Node(id="trigger", type="manual_trigger"),
            Node(id="condition", type="condition", configuration={"expression": "$trigger.approved == true"}),
            Node(id="approved", type="set", configuration={"values": {"decision": "approved"}}),
            Node(id="declined", type="set", configuration={"values": {"decision": "declined"}}),
        ],
        edges=[
            Edge(id="a", source_node_id="trigger", target_node_id="condition"),
            Edge(id="b", source_node_id="condition", source_handle="true", target_node_id="approved"),
            Edge(id="c", source_node_id="condition", source_handle="false", target_node_id="declined"),
        ],
    ),
    "parallel-join": Graph(
        nodes=[
            Node(id="trigger", type="manual_trigger"),
            Node(id="left", type="set", configuration={"values": {"branch": "left"}}),
            Node(id="right", type="set", configuration={"values": {"branch": "right"}}),
            Node(id="join", type="merge", configuration={"mode": "wait_for_all"}),
        ],
        edges=[
            Edge(id="a", source_node_id="trigger", target_node_id="left"),
            Edge(id="b", source_node_id="trigger", target_node_id="right"),
            Edge(id="c", source_node_id="left", target_node_id="join"),
            Edge(id="d", source_node_id="right", target_node_id="join"),
        ],
    ),
    "durable-delay": Graph(
        nodes=[
            Node(id="trigger", type="manual_trigger"),
            Node(id="wait", type="delay", configuration={"seconds": 5}),
            Node(id="done", type="set", configuration={"values": {"resumed": True}}),
        ],
        edges=[
            Edge(id="a", source_node_id="trigger", target_node_id="wait"),
            Edge(id="b", source_node_id="wait", target_node_id="done"),
        ],
    ),
}


async def seed() -> None:
    async with SessionFactory() as db:
        project = await db.scalar(select(Project).where(Project.slug == "flowapi-demo"))
        if project is None:
            project = Project(name="FlowAPI Demo", slug="flowapi-demo")
            db.add(project)
            await db.commit()
        project_id = project.id
    for slug, graph in DEMOS.items():
        async with SessionFactory() as db:
            workflow = await db.scalar(select(Workflow).where(Workflow.project_id == project_id, Workflow.slug == slug))
            if workflow is None:
                workflow = Workflow(project_id=project_id, name=slug.replace("-", " ").title(), slug=slug)
                db.add(workflow)
                await db.commit()
            workflow_id, revision = workflow.id, workflow.draft_revision
        async with SessionFactory() as db:
            await update_draft(db, workflow_id, revision, graph)
        async with SessionFactory() as db:
            await publish(db, workflow_id)
    print("Demo workflows published. Execute them through the API or editor; no history was fabricated.")


if __name__ == "__main__":
    asyncio.run(seed())
