import asyncio
import hmac
import json
import uuid
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from .auth import (
    SESSION_COOKIE,
    authenticate,
    authenticated_session,
    create_first_admin,
    current_user,
    issue_session,
    require_csrf,
    revoke_session,
    set_session_cookie,
)
from .config import get_settings
from .database import SessionFactory, session
from .graph import Graph, validate_graph
from .models import (
    AuditEvent,
    Credential,
    Execution,
    ExecutionStatus,
    IdempotencyKey,
    NodeExecution,
    NodeExecutionAttempt,
    Project,
    Session,
    User,
    WebhookTrigger,
    Workflow,
)
from .security import CredentialCipher
from .service import (
    ConflictError,
    ValidationError,
    cancel_execution,
    create_execution,
    create_execution_in_transaction,
    publish,
    update_draft,
)

app = FastAPI(title="FlowAPI", version="0.1.0")
DB = Annotated[AsyncSession, Depends(session)]
CurrentUser = Annotated[User, Depends(current_user)]
Csrf = Annotated[None, Depends(require_csrf)]


class DraftUpdate(BaseModel):
    draft_revision: int
    graph: Graph


class RunRequest(BaseModel):
    trigger_data: dict[str, Any] = Field(default_factory=dict)


class Credentials(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=12, max_length=1024)


class ProjectCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    slug: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", max_length=100)


class WorkflowCreate(BaseModel):
    project_id: uuid.UUID
    name: str = Field(min_length=1, max_length=200)
    slug: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$", max_length=100)


class CredentialWrite(BaseModel):
    project_id: uuid.UUID
    name: str = Field(min_length=1, max_length=200)
    type: str = Field(pattern=r"^(bearer_token|basic_auth|api_key|postgresql)$")
    data: dict[str, str]


def validate_credential_fields(kind: str, data: dict[str, str]) -> None:
    required = {
        "bearer_token": {"token"},
        "basic_auth": {"username", "password"},
        "api_key": {"header_name", "value"},
        "postgresql": {"host", "port", "database", "username", "password"},
    }[kind]
    if not required.issubset(data) or any(not data[field] for field in required):
        raise HTTPException(status_code=422, detail=f"Credential requires: {', '.join(sorted(required))}")


async def read_limited_body(request: Request, limit: int) -> bytes:
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > limit:
            raise HTTPException(status_code=413, detail="Webhook payload exceeds size limit")
        body.extend(chunk)
    return bytes(body)


def safe_webhook_headers(request: Request) -> dict[str, str]:
    redacted = {"authorization", "cookie", "proxy-authorization", "x-api-key"}
    return {key: "[REDACTED]" if key.lower() in redacted else value for key, value in request.headers.items()}


async def authorize_webhook(db: AsyncSession, webhook: WebhookTrigger, request: Request) -> None:
    if webhook.auth_type == "none":
        return
    if webhook.auth_type != "bearer" or webhook.credential_id is None:
        raise HTTPException(status_code=401, detail="Webhook authentication failed")
    credential = await db.get(Credential, webhook.credential_id)
    if credential is None:
        raise HTTPException(status_code=401, detail="Webhook authentication failed")
    values = CredentialCipher(get_settings().FLOWAPI_ENCRYPTION_KEY).decrypt_json(credential.encrypted_data)
    expected = values.get("token", "")
    supplied = request.headers.get("authorization", "")
    if not supplied.startswith("Bearer ") or not hmac.compare_digest(supplied[7:], expected):
        raise HTTPException(status_code=401, detail="Webhook authentication failed")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.api_route(
    "/hooks/{token}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"],
    status_code=status.HTTP_202_ACCEPTED,
    response_model=None,
)
async def receive_webhook(token: str, request: Request, db: DB) -> dict[str, Any] | Response:
    settings = get_settings()
    raw_body = await read_limited_body(request, settings.MAX_WEBHOOK_BODY_BYTES)
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if raw_body and content_type == "application/json":
        try:
            body: Any = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Invalid JSON webhook payload") from exc
    else:
        body = raw_body.decode("utf-8", errors="replace")
    idempotency_key = request.headers.get("idempotency-key")
    if idempotency_key is not None and (not idempotency_key or len(idempotency_key) > 200):
        raise HTTPException(status_code=400, detail="Invalid Idempotency-Key")

    async with db.begin():
        webhook = await db.scalar(
            select(WebhookTrigger).where(WebhookTrigger.token == token).with_for_update(key_share=True)
        )
        if webhook is None or not webhook.enabled or webhook.method != request.method:
            raise HTTPException(status_code=404, detail="Webhook not found")
        await authorize_webhook(db, webhook, request)
        scope = f"webhook:{webhook.id}"
        if idempotency_key:
            # An advisory transaction lock also serializes concurrent first use,
            # where SELECT FOR UPDATE cannot lock a row that does not exist yet.
            await db.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:value, 0))"),
                {"value": f"{scope}:{idempotency_key}"},
            )
            existing = await db.scalar(
                select(IdempotencyKey).where(IdempotencyKey.scope == scope, IdempotencyKey.key == idempotency_key)
            )
            if existing is not None:
                if request.method == "HEAD":
                    return Response(status_code=status.HTTP_202_ACCEPTED)
                return {"execution_id": existing.execution_id, "status": "queued"}
        workflow = await db.scalar(
            select(Workflow).where(Workflow.id == webhook.workflow_id).with_for_update(key_share=True)
        )
        if workflow is None:
            raise HTTPException(status_code=404, detail="Webhook not found")
        execution = await create_execution_in_transaction(
            db,
            workflow,
            "webhook",
            {
                "method": request.method,
                "query": dict(request.query_params.multi_items()),
                "headers": safe_webhook_headers(request),
                "body": body,
            },
        )
        if idempotency_key:
            db.add(IdempotencyKey(scope=scope, key=idempotency_key, execution_id=execution.id))
    if request.method == "HEAD":
        return Response(status_code=status.HTTP_202_ACCEPTED)
    return {"execution_id": execution.id, "status": "queued"}


@app.get("/api/v1/setup/status")
async def setup_status(db: DB) -> dict[str, bool]:
    return {"setup_required": (await db.scalar(select(func.count()).select_from(User))) == 0}


@app.post("/api/v1/setup", status_code=status.HTTP_201_CREATED)
async def setup(body: Credentials, db: DB, response: Response) -> dict[str, Any]:
    try:
        user = await create_first_admin(db, body.email, body.password)
    except (PermissionError, ValueError) as exc:
        code = status.HTTP_409_CONFLICT if isinstance(exc, PermissionError) else status.HTTP_422_UNPROCESSABLE_ENTITY
        raise HTTPException(status_code=code, detail=str(exc)) from exc
    token, csrf, _ = await issue_session(db, user, get_settings())
    set_session_cookie(response, token, get_settings())
    return {"user": {"id": user.id, "email": user.email}, "csrf_token": csrf}


@app.post("/api/v1/auth/login")
async def login(body: Credentials, db: DB, response: Response) -> dict[str, Any]:
    user = await authenticate(db, body.email, body.password)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")
    token, csrf, _ = await issue_session(db, user, get_settings())
    set_session_cookie(response, token, get_settings())
    return {"user": {"id": user.id, "email": user.email}, "csrf_token": csrf}


@app.post("/api/v1/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    db: DB,
    response: Response,
    record: Annotated[Session, Depends(authenticated_session)],
    _: Csrf,
) -> None:
    await revoke_session(db, record)
    response.delete_cookie(SESSION_COOKIE, path="/")


@app.get("/api/v1/auth/me")
async def me(user: CurrentUser) -> dict[str, Any]:
    return {"id": user.id, "email": user.email, "is_admin": user.is_admin}


@app.get("/api/v1/projects")
async def list_projects(db: DB, _: CurrentUser) -> list[dict[str, Any]]:
    rows = (await db.scalars(select(Project).order_by(Project.created_at))).all()
    return [{"id": row.id, "name": row.name, "slug": row.slug, "created_at": row.created_at} for row in rows]


@app.post("/api/v1/projects", status_code=status.HTTP_201_CREATED)
async def create_project(body: ProjectCreate, db: DB, user: CurrentUser, _: Csrf) -> dict[str, Any]:
    project = Project(name=body.name, slug=body.slug)
    db.add(project)
    await db.flush()
    db.add(
        AuditEvent(
            actor_user_id=user.id, event_type="project.created", target_type="project", target_id=str(project.id)
        )
    )
    await db.commit()
    return {"id": project.id, "name": project.name, "slug": project.slug}


@app.get("/api/v1/credentials")
async def list_credentials(db: DB, _: CurrentUser, project_id: uuid.UUID) -> list[dict[str, Any]]:
    rows = (
        await db.scalars(
            select(Credential).where(Credential.project_id == project_id).order_by(Credential.updated_at.desc())
        )
    ).all()
    return [
        {"id": row.id, "project_id": row.project_id, "name": row.name, "type": row.type, "updated_at": row.updated_at}
        for row in rows
    ]


@app.post("/api/v1/credentials", status_code=status.HTTP_201_CREATED)
async def create_credential(body: CredentialWrite, db: DB, user: CurrentUser, _: Csrf) -> dict[str, Any]:
    if await db.get(Project, body.project_id) is None:
        raise HTTPException(status_code=404, detail="Project not found")
    validate_credential_fields(body.type, body.data)
    encrypted = CredentialCipher(get_settings().FLOWAPI_ENCRYPTION_KEY).encrypt_json(body.data)
    credential = Credential(
        project_id=body.project_id,
        name=body.name,
        type=body.type,
        encrypted_data=encrypted,
    )
    db.add(credential)
    await db.flush()
    db.add(
        AuditEvent(
            actor_user_id=user.id,
            event_type="credential.created",
            target_type="credential",
            target_id=str(credential.id),
            metadata_json={"type": credential.type, "name": credential.name},
        )
    )
    await db.commit()
    return {"id": credential.id, "project_id": credential.project_id, "name": credential.name, "type": credential.type}


@app.put("/api/v1/credentials/{credential_id}")
async def update_credential(
    credential_id: uuid.UUID, body: CredentialWrite, db: DB, user: CurrentUser, _: Csrf
) -> dict[str, Any]:
    credential = await db.get(Credential, credential_id, with_for_update=True)
    if credential is None or credential.project_id != body.project_id:
        raise HTTPException(status_code=404, detail="Credential not found")
    validate_credential_fields(body.type, body.data)
    credential.name = body.name
    credential.type = body.type
    credential.encrypted_data = CredentialCipher(get_settings().FLOWAPI_ENCRYPTION_KEY).encrypt_json(body.data)
    db.add(
        AuditEvent(
            actor_user_id=user.id,
            event_type="credential.updated",
            target_type="credential",
            target_id=str(credential.id),
            metadata_json={"type": credential.type, "name": credential.name},
        )
    )
    await db.commit()
    return {"id": credential.id, "project_id": credential.project_id, "name": credential.name, "type": credential.type}


@app.get("/api/v1/workflows")
async def list_workflows(db: DB, _: CurrentUser, project_id: uuid.UUID | None = None) -> list[dict[str, Any]]:
    query = select(Workflow).order_by(Workflow.updated_at.desc())
    if project_id is not None:
        query = query.where(Workflow.project_id == project_id)
    rows = (await db.scalars(query)).all()
    return [
        {
            "id": row.id,
            "project_id": row.project_id,
            "name": row.name,
            "slug": row.slug,
            "status": row.status,
            "draft_revision": row.draft_revision,
            "active_version_id": row.active_version_id,
        }
        for row in rows
    ]


@app.get("/api/v1/workflows/{workflow_id}")
async def workflow_detail(workflow_id: uuid.UUID, db: DB, _: CurrentUser) -> dict[str, Any]:
    workflow = await db.get(Workflow, workflow_id)
    if workflow is None:
        raise HTTPException(status_code=404, detail="Workflow not found")
    webhook = await db.scalar(select(WebhookTrigger).where(WebhookTrigger.workflow_id == workflow.id))
    return {
        "id": workflow.id,
        "project_id": workflow.project_id,
        "name": workflow.name,
        "slug": workflow.slug,
        "status": workflow.status,
        "draft_revision": workflow.draft_revision,
        "draft_definition": workflow.draft_definition,
        "active_version_id": workflow.active_version_id,
        "webhook_path": f"/hooks/{webhook.token}" if webhook else None,
    }


@app.post("/api/v1/workflows", status_code=status.HTTP_201_CREATED)
async def create_workflow(body: WorkflowCreate, db: DB, user: CurrentUser, _: Csrf) -> dict[str, Any]:
    if await db.get(Project, body.project_id) is None:
        raise HTTPException(status_code=404, detail="Project not found")
    workflow = Workflow(project_id=body.project_id, name=body.name, slug=body.slug)
    db.add(workflow)
    await db.flush()
    db.add(
        AuditEvent(
            actor_user_id=user.id, event_type="workflow.created", target_type="workflow", target_id=str(workflow.id)
        )
    )
    await db.commit()
    return {
        "id": workflow.id,
        "project_id": workflow.project_id,
        "name": workflow.name,
        "slug": workflow.slug,
        "draft_revision": workflow.draft_revision,
    }


@app.post("/api/v1/workflows/validate")
async def validate(body: Graph, _: CurrentUser) -> dict[str, Any]:
    errors = validate_graph(body)
    return {"valid": not errors, "errors": [error.model_dump() for error in errors]}


@app.put("/api/v1/workflows/{workflow_id}/draft")
async def save_draft(workflow_id: uuid.UUID, body: DraftUpdate, db: DB, _: CurrentUser, _csrf: Csrf) -> dict[str, Any]:
    try:
        workflow = await update_draft(db, workflow_id, body.draft_revision, body.graph)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail={"code": "STALE_DRAFT_REVISION", "message": str(exc)}) from exc
    return {"id": workflow.id, "draft_revision": workflow.draft_revision}


@app.post("/api/v1/workflows/{workflow_id}/publish")
async def publish_workflow(workflow_id: uuid.UUID, db: DB, _: CurrentUser, _csrf: Csrf) -> dict[str, Any]:
    try:
        version = await publish(db, workflow_id)
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={"code": "WORKFLOW_VALIDATION_FAILED", "errors": [p.model_dump() for p in exc.problems]},
        ) from exc
    return {"id": version.id, "workflow_id": version.workflow_id, "version_number": version.version_number}


@app.post("/api/v1/workflows/{workflow_id}/run", status_code=202)
async def run_workflow(workflow_id: uuid.UUID, body: RunRequest, db: DB, _: CurrentUser, _csrf: Csrf) -> dict[str, Any]:
    execution = await create_execution(db, workflow_id, "manual", body.trigger_data)
    return {"execution_id": execution.id, "status": "queued"}


@app.get("/api/v1/executions")
async def list_executions(db: DB, _: CurrentUser, workflow_id: uuid.UUID | None = None) -> list[dict[str, Any]]:
    query = select(Execution).order_by(Execution.created_at.desc()).limit(100)
    if workflow_id is not None:
        query = query.where(Execution.workflow_id == workflow_id)
    rows = (await db.scalars(query)).all()
    return [
        {
            "id": row.id,
            "workflow_id": row.workflow_id,
            "workflow_version_id": row.workflow_version_id,
            "trigger_type": row.trigger_type,
            "status": row.status,
            "created_at": row.created_at,
            "started_at": row.started_at,
            "finished_at": row.finished_at,
        }
        for row in rows
    ]


@app.get("/api/v1/executions/{execution_id}")
async def execution_detail(execution_id: uuid.UUID, db: DB, _: CurrentUser) -> dict[str, Any]:
    execution = await db.get(Execution, execution_id)
    if execution is None:
        raise HTTPException(status_code=404, detail="Execution not found")
    nodes = (await db.scalars(select(NodeExecution).where(NodeExecution.execution_id == execution.id))).all()
    attempts = (
        await db.scalars(
            select(NodeExecutionAttempt)
            .join(NodeExecution, NodeExecution.id == NodeExecutionAttempt.node_execution_id)
            .where(NodeExecution.execution_id == execution.id)
            .order_by(NodeExecutionAttempt.started_at)
        )
    ).all()
    attempts_by_node: dict[uuid.UUID, list[dict[str, Any]]] = {}
    for attempt in attempts:
        attempts_by_node.setdefault(attempt.node_execution_id, []).append(
            {
                "attempt": attempt.attempt_number,
                "status": attempt.status,
                "input": attempt.input_data,
                "output": attempt.output_data,
                "error_code": attempt.error_code,
                "error_message": attempt.error_message,
                "started_at": attempt.started_at,
                "finished_at": attempt.finished_at,
            }
        )
    return {
        "id": execution.id,
        "workflow_id": execution.workflow_id,
        "workflow_version_id": execution.workflow_version_id,
        "status": execution.status,
        "trigger_type": execution.trigger_type,
        "cancellation_requested": execution.cancellation_requested,
        "error_code": execution.error_code,
        "error_message": execution.error_message,
        "nodes": [
            {
                "node_id": node.node_id,
                "node_type": node.node_type,
                "status": node.status,
                "configuration": node.configuration_snapshot,
                "output": node.output_data,
                "attempts": attempts_by_node.get(node.id, []),
            }
            for node in nodes
        ],
    }


@app.get("/api/v1/executions/{execution_id}/events")
async def execution_events(execution_id: uuid.UUID, _: CurrentUser) -> StreamingResponse:
    async def stream() -> Any:
        previous = ""
        for _iteration in range(3600):
            async with SessionFactory() as event_db:
                execution = await event_db.get(Execution, execution_id)
                if execution is None:
                    yield 'event: error\ndata: {"code":"NOT_FOUND"}\n\n'
                    return
                nodes = (
                    await event_db.scalars(
                        select(NodeExecution)
                        .where(NodeExecution.execution_id == execution_id)
                        .order_by(NodeExecution.node_id)
                    )
                ).all()
                payload = json.dumps(
                    {
                        "status": execution.status,
                        "nodes": [{"node_id": node.node_id, "status": node.status} for node in nodes],
                    },
                    default=str,
                )
            if payload != previous:
                yield f"event: execution\ndata: {payload}\n\n"
                previous = payload
            if execution.status in {ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED}:
                return
            await asyncio.sleep(1)

    return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


@app.post("/api/v1/executions/{execution_id}/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel(execution_id: uuid.UUID, db: DB, user: CurrentUser, _: Csrf) -> dict[str, Any]:
    try:
        execution = await cancel_execution(db, execution_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail="Execution not found") from exc
    db.add(
        AuditEvent(
            actor_user_id=user.id,
            event_type="execution.cancel_requested",
            target_type="execution",
            target_id=str(execution.id),
        )
    )
    await db.commit()
    return {
        "execution_id": execution.id,
        "status": execution.status,
        "cancellation_requested": execution.cancellation_requested,
    }
