"""Add durable webhook and schedule triggers.

Revision ID: 0004_triggers
Revises: 0003_credentials
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_triggers"
down_revision: str | None = "0003_credentials"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "webhook_triggers",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("workflow_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("workflows.id"), nullable=False),
        sa.Column("token", sa.String(100), nullable=False),
        sa.Column("method", sa.String(10), nullable=False),
        sa.Column("auth_type", sa.String(30), nullable=False),
        sa.Column("credential_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("credentials.id")),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_webhook_triggers_workflow_id", "webhook_triggers", ["workflow_id"], unique=True)
    op.create_index("ix_webhook_triggers_token", "webhook_triggers", ["token"], unique=True)
    op.create_table(
        "schedule_triggers",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("workflow_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("workflows.id"), nullable=False),
        sa.Column("cron", sa.String(200), nullable=False),
        sa.Column("timezone", sa.String(100), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("last_run_at", sa.DateTime(timezone=True)),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_schedule_triggers_workflow_id", "schedule_triggers", ["workflow_id"], unique=True)
    op.create_index("ix_schedule_triggers_next_run_at", "schedule_triggers", ["next_run_at"])
    op.create_table(
        "schedule_occurrences",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("schedule_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("schedule_triggers.id"), nullable=False),
        sa.Column("occurrence_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("executions.id"), nullable=False),
        sa.UniqueConstraint("schedule_id", "occurrence_time"),
        sa.UniqueConstraint("execution_id"),
    )
    op.create_index("ix_schedule_occurrences_schedule_id", "schedule_occurrences", ["schedule_id"])
    op.create_table(
        "idempotency_keys",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("scope", sa.String(200), nullable=False),
        sa.Column("key", sa.String(200), nullable=False),
        sa.Column("execution_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("executions.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("scope", "key"),
    )


def downgrade() -> None:
    op.drop_table("idempotency_keys")
    op.drop_table("schedule_occurrences")
    op.drop_table("schedule_triggers")
    op.drop_table("webhook_triggers")
