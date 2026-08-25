from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True, extra="ignore")

    DATABASE_URL: str = "postgresql+asyncpg://flowapi:flowapi@localhost:5432/flowapi"
    REDIS_URL: str = "redis://localhost:6379/0"
    FLOWAPI_SECRET_KEY: str = Field(min_length=32)
    FLOWAPI_ENCRYPTION_KEY: str
    SESSION_COOKIE_SECURE: bool = True
    SESSION_TTL_HOURS: int = Field(default=24, ge=1, le=720)
    MAX_WORKFLOW_NODES: int = Field(default=500, ge=1, le=10_000)
    MAX_WORKFLOW_EDGES: int = Field(default=1000, ge=1, le=20_000)
    MAX_NODE_OUTPUT_BYTES: int = Field(default=1_048_576, ge=1024)
    MAX_EXECUTION_NODE_RUNS: int = Field(default=10_000, ge=1)
    MAX_LOOP_ITEMS: int = Field(default=1000, ge=1, le=100_000)
    POSTGRES_CONNECT_TIMEOUT: float = Field(default=10, ge=0.1, le=60)
    POSTGRES_QUERY_TIMEOUT: float = Field(default=30, ge=0.1, le=300)
    WORKER_STALE_AFTER: int = Field(default=60, ge=10)
    WORKER_HEARTBEAT_INTERVAL: int = Field(default=10, ge=1, le=300)
    OUTBOX_POLL_INTERVAL: float = Field(default=1, ge=0.05, le=60)
    SCHEDULER_POLL_INTERVAL: float = Field(default=1, ge=0.05, le=60)
    MAX_WEBHOOK_BODY_BYTES: int = Field(default=1_048_576, ge=1024, le=16_777_216)
    SSRF_ALLOW_PRIVATE_NETWORKS: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
