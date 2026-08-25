import asyncio
import json
import socket
import uuid
from typing import cast

from redis.asyncio import Redis

from .config import get_settings
from .database import SessionFactory
from .executor import claim_node, resume_delay, run_claimed_node
from .models import Worker
from .queue import QUEUE_NAME, heartbeat, recover_stale_nodes


async def heartbeat_loop(worker_id: uuid.UUID) -> None:
    settings = get_settings()
    while True:
        async with SessionFactory() as db:
            await heartbeat(db, worker_id, 0)
            await recover_stale_nodes(db, settings.WORKER_STALE_AFTER)
        await asyncio.sleep(settings.WORKER_HEARTBEAT_INTERVAL)


async def run() -> None:
    settings = get_settings()
    worker_id = uuid.uuid4()
    async with SessionFactory() as db:
        db.add(Worker(id=worker_id, hostname=socket.gethostname()))
        await db.commit()
    redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    heartbeat_task = asyncio.create_task(heartbeat_loop(worker_id))
    try:
        while True:
            raw_job = await redis.lpop(QUEUE_NAME)
            if raw_job is None:
                await asyncio.sleep(0.25)
                continue
            try:
                job = json.loads(cast(str | bytes | bytearray, raw_job))
                job_type = job.get("type")
                if job_type not in {"node.ready", "node.resume"}:
                    continue
                node_execution_id = uuid.UUID(job["node_execution_id"])
            except (ValueError, KeyError, TypeError, json.JSONDecodeError):
                continue
            if job_type == "node.resume":
                async with SessionFactory() as db:
                    await resume_delay(db, node_execution_id)
                continue
            async with SessionFactory() as db:
                claimed = await claim_node(db, node_execution_id, worker_id)
            if claimed is None:
                continue
            async with SessionFactory() as db:
                await run_claimed_node(db, node_execution_id)
    finally:
        heartbeat_task.cancel()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(run())
