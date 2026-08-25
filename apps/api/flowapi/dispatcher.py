import asyncio

from redis.asyncio import Redis

from .config import get_settings
from .database import SessionFactory
from .queue import dispatch_batch


async def run() -> None:
    settings = get_settings()
    redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    try:
        while True:
            async with SessionFactory() as db:
                processed = await dispatch_batch(db, redis)
            if processed == 0:
                await asyncio.sleep(settings.OUTBOX_POLL_INTERVAL)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(run())
