import asyncio
import logging

from redis.asyncio import Redis

from config import get_settings
from models import QueueItem
from queue_store import PriorityQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("jevstream.worker")


async def handle(item: QueueItem, threshold: float) -> None:
    destination = "automation" if item.judgment.urgency >= threshold else "human"
    logger.info(
        "Routed ticket=%s department=%s urgency=%.2f destination=%s",
        item.ticket_id,
        item.judgment.department,
        item.judgment.urgency,
        destination,
    )


async def run() -> None:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    queue = PriorityQueue(redis, settings)
    logger.info("Priority worker started")
    try:
        while True:
            recovered = await queue.recover_expired()
            if recovered:
                logger.warning("Recovered %d expired ticket claim(s)", recovered)

            item = await queue.claim()
            if item is None:
                await asyncio.sleep(settings.worker_poll_interval_seconds)
                continue

            try:
                await handle(item, settings.auto_route_threshold)
            except Exception:
                logger.exception("Ticket handling failed: %s", item.ticket_id)
                await queue.requeue(item.ticket_id)
            else:
                await queue.acknowledge(item.ticket_id)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Priority worker stopped")