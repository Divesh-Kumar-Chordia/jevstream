import json
import time
from typing import Optional

from redis.asyncio import Redis

from config import Settings
from models import QueueItem

ENQUEUE_SCRIPT = """
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then
    return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
"""

CLAIM_SCRIPT = """
local top = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #top == 0 then
    return nil
end
local ticket_id = top[1]
local priority = top[2]
redis.call('ZREM', KEYS[1], ticket_id)
local payload = redis.call('HGET', KEYS[3], ticket_id)
if not payload then
    return nil
end
redis.call('ZADD', KEYS[2], ARGV[1], ticket_id)
return {ticket_id, priority, payload}
"""

ACK_SCRIPT = """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
"""

REQUEUE_SCRIPT = """
local payload = redis.call('HGET', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
if not payload then
    return 0
end
local item = cjson.decode(payload)
redis.call('ZADD', KEYS[1], item['judgment']['urgency'], ARGV[1])
return 1
"""

RECOVER_SCRIPT = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local recovered = 0
for _, ticket_id in ipairs(expired) do
    local payload = redis.call('HGET', KEYS[3], ticket_id)
    redis.call('ZREM', KEYS[2], ticket_id)
    if payload then
        local item = cjson.decode(payload)
        redis.call('ZADD', KEYS[1], item['judgment']['urgency'], ticket_id)
        recovered = recovered + 1
    end
end
return recovered
"""


class PriorityQueue:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.active_key = settings.queue_key
        self.processing_key = settings.processing_key
        self.payload_key = settings.payload_key
        self.lease_seconds = settings.claim_lease_seconds

    async def enqueue(self, item: QueueItem) -> bool:
        return bool(
            await self.redis.eval(
                ENQUEUE_SCRIPT,
                2,
                self.active_key,
                self.payload_key,
                item.ticket_id,
                item.judgment.urgency,
                item.model_dump_json(),
            )
        )

    async def claim(self) -> Optional[QueueItem]:
        lease_deadline_ms = int((time.time() + self.lease_seconds) * 1000)
        result = await self.redis.eval(
            CLAIM_SCRIPT,
            3,
            self.active_key,
            self.processing_key,
            self.payload_key,
            lease_deadline_ms,
        )
        if not result:
            return None
        return QueueItem.model_validate_json(result[2])

    async def acknowledge(self, ticket_id: str) -> None:
        await self.redis.eval(ACK_SCRIPT, 2, self.processing_key, self.payload_key, ticket_id)

    async def requeue(self, ticket_id: str) -> None:
        await self.redis.eval(
            REQUEUE_SCRIPT,
            3,
            self.active_key,
            self.processing_key,
            self.payload_key,
            ticket_id,
        )

    async def recover_expired(self, batch_size: int = 100) -> int:
        return int(
            await self.redis.eval(
                RECOVER_SCRIPT,
                3,
                self.active_key,
                self.processing_key,
                self.payload_key,
                int(time.time() * 1000),
                batch_size,
            )
        )