import json
from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

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
local payload = redis.call('HGET', KEYS[3], ticket_id)
if not payload then
    redis.call('ZREM', KEYS[1], ticket_id)
    return nil
end
local now = redis.call('TIME')
local deadline_ms = now[1] * 1000 + math.floor(now[2] / 1000) + ARGV[1] * 1000
redis.call('ZREM', KEYS[1], ticket_id)
redis.call('ZADD', KEYS[2], deadline_ms, ticket_id)
redis.call('HSET', KEYS[4], ticket_id, ARGV[2])
return {ticket_id, priority, payload, ARGV[2]}
"""

ACK_SCRIPT = """
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[2] then
    return 0
end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
return 1
"""

REQUEUE_SCRIPT = """
if redis.call('HGET', KEYS[4], ARGV[1]) ~= ARGV[2] then
    return 0
end
local payload = redis.call('HGET', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
if not payload then
    return 0
end
local item = cjson.decode(payload)
redis.call('ZADD', KEYS[1], item['judgment']['urgency'], ARGV[1])
return 1
"""

RECOVER_SCRIPT = """
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms, 'LIMIT', 0, ARGV[1])
local recovered = 0
for _, ticket_id in ipairs(expired) do
    local payload = redis.call('HGET', KEYS[3], ticket_id)
    redis.call('ZREM', KEYS[2], ticket_id)
    redis.call('HDEL', KEYS[4], ticket_id)
    if payload then
        local item = cjson.decode(payload)
        redis.call('ZADD', KEYS[1], item['judgment']['urgency'], ticket_id)
        recovered = recovered + 1
    end
end
return recovered
"""


@dataclass(frozen=True)
class QueueClaim:
    item: QueueItem
    token: str


class PriorityQueue:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.active_key = settings.queue_key
        self.processing_key = settings.processing_key
        self.payload_key = settings.payload_key
        self.leases_key = settings.leases_key
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

    async def claim(self) -> Optional[QueueClaim]:
        token = uuid4().hex
        result = await self.redis.eval(
            CLAIM_SCRIPT,
            4,
            self.active_key,
            self.processing_key,
            self.payload_key,
            self.leases_key,
            self.lease_seconds,
            token,
        )
        if not result:
            return None
        return QueueClaim(item=QueueItem.model_validate_json(result[2]), token=result[3])

    async def acknowledge(self, ticket_id: str, token: str) -> bool:
        return bool(
            await self.redis.eval(
                ACK_SCRIPT,
                3,
                self.processing_key,
                self.payload_key,
                self.leases_key,
                ticket_id,
                token,
            )
        )

    async def requeue(self, ticket_id: str, token: str) -> bool:
        return bool(
            await self.redis.eval(
                REQUEUE_SCRIPT,
                4,
                self.active_key,
                self.processing_key,
                self.payload_key,
                self.leases_key,
                ticket_id,
                token,
            )
        )

    async def recover_expired(self, batch_size: int = 100) -> int:
        return int(
            await self.redis.eval(
                RECOVER_SCRIPT,
                4,
                self.active_key,
                self.processing_key,
                self.payload_key,
                self.leases_key,
                batch_size,
            )
        )