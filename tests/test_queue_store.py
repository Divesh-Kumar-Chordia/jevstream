import asyncio

from config import Settings
from models import Judgment, QueueItem
from queue_store import ACK_SCRIPT, CLAIM_SCRIPT, RECOVER_SCRIPT, REQUEUE_SCRIPT, PriorityQueue


class RecordingRedis:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def eval(self, *args):
        self.calls.append(args)
        return self.result


def make_item() -> QueueItem:
    return QueueItem(
        ticket_id="ticket-1",
        text="Example ticket",
        judgment=Judgment(urgency=0.9, department="technical"),
    )


def test_claim_returns_fencing_token_and_uses_configured_lease() -> None:
    item = make_item()
    redis = RecordingRedis([item.ticket_id, "0.9", item.model_dump_json(), "claim-token"])
    settings = Settings(claim_lease_seconds=45)

    claim = asyncio.run(PriorityQueue(redis, settings).claim())

    assert claim is not None
    assert claim.item.ticket_id == item.ticket_id
    assert claim.token == "claim-token"
    call = redis.calls[0]
    assert call[1] == 4
    assert call[2:6] == (
        settings.queue_key,
        settings.processing_key,
        settings.payload_key,
        settings.leases_key,
    )
    assert call[6] == 45
    assert "redis.call('TIME')" in CLAIM_SCRIPT


def test_stale_claim_cannot_acknowledge_or_requeue() -> None:
    redis = RecordingRedis(0)
    queue = PriorityQueue(redis, Settings())

    acknowledged = asyncio.run(queue.acknowledge("ticket-1", "stale-token"))
    requeued = asyncio.run(queue.requeue("ticket-1", "stale-token"))

    assert not acknowledged
    assert not requeued
    assert "HGET', KEYS[3], ARGV[1]) ~= ARGV[2]" in ACK_SCRIPT
    assert "HGET', KEYS[4], ARGV[1]) ~= ARGV[2]" in REQUEUE_SCRIPT


def test_recovery_uses_redis_time_and_clears_lease_token() -> None:
    assert "redis.call('TIME')" in RECOVER_SCRIPT
    assert "'-inf', now_ms, 'LIMIT', 0, ARGV[1]" in RECOVER_SCRIPT
    assert "HDEL', KEYS[4], ticket_id" in RECOVER_SCRIPT