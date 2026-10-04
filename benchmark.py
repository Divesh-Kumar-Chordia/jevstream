import argparse
import asyncio
from collections import Counter
import json
import math
import time
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError

from config import get_settings


def percentile_ms(samples: List[float], percentile: float) -> Optional[float]:
    if not samples:
        return None
    ordered = sorted(samples)
    index = min(len(ordered) - 1, max(0, math.ceil(percentile * len(ordered)) - 1))
    return round(ordered[index] * 1000, 2)


async def post_ticket(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    index: int,
) -> Tuple[float, Optional[int], Optional[str]]:
    payload = {
        "ticket_id": f"bench-{uuid4().hex}",
        "text": "Benchmark event {}: routine account question".format(index),
    }
    started = time.perf_counter()
    async with semaphore:
        try:
            response = await client.post("", json=payload)
            return time.perf_counter() - started, response.status_code, None
        except httpx.HTTPError as exc:
            return time.perf_counter() - started, None, type(exc).__name__


async def read_queue_state() -> Dict[str, Any]:
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await redis.ping()
        pipe = redis.pipeline(transaction=False)
        pipe.zcard(settings.queue_key)
        pipe.zcard(settings.processing_key)
        pipe.hlen(settings.payload_key)
        pipe.hlen(settings.leases_key)
        active, processing, payloads, leases = await pipe.execute()
        return {
            "active_tickets": active,
            "processing_tickets": processing,
            "payloads": payloads,
            "lease_tokens": leases,
        }
    except RedisError as exc:
        return {"available": False, "error": type(exc).__name__}
    finally:
        await redis.aclose()


async def run_benchmark(url: str, requests: int, concurrency: int, timeout: float) -> Dict[str, Any]:
    semaphore = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency)
    async with httpx.AsyncClient(base_url=url, timeout=timeout, limits=limits) as client:
        started = time.perf_counter()
        results = await asyncio.gather(
            *(post_ticket(client, semaphore, index) for index in range(requests))
        )
        elapsed = time.perf_counter() - started

    latencies = [duration for duration, _, _ in results]
    statuses = Counter(str(status) for _, status, _ in results if status is not None)
    errors = Counter(error for _, _, error in results if error is not None)
    successful = sum(count for status, count in statuses.items() if 200 <= int(status) < 300)
    return {
        "url": url,
        "requests": requests,
        "concurrency": concurrency,
        "elapsed_seconds": round(elapsed, 3),
        "successful_requests_per_second": round(successful / elapsed, 2) if elapsed else 0,
        "successful_responses": successful,
        "status_codes": dict(statuses),
        "request_errors": dict(errors),
        "latency_ms": {
            "p50": percentile_ms(latencies, 0.50),
            "p95": percentile_ms(latencies, 0.95),
            "max": round(max(latencies) * 1000, 2) if latencies else None,
        },
        "redis_queue_state": await read_queue_state(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send a concurrent burst of test tickets to JevStream.")
    parser.add_argument("--url", default="http://127.0.0.1:8000/ingest", help="Ingestion endpoint URL")
    parser.add_argument("--requests", type=int, default=500, help="Number of unique tickets to send")
    parser.add_argument("--concurrency", type=int, default=100, help="Maximum concurrent HTTP requests")
    parser.add_argument("--timeout", type=float, default=10.0, help="Per-request timeout in seconds")
    args = parser.parse_args()
    if args.requests < 1 or args.concurrency < 1 or args.timeout <= 0:
        parser.error("requests, concurrency, and timeout must be positive")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    report = asyncio.run(
        run_benchmark(arguments.url, arguments.requests, arguments.concurrency, arguments.timeout)
    )
    print(json.dumps(report, indent=2, sort_keys=True))