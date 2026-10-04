from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, HTTPException, Request
from redis.asyncio import Redis
from redis.exceptions import RedisError

from config import get_settings
from judge import HttpJudge, create_judge
from models import IngestRequest, IngestResponse, QueueItem
from queue_store import PriorityQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("jevstream.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    await redis.ping()
    app.state.settings = settings
    app.state.redis = redis
    app.state.queue = PriorityQueue(redis, settings)
    app.state.judge = create_judge(settings)
    try:
        yield
    finally:
        if isinstance(app.state.judge, HttpJudge):
            await app.state.judge.close()
        await redis.aclose()


app = FastAPI(title="JevStream", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    try:
        await request.app.state.redis.ping()
    except RedisError as exc:
        raise HTTPException(status_code=503, detail="Redis unavailable") from exc
    return {"status": "ok"}


@app.post("/ingest", response_model=IngestResponse, status_code=202)
async def ingest(payload: IngestRequest, request: Request) -> IngestResponse:
    try:
        judgment = await request.app.state.judge.evaluate(payload.text)
    except Exception as exc:
        logger.exception("Judgment failed for ticket %s", payload.ticket_id)
        raise HTTPException(status_code=502, detail="Judgment provider failed") from exc

    item = QueueItem(ticket_id=payload.ticket_id, text=payload.text, judgment=judgment)
    try:
        queued = await request.app.state.queue.enqueue(item)
    except RedisError as exc:
        logger.exception("Queue write failed for ticket %s", payload.ticket_id)
        raise HTTPException(status_code=503, detail="Queue unavailable") from exc
    if not queued:
        raise HTTPException(status_code=409, detail="ticket_id has already been ingested")

    threshold = request.app.state.settings.auto_route_threshold
    return IngestResponse(
        ticket_id=payload.ticket_id,
        urgency=judgment.urgency,
        department=judgment.department,
        destination="automation" if judgment.urgency >= threshold else "human",
        queued=True,
    )