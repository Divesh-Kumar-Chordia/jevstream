from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class IngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ticket_id: str = Field(default_factory=lambda: str(uuid4()), min_length=1, max_length=128)
    text: str = Field(min_length=1, max_length=20_000)


class Judgment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    urgency: float = Field(ge=0, le=1)
    department: Literal["billing", "technical", "security"]


class QueueItem(BaseModel):
    ticket_id: str
    text: str
    judgment: Judgment
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class IngestResponse(BaseModel):
    ticket_id: str
    urgency: float
    department: str
    destination: Literal["automation", "human"]
    queued: bool