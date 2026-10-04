from typing import Protocol

import httpx

from config import Settings
from models import Judgment


class Judge(Protocol):
    async def evaluate(self, text: str) -> Judgment: ...


class MockJudge:
    """Deterministic local judge for development; not an AI model."""

    async def evaluate(self, text: str) -> Judgment:
        normalized = text.casefold()
        urgent_terms = ("urgent", "locked", "losing", "down", "outage", "immediately", "fraud")
        urgency = 0.92 if any(term in normalized for term in urgent_terms) else 0.42

        if any(
            term in normalized
            for term in ("account locked", "account is locked", "security", "fraud", "breach")
        ):
            department = "security"
        elif any(term in normalized for term in ("bill", "billing", "refund", "payment", "invoice")):
            department = "billing"
        else:
            department = "technical"

        return Judgment(urgency=urgency, department=department)


class HttpJudge:
    """Adapter for an HTTP endpoint implementing JevStream's documented JSON contract."""

    def __init__(self, settings: Settings) -> None:
        headers = {}
        if settings.judge_api_key:
            headers["Authorization"] = f"Bearer {settings.judge_api_key}"
        self.client = httpx.AsyncClient(
            base_url=settings.judge_api_url,
            headers=headers,
            timeout=settings.judge_timeout_seconds,
        )

    async def evaluate(self, text: str) -> Judgment:
        response = await self.client.post(
            "",
            json={
                "text": text,
                "questions": {
                    "urgency": {"type": "score", "minimum": 0, "maximum": 1},
                    "department": {
                        "type": "choice",
                        "options": ["billing", "technical", "security"],
                    },
                },
            },
        )
        response.raise_for_status()
        return Judgment.model_validate(response.json())

    async def close(self) -> None:
        await self.client.aclose()


def create_judge(settings: Settings) -> Judge:
    if settings.judge_mode == "http":
        return HttpJudge(settings)
    return MockJudge()