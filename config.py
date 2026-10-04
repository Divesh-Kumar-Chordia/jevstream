from functools import lru_cache
from typing import Literal, Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    judge_mode: Literal["mock", "http"] = "mock"
    judge_api_url: Optional[str] = None
    judge_api_key: Optional[str] = None
    judge_timeout_seconds: float = Field(default=2.0, gt=0)
    queue_key: str = "jevstream:active"
    processing_key: str = "jevstream:processing"
    payload_key: str = "jevstream:payloads"
    leases_key: str = "jevstream:leases"
    auto_route_threshold: float = Field(default=0.85, ge=0, le=1)
    claim_lease_seconds: int = Field(default=30, gt=0)
    worker_poll_interval_seconds: float = Field(default=0.25, gt=0)

    @model_validator(mode="after")
    def validate_http_judge(self) -> "Settings":
        if self.judge_mode == "http" and not self.judge_api_url:
            raise ValueError("JUDGE_API_URL is required when JUDGE_MODE=http")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()