from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Survey Chatbot"
    app_env: str = "development"
    debug: bool = True
    host: str = "0.0.0.0"
    port: int = 8000
    database_url: str = Field(
        default="postgresql+asyncpg://survey:survey@localhost:5432/survey_chat"
    )
    min_sample_warning_threshold: int = 15
    examples_limit: int = 10
    chat_rate_limit_requests: int = 10
    chat_rate_limit_window_seconds: int = 60
    openai_api_key: str | None = None
    openai_model: str = "gpt-5.3-chat-latest"
    openai_base_url: str = "https://api.openai.com/v1"
    openai_timeout_seconds: float = 45.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("debug", mode="before")
    @classmethod
    def parse_debug(cls, value):
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "on", "debug"}:
            return True
        if normalized in {"0", "false", "no", "off", "release", ""}:
            return False
        return False

    @field_validator("database_url", mode="before")
    @classmethod
    def normalize_database_url(cls, value):
        if value is None:
            return value
        database_url = str(value).strip()
        if database_url.startswith("postgresql+asyncpg://"):
            return database_url
        if database_url.startswith("postgres://"):
            return "postgresql+asyncpg://" + database_url.split("://", 1)[1]
        if database_url.startswith("postgresql://"):
            return "postgresql+asyncpg://" + database_url.split("://", 1)[1]
        return database_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
