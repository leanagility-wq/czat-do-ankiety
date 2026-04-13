from typing import Literal

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(min_length=3, max_length=500)


class ChatResponse(BaseModel):
    answer: str
    answer_type: Literal["sql", "open_topics", "refusal", "no_data"]
    source: str
    warning: str | None = None
    matched_example: str | None = None


class RoutedQuestion(BaseModel):
    route: Literal["sql", "open_topics", "refusal"]
    metric_key: str | None = None
    topic_key: str | None = None
    segment_key: str = "all"
    matched_example: str | None = None


class ExampleQuestion(BaseModel):
    question: str


class HealthResponse(BaseModel):
    status: str
    app_name: str
