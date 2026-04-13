from typing import Literal

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(min_length=3, max_length=1000)


class ChatResponse(BaseModel):
    answer: str
    answer_type: Literal["llm", "sql", "open_topics", "refusal", "no_data", "config_error"]
    source: str
    warning: str | None = None
    matched_example: str | None = None


class ExampleQuestion(BaseModel):
    question: str


class HealthResponse(BaseModel):
    status: str
    app_name: str


class AggregatePlanRequest(BaseModel):
    metric_name: str
    segment_type: str | None = None
    segment_value: str | None = None
    subsegment_type: str | None = None
    subsegment_value: str | None = None
    limit: int = 8


class CorrelationPlanRequest(BaseModel):
    x_metric: str
    y_metric: str
    group_name: str = "all"


class OpenTopicPlanRequest(BaseModel):
    question_field: str
    role_group: str | None = None
    experience_group: str | None = None
    topic_name: str | None = None
    limit: int = 8


class NumericStatsPlanRequest(BaseModel):
    field_name: str
    role_group: str | None = None
    experience_group: str | None = None
    company_type: str | None = None
    company_size_group: str | None = None
    employment_status: str | None = None


class CategoricalStatsPlanRequest(BaseModel):
    field_name: str
    role_group: str | None = None
    experience_group: str | None = None
    company_type: str | None = None
    company_size_group: str | None = None
    employment_status: str | None = None
    limit: int = 8


class TextResponsePlanRequest(BaseModel):
    field_name: str
    role_group: str | None = None
    experience_group: str | None = None
    company_type: str | None = None
    company_size_group: str | None = None
    employment_status: str | None = None
    sort_by: Literal["length_desc", "length_asc", "newest", "oldest"] = "length_desc"
    limit: int = 5


class RetrievalPlan(BaseModel):
    is_in_scope: bool = True
    aggregate_requests: list[AggregatePlanRequest] = Field(default_factory=list)
    correlation_requests: list[CorrelationPlanRequest] = Field(default_factory=list)
    open_topic_requests: list[OpenTopicPlanRequest] = Field(default_factory=list)
    numeric_stats_requests: list[NumericStatsPlanRequest] = Field(default_factory=list)
    categorical_stats_requests: list[CategoricalStatsPlanRequest] = Field(default_factory=list)
    text_response_requests: list[TextResponsePlanRequest] = Field(default_factory=list)
    reasoning: str = ""


class GroundedAnswer(BaseModel):
    answer: str
    insufficient_data: bool = False
    cites_small_sample: bool = False
