from sqlalchemy import Boolean, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Response(Base):
    __tablename__ = "responses"

    response_id: Mapped[str] = mapped_column(Text, primary_key=True)
    submitted_at: Mapped[str | None] = mapped_column(Text)
    role_raw: Mapped[str | None] = mapped_column(Text, index=True)
    role_group: Mapped[str | None] = mapped_column(String(128), index=True)
    experience_group: Mapped[str | None] = mapped_column(String(128), index=True)
    company_size_group: Mapped[str | None] = mapped_column(String(128))
    company_type: Mapped[str | None] = mapped_column(String(128))
    ai_usage_frequency: Mapped[str | None] = mapped_column(String(128))
    ai_usage_frequency_score: Mapped[float | None] = mapped_column(Float)
    ai_tools_used: Mapped[str | None] = mapped_column(Text)
    ai_effectiveness: Mapped[float | None] = mapped_column(Float)
    ai_investment_level: Mapped[float | None] = mapped_column(Float)
    ai_company_sentiment: Mapped[float | None] = mapped_column(Float)
    ai_effectiveness_comment: Mapped[str | None] = mapped_column(Text)
    employment_status: Mapped[str | None] = mapped_column(String(128))
    role_confidence: Mapped[float | None] = mapped_column(Float)
    confidence_uncertainty_source: Mapped[str | None] = mapped_column(Text)
    ai_replacement_risk: Mapped[float | None] = mapped_column(Float)
    future_actions_considered: Mapped[str | None] = mapped_column(Text)
    future_actions_priority: Mapped[str | None] = mapped_column(Text)
    skills_to_develop: Mapped[str | None] = mapped_column(Text)
    role_future_outlook: Mapped[str | None] = mapped_column(Text)


class QuestionMetadata(Base):
    __tablename__ = "question_metadata"

    field_name: Mapped[str] = mapped_column(Text, primary_key=True)
    original_column_name: Mapped[str] = mapped_column(Text)
    question_text: Mapped[str] = mapped_column(Text)
    question_type: Mapped[str] = mapped_column(String(64))
    scale_min: Mapped[float | None] = mapped_column(Float)
    scale_max: Mapped[float | None] = mapped_column(Float)
    allowed_values: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)


class Aggregate(Base):
    __tablename__ = "aggregates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    metric_name: Mapped[str] = mapped_column(String(255), index=True)
    segment_type: Mapped[str] = mapped_column(String(255), index=True)
    segment_value: Mapped[str] = mapped_column(String(255), index=True)
    subsegment_type: Mapped[str | None] = mapped_column(String(255))
    subsegment_value: Mapped[str | None] = mapped_column(String(255))
    value: Mapped[float | None] = mapped_column(Float)
    value_type: Mapped[str] = mapped_column(String(64))
    n: Mapped[int] = mapped_column(Integer)
    small_sample_warning: Mapped[bool] = mapped_column(Boolean, default=False)
    notes: Mapped[str | None] = mapped_column(Text)


class Correlation(Base):
    __tablename__ = "correlations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    x_metric: Mapped[str] = mapped_column(String(255), index=True)
    y_metric: Mapped[str] = mapped_column(String(255), index=True)
    group_name: Mapped[str] = mapped_column(String(255), index=True)
    correlation_type: Mapped[str] = mapped_column(String(64))
    correlation_value: Mapped[float | None] = mapped_column(Float)
    p_value: Mapped[float | None] = mapped_column(Float)
    is_significant: Mapped[bool] = mapped_column(Boolean, default=False)
    plain_language_summary: Mapped[str] = mapped_column(Text)
    n: Mapped[int] = mapped_column(Integer)
    notes: Mapped[str | None] = mapped_column(Text)


class OpenTopic(Base):
    __tablename__ = "open_topics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    response_id: Mapped[str] = mapped_column(Text, index=True)
    question_field: Mapped[str] = mapped_column(String(255), index=True)
    role_group: Mapped[str | None] = mapped_column(String(128), index=True)
    experience_group: Mapped[str | None] = mapped_column(String(128), index=True)
    topic_name: Mapped[str] = mapped_column(String(255), index=True)
    topic_group: Mapped[str] = mapped_column(String(255), index=True)
    quote_short: Mapped[str] = mapped_column(Text)
    quote_full: Mapped[str] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)
