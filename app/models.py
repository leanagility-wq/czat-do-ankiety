from sqlalchemy import Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Response(Base):
    __tablename__ = "responses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    respondent_id: Mapped[str] = mapped_column(String(64), index=True)
    role: Mapped[str | None] = mapped_column(String(128), index=True)
    question_code: Mapped[str] = mapped_column(String(128), index=True)
    question_text: Mapped[str] = mapped_column(Text)
    answer_text: Mapped[str | None] = mapped_column(Text)
    answer_numeric: Mapped[float | None] = mapped_column(Float)


class Aggregate(Base):
    __tablename__ = "aggregates"
    __table_args__ = (
        UniqueConstraint(
            "metric_key",
            "segment_key",
            "answer_key",
            name="uq_aggregate_metric_segment_answer",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    metric_key: Mapped[str] = mapped_column(String(128), index=True)
    metric_label: Mapped[str] = mapped_column(String(255))
    segment_key: Mapped[str] = mapped_column(String(128), default="all", index=True)
    segment_label: Mapped[str] = mapped_column(String(255), default="Wszyscy badani")
    answer_key: Mapped[str] = mapped_column(String(128), default="value", index=True)
    answer_label: Mapped[str] = mapped_column(String(255))
    value_numeric: Mapped[float | None] = mapped_column(Float)
    value_text: Mapped[str | None] = mapped_column(Text)
    sample_size: Mapped[int] = mapped_column(Integer, default=0)


class OpenTopic(Base):
    __tablename__ = "open_topics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic_key: Mapped[str] = mapped_column(String(128), index=True)
    topic_label: Mapped[str] = mapped_column(String(255))
    segment_key: Mapped[str] = mapped_column(String(128), default="all", index=True)
    segment_label: Mapped[str] = mapped_column(String(255), default="Wszyscy badani")
    quote_text: Mapped[str | None] = mapped_column(Text)
    quote_source: Mapped[str | None] = mapped_column(String(255))
    mention_count: Mapped[int] = mapped_column(Integer, default=0)
    sample_size: Mapped[int] = mapped_column(Integer, default=0)


class QuestionMetadata(Base):
    __tablename__ = "question_metadata"

    question_code: Mapped[str] = mapped_column(String(128), primary_key=True)
    question_text: Mapped[str] = mapped_column(Text)
    question_type: Mapped[str] = mapped_column(String(64))
    description: Mapped[str | None] = mapped_column(Text)
    allowed_operations: Mapped[str | None] = mapped_column(String(255))
