import asyncio
import csv
from pathlib import Path

from sqlalchemy import delete

from app.db import AsyncSessionLocal, Base, engine
from app.models import Aggregate, OpenTopic, QuestionMetadata, Response


DATA_DIR = Path("data/input")


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


async def import_all() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        await session.execute(delete(Response))
        await session.execute(delete(Aggregate))
        await session.execute(delete(OpenTopic))
        await session.execute(delete(QuestionMetadata))

        for row in load_csv(DATA_DIR / "responses.csv"):
            session.add(
                Response(
                    respondent_id=row["respondent_id"],
                    role=row.get("role") or None,
                    question_code=row["question_code"],
                    question_text=row["question_text"],
                    answer_text=row.get("answer_text") or None,
                    answer_numeric=float(row["answer_numeric"])
                    if row.get("answer_numeric")
                    else None,
                )
            )

        for row in load_csv(DATA_DIR / "aggregates.csv"):
            session.add(
                Aggregate(
                    metric_key=row["metric_key"],
                    metric_label=row["metric_label"],
                    segment_key=row.get("segment_key") or "all",
                    segment_label=row.get("segment_label") or "Wszyscy badani",
                    answer_key=row.get("answer_key") or "value",
                    answer_label=row["answer_label"],
                    value_numeric=float(row["value_numeric"])
                    if row.get("value_numeric")
                    else None,
                    value_text=row.get("value_text") or None,
                    sample_size=int(row.get("sample_size") or 0),
                )
            )

        for row in load_csv(DATA_DIR / "open_topics.csv"):
            session.add(
                OpenTopic(
                    topic_key=row["topic_key"],
                    topic_label=row["topic_label"],
                    segment_key=row.get("segment_key") or "all",
                    segment_label=row.get("segment_label") or "Wszyscy badani",
                    quote_text=row.get("quote_text") or None,
                    quote_source=row.get("quote_source") or None,
                    mention_count=int(row.get("mention_count") or 0),
                    sample_size=int(row.get("sample_size") or 0),
                )
            )

        for row in load_csv(DATA_DIR / "question_metadata.csv"):
            session.add(
                QuestionMetadata(
                    question_code=row["question_code"],
                    question_text=row["question_text"],
                    question_type=row["question_type"],
                    description=row.get("description") or None,
                    allowed_operations=row.get("allowed_operations") or None,
                )
            )

        await session.commit()


if __name__ == "__main__":
    asyncio.run(import_all())
