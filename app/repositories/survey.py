from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Aggregate, OpenTopic


async def fetch_aggregate_rows(
    session: AsyncSession,
    metric_key: str,
    segment_key: str = "all",
) -> list[Aggregate]:
    result = await session.execute(
        select(Aggregate)
        .where(Aggregate.metric_key == metric_key)
        .where(Aggregate.segment_key == segment_key)
        .order_by(Aggregate.value_numeric.desc().nullslast(), Aggregate.answer_label.asc())
    )
    return list(result.scalars())


async def fetch_open_topics(
    session: AsyncSession,
    topic_key: str,
    segment_key: str = "all",
) -> list[OpenTopic]:
    result = await session.execute(
        select(OpenTopic)
        .where(OpenTopic.topic_key == topic_key)
        .where(OpenTopic.segment_key == segment_key)
        .order_by(OpenTopic.mention_count.desc(), OpenTopic.topic_label.asc())
    )
    return list(result.scalars())
