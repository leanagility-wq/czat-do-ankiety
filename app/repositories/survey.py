import json

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Aggregate, Correlation, OpenTopic, QuestionMetadata, Response


async def fetch_question_metadata(session: AsyncSession) -> list[QuestionMetadata]:
    result = await session.execute(
        select(QuestionMetadata).order_by(QuestionMetadata.field_name.asc())
    )
    return list(result.scalars())


def parse_normalization_notes(raw_value: str | None):
    if not raw_value:
        return None
    try:
        return json.loads(raw_value)
    except json.JSONDecodeError:
        return None


async def fetch_catalog(session: AsyncSession) -> dict:
    metadata_rows = await fetch_question_metadata(session)
    role_groups = list(
        (
            await session.execute(
                select(Response.role_group).distinct().where(Response.role_group.is_not(None)).order_by(Response.role_group.asc())
            )
        ).scalars()
    )
    experience_groups = list(
        (
            await session.execute(
                select(Response.experience_group)
                .distinct()
                .where(Response.experience_group.is_not(None))
                .order_by(Response.experience_group.asc())
            )
        ).scalars()
    )

    metric_names = list(
        (
            await session.execute(
                select(Aggregate.metric_name)
                .distinct()
                .order_by(Aggregate.metric_name.asc())
            )
        ).scalars()
    )
    aggregate_segments = list(
        (
            await session.execute(
                select(
                    Aggregate.metric_name,
                    Aggregate.segment_type,
                    Aggregate.segment_value,
                    Aggregate.subsegment_type,
                    Aggregate.subsegment_value,
                )
                .distinct()
                .order_by(
                    Aggregate.metric_name.asc(),
                    Aggregate.segment_type.asc(),
                    Aggregate.segment_value.asc(),
                )
            )
        ).all()
    )
    correlation_pairs = list(
        (
            await session.execute(
                select(
                    Correlation.x_metric,
                    Correlation.y_metric,
                    Correlation.group_name,
                )
                .distinct()
                .order_by(Correlation.group_name.asc(), Correlation.x_metric.asc(), Correlation.y_metric.asc())
            )
        ).all()
    )
    open_topic_fields = list(
        (
            await session.execute(
                select(
                    OpenTopic.question_field,
                    OpenTopic.role_group,
                    OpenTopic.experience_group,
                    OpenTopic.topic_name,
                    func.count(OpenTopic.id),
                )
                .group_by(
                    OpenTopic.question_field,
                    OpenTopic.role_group,
                    OpenTopic.experience_group,
                    OpenTopic.topic_name,
                )
                .order_by(OpenTopic.question_field.asc(), OpenTopic.topic_name.asc())
            )
        ).all()
    )

    return {
        "question_metadata": [
            {
                "field_name": row.field_name,
                "original_column_name": row.original_column_name,
                "question_text": row.question_text,
                "question_type": row.question_type,
                "allowed_values": [
                    item.strip()
                    for item in (row.allowed_values or "").split("|")
                    if item.strip()
                ],
                "notes": row.notes,
                "normalization_notes": parse_normalization_notes(row.normalization_notes),
            }
            for row in metadata_rows
        ],
        "raw_numeric_fields": [
            row.field_name for row in metadata_rows if row.question_type == "scale"
        ],
        "raw_open_text_fields": [
            row.field_name for row in metadata_rows if row.question_type == "open_text"
        ],
        "role_groups": role_groups,
        "experience_groups": experience_groups,
        "response_filter_dimensions": {
            "role_group": role_groups,
            "experience_group": experience_groups,
            "company_type": [
                item.strip()
                for row in metadata_rows
                if row.field_name == "company_type"
                for item in (row.allowed_values or "").split("|")
                if item.strip()
            ],
            "company_size_group": [
                item.strip()
                for row in metadata_rows
                if row.field_name == "company_size_group"
                for item in (row.allowed_values or "").split("|")
                if item.strip()
            ],
            "employment_status": [
                item.strip()
                for row in metadata_rows
                if row.field_name == "employment_status"
                for item in (row.allowed_values or "").split("|")
                if item.strip()
            ],
        },
        "ordered_dimensions": {
            "experience_group": {
                "ordered_values_low_to_high": [
                    value
                    for value in ["1-3 lata", "3-5 lat", "5-10 lat", "10+ lat", "Inne"]
                    if value in experience_groups
                ],
                "semantic_extremes": {
                    "lowest": next((value for value in ["1-3 lata", "Inne"] if value in experience_groups), None),
                    "highest": next((value for value in ["10+ lat", "5-10 lat"] if value in experience_groups), None),
                },
            },
            "ai_usage_frequency": {
                "ordered_values_low_to_high": [
                    value
                    for value in ["Rzadziej niż raz w tygodniu", "Raz na kilka dni", "Codziennie"]
                    if any(
                        row.field_name == "ai_usage_frequency" and value in (row.allowed_values or "")
                        for row in metadata_rows
                    )
                ],
                "semantic_extremes": {
                    "lowest": "Rzadziej niż raz w tygodniu",
                    "highest": "Codziennie",
                },
            },
        },
        "aggregate_metrics": metric_names,
        "aggregate_segments": [
            {
                "metric_name": metric_name,
                "segment_type": segment_type,
                "segment_value": segment_value,
                "subsegment_type": subsegment_type,
                "subsegment_value": subsegment_value,
            }
            for metric_name, segment_type, segment_value, subsegment_type, subsegment_value in aggregate_segments
        ],
        "correlations": [
            {
                "x_metric": x_metric,
                "y_metric": y_metric,
                "group_name": group_name,
            }
            for x_metric, y_metric, group_name in correlation_pairs
        ],
        "open_topics": [
            {
                "question_field": question_field,
                "role_group": role_group,
                "experience_group": experience_group,
                "topic_name": topic_name,
                "count": count,
            }
            for question_field, role_group, experience_group, topic_name, count in open_topic_fields
        ],
    }


async def fetch_aggregate_rows(
    session: AsyncSession,
    metric_name: str,
    segment_type: str | None = None,
    segment_value: str | None = None,
    subsegment_type: str | None = None,
    subsegment_value: str | None = None,
    limit: int = 8,
) -> list[Aggregate]:
    query = select(Aggregate).where(Aggregate.metric_name == metric_name)
    if segment_type is not None:
        query = query.where(Aggregate.segment_type == segment_type)
    if segment_value is not None:
        query = query.where(Aggregate.segment_value == segment_value)
    if subsegment_type is not None:
        query = query.where(Aggregate.subsegment_type == subsegment_type)
    if subsegment_value is not None:
        query = query.where(Aggregate.subsegment_value == subsegment_value)

    result = await session.execute(
        query.order_by(
            Aggregate.segment_value.asc(),
            Aggregate.value.desc().nullslast(),
            Aggregate.subsegment_value.asc().nullslast(),
        ).limit(limit)
    )
    return list(result.scalars())


async def fetch_correlation_row(
    session: AsyncSession,
    x_metric: str,
    y_metric: str,
    group_name: str = "all",
) -> Correlation | None:
    result = await session.execute(
        select(Correlation)
        .where(Correlation.x_metric == x_metric)
        .where(Correlation.y_metric == y_metric)
        .where(Correlation.group_name == group_name)
    )
    return result.scalar_one_or_none()


async def fetch_open_topics(
    session: AsyncSession,
    question_field: str,
    role_group: str | None = None,
    experience_group: str | None = None,
    topic_name: str | None = None,
    limit: int = 8,
) -> list[OpenTopic]:
    query = select(OpenTopic).where(OpenTopic.question_field == question_field)
    if role_group is not None:
        query = query.where(OpenTopic.role_group == role_group)
    if experience_group is not None:
        query = query.where(OpenTopic.experience_group == experience_group)
    if topic_name is not None:
        query = query.where(OpenTopic.topic_name == topic_name)

    result = await session.execute(
        query.order_by(OpenTopic.id.asc()).limit(limit)
    )
    return list(result.scalars())


async def fetch_open_topic_summary(
    session: AsyncSession,
    *,
    question_field: str,
    role_group: str | None = None,
    experience_group: str | None = None,
    limit: int = 12,
) -> dict | None:
    query = select(OpenTopic).where(OpenTopic.question_field == question_field)
    if role_group is not None:
        query = query.where(OpenTopic.role_group == role_group)
    if experience_group is not None:
        query = query.where(OpenTopic.experience_group == experience_group)

    total_rows = await session.execute(
        select(func.count(OpenTopic.id)).where(
            OpenTopic.question_field == question_field,
            *( [OpenTopic.role_group == role_group] if role_group is not None else [] ),
            *( [OpenTopic.experience_group == experience_group] if experience_group is not None else [] ),
        )
    )
    total_topic_rows = int(total_rows.scalar_one() or 0)
    if not total_topic_rows:
        return None

    total_responses_query = select(func.count(func.distinct(OpenTopic.response_id))).where(
        OpenTopic.question_field == question_field
    )
    if role_group is not None:
        total_responses_query = total_responses_query.where(OpenTopic.role_group == role_group)
    if experience_group is not None:
        total_responses_query = total_responses_query.where(OpenTopic.experience_group == experience_group)

    topic_counts_query = (
        select(
            OpenTopic.topic_name,
            OpenTopic.topic_group,
            func.count(OpenTopic.id).label("topic_count"),
            func.count(func.distinct(OpenTopic.response_id)).label("response_count"),
        )
        .where(OpenTopic.question_field == question_field)
        .group_by(OpenTopic.topic_name, OpenTopic.topic_group)
        .order_by(func.count(func.distinct(OpenTopic.response_id)).desc(), OpenTopic.topic_name.asc())
        .limit(limit)
    )
    if role_group is not None:
        topic_counts_query = topic_counts_query.where(OpenTopic.role_group == role_group)
    if experience_group is not None:
        topic_counts_query = topic_counts_query.where(OpenTopic.experience_group == experience_group)

    total_responses = int((await session.execute(total_responses_query)).scalar_one() or 0)
    topic_rows = (await session.execute(topic_counts_query)).all()

    return {
        "question_field": question_field,
        "role_group": role_group,
        "experience_group": experience_group,
        "total_topic_rows": total_topic_rows,
        "total_responses": total_responses,
        "top_topics": [
            {
                "topic_name": topic_name,
                "topic_group": topic_group,
                "topic_count": int(topic_count),
                "response_count": int(response_count),
            }
            for topic_name, topic_group, topic_count, response_count in topic_rows
        ],
    }


def _apply_response_filters(
    query,
    *,
    role_group: str | None = None,
    experience_group: str | None = None,
    company_type: str | None = None,
    company_size_group: str | None = None,
    employment_status: str | None = None,
):
    if role_group is not None:
        query = query.where(Response.role_group == role_group)
    if experience_group is not None:
        query = query.where(Response.experience_group == experience_group)
    if company_type is not None:
        query = query.where(Response.company_type == company_type)
    if company_size_group is not None:
        query = query.where(Response.company_size_group == company_size_group)
    if employment_status is not None:
        query = query.where(Response.employment_status == employment_status)
    return query


async def fetch_numeric_summary(
    session: AsyncSession,
    *,
    field_name: str,
    role_group: str | None = None,
    experience_group: str | None = None,
    company_type: str | None = None,
    company_size_group: str | None = None,
    employment_status: str | None = None,
) -> dict | None:
    column = getattr(Response, field_name, None)
    if column is None:
        return None

    query = select(
        func.count(column),
        func.avg(column),
        func.min(column),
        func.max(column),
    ).where(column.is_not(None))
    query = _apply_response_filters(
        query,
        role_group=role_group,
        experience_group=experience_group,
        company_type=company_type,
        company_size_group=company_size_group,
        employment_status=employment_status,
    )
    count_value, avg_value, min_value, max_value = (await session.execute(query)).one()
    if not count_value:
        return None
    return {
        "field_name": field_name,
        "n": int(count_value),
        "mean": float(avg_value) if avg_value is not None else None,
        "min": float(min_value) if min_value is not None else None,
        "max": float(max_value) if max_value is not None else None,
        "filters": {
            "role_group": role_group,
            "experience_group": experience_group,
            "company_type": company_type,
            "company_size_group": company_size_group,
            "employment_status": employment_status,
        },
    }


async def fetch_categorical_distribution(
    session: AsyncSession,
    *,
    field_name: str,
    role_group: str | None = None,
    experience_group: str | None = None,
    company_type: str | None = None,
    company_size_group: str | None = None,
    employment_status: str | None = None,
    limit: int = 8,
) -> list[dict]:
    column = getattr(Response, field_name, None)
    if column is None:
        return []

    query = (
        select(column.label("value"), func.count(column).label("count"))
        .where(column.is_not(None))
        .where(column != "")
        .group_by(column)
    )
    query = _apply_response_filters(
        query,
        role_group=role_group,
        experience_group=experience_group,
        company_type=company_type,
        company_size_group=company_size_group,
        employment_status=employment_status,
    )
    query = query.order_by(func.count(column).desc(), column.asc()).limit(limit)
    rows = (await session.execute(query)).all()
    total_n = sum(int(count) for _, count in rows)
    return [
        {
            "field_name": field_name,
            "value": value,
            "count": int(count),
            "share": (int(count) / total_n) if total_n else 0.0,
            "n": total_n,
            "filters": {
                "role_group": role_group,
                "experience_group": experience_group,
                "company_type": company_type,
                "company_size_group": company_size_group,
                "employment_status": employment_status,
            },
        }
        for value, count in rows
    ]


async def fetch_text_responses(
    session: AsyncSession,
    *,
    field_name: str,
    role_group: str | None = None,
    experience_group: str | None = None,
    company_type: str | None = None,
    company_size_group: str | None = None,
    employment_status: str | None = None,
    sort_by: str = "length_desc",
    limit: int = 5,
) -> list[dict]:
    column = getattr(Response, field_name, None)
    if column is None:
        return []

    query = select(
        Response.response_id,
        Response.role_group,
        Response.experience_group,
        Response.submitted_at,
        column.label("quote_full"),
    ).where(column.is_not(None)).where(column != "")
    query = _apply_response_filters(
        query,
        role_group=role_group,
        experience_group=experience_group,
        company_type=company_type,
        company_size_group=company_size_group,
        employment_status=employment_status,
    )

    if sort_by == "length_asc":
        query = query.order_by(func.length(column).asc())
    elif sort_by == "newest":
        query = query.order_by(Response.submitted_at.desc().nullslast())
    elif sort_by == "oldest":
        query = query.order_by(Response.submitted_at.asc().nullslast())
    else:
        query = query.order_by(func.length(column).desc())

    rows = (await session.execute(query.limit(limit))).all()
    return [
        {
            "response_id": response_id,
            "role_group": role_group_value,
            "experience_group": experience_group_value,
            "submitted_at": submitted_at.isoformat() if hasattr(submitted_at, "isoformat") else submitted_at,
            "quote_full": quote_full,
            "quote_short": str(quote_full)[:160],
            "length": len(str(quote_full)),
            "field_name": field_name,
        }
        for response_id, role_group_value, experience_group_value, submitted_at, quote_full in rows
    ]
