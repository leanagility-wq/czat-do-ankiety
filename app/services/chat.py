from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.repositories.survey import fetch_aggregate_rows, fetch_open_topics
from app.schemas import ChatResponse
from app.services.query_router import route_question


NO_DATA_MESSAGE = "Tego nie da się stwierdzić na podstawie tej ankiety."
REFUSAL_MESSAGE = (
    "Mogę odpowiadać wyłącznie na podstawie danych z tej ankiety."
)
settings = get_settings()


def build_small_sample_warning(sample_size: int | None) -> str | None:
    if sample_size is None:
        return None
    if sample_size < settings.min_sample_warning_threshold:
        return f"Uwaga: wynik opiera się na małej próbie (n={sample_size})."
    return None


def render_aggregate_answer(metric_label: str, rows: list) -> str:
    if not rows:
        return NO_DATA_MESSAGE

    fragments: list[str] = []
    for row in rows[:4]:
        value = row.value_text
        if value is None and row.value_numeric is not None:
            value = f"{row.value_numeric:g}"
        fragments.append(f"{row.answer_label}: {value}")
    return f"{metric_label}: " + "; ".join(fragments) + "."


def render_open_topics_answer(segment_label: str, rows: list) -> str:
    if not rows:
        return NO_DATA_MESSAGE

    topics = ", ".join(
        f"{row.topic_label} ({row.mention_count})"
        for row in rows[:3]
    )

    lead = f"Najczęściej wskazywane tematy dla grupy {segment_label}: {topics}."
    quote = next((row.quote_text for row in rows if row.quote_text), None)
    if quote:
        return f'{lead} Przykładowy cytat: "{quote}"'
    return lead


async def answer_question(question: str, session: AsyncSession) -> ChatResponse:
    route = route_question(question)

    if route.route == "refusal":
        return ChatResponse(
            answer=REFUSAL_MESSAGE,
            answer_type="refusal",
            source="guardrails",
        )

    if route.route == "open_topics" and route.topic_key:
        rows = await fetch_open_topics(session, route.topic_key, route.segment_key)
        if not rows:
            return ChatResponse(
                answer=NO_DATA_MESSAGE,
                answer_type="no_data",
                source="open_topics",
                matched_example=route.matched_example,
            )

        return ChatResponse(
            answer=render_open_topics_answer(rows[0].segment_label, rows),
            answer_type="open_topics",
            source="open_topics",
            warning=build_small_sample_warning(rows[0].sample_size),
            matched_example=route.matched_example,
        )

    if route.route == "sql" and route.metric_key:
        rows = await fetch_aggregate_rows(session, route.metric_key, route.segment_key)
        if not rows:
            return ChatResponse(
                answer=NO_DATA_MESSAGE,
                answer_type="no_data",
                source="sql",
                matched_example=route.matched_example,
            )

        return ChatResponse(
            answer=render_aggregate_answer(rows[0].metric_label, rows),
            answer_type="sql",
            source="aggregates",
            warning=build_small_sample_warning(rows[0].sample_size),
            matched_example=route.matched_example,
        )

    return ChatResponse(
        answer=NO_DATA_MESSAGE,
        answer_type="no_data",
        source="guardrails",
        matched_example=route.matched_example,
    )
