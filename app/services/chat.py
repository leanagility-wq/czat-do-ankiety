import json
import re

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models import Aggregate, Correlation, OpenTopic
from app.repositories.survey import (
    fetch_categorical_distribution,
    fetch_aggregate_rows,
    fetch_catalog,
    fetch_correlation_row,
    fetch_numeric_summary,
    fetch_open_topics,
    fetch_open_topic_summary,
    fetch_text_responses,
)
from app.schemas import (
    CategoricalStatsPlanRequest,
    ChatResponse,
    CorrelationPlanRequest,
    GroundedAnswer,
    NumericStatsPlanRequest,
    OpenTopicPlanRequest,
    QuestionMetadataPlanRequest,
    RetrievalPlan,
    TextResponsePlanRequest,
)
from app.services.openai_client import OpenAIClient


NO_DATA_MESSAGE = "Tego nie da się stwierdzić na podstawie tej ankiety."
REFUSAL_MESSAGE = "Mogę odpowiadać wyłącznie na podstawie danych z tej ankiety."
CONFIG_MESSAGE = "Aplikacja nie ma jeszcze skonfigurowanego OPENAI_API_KEY."
settings = get_settings()
openai_client = OpenAIClient()


RETRIEVAL_PLAN_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "is_in_scope": {"type": "boolean"},
        "reasoning": {"type": "string"},
        "question_metadata_requests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "field_name": {"type": ["string", "null"]},
                    "question_type": {"type": ["string", "null"]},
                    "limit": {"type": "integer"},
                },
                "required": ["field_name", "question_type", "limit"],
            },
        },
        "aggregate_requests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "metric_name": {"type": "string"},
                    "segment_type": {"type": ["string", "null"]},
                    "segment_value": {"type": ["string", "null"]},
                    "subsegment_type": {"type": ["string", "null"]},
                    "subsegment_value": {"type": ["string", "null"]},
                    "limit": {"type": "integer"},
                },
                "required": [
                    "metric_name",
                    "segment_type",
                    "segment_value",
                    "subsegment_type",
                    "subsegment_value",
                    "limit",
                ],
            },
        },
        "correlation_requests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "x_metric": {"type": "string"},
                    "y_metric": {"type": "string"},
                    "group_name": {"type": "string"},
                },
                "required": ["x_metric", "y_metric", "group_name"],
            },
        },
        "open_topic_requests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "question_field": {"type": "string"},
                    "role_group": {"type": ["string", "null"]},
                    "experience_group": {"type": ["string", "null"]},
                    "topic_name": {"type": ["string", "null"]},
                    "limit": {"type": "integer"},
                },
                "required": ["question_field", "role_group", "experience_group", "topic_name", "limit"],
            },
        },
        "numeric_stats_requests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "field_name": {"type": "string"},
                    "role_group": {"type": ["string", "null"]},
                    "experience_group": {"type": ["string", "null"]},
                    "company_type": {"type": ["string", "null"]},
                    "company_size_group": {"type": ["string", "null"]},
                    "employment_status": {"type": ["string", "null"]},
                },
                "required": [
                    "field_name",
                    "role_group",
                    "experience_group",
                    "company_type",
                    "company_size_group",
                    "employment_status",
                ],
            },
        },
        "categorical_stats_requests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "field_name": {"type": "string"},
                    "role_group": {"type": ["string", "null"]},
                    "experience_group": {"type": ["string", "null"]},
                    "company_type": {"type": ["string", "null"]},
                    "company_size_group": {"type": ["string", "null"]},
                    "employment_status": {"type": ["string", "null"]},
                    "limit": {"type": "integer"},
                },
                "required": [
                    "field_name",
                    "role_group",
                    "experience_group",
                    "company_type",
                    "company_size_group",
                    "employment_status",
                    "limit",
                ],
            },
        },
        "text_response_requests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "field_name": {"type": "string"},
                    "role_group": {"type": ["string", "null"]},
                    "experience_group": {"type": ["string", "null"]},
                    "company_type": {"type": ["string", "null"]},
                    "company_size_group": {"type": ["string", "null"]},
                    "employment_status": {"type": ["string", "null"]},
                    "sort_by": {"type": "string"},
                    "limit": {"type": "integer"},
                },
                "required": [
                    "field_name",
                    "role_group",
                    "experience_group",
                    "company_type",
                    "company_size_group",
                    "employment_status",
                    "sort_by",
                    "limit",
                ],
            },
        },
    },
    "required": [
        "is_in_scope",
        "reasoning",
        "question_metadata_requests",
        "aggregate_requests",
        "correlation_requests",
        "open_topic_requests",
        "numeric_stats_requests",
        "categorical_stats_requests",
        "text_response_requests",
    ],
}

GROUNDED_ANSWER_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "answer": {"type": "string"},
        "insufficient_data": {"type": "boolean"},
        "cites_small_sample": {"type": "boolean"},
    },
    "required": ["answer", "insufficient_data", "cites_small_sample"],
}


def build_small_sample_warning(sample_size: int | None) -> str | None:
    if sample_size is None:
        return None
    if sample_size < settings.min_sample_warning_threshold:
        return f"Uwaga: wynik opiera się na małej próbie (n={sample_size})."
    return None


def build_planner_system_prompt() -> str:
    return (
        "Jesteś plannerem pobrania danych do chatbota ankietowego. "
        "Twoim zadaniem NIE jest odpowiadać na pytanie. Masz zwrócić tylko plan pobrania danych w JSON. "
        "Możesz używać wyłącznie katalogu danych ankiety przekazanego w promptcie. "
        "Jeśli pytanie wykracza poza ankietę, ustaw is_in_scope=false. "
        "Nie wymyślaj nazw metryk, pól ani grup spoza katalogu. "
        "Używaj nazw pól, filtrów i wartości dokładnie takich, jakie są w katalogu. "
        "Interpretuj pytanie semantycznie na podstawie question_text, a nie tylko literalnych nazw field_name. "
        "Jeżeli użytkownik używa parafraz typu 'staż zawodowy', 'doświadczenie', 'najkrótszy staż', odnoś je do odpowiednich wymiarów i uporządkowanych wartości z katalogu. "
        "Jeżeli użytkownik pyta 'jak często używają narzędzi AI', traktuj to jako pytanie o pole lub metrykę związaną z ai_usage_frequency. "
        "Jeżeli pytanie wygląda na mieszczące się w ankiecie, nie oznaczaj go jako out of scope tylko dlatego, że nie używa dokładnych nazw z katalogu. "
        "Dla pytań o to, jakie były pytania w ankiecie, jakie były skale, jakie były możliwe odpowiedzi albo co obejmował kwestionariusz, używaj question_metadata_requests. "
        "Jeśli użytkownik pyta o pytania otwarte, użyj question_metadata_requests z question_type='open_text'. "
        "Jeśli użytkownik pyta o skale w ankiecie, użyj question_metadata_requests z question_type='scale'. "
        "Jeśli użytkownik pyta o możliwe odpowiedzi, warianty odpowiedzi, kafeterię albo opcje wyboru dla konkretnego pytania, użyj question_metadata_requests dla najlepiej pasującego field_name. "
        "Jeśli użytkownik pyta o pytania zamknięte, zwykle chodzi o question_type='single_choice' lub question_type='multi_choice'. "
        "Dla pytań o zależności używaj correlations. "
        "Dla pytań o liczebności i gotowe agregaty używaj aggregates. "
        "Dla pytań o skale lub pola liczbowe z dodatkowymi filtrami używaj numeric_stats_requests. "
        "Dla pytań o rozkład odpowiedzi w polach kategorycznych z filtrami używaj categorical_stats_requests. "
        "Dla pytań o pełne cytaty, najdłuższe wypowiedzi, przykłady odpowiedzi otwartych i selekcję tekstów używaj text_response_requests. "
        "Dla pytań o tematy, obawy, kompetencje i grupy tematów używaj open_topic_requests. "
        "Jeśli pytanie prosi o cytaty lub pełne wypowiedzi, preferuj text_response_requests nad samym open_topic_requests. "
        "Jeśli pytanie wymaga filtrowania, użyj response_filter_dimensions i allowed_values z katalogu. "
        "Jeśli pytanie używa pojęć względnych typu 'najkrótszy', 'najdłuższy', 'najwyższy', 'najniższy', użyj ordered_dimensions z katalogu."
    )


def build_recovery_planner_system_prompt() -> str:
    return (
        "Jesteś plannerem odzyskującym intencję pytania do chatbota ankietowego. "
        "Masz spróbować jeszcze raz zaplanować pobranie danych WYŁĄCZNIE z katalogu ankiety. "
        "Zakładaj in-scope, jeśli pytanie można rozsądnie powiązać z question_text, allowed_values, ordered_dimensions albo response_filter_dimensions. "
        "Nie wymagaj dosłownego dopasowania słów. "
        "Przykład: 'staż zawodowy' może odnosić się do experience_group, jeśli to jedyny wymiar doświadczenia w katalogu. "
        "Przykład: 'jak często używają narzędzi AI' może odnosić się do ai_usage_frequency lub gotowych agregatów opartych o ai_usage_frequency. "
        "Przykład: 'Jakie były pytania w ankiecie?' powinno użyć question_metadata_requests. "
        "Przykład: 'Jakie były pytania otwarte?' powinno użyć question_metadata_requests z question_type='open_text'. "
        "Przykład: 'Jakie skale były w ankiecie?' powinno użyć question_metadata_requests z question_type='scale'. "
        "Przykład: 'Jakie odpowiedzi były możliwe przy pytaniu o częstotliwość używania AI?' powinno użyć question_metadata_requests dla pola ai_usage_frequency. "
        "Jeśli pytanie naprawdę wykracza poza ankietę, dopiero wtedy ustaw is_in_scope=false."
    )


def build_answer_system_prompt() -> str:
    return (
        "Jesteś analitykiem ankiety. Odpowiadasz wyłącznie na podstawie przekazanego kontekstu z danych ankiety. "
        "Nie wolno Ci dodawać żadnych faktów spoza kontekstu. "
        "Jeśli kontekst nie wystarcza do odpowiedzi, ustaw insufficient_data=true i zwróć dokładnie: "
        f'"{NO_DATA_MESSAGE}". '
        "Jeśli w kontekście widać małą próbę, uwzględnij to w odpowiedzi zwięźle i ostrożnie. "
        "Przy pytaniach otwartych dawaj bogatszą odpowiedź: krótka synteza, 2-4 najważniejsze wnioski i cytaty, jeśli o nie proszono. "
        "Jeśli w kontekście jest open_topic_summary, oprzyj syntezę przede wszystkim na nim i jasno komunikuj, ilu odpowiedzi dotyczy synteza. "
        "Jeśli open_topic_summary zawiera total_responses, nie sugeruj, że synteza opiera się tylko na kilku przykładach, chyba że użytkownik wyraźnie poprosił wyłącznie o przykładowe cytaty. "
        "Jeśli użytkownik prosi o cytat pełny albo najdłuższe wypowiedzi, używaj quote_full i nie skracaj cytatu bez potrzeby. "
        "Przy pytaniach o filtrowane skale, np. rola + doświadczenie, jasno nazwij oba filtry i podaj n oraz średnią, jeśli są dostępne. "
        "Jeśli pytanie dotyczy pytań ankietowych lub struktury kwestionariusza, wypisz je czytelnie w punktach na podstawie question_metadata. "
        "Jeśli pytanie dotyczy skal, pokaż zakres skali przy każdym pytaniu, jeśli jest dostępny. "
        "Jeśli pytanie dotyczy możliwych odpowiedzi, wypisz allowed_values jako listę punktowaną. "
        "Jeśli question_metadata zawiera normalization_notes, pokaż również ważne warianty źródłowe, które zostały znormalizowane do danej odpowiedzi lub skali. "
        "Jeśli pytanie dotyczy tylko pytań otwartych albo tylko pytań zamkniętych, nie mieszaj innych typów. "
        "Formatuj odpowiedzi czytelnie. "
        "Jeśli odpowiedź zawiera więcej niż jedną istotną informację, użyj krótkiego zdania wprowadzającego, pustej linii i listy punktowanej. "
        "Jeśli porównujesz kilka wyników, każdy wynik podaj w osobnym punkcie. "
        "Jeśli cytujesz, każdy cytat umieść w osobnym wierszu poprzedzonym znakiem >. "
        "Unikaj ściany tekstu i długich bloków bez podziału na linie. "
        "Odpowiadaj po polsku, konkretnie, ale nie przesadnie krótko."
    )


def format_answer_text(answer: str) -> str:
    text = (answer or "").strip()
    if not text:
        return text

    if "\n" in text or text.startswith("- ") or text.startswith("> "):
        return text

    sentence_breaks = re.split(r"(?<=[.!?])\s+(?=[A-ZĄĆĘŁŃÓŚŹŻ])", text)
    if len(sentence_breaks) >= 3:
        lead = sentence_breaks[0].strip()
        bullets = [item.strip() for item in sentence_breaks[1:] if item.strip()]
        if bullets:
            return lead + "\n\n" + "\n".join(f"- {item}" for item in bullets)

    clause_breaks = [item.strip() for item in text.split(";") if item.strip()]
    if len(clause_breaks) >= 3:
        lead = clause_breaks[0]
        bullets = clause_breaks[1:]
        return lead + "\n\n" + "\n".join(f"- {item}" for item in bullets)

    return text


def serialize_aggregate_rows(rows: list[Aggregate]) -> list[dict]:
    return [
        {
            "metric_name": row.metric_name,
            "segment_type": row.segment_type,
            "segment_value": row.segment_value,
            "subsegment_type": row.subsegment_type,
            "subsegment_value": row.subsegment_value,
            "value": row.value,
            "value_type": row.value_type,
            "n": row.n,
            "small_sample_warning": row.small_sample_warning,
            "notes": row.notes,
        }
        for row in rows
    ]


def serialize_correlation_row(row: Correlation) -> dict:
    return {
        "x_metric": row.x_metric,
        "y_metric": row.y_metric,
        "group_name": row.group_name,
        "correlation_type": row.correlation_type,
        "correlation_value": row.correlation_value,
        "p_value": row.p_value,
        "is_significant": row.is_significant,
        "plain_language_summary": row.plain_language_summary,
        "n": row.n,
        "notes": row.notes,
    }


def serialize_open_topic_rows(rows: list[OpenTopic]) -> list[dict]:
    return [
        {
            "response_id": row.response_id,
            "question_field": row.question_field,
            "role_group": row.role_group,
            "experience_group": row.experience_group,
            "topic_name": row.topic_name,
            "topic_group": row.topic_group,
            "quote_short": row.quote_short,
            "quote_full": row.quote_full,
            "notes": row.notes,
        }
        for row in rows
    ]


def serialize_question_metadata_rows(rows: list[dict]) -> list[dict]:
    return [
        {
            "field_name": row["field_name"],
            "question_text": row["question_text"],
            "question_type": row["question_type"],
            "allowed_values": row.get("allowed_values", []),
            "scale_min": row.get("scale_min"),
            "scale_max": row.get("scale_max"),
            "notes": row.get("notes"),
            "normalization_notes": row.get("normalization_notes"),
        }
        for row in rows
    ]


def compute_warning_from_context(context_payload: dict) -> str | None:
    sample_sizes: list[int] = []
    for item in context_payload["open_topic_summaries"]:
        if isinstance(item.get("summary", {}).get("total_responses"), int):
            sample_sizes.append(item["summary"]["total_responses"])
    for item in context_payload["aggregates"]:
        for row in item["rows"]:
            if isinstance(row.get("n"), int):
                sample_sizes.append(row["n"])
    for item in context_payload["correlations"]:
        if isinstance(item["row"].get("n"), int):
            sample_sizes.append(item["row"]["n"])
    for item in context_payload["open_topics"]:
        sample_sizes.append(len({row["response_id"] for row in item["rows"]}))
    for item in context_payload["numeric_stats"]:
        if isinstance(item["row"].get("n"), int):
            sample_sizes.append(item["row"]["n"])
    for item in context_payload["categorical_stats"]:
        for row in item["rows"]:
            if isinstance(row.get("n"), int):
                sample_sizes.append(row["n"])
    for item in context_payload["text_responses"]:
        sample_sizes.append(len({row["response_id"] for row in item["rows"]}))
    if not sample_sizes:
        return None
    return build_small_sample_warning(min(sample_sizes))


def build_planner_catalog(catalog: dict) -> dict:
    return {
        "question_metadata": catalog["question_metadata"],
        "response_filter_dimensions": catalog["response_filter_dimensions"],
        "ordered_dimensions": catalog["ordered_dimensions"],
        "raw_numeric_fields": catalog["raw_numeric_fields"],
        "raw_open_text_fields": catalog["raw_open_text_fields"],
        "aggregate_metrics": catalog["aggregate_metrics"],
        "aggregate_segments": catalog["aggregate_segments"],
        "correlations": catalog["correlations"],
        "open_topics": catalog["open_topics"],
    }


async def plan_retrieval(question: str, catalog: dict) -> RetrievalPlan:
    return await _plan_retrieval_with_prompt(question, catalog, build_planner_system_prompt())


async def plan_retrieval_recovery(question: str, catalog: dict) -> RetrievalPlan:
    return await _plan_retrieval_with_prompt(question, catalog, build_recovery_planner_system_prompt())


async def _plan_retrieval_with_prompt(question: str, catalog: dict, system_prompt: str) -> RetrievalPlan:
    planner_catalog = build_planner_catalog(catalog)
    user_prompt = (
        f"Pytanie użytkownika:\n{question}\n\n"
        "Dostępny katalog danych ankiety:\n"
        f"{json.dumps(planner_catalog, ensure_ascii=False, indent=2)}"
    )
    raw_plan = await openai_client.create_json_completion(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        schema_name="retrieval_plan",
        schema=RETRIEVAL_PLAN_SCHEMA,
    )
    return RetrievalPlan.model_validate(raw_plan)


def filter_plan_against_catalog(plan: RetrievalPlan, catalog: dict) -> RetrievalPlan:
    question_metadata_rows = catalog["question_metadata"]
    metadata_field_names = {item["field_name"] for item in question_metadata_rows}
    metadata_question_types = {item["question_type"] for item in question_metadata_rows}
    aggregate_keys = {
        (
            item["metric_name"],
            item["segment_type"],
            item["segment_value"],
            item["subsegment_type"],
            item["subsegment_value"],
        )
        for item in catalog["aggregate_segments"]
    }
    correlation_keys = {
        (item["x_metric"], item["y_metric"], item["group_name"])
        for item in catalog["correlations"]
    }
    open_topic_keys = {
        (
            item["question_field"],
            item["role_group"],
            item["experience_group"],
            item["topic_name"],
        )
        for item in catalog["open_topics"]
    }
    raw_numeric_fields = set(catalog["raw_numeric_fields"])
    raw_open_text_fields = set(catalog["raw_open_text_fields"])
    filter_dimensions = catalog["response_filter_dimensions"]
    role_groups = set(filter_dimensions["role_group"])
    experience_groups = set(filter_dimensions["experience_group"])
    company_types = set(filter_dimensions["company_type"])
    company_sizes = set(filter_dimensions["company_size_group"])
    employment_statuses = set(filter_dimensions["employment_status"])

    allowed_question_metadata = [
        request
        for request in plan.question_metadata_requests
        if (request.field_name is None or request.field_name in metadata_field_names)
        and (request.question_type is None or request.question_type in metadata_question_types)
    ]

    allowed_aggregates = []
    for request in plan.aggregate_requests:
        lookup = (
            request.metric_name,
            request.segment_type,
            request.segment_value,
            request.subsegment_type,
            request.subsegment_value,
        )
        wildcard_lookup = (
            request.metric_name,
            request.segment_type,
            request.segment_value,
            None,
            None,
        )
        metric_only = (request.metric_name, None, None, None, None)
        if lookup in aggregate_keys or wildcard_lookup in aggregate_keys or metric_only in aggregate_keys:
            allowed_aggregates.append(request)

    allowed_correlations = [
        request
        for request in plan.correlation_requests
        if (request.x_metric, request.y_metric, request.group_name) in correlation_keys
    ]

    allowed_open_topics = []
    for request in plan.open_topic_requests:
        matches = [
            key
            for key in open_topic_keys
            if key[0] == request.question_field
            and (request.role_group is None or key[1] == request.role_group)
            and (request.experience_group is None or key[2] == request.experience_group)
            and (request.topic_name is None or key[3] == request.topic_name)
        ]
        if matches:
            allowed_open_topics.append(request)

    def is_allowed_filter(value: str | None, allowed_values: set[str]) -> bool:
        return value is None or value in allowed_values

    allowed_numeric_stats = [
        request
        for request in plan.numeric_stats_requests
        if request.field_name in raw_numeric_fields
        and is_allowed_filter(request.role_group, role_groups)
        and is_allowed_filter(request.experience_group, experience_groups)
        and is_allowed_filter(request.company_type, company_types)
        and is_allowed_filter(request.company_size_group, company_sizes)
        and is_allowed_filter(request.employment_status, employment_statuses)
    ]

    categorical_allowed_fields = {
        item["field_name"]
        for item in catalog["question_metadata"]
        if item["question_type"] in {"single_choice", "multi_choice"}
    }

    allowed_categorical_stats = [
        request
        for request in plan.categorical_stats_requests
        if request.field_name in categorical_allowed_fields
        and is_allowed_filter(request.role_group, role_groups)
        and is_allowed_filter(request.experience_group, experience_groups)
        and is_allowed_filter(request.company_type, company_types)
        and is_allowed_filter(request.company_size_group, company_sizes)
        and is_allowed_filter(request.employment_status, employment_statuses)
    ]

    allowed_text_responses = [
        request
        for request in plan.text_response_requests
        if request.field_name in raw_open_text_fields
        and is_allowed_filter(request.role_group, role_groups)
        and is_allowed_filter(request.experience_group, experience_groups)
        and is_allowed_filter(request.company_type, company_types)
        and is_allowed_filter(request.company_size_group, company_sizes)
        and is_allowed_filter(request.employment_status, employment_statuses)
    ]

    return RetrievalPlan(
        is_in_scope=plan.is_in_scope,
        reasoning=plan.reasoning,
        question_metadata_requests=allowed_question_metadata,
        aggregate_requests=allowed_aggregates,
        correlation_requests=allowed_correlations,
        open_topic_requests=allowed_open_topics,
        numeric_stats_requests=allowed_numeric_stats,
        categorical_stats_requests=allowed_categorical_stats,
        text_response_requests=allowed_text_responses,
    )


async def execute_plan(session: AsyncSession, plan: RetrievalPlan, catalog: dict) -> dict:
    question_metadata_context: list[dict] = []
    for request in plan.question_metadata_requests:
        rows = [
            row
            for row in catalog["question_metadata"]
            if (request.field_name is None or row["field_name"] == request.field_name)
            and (request.question_type is None or row["question_type"] == request.question_type)
        ][: min(max(request.limit, 1), 50)]
        if rows:
            question_metadata_context.append(
                {
                    "request": request.model_dump(),
                    "rows": serialize_question_metadata_rows(rows),
                }
            )

    open_topic_summaries_context: list[dict] = []
    for request in plan.open_topic_requests:
        summary = await fetch_open_topic_summary(
            session,
            question_field=request.question_field,
            role_group=request.role_group,
            experience_group=request.experience_group,
            limit=12,
        )
        if summary is not None:
            open_topic_summaries_context.append(
                {
                    "request": request.model_dump(),
                    "summary": summary,
                }
            )

    aggregates_context: list[dict] = []
    for request in plan.aggregate_requests:
        rows = await fetch_aggregate_rows(
            session,
            metric_name=request.metric_name,
            segment_type=request.segment_type,
            segment_value=request.segment_value,
            subsegment_type=request.subsegment_type,
            subsegment_value=request.subsegment_value,
            limit=min(max(request.limit, 1), 12),
        )
        if rows:
            aggregates_context.append(
                {
                    "request": request.model_dump(),
                    "rows": serialize_aggregate_rows(rows),
                }
            )

    correlations_context: list[dict] = []
    for request in plan.correlation_requests:
        row = await fetch_correlation_row(
            session,
            x_metric=request.x_metric,
            y_metric=request.y_metric,
            group_name=request.group_name,
        )
        if row is not None:
            correlations_context.append(
                {
                    "request": request.model_dump(),
                    "row": serialize_correlation_row(row),
                }
            )

    open_topics_context: list[dict] = []
    for request in plan.open_topic_requests:
        rows = await fetch_open_topics(
            session,
            question_field=request.question_field,
            role_group=request.role_group,
            experience_group=request.experience_group,
            topic_name=request.topic_name,
            limit=max(min(max(request.limit, 1), 30), 20),
        )
        if rows:
            open_topics_context.append(
                {
                    "request": request.model_dump(),
                    "rows": serialize_open_topic_rows(rows),
                }
            )

    numeric_stats_context: list[dict] = []
    for request in plan.numeric_stats_requests:
        row = await fetch_numeric_summary(
            session,
            field_name=request.field_name,
            role_group=request.role_group,
            experience_group=request.experience_group,
            company_type=request.company_type,
            company_size_group=request.company_size_group,
            employment_status=request.employment_status,
        )
        if row is not None:
            numeric_stats_context.append(
                {
                    "request": request.model_dump(),
                    "row": row,
                }
            )

    categorical_stats_context: list[dict] = []
    for request in plan.categorical_stats_requests:
        rows = await fetch_categorical_distribution(
            session,
            field_name=request.field_name,
            role_group=request.role_group,
            experience_group=request.experience_group,
            company_type=request.company_type,
            company_size_group=request.company_size_group,
            employment_status=request.employment_status,
            limit=min(max(request.limit, 1), 12),
        )
        if rows:
            categorical_stats_context.append(
                {
                    "request": request.model_dump(),
                    "rows": rows,
                }
            )

    text_responses_context: list[dict] = []
    for request in plan.text_response_requests:
        rows = await fetch_text_responses(
            session,
            field_name=request.field_name,
            role_group=request.role_group,
            experience_group=request.experience_group,
            company_type=request.company_type,
            company_size_group=request.company_size_group,
            employment_status=request.employment_status,
            sort_by=request.sort_by,
            limit=min(max(request.limit, 1), 10),
        )
        if rows:
            text_responses_context.append(
                {
                    "request": request.model_dump(),
                    "rows": rows,
                }
            )

    return {
        "question_metadata": question_metadata_context,
        "open_topic_summaries": open_topic_summaries_context,
        "aggregates": aggregates_context,
        "correlations": correlations_context,
        "open_topics": open_topics_context,
        "numeric_stats": numeric_stats_context,
        "categorical_stats": categorical_stats_context,
        "text_responses": text_responses_context,
    }


async def generate_grounded_answer(question: str, context_payload: dict) -> GroundedAnswer:
    user_prompt = (
        f"Pytanie użytkownika:\n{question}\n\n"
        "Dane ankiety pobrane przez backend:\n"
        f"{json.dumps(context_payload, ensure_ascii=False, indent=2)}"
    )
    raw_answer = await openai_client.create_json_completion(
        system_prompt=build_answer_system_prompt(),
        user_prompt=user_prompt,
        schema_name="grounded_answer",
        schema=GROUNDED_ANSWER_SCHEMA,
    )
    return GroundedAnswer.model_validate(raw_answer)


async def answer_question(question: str, session: AsyncSession) -> ChatResponse:
    if not openai_client.is_configured:
        return ChatResponse(
            answer=CONFIG_MESSAGE,
            answer_type="config_error",
            source="config",
        )

    try:
        catalog = await fetch_catalog(session)
        plan = await plan_retrieval(question, catalog)
        if not plan.is_in_scope or (
            not plan.question_metadata_requests
            and
            not plan.aggregate_requests
            and not plan.correlation_requests
            and not plan.open_topic_requests
            and not plan.numeric_stats_requests
            and not plan.categorical_stats_requests
            and not plan.text_response_requests
        ):
            recovery_plan = await plan_retrieval_recovery(question, catalog)
            if recovery_plan.is_in_scope:
                plan = recovery_plan

        if not plan.is_in_scope:
            return ChatResponse(
                answer=REFUSAL_MESSAGE,
                answer_type="refusal",
                source="guardrails",
            )

        plan = filter_plan_against_catalog(plan, catalog)
        context_payload = await execute_plan(session, plan, catalog)

        if not any(context_payload.values()):
            return ChatResponse(
                answer=NO_DATA_MESSAGE,
                answer_type="no_data",
                source="guardrails",
            )

        grounded = await generate_grounded_answer(question, context_payload)
        if grounded.insufficient_data:
            return ChatResponse(
                answer=NO_DATA_MESSAGE,
                answer_type="no_data",
                source="guardrails",
            )

        return ChatResponse(
            answer=format_answer_text(grounded.answer),
            answer_type="llm",
            source="openai+survey_data",
            warning=compute_warning_from_context(context_payload),
        )
    except (httpx.HTTPError, RuntimeError, ValueError, KeyError):
        return ChatResponse(
            answer="Nie udało się uzyskać odpowiedzi z modelu LLM. Sprawdź OPENAI_API_KEY i konfigurację połączenia.",
            answer_type="config_error",
            source="openai",
        )
