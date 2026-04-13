import re

from app.schemas import RoutedQuestion


OUT_OF_SCOPE_PATTERNS = (
    r"\bporad",
    r"\brekomend",
    r"\brynek\b",
    r"\bstrategi",
    r"\bco powin",
    r"\bjak zarobi",
    r"\binwestować na rynku\b",
)


def route_question(question: str) -> RoutedQuestion:
    normalized = question.strip().lower()

    if any(re.search(pattern, normalized) for pattern in OUT_OF_SCOPE_PATTERNS):
        return RoutedQuestion(route="refusal")

    if "scrum master" in normalized and "obaw" in normalized:
        return RoutedQuestion(
            route="open_topics",
            topic_key="top_concerns",
            segment_key="role:scrum_master",
            matched_example="Jakie obawy najczęściej zgłaszali Scrum Masterzy?",
        )

    if "kompetencj" in normalized and ("inwest" in normalized or "rozw" in normalized):
        return RoutedQuestion(
            route="open_topics",
            topic_key="investment_skills",
            segment_key="all",
            matched_example="W jakie kompetencje badani chcą inwestować?",
        )

    ai_effectiveness_markers = (
        "ai" in normalized
        and ("efektyw" in normalized or "produktyw" in normalized)
        and ("wiąże" in normalized or "korel" in normalized or "częst" in normalized)
    )
    if ai_effectiveness_markers:
        return RoutedQuestion(
            route="sql",
            metric_key="ai_usage_vs_effectiveness",
            segment_key="all",
            matched_example="Czy częstsze używanie AI wiąże się z większą efektywnością?",
        )

    if ("ile" in normalized or "jaki odsetek" in normalized) and "ai" in normalized:
        return RoutedQuestion(
            route="sql",
            metric_key="ai_usage_frequency",
            segment_key="all",
        )

    if "najczęściej" in normalized and "obaw" in normalized:
        return RoutedQuestion(
            route="open_topics",
            topic_key="top_concerns",
            segment_key="all",
        )

    if "cytat" in normalized or "cytaty" in normalized or "tematy" in normalized:
        return RoutedQuestion(
            route="open_topics",
            topic_key="top_concerns",
            segment_key="all",
        )

    if "czy" in normalized or "ile" in normalized or "odsetek" in normalized:
        return RoutedQuestion(
            route="sql",
            metric_key="ai_usage_frequency",
            segment_key="all",
        )

    return RoutedQuestion(route="refusal")
