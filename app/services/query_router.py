import re
import unicodedata

from app.schemas import RoutedQuestion


OUT_OF_SCOPE_PATTERNS = (
    r"\bporad",
    r"\brekomend",
    r"\brynek\b",
    r"\bstrategi",
    r"\bco powin",
    r"\bjak zarobi",
    r"\binwestowac na rynku\b",
)


def normalize_text(value: str) -> str:
    text = str(value).translate(str.maketrans({"ł": "l", "Ł": "L"}))
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    return text.lower().strip()


def route_question(question: str) -> RoutedQuestion:
    normalized = normalize_text(question)

    if any(re.search(pattern, normalized) for pattern in OUT_OF_SCOPE_PATTERNS):
        return RoutedQuestion(route="refusal")

    if "scrum master" in normalized and ("obaw" in normalized or "niepewn" in normalized):
        return RoutedQuestion(
            route="open_topics",
            question_field="confidence_uncertainty_source",
            role_group="Scrum Master",
            matched_example="Jakie obawy najczęściej zgłaszali Scrum Masterzy?",
        )

    if "kompetencj" in normalized and ("inwest" in normalized or "rozw" in normalized):
        return RoutedQuestion(
            route="open_topics",
            question_field="skills_to_develop",
            matched_example="W jakie kompetencje badani chcą inwestować?",
        )

    if (
        "ai" in normalized
        and ("efektywn" in normalized or "produktywn" in normalized)
        and ("wiaze" in normalized or "korel" in normalized or "czestsz" in normalized)
    ):
        return RoutedQuestion(
            route="correlation",
            x_metric="ai_usage_frequency_score",
            y_metric="ai_effectiveness",
            group_name="all",
            matched_example="Czy częstsze używanie AI wiąże się z większą efektywnością?",
        )

    if "narzedzi" in normalized and ("najczesciej" in normalized or "najczesciej uzywane" in normalized):
        return RoutedQuestion(
            route="aggregate",
            metric_name="most_used_ai_tools",
            segment_type="overall",
            segment_value="all",
        )

    if "jak czesto" in normalized and "ai" in normalized and "rola" in normalized:
        return RoutedQuestion(
            route="aggregate",
            metric_name="ai_usage_frequency_by_role",
        )

    if ("ile" in normalized or "liczebn" in normalized) and "rola" in normalized:
        return RoutedQuestion(
            route="aggregate",
            metric_name="count_by_role",
            segment_type="role_group",
        )

    if "obaw" in normalized or "cytat" in normalized or "tematy" in normalized:
        return RoutedQuestion(
            route="open_topics",
            question_field="confidence_uncertainty_source",
            matched_example="Jakie obawy najczęściej zgłaszali Scrum Masterzy?",
        )

    return RoutedQuestion(route="refusal")
