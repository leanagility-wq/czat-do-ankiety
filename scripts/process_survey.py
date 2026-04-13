import argparse
import math
import re
import unicodedata
from pathlib import Path

import pandas as pd
from scipy.stats import spearmanr


BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_PATH = BASE_DIR / "input" / "raw_survey.csv"
OUTPUT_DIR = BASE_DIR / "output"
SMALL_SAMPLE_THRESHOLD = 5

# To jest najłatwiejsze miejsce do ręcznej korekty mapowania kolumn.
MANUAL_COLUMN_HINTS = {
    "submitted_at": ["sygnatura czasowa", "timestamp"],
    "role_raw": ["twoja obecna rola"],
    "experience_group": ["doswiadczenie w obecnej roli"],
    "company_size_group": ["wielkosc dzialu", "typowy rozmiar firmy"],
    "company_type": ["czy oprogramowanie jest glownym produktem firmy", "wspiera dzialanie firmy"],
    "ai_usage_frequency": ["jak czesto korzystasz z narzedzi ai"],
    "ai_tools_used": ["z jakich narzedzi korzystasz najczesciej"],
    "ai_effectiveness": ["jak oceniasz zmiane efektywnosci swojej pracy"],
    "ai_investment_level": ["w jakim stopniu twoja firma", "inwestuja w narzedzia ai"],
    "ai_company_sentiment": ["jaki jest stosunek ludzi w firmie"],
    "ai_effectiveness_comment": ["zostaw dowolny komentarz dotyczacy efektywnosci"],
    "employment_status": ["pracujesz obecnie"],
    "role_confidence": ["jak pewnie czujesz sie w obecnej roli"],
    "confidence_uncertainty_source": ["co jest glownym zrodlem niepewnosci", "zrodlem niepewnosci"],
    "ai_replacement_risk": ["jak blisko sa narzedzia ai do tego", "aby cie zastapic"],
    "future_actions_considered": ["jakie dzialania rozwazasz jesli chodzi o swoja przyszlosc", "jakie dzialania rozwazasz"],
    "future_actions_priority": ["ktore dzialania beda dla ciebie najwazniejsze", "beda dla ciebie najwazniejsze"],
    "skills_to_develop": ["jesli planujesz rozwoj umiejetnosci"],
    "role_future_outlook": ["jak wyglada wedlug ciebie przyszlosc rol projektowych", "przyszlosc rol projektowych"],
}

ROLE_PATTERNS = [
    ("Scrum Master", ["scrum master"]),
    ("Project Manager", ["project manager", "kierownik projektu"]),
    ("Agile Coach", ["agile coach", "agile consultant"]),
    (
        "Product Owner / Product Manager",
        ["product owner", "product manager", "technical product owner"],
    ),
    (
        "Delivery Lead / Delivery Manager",
        ["delivery lead", "delivery manager", "release train engineer"],
    ),
]

EXPERIENCE_PATTERNS = {
    "Mniej niż rok": ["mniej niz rok", "<1", "ponizej roku"],
    "1-3 lata": ["1-3", "1 - 3"],
    "3-5 lat": ["3-5", "3 - 5"],
    "5-10 lat": ["5-10", "5 - 10"],
    "10+ lat": ["10+", "10 +", "powyzej 10"],
}

COMPANY_SIZE_PATTERNS = {
    "1-50": ["1-10", "11-50", "1-50", "do 50"],
    "51-150": ["51-150", "50-150"],
    "150-500": ["150-500", "151-500"],
    "500+": ["500+", "powyzej 500", "500 i wiecej"],
}

COMPANY_TYPE_PATTERNS = {
    "Software Product": ["glowny produkt", "saas", "sprzedajemy oprogramowanie"],
    "Software as Support Function": ["wspierajace dzialanie firmy", "wspiera dzialanie"],
    "Usługi / Consulting": ["uslugi", "consulting", "doradztwo", "klienci"],
}

EMPLOYMENT_PATTERNS = {
    "Pełny etat": ["pelnego etatu", "1fte", "uop", "pelny etat"],
    "Niepełny etat": ["niepelny etat"],
    "Nie pracuję": ["nie", "urlop macierzynski"],
}

FREQUENCY_PATTERNS = {
    "Nigdy": (0, ["nigdy"]),
    "Rzadziej niż raz w tygodniu": (1, ["rzadziej niz raz w tygodniu", "nie korzystam"]),
    "Raz na kilka dni": (2, ["raz na kilka dni", "kilka razy w tygodniu"]),
    "Codziennie": (3, ["codziennie", "kilka razy dziennie", "wiele razy dziennie", "raz dziennie", "wielokrotnie w ciagu dnia"]),
}

TOOL_NORMALIZATION = {
    "chatgpt": "ChatGPT",
    "chat gpt": "ChatGPT",
    "copilot": "GitHub Copilot",
    "github copilot": "GitHub Copilot",
    "gemini": "Gemini",
    "claude": "Claude",
    "teams": "Microsoft Teams",
    "notion": "Notion AI",
    "jira": "Jira AI",
    "perplexity": "Perplexity",
    "midjourney": "Midjourney",
}

TOPIC_KEYWORDS = {
    "automatyzacja": ("sposób pracy", ["automatyz", "usprawni", "przyspiesz", "oszczed"]),
    "jakość odpowiedzi": ("zaufanie do AI", ["halucyn", "bled", "jakosc", "wiarygodn", "sprawdz", "weryfik"]),
    "bezpieczeństwo danych": ("ryzyka", ["bezpieczen", "poufn", "dane", "rodo", "privacy"]),
    "obawy o rolę": ("ryzyka", ["zastapi", "redukc", "rola", "praca", "etat", "niepewn"]),
    "kompetencje AI": ("rozwój", ["prompt", "ai", "llm", "narzedz", "agent"]),
    "kompetencje liderskie": ("rozwój", ["leadership", "lider", "facylit", "komunik", "coaching"]),
    "kompetencje produktowe": ("rozwój", ["produkt", "product", "roadmap", "discovery", "stakeholder"]),
    "kompetencje techniczne": ("rozwój", ["python", "analiz", "technicz", "data", "sql", "coding"]),
    "przyszłość hybrydowa": ("przyszłość roli", ["hybryd", "laczenie", "ewolu", "zmieni"]),
    "więcej strategii": ("przyszłość roli", ["strateg", "biznes", "dorad", "wartosc"]),
    "więcej facylitacji": ("przyszłość roli", ["facylit", "warsztat", "zespol", "moder"]),
}

SCALE_FIELDS = {
    "ai_effectiveness",
    "ai_investment_level",
    "ai_company_sentiment",
    "role_confidence",
    "ai_replacement_risk",
}


def normalize_text(value: str) -> str:
    text = str(value).translate(str.maketrans({"ł": "l", "Ł": "L"}))
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def snake_case(value: str) -> str:
    normalized = normalize_text(value)
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "field"


def unique_name(name: str, used: set[str]) -> str:
    candidate = name
    suffix = 2
    while candidate in used:
        candidate = f"{name}_{suffix}"
        suffix += 1
    used.add(candidate)
    return candidate


def split_multi_value(value: str) -> list[str]:
    if not value or pd.isna(value):
        return []
    parts = re.split(r"[,\n;\|]+", str(value))
    return [part.strip() for part in parts if part.strip()]


def map_column_name(original_name: str, used_names: set[str]) -> str:
    normalized = normalize_text(original_name)
    for field_name, hints in MANUAL_COLUMN_HINTS.items():
        if any(hint in normalized for hint in hints):
            return unique_name(field_name, used_names)
    return unique_name(snake_case(original_name), used_names)


def infer_role_group(value: str, raw_counts: dict[str, int]) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return "Inne"
    if raw_counts.get(raw_value, 0) <= 1:
        return "Inne"
    normalized = normalize_text(raw_value)
    for label, patterns in ROLE_PATTERNS:
        if any(pattern in normalized for pattern in patterns):
            return label
    return "Inne"


def map_category(value: str, patterns: dict[str, list[str]], default: str = "Inne") -> str:
    normalized = normalize_text(value)
    for label, hints in patterns.items():
        if any(hint in normalized for hint in hints):
            return label
    return default


def map_employment_status(value: str) -> str:
    normalized = normalize_text(value)
    if not normalized:
        return ""
    if "niepelny etat" in normalized:
        return "Niepełny etat"
    if normalized == "nie" or normalized.startswith("nie "):
        return "Nie pracuję"
    if "urlop macierzynski" in normalized:
        return "Nie pracuję"
    if any(hint in normalized for hint in EMPLOYMENT_PATTERNS["Pełny etat"]):
        return "Pełny etat"
    return "Inne"


def map_usage_frequency(value: str) -> tuple[str, float | None]:
    normalized = normalize_text(value)
    if not normalized:
        return "", None
    for label, (score, hints) in FREQUENCY_PATTERNS.items():
        if any(hint in normalized for hint in hints):
            return label, float(score)
    return str(value).strip(), None


def parse_scale_value(value) -> float | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"-?\d+(?:[.,]\d+)?", text)
    if match:
        return float(match.group(0).replace(",", "."))

    normalized = normalize_text(text)
    label_map = {
        "zdecydowanie nie": 1.0,
        "raczej nie": 2.0,
        "trudno powiedziec": 3.0,
        "raczej tak": 4.0,
        "zdecydowanie tak": 5.0,
    }
    return label_map.get(normalized)


def clean_tool_name(value: str) -> str:
    normalized = normalize_text(value)
    for hint, label in TOOL_NORMALIZATION.items():
        if hint in normalized:
            return label
    return str(value).strip()


def infer_question_type(series: pd.Series, field_name: str) -> str:
    non_null = series.dropna().astype(str).str.strip()
    non_null = non_null[non_null != ""]
    if non_null.empty:
        return "empty"
    if field_name == "submitted_at":
        return "technical"
    if field_name in SCALE_FIELDS or non_null.str.fullmatch(r"\d+(?:[.,]\d+)?").all():
        return "scale"
    avg_length = non_null.str.len().mean()
    unique_ratio = non_null.nunique() / len(non_null)
    if avg_length > 35 and unique_ratio > 0.35:
        return "open_text"
    if non_null.str.contains(r"[,;\n\|]").mean() > 0.2:
        return "multi_choice"
    return "single_choice"


def detect_open_text_fields(df: pd.DataFrame, metadata_rows: list[dict]) -> list[str]:
    fields: list[str] = []
    for row in metadata_rows:
        if row["question_type"] == "open_text" and row["field_name"] in df.columns:
            fields.append(row["field_name"])
    return fields


def coerce_datetime(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.replace(r"\s+[A-Z]{2,5}$", "", regex=True)
    return pd.to_datetime(cleaned, errors="coerce", utc=True)


def build_metadata(
    original_columns: list[str],
    original_to_clean: dict[str, str],
    clean_df: pd.DataFrame,
    dropped_columns: set[str],
) -> list[dict]:
    rows: list[dict] = []
    for original_name in original_columns:
        field_name = original_to_clean[original_name]
        notes_parts: list[str] = []
        if original_name in dropped_columns:
            notes_parts.append("dropped_from_clean_output")
        series = clean_df[field_name] if field_name in clean_df.columns else pd.Series(dtype="object")
        question_type = infer_question_type(series, field_name) if field_name in clean_df.columns else "technical"

        scale_min = ""
        scale_max = ""
        if question_type == "scale" and field_name in clean_df.columns:
            numeric_series = pd.to_numeric(series, errors="coerce").dropna()
            if not numeric_series.empty:
                scale_min = float(numeric_series.min())
                scale_max = float(numeric_series.max())

        allowed_values = ""
        if question_type in {"single_choice", "multi_choice", "scale"} and field_name in clean_df.columns:
            unique_values = [
                str(value).strip()
                for value in series.dropna().astype(str).unique().tolist()
                if str(value).strip()
            ][:25]
            allowed_values = " | ".join(sorted(unique_values))

        rows.append(
            {
                "field_name": field_name,
                "original_column_name": original_name,
                "question_text": original_name.strip(),
                "question_type": question_type,
                "scale_min": scale_min,
                "scale_max": scale_max,
                "allowed_values": allowed_values,
                "notes": "; ".join(notes_parts),
            }
        )
    return rows


def add_metric_row(
    rows: list[dict],
    metric_name: str,
    segment_type: str,
    segment_value: str,
    subsegment_type: str,
    subsegment_value: str,
    value: float,
    value_type: str,
    n: int,
    notes: str = "",
) -> None:
    rows.append(
        {
            "metric_name": metric_name,
            "segment_type": segment_type,
            "segment_value": segment_value,
            "subsegment_type": subsegment_type,
            "subsegment_value": subsegment_value,
            "value": value,
            "value_type": value_type,
            "n": n,
            "small_sample_warning": n < SMALL_SAMPLE_THRESHOLD,
            "notes": notes,
        }
    )


def build_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    for role, count in df["role_group"].value_counts(dropna=False).items():
        add_metric_row(rows, "count_by_role", "role_group", str(role), "", "", int(count), "count", int(count))

    for experience, count in df["experience_group"].value_counts(dropna=False).items():
        add_metric_row(
            rows,
            "count_by_experience",
            "experience_group",
            str(experience),
            "",
            "",
            int(count),
            "count",
            int(count),
        )

    role_usage = (
        df.dropna(subset=["role_group", "ai_usage_frequency"])
        .groupby(["role_group", "ai_usage_frequency"])
        .size()
        .reset_index(name="value")
    )
    for _, row in role_usage.iterrows():
        total_n = int(df[df["role_group"] == row["role_group"]].shape[0])
        share = float(row["value"]) / total_n if total_n else 0.0
        add_metric_row(
            rows,
            "ai_usage_frequency_by_role",
            "role_group",
            row["role_group"],
            "ai_usage_frequency",
            row["ai_usage_frequency"],
            int(row["value"]),
            "count",
            total_n,
            f"share={share:.3f}",
        )

    experience_usage = (
        df.dropna(subset=["experience_group", "ai_usage_frequency"])
        .groupby(["experience_group", "ai_usage_frequency"])
        .size()
        .reset_index(name="value")
    )
    for _, row in experience_usage.iterrows():
        total_n = int(df[df["experience_group"] == row["experience_group"]].shape[0])
        share = float(row["value"]) / total_n if total_n else 0.0
        add_metric_row(
            rows,
            "ai_usage_frequency_by_experience",
            "experience_group",
            row["experience_group"],
            "ai_usage_frequency",
            row["ai_usage_frequency"],
            int(row["value"]),
            "count",
            total_n,
            f"share={share:.3f}",
        )

    for metric_name, field_name in [
        ("ai_effectiveness_by_role", "ai_effectiveness"),
        ("role_confidence_by_role", "role_confidence"),
        ("ai_replacement_risk_by_role", "ai_replacement_risk"),
    ]:
        subset = df.dropna(subset=["role_group", field_name])
        grouped = subset.groupby("role_group")[field_name].agg(["mean", "count"]).reset_index()
        for _, row in grouped.iterrows():
            add_metric_row(
                rows,
                metric_name,
                "role_group",
                row["role_group"],
                "",
                "",
                float(row["mean"]),
                "mean",
                int(row["count"]),
            )

    tool_counts: dict[str, int] = {}
    total_tool_mentions = 0
    for value in df["ai_tools_used"].fillna(""):
        for tool in split_multi_value(value):
            clean_name = clean_tool_name(tool)
            if clean_name:
                tool_counts[clean_name] = tool_counts.get(clean_name, 0) + 1
                total_tool_mentions += 1
    for tool, count in sorted(tool_counts.items(), key=lambda item: (-item[1], item[0])):
        share = count / total_tool_mentions if total_tool_mentions else 0.0
        add_metric_row(
            rows,
            "most_used_ai_tools",
            "overall",
            "all",
            "tool_name",
            tool,
            count,
            "count",
            total_tool_mentions,
            f"share={share:.3f}",
        )

    for metric_name, field_name, note in [
        ("company_ai_actions", "ai_investment_level", "Proxy based on self-reported company AI investment."),
        ("company_attitude_to_ai", "ai_company_sentiment", ""),
    ]:
        subset = df.dropna(subset=[field_name])
        grouped = subset.groupby(field_name).size().reset_index(name="value")
        total_n = int(subset.shape[0])
        for _, row in grouped.iterrows():
            share = float(row["value"]) / total_n if total_n else 0.0
            label = str(int(row[field_name]) if float(row[field_name]).is_integer() else row[field_name])
            add_metric_row(
                rows,
                metric_name,
                "overall",
                "all",
                field_name,
                label,
                int(row["value"]),
                "count",
                total_n,
                f"{note} share={share:.3f}".strip(),
            )

    return pd.DataFrame(rows)


def describe_correlation(x_metric: str, y_metric: str, rho: float, p_value: float, n: int) -> str:
    if pd.isna(rho):
        return f"Za mało danych, aby ocenić zależność między {x_metric} i {y_metric}."
    strength = "słaba"
    abs_rho = abs(rho)
    if abs_rho >= 0.6:
        strength = "silna"
    elif abs_rho >= 0.3:
        strength = "umiarkowana"
    direction = "dodatnia" if rho > 0 else "ujemna"
    significance = "istotna statystycznie" if p_value < 0.05 else "nieistotna statystycznie"
    return (
        f"W grupie {n} odpowiedzi zależność {x_metric} vs {y_metric} jest {direction}, "
        f"{strength} i {significance}."
    )


def calculate_correlation_rows(df: pd.DataFrame) -> pd.DataFrame:
    pairs = [
        ("ai_usage_frequency_score", "ai_effectiveness"),
        ("ai_usage_frequency_score", "role_confidence"),
        ("ai_replacement_risk", "role_confidence"),
    ]

    rows: list[dict] = []
    groups = [("all", df)] + [(f"role:{role}", frame.copy()) for role, frame in df.groupby("role_group")]

    for x_metric, y_metric in pairs:
        for group_name, frame in groups:
            subset = frame[[x_metric, y_metric]].dropna()
            n = int(subset.shape[0])
            if n < 3:
                rows.append(
                    {
                        "x_metric": x_metric,
                        "y_metric": y_metric,
                        "group_name": group_name,
                        "correlation_type": "spearman",
                        "correlation_value": "",
                        "p_value": "",
                        "is_significant": False,
                        "plain_language_summary": describe_correlation(x_metric, y_metric, float("nan"), 1.0, n),
                        "n": n,
                        "notes": "insufficient_data",
                    }
                )
                continue

            rho, p_value = spearmanr(subset[x_metric], subset[y_metric])
            rows.append(
                {
                    "x_metric": x_metric,
                    "y_metric": y_metric,
                    "group_name": group_name,
                    "correlation_type": "spearman",
                    "correlation_value": round(float(rho), 4),
                    "p_value": round(float(p_value), 6),
                    "is_significant": bool(p_value < 0.05),
                    "plain_language_summary": describe_correlation(
                        x_metric,
                        y_metric,
                        float(rho),
                        float(p_value),
                        n,
                    ),
                    "n": n,
                    "notes": "",
                }
            )

    return pd.DataFrame(rows)


def assign_topics(question_field: str, text: str) -> list[tuple[str, str, str]]:
    normalized = normalize_text(text)
    hits: list[tuple[str, str, int, str]] = []
    for topic_name, (topic_group, keywords) in TOPIC_KEYWORDS.items():
        matched = [keyword for keyword in keywords if keyword in normalized]
        if matched:
            hits.append((topic_name, topic_group, len(matched), ", ".join(matched[:4])))

    if not hits:
        fallback_group = "inne"
        if "skills" in question_field or "develop" in question_field:
            fallback_group = "rozwój"
        elif "future" in question_field or "outlook" in question_field:
            fallback_group = "przyszłość roli"
        elif "confidence" in question_field or "uncertainty" in question_field:
            fallback_group = "ryzyka"
        return [("inne", fallback_group, "no_keyword_match")]

    hits.sort(key=lambda item: (-item[2], item[0]))
    return [(topic_name, topic_group, notes) for topic_name, topic_group, _, notes in hits[:3]]


def build_open_topics(df: pd.DataFrame, open_fields: list[str]) -> pd.DataFrame:
    rows: list[dict] = []
    for _, response in df.iterrows():
        for field_name in open_fields:
            quote_full = str(response.get(field_name) or "").strip()
            if not quote_full:
                continue
            topics = assign_topics(field_name, quote_full)
            for topic_name, topic_group, notes in topics:
                rows.append(
                    {
                        "response_id": response["response_id"],
                        "question_field": field_name,
                        "role_group": response.get("role_group", ""),
                        "experience_group": response.get("experience_group", ""),
                        "topic_name": topic_name,
                        "topic_group": topic_group,
                        "quote_short": quote_full[:160],
                        "quote_full": quote_full,
                        "notes": notes,
                    }
                )
    return pd.DataFrame(rows)


def prepare_clean_dataframe(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict], set[str]]:
    original_columns = raw_df.columns.tolist()
    used_names: set[str] = set()
    rename_map: dict[str, str] = {}
    for column in original_columns:
        rename_map[column] = map_column_name(column, used_names)

    df = raw_df.rename(columns=rename_map).copy()
    df = df.dropna(axis=1, how="all")

    dropped_columns: set[str] = set()
    for original_name, clean_name in list(rename_map.items()):
        if clean_name not in df.columns:
            dropped_columns.add(original_name)
            continue
        if clean_name == "submitted_at":
            continue
        series = df[clean_name]
        if series.astype(str).str.strip().eq("").all():
            df = df.drop(columns=[clean_name])
            dropped_columns.add(original_name)

    if "response_id" not in df.columns:
        df.insert(0, "response_id", [f"r{i:04d}" for i in range(1, len(df) + 1)])

    if "submitted_at" in df.columns:
        df["submitted_at"] = coerce_datetime(df["submitted_at"])

    raw_role_counts = (
        df["role_raw"].fillna("").astype(str).str.strip().value_counts().to_dict()
        if "role_raw" in df.columns
        else {}
    )

    if "role_raw" in df.columns:
        df["role_group"] = df["role_raw"].apply(lambda value: infer_role_group(value, raw_role_counts))
    else:
        df["role_group"] = "Inne"

    if "experience_group" in df.columns:
        df["experience_group"] = df["experience_group"].apply(
            lambda value: map_category(value, EXPERIENCE_PATTERNS, default="Inne")
        )

    if "company_size_group" in df.columns:
        df["company_size_group"] = df["company_size_group"].apply(
            lambda value: map_category(value, COMPANY_SIZE_PATTERNS, default="Inne")
        )

    if "company_type" in df.columns:
        df["company_type"] = df["company_type"].apply(
            lambda value: map_category(value, COMPANY_TYPE_PATTERNS, default="Inne")
        )

    if "employment_status" in df.columns:
        df["employment_status"] = df["employment_status"].apply(map_employment_status)

    if "ai_usage_frequency" in df.columns:
        usage_pairs = df["ai_usage_frequency"].apply(map_usage_frequency)
        df["ai_usage_frequency"] = usage_pairs.apply(lambda item: item[0])
        df["ai_usage_frequency_score"] = usage_pairs.apply(lambda item: item[1])
    else:
        df["ai_usage_frequency_score"] = None

    for field_name in SCALE_FIELDS:
        if field_name in df.columns:
            df[field_name] = df[field_name].apply(parse_scale_value)

    preferred_columns = [
        "response_id",
        "submitted_at",
        "role_raw",
        "role_group",
        "experience_group",
        "company_size_group",
        "company_type",
        "ai_usage_frequency",
        "ai_usage_frequency_score",
        "ai_tools_used",
        "ai_effectiveness",
        "ai_investment_level",
        "ai_company_sentiment",
        "ai_effectiveness_comment",
        "employment_status",
        "role_confidence",
        "confidence_uncertainty_source",
        "ai_replacement_risk",
        "future_actions_considered",
        "future_actions_priority",
        "skills_to_develop",
        "role_future_outlook",
    ]
    existing_columns = [column for column in preferred_columns if column in df.columns]
    remaining_columns = [column for column in df.columns if column not in existing_columns]
    df = df[existing_columns + remaining_columns]

    metadata_rows = build_metadata(original_columns, rename_map, df, dropped_columns)
    return df, metadata_rows, dropped_columns


def print_dry_run_summary(df: pd.DataFrame, metadata_rows: list[dict]) -> None:
    print("WYKRYTE KOLUMNY:")
    for row in metadata_rows:
        print(f"- {row['original_column_name']} -> {row['field_name']} [{row['question_type']}]")

    print("\nPROPONOWANE MAPOWANIE:")
    for field_name in [
        "role_raw",
        "experience_group",
        "company_size_group",
        "company_type",
        "ai_usage_frequency",
        "ai_effectiveness",
        "ai_investment_level",
        "ai_company_sentiment",
        "role_confidence",
        "ai_replacement_risk",
    ]:
        if field_name in df.columns:
            sample_values = [
                str(value)
                for value in df[field_name].dropna().astype(str).unique().tolist()[:8]
            ]
            print(f"- {field_name}: {', '.join(sample_values)}")

    open_fields = detect_open_text_fields(df, metadata_rows)
    scale_fields = [row["field_name"] for row in metadata_rows if row["question_type"] == "scale"]

    print("\nPYTANIA OTWARTE:")
    for field_name in open_fields:
        print(f"- {field_name}")

    print("\nPYTANIA SKALOWE:")
    for field_name in scale_fields:
        print(f"- {field_name}")


def save_csv(dataframe: pd.DataFrame, path: Path) -> None:
    dataframe = dataframe.copy()
    for column in dataframe.columns:
        if pd.api.types.is_datetime64_any_dtype(dataframe[column]):
            dataframe[column] = dataframe[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    dataframe.to_csv(path, index=False, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_PATH), help="Ścieżka do surowego CSV.")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Katalog na pliki wynikowe.")
    parser.add_argument("--dry-run", action="store_true", help="Tylko podgląd mapowania bez zapisu plików.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    raw_df = pd.read_csv(input_path, encoding="utf-8-sig")
    clean_df, metadata_rows, _ = prepare_clean_dataframe(raw_df)

    if args.dry_run:
        print_dry_run_summary(clean_df, metadata_rows)
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_df = pd.DataFrame(metadata_rows)
    open_fields = detect_open_text_fields(clean_df, metadata_rows)
    aggregates_df = build_aggregates(clean_df)
    correlations_df = calculate_correlation_rows(clean_df)
    open_topics_df = build_open_topics(clean_df, open_fields)

    save_csv(clean_df, output_dir / "responses_clean.csv")
    save_csv(metadata_df, output_dir / "question_metadata.csv")
    save_csv(aggregates_df, output_dir / "aggregates.csv")
    save_csv(correlations_df, output_dir / "correlations.csv")
    save_csv(open_topics_df, output_dir / "open_topics.csv")

    print("Wygenerowano pliki:")
    for file_name in [
        "responses_clean.csv",
        "question_metadata.csv",
        "aggregates.csv",
        "correlations.csv",
        "open_topics.csv",
    ]:
        print(f"- {output_dir / file_name}")


if __name__ == "__main__":
    main()
