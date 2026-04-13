CREATE TABLE IF NOT EXISTS responses (
    response_id TEXT PRIMARY KEY,
    submitted_at TIMESTAMPTZ NULL,
    role_raw TEXT NULL,
    role_group TEXT NULL,
    experience_group TEXT NULL,
    company_size_group TEXT NULL,
    company_type TEXT NULL,
    ai_usage_frequency TEXT NULL,
    ai_usage_frequency_score DOUBLE PRECISION NULL,
    ai_tools_used TEXT NULL,
    ai_effectiveness DOUBLE PRECISION NULL,
    ai_investment_level DOUBLE PRECISION NULL,
    ai_company_sentiment DOUBLE PRECISION NULL,
    ai_effectiveness_comment TEXT NULL,
    employment_status TEXT NULL,
    role_confidence DOUBLE PRECISION NULL,
    confidence_uncertainty_source TEXT NULL,
    ai_replacement_risk DOUBLE PRECISION NULL,
    future_actions_considered TEXT NULL,
    future_actions_priority TEXT NULL,
    skills_to_develop TEXT NULL,
    role_future_outlook TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_responses_role_group ON responses (role_group);
CREATE INDEX IF NOT EXISTS idx_responses_experience_group ON responses (experience_group);
CREATE INDEX IF NOT EXISTS idx_responses_ai_usage_score ON responses (ai_usage_frequency_score);

CREATE TABLE IF NOT EXISTS question_metadata (
    field_name TEXT PRIMARY KEY,
    original_column_name TEXT NOT NULL,
    question_text TEXT NOT NULL,
    question_type TEXT NOT NULL,
    scale_min DOUBLE PRECISION NULL,
    scale_max DOUBLE PRECISION NULL,
    allowed_values TEXT NULL,
    notes TEXT NULL
);

CREATE TABLE IF NOT EXISTS aggregates (
    id BIGSERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    segment_type TEXT NOT NULL,
    segment_value TEXT NOT NULL,
    subsegment_type TEXT NULL,
    subsegment_value TEXT NULL,
    value DOUBLE PRECISION NULL,
    value_type TEXT NOT NULL,
    n INTEGER NOT NULL,
    small_sample_warning BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_aggregates_metric ON aggregates (metric_name);
CREATE INDEX IF NOT EXISTS idx_aggregates_segment ON aggregates (segment_type, segment_value);

CREATE TABLE IF NOT EXISTS correlations (
    id BIGSERIAL PRIMARY KEY,
    x_metric TEXT NOT NULL,
    y_metric TEXT NOT NULL,
    group_name TEXT NOT NULL,
    correlation_type TEXT NOT NULL,
    correlation_value DOUBLE PRECISION NULL,
    p_value DOUBLE PRECISION NULL,
    is_significant BOOLEAN NOT NULL DEFAULT FALSE,
    plain_language_summary TEXT NOT NULL,
    n INTEGER NOT NULL,
    notes TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_correlations_metrics ON correlations (x_metric, y_metric);
CREATE INDEX IF NOT EXISTS idx_correlations_group_name ON correlations (group_name);

CREATE TABLE IF NOT EXISTS open_topics (
    id BIGSERIAL PRIMARY KEY,
    response_id TEXT NOT NULL,
    question_field TEXT NOT NULL,
    role_group TEXT NULL,
    experience_group TEXT NULL,
    topic_name TEXT NOT NULL,
    topic_group TEXT NOT NULL,
    quote_short TEXT NOT NULL,
    quote_full TEXT NOT NULL,
    notes TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_open_topics_question_field ON open_topics (question_field);
CREATE INDEX IF NOT EXISTS idx_open_topics_topic_name ON open_topics (topic_name);
CREATE INDEX IF NOT EXISTS idx_open_topics_role_group ON open_topics (role_group);
