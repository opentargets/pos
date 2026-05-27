CREATE TABLE IF NOT EXISTS association_time_series_log (
    diseaseId String,
    targetId String,
    aggregationType Enum8 ('datasourceId', 'overall'),
    aggregationValue LowCardinality (String),
    year Nullable (UInt16),
    associationScore Float64,
    novelty Nullable (Float64),
    yearlyEvidenceCount Nullable (UInt32),
    isDirect Bool
) engine = Log;
