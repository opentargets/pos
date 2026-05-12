CREATE TABLE IF NOT EXISTS association_time_series ENGINE = MergeTree ()
ORDER BY (
        diseaseId, targetId, isDirect, aggregationValue, year
    ) SETTINGS allow_nullable_key = 1 AS
SELECT
    diseaseId,
    targetId,
    aggregationType,
    aggregationValue,
    year,
    associationScore,
    novelty,
    yearlyEvidenceCount,
    isDirect
FROM association_time_series_log;

OPTIMIZE TABLE association_time_series FINAL;

DROP TABLE association_time_series_log SYNC;
