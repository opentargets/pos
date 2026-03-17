CREATE TABLE IF NOT EXISTS evidence engine = MergeTree ()
order by id as (
        select *
        from evidence_log
    );

CREATE TABLE IF NOT EXISTS evidence_by_variant engine = MergeTree ()
ORDER BY (
        datasourceId, variantId, score
    ) SETTINGS allow_nullable_key = 1 AS
SELECT
    datasourceId,
    variantId,
    score,
    id
FROM evidence_log
WHERE
    variantId IS NOT NULL;

CREATE TABLE IF NOT EXISTS evidence_by_disease_and_target engine = MergeTree ()
ORDER BY (
        datasourceId, diseaseId, targetId, score
    ) SETTINGS allow_nullable_key = 1 AS
SELECT
    datasourceId,
    diseaseId,
    targetId,
    score,
    id
FROM evidence_log
WHERE
    diseaseId IS NOT NULL
    AND targetId IS NOT NULL;

DROP TABLE IF EXISTS evidence_log SYNC;