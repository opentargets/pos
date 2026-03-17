CREATE TABLE IF NOT EXISTS associations_otf_disease ENGINE = MergeTree ()
ORDER BY (A, B, datasourceId) AS
SELECT
    rowId,
    diseaseId AS A,
    targetId AS B,
    datatypeId,
    datasourceId,
    rowScore,
    lower(diseaseData) AS searchA,
    lower(targetData) AS searchB,
    noveltyDirect,
    noveltyIndirect
FROM associations_otf_log;

OPTIMIZE TABLE associations_otf_disease FINAL;

CREATE TABLE IF NOT EXISTS associations_otf_target ENGINE = MergeTree ()
ORDER BY (A, B, datasourceId) AS
SELECT
    rowId,
    targetId AS A,
    diseaseId AS B,
    datatypeId,
    datasourceId,
    rowScore,
    lower(targetData) AS searchA,
    lower(diseaseData) AS searchB,
    has (
        therapeuticAreas,
        'EFO_0001444'
    ) as isMeasurement,
    noveltyDirect,
    noveltyIndirect
FROM
    associations_otf_log
    LEFT OUTER JOIN disease_log ON associations_otf_log.diseaseId = disease_log.id;

OPTIMIZE TABLE associations_otf_target FINAL;

drop table associations_otf_log SYNC;

drop table disease_log SYNC;