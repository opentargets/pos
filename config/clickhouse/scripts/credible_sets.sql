CREATE TABLE IF NOT EXISTS credible_sets ENGINE = MergeTree ()
ORDER BY studyLocusId AS (
        SELECT * except locus
        FROM credible_sets_log
    );

CREATE TABLE IF NOT EXISTS credible_sets_by_study ENGINE = MergeTree ()
ORDER BY studyId AS (
        SELECT
            studyId, groupArrayDistinct (studyLocusId) AS studyLocusIds
        FROM credible_sets_log
        WHERE
            studyId IS NOT NULL
        GROUP BY
            studyId
    );

CREATE TABLE IF NOT EXISTS credible_sets_by_variant ENGINE = MergeTree ()
ORDER BY variantId AS (
        SELECT
            arrayJoin (locus.variantId) AS variantId, groupArrayDistinct (studyLocusId) AS studyLocusIds
        FROM credible_sets_log
        GROUP BY
            variantId
    );

CREATE TABLE IF NOT EXISTS credible_sets_by_region ENGINE = MergeTree ()
ORDER BY region AS (
        SELECT
            groupArrayDistinct (studyLocusId) AS studyLocusIds, region
        FROM credible_sets_log
        WHERE
            region IS NOT NULL
        GROUP BY
            region
    );

CREATE TABLE IF NOT EXISTS credible_sets_locus ENGINE = MergeTree ()
ORDER BY studyLocusId AS (
        SELECT studyLocusId, locus
        FROM credible_sets_log
    );