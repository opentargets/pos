CREATE TABLE IF NOT EXISTS credible_sets_by_study ENGINE = MergeTree
ORDER BY (studyId) AS (
        SELECT
            groupArrayDistinct (studyLocusId) AS studyLocusIds, studyId
        FROM credible_sets_log
        GROUP BY
            studyId
    );

CREATE TABLE if not exists studies engine = MergeTree ()
order by studyId as (
        select *
        from
            studies_log
            left outer join credible_sets_by_study on studies_log.studyId = credible_sets_by_study.studyId
    );

OPTIMIZE TABLE studies FINAL;

DROP TABLE IF EXISTS credible_sets_by_study SYNC;