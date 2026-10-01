CREATE TABLE if not exists studies engine = EmbeddedRocksDB () primary key studyId as (
    select *
    from
        studies_log
        left outer join credible_sets_by_study on studies_log.studyId = credible_sets_by_study.studyId
);

DROP TABLE IF EXISTS credible_sets_by_study SYNC;
