-- create the credible_sets table indexed by studyId
CREATE TABLE if not exists target_credible_sets_by_study engine = MergeTree
order by (studyId) SETTINGS allow_nullable_key = 1 as (
        select studyId, studyLocusId
        from credible_sets_log
        where
            studyId is not null
            and studyLocusId is not null
    );

-- create the credible_sets table indexed by geneId
-- join with credible_sets_by_study to get array(studyLocusId) for each gene
CREATE TABLE if not exists credible_sets_by_gene engine = MergeTree
order by (geneId) SETTINGS allow_nullable_key = 1 as (
        select
            geneId,
            groupArrayDistinctIf (
                studyLocusId,
                studyLocusId != ''
            ) as studyLocusIds
        from
            studies_log
            left outer join target_credible_sets_by_study on studies_log.studyId = target_credible_sets_by_study.studyId
        where
            geneId is not null
            and studyLocusId is not null
        group by
            geneId
    );

-- create the targets table indexed by id
-- and join credible_sets_by_gene to get array(studyLocusIds) for each target

CREATE TABLE if not exists targets engine = MergeTree ()
order by id as (
        select * except geneId
        from
            targets_log
            left outer join credible_sets_by_gene on targets_log.id = credible_sets_by_gene.geneId
    );

OPTIMIZE TABLE targets FINAL;

DROP TABLE IF EXISTS targets_log SYNC;

DROP TABLE IF EXISTS target_credible_sets_by_study SYNC;

DROP TABLE IF EXISTS credible_sets_by_gene SYNC;

DROP TABLE IF EXISTS studies_log SYNC;

DROP TABLE IF EXISTS credible_sets_log SYNC;