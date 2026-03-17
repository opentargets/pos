CREATE TABLE if not exists studies_by_disease engine = MergeTree ()
order by (diseaseId) settings allow_nullable_key = 1 as
select
    arrayJoin (diseaseIds) as diseaseId,
    groupArrayDistinctIf (studyId, studyId != '') as studyIds
from studies_log
where
    diseaseIds is not null
group by
    diseaseId;

create table if not exists studies_by_disease_indirect engine = MergeTree ()
order by (diseaseId) settings allow_nullable_key = 1 as
SELECT
    id as diseaseId,
    arrayDistinct (groupArrayArray (studyIds)) AS indirectStudyIds,
    length(indirectStudyIds) AS studyCount
FROM (
        SELECT
            id,
            arrayJoin (descendants) AS indirectDiseaseId
        FROM disease_log
    ) AS disease_descendants
    LEFT JOIN studies_by_disease ON indirectDiseaseId = studies_by_disease.diseaseId
GROUP BY
    diseaseId;

CREATE TABLE if not exists disease engine = MergeTree ()
order by
    id settings allow_nullable_key = 1 as (
        select
            id,
            name,
            therapeuticAreas,
            description,
            dbXRefs,
            directLocationIds,
            indirectLocationIds,
            obsoleteTerms,
            CAST(
                tupleToNameValuePairs (synonyms),
                'Array(Tuple(relation String, terms Array(Nullable(String))))'
            ) as synonyms,
            parents,
            children,
            ancestors,
            descendants,
            ontology.isTherapeuticArea as isTherapeuticArea,
            studies_by_disease.studyIds as studyIds,
            indirect_studies.indirectStudyIds as indirectStudyIds
        from
            disease_log
            left outer join studies_by_disease on disease_log.id = studies_by_disease.diseaseId
            left outer join studies_by_disease_indirect as indirect_studies on disease_log.id = indirect_studies.diseaseId
    );

OPTIMIZE TABLE disease FINAL;

DROP TABLE IF EXISTS studies_by_disease SYNC;

DROP TABLE IF EXISTS studies_by_disease_indirect SYNC;