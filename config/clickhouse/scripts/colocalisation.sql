create table if not exists colocalisation_left engine = MergeTree ()
order by (rightStudyType, studyLocusId) as
select
    studyLocusId,
    otherStudyLocusId,
    rightStudyType,
    chromosome,
    colocalisationMethod,
    numberColocalisingVariants,
    h3,
    h4,
    clpp,
    betaRatioSignAverage
from (
        select
            leftStudyLocusId as studyLocusId, rightStudyLocusId as otherStudyLocusId, rightStudyType, chromosome, colocalisationMethod, numberColocalisingVariants, h3, h4, clpp, betaRatioSignAverage
        from colocalisation_log
    ) as left_colocs;

OPTIMIZE TABLE colocalisation_left FINAL;

create table if not exists colocalisation_right engine = MergeTree ()
order by (rightStudyType, studyLocusId) as
select
    studyLocusId,
    otherStudyLocusId,
    rightStudyType,
    chromosome,
    colocalisationMethod,
    numberColocalisingVariants,
    h3,
    h4,
    clpp,
    betaRatioSignAverage
from (
        select
            rightStudyLocusId as studyLocusId, leftStudyLocusId as otherStudyLocusId, 'gwas' as rightStudyType, chromosome, colocalisationMethod, numberColocalisingVariants, h3, h4, clpp, betaRatioSignAverage
        from colocalisation_log
    ) as right_colocs;

OPTIMIZE TABLE colocalisation_right FINAL;


CREATE TABLE IF NOT EXISTS colocalisation ENGINE = MergeTree() ORDER BY studyLocusId AS (
    SELECT
        studyLocusId,
        groupArrayDistinct (
            
                (
                    studyLocusId,
                    otherStudyLocusId,
                    rightStudyType,
                    chromosome,
                    colocalisationMethod,
                    numberColocalisingVariants,
                    h3,
                    h4,
                    clpp,
                    betaRatioSignAverage
                )::
                Tuple(
                    `studyLocusId` String,
                    `otherStudyLocusId` String,
                    `rightStudyType` Enum(
                        'tuqtl',
                        'pqtl',
                        'eqtl',
                        'sqtl',
                        'sctuqtl',
                        'scpqtl',
                        'sceqtl',
                        'scsqtl',
                        'gwas'
                    ),
                    `chromosome` Enum(
                        '1',
                        '2',
                        '3',
                        '4',
                        '5',
                        '6',
                        '7',
                        '8',
                        '9',
                        '10',
                        '11',
                        '12',
                        '13',
                        '14',
                        '15',
                        '16',
                        '17',
                        '18',
                        '19',
                        '20',
                        '21',
                        '22',
                        'X',
                        'Y',
                        'MT'
                    ),
                    `colocalisationMethod` LowCardinality (String),
                    `numberColocalisingVariants` UInt32,
                    `h3` Float64,
                    `h4` Float64,
                    `clpp` Float64,
                    `betaRatioSignAverage` Float64
                )
            ) as colocalisation
    FROM (
        SELECT *
        FROM colocalisation_left
        UNION ALL
        SELECT *
        FROM colocalisation_right
    )
    GROUP BY
        studyLocusId
);

drop table colocalisation_log SYNC;

drop table colocalisation_left SYNC;

drop table colocalisation_right SYNC;