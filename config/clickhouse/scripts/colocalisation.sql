create table if not exists colocalisation_left engine = MergeTree ()
order by (rightStudyType, credibleSetId) as
select
    credibleSetId,
    otherCredibleSetId,
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
            leftLocusId as credibleSetId, rightLocusId as otherCredibleSetId, rightStudyType, chromosome, colocalisationMethod, numberColocalisingVariants, h3, h4, clpp, betaRatioSignAverage
        from colocalisation_log
    ) as left_colocs;

create table if not exists colocalisation_right engine = MergeTree ()
order by (rightStudyType, credibleSetId) as
select
    credibleSetId,
    otherCredibleSetId,
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
            rightLocusId as credibleSetId, leftLocusId as otherCredibleSetId, 'gwas' as rightStudyType, chromosome, colocalisationMethod, numberColocalisingVariants, h3, h4, clpp, betaRatioSignAverage
        from colocalisation_log
    ) as right_colocs;


CREATE TABLE IF NOT EXISTS colocalisation ENGINE = EmbeddedRocksDB () PRIMARY KEY credibleSetId AS (
    SELECT
        credibleSetId,
        groupArrayDistinct (
            
                (
                    credibleSetId,
                    otherCredibleSetId,
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
                    `credibleSetId` String,
                    `otherCredibleSetId` String,
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
                    `betaRatioSignAverage` Nullable(Float64)
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
        credibleSetId
);

drop table colocalisation_log SYNC;

drop table colocalisation_left SYNC;

drop table colocalisation_right SYNC;