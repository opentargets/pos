CREATE TABLE IF NOT EXISTS mouse_phenotypes ENGINE = MergeTree() ORDER BY targetFromSourceId AS (
    SELECT 
        targetFromSourceId,
        groupArray((
            biologicalModels,
            modelPhenotypeClasses,
            modelPhenotypeId,
            modelPhenotypeLabel,
            targetFromSourceId,
            targetInModel,
            targetInModelEnsemblId,
            targetInModelMgiId
            )::Tuple(
                biologicalModels Array (
                    Tuple (
                        allelicComposition String,
                        geneticBackground String,
                        id Nullable (String),
                        literature Array (String)
                    )
                ),
                modelPhenotypeClasses Array (
                    Tuple (id String, label String)
                ),
                modelPhenotypeId String,
                modelPhenotypeLabel String,
                targetFromSourceId String,
                targetInModel String,
                targetInModelEnsemblId Nullable (String),
                targetInModelMgiId String
            )
        ) AS mouse_phenotypes
    FROM mouse_phenotypes_log
    GROUP BY
        targetFromSourceId
);

OPTIMIZE TABLE mouse_phenotypes FINAL;

DROP TABLE IF EXISTS mouse_phenotypes_log SYNC;