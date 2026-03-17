CREATE TABLE IF NOT EXISTS disease_hpo ENGINE = MergeTree() ORDER BY disease AS (
    SELECT 
        disease,
        groupArray((
            disease,
            phenotype,
            evidence)::Tuple(
                disease String,
                phenotype String,
                evidence Array (
                    Tuple (
                        aspect Nullable (String),
                        bioCuration Nullable (String),
                        diseaseFromSourceId String,
                        diseaseFromSource String,
                        diseaseName String,
                        evidenceType Nullable (String),
                        frequency Nullable (String),
                        modifiers Array (String),
                        onset Array (String),
                        qualifierNot Bool,
                        references Array (String),
                        sex Nullable (String),
                        resource String
                    )
                )
             )
         ) AS phenotypes
    FROM disease_hpo_log
    GROUP BY disease
);

DROP TABLE IF EXISTS disease_hpo_log SYNC;