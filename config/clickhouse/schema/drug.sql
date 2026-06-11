CREATE TABLE IF NOT EXISTS drug_log (
    id String,
    name String,
    synonyms Array (
        Tuple (
            label String,
            source String
        )
    ),
    tradeNames Array (
        Tuple (
            label String,
            source String
        )
    ),
    childChemblIds Array (String),
    drugType String,
    crossReferences Array (
        Tuple (
            source String,
            ids Array (String)
        )
    ),
    parentId Nullable (String),
    maximumClinicalStage String,
    description Nullable (String),
    molblock Nullable (String),
) ENGINE = Log;
